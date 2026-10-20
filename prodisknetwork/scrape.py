"""Scrape prodisknetwork.com (Pro Disk Network, headless Magento on plain nginx, no Cloudflare):
every run's full snapshot appended to Postgres.

Data path: the store's own Google Merchant feed, https://api.prodisknetwork.com/merchant-feed.xml.
The site advertises it in its JSON-LD Dataset ("Google Merchant Feed (~150K products)") and
robots.txt does not disallow it (robots.txt does disallow /graphql, /rest/ and /search for `*`,
so those are NOT used). The feed is one RSS file, ~15.7 MB gzip (served with Content-Encoding
gzip, ~320 MB of XML once decoded), rebuilt daily around 04:00 UTC (Cache-Control max-age 6 h),
with no Range / resume support. One GET gives every product in the feed (see Coverage): no page
crawl. The server (Hetzner, Oregon) is slow and drops long transfers: on 2026-10-20 a local
download ran at 9-260 KB/s and broke at 3.8 MB and 14.3 MB before the third try got it all.

Download: one streamed GET; requests decodes the gzip and the XML is parsed as it arrives
(XMLPullParser, each <item> dropped from the tree once read), so memory stays low and a test run
(MAX_PRODUCTS) stops after the first few hundred KB. A download is only accepted when it is
complete: urllib3 checks the body against Content-Length (a cut-off transfer raises
ChunkedEncodingError), the XML must close (</rss>, else ParseError), and a full run must have at
least MIN_ITEMS items. Any of those -- or a stalled / broken connection -- throws the whole
attempt away and downloads again from the start (TRIES times, RETRY_PAUSE apart); the last
failure raises, so a partial feed is never saved.

Per <item> (Google Merchant fields, namespace g:):
  product_id, sku  g:id (the store's SKU, unique in the feed)
  part_number      g:mpn (same as g:id in every item sampled), else g:id
  parent_id        g:item_group_id: the store groups related part numbers under the base part
                   (376176-B21 and 376176-001 -> 376176); on about half the items
  title, brand, url  g:title, g:brand, g:link
  category         last step of g:product_type ("Electronics > ... > Server Accessories")
  condition        g:condition, capitalised (refurbished -> Refurbished, new -> New)
  price            g:sale_price when lower than g:price (then g:price is compare_at_price),
                   else g:price; None when missing or 0 (never 0)
  available        g:availability == in_stock
  stock_qty        blank: the feed (and every product page) only says "In Stock", no counts
  image_url, description  g:image_link (absolute), g:description (as plain text)
  specs            g:product_detail attribute name / value pairs: Interface, Form Factor,
                   Rotational Speed, Cache, Endurance go to their columns, the rest (Data Rate,
                   Speed, Memory Type, ...) to other_specs ("Name: value; ..."). The feed's
                   "Capacity" is only kept on drive / memory / flash / tape items (title or
                   category), not on whole servers / CPU boards, and not when it is really the
                   link speed ("100 GB" next to "Data Rate: 100 Gb/s"). price_per_tb: drives.

Coverage (2026-10-20 full feed): 124,469 items, unique g:id, all priced ($10.53-$52,429),
124,468 in_stock. The feed is NOT the whole priced catalogue: of 105 priced items on 7 category
pages, 52 were in the feed (all at the same price) and 53 were not, with nothing on their pages
to tell them apart. Quote-only ("Contact Us for Price") products are never in it. So this
scraper tracks the feed's ~124k products, roughly half of the store's priced items.

listed = the number of <item>s in the feed, so the runner's 98% check confirms every item was
turned into a row. Run around 05:00-23:00 UTC, away from the ~04:00 UTC rebuild.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
import time
import xml.etree.ElementTree as ET

import requests

from common import http, runner, schema

SITE = "prodisknetwork"
FEED = "https://api.prodisknetwork.com/merchant-feed.xml"
G = "{http://base.google.com/ns/1.0}"
TRIES = 5                    # whole-feed download attempts
RETRY_PAUSE = 60             # seconds between attempts
TIMEOUT = (30, 120)          # connect, and max stall between chunks
CHUNK = 256 * 1024
MIN_ITEMS = 50000            # a full feed has ~110-150k items; fewer = a broken / partial file
PROGRESS_EVERY = 25000

PRICE_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")
CAPACITY_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(TB|GB|MB)\s*$", re.I)
DRIVE_RE = re.compile(r"hard (?:disk )?drive|\bhdd\b|\bssd\b|solid state|\bnvme\b|disk drive",
                      re.I)
# Items whose "Capacity" is a real storage / memory size ...
STORAGE_RE = re.compile(DRIVE_RE.pattern + r"|\bmemory\b|\bdimm\b|\brdimm\b|\blrdimm\b|\bsdram\b"
                        r"|\bddr\d|\bflash\b|\btape\b", re.I)
# ... but not whole servers / CPU boards ("Quad Core 3.1GHz 4GB RAM No HDD Hard Drive")
COMPUTER_RE = re.compile(r"\d\s?GB\s+RAM\b|\bghz\b|\d(?:\.\d+)?ghz|\bcpus?\b|\bxeon\b|\bcore i\d"
                         r"|\bepyc\b|\bopteron\b|\bitanium\b|\b(?:dual|quad|hexa|octa|\d+)[- ]core\b",
                         re.I)
NUMBER_RE = re.compile(r"\d+(?:\.\d+)?")
SPEC_COLUMNS = {
    "interface": "interface", "form factor": "form_factor",
    "rotational speed": "spindle_speed", "spindle speed": "spindle_speed",
    "cache": "cache", "cache size": "cache", "buffer size": "cache",
    "endurance": "endurance",
}
CONDITIONS = {"new": "New", "refurbished": "Refurbished", "used": "Used"}

# A download that broke, stalled, or came back incomplete: worth another full attempt
RETRYABLE = (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError,
             requests.exceptions.ContentDecodingError, ET.ParseError)


class IncompleteFeed(Exception):
    pass


def text(item, tag):
    """A field of an <item>: g:<tag>, or plain <tag> (the Merchant spec allows both)."""
    return re.sub(r"\s+", " ", item.findtext(G + tag) or item.findtext(tag) or "").strip()


def money(value):
    m = PRICE_RE.search(value or "")
    return float(m.group(0).replace(",", "")) if m else None


def specs(item):
    out = {}
    for d in item.findall(G + "product_detail"):
        name, value = text(d, "attribute_name"), text(d, "attribute_value")
        if name and value and name not in out:
            out[name] = value
    return out


def build_row(item, scraped_at):
    sku = text(item, "id")
    title = text(item, "title")
    category = text(item, "product_type").split(" > ")[-1].strip()
    regular = money(text(item, "price")) or None          # $0 = no real price
    sale = money(text(item, "sale_price")) or None
    price, compare = regular, None
    if sale and regular and sale < regular:
        price, compare = sale, regular
    elif sale and not regular:
        price = sale

    attrs = specs(item)
    cols, other = {}, []
    words = f"{title} {category}"
    # On network gear "Capacity" is the link speed: "100 GB" next to "Data Rate: 100 Gb/s"
    rates = {n for k, v in attrs.items() if k.lower() in ("data rate", "network speed")
             for n in NUMBER_RE.findall(v)}
    sized = STORAGE_RE.search(words) and not COMPUTER_RE.search(title)
    for name, value in attrs.items():
        key = name.lower()
        if key == "capacity":
            m = CAPACITY_RE.match(value)
            if m and sized and not (m.group(2).upper() == "GB" and m.group(1) in rates):
                cols["capacity"] = f"{m.group(1)}{m.group(2).upper()}"
            continue
        col = SPEC_COLUMNS.get(key)
        if col == "cache":
            value = re.sub(r"\s*cache\s*$", "", value, flags=re.I)      # "8 MB cache"
        if col and col not in cols:
            cols[col] = value
        else:
            other.append(f"{name}: {value}")

    tb = None
    cap = CAPACITY_RE.match(cols.get("capacity", ""))
    if cap and cap.group(2).upper() in ("TB", "GB") and DRIVE_RE.search(words):
        tb = float(cap.group(1)) / (1 if cap.group(2).upper() == "TB" else 1000)
    condition = text(item, "condition").lower()

    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": sku,
        "parent_id": text(item, "item_group_id"),
        "sku": sku,
        "part_number": text(item, "mpn") or sku,
        "title": title,
        "brand": text(item, "brand"),
        "category": category,
        "condition": CONDITIONS.get(condition, condition.title()),
        "capacity": cols.get("capacity"),
        "interface": cols.get("interface"),
        "form_factor": cols.get("form_factor"),
        "spindle_speed": cols.get("spindle_speed"),
        "cache": cols.get("cache"),
        "endurance": cols.get("endurance"),
        "other_specs": "; ".join(other),
        "price": price,
        "compare_at_price": compare,
        "price_per_tb": round(price / tb, 2) if price and tb else None,
        "discount_pct": round((compare - price) / compare * 100, 1) if compare else None,
        "available": text(item, "availability").lower() == "in_stock",
        "stock_qty": None,                  # no counts anywhere on the site
        "image_url": text(item, "image_link") or None,
        "description": schema.plain_text(text(item, "description")),
        "url": text(item, "link"),
    })


def read_feed(limit, scraped_at):
    """One download of the feed, parsed while it streams. Returns (rows, items in the feed);
    a test run (limit) stops after `limit` rows and returns None for the item count. Raises
    (RETRYABLE / IncompleteFeed) unless the whole feed arrived."""
    started = time.time()
    by_id, items, dupes, no_id = {}, 0, 0, 0
    parser = ET.XMLPullParser(events=("start", "end"))
    parents = []                            # open elements, to drop each <item> once read

    def handle(events):
        nonlocal items, dupes, no_id
        for event, el in events:
            if event == "start":
                parents.append(el)
                continue
            parents.pop()
            if el.tag != "item":
                continue
            items += 1
            row = build_row(el, scraped_at)
            pid = row[schema.PRODUCT_COLUMNS.index("product_id")]
            if not pid:
                no_id += 1
            elif pid in by_id:
                dupes += 1
            else:
                by_id[pid] = row
            if parents:
                parents[-1].remove(el)      # keep memory flat over ~150k items
            if items % PROGRESS_EVERY == 0:
                got = r.raw.tell() / 1e6
                print(f"  {items} items, {got:.1f} of {size / 1e6:.1f} MB, "
                      f"{time.time() - started:.0f}s", flush=True)
            if limit and len(by_id) >= limit:
                return True
        return False

    with http.session().get(FEED, stream=True, timeout=TIMEOUT,
                            headers={"Accept-Encoding": "gzip"}) as r:
        r.raise_for_status()
        size = int(r.headers.get("Content-Length") or 0)
        print(f"feed: {size / 1e6:.1f} MB ({r.headers.get('Content-Encoding') or 'plain'}), "
              f"built {r.headers.get('Last-Modified')}", flush=True)
        for chunk in r.iter_content(CHUNK):
            parser.feed(chunk)
            if handle(parser.read_events()):
                return list(by_id.values()), None
        parser.close()                      # ParseError if the XML did not close
        handle(parser.read_events())
    if no_id or dupes:
        print(f"WARN {no_id} items without g:id, {dupes} repeated g:id (first kept)", flush=True)
    if not limit and items < MIN_ITEMS:
        raise IncompleteFeed(f"feed has only {items} items (expected {MIN_ITEMS}+)")
    print(f"feed parsed: {items} items, {len(by_id)} products in {time.time() - started:.0f}s",
          flush=True)
    return list(by_id.values()), items


def scrape(limit, scraped_at):
    for attempt in range(1, TRIES + 1):
        try:
            return read_feed(limit, scraped_at)
        except RETRYABLE + (IncompleteFeed,) as e:
            if attempt == TRIES:
                raise
            print(f"WARN feed attempt {attempt}/{TRIES} failed ({e.__class__.__name__}: "
                  f"{str(e)[:160]}); retrying in {RETRY_PAUSE}s", flush=True)
            time.sleep(RETRY_PAUSE)


if __name__ == "__main__":
    runner.run(SITE, scrape)
