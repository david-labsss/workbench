"""Scrape store.vibrant.com (Vibrant Technologies; AmeriCommerce / Cart.com storefront, ASP.NET
WebForms, behind Cloudflare; ~36k used / refurbished servers, storage and networking parts): every
run's full snapshot appended to Postgres.

The store has no product API or feed, but its three top categories list every product, and a
category page takes the page number, page size and sort order in its URL:
  /<category>?pi=N&ps=250&sort=ItemIDDesc
(robots.txt only disallows the admin / cart / checkout / review pages, attribute filters
"Attrib(s)=" and /resize). Categories (path, "of N" products on 2026-10-20):
  data-storage/index.html          8,085  Storage
  networking/index.html            2,985  Networking
  used-servers-for-sale.aspx      25,044  Servers   (servers plus their parts: CPUs, memory,
                                                     drives, NICs, RAID cards, rails ...)
That is ~146 pages of 250 products (~1 MB each; the server takes ~8 s to build one), read 3 at a
time: ~5 min. 195 products are in two of the categories (2026-10-20: 36,114 listed, 35,919 unique). sort=ItemIDDesc ("Newest Products") orders by the store's item id, which is unique, so the
order cannot change between pages the way the default "Relevance" order might; a product added
during the run (new, highest id) only pushes others down a page, which shows up as a duplicate
and is deduped. Products are deduped by item id across categories too (the first category in
CATEGORIES that lists a product names its category).

Every page is checked: its "Products A-B of N" label must start at item (page - 1) * 250 + 1, end
at min(page * 250, N), and show exactly B - A + 1 product cards, or the page raises (after the
download retries in get()). A page past the end because the catalogue shrank mid-run shows no
cards and is accepted with a warning. After each category, the unique products found must reach
the smallest "of N" it showed (a few short only when products disappeared mid-run, else it raises).

Each card gives the item id (data-itemId), name, URL, price ("$1,234.00"; "Contact for Pricing"
-> price None, never 0), the strike-through retail price when there is one (compare_at_price),
"Item #" (the part number; saved as sku and part_number), and the availability text with the
exact stock count in an HTML comment next to it:
  <!--<span ...>Availability: In Stock</span> (12) -->  In Stock
  "In Stock"                  available, stock_qty = the count (product pages allow buying up to
                              that many, data-qty-max)
  "Contact for Availability"  not available, count 0 (product pages allow 0); many of these are
                              also "Contact for Pricing"
  "Discontinued"              not available; only on product pages (see the safety net below)
  anything else               not available (printed at the end, so new texts show up)
On 2026-10-20 every In Stock product had a price and no other product had one.
The count is only used when it agrees with the text (In Stock with a count of 0 -> stock_qty
empty). Names are "<Brand> <Item #> <description>", so brand = the words before the item number
(each brand's most common spelling: the store writes both "DELL" and "Dell").
Condition is not on the card; every product page says "Unless otherwise noted, this item is
Refurbished", so condition is Refurbished unless the name says New / NOB (New Open Box) / Used.
Capacity is read from drive / memory names (not whole servers). Category = the top category.

Safety net: the product sitemaps (sitemap_products_01..04.xml, ~36k URLs, rebuilt daily) are
compared with what the listings gave. Product URLs end in "-<item id>.html" (a few dozen are
custom, e.g. /Dell-X0884.html, and are matched by URL instead). Sitemap products missing from the
listings are read from their own product pages (JSON-LD name / mpn / manufacturer / price, plus
the page's "Availability:" text and data-qty-max; the JSON-LD availability always says InStock,
so it is not used). On 2026-10-20 that was 61 products, all "Discontinued" (the listings hide
those; they are saved as not available, no price), plus 7 second URLs of products already found:
~1 min. More than MAX_BACKFILL missing means the listings are broken: the run raises.
listed = the number of products in the sitemaps (an independent count; the three "of N" totals
can overlap, so their sum is not used).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import json
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit

import requests
from bs4 import BeautifulSoup, Comment

from common import http, runner, schema

SITE = "vibrant"
BASE = "https://store.vibrant.com"
CATEGORIES = [                 # (path, category name); first one listing a product names it
    ("data-storage/index.html", "Storage"),
    ("networking/index.html", "Networking"),
    ("used-servers-for-sale.aspx", "Servers"),
]
PAGE_SIZE = 250                # the largest page size seen to work (the menu offers up to 60)
SORT = "ItemIDDesc"            # "Newest Products": by unique item id, stable across pages
WORKERS = 3                    # requests in flight
DELAY = 1.0                    # per worker, after each request
TRIES = 3                      # attempts per request for stalled / cut-off downloads
RETRY_PAUSE = 30
CHURN_SLACK = 25               # products a category may come up short when some disappear mid-run
MAX_BACKFILL = 500             # sitemap products missing from the listings read one by one
COUNT_RE = re.compile(r"([\d,]+)\s*-\s*([\d,]+)\s+of\s+([\d,]+)")
STOCK_RE = re.compile(r"Availability:\s*([^<]*?)\s*</span>\s*\((-?\d+)\)")
PRICE_RE = re.compile(r"\$\s*([\d,]+(?:\.\d+)?)")
SITEMAP_ID_RE = re.compile(r"-(\d+)\.html$")
IN_STOCK_RE = re.compile(r"^in stock\b", re.I)
CAPACITY_RE = re.compile(r"(?<![\d.])\b(\d+(?:\.\d+)?)\s?(TB|GB)\b(?!\s*/\s*s|ps)")
DRIVE_RE = re.compile(r"hard (?:disk )?drive|\bhdd\b|\bssd\b|solid state|\bnvme\b", re.I)
MEMORY_RE = re.compile(r"\bmemory\b|\bdimm\b|\brdimm\b|\blrdimm\b|\bsdram\b|\bddr\d", re.I)
# Whole servers / storage systems ("R740 2x Gold 6150 256GB RAM 8x 1.2TB") are not drives / memory
COMPUTER_RE = re.compile(r"GB\s+RAM\b|\b(?:xeon|epyc|opteron|core i\d|poweredge|proliant|"
                         r"chassis|array|enclosure)\b", re.I)
# Storage systems / drive bundles ("with 24 x 4.8TB NVMe"): the first capacity is not the item's
# (memory kits "16GB Kit (2X8GB)" are fine: the first capacity is the kit's)
BUNDLE_RE = re.compile(r"\b(?:[2-9]|\d\d+)\s?x\s?\d+(?:\.\d+)?\s?(?:TB|GB)\b", re.I)
PAGE_STOCK_RE = re.compile(r"<b>\s*Availability:\s*([^<]*?)\s*</b>")
CONDITIONS = [(re.compile(r"\bnob\b|\bopen[\s-]?box\b", re.I), "New Open Box"),
              (re.compile(r"\bnew\b|\bnib\b|factory sealed", re.I), "New"),
              (re.compile(r"\bused\b|\bpre-?owned\b", re.I), "Used"),
              (re.compile(r"\brefurb", re.I), "Refurbished")]
DEFAULT_CONDITION = "Refurbished"   # "Unless otherwise noted, this item is Refurbished"


class Gone(Exception):
    """A product page that no longer exists (404 / 410)."""


def get(url, allow_gone=False):
    """GET with http's 429 / 5xx retries, plus a few tries for a download the server stalls or
    cuts off part-way (requests does not retry those). The last failure raises. A Cloudflare
    challenge raises at once (it is not worked around)."""
    for attempt in range(1, TRIES + 1):
        try:
            r = http.session().get(url, timeout=(30, 240))
            if r.status_code in (403, 503) and (r.headers.get("cf-mitigated")
                                                or "Just a moment" in r.text[:3000]):
                raise RuntimeError(f"{url}: Cloudflare challenge (HTTP {r.status_code}); "
                                   "not bypassing it - slow down or try later")
            if allow_gone and r.status_code in (404, 410):
                raise Gone(url)
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            if attempt == TRIES:
                raise
            print(f"WARN {url[:100]}: {e.__class__.__name__}, retrying in {RETRY_PAUSE}s",
                  flush=True)
            time.sleep(RETRY_PAUSE)


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def money(text):
    m = PRICE_RE.search(text or "")
    value = float(m.group(1).replace(",", "")) if m else None
    return value or None                          # "Contact for Pricing" / $0 = no real price


def stock(text, count):
    """(available, stock_qty) from the availability text and the count next to it."""
    available = bool(IN_STOCK_RE.search(text or ""))
    if count is None or available != (count > 0):
        return available, None                    # no count, or one that contradicts the text
    return available, count


def page_url(path, page):
    return f"{BASE}/{path}?pi={page}&ps={PAGE_SIZE}&sort={SORT}"


def parse_cards(soup, category):
    products = []
    for info in soup.select("input.CategoryListItemInfo"):
        pid = info.get("data-itemid") or info.get("data-itemId")
        card = info.find_next_sibling("div", class_="category-product")
        link = card.select_one("a.h5[href]") if card else None
        if not pid or not link:
            raise RuntimeError(f"product card {pid} without a name / link")
        avail = card.select_one("div.CategoryProductAvailability")
        note = avail.find(string=lambda s: isinstance(s, Comment)) if avail else None
        m = STOCK_RE.search(str(note or ""))
        text = clean(avail.get_text()) if avail else ""
        available, qty = stock(text or (m.group(1) if m else ""), int(m.group(2)) if m else None)
        nr = card.select_one(".CategoryProductItemNr")
        item_nr = ""
        if nr:
            for extra in nr.select(".CategoryProductItemNrLbl, .CategoryProductItemNrDash"):
                extra.extract()
            item_nr = clean(nr.get_text())
        price_el = card.select_one(".CategoryProductPrice")
        retail = card.select_one(".CategoryProductRetailArea del")
        products.append({
            "id": pid,
            "title": clean(link.get_text()),
            "url": BASE + link["href"] if link["href"].startswith("/") else link["href"],
            "item_nr": item_nr,
            "price": money(price_el.get_text() if price_el else ""),
            "retail": money(retail.get_text() if retail else ""),
            "availability": text,
            "available": available,
            "stock_qty": qty,
            "category": category,
        })
    return products


def fetch_page(cat, page):
    """(cards, total) of one listing page; a page whose cards don't match its label raises."""
    path, category = cat
    url = page_url(path, page)
    soup = BeautifulSoup(get(url).text, "html.parser")
    cards = parse_cards(soup, category)
    label = soup.select_one("#lblProductCountTop")
    m = COUNT_RE.search(clean(label.get_text())) if label else None
    if not m:
        raise RuntimeError(f"{url}: no 'Products A-B of N' label on the page")
    first, last, total = (int(x.replace(",", "")) for x in m.groups())
    start = (page - 1) * PAGE_SIZE + 1
    if start > total and not cards:              # the catalogue shrank while walking
        print(f"WARN {url}: past the end now ({total} products), no cards", flush=True)
    elif (first, last, len(cards)) != (start, min(page * PAGE_SIZE, total), last - first + 1):
        raise RuntimeError(f"{url}: {len(cards)} cards, label says {first}-{last} of {total}, "
                           f"page {page} should show {start}-{min(page * PAGE_SIZE, total)}")
    time.sleep(DELAY)
    return cards, total


def walk(limit):
    """{item id: card} from every page of every category (first category wins)."""
    cats = CATEGORIES[:1] if limit else CATEGORIES
    by_id, overlap = {}, 0
    with ThreadPoolExecutor(WORKERS) as pool:
        firsts = list(pool.map(lambda c: fetch_page(c, 1), cats))
        tasks = []
        for cat, (cards, total) in zip(cats, firsts):
            pages = math.ceil(total / PAGE_SIZE)
            print(f"{cat[0]}: {total} products, {pages} pages", flush=True)
            if not (limit and len(cards) >= limit):
                tasks += [(cat, page) for page in range(2, pages + 1)]
        results = {cat: [first] for cat, first in zip(cats, firsts)}
        for done, (task, result) in enumerate(
                zip(tasks, pool.map(lambda t: fetch_page(*t), tasks)), 1):
            results[task[0]].append(result)
            if done % 10 == 0 or done == len(tasks):
                print(f"{done}/{len(tasks)} more pages read", flush=True)
    totals = {}
    for cat in cats:
        found = {}
        for cards, _ in results[cat]:
            for c in cards:
                found.setdefault(c["id"], c)
        shown = min(total for _, total in results[cat])
        totals[cat[1]] = shown
        short = shown - len(found)
        if short > CHURN_SLACK and not limit:
            raise RuntimeError(f"{cat[0]}: only {len(found)} unique products of {shown}")
        if short > 0 and not limit:
            print(f"WARN {cat[0]}: {len(found)} unique products of {shown} "
                  "(some disappeared mid-run?)", flush=True)
        for pid, c in found.items():
            if pid in by_id:
                overlap += 1
            else:
                by_id[pid] = c
        print(f"{cat[0]}: {len(found)} unique products of {shown}", flush=True)
    print(f"{len(by_id)} unique products from the listings (category totals {totals}, "
          f"{overlap} listed in more than one)", flush=True)
    return by_id


def sitemap_products():
    """{key: url} of every product sitemap URL; key = item id, or the URL path when the URL
    carries no id."""
    index = get(f"{BASE}/sitemap.xml").text
    maps = re.findall(r"<loc>([^<]*sitemap_products_\d+\.xml)</loc>", index)
    if not maps:
        raise RuntimeError("no product sitemaps in sitemap.xml")
    products = {}
    for sm in maps:
        locs = re.findall(r"<loc>([^<]+)</loc>", get(sm).text)
        if not locs:
            raise RuntimeError(f"{sm}: no product URLs")
        for loc in locs:
            loc = clean(loc)
            m = SITEMAP_ID_RE.search(loc)
            products[m.group(1) if m else urlsplit(loc).path.lower()] = loc
    return products


def product_page(url):
    """A card-like dict from a product page (JSON-LD + availability), or None if it is gone."""
    try:
        r = get(url, allow_gone=True)
    except Gone:
        return None
    finally:
        time.sleep(DELAY)
    soup = BeautifulSoup(r.text, "html.parser")
    data = None
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            d = json.loads(script.string or "")
        except ValueError:
            continue
        if isinstance(d, dict) and d.get("@type") == "Product":
            data = d
            break
    if not data or not str(data.get("sku", "")).isdigit():
        return None                                  # redirected away: no product here now
    m = PAGE_STOCK_RE.search(r.text)
    text = clean(m.group(1)) if m else ""
    qty_el = soup.select_one("#dvProductQuantity[data-qty-max]")
    qty = int(qty_el["data-qty-max"]) if qty_el and qty_el["data-qty-max"].isdigit() else None
    available, qty = stock(text, qty)
    trail = soup.select("#lblCategoryTrail a[href]")[1:]       # [0] is Home
    names = {p.lower(): n for p, n in CATEGORIES}
    first = trail[0]["href"].strip("/").lower() if trail else ""
    offer = data.get("offers") if isinstance(data.get("offers"), dict) else {}
    price = clean(str(offer.get("price", "")))      # "95.00", or a "Contact for Pricing" link
    item_nr = clean(str(data.get("mpn", "")))
    return {
        "id": str(data["sku"]),
        "title": clean(str(data.get("name", ""))),
        "url": clean(offer.get("url") or r.url),
        "item_nr": item_nr,
        "brand": clean(str(data.get("manufacturer", ""))),
        "price": money("$" + price) if re.fullmatch(r"[\d,]+(?:\.\d+)?", price) else None,
        "retail": None,
        "availability": text,
        "available": available,
        "stock_qty": qty,
        "category": names.get(first, clean(trail[0].get_text()) if trail else ""),
    }


def backfill(by_id):
    """Add the sitemap's products that the listings missed; returns the sitemap's size."""
    sitemap = sitemap_products()
    paths = {urlsplit(c["url"]).path.lower() for c in by_id.values()}
    missing = [url for key, url in sitemap.items()
               if key not in by_id and urlsplit(url).path.lower() not in paths]
    sitemap_paths = {urlsplit(url).path.lower() for url in sitemap.values()}
    extra = sum(1 for pid, c in by_id.items()
                if pid not in sitemap and urlsplit(c["url"]).path.lower() not in sitemap_paths)
    print(f"sitemaps list {len(sitemap)} products; {len(missing)} of them not in the listings, "
          f"{extra} listing products not in the sitemaps (newer?)", flush=True)
    if len(missing) > MAX_BACKFILL:
        raise RuntimeError(f"{len(missing)} sitemap products missing from the listings "
                           f"(more than {MAX_BACKFILL}): the listings look broken")
    added = gone = 0
    with ThreadPoolExecutor(WORKERS) as pool:
        for p in pool.map(product_page, missing):
            if p is None:
                gone += 1
            elif p["id"] not in by_id:
                by_id[p["id"]] = p
                added += 1
    print(f"backfill: {added} products added from product pages, {gone} pages gone, "
          f"{len(missing) - added - gone} already listed under another URL", flush=True)
    return len(sitemap)


def brand_of(title, item_nr):
    """Names are "<Brand> <Item #> <description>": the words before the item number."""
    if item_nr and f" {item_nr} " in f" {title} ":
        brand = f" {title} ".split(f" {item_nr} ", 1)[0].strip()
        if brand and len(brand.split()) <= 4:
            return brand
        if not brand:
            return ""                             # the name starts with the item number
    words = title.split(" ", 1)
    return words[0] if len(words) > 1 else ""     # a bare part number is not a brand


def unify_brands(products):
    """The store spells some brands two ways ("DELL" / "Dell"): give each its most common
    spelling."""
    spellings = {}
    for p in products:
        p["brand"] = p.get("brand") or brand_of(p["title"], p["item_nr"])
        counts = spellings.setdefault(p["brand"].lower(), {})
        counts[p["brand"]] = counts.get(p["brand"], 0) + 1
    for p in products:
        counts = spellings[p["brand"].lower()]
        p["brand"] = max(counts, key=counts.get)


def build_row(p, scraped_at):
    title = p["title"]
    drive = DRIVE_RE.search(title) and not BUNDLE_RE.search(title)
    part = (drive or MEMORY_RE.search(title)) and not COMPUTER_RE.search(title)
    cap = CAPACITY_RE.search(title) if part else None
    tb = None
    if cap and drive:
        tb = float(cap.group(1)) if cap.group(2) == "TB" else float(cap.group(1)) / 1000
    price = p["price"]
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": str(p["id"]),
        "sku": p["item_nr"],
        "part_number": p["item_nr"],
        "title": title,
        "brand": p["brand"],
        "category": p["category"],
        "condition": next((c for rx, c in CONDITIONS if rx.search(title)), DEFAULT_CONDITION),
        "capacity": f"{cap.group(1)}{cap.group(2)}" if cap else "",
        "price": price,
        "compare_at_price": p["retail"] if p["retail"] and price and p["retail"] > price else None,
        "price_per_tb": round(price / tb, 2) if price and tb else None,
        "available": p["available"],
        "stock_qty": p["stock_qty"],
        "url": p["url"],
    })


def scrape(limit, scraped_at):
    by_id = walk(limit)
    listed = None
    if not limit:                                   # test runs skip the sitemaps and backfill
        listed = backfill(by_id)
    texts = {}
    for p in by_id.values():
        texts[p["availability"]] = texts.get(p["availability"], 0) + 1
    print(f"availability texts: {texts}", flush=True)
    unify_brands(by_id.values())
    items = list(by_id.values())[:limit] if limit else list(by_id.values())
    return [build_row(p, scraped_at) for p in items], listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
