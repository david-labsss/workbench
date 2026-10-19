"""Scrape ithardwarehub.com (BigCommerce Stencil, ~203k products): every run's full snapshot
appended to Postgres.

The store has no storefront GraphQL token in its pages (so common/bigcommerce.py cannot be used).
Its brand listing pages only reach ~56% of the catalogue (112,838 of 203,022 products after all
235 brands, 1,288 pages, ~40 min, 2026-10-19: many products have no brand), and walking its 340
categories on top would take another ~40 min. So products are read the way the cpumedics and
flagshiptech scrapers do: from BigCommerce's product comparison page, which renders any list of
product ids given in its URL (/compare/1/2/3/...; robots.txt does not disallow it). The store's
product ids run from ~110 to ~220,800 with few gaps (~92% used), so the scraper asks for 200 ids
at a time across that whole range (~1,100 pages of ~1.4 MB, 3 at a time, ~15-20 min). The top is
found each run by probing upwards (the catalogue can grow), and nothing is skipped in the middle,
so a gap in the id space cannot end the scan early. A failed page raises.

Each comparison column is one product. Its id is the one missing from its own "remove" link (that
link lists every other product on the page), with the product image path (/products/<id>/) as a
fallback. The column gives the title, URL, brand, price ([data-product-price-without-tax]) and the
RRP / non-sale price (saved as compare_at_price when higher), the buy button and the store's
"Availability" text:
  "Add to Cart" / "Choose Options" / "Buy Now" (and no "out of stock" text)   in stock
  anything else / none                                                        out of stock
On 2026-10-19 every product sampled had "Add to Cart" with "In Stock" (the comparison page leaves
out products the store hides). Titles start with the part number ("Part No: 39Y9278 - IBM ...",
"00AD010 - IBM 1TB ..."), which the product page repeats as "Part Number #", so part_number is
read from the title (leading or trailing " - " token, "Part No X", or a trailing "(X)"). SKU,
category, condition and stock counts are not on the page; condition is read from the title when
it says so, capacity from drive / memory titles (upper-case GB / TB, not GB/s; not from whole
computers' titles). The number of products to check the snapshot against is the product
sitemap's URL count.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "ithardwarehub"
BASE = "https://ithardwarehub.com"
CHUNK = 200                   # product ids per comparison page
WORKERS = 3
DELAY = 0.5                   # between batches of requests, to stay polite
START = 1                     # first id to scan on a full run (ids begin ~110)
SEED = 200000                 # a known-populated id, used to find the top and for test runs
TRIES = 3                     # attempts per request for stalled / cut-off downloads
RETRY_PAUSE = 30
IN_STOCK_BUTTONS = {"add to cart", "choose options", "buy now"}
OUT_OF_STOCK_RE = re.compile(r"out of stock|sold out|unavailable|discontinued", re.I)
PRICE_RE = re.compile(r"[\d,]+(?:\.\d+)?")
TOKEN = r"([A-Z0-9][A-Za-z0-9#./_+-]*\d[A-Za-z0-9#./_+-]*)"
LEAD_PART_RE = re.compile(rf"^(?:part\s*(?:no|number|#)\.?\s*:?\s*)?{TOKEN}\s+-\s+", re.I)
TAIL_PART_RE = re.compile(rf"\s+-\s+{TOKEN}$")
PART_NO_RE = re.compile(rf"\bpart\s*(?:no|number|#)\.?\s*:?\s*{TOKEN}", re.I)
PAREN_PART_RE = re.compile(rf"\({TOKEN}\)$")
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s?(TB|GB)\b(?!\s*/\s*s|ps)")
DRIVE_RE = re.compile(r"hard (?:disk )?drive|\bhdd\b|\bssd\b|solid state|\bnvme\b|disk drive", re.I)
MEMORY_RE = re.compile(r"\bmemory\b|\bdimm\b|\brdimm\b|\blrdimm\b|\bsdram\b|\bddr\d", re.I)
# Whole computers ("Core i5 ... 8 GB RAM - 256 SSD") are not drives / memory
COMPUTER_RE = re.compile(r"\bGB\s+RAM\b|\b(?:xeon|core i\d|core ultra|ryzen|epyc|celeron|"
                         r"pentium|athlon|opteron|a-series)\b", re.I)
CONDITIONS = [(re.compile(r"\brefurbished\b|\brefurb\b|\brenewed\b", re.I), "Refurbished"),
              (re.compile(r"\bused\b|\bpre-?owned\b", re.I), "Used"),
              (re.compile(r"\bbrand new\b|\bnew\b", re.I), "New")]


def get(url):
    """GET with http's 429 / 5xx retries, plus a few tries for a download the server stalls or
    cuts off part-way (the sitemap pages are built on request and sometimes hang; requests does
    not retry those). The last failure raises."""
    for attempt in range(1, TRIES + 1):
        try:
            r = http.session().get(url, timeout=180)
            r.raise_for_status()
            return r.text
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            if attempt == TRIES:
                raise
            print(f"WARN {url[:90]}: {e.__class__.__name__}, retrying in {RETRY_PAUSE}s", flush=True)
            time.sleep(RETRY_PAUSE)


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def sitemap_count():
    """Unique product URLs across the store's product sitemap pages (its catalogue size)."""
    index = get(f"{BASE}/xmlsitemap.php")
    pages = sorted(set(re.findall(r"type=products&(?:amp;)?page=(\d+)", index)), key=int)
    if not pages:
        raise RuntimeError("no product pages in the sitemap index")
    urls = set()
    for page in pages:
        urls.update(re.findall(r"<loc>([^<]+)</loc>",
                               get(f"{BASE}/xmlsitemap.php?type=products&page={page}")))
    return len(urls)


def money(card, *attrs):
    for attr in attrs:
        el = card.select_one(f"[{attr}]")
        m = PRICE_RE.search(el.get_text() if el else "")
        if m:
            return float(m.group(0).replace(",", ""))
    return None


def parse_compare(html):
    """[{fields}] for every product column of a comparison page."""
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("tr.compareTable-row")
    cards = rows[0].select("th.compareTable-product") if rows else []
    if not cards:
        return []
    actions = rows[1].select("td.compareTable-action") if len(rows) > 1 else []
    availability = []
    for row in rows:
        head = row.select_one("th.compareTable-heading")
        if head and clean(head.get_text()).lower() == "availability":
            availability = [clean(td.get_text(" ")) for td in row.select("td")]

    def link_ids(card):
        a = card.select_one("a.compareTable-removeProduct")
        tail = (a.get("href") or "").split("/compare/", 1)[-1] if a else ""
        return {x for x in tail.strip("/").split("/") if x.isdigit()}

    links = [link_ids(c) for c in cards]
    on_page = set().union(*links)
    products = []
    for i, card in enumerate(cards):
        mine = on_page - links[i]
        if len(mine) == 1:
            pid = mine.pop()
        else:                                   # single-product page: id from the image path
            m = re.search(r"/products/(\d+)/", str(card))
            pid = m.group(1) if m else None
        link = card.select_one(".card-title a")
        if not pid or not link:
            print("WARN skipped a comparison column without product id / link", flush=True)
            continue
        brand = card.select_one("p.card-text a")
        price = money(card, "data-product-price-without-tax")
        compare = money(card, "data-product-rrp-without-tax", "data-product-rrp-price-without-tax",
                        "data-product-non-sale-price-without-tax")
        button = clean(actions[i].get_text(" ")).lower() if i < len(actions) else ""
        stock_text = availability[i] if i < len(availability) else ""
        products.append({
            "id": pid,
            "title": clean(link.get_text()),
            "url": clean(link.get("href")),
            "brand": clean(brand.get_text()) if brand else "",
            "price": price or None,                     # $0 = no real price
            "compare": compare if compare and price and compare > price else None,
            "available": button in IN_STOCK_BUTTONS and not OUT_OF_STOCK_RE.search(stock_text),
        })
    return products


def fetch_chunk(start):
    ids = "/".join(str(i) for i in range(start, start + CHUNK))
    return parse_compare(get(f"{BASE}/compare/{ids}"))


def populated(start, n=60):
    ids = "/".join(str(i) for i in range(start, start + n))
    return len(parse_compare(get(f"{BASE}/compare/{ids}"))) > 0


def find_ceiling(seed=SEED, step=5000, clear=3):
    """Highest id to scan: probe upwards from a populated seed until `clear` empty windows in a
    row (wide enough to step over any gap in the id space); the catalogue can grow between runs."""
    hi, k, empty = seed, 1, 0
    while empty < clear:
        start = seed + k * step
        if populated(start):
            hi, empty = start, 0
        else:
            empty += 1
        k += 1
    return hi + step


def part_number(title):
    for rx in (LEAD_PART_RE, TAIL_PART_RE, PART_NO_RE, PAREN_PART_RE):
        m = rx.search(title)
        if m and len(m.group(1)) >= 4:
            return m.group(1).rstrip("-.,")
    return ""


def build_row(p, scraped_at):
    title = p["title"]
    part = (DRIVE_RE.search(title) or MEMORY_RE.search(title)) and not COMPUTER_RE.search(title)
    cap = CAPACITY_RE.search(title) if part else None
    tb = None
    if cap and DRIVE_RE.search(title):
        tb = float(cap.group(1)) if cap.group(2) == "TB" else float(cap.group(1)) / 1000
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": str(p["id"]),
        "part_number": part_number(title),
        "title": title,
        "brand": p["brand"],
        "condition": next((c for rx, c in CONDITIONS if rx.search(title)), ""),
        "capacity": f"{cap.group(1)}{cap.group(2)}" if cap else "",
        "price": p["price"],
        "compare_at_price": p["compare"],
        "price_per_tb": round(p["price"] / tb, 2) if p["price"] and tb else None,
        "available": p["available"],
        "stock_qty": None,                 # not shown on the comparison page
        "url": p["url"],
    })


def scrape(limit, scraped_at):
    listed = None if limit else sitemap_count()     # the 21 sitemap pages are slow; tests skip them
    print(f"sitemap lists {listed} products", flush=True)
    if limit:
        lo, hi = SEED, None                # test run: from a populated id until limit is reached
    else:
        lo, hi = START, find_ceiling()
        print(f"scanning ids {lo}..{hi}", flush=True)
    by_id, start, batches = {}, lo, 0
    with ThreadPoolExecutor(WORKERS) as pool:
        while True:
            starts = [start + k * CHUNK for k in range(WORKERS * 2)]
            if hi is not None:
                starts = [s for s in starts if s <= hi]
            if not starts:
                break
            for products in pool.map(fetch_chunk, starts):
                for p in products:
                    by_id[p["id"]] = p
            start = starts[-1] + CHUNK
            batches += 1
            if batches % 20 == 0 or (hi is not None and start > hi):
                print(f"ids up to {start - 1}: {len(by_id)} products", flush=True)
            if limit and len(by_id) >= limit:
                break
            time.sleep(DELAY)
    items = list(by_id.values())[:limit] if limit else list(by_id.values())
    return [build_row(p, scraped_at) for p in items], listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
