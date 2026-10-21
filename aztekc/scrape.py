"""Scrape aztekcomputers.com (Aztek Computers, custom PHP store): every run's snapshot appended to
Postgres.

The old aztekc.com store moved here. The catalogue is huge (4M+ SKUs the site claims), far too
large to take whole, so this scraper walks a fixed set of the main server and storage leaf
categories. Each category lists 50 products a page at /{slug} (page 1) and /{slug}/{N} (page N),
and every product is rendered twice - a grid card and a list card. The list card
(.wa-list-caption) carries every field cleanly:

  MFG: Intel · MFG. Part: BX8071514100 · SKU: UETFYNWZP6 · UPC: 735858547512 · Condition: New
  price (with an optional <del> "was" price)  ·  h2 > a = title + product URL

The internal SKU is the product id. A product can sit in more than one category, so rows are
de-duplicated by SKU. Because this is a curated slice of the catalogue (not the whole store) and
rows are de-duplicated across categories, there is no single trustworthy "listed" total to gate a
full run on, so the scraper returns listed = None and instead walks each category to its reported
last page and aborts on any page that fails after retries. MAX_PRODUCTS caps a test run.

Availability (real, never guessed): the category listing cards carry NO stock state (every card
shows the same "Add to Cart"), and the visible stock text on a product page is a JS-filled template
default that reads "in stock" even on out-of-stock items, so neither can be trusted. The one honest
source in the static HTML is the product page's Product JSON-LD "availability"
(schema.org/InStock | OutOfStock | ...), which matches the live page exactly. The store's own
quick-view JSON (/ajax/ajax.php) carries the same value but sits under a robots.txt-disallowed path,
so we do NOT use it. Reading JSON-LD means one product-page fetch per product (~190 KB each, ~141k
products) - too heavy to do on every run - so stock is refreshed on a schedule and carried forward
in between:

  * FULL pass (AZ_FULL=1, the weekly cron): fetch every product page, read the real availability.
  * default pass (the daily n8n trigger): walk the listings for price only, and carry each product's
    availability forward from the last saved full run (a real value last observed <=7 days ago - not
    fabricated). A product never yet seen by a FULL pass stays unknown (available = None), never a
    guessed "in stock".

Stock count is left blank: the JSON-LD exposes only in/out, not a quantity.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import math
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import db, http, runner, schema

SITE = "aztekc"
BASE = "https://www.aztekcomputers.com"
WORKERS = int(os.getenv("AZ_WORKERS") or 12)   # threads for the FULL availability pass
PER_PAGE = 50
DELAY = 0.3
PRICE_RE = re.compile(r"[\d,]+(?:\.\d+)?")
COUNT_RE = re.compile(r"of\s+([\d,]+)\s+item", re.I)
# The product page's single Product JSON-LD "availability" (a schema.org URL). Confirmed one per
# page, so a direct search is safe.
AVAIL_RE = re.compile(r'"availability"\s*:\s*"?([^",}]+)', re.I)
AVAIL_IN = ("instock", "limitedavailability", "instoreonly", "onlineonly")
AVAIL_OUT = ("outofstock", "soldout", "discontinued", "preorder", "backorder", "presale")

# The main server / storage leaf categories (slugs under aztekcomputers.com/{slug}).
CATEGORIES = [
    "computers-servers-server-components-server-cpus-processors",
    "computers-servers-server-components-server-memory-ram",
    "computers-servers-server-components-disk-controllers-raid-cards",
    "computers-storage-hard-drives-internal-hard-disk-drives",
    "computers-storage-hard-drives-solid-state-drives",
    "computers-networking",
]


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def money(text):
    m = PRICE_RE.search(text or "")
    return float(m.group(0).replace(",", "")) if m else None


def absolute(u):
    """Absolute https URL for a possibly relative/protocol-relative one, else None."""
    u = (u or "").strip()
    if not u or u.startswith("data:"):
        return None
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return BASE + u
    return u


def get(url):
    r = http.session().get(url, timeout=60)
    r.raise_for_status()
    return r.text


def avail_from_token(token):
    """True / False / None from a schema.org availability value (e.g. ".../OutOfStock")."""
    t = (token or "").rsplit("/", 1)[-1].strip().lower()
    if not t:
        return None
    if any(t == k or k in t for k in AVAIL_IN):
        return True
    if any(t == k or k in t for k in AVAIL_OUT):
        return False
    return None


def fetch_stock(url):
    """Real availability for one product from its page's Product JSON-LD, or None when it can't be
    read / is unrecognised (so the row stays an honest "unknown", never a guess)."""
    for attempt in range(3):
        try:
            m = AVAIL_RE.search(get(url))
            return avail_from_token(m.group(1)) if m else None
        except Exception:                                   # noqa: BLE001
            time.sleep(0.5 * (attempt + 1))
    return None


def enrich_real_stock(rows):
    """FULL pass: set `available` on each (row, url) from the product page's real JSON-LD
    availability. Threaded; an unreadable page leaves the row unknown (None)."""
    ai = schema.PRODUCT_COLUMNS.index("available")

    def work(item):
        row, url = item
        if not url:
            return
        time.sleep(random.uniform(0, 0.08))                 # light jitter, polite
        row[ai] = fetch_stock(url)

    with ThreadPoolExecutor(WORKERS) as pool:
        for done, _ in enumerate(pool.map(work, rows), 1):
            if done % 1000 == 0 or done == len(rows):
                print(f"  availability: {done}/{len(rows)}", flush=True)


def carry_forward_stock(rows):
    """Default (daily) pass: no product-page fetches. Reuse each product's real availability from
    the last saved full run (a value last observed <=7 days ago). A product the full pass has
    never seen stays unknown (None)."""
    pi = schema.PRODUCT_COLUMNS.index("product_id")
    ai = schema.PRODUCT_COLUMNS.index("available")
    prev = db.previous_run(SITE, "product_id", ["product_id", "available"])

    def parse(v):
        v = (v or "").strip().lower()
        return True if v in ("true", "t", "1") else False if v in ("false", "f", "0") else None

    carried = 0
    for row, _ in rows:
        state = parse((prev.get(row[pi]) or {}).get("available"))
        row[ai] = state
        if state is not None:
            carried += 1
    print(f"  availability carried forward from last full run: {carried}/{len(rows)}", flush=True)


def page_url(slug, page):
    return f"{BASE}/{slug}" if page == 1 else f"{BASE}/{slug}/{page}"


def category_name(html, slug):
    """The category's human name from the page's BreadcrumbList JSON-LD, or a cleaned slug."""
    import json
    for block in re.findall(r'<script type="application/ld\+json">(.*?)</script>', html, re.S):
        try:
            data = json.loads(block)
        except ValueError:
            continue
        if isinstance(data, dict) and data.get("@type") == "BreadcrumbList":
            items = data.get("itemListElement") or []
            if items:
                return clean(items[-1].get("name"))
    return slug.split("-")[-1].title()


def parse_cards(html, scraped_at, category=""):
    """[(product_row, url)] for the list cards on one category page. `url` is the product page,
    used later to read real availability (enrich_real_stock)."""
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for card in soup.select(".wa-list-caption"):
        a = card.select_one("h2 a")
        if not a:
            continue
        price_el = card.select_one(".price")
        compare = None
        if price_el:
            del_el = price_el.select_one("del")      # a visible "was" price
            if del_el:
                compare = money(del_el.get_text())
                del_el.extract()
            price = money(price_el.get_text())
        else:
            price = None
        fields = {}
        for span in card.select("p span"):
            txt = clean(span.get_text())
            if ":" in txt:
                k, v = txt.split(":", 1)
                fields[k.strip().lower()] = v.strip()
        sku = fields.get("sku", "")
        url = absolute(a.get("href", ""))
        pid = sku or (url.rstrip("/").rsplit("/", 1)[-1] if url else "")
        if not pid:
            continue
        # The product thumbnail sits in the same .wa-products wrapper as this list caption.
        wrapper = card.find_parent(class_="wa-products")
        img = wrapper.find("img") if wrapper else None
        image_url = absolute(img.get("src")) if img else None
        if image_url and "no-image" in image_url.lower():
            image_url = None             # the store's "no image" placeholder, not a real photo
        row = schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": pid,
            "sku": sku,
            "part_number": fields.get("mfg. part", ""),
            "title": clean(a.get("title") or a.get_text()),
            "brand": fields.get("mfg", ""),
            "category": category,
            "condition": fields.get("condition", ""),
            "other_specs": f"UPC: {fields['upc']}" if fields.get("upc") else "",
            "price": price,
            "compare_at_price": compare if compare and price and compare > price else None,
            # Unknown until a real source sets it: the FULL pass reads the product page's JSON-LD,
            # the daily pass carries the last full run's value forward. The listing itself shows no
            # stock state, so we never guess one from the price.
            "available": None,
            "image_url": image_url,
            # description lives only on the product page; these category listings (all the daily
            # pass fetches) carry none, and this is a huge catalogue, so it is left blank.
            "description": None,
            "url": url or "",
        })
        rows.append((row, url))
    return rows


def scrape_category(slug, scraped_at, limit, seen, rows):
    first = get(page_url(slug, 1))
    m = COUNT_RE.search(re.sub(r"<[^>]+>", " ", first))
    total = int(m.group(1).replace(",", "")) if m else None
    last_page = math.ceil(total / PER_PAGE) if total else None
    category = category_name(first, slug)
    print(f"[{slug}] {total} items ({last_page} pages) as '{category}'", flush=True)

    def add(page_rows):
        for item in page_rows:                           # item = (row, url)
            pid = item[0][schema.PRODUCT_COLUMNS.index("product_id")]
            if pid not in seen:
                seen.add(pid)
                rows.append(item)

    add(parse_cards(first, scraped_at, category))
    page = 2
    while not (limit and len(rows) >= limit):
        if last_page and page > last_page:
            break
        try:
            page_rows = parse_cards(get(page_url(slug, page)), scraped_at, category)
        except Exception as e:                       # noqa: BLE001
            sys.exit(f"[{slug}] page {page} failed: {e}")
        if not page_rows:
            break
        add(page_rows)
        page += 1
        time.sleep(DELAY)


def scrape(limit, scraped_at):
    rows, seen = [], set()
    for slug in CATEGORIES:
        if limit and len(rows) >= limit:
            break
        scrape_category(slug, scraped_at, limit, seen, rows)
        print(f"  running total: {len(rows)} unique products", flush=True)
    rows = rows[:limit] if limit else rows

    # Real availability. FULL pass (weekly, or a forced test) reads it from each product page;
    # the daily pass carries the last full run's real value forward (no per-product fetches).
    if os.getenv("AZ_FULL") == "1":
        print(f"FULL pass: reading real availability from {len(rows)} product pages", flush=True)
        enrich_real_stock(rows)
    else:
        carry_forward_stock(rows)

    # No reliable global count for a curated, de-duplicated slice (see module docstring).
    return [item[0] for item in rows], None


if __name__ == "__main__":
    runner.run(SITE, scrape)
