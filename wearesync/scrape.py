"""Scrape shop.wearesync.co.uk (Magento 2, GraphQL disabled): snapshot appended to Postgres.

Sync is a UK Apple/IT reseller; prices are in GBP, shown INCLUDING VAT, and the displayed number
is stored as-is in the price column.

Why HTML and not GraphQL: this store's Magento has the GraphQL endpoint switched off (POST
/graphql returns a Magento 404), there is no reachable sitemap (/sitemap.xml 404), and the REST
catalogue API needs auth. So the data comes from the on-site category listing pages, the fallback
the brief calls for. The storefront theme renders most landing pages (e.g. /ipad.html) with a
client-side product widget, but the catch-all category /shop-all.html DOES render the standard
Magento product grid server-side, with product id, name, URL and price (final + base) in the HTML.
This scraper pages /shop-all.html and parses that grid.

Each grid item gives: product_id (Magento entity id from data-product-id), title, URL, price
(finalPrice, incl VAT) and compare_at_price (basePrice when higher). sku / part_number are derived
from the product URL slug (the grid carries no separate SKU field); brand and category are not in
the grid and are left blank. Availability defaults to in-stock unless the item shows an
out-of-stock / "notify me" marker.

====================================================================================================
COVERAGE CAVEAT (read before relying on a full run). /shop-all.html reports ~282k products at 36
per page (~7.8k pages), and the store's deep pagination is NOT reliable: pages past roughly p=2000
return inconsistent / empty results, and there is no sitemap or API to enumerate the rest. So a
single HTTP run cannot be guaranteed to reach the whole catalogue. The scraper therefore pages
until the listing stops yielding new products (or MAX_PAGES), dedupes by product id, and reports
`listed = None` so common/runner.py does not treat the partial set as a failed full snapshot. For
guaranteed-complete coverage this store needs a product feed, a working sitemap, or a headless
renderer - flagged for follow-up. (A DRY_RUN / MAX_PRODUCTS test run exercises the parser fine.)
====================================================================================================

Env vars:
  DATABASE_URL   Postgres URL for the scraper_writer role
  MAX_PRODUCTS   optional, stop after about N products (test runs)
  MAX_PAGES      optional, hard cap on listing pages to fetch (default 5000)
  DRY_RUN        optional, "1" = print sample rows, don't write anything
Run steps and changes: common/runner.py. Columns: common/schema.py.
"""
import html
import os
import re
import time

from common import http, runner, schema

SITE = "wearesync"
BASE = "https://shop.wearesync.co.uk"
LISTING = f"{BASE}/shop-all.html"
PAGE_LIMIT = 36          # the largest product_list_limit the store allows (12 / 24 / 36)
DELAY = 0.5              # polite pause between page requests
STALE_PAGES = 4          # stop after this many consecutive pages with no new products
DEFAULT_MAX_PAGES = 5000

# One grid item: everything from "product-item-details" to the next one (or list end).
ITEM_RE = re.compile(r"product-item-details(.*?)(?=product-item-details|</ol>|</body>)", re.S)
LINK_RE = re.compile(r'class="product-item-link"\s+href="([^"]+)"\s*>\s*(.*?)\s*</a>', re.S)
PID_RE = re.compile(r'data-product-id="(\d+)"')
FINAL_RE = re.compile(r'data-price-amount="([\d.]+)"\s*data-price-type="finalPrice"')
BASE_RE = re.compile(r'data-price-amount="([\d.]+)"\s*data-price-type="basePrice"')
OOS_RE = re.compile(r"out of stock|out-of-stock|sold out|unavailable|notify me when", re.I)
TOTAL_RE = re.compile(r"of\s+<[^>]*>?\s*([\d,]+)")

CONDITIONS = [
    ("refurbished", "Refurbished"), ("recertified", "Recertified"), ("renewed", "Renewed"),
    ("open box", "Open Box"), ("open-box", "Open Box"), ("pre-owned", "Used"),
    ("preowned", "Used"), ("ex-demo", "Used"), ("used", "Used"),
]


def fetch(page):
    url = f"{LISTING}?product_list_limit={PAGE_LIMIT}&p={page}"
    r = http.session().get(url, timeout=60)
    r.raise_for_status()
    return r.text


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def money(v):
    f = to_float(v)
    return round(f, 2) if f else None  # store sends float noise (x.xxx001); 2dp = displayed GBP


def sku_from_url(url):
    """The store's identifier from the product URL: the .html slug, or the /s/<sku>/ segment of a
    non-rewritten catalog URL."""
    m = re.search(r"/s/([^/]+)/", url)
    if m:
        return m.group(1)
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    return re.sub(r"\.html?$", "", slug)


def condition_of(title):
    low = (title or "").lower()
    for needle, label in CONDITIONS:
        if needle in low:
            return label
    return ""


def parse_page(text, seen, scraped_at):
    """Add every grid item on one page to `seen`. Returns the number of NEW products added."""
    added = 0
    for chunk in ITEM_RE.findall(text):
        link = LINK_RE.search(chunk)
        pid = PID_RE.search(chunk)
        if not link or not pid:
            continue
        product_id = pid.group(1)
        if product_id in seen:
            continue
        url = html.unescape(link.group(1)).strip()
        title = re.sub(r"\s+", " ", html.unescape(link.group(2))).strip()
        final = money(FINAL_RE.search(chunk).group(1)) if FINAL_RE.search(chunk) else None
        base = money(BASE_RE.search(chunk).group(1)) if BASE_RE.search(chunk) else None
        sku = sku_from_url(url)
        seen[product_id] = schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": product_id,
            "variant_id": product_id,
            "sku": sku,
            "part_number": sku,          # derived from the URL slug (grid has no SKU field)
            "model_number": sku,
            "title": title,
            "brand": "",                 # not present in the grid
            "category": "",              # not present in the shop-all grid
            "condition": condition_of(title),
            "price": final,
            "compare_at_price": base if (final and base and base > final) else None,
            "available": not bool(OOS_RE.search(chunk)),
            "url": url,
        })
        added += 1
    return added


def scrape(limit, scraped_at):
    max_pages = int(os.getenv("MAX_PAGES") or DEFAULT_MAX_PAGES)
    seen, stale = {}, 0
    first = fetch(1)
    m = TOTAL_RE.search(first)
    print(f"shop-all lists ~{m.group(1) if m else '?'} products "
          f"(deep paging is unreliable past ~p=2000 - see module docstring)", flush=True)
    text = first
    for page in range(1, max_pages + 1):
        if page > 1:
            text = fetch(page)
        added = parse_page(text, seen, scraped_at)
        if page % 25 == 0 or page == 1:
            print(f"  page {page}: +{added} ({len(seen)} unique total)", flush=True)
        if limit and len(seen) >= limit:
            break
        stale = stale + 1 if added == 0 else 0
        if stale >= STALE_PAGES:
            print(f"  stopped at page {page}: {STALE_PAGES} pages with no new products", flush=True)
            break
        time.sleep(DELAY)
    rows = list(seen.values())
    # listed = None on purpose: the full catalogue is not reliably reachable over HTTP, so the
    # completeness gate in common/runner.py must not treat this snapshot as a failed full run.
    return (rows[:limit] if limit else rows), None


if __name__ == "__main__":
    runner.run(SITE, scrape)
