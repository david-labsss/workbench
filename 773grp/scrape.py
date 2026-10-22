"""Scrape 773grp.com (Shopify, electronic-components distributor): every run's full snapshot
appended to Postgres.

Fetch is Shopify's public /products.json (?limit=250&page=N) paged until an empty page; row
mapping follows atechmemory - one row per variant (product_id = variant id, parent_id = product
id), price the variant price in USD ("0.00" = quote-only -> empty, never 0), compare_at_price ->
old_price, availability = variant available, brand = vendor, category = product_type, part number
the SKU, condition and capacity read from the title, image from images[0].src and description from
body_html.

CAUTION - catalogue size: this store is a passive-components aggregator with ~8.4 MILLION products
(its sitemap index lists 8,439 product sub-sitemaps of ~1,000 each, with monotonic product-id
ranges). Shopify hard-caps paginated /products.json at 25,000 products (page 101 returns HTTP 400,
confirmed), so /products.json can reach only the first ~25k (~0.3%) of the catalogue. listed_total
is read from the product sitemap as a completeness check, so the runner (common/runner.py,
COMPLETE = 0.98) will REFUSE to save this partial as the full catalogue - by design, so bad data is
never written. Covering the whole catalogue needs the Storefront GraphQL windowed crawl
(atechmemory/altatechnologies style), which is an ~8.4M-product daily job and needs a scope
decision before it is built. See the run report.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.

Env vars:
  DATABASE_URL   Postgres URL for the scraper_writer role
  MAX_PRODUCTS   optional, stop after about N products (test runs; also skips the sitemap count)
  DRY_RUN        optional, "1" = print sample rows, don't write anything
"""
import re
import sys
import time

import requests

from common import http, runner, schema

SITE = "773grp"
BASE = "https://773grp.com"
PAGE = 250           # max products per /products.json page
PAGE_DELAY = 0.5     # polite pause between pages
USER_AGENT = http.USER_AGENT

CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b")   # upper-case only: "8Gb" is a link speed
CONDITIONS = [
    (re.compile(r"\brefurbished\b", re.I), "Refurbished"),
    (re.compile(r"\brecertified\b", re.I), "Recertified"),
    (re.compile(r"\bopen\s+box\b", re.I), "Open Box"),
    (re.compile(r"\bused\b", re.I), "Used"),
    (re.compile(r"\bnew\b", re.I), "New"),
]


# --- fetching ----------------------------------------------------------------
def fetch_catalog(limit):
    """Every product from /products.json. A failed page raises (common/http retries 429/5xx
    first): it must never look like the end of the list. Shopify refuses pages past 25,000
    products with HTTP 400 - that is the platform cap, surfaced loudly, not treated as the end."""
    products, page = [], 1
    while True:
        r = http.session().get(f"{BASE}/products.json", params={"limit": PAGE, "page": page},
                               timeout=30)
        if r.status_code == 400 and page > 1:
            print(f"WARN /products.json refused page {page} (HTTP 400): Shopify's 25,000-product "
                  f"deep-paging cap; {len(products)} products reached", flush=True)
            return products
        r.raise_for_status()
        batch = r.json().get("products", [])
        if not batch:
            return products
        products.extend(batch)
        print(f"page {page}: {len(batch)} products (total {len(products)})", flush=True)
        if limit and len(products) >= limit:
            return products[:limit]
        page += 1
        time.sleep(PAGE_DELAY)


def listed_total():
    """Product count from the sitemap, a completeness check only. The sitemap index links one
    product sub-sitemap per ~1,000 products; counting them (first and last file exactly) gives the
    catalogue size without downloading all of them. Returns None if the layout isn't what we
    expect (then the check is skipped)."""
    try:
        s = requests.Session()
        s.headers["User-Agent"] = USER_AGENT
        index = s.get(f"{BASE}/sitemap.xml", timeout=30).text
        subs = [u.replace("&amp;", "&")
                for u in re.findall(r"<loc>([^<]*sitemap_products_[^<]*)</loc>", index)]
        if not subs:
            return None

        def count(url):
            return len(re.findall(r"<loc>[^<]*/products/[^<]*</loc>", s.get(url, timeout=30).text))

        first = count(subs[0])
        if first != 1000 and len(subs) > 1:
            return None
        last = count(subs[-1]) if len(subs) > 1 else first
        return (len(subs) - 1) * 1000 + last
    except Exception as e:  # noqa: BLE001 - only a sanity check
        print(f"WARN couldn't read the sitemap product count: {e}", flush=True)
        return None


# --- mapping -----------------------------------------------------------------
def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def condition_of(title):
    for rx, label in CONDITIONS:
        if rx.search(title or ""):
            return label
    return ""


def build_rows(product, scraped_at):
    """One row per variant of a product (product_id = variant id, parent_id = product id)."""
    title = re.sub(r"\s+", " ", product.get("title") or "").strip()
    cap = CAPACITY_RE.search(title)
    capacity = (cap.group(1) + cap.group(2)) if cap else ""
    condition = condition_of(title)
    image_url = (product.get("images") or [{}])[0].get("src") or None
    description = schema.plain_text(product.get("body_html"))
    variants = product.get("variants") or []
    multi = len(variants) > 1
    rows = []
    for v in variants:
        vid = str(v["id"])
        sku = (v.get("sku") or "").strip()
        vtitle = v.get("title")
        price = to_float(v.get("price")) or None          # "0.00" = quote-only -> empty
        compare = to_float(v.get("compare_at_price"))
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": vid,        # unique: one row per variant
            "parent_id": str(product["id"]),
            "variant_id": vid,
            "sku": sku,
            "part_number": sku,
            "title": title + ("" if vtitle in (None, "Default Title") else f" - {vtitle}"),
            "brand": product.get("vendor") or "",
            "category": product.get("product_type") or "",
            "condition": condition,
            "capacity": capacity,
            "price": price,
            "compare_at_price": compare if price and compare and compare > price else None,
            "available": bool(v.get("available")),
            "stock_qty": None,   # not published by the public /products.json endpoint
            "image_url": image_url,
            "description": description,
            "url": f"{BASE}/products/{product.get('handle')}" + (f"?variant={vid}" if multi else ""),
        }))
    return rows


def scrape(limit, scraped_at):
    catalog = fetch_catalog(limit)
    rows = [row for product in catalog for row in build_rows(product, scraped_at)]
    listed = None if limit else listed_total()
    # Product-level completeness guard (rows are per variant; products are the right unit here).
    # /products.json caps at 25k, so on this ~8.4M-product store this will trip and stop the run -
    # intended: a 0.3% slice is never saved as the whole catalogue.
    if not limit and listed and len(catalog) < 0.98 * listed:
        sys.exit(f"Only fetched {len(catalog)} of {listed} products ({len(catalog) / listed:.0%}); "
                 "not saving a partial snapshot. /products.json caps at 25,000; this catalogue is "
                 "far larger and needs the Storefront GraphQL windowed crawl (see module docstring).")
    print(f"fetched {len(catalog)} products -> {len(rows)} variant rows (sitemap lists {listed})",
          flush=True)
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
