"""Scrape parts.theserverstore.com (Shopify, ~19,777 products): every run's full snapshot
appended to Postgres.

The catalogue is under Shopify's 25,000-item deep-paging cap, so plain /products.json (250 per
page, paged until it runs out) reaches all of it - modelled on waterpanther. Note the "parts."
subdomain: the store lives at parts.theserverstore.com.

One row per variant: product_id is the variant id, parent_id the product id. Price is the
variant price, old_price its compare-at price, in stock = available, brand = vendor,
category = product_type. Part number is the SKU; condition and capacity are read from the title.
The store doesn't publish stock counts, so stock_qty stays blank.

The product count comes from the store's product sitemap (1,000 products per file) only as a
completeness check, so a truncated crawl is never saved as the full catalogue.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
import time

from common import http, runner, schema

SITE = "theserverstore"
BASE = "https://parts.theserverstore.com"
PAGE_DELAY = 1.0

# Upper-case only: "8Gb" in a title is a link speed (gigabit), not a capacity
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b")
CONDITIONS = [
    (re.compile(r"\brefurbished\b", re.I), "Refurbished"),
    (re.compile(r"\brecertified\b", re.I), "Recertified"),
    (re.compile(r"\bopen\s+box\b", re.I), "Open Box"),
    (re.compile(r"\bused\b", re.I), "Used"),
    (re.compile(r"\bnew\b", re.I), "New"),
]


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


def listed_total():
    """Product count from the sitemap, a completeness check only. Shopify's product sitemaps
    hold 1,000 URLs each except the last, so the total is read from just the index and the last
    file. Returns None if the layout isn't what we expect (then the check is skipped)."""
    try:
        s = http.session()
        index = s.get(f"{BASE}/sitemap.xml", timeout=30).text
        subs = [u.replace("&amp;", "&")
                for u in re.findall(r"<loc>([^<]*sitemap_products_[^<]*)</loc>", index)]
        if not subs:
            return None

        def count(url):
            body = s.get(url, timeout=30).text
            return len(re.findall(r"<loc>[^<]*/products/[^<]*</loc>", body))

        first = count(subs[0])
        if first != 1000 and len(subs) > 1:   # method assumes full 1,000-item files before the last
            return None
        last = count(subs[-1]) if len(subs) > 1 else first
        return (len(subs) - 1) * 1000 + last
    except Exception as e:  # noqa: BLE001 - only a sanity check
        print(f"WARN couldn't read the sitemap product count: {e}", flush=True)
        return None


def fetch_catalog(limit):
    """Every product. A failed page raises: it must never look like the end of the list."""
    products, page = [], 1
    while True:
        r = http.session().get(f"{BASE}/products.json", params={"limit": 250, "page": page},
                               timeout=30)
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


def build_rows(product, scraped_at):
    title = product.get("title", "")
    cap = CAPACITY_RE.search(title)
    condition = condition_of(title)
    variants = product.get("variants", [])
    rows = []
    for v in variants:
        sku = (v.get("sku") or "").strip()
        vtitle = v.get("title")
        compare = to_float(v.get("compare_at_price"))
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": str(v["id"]),        # unique: one row per variant
            "parent_id": str(product["id"]),
            "variant_id": str(v["id"]),
            "sku": sku,
            "part_number": sku,
            "title": title + ("" if vtitle in (None, "Default Title") else f" - {vtitle}"),
            "brand": product.get("vendor", ""),
            "category": product.get("product_type") or "",
            "condition": condition,
            "capacity": (cap.group(1) + cap.group(2)) if cap else "",
            "price": to_float(v.get("price")),
            "compare_at_price": compare if compare else "",  # the store sends 0.00 for "none"
            "available": bool(v.get("available")),
            "stock_qty": None,   # not published by the store
            "url": f"{BASE}/products/{product['handle']}"
            + (f"?variant={v['id']}" if len(variants) > 1 else ""),
        }))
    return rows


def scrape(limit, scraped_at):
    catalog = fetch_catalog(limit)
    rows = [row for product in catalog for row in build_rows(product, scraped_at)]
    # No listed total on test runs (the completeness check only runs on full runs anyway)
    return rows, (None if limit else listed_total())


if __name__ == "__main__":
    runner.run(SITE, scrape)
