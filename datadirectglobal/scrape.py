"""Scrape datadirectglobal.com (Shopify, ~1,160 products): every run's full snapshot appended to Postgres.

Data Direct Global is a Hong Kong / China networking reseller (Fortinet, Meraki, Aruba, Cisco,
H3C licences and hardware) selling in USD - not a US store.

/products.json, 250 per page, paged until an empty page. One row per variant (product_id =
variant id, parent_id = product id):
  price             the variant price; a $0 price is saved empty (the page shows no price then),
                    never as 0. About 3% of products carry a "hide_price" tag, but the store's
                    quote app has hide-price switched off and those pages still show the price
                    and an Add to cart button, so their price is kept.
  compare_at_price  only when it is above the price (many products repeat the price there)
  available         the in-stock flag (the store marks everything available)
  sku               the manufacturer part number; part_number = sku
  brand             vendor; category = product_type
  condition         blank: the store has no per-product condition (only a site-wide
                    "Brand New & Original" banner)
  capacity          drives / memory only, from the title (upper-case GB/TB: "8Gb" is a link speed)
The public endpoint carries no stock count, so stock_qty stays blank.

The product count comes from the store's product sitemaps (every file counted: they don't hold
exactly 1,000 each) as a completeness check, so a truncated crawl is never saved as the full
catalogue.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
import sys
import time

from common import http, runner, schema

SITE = "datadirectglobal"
BASE = "https://www.datadirectglobal.com"
PAGE_DELAY = 1.0

CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b")   # upper-case only: "8Gb" is a link speed
SIZED_RE = re.compile(r"HDD|SSD|Drive|Memory|RAM", re.I)      # product types where capacity applies


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


def listed_total():
    """Product count from the product sitemaps, a completeness check only (None if unreadable)."""
    try:
        s = http.session()
        index = s.get(f"{BASE}/sitemap.xml", timeout=30)
        index.raise_for_status()
        subs = [u.replace("&amp;", "&")
                for u in re.findall(r"<loc>([^<]*sitemap_products_[^<]*)</loc>", index.text)]
        total = 0
        for url in subs:
            r = s.get(url, timeout=30)
            r.raise_for_status()
            total += len(re.findall(r"<loc>[^<]*/products/[^<]*</loc>", r.text))
        return total or None
    except Exception as e:  # noqa: BLE001 - only a sanity check
        print(f"WARN couldn't read the sitemap product count: {e}", flush=True)
        return None


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def build_rows(product, scraped_at):
    title = re.sub(r"\s+", " ", product.get("title") or "").strip()
    category = product.get("product_type") or ""
    cap = CAPACITY_RE.search(title) if SIZED_RE.search(category) else None
    variants = product.get("variants", [])
    rows = []
    for v in variants:
        sku = (v.get("sku") or "").strip()
        price = to_float(v.get("price")) or None        # $0 = no price shown -> empty
        compare = to_float(v.get("compare_at_price"))
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": str(v["id"]),        # unique: one row per variant
            "parent_id": str(product["id"]),
            "variant_id": str(v["id"]),
            "sku": sku,
            "part_number": sku,
            "title": title + ("" if v.get("title") in (None, "Default Title") else f" - {v['title']}"),
            "brand": product.get("vendor", ""),
            "category": category,
            "capacity": (cap.group(1) + cap.group(2)) if cap else "",
            "price": price,
            "compare_at_price": compare if price and compare and compare > price else None,
            "available": bool(v.get("available")),
            "stock_qty": None,  # not published by the public /products.json endpoint
            "url": f"{BASE}/products/{product['handle']}"
            + (f"?variant={v['id']}" if len(variants) > 1 else ""),
        }))
    return rows


def scrape(limit, scraped_at):
    catalog = fetch_catalog(limit)
    rows = [row for product in catalog for row in build_rows(product, scraped_at)]
    listed = None if limit else listed_total()
    if not limit and listed and len(catalog) < 0.98 * listed:
        sys.exit(f"Only fetched {len(catalog)} of {listed} products ({len(catalog) / listed:.0%}); "
                 "not saving a partial snapshot")
    print(f"fetched {len(catalog)} products -> {len(rows)} variant rows (sitemap lists {listed})",
          flush=True)
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
