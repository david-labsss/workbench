"""Scrape oempcworld.com (Shopify, ~3.2k products): every run's full snapshot appended to Postgres.

/products.json, 250 products per page, paged until an empty page: per variant the title, SKU,
price, compare-at price (old_price) and in-stock yes/no. The public endpoint carries no stock
count, so stock_qty stays blank. Run steps, changes and env vars: common/runner.py. Columns:
common/schema.py.
"""
import time

from common import http, runner, schema

SITE = "oempcworld"
BASE = "https://oempcworld.com"
PAGE_DELAY = 1.0


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


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def build_rows(product, scraped_at):
    title = product.get("title", "")
    variants = product.get("variants", [])
    rows = []
    for v in variants:
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": str(v["id"]),        # unique: one row per variant
            "parent_id": str(product["id"]),
            "variant_id": str(v["id"]),
            "sku": v.get("sku") or "",
            "title": title + ("" if v.get("title") in (None, "Default Title") else f" - {v['title']}"),
            "brand": product.get("vendor", ""),
            "category": product.get("product_type", ""),
            "price": to_float(v.get("price")) or None,
            "compare_at_price": to_float(v.get("compare_at_price")),
            "available": bool(v.get("available")),
            "stock_qty": None,  # not published by the public /products.json endpoint
            "image_url": (product.get("images") or [{}])[0].get("src") or None,
            "description": schema.plain_text(product.get("body_html")),
            "url": f"{BASE}/products/{product['handle']}"
            + (f"?variant={v['id']}" if len(variants) > 1 else ""),
        }))
    return rows


def scrape(limit, scraped_at):
    catalog = fetch_catalog(limit)
    rows = [row for product in catalog for row in build_rows(product, scraped_at)]
    # /products.json has no total; it is the whole catalogue when the pages run out
    return rows, None


if __name__ == "__main__":
    runner.run(SITE, scrape)
