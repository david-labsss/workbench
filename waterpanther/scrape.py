"""Scrape waterpanther.com (Shopify, ~1.4k products): every run's full snapshot appended to Postgres.

/products.json, 250 per page: title, SKU, price, compare-at price, in stock yes/no. Specs come
from the product tags, e.g. "Interface_SATA 6Gb/s", "2-Storage Capacity^480GB". Water Panther
sells its own brand, so there is no OEM part number. The store doesn't publish stock counts
(quantityAvailable is access-denied), so stock_qty stays blank.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
import time

from common import http, runner, schema

SITE = "waterpanther"
BASE = "https://www.waterpanther.com"
PAGE_DELAY = 1.0

# Tag name -> column. Tags look like "Name_Value" or "<n>-Name^Value"; "Name_Value" tags are
# cleaner, so they win. Capacity comes from the title first: some tags are wrong.
TAG_COLUMNS = {
    "device type": "category",
    "item condition": "condition",
    "storage capacity": "capacity",
    "interface": "interface",
    "form factor": "form_factor",
    "spindle speed": "spindle_speed", "rpm": "spindle_speed",
    "model series": "series",
}
TAG_RE = re.compile(r"^(?:\d+-)?([^_^]+)[_^](.+)$")
CAPACITY_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(TB|GB)\b", re.I)


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


def tag_specs(tags):
    specs, other = {}, {}
    for tag in sorted(tags, key=lambda t: "^" in t):
        m = TAG_RE.match(tag)
        if not m:
            continue
        name, value = m.group(1).strip(), m.group(2).strip()
        column = TAG_COLUMNS.get(name.lower())
        if column:
            specs.setdefault(column, value)
        else:
            other.setdefault(name, value)
    return specs, "; ".join(f"{k}: {v}" for k, v in other.items())


def capacity_tb(text):
    m = CAPACITY_RE.search(text or "")
    if not m:
        return None
    size = float(m.group(1))
    return size if m.group(2).upper() == "TB" else size / 1000


def build_rows(product, scraped_at):
    specs, other = tag_specs(product.get("tags", []))
    title = product.get("title", "")
    cap = CAPACITY_RE.search(title)
    if cap:
        specs["capacity"] = cap.group(1) + cap.group(2).upper()
    if not specs.get("condition"):
        specs["condition"] = "Refurbished" if title.lower().startswith("refurbished") else ""
    variants = product.get("variants", [])
    rows = []
    for v in variants:
        price = to_float(v.get("price"))
        tb = capacity_tb(specs.get("capacity") or title)
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": str(v["id"]),        # unique: one row per variant
            "parent_id": str(product["id"]),
            "variant_id": str(v["id"]),
            "sku": v.get("sku") or "",
            "title": title + ("" if v.get("title") in (None, "Default Title") else f" - {v['title']}"),
            "brand": product.get("vendor", ""),
            "category": product.get("product_type") or specs.get("category", ""),
            "condition": specs.get("condition", ""),
            "series": specs.get("series", ""),
            "capacity": specs.get("capacity", ""),
            "interface": specs.get("interface", ""),
            "form_factor": specs.get("form_factor", ""),
            "spindle_speed": specs.get("spindle_speed", ""),
            "other_specs": other,
            "price": price,
            "compare_at_price": to_float(v.get("compare_at_price")),
            "price_per_tb": round(price / tb, 2) if price and tb else None,
            "available": bool(v.get("available")),
            "stock_qty": None,  # not published by the store
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
