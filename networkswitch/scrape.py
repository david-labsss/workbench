"""Scrape network-switch.com (Shopify, ~17k products): every run's full snapshot appended to Postgres.

Network-Switch / NSComm sells networking switches. Prices are in HKD (Hong Kong dollar, the
store's base currency, confirmed via /cart.js); the price NUMBER is saved as-is into the
standard price column.

/products.json, 250 per page, paged until the list runs out. The catalogue is under the ~25k
ceiling /products.json pages to, so walking every page reaches the whole catalogue. Standard
Shopify mapping: product_id = variant id, parent_id = product id, sku, title, price,
compare_at_price, available, brand = vendor, category = product_type, url. Condition and
capacity are read from the title / variant title when present. The store doesn't publish stock
counts in /products.json, so stock_qty stays blank.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
import time

from common import http, runner, schema

SITE = "networkswitch"
BASE = "https://network-switch.com"
CURRENCY = "HKD"          # documented only; prices are saved as the raw number
PAGE_DELAY = 0.5

CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB|MB)\b", re.I)
CONDITIONS = [
    (re.compile(r"\brefurbished\b", re.I), "Refurbished"),
    (re.compile(r"\brecertified\b", re.I), "Recertified"),
    (re.compile(r"\brenewed\b", re.I), "Renewed"),
    (re.compile(r"\bopen\s*box\b", re.I), "Open Box"),
    (re.compile(r"\bused\b", re.I), "Used"),
    (re.compile(r"\bnew\b", re.I), "New"),
]
# A variant title that is a condition grade on its own (not a configuration like "16GB / Silver")
GRADE_RE = re.compile(
    r"^(?:brand\s+new|as\s+new|like\s+new|open\s*box|excellent|very\s+good|good|fair|"
    r"acceptable|mint|pristine|refurbished|recertified|renewed|used|new|"
    r"grade\s*[a-c]\b|[a-c][-\s]?grade)", re.I)


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def condition_of(title, variant_title):
    """Use the variant title as the condition only when it is a grade (e.g. 'Excellent',
    'Refurbished'); configuration variant titles (RAM/colour/size) are ignored. Otherwise read a
    condition keyword from the product title."""
    vt = (variant_title or "").strip()
    if vt and vt != "Default Title" and GRADE_RE.match(vt):
        return vt
    for rx, label in CONDITIONS:
        if rx.search(title or ""):
            return label
    return ""


def fetch_catalog(limit):
    """Every product, deduped by product id. A failed page raises: it must never look like the
    end of the list."""
    by_id, page = {}, 1
    while True:
        r = http.session().get(f"{BASE}/products.json", params={"limit": 250, "page": page},
                               timeout=30)
        r.raise_for_status()
        batch = r.json().get("products", [])
        if not batch:
            return list(by_id.values())
        for p in batch:
            by_id.setdefault(p["id"], p)
        print(f"page {page}: {len(batch)} products (total {len(by_id)})", flush=True)
        if limit and len(by_id) >= limit:
            return list(by_id.values())[:limit]
        page += 1
        time.sleep(PAGE_DELAY)


def build_rows(product, scraped_at):
    title = (product.get("title") or "").strip()
    variants = product.get("variants", [])
    cap = CAPACITY_RE.search(title)
    capacity = (cap.group(1) + cap.group(2).upper()) if cap else ""
    rows = []
    for v in variants:
        price = to_float(v.get("price")) or None
        compare = to_float(v.get("compare_at_price"))
        vtitle = v.get("title")
        sku = v.get("sku") or ""
        discount = (round((compare - price) / compare * 100, 1)
                    if price and compare and compare > price else None)
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
            "category": product.get("product_type", ""),
            "condition": condition_of(title, vtitle),
            "capacity": capacity,
            "price": price,
            "compare_at_price": compare,
            "discount_pct": discount,
            "available": bool(v.get("available")),
            "stock_qty": None,                 # not published in /products.json
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
