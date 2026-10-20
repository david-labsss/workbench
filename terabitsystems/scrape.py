"""Scrape shop.terabitsystems.com (Shopify, ~236 products: Juniper / Arista / Dell networking
and server gear): every run's full snapshot appended to Postgres.

Only the Shopify shop is used (www.terabitsystems.com is an unpriced info catalogue).
/products.json, 250 per page, paged until an empty page. One row per variant (product_id =
variant id, parent_id = product id). Mapping:
  sku            the variant SKU (Terabit's own code, e.g. QSFP100GBASELR4U)
  part_number    the model tag (a tag that looks like a part number, e.g. "QSFP-100GBASE-LR4");
                 when there is none, a part number read from the title ("(Part: X)", else the
                 first hyphenated upper-case code, else the first letters+digits code)
  brand          vendor
  category       the last level of product_type ("...: Networking: Network Switches")
  condition      the title's closing "(Used - Good)" / "(New Open Box)" / "(Refurbished)", else
                 the same words in the tags, else a leading "New" in the title
  compare_at     only when above the price
  available      the variant's available flag; the store publishes no stock count
  image_url      the variant's own image, else the product's first; description = body_html as text
Capacity is read from the title for drives / memory only (upper-case GB/TB).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
import time

from common import http, runner, schema

SITE = "terabitsystems"
BASE = "https://shop.terabitsystems.com"
PAGE_DELAY = 1.0

# condition spelling on the store -> stored value
CONDITIONS = {
    "used - good": "Used - Good", "used-good": "Used - Good", "used good": "Used - Good",
    "new open box": "New Open Box", "open box": "New Open Box",
    "refurbished": "Refurbished", "new": "New", "used": "Used",
}
TAG_PART_RE = re.compile(r"^(?=.*\d)(?=.*[A-Za-z])[A-Za-z0-9][A-Za-z0-9\-/.+]{3,}$")
NUMBER_RE = re.compile(r"^\d+(?:\.\d+)?$")
PART_IN_TITLE_RE = re.compile(r"\(Part:\s*([^)\s]+)\)", re.I)
HYPHEN_PART_RE = re.compile(r"^(?=.*\d)[A-Z0-9]+(?:-[A-Z0-9]+)+(?:/[A-Z0-9]+)?$")
CODE_RE = re.compile(r"^(?=.*\d)[A-Z][A-Za-z0-9]{3,}$")             # TH3030S, R730xd, CF4P0
CPU_MODEL_RE = re.compile(r"^E\d-\d{4}")                # Intel E3/E5 CPU models in server titles
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b")        # upper case only
CAPACITY_TYPES = re.compile(r"memory|drive|storage", re.I)         # product_type levels
CAPACITY_TITLES = re.compile(r"\b(?:SSD|HDD|DIMM|RDIMM|LRDIMM|hard drive)\b", re.I)
NOT_A_DRIVE_RE = re.compile(r"server|poweredge|router|routing|switch|\bno hdd\b", re.I)


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


def image_of(product, variant):
    """Absolute URL of the variant's own image, else the product's first image, else None."""
    images = product.get("images") or []
    by_id = {img.get("id"): img.get("src") for img in images}
    src = ((variant.get("featured_image") or {}).get("src") or by_id.get(variant.get("image_id"))
           or (images[0].get("src") if images else None))
    if not src:
        return None
    return "https:" + src if src.startswith("//") else src


def condition_of(title, tags):
    m = re.search(r"\(([^()]*)\)\s*$", title)
    if m and m.group(1).strip().lower() in CONDITIONS:
        return CONDITIONS[m.group(1).strip().lower()]
    for key in ("new open box", "refurbished", "used - good", "used-good", "used", "new"):
        if any(t.strip().lower() == key for t in tags):
            return CONDITIONS[key]
    m = re.search(r"\((used\s*-?\s*good|new open box|refurbished)\b", title, re.I)
    if m:
        return CONDITIONS.get(re.sub(r"\s+", " ", m.group(1).lower()), "Used - Good")
    return "New" if re.match(r"^new\b", title, re.I) else ""


def part_from_tags(title, tags):
    cands = [t.strip() for t in tags
             if TAG_PART_RE.match(t.strip()) and not NUMBER_RE.match(t.strip())]
    if not cands:
        return ""
    in_title = [c for c in cands if c.upper() in title.upper()]
    return (in_title or cands)[0]


def part_from_title(title):
    m = PART_IN_TITLE_RE.search(title)
    if m:
        return m.group(1)
    tokens = [t.strip("(),;:*") for t in title.split()]
    for t in tokens:
        if HYPHEN_PART_RE.match(t) and not CPU_MODEL_RE.match(t) and len(t) >= 5:
            return t
    for t in tokens:
        if CODE_RE.match(t):                    # starts with a letter: not 256GB / 750W
            return t
    return ""


def build_rows(product, scraped_at):
    title = product.get("title", "").strip()
    tags = product.get("tags", [])
    if isinstance(tags, str):
        tags = [t.strip() for t in tags.split(",") if t.strip()]
    category = (product.get("product_type") or "").split(":")[-1].strip()
    part = part_from_tags(title, tags) or part_from_title(title)
    capacity = ""
    cap = CAPACITY_RE.search(title)
    if cap and (CAPACITY_TYPES.search(product.get("product_type") or "")
                or CAPACITY_TITLES.search(title)) and not NOT_A_DRIVE_RE.search(title):
        capacity = cap.group(1) + cap.group(2)
    variants = product.get("variants", [])
    description = schema.plain_text(product.get("body_html"))
    rows = []
    for v in variants:
        price = to_float(v.get("price"))
        compare = to_float(v.get("compare_at_price"))
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": str(v["id"]),        # unique: one row per variant
            "parent_id": str(product["id"]),
            "variant_id": str(v["id"]),
            "sku": (v.get("sku") or "").strip(),
            "part_number": part,
            "title": title + ("" if v.get("title") in (None, "Default Title") else f" - {v['title']}"),
            "brand": (product.get("vendor") or "").strip(),
            "category": category,
            "condition": condition_of(title, tags),
            "capacity": capacity,
            "price": price or None,            # $0 = no real price
            "compare_at_price": compare if compare and price and compare > price else None,
            "available": bool(v.get("available")),
            "stock_qty": None,                 # not published by the store
            "image_url": image_of(product, v),
            "description": description,
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
