"""Scrape tecisoft.com (Shopify, ~410k products in all; a general IT / consumer store): every run's
snapshot of its SERVER-RELEVANT collections appended to Postgres.

The whole store (StarTech cables, TVs, boat parts, ...) is NOT scraped. Only these collections
are read, each via /collections/<handle>/products.json?limit=250&page=N until an empty page
(products_count from /collections.json on 2026-10-19; the products.json feeds return fewer, as
the count includes products not published to the online store):

    handle                            products_count   in products.json
    memory-modules                    9,785            8,768
    ram-modules                       14,081           12,665
    memory-1                          10,838           9,586
    processors                        869              708
    motherboards-1                    718              515
    motherboards                      1,359            751
    hard-drives                       6,260            4,209
    internal-solid-state-drives       2,192            1,812
    enterprise-ssds                   83               82
    storage-drives                    8,078            5,320
    drive-cabinets                    1,276            882
    storage-arrays-servers            1,815            1,335
    raid-controllers                  65               60
    i-o-storage-controllers-1         485              368
    network-cards                     1,354            1,192
    network-switches                  2,971            2,479
    network-transceiver-modules       7,518            7,304
    transceivers-media-converters-1   1,758            1,369
    power-supply-units                238              117
    power-supplies                    415              293
    servers                           315              255
    server-workstation-systems        1,548            1,011
    server-barebones                  361              67
    computer-systems                  7,488            4,109  (servers + notebooks/desktops)

~43.7k unique products / 43,785 variant rows after removing overlaps (a product can be in several
collections, so rows are deduped by variant id); ~260 requests, one at a time, ~13 min. Every collection must stay below 25,000 products (page-based
paging); a bigger one stops the run instead of being cut short.

Mapping, one row per variant (product_id = variant id, parent_id = product id):
  sku / part_number   the variant SKU, which is the manufacturer part number on this store
  brand               vendor
  category            product_type when set, else the title of the first collection above that
                      lists the product
  condition           none published (blank)
  capacity            upper-case GB/TB in the title, for memory and drive products only
  available           the variant's available flag; no stock count is published. None for
                      products tagged "ContactUsBeforebuy": their page shows Add to cart but
                      also "DO NOT ORDER WITHOUT CONFIRMING STOCK ... limited availability"
                      (~half of the "available" products, checked 2026-10-20), so stock is unknown
  image_url           the variant's featured image, else the product's first image
  description         body_html as plain text
listed = None: the collections overlap and products_count is not the published count, so there
is no exact total to check against; a failed page raises instead of ending a collection early.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
import time

from common import http, runner, schema

SITE = "tecisoft"
BASE = "https://www.tecisoft.com"
PAGE = 250
PAGE_DELAY = 1.0              # one request at a time, 1 s apart
MAX_PER_COLLECTION = 25000    # page-based paging is only trusted below this

# handle -> category used when a product has no product_type (first match wins)
COLLECTIONS = {
    "memory-modules": "Computer Memory Modules",
    "ram-modules": "RAM Modules",
    "memory-1": "Memory",
    "processors": "Computer Processors",
    "motherboards-1": "Computer Motherboards",
    "motherboards": "Motherboards",
    "hard-drives": "Hard Drives/Solid State Drives(SSD)",
    "internal-solid-state-drives": "Internal Solid State Drives",
    "enterprise-ssds": "Enterprise SSDs",
    "storage-drives": "Storage Drives",
    "drive-cabinets": "Drive Cabinets",
    "storage-arrays-servers": "Storage Arrays & Servers",
    "raid-controllers": "RAID Controllers",
    "i-o-storage-controllers-1": "I/O & Storage Controllers",
    "network-cards": "Network Cards & Adapters",
    "network-switches": "Network Switches",
    "network-transceiver-modules": "Network Transceiver Modules",
    "transceivers-media-converters-1": "Transceivers/Media Converters",
    "power-supply-units": "Power Supply Units",
    "power-supplies": "Power Supplies",
    "servers": "Servers",
    "server-workstation-systems": "Server & Workstation Systems",
    "server-barebones": "Server Barebones",
    "computer-systems": "Computer Systems",
}
CAPACITY_RE = re.compile(r"(?<![A-Za-z0-9.])(\d+(?:\.\d+)?)\s*(TB|GB)\b")   # upper case only
CAPACITY_HINT = re.compile(r"memory|ram\b|dimm|drive|ssd|hdd|storage", re.I)
UNCONFIRMED_STOCK_TAG = "contactusbeforebuy"   # page: "DO NOT ORDER WITHOUT CONFIRMING STOCK"


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def build_rows(product, collection_title, scraped_at):
    title = (product.get("title") or "").strip()
    category = (product.get("product_type") or "").strip() or collection_title
    sized = bool(CAPACITY_HINT.search(category) or CAPACITY_HINT.search(collection_title))
    tags = product.get("tags") or []
    if isinstance(tags, str):
        tags = tags.split(",")
    stock_unconfirmed = any(t.strip().lower() == UNCONFIRMED_STOCK_TAG for t in tags)
    description = schema.plain_text(product.get("body_html"))
    product_image = (product.get("images") or [{}])[0].get("src")
    variants = product.get("variants", [])
    rows = []
    for v in variants:
        price = to_float(v.get("price"))
        compare = to_float(v.get("compare_at_price"))
        sku = (v.get("sku") or "").strip()
        full_title = title + ("" if v.get("title") in (None, "Default Title") else f" - {v['title']}")
        # a variant's own size ("32GB (2 x 16GB)") wins over the product title's first size
        cap = (CAPACITY_RE.search(v.get("title") or "") or CAPACITY_RE.search(title)) \
            if sized else None
        capacity = cap.group(1) + cap.group(2) if cap else ""
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": str(v["id"]),        # unique: one row per variant
            "parent_id": str(product["id"]),
            "variant_id": str(v["id"]),
            "sku": sku,
            "part_number": sku,                # the SKU is the manufacturer part number here
            "title": full_title,
            "brand": (product.get("vendor") or "").strip(),
            "category": category,
            "capacity": capacity,
            "price": price or None,            # $0 = no real price
            "compare_at_price": compare if compare and price and compare > price else None,
            # sold out stays False; "available" but stock-unconfirmed -> unknown
            "available": (None if stock_unconfirmed else True) if v.get("available") else False,
            "stock_qty": None,                 # not published by the public products.json
            "image_url": (v.get("featured_image") or {}).get("src") or product_image,
            "description": description,
            "url": f"{BASE}/products/{product['handle']}"
            + (f"?variant={v['id']}" if len(variants) > 1 else ""),
        }))
    return rows


def fetch_collection(handle, title, rows_by_id, limit, scraped_at):
    """Adds the rows of every product in one collection (not seen yet) to rows_by_id. A failed
    page raises (common/http retries first): it must never look like the end of the list."""
    count, page = 0, 1
    while True:
        r = http.session().get(f"{BASE}/collections/{handle}/products.json",
                               params={"limit": PAGE, "page": page}, timeout=90)
        r.raise_for_status()
        batch = r.json().get("products")
        if batch is None:
            raise RuntimeError(f"{handle} page {page}: no products key in the response")
        if not batch:
            break
        count += len(batch)
        if count >= MAX_PER_COLLECTION:
            raise RuntimeError(f"collection {handle} has {count}+ products; page-based paging "
                               "may not reach them all")
        for product in batch:
            for row in build_rows(product, title, scraped_at):
                rows_by_id.setdefault(row[schema.PRODUCT_COLUMNS.index("product_id")], row)
        if limit and len(rows_by_id) >= limit:
            break
        page += 1
        time.sleep(PAGE_DELAY)
    print(f"{handle}: {count} products, {len(rows_by_id)} unique rows so far", flush=True)


def scrape(limit, scraped_at):
    rows_by_id = {}
    for handle, title in COLLECTIONS.items():
        fetch_collection(handle, title, rows_by_id, limit, scraped_at)
        if limit and len(rows_by_id) >= limit:
            break
        time.sleep(PAGE_DELAY)
    rows = list(rows_by_id.values())
    # Overlapping collections and an unpublished-inclusive products_count: no exact total
    return (rows[:limit] if limit else rows), None


if __name__ == "__main__":
    runner.run(SITE, scrape)
