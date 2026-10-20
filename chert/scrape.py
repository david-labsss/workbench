"""Scrape chert.ng (Magento 2 GraphQL), SCOPED to the Storage category: every run's full
snapshot appended to Postgres.

Chert is a large Nigerian IT retailer (~87k products in all); we only want its server/storage
hardware, so this scraper is scoped to the top-level "Storage" category (category_uid "NzE=":
NAS/storage servers, internal/external hard drives, SSDs, tape drives, storage devices, ...).
It never crawls the whole catalogue. Prices are in NGN (the number is stored as-is).

Everything comes from the store's public GraphQL API (POST /graphql), no product pages - the
same plumbing as the avoira/teqex Magento scrapers (gql, gql_filter, price/stock mapping). Two
quirks of THIS store shape the paging and are handled deliberately:

  * total_count lies. The Storage filter reports total_count = 2701, but the `items` resolver
    only ever returns the ~940 salable products (in stock, visible, priced); the rest are
    disabled / out-of-stock / not on the default store and are counted but never rendered. So
    `listed` is returned as None (there is no reliable total) and the runner's completeness
    check is skipped - the full renderable set IS collected, it just isn't 2701.
  * Interior pages come back empty. At small page sizes some middle pages return [] even though
    later pages have products, so we must NOT treat an empty page as the end. We page EVERY page
    1..total_pages (from page_info) without breaking, deduping by the Magento entity id (decoded
    from `uid`). pageSize 300 returns all 10 pages with no gaps; walking them all is exact.

The category is far under Magento's 10,000-per-filter cap, so no price/manufacturer splitting is
needed (a warning fires if the store ever exceeds it).

Brand: the store exposes `manufacturer` as an integer option id per product, not a label. The id
-> label map is fetched once from `customAttributeMetadata` and applied (numeric/punctuation-only
labels are discarded as junk). No `condition` attribute exists, so condition is read from the
title / category path when present. The deepest category path is reported; many Storage products
are cross-listed in deeper computing/peripherals trees, so `category` may show that branch.

Mapped: product_id (decoded uid), sku / part_number / model_number (the store's SKU), title,
brand (manufacturer label), category (deepest path), price (final, NGN), compare_at_price
(regular when a sale price is lower), availability (stock_status), condition (from text), url.

Env vars:
  DATABASE_URL   Postgres URL for the scraper_writer role
  MAX_PRODUCTS   optional, stop after about N products (test runs)
  DRY_RUN        optional, "1" = print sample rows, don't write anything
Run steps and changes: common/runner.py. Columns: common/schema.py.
"""
import base64
import re
import time

from common import http, runner, schema

SITE = "chert"
BASE = "https://chert.ng"
GRAPHQL = f"{BASE}/graphql"
# Top-level "Storage" category (the server/storage scope). category_uid is Magento's base64
# category id; this one decodes to 71. ~940 salable products are exposed (total_count is 2701).
CATEGORY_UID = "NzE="
CAP = 10000          # Magento never returns more results than this for one filter
PAGE = 300           # products per page; at this size the store returns every page with no gaps
MAX_PAGE = CAP // PAGE
DELAY = 0.3          # polite pause between requests

PRODUCT_FIELDS = (
    "uid sku name url_key url_suffix stock_status only_x_left_in_stock manufacturer "
    "price_range{minimum_price{regular_price{value} final_price{value}}} "
    "categories{name url_path}"
)

CONDITIONS = [
    ("refurbished", "Refurbished"), ("recertified", "Recertified"), ("renewed", "Renewed"),
    ("open box", "Open Box"), ("open-box", "Open Box"), ("pre-owned", "Used"),
    ("preowned", "Used"), ("ex-demo", "Used"), ("used", "Used"), ("new", "New"),
]


# ---------------------------------------------------------------- GraphQL
def gql(query):
    """Run one GraphQL request. Any error raises (a failed request must never look like
    "no more products": Magento answers some errors with HTTP 200 + an errors key)."""
    for attempt in range(1, 5):
        try:
            r = http.session().post(GRAPHQL, json={"query": query}, timeout=120)
            body = r.json()
            if r.status_code == 200 and body.get("data") and not body.get("errors"):
                return body["data"]
            raise RuntimeError(f"HTTP {r.status_code}: {str(body.get('errors') or body)[:300]}")
        except Exception as e:  # noqa: BLE001 - retry transient failures, then give up loudly
            if attempt == 4:
                raise RuntimeError(f"GraphQL request failed 4 times: {e}") from e
            print(f"WARN GraphQL attempt {attempt} failed: {e}", flush=True)
            time.sleep(5 * attempt)


def category_query(page, fields, with_info=False):
    info = "page_info{total_pages} " if with_info else ""
    return (f'{{products(filter:{{category_uid:{{eq:"{CATEGORY_UID}"}}}},'
            f"pageSize:{PAGE},currentPage:{page}){{total_count {info}items{{{fields}}}}}}}")


# ---------------------------------------------------------------- paging
def walk_category(seen, scraped_at, mfr, limit):
    """Collect every product in the Storage category, deduped by entity id. Pages EVERY page
    1..total_pages (this store returns empty interior pages, so an empty page is NOT the end)."""
    head = gql(category_query(1, "uid", with_info=True))["products"]
    total_count = head["total_count"]
    total_pages = head["page_info"]["total_pages"]
    print(f"Storage category: total_count={total_count} (inflated; counts non-salable items), "
          f"total_pages={total_pages} at pageSize {PAGE}", flush=True)
    if total_count >= CAP:
        print(f"WARN Storage total_count {total_count} hit the {CAP} cap; paging only the first "
              f"{MAX_PAGE} pages (a price split would be needed to go deeper)", flush=True)
    for page in range(1, min(total_pages, MAX_PAGE) + 1):
        items = gql(category_query(page, PRODUCT_FIELDS))["products"]["items"]
        for it in items:
            add_item(it, seen, scraped_at, mfr)
        if limit and len(seen) >= limit:
            break
        time.sleep(DELAY)


# ---------------------------------------------------------------- brand (manufacturer) labels
def manufacturer_labels():
    """{option id -> brand label} for the `manufacturer` attribute (junk labels dropped)."""
    data = gql('{customAttributeMetadata(attributes:[{attribute_code:"manufacturer",'
               'entity_type:"catalog_product"}]){items{attribute_options{label value}}}}')
    items = data["customAttributeMetadata"]["items"]
    out = {}
    for o in (items[0]["attribute_options"] if items else []):
        label = (o.get("label") or "").strip()
        if label and re.search(r"[A-Za-z]", label) and len(label) >= 2:
            out[str(o["value"])] = label
    return out


# ---------------------------------------------------------------- mapping
def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def money(value):
    v = to_float(value)
    return round(v, 2) if v else None  # NGN as-is; drop any float noise to 2dp


def category_path(categories):
    """Deepest category as "Storage > Internal hard drives"."""
    names = {c["url_path"]: c["name"] for c in categories or [] if c.get("url_path")}
    if not names:
        return ""
    parts = max(names, key=lambda p: p.count("/")).split("/")
    return " > ".join(
        names.get("/".join(parts[:i + 1]), parts[i].replace("-", " ").title())
        for i in range(len(parts)))


def condition_of(*texts):
    blob = " ".join(t for t in texts if t).lower()
    for needle, label in CONDITIONS:
        if needle in blob:
            return label
    return ""


def decode_uid(uid):
    try:
        return base64.b64decode(uid).decode()
    except Exception:  # noqa: BLE001 - uid may already be a plain id
        return str(uid)


def add_item(item, seen, scraped_at, mfr):
    pid = decode_uid(item.get("uid"))
    if pid in seen:
        return
    title = item.get("name") or ""
    category = category_path(item.get("categories"))
    prices = (item.get("price_range") or {}).get("minimum_price") or {}
    final = money((prices.get("final_price") or {}).get("value"))
    regular = money((prices.get("regular_price") or {}).get("value"))
    sku = item.get("sku") or ""
    brand = mfr.get(str(item.get("manufacturer"))) if item.get("manufacturer") is not None else ""
    url_key = item.get("url_key") or ""
    url = f"{BASE}/{url_key}{item.get('url_suffix') or ''}" if url_key else BASE
    seen[pid] = schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": pid,
        "variant_id": pid,
        "sku": sku,
        "part_number": sku,          # the store's one identifier
        "model_number": sku,
        "title": title,
        "brand": brand or "",
        "category": category,
        "condition": condition_of(title, category),
        "price": final,
        "compare_at_price": regular if (final and regular and regular > final) else None,
        "available": item.get("stock_status") == "IN_STOCK",
        "stock_qty": item.get("only_x_left_in_stock"),
        "url": url,
    })


# ---------------------------------------------------------------- run
def scrape(limit, scraped_at):
    mfr = manufacturer_labels()
    print(f"manufacturer labels: {len(mfr)}", flush=True)
    seen = {}
    walk_category(seen, scraped_at, mfr, limit)
    print(f"collected {len(seen)} unique products", flush=True)
    rows = list(seen.values())
    if limit:
        rows = rows[:limit]
    # listed is None on purpose: the store's total_count counts non-salable items it never
    # returns, so there is no reliable total to check completeness against.
    return rows, None


if __name__ == "__main__":
    runner.run(SITE, scrape)
