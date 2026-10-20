"""Scrape compuwyze.co.uk (Compuwyze, Magento 2 GraphQL): every run's full snapshot appended to
Postgres.

Compuwyze is a UK reseller of refurbished enterprise servers, storage and components; prices are in
GBP (the number is stored as-is in the price column). Everything comes from the store's public
GraphQL API (POST /graphql), no product pages. The catalogue is small (~2,600 products), so a single
`price:{from:"0"}` filter returns the whole catalogue in pages of 200 - comfortably under Magento's
10,000-results-per-filter cap. The cap-splitting machinery is kept exactly as in the sibling Magento
scrapers (avoira / teqex) as a safety net: a filter that ever hits the cap is split by PRICE, and a
single price point still over the cap by `manufacturer` (the store's brand attribute). Products are
deduped by their Magento entity id (decoded from `uid`). About 40 products (~1.6%) are of a Magento
product type the store's GraphQL schema can't resolve and come back `null` on every field; they are
unreachable via this API and skipped, so a full run covers ~98.4% of the listed count (above the
runner's 98% completeness floor). See gql().

Brand: the store exposes `manufacturer` as an integer option id per product, not a label. The id ->
label map is fetched once from `customAttributeMetadata` and applied (numeric/punctuation-only
labels are discarded as junk). No `condition` attribute is read directly; condition is taken from the
title / category path (the store sells refurbished gear, so categories like "refurbished-servers"
flag it).

Mapped: product_id (decoded uid), sku / part_number / model_number (the store's SKU), title,
brand (manufacturer label), category (deepest path), price (final, GBP), compare_at_price (regular
when a sale price is lower), availability (stock_status), condition (from text), url.

Env vars:
  DATABASE_URL   Postgres URL for the scraper_writer role
  MAX_PRODUCTS   optional, stop after about N products (test runs)
  DRY_RUN        optional, "1" = print sample rows, don't write anything
Run steps and changes: common/runner.py. Columns: common/schema.py.
"""
import base64
import math
import re
import time

from common import http, runner, schema

SITE = "compuwyze"
BASE = "https://compuwyze.co.uk"
GRAPHQL = f"{BASE}/graphql"
CAP = 10000          # Magento never returns more results than this for one filter
PAGE = 200           # products per page (currentPage * pageSize must stay <= CAP)
MAX_PAGE = CAP // PAGE
DELAY = 0.3          # polite pause between requests

# Starting price ranges (GBP), same shape as directmacro/techatlantix; a range still over the cap
# is quartered. Edges sit on half pennies so no real price (x.xx...) ever lands exactly on one.
PRICE_EDGES = [0.005] + [e + 0.005 for e in (
    25, 50, 75, 100, 150, 200, 250, 300, 400, 500, 750, 1000, 1500, 2000, 3000, 5000, 10000)]

# Core product fields + manufacturer (a store attribute that IS a valid output field here: it
# returns the option id, mapped to a label below).
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
# ~40 of the store's ~2,560 products are of a Magento product type with no GraphQL type registered;
# Magento returns them as `null` in `items` with this per-item error (nothing - not even `uid` -
# can be read for them via the API). It is a stable store-side defect, not a transient failure, so
# these errors are tolerated: the good items in the same response are kept and the null ones skipped.
TOLERABLE_ERROR = "Concrete type for ProductInterface not implemented"


def _only_tolerable(errors):
    return bool(errors) and all(TOLERABLE_ERROR in (e.get("message") or "") for e in errors)


def gql(query):
    """Run one GraphQL request. A genuine failure raises (it must never look like "no more
    products": Magento answers some errors with HTTP 200 + an errors key). Per-item
    "Concrete type ... not implemented" errors are tolerated and the surrounding data returned."""
    for attempt in range(1, 5):
        try:
            r = http.session().post(GRAPHQL, json={"query": query}, timeout=120)
            body = r.json()
            if r.status_code == 200 and body.get("data") is not None and (
                    not body.get("errors") or _only_tolerable(body["errors"])):
                return body["data"]
            raise RuntimeError(f"HTTP {r.status_code}: {str(body.get('errors') or body)[:300]}")
        except Exception as e:  # noqa: BLE001 - retry transient failures, then give up loudly
            if attempt == 4:
                raise RuntimeError(f"GraphQL request failed 4 times: {e}") from e
            print(f"WARN GraphQL attempt {attempt} failed: {e}", flush=True)
            time.sleep(5 * attempt)


def gql_filter(f):
    parts = []
    for key, value in f.items():
        if key == "price":
            lo, hi = value
            rng = ([f'from:"{lo:.3f}"'] if lo is not None else []) + (
                [f'to:"{hi:.3f}"'] if hi is not None else [])
            parts.append(f"price:{{{','.join(rng)}}}")
        else:
            parts.append(f'{key}:{{eq:"{value}"}}')
    return "{" + ",".join(parts) + "}"


def products_query(f, page_size, page, fields):
    return (f"{{products(filter:{gql_filter(f)},pageSize:{page_size},currentPage:{page})"
            f"{{total_count items{{{fields}}}}}}}")


def count(f):
    return gql(products_query(f, 1, 1, "uid"))["products"]["total_count"]


def page_all(f, seen, scraped_at, mfr, limit):
    """Page every product matching f (up to the cap) into `seen`."""
    total = count(f)
    if total == 0:
        return
    pages = min(math.ceil(total / PAGE), MAX_PAGE)
    for page in range(1, pages + 1):
        items = gql(products_query(f, PAGE, page, PRODUCT_FIELDS))["products"]["items"]
        for it in items:
            if it:                       # skip null items (unresolvable product type, see gql())
                add_item(it, seen, scraped_at, mfr)
        if not items or (limit and len(seen) >= limit):
            break
        time.sleep(DELAY)


# ---------------------------------------------------------------- splitting under the cap
def price_children(lo, hi):
    """Split a price range into sub-ranges, or [] when it can't be split further."""
    if hi is None:
        # top open-ended bucket: carve off a finite slice and keep the remainder open
        mid = round(max((lo or 0) * 4, (lo or 0) + 1000) + 0.005, 3)
        return [(lo, mid), (mid, None)]
    edges = sorted({round(lo + (hi - lo) * k / 4 + 0.005, 3) for k in (1, 2, 3)})
    edges = [e for e in edges if lo < e < hi]
    points = [lo] + edges + [hi]
    return list(zip(points, points[1:])) if edges else []


def manufacturer_children(f):
    """A single price with CAP+ products: split by manufacturer (the store's brand attribute).
    Uses the layered-navigation aggregation for this exact price slice."""
    data = gql("{products(filter:" + gql_filter(f) + ",pageSize:1){aggregations"
               "{attribute_code options{value count}}}}")
    for agg in data["products"]["aggregations"]:
        if agg["attribute_code"] == "manufacturer":
            return [{**f, "manufacturer": o["value"]} for o in agg["options"] if o["count"]]
    return []


def collect(f, seen, scraped_at, mfr, limit):
    """Fetch every product matching f into `seen`; if f hits the cap, split it (price, then
    manufacturer). A slice that still can't be split takes the first CAP and warns."""
    total = count(f)
    if total == 0:
        return
    if total < CAP:
        page_all(f, seen, scraped_at, mfr, limit)
        return
    children = price_children(*f["price"]) if "manufacturer" not in f else []
    builder = lambda r: {**f, "price": r}  # noqa: E731
    if not children and "manufacturer" not in f:
        children = manufacturer_children(f)
        builder = lambda m: m  # noqa: E731 - manufacturer_children already returns full filters
    if not children:
        print(f"WARN {f} is capped at {total} and can't be split further; taking the first {CAP}",
              flush=True)
        page_all(f, seen, scraped_at, mfr, limit)
        return
    for child in children:
        if limit and len(seen) >= limit:
            return
        collect(builder(child) if "manufacturer" not in f else child,
                seen, scraped_at, mfr, limit)


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
    return round(v, 2) if v else None  # the store sends float noise (x.xxx001); 2dp = real GBP


def category_path(categories):
    """Deepest category as "Servers > Rack Servers > HPE ProLiant DL Servers"."""
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
        "part_number": sku,          # the store's one identifier (SKU)
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
    root_total = count({"price": (0.0, None)})
    print(f"catalogue lists {root_total} products (price>=0)", flush=True)
    seen = {}
    collect({"price": (0.0, None)}, seen, scraped_at, mfr, limit)
    print(f"collected {len(seen)} unique products", flush=True)
    rows = list(seen.values())
    if limit:
        return rows[:limit], None
    return rows, root_total


if __name__ == "__main__":
    runner.run(SITE, scrape)
