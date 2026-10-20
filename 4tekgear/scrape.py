"""Scrape 4tekgear.com (Magento 2 GraphQL): every run's full snapshot appended to Postgres.

4tekgear is a US networking / IT-hardware reseller; prices are in USD (stored as-is in the price
column). Everything comes from the store's public GraphQL API (POST /graphql), no product pages.
Magento's top-level `products` query never returns more than 10,000 results for one filter (it then
reports total_count = 10000 and refuses deeper pages), so - exactly like teqex/directmacro - the
catalogue is covered by splitting a price filter until every slice is under the 10,000 cap.

Scale (probed 2026-10-20): this store is far bigger than it looks from the storefront - about
98,000 products reachable through GraphQL (~96,984 priced across ~36 price slices + ~1,067 quote
items at price 0). The whole priced side exceeds the 10k cap, so price-range splitting is genuinely
required here (no single price point exceeds the cap, so splitting by price alone is enough; the
manufacturer fallback stays in for safety). Real USD prices confirmed (e.g. a switch bundle at
$2,251.30 with a $3,174 regular price).

Store quirks (probed):
  * The integer `id` is used as the product key (no base64 decode). `uid` also exists here, but `id`
    is kept for parity with the enterasource scraper.
  * A price filter needs BOTH bounds: `price:{from:"0"}` alone returns 0 results, while
    `price:{from:"0",to:"..."}` returns the real catalogue. So every range is emitted with from+to.
  * Quote items (price 0) are kept as rows with price=None (never 0). The priced side is collected
    first so a sample/DRY_RUN run shows real prices, not quote rows.

Brand: `manufacturer` is an integer option id per product; the id -> label map is fetched once from
`customAttributeMetadata`. Condition is read from the title / category / description text.

Mapped: product_id (integer id), sku / part_number / model_number, title, brand (manufacturer
label), category (deepest path), price (final, USD), compare_at_price (regular when a sale price is
lower), availability (stock_status), condition (from text), image_url, description, url.

Env vars:
  DATABASE_URL   Postgres URL for the scraper_writer role
  MAX_PRODUCTS   optional, stop after about N products (test runs)
  DRY_RUN        optional, "1" = print sample rows, don't write anything
Run steps and changes: common/runner.py. Columns: common/schema.py.
"""
import math
import re
import time

from common import http, runner, schema

SITE = "4tekgear"
BASE = "https://4tekgear.com"
GRAPHQL = f"{BASE}/graphql"
CAP = 10000          # Magento never returns more results than this for one filter
PAGE = 200           # products per page (currentPage * pageSize must stay <= CAP)
MAX_PAGE = CAP // PAGE
DELAY = 0.3          # polite pause between requests
TOP = 100_000_000.0  # finite upper bound (a lone `from` filter returns nothing here)
QUOTE_HI = 0.004     # price == 0 (quote) slice: [0, 0.004]
PRICED_LO = 0.005    # priced slice starts here, so edges never land on a real 2dp price

# Core product fields. `id` (not `uid`) is the key, for parity with the enterasource scraper.
PRODUCT_FIELDS = (
    "id sku name url_key url_suffix stock_status only_x_left_in_stock manufacturer "
    "price_range{minimum_price{regular_price{value} final_price{value}}} "
    "categories{name url_path} image{url} small_image{url} "
    "description{html} short_description{html}"
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


def gql_filter(f):
    parts = []
    for key, value in f.items():
        if key == "price":
            lo, hi = value
            parts.append(f'price:{{from:"{lo:.3f}",to:"{hi:.3f}"}}')
        else:
            parts.append(f'{key}:{{eq:"{value}"}}')
    return "{" + ",".join(parts) + "}"


def products_query(f, page_size, page, fields):
    return (f"{{products(filter:{gql_filter(f)},pageSize:{page_size},currentPage:{page})"
            f"{{total_count items{{{fields}}}}}}}")


def count(f):
    return gql(products_query(f, 1, 1, "id"))["products"]["total_count"]


def page_all(f, seen, scraped_at, mfr, limit):
    """Page every product matching f (up to the cap) into `seen`."""
    total = count(f)
    if total == 0:
        return
    pages = min(math.ceil(total / PAGE), MAX_PAGE)
    for page in range(1, pages + 1):
        items = gql(products_query(f, PAGE, page, PRODUCT_FIELDS))["products"]["items"]
        for it in items:
            add_item(it, seen, scraped_at, mfr)
        if not items or (limit and len(seen) >= limit):
            break
        time.sleep(DELAY)


# ---------------------------------------------------------------- splitting under the cap
def price_children(lo, hi):
    """Quarter a finite price range; [] when it can't be split (a single price point)."""
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
    manufacturer). Returns the sum of the leaf-slice counts (the reachable catalogue size)."""
    total = count(f)
    if total == 0:
        return 0
    if total < CAP:
        page_all(f, seen, scraped_at, mfr, limit)
        return total
    children = price_children(*f["price"]) if "manufacturer" not in f else []
    builder = lambda r: {**f, "price": r}  # noqa: E731
    if not children and "manufacturer" not in f:
        children = manufacturer_children(f)
        builder = lambda m: m  # noqa: E731 - manufacturer_children already returns full filters
    if not children:
        print(f"WARN {f} is capped at {total} and can't be split further; taking the first {CAP}",
              flush=True)
        page_all(f, seen, scraped_at, mfr, limit)
        return total
    listed = 0
    for child in children:
        if limit and len(seen) >= limit:
            break
        listed += collect(builder(child) if "manufacturer" not in f else child,
                           seen, scraped_at, mfr, limit)
    return listed


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
    return round(v, 2) if v else None  # 0 / missing -> None (quote item), never a 0 price


def category_path(categories):
    """Deepest category path as "Products > Switches > ...", joined with " > "."""
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


def add_item(item, seen, scraped_at, mfr):
    pid = str(item.get("id"))
    if not pid or pid == "None" or pid in seen:
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
    description = schema.plain_text((item.get("description") or {}).get("html")
                                    or (item.get("short_description") or {}).get("html"))
    seen[pid] = schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": pid,
        "variant_id": pid,
        "sku": sku,
        "part_number": sku,          # the store's one identifier (Mfr Part#)
        "model_number": sku,
        "title": title,
        "brand": brand or "",
        "category": category,
        "condition": condition_of(title, category, description),
        "price": final,
        "compare_at_price": regular if (final and regular and regular > final) else None,
        "available": item.get("stock_status") == "IN_STOCK",
        "stock_qty": item.get("only_x_left_in_stock"),
        "image_url": ((item.get("image") or {}).get("url")
                      or (item.get("small_image") or {}).get("url")) or None,
        "description": description,
        "url": url,
    })


# ---------------------------------------------------------------- run
def scrape(limit, scraped_at):
    mfr = manufacturer_labels()
    print(f"manufacturer labels: {len(mfr)}", flush=True)
    seen = {}
    # Priced items first (so a sample shows real prices), then the quote ($0) slice.
    listed = collect({"price": (PRICED_LO, TOP)}, seen, scraped_at, mfr, limit)
    priced = len(seen)
    if not (limit and len(seen) >= limit):
        listed += collect({"price": (0.0, QUOTE_HI)}, seen, scraped_at, mfr, limit)
    print(f"catalogue reachable total {listed} ({priced} priced, {len(seen) - priced} quote); "
          f"collected {len(seen)} unique", flush=True)
    rows = list(seen.values())
    if limit:
        return rows[:limit], None
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
