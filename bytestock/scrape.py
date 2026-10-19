"""Scrape shop.bytestock.com (Magento 2 GraphQL): every run's full snapshot appended to Postgres.

Bytestock is a UK refurbished-server reseller; prices are in GBP (the number is stored as-is in
the price column). Everything comes from the store's public GraphQL API (POST /graphql), no
product pages.

Strategy differs from the avoira/teqex scrapers: bytestock's Magento schema does NOT expose a
`price` filter (`ProductAttributeFilterInput` has no `price` field), so the catalogue can't be
split by price. Instead it is covered by CATEGORY: the category tree is read, and every category
node with products is fetched by `category_uid` and the products deduped by their Magento entity
id. The catalogue is small (~3.4k products) and every category is well under Magento's 10,000
-result-per-filter cap, so no further splitting is needed; a category that ever hit the cap would
take its first 10,000 and warn (there is no price axis to split on).

Mapped: product_id (decoded uid), sku / part_number / model_number (the store's Mfr Part#), title,
brand (manufacturer label when the store populates it - usually blank here), category (deepest
path), price (final, GBP), compare_at_price (regular when a sale price is lower), availability
(stock_status), url. `url_suffix` is empty on this store, so product URLs are BASE/<url_key>.

====================================================================================================
NOT VERIFIED END-TO-END. From a non-US IP (Pakistan) the whole shop.bytestock.com domain sits
behind a Cloudflare managed challenge (cf-mitigated: challenge), so a local DRY_RUN cannot reach
it. From a US IP the store answers normally: the GraphQL API, category tree and product records
used here were all validated from the US (GET /graphql?query=..., which Magento accepts for
queries). This scraper therefore MUST run from a US runner. If a future schema check shows the
store DOES expose a `price` filter, switch to the simpler whole-catalogue price split used by the
avoira scraper.
====================================================================================================

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

SITE = "bytestock"
BASE = "https://shop.bytestock.com"
GRAPHQL = f"{BASE}/graphql"
CAP = 10000          # Magento never returns more results than this for one filter
PAGE = 200           # products per page (currentPage * pageSize must stay <= CAP)
MAX_PAGE = CAP // PAGE
DELAY = 0.3          # polite pause between requests

PRODUCT_FIELDS = (
    "uid sku name url_key url_suffix stock_status only_x_left_in_stock manufacturer "
    "price_range{minimum_price{regular_price{value} final_price{value}}} "
    "categories{name url_path}"
)
_CAT_FIELDS = "uid name level product_count children_count"

CONDITIONS = [
    ("refurbished", "Refurbished"), ("recertified", "Recertified"), ("renewed", "Renewed"),
    ("open box", "Open Box"), ("open-box", "Open Box"), ("pre-owned", "Used"),
    ("preowned", "Used"), ("ex-demo", "Used"), ("used", "Used"), ("new", "New"),
]


def _category_query(depth=7):
    inner = _CAT_FIELDS
    for _ in range(depth - 1):
        inner = f"{_CAT_FIELDS} children{{{inner}}}"
    return f"{{categoryList(filters:{{}}){{{inner}}}}}"


CATEGORY_QUERY = _category_query()


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


def products_query(category_uid, page_size, page, fields):
    return (f'{{products(filter:{{category_uid:{{eq:"{category_uid}"}}}},'
            f"pageSize:{page_size},currentPage:{page}){{total_count items{{{fields}}}}}}}")


def count(category_uid):
    return gql(products_query(category_uid, 1, 1, "uid"))["products"]["total_count"]


def page_category(category_uid, seen, scraped_at, mfr, limit):
    """Page every product in one category (up to the cap) into `seen`."""
    total = count(category_uid)
    if total == 0:
        return
    if total >= CAP:
        print(f"WARN category {category_uid} has {total} products (>= cap); "
              f"taking the first {CAP} (no price filter to split on)", flush=True)
    pages = min(math.ceil(min(total, CAP) / PAGE), MAX_PAGE)
    for page in range(1, pages + 1):
        items = gql(products_query(category_uid, PAGE, page, PRODUCT_FIELDS))["products"]["items"]
        for it in items:
            add_item(it, seen, scraped_at, mfr)
        if not items or (limit and len(seen) >= limit):
            break
        time.sleep(DELAY)


# ---------------------------------------------------------------- categories
def category_nodes():
    """Every category node (uid, name, product_count) that has products, INCLUDING the store root
    (on this store the root is an anchor that lists the whole catalogue, so fetching it alone
    covers everything; the child categories are kept as a safety net and the overlap is removed by
    product id). Returns (nodes sorted largest-first, root_total). Deduped by uid."""
    data = gql(CATEGORY_QUERY)
    roots = data.get("categoryList") or []
    best, root_total = {}, 0

    def walk(node):
        pc = node.get("product_count") or 0
        if pc > 0:
            uid = node["uid"]
            if uid not in best or pc > best[uid][1]:
                best[uid] = (node.get("name") or uid, pc)
        for child in node.get("children") or []:
            walk(child)

    for root in roots:
        root_total = max(root_total, root.get("product_count") or 0)
        walk(root)
    nodes = sorted(((uid, name, pc) for uid, (name, pc) in best.items()),
                   key=lambda n: -n[2])  # largest first: the root anchor goes first
    return nodes, root_total


# ---------------------------------------------------------------- brand (manufacturer) labels
def manufacturer_labels():
    """{option id -> brand label}; best-effort (the store may leave manufacturer unset)."""
    try:
        data = gql('{customAttributeMetadata(attributes:[{attribute_code:"manufacturer",'
                   'entity_type:"catalog_product"}]){items{attribute_options{label value}}}}')
        items = data["customAttributeMetadata"]["items"]
        out = {}
        for o in (items[0]["attribute_options"] if items else []):
            label = (o.get("label") or "").strip()
            if label and re.search(r"[A-Za-z]", label) and len(label) >= 2:
                out[str(o["value"])] = label
        return out
    except Exception as e:  # noqa: BLE001 - brand is optional; never fail the run over it
        print(f"WARN could not load manufacturer labels: {e}", flush=True)
        return {}


# ---------------------------------------------------------------- mapping
def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def money(value):
    v = to_float(value)
    return round(v, 2) if v else None  # store sends float noise (x.xxx001); 2dp = real GBP


def category_path(categories):
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
        "part_number": sku,          # the store's one identifier (Mfr Part#)
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
    nodes, root_total = category_nodes()
    print(f"catalogue root lists {root_total} products across {len(nodes)} categories; "
          f"manufacturer labels: {len(mfr)}", flush=True)
    seen = {}
    for uid, name, pc in nodes:
        if limit and len(seen) >= limit:
            break
        # The root anchor (fetched first) covers the whole catalogue; once every listed product
        # is seen, the remaining child categories would only re-fetch duplicates.
        if not limit and root_total and len(seen) >= root_total:
            break
        before = len(seen)
        page_category(uid, seen, scraped_at, mfr, limit)
        print(f"  {name!r} (~{pc}): +{len(seen) - before} ({len(seen)} unique total)", flush=True)
    rows = list(seen.values())
    if limit:
        return rows[:limit], None
    return rows, root_total


if __name__ == "__main__":
    runner.run(SITE, scrape)
