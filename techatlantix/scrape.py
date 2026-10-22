"""Scrape techatlantix.com (Magento 2 GraphQL): every run's full snapshot appended to Postgres.

Everything comes from the store's public GraphQL API (POST /graphql), no product pages. Magento's
top-level `products` query never returns more than 10,000 results for one filter (it then reports
total_count = 10000 and refuses deeper pages), so the catalogue is covered per category, exactly
the way the directmacro scraper splits by filter: we read the category tree, then page each leaf
category by `category_uid`. A category that still hits the 10,000 cap is split into price ranges
(the same ranges directmacro uses) until each slice is under the cap. Products are deduped by
their Magento entity id (decoded from `uid`) across categories.

Mapped: product_id (decoded uid), sku / part_number / model_number (the store's Mfr Part#),
title, category (deepest path), price (final), compare-at price (the regular price when a sale
price is lower), availability (stock_status), condition (read from the title / category path),
url. Brand is left blank: it is a custom attribute that is not a core GraphQL output field, so it
cannot be requested blind without risking a schema error - see the note below.

====================================================================================================
NOT VERIFIED LIVE. From a non-US / datacentre IP the whole techatlantix.com domain - HTML, robots,
sitemap and /graphql alike - sits behind a Cloudflare managed challenge ("Just a moment...",
cf-mitigated: challenge), which also blocks WebFetch, so none of this could be exercised against
the live store. It is written to the standard Magento 2 GraphQL pattern (shared with directmacro)
using only core schema fields, and MUST be validated from a US runner where the store answers. If
validation shows (a) the store uses `category_id` instead of `category_uid`, or (b) a brand /
manufacturer attribute is exposed, adjust FILTER_KEY / PRODUCT_FIELDS accordingly (marked below).
====================================================================================================

Env vars:
  DATABASE_URL   Postgres URL for the scraper_writer role
  MAX_PRODUCTS   optional, stop after about N products (test runs)
  DRY_RUN        optional, "1" = print sample rows, don't write anything
Run steps and changes: common/runner.py. Columns: common/schema.py.
"""
import base64
import math
import time

from common import http, runner, schema

SITE = "techatlantix"
BASE = "https://techatlantix.com"
GRAPHQL = f"{BASE}/graphql"
CAP = 10000          # Magento never returns more results than this for one filter
PAGE = 250           # products per page (currentPage * pageSize must stay <= CAP)
MAX_PAGE = CAP // PAGE
DELAY = 0.4          # polite pause between requests
FILTER_KEY = "category_uid"   # swap to "category_id" (with the decoded id) on older Magento

# Core product fields only - every field here is part of the standard Magento 2 GraphQL schema,
# so the query works on any Magento 2 store. Custom attributes (brand/manufacturer, specs) are
# NOT added here: requesting an attribute the store doesn't expose would fail the whole query.
PRODUCT_FIELDS = (
    "uid sku name url_key url_suffix stock_status only_x_left_in_stock "
    "price_range{minimum_price{regular_price{value} final_price{value}}} "
    "categories{name url_path}"
)

_CAT_FIELDS = "uid name level url_path product_count children_count"


def _category_query(depth=6):
    """categoryList nested `depth` levels deep (built here so the GraphQL stays valid)."""
    inner = _CAT_FIELDS
    for _ in range(depth - 1):
        inner = f"{_CAT_FIELDS} children{{{inner}}}"
    return f"{{categoryList(filters:{{}}){{{inner}}}}}"


CATEGORY_QUERY = _category_query()

# Starting price ranges (USD), same shape as directmacro; a range still over the cap is split in
# four. Edges sit on half cents so no product price is ever exactly on one.
PRICE_EDGES = [0.005] + [e + 0.005 for e in (
    25, 50, 75, 100, 150, 200, 250, 300, 400, 500, 750, 1000, 1500, 2000, 3000, 5000, 10000)]

CONDITIONS = [
    ("refurbished", "Refurbished"), ("recertified", "Recertified"),
    ("renewed", "Renewed"), ("open box", "Open Box"), ("open-box", "Open Box"),
    ("pre-owned", "Used"), ("preowned", "Used"), ("used", "Used"), ("new", "New"),
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
            message = f"HTTP {r.status_code}: {str(body.get('errors') or body)[:300]}"
            raise RuntimeError(message)
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


def page_all(f, limit=None):
    """Page every product matching f (up to the cap). Returns (items, total_count)."""
    total = count(f)
    if total == 0:
        return [], 0
    pages = min(math.ceil(total / PAGE), MAX_PAGE)
    items = []
    for page in range(1, pages + 1):
        batch = gql(products_query(f, PAGE, page, PRODUCT_FIELDS))["products"]["items"]
        items.extend(batch)
        if not batch or (limit and len(items) >= limit):
            break
        time.sleep(DELAY)
    return items, total


# ---------------------------------------------------------------- splitting under the cap
def price_children(lo, hi):
    """Split a price range into sub-ranges, or [] when it can't be split further."""
    if lo is None or hi is None:
        # lowest bucket (< ~$25) is already small; split only the top open-ended bucket
        if lo is not None and hi is None:
            mid = round(max(lo * 4, lo + 1000) + 0.005, 3)
            return [(lo, mid), (mid, None)]
        return []
    edges = sorted({round(lo + (hi - lo) * k / 4 + 0.005, 3) for k in (1, 2, 3)})
    edges = [e for e in edges if lo < e < hi]
    points = [lo] + edges + [hi]
    return list(zip(points, points[1:])) if edges else []


def price_split(f):
    if "price" not in f:
        ranges = ([(None, PRICE_EDGES[0])] + list(zip(PRICE_EDGES, PRICE_EDGES[1:]))
                  + [(PRICE_EDGES[-1], None)])
        return [{**f, "price": r} for r in ranges]
    return [{**f, "price": r} for r in price_children(*f["price"])]


def collect(f, seen, scraped_at, limit):
    """Fetch every product matching f into `seen`; if f hits the cap, split it by price."""
    total = count(f)
    if total == 0:
        return
    if total < CAP:
        items, _ = page_all(f, limit)
        for it in items:
            add_item(it, seen, scraped_at)
        return
    children = price_split(f)
    if not children:
        print(f"WARN {f} is still capped at a single price; fetching the first {CAP}", flush=True)
        items, _ = page_all(f, limit)
        for it in items:
            add_item(it, seen, scraped_at)
        return
    for child in children:
        if limit and len(seen) >= limit:
            return
        collect(child, seen, scraped_at, limit)


# ---------------------------------------------------------------- categories
def leaf_categories():
    """Leaf categories (by category_uid) whose union covers the catalogue. A node with children
    we didn't fetch (deeper than the query) is itself taken as a target and price-split if needed.
    Returns (leaves, root_total)."""
    data = gql(CATEGORY_QUERY)
    roots = data.get("categoryList") or []
    leaves, root_total = [], 0

    def walk(node):
        pc = node.get("product_count") or 0
        cc = node.get("children_count") or 0
        children = node.get("children") or []
        if pc <= 0:
            for c in children:
                walk(c)
            return
        if cc == 0:
            leaves.append((node["uid"], node.get("name") or node["uid"], pc))  # true leaf
        elif children:
            for c in children:                                                 # recurse fetched children
                walk(c)
        else:
            leaves.append((node["uid"], node.get("name") or node["uid"], pc))  # deeper than our query

    for root in roots:
        root_total = max(root_total, root.get("product_count") or 0)
        # skip the store root itself (it would hit the cap); walk its children
        for c in (root.get("children") or []):
            walk(c)
        if not (root.get("children") or []) and (root.get("product_count") or 0) > 0:
            leaves.append((root["uid"], root.get("name") or root["uid"], root["product_count"]))
    # de-duplicate category uids, keep the largest product_count seen
    best = {}
    for uid, name, pc in leaves:
        if uid not in best or pc > best[uid][1]:
            best[uid] = (name, pc)
    return [(uid, name, pc) for uid, (name, pc) in best.items()], root_total


# ---------------------------------------------------------------- mapping
def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def category_path(categories):
    """Deepest category as "Networking > Switches > Gigabit Switches"."""
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
    except Exception:  # noqa: BLE001 - uid may already be a plain id on some stores
        return str(uid)


def add_item(item, seen, scraped_at):
    pid = decode_uid(item.get("uid"))
    if pid in seen:
        return
    title = item.get("name") or ""
    category = category_path(item.get("categories"))
    prices = (item.get("price_range") or {}).get("minimum_price") or {}
    final = to_float((prices.get("final_price") or {}).get("value")) or None
    regular = to_float((prices.get("regular_price") or {}).get("value")) or None
    sku = item.get("sku") or ""
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
        "brand": "",                 # custom attribute - add once the live schema is confirmed
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
    leaves, root_total = leaf_categories()
    print(f"catalogue root lists {root_total} products across {len(leaves)} leaf categories",
          flush=True)
    seen = {}
    for uid, name, pc in leaves:
        if limit and len(seen) >= limit:
            break
        before = len(seen)
        collect({FILTER_KEY: uid}, seen, scraped_at, limit)
        print(f"  {name!r} (~{pc}): +{len(seen) - before} ({len(seen)} unique total)", flush=True)
    if not limit and root_total and len(seen) < 0.95 * root_total:
        print(f"WARN only {len(seen)} of ~{root_total} products reached - some may be assigned "
              f"only to non-leaf categories; validate from the US runner", flush=True)
    rows = list(seen.values())
    return (rows[:limit] if limit else rows), None


if __name__ == "__main__":
    runner.run(SITE, scrape)
