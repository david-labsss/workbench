"""Scrape ziestech.com (Shopify, ~210k products): every run's full snapshot appended to Postgres.

Ziestech resells a very large VoIP / networking / IT catalogue (Fanvil, Ubiquiti, StarTech, ...).
The store has ~210,000 products - far more than Shopify's 25,000-item deep-paging cap - so the
public /products.json (and any single collection listing) stops at 25k and cannot reach the whole
catalogue (confirmed: /products.json?page=101 returns HTTP 400). We read the Storefront GraphQL
API (/api/<ver>/graphql.json) instead: one products listing sorted by CREATED_AT, walked one
calendar month at a time oldest-first, a few months in parallel; when a month's listing nears the
25k cap we restart it from the last createdAt seen (">=" so items sharing that second aren't
skipped; the overlap is removed by product id). Modelled on atechmemory. The store answers
tokenless; it also ships a public storefront token used as the default.

One row per variant: product_id = variant id, parent_id = product id. Fields: title, vendor
(brand), productType (category), variant SKU, price, compare-at price, availableForSale. Part
number = SKU, or the first word of the title when that looks like a part number. Condition and
capacity are read from the title / variant title when present. Prices are the store's USD prices
and saved as-is. The public API carries no stock count, so stock_qty stays blank.

A $0.00 price means the item is quote/hidden, not free, so it is saved as blank (never 0); the
same guard applies to compare_at_price.

The product count comes from the store's product sitemap only as a completeness check, so a
truncated crawl is never saved as the full catalogue.

Env vars:
  DATABASE_URL        Postgres URL for the scraper_writer role
  STOREFRONT_TOKEN    optional, defaults to the public token the store ships
  MAX_PRODUCTS        optional, stop after about N products (test runs)
  DRY_RUN             optional, "1" = print sample rows, don't write anything
Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from common import http, runner, schema

SITE = "ziestech"
BASE = "https://ziestech.com"
API = BASE + "/api/2025-07/graphql.json"
TOKEN = os.getenv("STOREFRONT_TOKEN") or "21187a1bac593b10ef218b58544ffe1d"
PAGE = 250            # Storefront max per request
CAP = 25000           # Shopify won't page a single listing deeper than this
WORKERS = 4           # low concurrency: be polite
PAGE_DELAY = 0.3

CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB|MB)\b", re.I)
CONDITIONS = [
    (re.compile(r"\brefurbished\b", re.I), "Refurbished"),
    (re.compile(r"\brecertified\b", re.I), "Recertified"),
    (re.compile(r"\brenewed\b", re.I), "Renewed"),
    (re.compile(r"\bopen\s*box\b", re.I), "Open Box"),
    (re.compile(r"\bused\b", re.I), "Used"),
    (re.compile(r"\bnew\b", re.I), "New"),
]

PRODUCT_FIELDS = """
  id title handle vendor productType tags createdAt featuredImage { url } descriptionHtml
  variants(first: 50) {
    pageInfo { hasNextPage }
    nodes { id sku title price { amount } compareAtPrice { amount } availableForSale }
  }
"""
LIST_QUERY = """
query($first: Int!, $after: String, $query: String!) {
  products(first: $first, after: $after, query: $query, sortKey: CREATED_AT) {
    pageInfo { hasNextPage endCursor }
    nodes { %s }
  }
}""" % PRODUCT_FIELDS
EARLIEST_QUERY = "{ products(first: 1, sortKey: CREATED_AT, reverse: false) { nodes { createdAt } } }"


def gql(query, variables=None):
    """Run a query; retry GraphQL-level throttling / internal errors, raise on anything else, so a
    failed request is never read as the end of a listing."""
    for attempt in range(6):
        r = http.session().post(
            API, json={"query": query, "variables": variables or {}},
            headers={"Content-Type": "application/json",
                     "X-Shopify-Storefront-Access-Token": TOKEN}, timeout=60)
        r.raise_for_status()
        data = r.json()
        errors = data.get("errors")
        if not errors:
            return data["data"]
        codes = {(e.get("extensions") or {}).get("code") for e in errors}
        if codes & {"THROTTLED", "INTERNAL_SERVER_ERROR"} and attempt < 5:
            time.sleep(5 * (attempt + 1))
            continue
        raise RuntimeError(f"Storefront API error: {errors[0].get('message')}")
    raise RuntimeError("Storefront API kept failing")


def earliest_year():
    try:
        nodes = gql(EARLIEST_QUERY)["products"]["nodes"]
        return int(nodes[0]["createdAt"][:4]) if nodes else datetime.now(timezone.utc).year
    except Exception as e:  # noqa: BLE001 - fall back to a safe early year
        print(f"WARN couldn't read the earliest product date: {e}; starting at 2015", flush=True)
        return 2015


def month_windows(now, first_year):
    """[start, end) pairs, one per calendar month from first_year to next month."""
    edges, y, m = [], first_year, 1
    while True:
        edges.append(f"{y:04d}-{m:02d}-01T00:00:00Z")
        if (y, m) > (now.year, now.month):
            break
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return list(zip(edges, edges[1:]))


def fetch_window(window, limit=0):
    """Every product created in [start, end), oldest first. A listing stops at 25k, so when it
    nears the cap we start a new listing from the last createdAt seen; overlap is removed by id."""
    start, end = window
    by_id, after, frm, depth = {}, None, start, 0
    while True:
        q = f"created_at:>='{frm}' AND created_at:<'{end}'"
        page = gql(LIST_QUERY, {"first": PAGE, "after": after, "query": q})["products"]
        for n in page["nodes"]:
            by_id[n["id"]] = n
        depth += len(page["nodes"])
        if not page["pageInfo"]["hasNextPage"] or (limit and len(by_id) >= limit):
            break
        if depth + PAGE > CAP:
            last = page["nodes"][-1]["createdAt"]
            if last == frm:
                print(f"WARN 25k+ products created at {last}; only 25k are reachable", flush=True)
                break
            frm, after, depth = last, None, 0
        else:
            after = page["pageInfo"]["endCursor"]
        time.sleep(PAGE_DELAY)
    if by_id:
        print(f"created {start[:7]}: {len(by_id)} products", flush=True)
    return list(by_id.values())


def fetch_all(limit=0):
    windows = month_windows(datetime.now(timezone.utc), earliest_year())
    print(f"{len(windows)} monthly windows", flush=True)
    by_id = {}
    if limit:
        for w in windows:
            for n in fetch_window(w, limit):
                by_id[n["id"]] = n
            if len(by_id) >= limit:
                break
        return list(by_id.values())[:limit]
    with ThreadPoolExecutor(WORKERS) as pool:
        for nodes in pool.map(fetch_window, windows):
            for n in nodes:
                by_id[n["id"]] = n
            print(f"  unique so far: {len(by_id)}", flush=True)
    return list(by_id.values())


def listed_total():
    """Product count from the sitemap, a completeness check only. Shopify's product sitemaps hold
    1,000 URLs each except the last, so the total is read from just the index and the last file.
    Returns None if the layout isn't what we expect (then the check is skipped)."""
    try:
        s = http.session()
        index = s.get(f"{BASE}/sitemap.xml", timeout=30).text
        subs = [u.replace("&amp;", "&")
                for u in re.findall(r"<loc>([^<]*sitemap_products_[^<]*)</loc>", index)]
        if not subs:
            return None

        def count(url):
            return len(re.findall(r"<loc>[^<]*/products/[^<]*</loc>", s.get(url, timeout=30).text))

        first = count(subs[0])
        if first != 1000 and len(subs) > 1:
            return None
        last = count(subs[-1]) if len(subs) > 1 else first
        return (len(subs) - 1) * 1000 + last
    except Exception as e:  # noqa: BLE001 - only a sanity check
        print(f"WARN couldn't read the sitemap product count: {e}", flush=True)
        return None


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def money(m):
    """A positive USD price from a {amount} money object, or None. "0.00" means quote/hidden,
    not free, so it becomes None rather than 0."""
    return to_float((m or {}).get("amount")) or None


def condition_of(*texts):
    blob = " ".join(t for t in texts if t)
    for rx, label in CONDITIONS:
        if rx.search(blob):
            return label
    return ""


def build_rows(n, scraped_at):
    """One row per variant (product_id = the variant id; parent_id = the product id)."""
    title = re.sub(r"\s+", " ", n.get("title") or "").strip()
    brand = n.get("vendor") or ""
    category = n.get("productType") or ""
    handle = n.get("handle")
    tags = " ".join(n.get("tags") or [])
    product_id = n["id"].rsplit("/", 1)[-1]
    variants = (n.get("variants") or {})
    nodes = variants.get("nodes") or []
    if variants.get("pageInfo", {}).get("hasNextPage"):
        print(f"WARN product {product_id} has more than 50 variants; some are skipped", flush=True)
    image_url = (n.get("featuredImage") or {}).get("url") or None
    description = schema.plain_text(n.get("descriptionHtml"))
    first_word = title.split(" ", 1)[0] if title else ""
    multi = len(nodes) > 1
    rows = []
    for v in nodes:
        vid = v["id"].rsplit("/", 1)[-1]
        sku = (v.get("sku") or "").strip()
        option = v.get("title") or ""
        full_title = title if option in ("", "Default Title") else f"{title} - {option}"
        cap = CAPACITY_RE.search(full_title)
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": vid,                 # unique: one row per variant
            "parent_id": product_id,
            "variant_id": vid,
            "sku": sku,
            "part_number": sku or (first_word if re.search(r"\d", first_word) else ""),
            "title": full_title,
            "brand": brand,
            "category": category,
            "condition": condition_of(full_title, tags),
            "capacity": (cap.group(1) + cap.group(2).upper()) if cap else "",
            "price": money(v.get("price")),                      # $0.00 -> blank (quote)
            "compare_at_price": money(v.get("compareAtPrice")),  # $0.00 -> blank
            "available": bool(v.get("availableForSale")),
            "stock_qty": None,                 # not published by the public API
            "image_url": image_url,
            "description": description,
            "url": f"{BASE}/products/{handle}" + (f"?variant={vid}" if multi else ""),
        }))
    return rows


def scrape(limit, scraped_at):
    items = fetch_all(limit)
    rows = [row for n in items for row in build_rows(n, scraped_at)]
    listed = None if limit else listed_total()
    # Product-level completeness guard (rows are per variant; products are the right unit here)
    if not limit and listed and len(items) < 0.98 * listed:
        sys.exit(f"Only fetched {len(items)} of {listed} products ({len(items) / listed:.0%}); "
                 "not saving a partial snapshot")
    print(f"fetched {len(items)} unique products -> {len(rows)} variant rows "
          f"(sitemap lists {listed})", flush=True)
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
