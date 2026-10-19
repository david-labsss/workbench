"""Scrape atechmemory.com (Shopify, ~113,343 products): every run's full snapshot appended to
Postgres.

atechmemory.com has far more products than Shopify's 25,000-item deep-paging cap, so
/products.json and collection listings can't reach the whole catalogue. The Storefront GraphQL
API (/api/<version>/graphql.json, with the public storefront token the theme ships in its HTML)
has a top-level products search that takes a created_at range and sorts by CREATED_AT. We walk
each calendar month oldest first, 250 per page, a few months in parallel; when a listing reaches
the 25k cap we restart it from the last createdAt seen. Products are deduped by id. Modelled on
orangehardwares - but the store returns the same prices the Storefront API gives without a country
context (checked against /products.json), so there is no price adjustment to make.

One row per variant: product_id is the variant id, parent_id the product id. Price is the variant
price, old_price its compare-at price, in stock = available, brand = vendor, category =
product_type. Part number is the SKU; condition and capacity are read from the title. The store
doesn't publish stock counts (quantityAvailable is access-denied), so stock_qty stays blank.

The product count comes from the store's product sitemap (1,000 per file) only as a completeness
check, so a truncated crawl is never saved as the full catalogue.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.

Env vars:
  DATABASE_URL        Postgres URL for the scraper_writer role
  STOREFRONT_TOKEN    optional, defaults to the public token from the site's HTML
  MAX_PRODUCTS        optional, stop after about N products (test runs)
  DRY_RUN             optional, "1" = print sample rows, don't write anything
"""
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common import runner, schema

SITE = "atechmemory"
BASE = "https://atechmemory.com"
API = BASE + "/api/2025-07/graphql.json"
TOKEN = os.getenv("STOREFRONT_TOKEN") or "99cd14df7e3a9ad341c541f029040326"
PAGE = 250            # max per request
VARIANTS = 10         # variants fetched per product (these stores are ~1 variant each)
CAP = 25000           # Shopify won't page deeper than this per listing
WORKERS = 4           # low concurrency: be polite
PAGE_DELAY = 0.3      # small pause between pages within a window
USER_AGENT = "atechmemory-scraper/1.0 (price monitoring)"

CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b")   # upper-case only: "8Gb" is a link speed
CONDITIONS = [
    (re.compile(r"\brefurbished\b", re.I), "Refurbished"),
    (re.compile(r"\brecertified\b", re.I), "Recertified"),
    (re.compile(r"\bopen\s+box\b", re.I), "Open Box"),
    (re.compile(r"\bused\b", re.I), "Used"),
    (re.compile(r"\bnew\b", re.I), "New"),
]

_local = threading.local()

PRODUCT_FIELDS = """
  id title handle vendor productType availableForSale createdAt
  variants(first: 10) {
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


# --- fetching ----------------------------------------------------------------
def session():
    s = getattr(_local, "s", None)
    if s is None:
        s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT, "Content-Type": "application/json",
                          "X-Shopify-Storefront-Access-Token": TOKEN})
        retry = Retry(total=6, backoff_factor=2, status_forcelist=[429, 430, 500, 502, 503, 504],
                      allowed_methods=["POST"], respect_retry_after_header=True)
        s.mount("https://", HTTPAdapter(max_retries=retry))
        _local.s = s
    return s


def gql(query, variables=None):
    """Run a query; retry GraphQL-level throttling / internal errors, raise on anything else."""
    for attempt in range(6):
        r = session().post(API, json={"query": query, "variables": variables or {}}, timeout=60)
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
    """The year the oldest product was created, so the month windows start there."""
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
    gets there we start a new listing from the last createdAt seen (">=" so products sharing that
    second aren't skipped; the overlap is removed by product id)."""
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
        # Test runs: one window at a time until there are enough products
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
        s = requests.Session()
        s.headers["User-Agent"] = USER_AGENT
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


# --- mapping -----------------------------------------------------------------
def _to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def money(m):
    return _to_float((m or {}).get("amount"))


def condition_of(title):
    for rx, label in CONDITIONS:
        if rx.search(title or ""):
            return label
    return ""


def build_rows(n, scraped_at):
    """One row per variant of a product (product_id = variant id, parent_id = product id)."""
    title = re.sub(r"\s+", " ", n.get("title") or "").strip()
    cap = CAPACITY_RE.search(title)
    capacity = (cap.group(1) + cap.group(2)) if cap else ""
    condition = condition_of(title)
    first_word = title.split(" ", 1)[0] if title else ""
    product_id = n["id"].rsplit("/", 1)[-1]
    variants = (n.get("variants") or {})
    nodes = variants.get("nodes") or []
    if variants.get("pageInfo", {}).get("hasNextPage"):
        print(f"WARN product {product_id} has more than {VARIANTS} variants; some are skipped",
              flush=True)
    multi = len(nodes) > 1
    rows = []
    for v in nodes:
        vid = v["id"].rsplit("/", 1)[-1]
        sku = (v.get("sku") or "").strip()
        vtitle = v.get("title")
        price = money(v.get("price"))
        compare = money(v.get("compareAtPrice"))
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": vid,        # unique: one row per variant
            "parent_id": product_id,
            "variant_id": vid,
            "sku": sku,
            "part_number": sku or (first_word if re.search(r"\d", first_word) else ""),
            "title": title + ("" if vtitle in (None, "Default Title") else f" - {vtitle}"),
            "brand": n.get("vendor") or "",
            "category": n.get("productType") or "",
            "condition": condition,
            "capacity": capacity,
            "price": price if price is not None else "",
            "compare_at_price": compare if compare else "",
            "available": bool(v.get("availableForSale")),
            "stock_qty": None,   # not published by the store
            "url": f"{BASE}/products/{n.get('handle')}" + (f"?variant={vid}" if multi else ""),
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
