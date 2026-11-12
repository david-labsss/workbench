"""Scrape orangehardwares.com: every run's full snapshot appended to Postgres.

orangehardwares.com is Shopify with ~467k products. /products.json and every collection listing
stop at 25,000 items and ignore sort / filter params, so they can't reach the whole catalogue.
The sitemap lists only ~27k of them. Collection filters (price etc.) only cover a collection's
first 5,000 products, so they can't split it either.

What works: the Storefront GraphQL API (/api/<version>/graphql.json, with the public storefront
token the theme ships in its HTML) has a top-level products search that takes a created_at range
and sorts by CREATED_AT. We walk each calendar month oldest first, 250 per page, several months
in parallel; when a listing reaches the 25k cap we restart it from the last createdAt seen.
Products are deduped by id, and the total is checked against the collection's product count.

Fields: title, vendor (brand), product type (category), SKU, price, compare-at price,
availableForSale. Part number = SKU, or the first word of the title (titles start with it).
Prices are the US market's (@inContext(country: US)), which the store pages show: 5% above
the base price the API returns without a context.
The store doesn't publish stock counts to the API (quantityAvailable is access-denied), so
stock_qty stays blank and available is the in-stock flag.

common/runner.py checks the run, saves it into the "orangehardwares" table and logs the changes against
the previous full run into the shared prices table (common/changes.py), the same way as every
other site. Columns: common/schema.py.

Env vars:
  DATABASE_URL                 Postgres URL for the scraper_writer role
  STOREFRONT_TOKEN             optional, defaults to the public token from the site's HTML
  MAX_PRODUCTS                 optional, stop after about N products (test runs)
  DRY_RUN                      optional, "1" = print sample rows, don't write anything
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

SITE = "orangehardwares"
BASE = "https://www.orangehardwares.com"
API = BASE + "/api/2025-07/graphql.json"
TOKEN = os.getenv("STOREFRONT_TOKEN") or "47daa3a98ae9ca9a00b1d4e57a3970d0"
COLLECTION = "stock-in-stock"   # holds every product; only used for its product count
PAGE = 250                      # max per request
CAP = 25000                     # Shopify won't page deeper than this per listing
WORKERS = 6
FIRST_YEAR = 2023               # oldest product was created 2023-12-04
USER_AGENT = "orangehardwares-scraper/1.0 (price monitoring)"

COLUMNS = schema.PRODUCT_COLUMNS

PRODUCT_FIELDS = """
  id title handle vendor productType availableForSale featuredImage { url } descriptionHtml
  variants(first: 1) { nodes { sku price { amount } compareAtPrice { amount } availableForSale } }
"""
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB|MB)\b", re.I)
CONDITIONS = [
    (re.compile(r"\brefurbished\b", re.I), "Refurbished"),
    (re.compile(r"\brecertified\b", re.I), "Recertified"),
    (re.compile(r"\bopen\s+box\b", re.I), "Open Box"),
    (re.compile(r"\bused\b", re.I), "Used"),
    (re.compile(r"\bnew\b", re.I), "New"),
]

_local = threading.local()


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


def gql(query, variables):
    """Run a query; retry GraphQL-level throttling / internal errors, raise on anything else."""
    for attempt in range(6):
        r = session().post(API, json={"query": query, "variables": variables}, timeout=60)
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

LIST_QUERY = """
query($first: Int!, $after: String, $query: String!) @inContext(country: US) {
  products(first: $first, after: $after, query: $query, sortKey: CREATED_AT) {
    pageInfo { hasNextPage endCursor }
    nodes { createdAt %s }
  }
}""" % PRODUCT_FIELDS


def month_windows(now):
    """[start, end) pairs, one per calendar month from the store's first product to next month."""
    edges, y, m = [], FIRST_YEAR, 1
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
            continue
        after = page["pageInfo"]["endCursor"]
    if by_id:
        print(f"created {start[:7]}: {len(by_id)} products", flush=True)
    return list(by_id.values())


def fetch_all(limit=0):
    windows = month_windows(datetime.now(timezone.utc))
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
    """The collection's product count from the REST endpoint, to check the scrape is complete."""
    try:
        r = requests.get(f"{BASE}/collections/{COLLECTION}.json",
                         headers={"User-Agent": USER_AGENT}, timeout=30)
        r.raise_for_status()
        return int(r.json()["collection"]["products_count"])
    except Exception as e:  # noqa: BLE001 - only used as a sanity check
        print(f"WARN couldn't read the collection's product count: {e}", flush=True)
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


def map_item(n, scraped_at):
    title = re.sub(r"\s+", " ", n.get("title") or "").strip()
    v = ((n.get("variants") or {}).get("nodes") or [{}])[0]
    sku = (v.get("sku") or "").strip()
    first_word = title.split(" ", 1)[0] if title else ""
    cap = CAPACITY_RE.search(title)
    price = money(v.get("price")) or None
    compare = money(v.get("compareAtPrice"))
    available = v.get("availableForSale", n.get("availableForSale"))
    row = {
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": n["id"].rsplit("/", 1)[-1],
        "title": title,
        "brand": n.get("vendor") or "",
        "category": n.get("productType") or "",
        "condition": condition_of(title),
        "sku": sku,
        "part_number": sku or (first_word if re.search(r"\d", first_word) else ""),
        "capacity": (cap.group(1) + cap.group(2).upper()) if cap else "",
        "price": price if price is not None else "",
        "compare_at_price": compare if compare is not None else "",
        "available": "Yes" if available else "No",
        "stock_qty": "",   # not published: quantityAvailable is access-denied on this store
        "image_url": (n.get("featuredImage") or {}).get("url") or None,
        "description": schema.plain_text(n.get("descriptionHtml")),
        "url": f"{BASE}/products/{n.get('handle')}",
    }
    return schema.product_row(row)


# --- run ---------------------------------------------------------------------
def scrape(limit, scraped_at):
    """The whole catalogue as rows, and the collection's product count."""
    started = time.time()
    items = fetch_all(limit)
    total = listed_total()
    print(f"fetched {len(items)} unique products in {time.time() - started:.0f}s "
          f"(collection lists {total})", flush=True)
    if not items:
        sys.exit("No products returned by the Storefront API")
    return [map_item(n, scraped_at) for n in items], total


if __name__ == "__main__":
    runner.run(SITE, scrape)
