"""Scrape avendor.com (Shopify, ~56,000 products): every run's full snapshot appended to Postgres.

AVendor is an AV / IT integrator store (displays, video walls, Cisco, APC, HPE, ZPE, ... plus some
service SKUs); we scrape everything it lists. The catalogue is bigger than Shopify's 25,000-item
deep-paging cap, so /products.json and collection listings can't reach all of it. Like esaitech /
orangehardwares we use the Storefront GraphQL API (/api/<version>/graphql.json): a top-level
products search per calendar month of created_at, sorted by CREATED_AT, 250 per page, a few
months in parallel; when a listing reaches the 25k cap we restart it from the last createdAt
seen. Products are deduped by id.

The public storefront token is read fresh from the homepage every run (<script
id="shopify-features"> -> "accessToken"), so a rotated token doesn't break the scraper.
STOREFRONT_TOKEN overrides it.

One row per variant: product_id is the variant id, parent_id the product id.
  price             the variant price in USD (every product is priced; a $0 price would be saved
                    empty, never as 0). The store's request-a-quote app shows prices: its
                    hide-price rule is empty, so "RFQ"-tagged products keep their price.
  compare_at_price  the compare-at price, only when it is above the price
  available         availableForSale (the store doesn't publish stock counts: quantityAvailable is
                    access-denied, so stock_qty stays blank)
  sku               the manufacturer part number; part_number = sku
  brand             vendor (else a "Brand_X" tag)
  category          productType (else a "Type_X" tag)
  condition         the store has no condition field; only an explicit word in the title
                    (Refurbished / Recertified / Open Box / Used) is saved
  image_url         featuredImage; description = descriptionHtml as plain text
The product count comes from the store's product sitemap (1,000 per file) as a completeness
check, so a truncated crawl is never saved as the full catalogue.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.

Env vars:
  DATABASE_URL        Postgres URL for the scraper_writer role
  STOREFRONT_TOKEN    optional, overrides the token read from the homepage
  MAX_PRODUCTS        optional, stop after about N products (test runs)
  DRY_RUN             optional, "1" = print sample rows, don't write anything
"""
import json
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

SITE = "avendor"
BASE = "https://www.avendor.com"
API = BASE + "/api/2025-07/graphql.json"
KNOWN_TOKEN = "336dabca4049d698a91412b1f0fb9b3a"   # last token seen in the homepage (fallback)
PAGE = 250            # max per request
CAP = 25000           # Shopify won't page deeper than this per listing
WORKERS = 4           # low concurrency: be polite
PAGE_DELAY = 0.3      # small pause between pages within a window
USER_AGENT = "avendor-scraper/1.0 (price monitoring)"

CONDITIONS = [   # explicit words only: the store sells new stock and has no condition field
    (re.compile(r"\brefurbished\b", re.I), "Refurbished"),
    (re.compile(r"\brecertified\b", re.I), "Recertified"),
    (re.compile(r"\bopen\s+box\b", re.I), "Open Box"),
    (re.compile(r"\bused\b", re.I), "Used"),
]

_local = threading.local()
_token = KNOWN_TOKEN

VARIANT_FIELDS = "id sku title price { amount } compareAtPrice { amount } availableForSale"
PRODUCT_FIELDS = """
  id title handle vendor productType tags createdAt featuredImage { url } descriptionHtml
  variants(first: 100) { pageInfo { hasNextPage endCursor } nodes { %s } }
""" % VARIANT_FIELDS
LIST_QUERY = """
query($first: Int!, $after: String, $query: String!) {
  products(first: $first, after: $after, query: $query, sortKey: CREATED_AT) {
    pageInfo { hasNextPage endCursor }
    nodes { %s }
  }
}""" % PRODUCT_FIELDS
MORE_VARIANTS_QUERY = """
query($id: ID!, $after: String) {
  node(id: $id) { ... on Product {
    variants(first: 250, after: $after) { pageInfo { hasNextPage endCursor } nodes { %s } }
  } }
}""" % VARIANT_FIELDS
EARLIEST_QUERY = "{ products(first: 1, sortKey: CREATED_AT, reverse: false) { nodes { createdAt } } }"


# --- fetching ----------------------------------------------------------------
def storefront_token():
    """The public storefront token the theme ships in the homepage (STOREFRONT_TOKEN overrides)."""
    if os.getenv("STOREFRONT_TOKEN"):
        return os.environ["STOREFRONT_TOKEN"]
    try:
        r = requests.get(BASE + "/", headers={"User-Agent": USER_AGENT}, timeout=30)
        r.raise_for_status()
        m = re.search(r'<script[^>]*id="shopify-features"[^>]*>(.*?)</script>', r.text, re.S)
        token = json.loads(m.group(1)).get("accessToken") if m else None
        if token:
            return token
        print("WARN no storefront token in the homepage; using the last known one", flush=True)
    except Exception as e:  # noqa: BLE001 - the known token may still work; the API call will tell
        print(f"WARN couldn't read the storefront token from the homepage: {e}", flush=True)
    return KNOWN_TOKEN


def session():
    s = getattr(_local, "s", None)
    if s is None:
        s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT, "Content-Type": "application/json",
                          "X-Shopify-Storefront-Access-Token": _token})
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
        print(f"WARN couldn't read the earliest product date: {e}; starting at 2012", flush=True)
        return 2012


def month_windows(now, first_year):
    """[start, end) pairs, one per calendar month from first_year to next month."""
    edges, y, m = [], first_year, 1
    while True:
        edges.append(f"{y:04d}-{m:02d}-01T00:00:00Z")
        if (y, m) > (now.year, now.month):
            break
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return list(zip(edges, edges[1:]))


def complete_variants(n):
    """Fetch the rest of a product's variants when it has more than the first page."""
    variants = n["variants"]
    while variants["pageInfo"]["hasNextPage"]:
        more = gql(MORE_VARIANTS_QUERY, {"id": n["id"],
                                         "after": variants["pageInfo"]["endCursor"]})
        page = more["node"]["variants"]
        variants["nodes"].extend(page["nodes"])
        variants["pageInfo"] = page["pageInfo"]


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
            complete_variants(n)
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
    print(f"  unique products: {len(by_id)}", flush=True)
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


def tag_value(tags, prefix):
    """"Brand_NEC" -> "NEC" for prefix "Brand_" (first such tag)."""
    for t in tags:
        if t.startswith(prefix) and t[len(prefix):].strip():
            return t[len(prefix):].strip()
    return ""


def condition_of(title):
    for rx, label in CONDITIONS:
        if rx.search(title or ""):
            return label
    return ""


def build_rows(n, scraped_at):
    """One row per variant of a product (product_id = variant id, parent_id = product id)."""
    title = re.sub(r"\s+", " ", n.get("title") or "").strip()
    tags = n.get("tags") or []
    product_id = n["id"].rsplit("/", 1)[-1]
    nodes = n["variants"]["nodes"]
    multi = len(nodes) > 1
    image_url = (n.get("featuredImage") or {}).get("url") or None
    description = schema.plain_text(n.get("descriptionHtml"))
    rows = []
    for v in nodes:
        vid = v["id"].rsplit("/", 1)[-1]
        sku = (v.get("sku") or "").strip()
        vtitle = v.get("title")
        price = money(v.get("price")) or None          # a $0 price is not a price -> empty
        compare = money(v.get("compareAtPrice"))
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": vid,        # unique: one row per variant
            "parent_id": product_id,
            "variant_id": vid,
            "sku": sku,
            "part_number": sku,
            "title": title + ("" if vtitle in (None, "Default Title") else f" - {vtitle}"),
            "brand": n.get("vendor") or tag_value(tags, "Brand_"),
            "category": n.get("productType") or tag_value(tags, "Type_"),
            "condition": condition_of(title),
            "price": price,
            "compare_at_price": compare if price and compare and compare > price else None,
            "available": bool(v.get("availableForSale")),
            "stock_qty": None,   # not published by the store
            "image_url": image_url,
            "description": description,
            "url": f"{BASE}/products/{n.get('handle')}" + (f"?variant={vid}" if multi else ""),
        }))
    return rows


def scrape(limit, scraped_at):
    global _token
    _token = storefront_token()
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
