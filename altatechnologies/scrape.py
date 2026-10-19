"""Scrape altatechnologies.com (Shopify, ~29,000 products): every run's full snapshot appended to Postgres.

The catalogue is bigger than Shopify's 25,000-item deep-paging cap (/products.json returns 400 on
page 101), so we use the Storefront GraphQL API (/api/<version>/graphql.json) like esaitech /
orangehardwares: a top-level products search per calendar month of created_at, sorted by
CREATED_AT, 250 per page, a few months in parallel; when a listing reaches the 25k cap we restart
it from the last createdAt seen. Products are deduped by id.

The public storefront token is read fresh from the homepage every run (<script
id="shopify-features"> -> "accessToken"), so a rotated token doesn't break the scraper.
STOREFRONT_TOKEN overrides it.

One row per variant: product_id is the variant id, parent_id the product id. Most products have
one variant; the CTO configurator option products ("component" tag) have many.
  price      the variant price, in USD. About a third of the catalogue is "Request a Quote" with
             a $0.00 price: those (and any other $0 price) are saved empty, never as 0.
  available  availableForSale (quote-only parts are mostly not for sale)
  sku        the manufacturer part number; part_number = sku, or "Part Number: X" from the
             description when the variant has no SKU
  brand      vendor
  category   productType is empty, so it comes from the description's "buy-back options for
             your <Servers> and <Hard Drives & SSDs> equipment" (saved "Servers > Hard Drives &
             SSDs"), else from the main tags (UsedServers, DataStorage, Networking, ...)
  condition  "Condition: Used" / "New Open Box" / ... in the description, else from the title
  capacity   drives / memory only: from the title (upper-case GB/TB only: "8Gb" is a link
             speed), else the description's "Storage Capacity" / "Total Capacity"
  interface, form_factor, spindle_speed  from the description's technical details (drives/memory)
The store doesn't publish stock counts (quantityAvailable is access-denied), so stock_qty stays
blank.

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

SITE = "altatechnologies"
BASE = "https://altatechnologies.com"
API = BASE + "/api/2025-07/graphql.json"
KNOWN_TOKEN = "2fe4f78465db63ef21102af38011a88a"   # last token seen in the homepage (fallback)
PAGE = 250            # max per request
CAP = 25000           # Shopify won't page deeper than this per listing
WORKERS = 4           # low concurrency: be polite
PAGE_DELAY = 0.3      # small pause between pages within a window
USER_AGENT = "altatechnologies-scraper/1.0 (price monitoring)"

CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b")   # upper-case only: "8Gb" is a link speed
DESC_CAPACITY_RE = re.compile(r"(?:Storage|Total) Capacity:\s*(\d+(?:\.\d+)?)\s*(TB|GB)\b", re.I)
CONDITION_RE = re.compile(
    r"Condition:\s*(New Open Box|New No Box|New Factory Sealed|New Sealed|Factory Sealed|"
    r"New Bulk|Brand New|Open Box|Like New|Refurbished|Recertified|Used|New)(?![a-z])", re.I)
CONDITIONS = [   # title fallback
    (re.compile(r"\brefurbished\b", re.I), "Refurbished"),
    (re.compile(r"\brecertified\b", re.I), "Recertified"),
    (re.compile(r"\bopen\s+box\b", re.I), "New Open Box"),
    (re.compile(r"\bused\b", re.I), "Used"),
    (re.compile(r"\bnew\b", re.I), "New"),
]
EQUIPMENT_RE = re.compile(r"options for your ([^.:;!?]{3,60}?) equipment\b")
TOP_LEVELS = {"Servers", "Networking", "Storage", "Workstations", "Power", "Components"}
TAG_CATEGORIES = [   # first matching tag wins
    ("Networking", "Networking"), ("DataStorage", "Storage"), ("UsedServers", "Servers"),
    ("server", "Servers"), ("component", "Server Components"),
]
SIZED_RE = re.compile(r"Drive|SSD|Memory", re.I)   # categories where capacity makes sense
PART_NUMBER_RE = re.compile(r"Part Number:\s*([A-Za-z0-9][\w./#+-]*)")
FORM_FACTOR_RE = re.compile(
    r"Form Factor:\s*(\d\.\d+)\s*in(?![a-z])|"
    r"Form Factor:\s*((?:VLP\s+)?(?:RDIMM|LRDIMM|UDIMM|SO-DIMM|SODIMM|DIMM)|M\.2|U\.2)")
INTERFACE_RE = re.compile(r"Interface:\s*(SAS|SATA|NVMe|SCSI|Fibre Channel|PCIe|U\.2|M\.2)(?![a-z])")
SPINDLE_RE = re.compile(r"Rotation Speed:\s*(\d[\d,]*)\s*RPM")

_local = threading.local()
_token = KNOWN_TOKEN

VARIANT_FIELDS = "id sku title price { amount } compareAtPrice { amount } availableForSale"
PRODUCT_FIELDS = """
  id title handle vendor productType tags description createdAt
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


def condition_of(description, title):
    m = CONDITION_RE.search(description or "")
    if m:
        return " ".join(w.capitalize() for w in m.group(1).split())
    for rx, label in CONDITIONS:
        if rx.search(title or ""):
            return label
    return ""


def category_of(description, tags):
    m = EQUIPMENT_RE.search(description or "")
    if m:
        top, _, sub = m.group(1).partition(" and ")
        if sub and top in TOP_LEVELS:
            return f"{top} > {sub}"
    # Other phrasings ("your excess Cisco networking equipment") are vaguer than the tags
    for tag, label in TAG_CATEGORIES:
        if tag in tags:
            return label
    return m.group(1) if m else ""


def specs_of(description, title, category):
    # Capacity only for drives / memory: elsewhere "8GB" is often a link speed or cache size
    sized = SIZED_RE.search(category) or DESC_CAPACITY_RE.search(description)
    cap = sized and (CAPACITY_RE.search(title) or DESC_CAPACITY_RE.search(description))
    ff = FORM_FACTOR_RE.search(description)
    iface = INTERFACE_RE.search(description)
    rpm = SPINDLE_RE.search(description)
    return {
        "capacity": (cap.group(1) + cap.group(2).upper()) if cap else "",
        "form_factor": (f'{ff.group(1)}"' if ff.group(1) else ff.group(2)) if ff else "",
        "interface": iface.group(1) if iface else "",
        "spindle_speed": f"{rpm.group(1).replace(',', '')} RPM" if rpm else "",
    }


def build_rows(n, scraped_at):
    """One row per variant of a product (product_id = variant id, parent_id = product id)."""
    title = re.sub(r"\s+", " ", n.get("title") or "").strip()
    description = n.get("description") or ""
    tags = n.get("tags") or []
    condition = condition_of(description, title)
    category = n.get("productType") or category_of(description, tags)
    specs = specs_of(description, title, category)
    pn = PART_NUMBER_RE.search(description)
    product_id = n["id"].rsplit("/", 1)[-1]
    nodes = n["variants"]["nodes"]
    multi = len(nodes) > 1
    rows = []
    for v in nodes:
        vid = v["id"].rsplit("/", 1)[-1]
        sku = (v.get("sku") or "").strip()
        vtitle = v.get("title")
        price = money(v.get("price")) or None          # $0.00 = Request a Quote -> empty
        compare = money(v.get("compareAtPrice"))
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": vid,        # unique: one row per variant
            "parent_id": product_id,
            "variant_id": vid,
            "sku": sku,
            "part_number": sku or (pn.group(1) if pn and not multi else ""),
            "title": title + ("" if vtitle in (None, "Default Title") else f" - {vtitle}"),
            "brand": n.get("vendor") or "",
            "category": category,
            "condition": condition,
            "price": price,
            "compare_at_price": compare if price and compare and compare > price else None,
            "available": bool(v.get("availableForSale")),
            "stock_qty": None,   # not published by the store
            "url": f"{BASE}/products/{n.get('handle')}" + (f"?variant={vid}" if multi else ""),
            **specs,
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
