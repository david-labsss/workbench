"""Scrape techmikeny.com (headless Shopify / Hydrogen): every run's full snapshot appended to Postgres.

techmikeny.com runs a headless Shopify (Hydrogen) storefront. Its /products.json does return
JSON, but it stops at Shopify's 25,000-product depth cap, and a single Storefront products
listing is capped the same way, so neither reaches the whole catalogue (25k+ products - most of
them created in one bulk import on 2018-06-07). We read the TOKENLESS Storefront GraphQL API
(/api/<ver>/graphql.json): one products listing sorted by CREATED_AT, restarted from the last
createdAt seen whenever it nears the 25k cap (">=" so items sharing that second aren't skipped;
the overlap is removed by product id). The store also answers with the public storefront token
it ships, used as a default.

One row per variant (product_id = the variant id; parent_id = the product id). This catalogue is
single-variant in practice. Fields: title, vendor (brand), productType (category), variant SKU,
price, compare-at price, availableForSale. Part number = SKU, or the first word of the title
when that looks like a part number. Prices are the store's USD prices and match the site with no
market context. The public API carries no stock count, so stock_qty stays blank. Condition is
read from the title / tags when stated (this store sells refurbished and used server gear).

Env vars:
  DATABASE_URL       Postgres URL for the scraper_writer role
  STOREFRONT_TOKEN   optional, defaults to the public token the store ships
  MAX_PRODUCTS       optional, stop after about N products (test runs)
  DRY_RUN            optional, "1" = print sample rows, don't write anything
Run steps and changes: common/runner.py. Columns: common/schema.py.
"""
import os
import re
import time

from common import http, runner, schema

SITE = "techmikeny"
BASE = "https://www.techmikeny.com"
API = BASE + "/api/2025-07/graphql.json"
TOKEN = os.getenv("STOREFRONT_TOKEN") or "cfab573bce7fc3c244e6a1ac7f651050"
PAGE = 250           # Storefront max per request
CAP = 25000          # Shopify won't page a single listing deeper than this

PRODUCT_FIELDS = """
  id handle title vendor productType tags
  variants(first: 100) {
    nodes { id sku price { amount } compareAtPrice { amount } availableForSale title }
  }
"""
LIST_QUERY = """
query($first: Int!, $after: String, $query: String) {
  products(first: $first, after: $after, query: $query, sortKey: CREATED_AT) {
    pageInfo { hasNextPage endCursor }
    nodes { createdAt %s }
  }
}""" % PRODUCT_FIELDS

CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB|MB)\b", re.I)
CONDITIONS = [
    (re.compile(r"\brefurbished\b", re.I), "Refurbished"),
    (re.compile(r"\brecertified\b", re.I), "Recertified"),
    (re.compile(r"\bopen\s*box\b", re.I), "Open Box"),
    (re.compile(r"\bused\b", re.I), "Used"),
    (re.compile(r"\bnew\b", re.I), "New"),
]


def gql(query, variables):
    """Run a query; retry GraphQL-level throttling / internal errors, raise on anything else, so
    a failed request is never read as the end of the listing."""
    for attempt in range(6):
        r = http.session().post(
            API, json={"query": query, "variables": variables},
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


def fetch_all(limit):
    """Every product, oldest first. A single listing caps at 25k, so when we near the cap we
    start a new listing from the last createdAt seen; products are deduped by product id."""
    by_id, after, frm, depth = {}, None, None, 0
    while True:
        query = f"created_at:>='{frm}'" if frm else None
        page = gql(LIST_QUERY, {"first": PAGE, "after": after, "query": query})["products"]
        nodes = page["nodes"]
        for n in nodes:
            by_id[n["id"]] = n
        depth += len(nodes)
        if limit and len(by_id) >= limit:
            break
        if not page["pageInfo"]["hasNextPage"]:
            break
        if depth + PAGE > CAP:
            last = nodes[-1]["createdAt"] if nodes else frm
            if last == frm:
                print(f"WARN 25k+ products created at {last}; only 25k are reachable", flush=True)
                break
            frm, after, depth = last, None, 0
            print(f"  restart from {last} ({len(by_id)} unique so far)", flush=True)
            continue
        after = page["pageInfo"]["endCursor"]
    return list(by_id.values())


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def money(m):
    return to_float((m or {}).get("amount"))


def condition_of(*texts):
    blob = " ".join(t for t in texts if t)
    for rx, label in CONDITIONS:
        if rx.search(blob):
            return label
    return ""


def build_rows(n, scraped_at):
    title = re.sub(r"\s+", " ", n.get("title") or "").strip()
    brand = n.get("vendor") or ""
    category = n.get("productType") or ""
    handle = n.get("handle")
    tags = " ".join(n.get("tags") or [])
    variants = (n.get("variants") or {}).get("nodes") or []
    parent_id = n["id"].rsplit("/", 1)[-1]
    multi = len(variants) > 1
    rows = []
    for v in variants:
        vid = v["id"].rsplit("/", 1)[-1]
        sku = (v.get("sku") or "").strip()
        option = v.get("title") or ""
        vtitle = title if option in ("", "Default Title") else f"{title} - {option}"
        first_word = title.split(" ", 1)[0] if title else ""
        cap = CAPACITY_RE.search(vtitle)
        price = money(v.get("price")) or None
        compare = money(v.get("compareAtPrice"))
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": vid,                       # unique: one row per variant
            "parent_id": parent_id,
            "variant_id": vid,
            "sku": sku,
            "part_number": sku or (first_word if re.search(r"\d", first_word) else ""),
            "title": vtitle,
            "brand": brand,
            "category": category,
            "condition": condition_of(vtitle, tags),
            "capacity": (cap.group(1) + cap.group(2).upper()) if cap else "",
            "price": price if price is not None else "",
            "compare_at_price": compare if (compare and price and compare > price) else "",
            "available": "Yes" if v.get("availableForSale") else "No",
            "stock_qty": "",                         # not published by the public API
            "url": f"{BASE}/products/{handle}" + (f"?variant={vid}" if multi else ""),
        }))
    return rows


def scrape(limit, scraped_at):
    items = fetch_all(limit)
    rows = [row for n in items for row in build_rows(n, scraped_at)]
    if limit:
        rows = rows[:limit]
    # The listing is the whole catalogue once it is exhausted; no reliable total to compare against
    return rows, None


if __name__ == "__main__":
    runner.run(SITE, scrape)
