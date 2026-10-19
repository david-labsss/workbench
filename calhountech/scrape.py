"""Scrape calhountech.com (BigCommerce, Bloomington MN): every run's snapshot of its PRICED
products (~80k of ~325k) appended to Postgres.

Scope: priced items only. About 3/4 of the catalogue is quote-only ($0 price, "Unavailable"),
so only products with a real price are read. A product that turns quote-only therefore looks
like it left the listing (logged as out_of_stock by common/changes.py), and one that gets a
price shows up as new.

How: the storefront GraphQL API (/graphql) with the public token from the home page HTML (its
Origin must be https://calhountech.com, the token's cors claim). site.products has no price
filter, so products are read through site.search.searchProducts(filters: {price: {minPrice,
maxPrice}}). One search returns at most 10,000 results (its totalItems stops at 10,000), so
the price range is split into windows that each hold fewer: starting from SEEDS, any window
whose count reaches the cap is halved, and repeated until every window is under it. Both
price bounds are inclusive, so windows meet at half-cent boundaries (x.xx5) that no price in
whole cents can sit on: nothing is counted twice and nothing falls between windows. Each
window is paged by cursor (A_TO_Z, 50 per page), WORKERS windows at a time; products are
deduped by entity id (a price change mid-run can move one into another window). The number to
check the snapshot against is the sum of the window counts.

Mapping is common/bigcommerce.build_rows (one row per variant: SKU, MPN, brand, deepest
category, condition, price, retail price, in stock / availabilityV2, stock count when
tracked). On top of it: a $0 price is saved empty (quote-only), and for drives / memory (by
category) capacity is read from the title, with price_per_tb for drives only.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
import time
from concurrent.futures import ThreadPoolExecutor

from common import bigcommerce as bc, http, runner, schema

SITE = "calhountech"
BASE = "https://calhountech.com"
PAGE = 50                     # products per GraphQL page (the API's max)
CAP = 10000                   # a search's result cap; windows must stay under it
WORKERS = 3                   # windows paged at the same time
PAGE_DELAY = 0.3              # pause between pages of one window
COUNT_DELAY = 0.3             # pause between window-count probes
# First window boundaries, in cents (a window is lo <= price*100 < hi; None = no upper end).
# Halved further each run wherever a window holds CAP or more products.
SEEDS = [1, 2500, 6250, 10000, 12500, 15000, 17500, 20000, 25000, 50000, 100000, None]
DRIVE_RE = re.compile(r"\b(hard (disk )?drives?|disk drives?|hdds?|ssds?|solid state|flash)\b",
                      re.I)
MEMORY_RE = re.compile(r"\b(memory|ram)\b", re.I)
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s?(TB|GB)\b")          # upper-case units only

NODE = """
        entityId sku mpn name path condition
        brand { name }
        categories(first: 10) { edges { node { name path } } }
        prices { price { value } salePrice { value } retailPrice { value } basePrice { value } }
        availabilityV2 { status }
        customFields(first: 50) { edges { node { name value } } }
        variants(first: 250) {
          pageInfo { hasNextPage }
          edges { node {
            entityId sku mpn
            prices { price { value } retailPrice { value } basePrice { value } }
            inventory { isInStock aggregated { availableToSell } }
            options { edges { node { displayName values { edges { node { label } } } } } }
          } }
        }"""
SEARCH = """
query($min: Float, $max: Float, $first: Int!, $after: String) {
  site { search { searchProducts(filters: {price: {minPrice: $min, maxPrice: $max}}, sort: A_TO_Z) {
    products(first: $first, after: $after) {
      collectionInfo { totalItems }
      pageInfo { hasNextPage endCursor }
      edges { node { %s } }
    }
  } } }
}"""
PAGE_QUERY = SEARCH % NODE
COUNT_QUERY = SEARCH % "entityId"


def headers():
    return {"Authorization": f"Bearer {bc.token(BASE)}", "Origin": BASE,
            "Content-Type": "application/json"}


def bounds(lo, hi):
    """GraphQL price filter for the window lo <= price*100 < hi (cents), at half-cent bounds."""
    return {"min": (lo - 0.5) / 100, "max": None if hi is None else (hi - 0.5) / 100}


def search(hdrs, query, variables):
    """One searchProducts page. A failed page raises: it must never look like the end."""
    r = http.session().post(BASE + "/graphql", headers=hdrs, timeout=120,
                            json={"query": query, "variables": variables})
    r.raise_for_status()
    data = r.json()
    if data.get("errors"):
        raise RuntimeError(f"GraphQL error: {data['errors'][0].get('message')}")
    return data["data"]["site"]["search"]["searchProducts"]["products"]


def count(hdrs, lo, hi):
    page = search(hdrs, COUNT_QUERY, {**bounds(lo, hi), "first": 1, "after": None})
    return page["collectionInfo"]["totalItems"]


def find_windows(hdrs):
    """[(lo, hi, total)] covering every price from $0.01 up, each under CAP products."""
    todo = list(zip(SEEDS, SEEDS[1:]))
    done = []
    while todo:
        lo, hi = todo.pop(0)
        total = count(hdrs, lo, hi)
        time.sleep(COUNT_DELAY)
        if total < CAP:
            if total:
                done.append((lo, hi, total))
            continue
        if hi is None:
            mid = lo * 4
        elif hi - lo <= 1:
            raise RuntimeError(f"{total}+ products priced ${lo / 100:.2f}: a one-cent window "
                               "cannot be split under the search cap")
        else:
            mid = (lo + hi) // 2
        todo[:0] = [(lo, mid), (mid, hi)]
    return done


def fetch_window(hdrs, lo, hi, total, limit=0):
    """Every product node in one price window, paged by cursor."""
    nodes, after = [], None
    while True:
        page = search(hdrs, PAGE_QUERY, {**bounds(lo, hi), "first": PAGE, "after": after})
        nodes += [e["node"] for e in page["edges"]]
        if not page["pageInfo"]["hasNextPage"] or (limit and len(nodes) >= limit):
            break
        after = page["pageInfo"]["endCursor"]
        time.sleep(PAGE_DELAY)
    if total and not limit and len(nodes) < total:
        print(f"WARN window ${lo / 100:.2f}-{'' if hi is None else f'${hi / 100:.2f}'}: "
              f"{len(nodes)} of {total} products", flush=True)
    return nodes


def build_rows(node, scraped_at):
    rows = []
    category = bc.category_of(node)
    is_drive = bool(DRIVE_RE.search(category))
    cap = CAPACITY_RE.search(node.get("name") or "")
    for row in bc.build_rows(SITE, BASE, node, scraped_at):
        r = schema.as_dict(row)
        price = r["price"] if r["price"] not in ("", 0, 0.0) else None
        r["price"] = price
        if r["compare_at_price"] != "" and not price:
            r["compare_at_price"] = None
        r["price_per_tb"] = None
        if cap and not r["capacity"] and (is_drive or MEMORY_RE.search(category)):
            r["capacity"] = f"{cap.group(1)}{cap.group(2)}"
        if cap and is_drive and price:
            tb = float(cap.group(1)) / (1 if cap.group(2) == "TB" else 1000)
            r["price_per_tb"] = round(price / tb, 2) if tb else None
        rows.append(schema.product_row(r))
    return rows


def scrape(limit, scraped_at):
    hdrs = headers()
    if limit:                          # test run: the first priced products only
        nodes = fetch_window(hdrs, SEEDS[0], None, 0, limit)[:limit]
        listed = None
    else:
        windows = find_windows(hdrs)
        listed = sum(t for *_, t in windows)
        print(f"{len(windows)} price windows, {listed} priced products", flush=True)
        windows.sort(key=lambda w: -w[2])            # biggest first, so workers finish together
        by_id, done = {}, 0
        with ThreadPoolExecutor(WORKERS) as pool:
            jobs = [pool.submit(fetch_window, hdrs, lo, hi, total) for lo, hi, total in windows]
            for job in jobs:
                for n in job.result():
                    by_id[n["entityId"]] = n
                done += 1
                print(f"{done}/{len(windows)} windows, {len(by_id)} products", flush=True)
        nodes = list(by_id.values())
    for n in nodes:
        if n["variants"]["pageInfo"]["hasNextPage"]:
            print(f"WARN product {n['entityId']} has more than 250 variants; "
                  "only the first 250 are saved", flush=True)
    rows = [row for n in nodes for row in build_rows(n, scraped_at)]
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
