"""Scrape ipsecuritydepot.com (BigCommerce, ~181k products, mostly security / surveillance gear):
only its server, storage and networking categories, every run's full snapshot appended to Postgres.

The storefront GraphQL API (/graphql) is open to the public token in the home page HTML (see
common/bigcommerce.py; the token's cors origin is the www host). Instead of paging the whole
store, the chosen categories are read with site.search.searchProducts(filters: {categoryEntityIds,
searchSubCategories: true, hideOutOfStock: false}), 50 products a page by cursor. One search never
returns more than 10,000 results (totalItems then says 10000), so a category that reaches that is
split into price ranges (filters.price {minPrice, maxPrice}), bisected until each range is under
the cap. Splitting by subcategory does not work here: the store files every product in the parent
category too. Price ranges were checked to add up exactly to the unsplit totals (categories 694
and 697, 2026-10-19); a range boundary shared by two ranges can only overlap, never leave a gap,
and products are deduped by entity id (also across categories, which overlap). Every search must
return as many products as its own totalItems, or the run fails. The full node with variants
costs ~7,800 of the 10,000 query complexity, so one search page runs per request (3 at a time).

Chosen categories (store category id, path, searchProducts total incl. subcategories, 2026-10-19):
  692  Infrastructure > Servers (rack/tower/edge servers, storage systems, server parts)   1,693
  694  Infrastructure > Server Drives & Storage (HDD, SSD, NVMe, RAID/HBA, tape)           2,518
  696  Infrastructure > Server Memory                                                    2,519
  695  Infrastructure > Server Processors                                                1,041
  697  Infrastructure > Server Components & Options (NICs, risers, PSUs, fans, rails)     4,426
  699  Infrastructure > AI & Data Center Compute (GPU servers, GPUs, DPUs/SmartNICs)        817
  208  Infrastructure > Network Switches                                                 3,297
  744  Infrastructure > Structured Cabling > Transceivers, DACs & AOCs                 ~13,500
  592  Infrastructure > Network Attached Storage                                           146
  896  Power & UPS > Power Supplies > Server & Redundant Power Supplies                     72
  1209 Power & UPS > Power Supplies > Rack Power Supplies > Switch, Server & Storage PSU    129
  1293 Accessories > Connectivity & Media > Storage & Memory Cards > Internal SSDs & HDDs   130
  353  Video & Storage > NVR Hard Drives & Storage                                          95
Left out: cameras, access control, intercoms, cabling/racks, workstations, laptops, KVMs, routers,
wireless, UPS/PDUs and the other non-server categories. (categoryTree productCount runs ~2x the
search totals; the search totals match category.products totals, so they are the real counts.)

Mapping: common/bigcommerce.build_rows (one row per variant, product_id = variant id, parent_id =
product id), after cleaning the store's custom fields (names come as "_Name", "-Name", "~Name"
and "Name" copies: the first of "_Name"/"Name" is kept; "~" / "-" copies, "Overview" text and the
all-lowercase internal flags such as fbt-product, bundle_items, backorder, __quote_only are dropped;
"_Product Type" is saved as "Product" in other_specs instead of replacing the store category).
The store category is the deepest one the product sits in. availabilityV2:
"Available" (ships same day / in 2-3 weeks) = in stock when the variant is in stock;
"Unavailable" ("Call for availability", "Call for a quote", incl. the __quote_only products) and
any "No Longer Available" product = available False, row kept. Quote / call-for-availability items
still show their price on the product page (checked 2026-10-19), so the price is kept;
a $0 price is saved empty. The store does not expose stock counts (aggregated is null), so
stock_qty stays empty. listed is None: the categories overlap, so no exact unique total exists;
the per-search totalItems checks above guard completeness instead.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import time
from concurrent.futures import ThreadPoolExecutor

from common import bigcommerce, http, runner, schema

SITE = "ipsecuritydepot"
BASE = "https://www.ipsecuritydepot.com"
CATEGORIES = [692, 694, 696, 695, 697, 699, 208, 744, 592, 896, 1209, 1293, 353]
CAP = 10000            # searchProducts never returns more results than this
PAGE = 50
WORKERS = 3
DELAY = 0.4            # between pages of one search
PRICE_BOUNDS = [0, 50, 100, 200, 500, 1000, 2000, 5000]   # first split of a capped category
DROP_FIELDS = {"overview"}                 # long marketing text
RENAME_FIELDS = {"product type": "Product"}   # keep in other_specs, not the category column

NODE = bigcommerce.QUERY[bigcommerce.QUERY.index("entityId sku mpn name path condition"):
                         bigcommerce.QUERY.rindex("} }\n    }\n  }\n}")]
SEARCH_QUERY = """query($cats: [Int!], $price: PriceSearchFilterInput, $first: Int!, $after: String) {
  site { search { searchProducts(filters: {categoryEntityIds: $cats, searchSubCategories: true,
                                           hideOutOfStock: false, price: $price}) {
    products(first: $first, after: $after) {
      collectionInfo { totalItems }
      pageInfo { hasNextPage endCursor }
      edges { node { availabilityV2 { status description } %s } }
    }
  } } }
}""" % NODE


class Api:
    def __init__(self):
        self.headers = {"Authorization": f"Bearer {bigcommerce.token(BASE)}", "Origin": BASE,
                        "Content-Type": "application/json"}

    def query(self, query, variables=None):
        """A failed request or GraphQL error raises: it must never look like an empty page."""
        r = http.session().post(BASE + "/graphql", headers=self.headers, timeout=90,
                                json={"query": query, "variables": variables or {}})
        r.raise_for_status()
        data = r.json()
        if data.get("errors"):
            raise RuntimeError(f"GraphQL error: {data['errors'][0].get('message')}")
        return data["data"]

    def search(self, cat, lo=None, hi=None, first=PAGE, after=None):
        price = None
        if lo is not None:
            price = {"minPrice": lo} if hi is None else {"minPrice": lo, "maxPrice": hi}
        page = self.query(SEARCH_QUERY, {"cats": [cat], "price": price, "first": first,
                                         "after": after})
        return page["site"]["search"]["searchProducts"]["products"]


def plan(api, cat, lo=None, hi=None):
    """[(category id, min price, max price, totalItems)] searches that together cover `cat`,
    each under the 10,000 cap."""
    total = api.search(cat, lo, hi, first=1)["collectionInfo"]["totalItems"]
    if total < CAP:
        return [(cat, lo, hi, total)] if total else []
    if lo is None:                                  # first split: fixed price bands
        bounds = PRICE_BOUNDS + [None]
        ranges = list(zip(bounds[:-1], bounds[1:]))
    elif hi is None:
        ranges = [(lo, lo * 4), (lo * 4, None)]
    elif hi - lo < 0.02:
        raise RuntimeError(f"category {cat}: {total}+ products priced {lo}-{hi}, can't split")
    else:
        mid = round((lo + hi) / 2, 2)
        ranges = [(lo, mid), (mid, hi)]
    searches = []
    for a, b in ranges:
        searches += plan(api, cat, a, b)
    return searches


def fetch_search(api, cat, lo, hi, total, limit=0):
    """Every product node of one search, paged by cursor; fewer than its own total raises."""
    nodes, after = {}, None
    while True:
        page = api.search(cat, lo, hi, after=after)
        for e in page["edges"]:
            nodes[e["node"]["entityId"]] = e["node"]
        if limit and len(nodes) >= limit:
            return list(nodes.values())
        if not page["pageInfo"]["hasNextPage"]:
            break
        after = page["pageInfo"]["endCursor"]
        time.sleep(DELAY)
    total = page["collectionInfo"]["totalItems"]          # the latest count, if it moved
    if len(nodes) < total * 0.99:
        raise RuntimeError(f"category {cat} (price {lo}-{hi}): got {len(nodes)} of {total}")
    return list(nodes.values())


def clean_node(node):
    """The store's custom fields and availability text made fit for bigcommerce.build_rows."""
    fields, seen = [], set()
    for f in bigcommerce.edges(node.get("customFields")):
        raw = (f.get("name") or "").strip()
        name = raw.lstrip("_").strip()
        # "~Name" / "-Name" are display copies of "_Name"; all-lowercase names are internal
        # flags (fbt-product, bundle_items, backorder, __quote_only, identifier_exists, ...)
        if (not name or raw[0] in "~-" or name == name.lower() or name.lower() in DROP_FIELDS
                or name.lower() in seen):
            continue
        seen.add(name.lower())
        fields.append({"node": {"name": RENAME_FIELDS.get(name.lower(), name),
                                "value": f.get("value")}})
    avail = dict(node.get("availabilityV2") or {})
    if "no longer available" in (avail.get("description") or "").lower():
        avail["status"] = "Unavailable"
    return dict(node, customFields={"edges": fields}, availabilityV2=avail)


def scrape(limit, scraped_at):
    api = Api()
    searches = []
    for cat in CATEGORIES:
        searches += plan(api, cat)
    print(f"{len(searches)} searches, {sum(s[3] for s in searches)} products before dedupe",
          flush=True)
    if limit:
        searches = searches[:1]
    by_id = {}
    with ThreadPoolExecutor(WORKERS) as pool:
        jobs = [pool.submit(fetch_search, api, *s, limit) for s in searches]
        for (cat, lo, hi, total), job in zip(searches, jobs):
            for n in job.result():
                by_id.setdefault(n["entityId"], n)
            price = f" price {lo}-{hi}" if lo is not None else ""
            print(f"category {cat}{price}: {total} products, {len(by_id)} unique so far",
                  flush=True)
    nodes = list(by_id.values())[:limit] if limit else list(by_id.values())
    rows = [row for n in nodes
            for row in bigcommerce.build_rows(SITE, BASE, clean_node(n), scraped_at)]
    price = schema.PRODUCT_COLUMNS.index("price")
    for row in rows:
        if row[price] == 0:
            row[price] = ""                # $0 = no real price
    return rows, None


if __name__ == "__main__":
    runner.run(SITE, scrape)
