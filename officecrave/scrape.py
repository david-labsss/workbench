"""Scrape officecrave.com's server-relevant products (~12.6k of its ~112k office/janitorial
catalogue): every run's full snapshot appended to Postgres.

The store's search box is Algolia. Its public search-only key ships in the site's own
/assets/js/algolia/algolia-autocomplete.js (re-read every run in case it rotates; the constants
below are the fallback), so the data comes straight from Algolia's API - the store itself only
serves that one JS file. Only these categories are scraped (hierarchicalCategories):
  lvl2  Electronics > Computer Hardware & Accessories > Processors (CPUs) / Computer Memory (RAM)
        / Computer Expansion Cards & Controllers / Power Supplies & Modules
  lvl2  Electronics > Data Storage Supplies > Computer Drives / Network Attached Storage (NAS)
  lvl1  Electronics > Networking (cables, racks, transceivers, switches, routers, ...)
They are fetched as ONE query (an OR of the categories), so a product in two of them counts once.

Algolia pages at most 1,000 hits per query (paginationLimitedTo), so that query is split into
price ranges (numericFilters on price), halved until every range has < 1,000 hits, and each
range is read in one page of up to 1,000. Counts are only exact when the query asks for a facet
(otherwise Algolia estimates nbHits), so every count query asks for one. A range whose page
returns fewer hits than its count raises, so a bad fetch never looks like the end of the list.
A "NOT price in range" check catches any product without a numeric price.

Fields: objectID (the store's product id, also the JSON-LD sku) -> product_id and sku; model
(the manufacturer part number, shown as "Model#") -> part_number and model_number; mpn (the
store's own item code, e.g. NGRGS105NA, shown as "SKU:" on the page) is kept in other_specs;
manufacturer -> brand; deepest category -> category; price (USD, what the customer pays);
inStock -> available; quantity (a real stock count in the index) -> stock_qty.
compare_at_price: old_price when higher than price, else map_price when higher (the page shows
the MAP price struck through with "Lower Price In Cart"). price is None for login-only items and
for 0 / negative prices (a few out-of-stock items carry negative placeholder prices).
No condition field (an office supplier selling new stock): left blank. Capacity is read from the
title for drives and memory (titles are lower-cased, e.g. "18tb", so "6gb/s" speeds are skipped).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import html
import re
import time

from common import http, runner, schema

SITE = "officecrave"
BASE = "https://www.officecrave.com"
KEYS_JS = BASE + "/assets/js/algolia/algolia-autocomplete.js"
APP_ID = "7UOKBM14LO"                          # fallback, from the JS file (2026-10-19)
API_KEY = "df082681b9b735cae4c072b04dc516b5"   # public search-only key, same source
INDEX = "prod_ofc"
CAP = 1000          # Algolia won't return more than this per query (paginationLimitedTo)
PAGE_DELAY = 0.2    # polite pause between requests

LVL2 = [
    "Electronics > Computer Hardware & Accessories > Processors (CPUs)",
    "Electronics > Computer Hardware & Accessories > Computer Memory (RAM)",
    "Electronics > Data Storage Supplies > Computer Drives",
    "Electronics > Computer Hardware & Accessories > Computer Expansion Cards & Controllers",
    "Electronics > Computer Hardware & Accessories > Power Supplies & Modules",
    "Electronics > Data Storage Supplies > Network Attached Storage (NAS)",
]
LVL1 = ["Electronics > Networking"]
# One OR group: every product in any of these categories
CATEGORY_FILTER = [[f"hierarchicalCategories.lvl2:{c}" for c in LVL2]
                   + [f"hierarchicalCategories.lvl1:{c}" for c in LVL1]]
CAPACITY_CATEGORIES = {"Computer Memory (RAM)", "Computer Drives", "Network Attached Storage (NAS)"}

RETRIEVE = ["objectID", "name", "model", "clean_model", "mpn", "manufacturer", "price",
            "old_price", "map_price", "login_only", "inStock", "quantity", "url",
            "categories", "hierarchicalCategories"]
CAPACITY_UPPER_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b")
# Titles are lower-cased ("1tb", "16gb"); skip link speeds like "6gb/s" / "12gbps"
CAPACITY_ANY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(tb|gb)\b(?!\s*/\s*s)", re.I)


# --- fetching ----------------------------------------------------------------
def algolia_keys():
    """(app id, search key) from the store's own autocomplete script; the constants if that
    fails or the script changed shape (a wrong key then fails loudly at the first query)."""
    try:
        r = http.session().get(KEYS_JS, timeout=30)
        r.raise_for_status()
        app = re.search(r"appId\s*=\s*['\"]([A-Z0-9]{6,})['\"]", r.text)
        key = re.search(r"apiKey\s*=\s*['\"]([0-9a-f]{32})['\"]", r.text)
        if app and key:
            return app.group(1), key.group(1)
        print("WARN Algolia keys not found in the autocomplete script; using the saved ones",
              flush=True)
    except Exception as e:  # noqa: BLE001 - fall back to the saved public key
        print(f"WARN couldn't read the autocomplete script ({e}); using the saved keys", flush=True)
    return APP_ID, API_KEY


class Algolia:
    def __init__(self, app_id, api_key):
        self.headers = {"X-Algolia-Application-Id": app_id, "X-Algolia-API-Key": api_key,
                        "Content-Type": "application/json"}
        self.hosts = [f"https://{app_id}-dsn.algolia.net", f"https://{app_id}-1.algolianet.com"]

    def query(self, body):
        body = dict(body, query="", analytics=False, clickAnalytics=False,
                    attributesToHighlight=[], attributesToSnippet=[],
                    facetFilters=CATEGORY_FILTER)
        error = None
        for host in self.hosts:   # the DSN host first, the fallback host if it is down
            try:
                r = http.session().post(f"{host}/1/indexes/{INDEX}/query", json=body,
                                        headers=self.headers, timeout=60)
                r.raise_for_status()
                time.sleep(PAGE_DELAY)
                return r.json()
            except Exception as e:  # noqa: BLE001 - try the next host, then raise
                error = e
        raise RuntimeError(f"Algolia query failed on every host: {error}")

    def count(self, numeric=None, filters=None):
        """Exact hit count (asking for a facet makes Algolia count exhaustively)."""
        body = {"hitsPerPage": 0, "facets": ["inStock", "price"]}
        if numeric:
            body["numericFilters"] = numeric
        if filters:
            body["filters"] = filters
        data = self.query(body)
        if not (data.get("exhaustive") or {}).get("nbHits", data.get("exhaustiveNbHits", True)):
            raise RuntimeError(f"Algolia count is an estimate for {numeric or filters}")
        return data["nbHits"], (data.get("facets_stats") or {}).get("price")

    def fetch(self, numeric):
        """Every hit of one price range (< CAP), in a single page. Raises when the page holds
        fewer hits than the range's exact count (or the range grew past the cap since planning)."""
        data = self.query({"hitsPerPage": CAP, "page": 0, "numericFilters": numeric,
                           "facets": ["inStock"], "attributesToRetrieve": RETRIEVE})
        hits = data.get("hits") or []
        if data["nbHits"] >= CAP or len(hits) < data["nbHits"]:
            raise RuntimeError(f"price range {numeric}: got {len(hits)} of {data['nbHits']} hits")
        return hits


def price_filter(lo, hi):
    return [f"price>={lo}", f"price<{hi}"]


def plan(api, lo, hi, found=None):
    """[(lo, hi, count)] price ranges [lo, hi) with fewer than CAP hits each."""
    if found is None:
        found, _ = api.count(price_filter(lo, hi))
    if found < CAP:
        return [(lo, hi, found)] if found else []
    if hi - lo < 0.02:
        # can't split one price further; never silently keep only the first 1,000
        raise RuntimeError(f"{found} products at price {lo}; more than one query can return")
    mid = round((lo + hi) / 2, 2)
    return plan(api, lo, mid) + plan(api, mid, hi)


# --- mapping -----------------------------------------------------------------
def clean(v):
    return re.sub(r"\s+", " ", html.unescape(str(v or ""))).strip()


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def category_of(hit):
    """The deepest category of the hit that we scrape for (leaf name)."""
    levels = hit.get("hierarchicalCategories") or {}
    lvl2 = levels.get("lvl2") or []
    lvl1 = levels.get("lvl1") or []
    lvl2 = [lvl2] if isinstance(lvl2, str) else lvl2
    lvl1 = [lvl1] if isinstance(lvl1, str) else lvl1
    for path in [p for p in lvl2 if p in LVL2] + [p for p in lvl2 if p.startswith(LVL1[0] + " >")]:
        return clean(path.split(" > ")[-1])
    for path in lvl1:
        if path in LVL1:
            return clean(path.split(" > ")[-1])
    cats = hit.get("categories") or []
    return clean(cats[-1]) if cats else ""


def capacity_of(title, category):
    if category not in CAPACITY_CATEGORIES:
        return ""
    m = CAPACITY_UPPER_RE.search(title) or CAPACITY_ANY_RE.search(title)
    return (m.group(1) + m.group(2).upper()) if m else ""


def in_stock(v):
    if isinstance(v, list):
        return any(in_stock(x) for x in v)
    return v is True or str(v).strip().lower() in ("true", "1", "yes")


def build_row(hit, scraped_at):
    title = clean(hit.get("name"))
    category = category_of(hit)
    price = to_float(hit.get("price"))
    if hit.get("login_only") in (1, True, "1", "true") or price is None or price <= 0:
        price = None   # login-only, $0 or a negative placeholder: no public price
    old = to_float(hit.get("old_price"))
    map_price = to_float(hit.get("map_price"))
    compare = None
    if price:
        if old and old > price:
            compare = old
        elif map_price and map_price > price:
            compare = map_price   # shown struck through on the page ("Lower Price In Cart")
    qty = hit.get("quantity")
    stock_qty = int(qty) if isinstance(qty, (int, float)) and not isinstance(qty, bool) \
        and qty >= 0 else None
    part = clean(hit.get("model")) or clean(hit.get("clean_model"))
    item_code = clean(hit.get("mpn"))
    url = clean(hit.get("url"))
    if url and not url.startswith("http"):
        url = f"{BASE}/{url.lstrip('/')}"
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": str(hit["objectID"]),
        "sku": str(hit["objectID"]),
        "part_number": part,
        "model_number": part,
        "title": title,
        "brand": clean(hit.get("manufacturer")),
        "category": category,
        "condition": "",
        "capacity": capacity_of(title, category),
        "other_specs": f"Item code: {item_code}" if item_code else "",
        "price": price,
        "compare_at_price": compare,
        "discount_pct": round((compare - price) / compare * 100, 1) if compare else None,
        "available": in_stock(hit.get("inStock")),
        "stock_qty": stock_qty,
        "url": url,
    })


def scrape(limit, scraped_at):
    api = Algolia(*algolia_keys())
    listed, stats = api.count()
    if not listed:
        raise RuntimeError("Algolia returned no products for the server categories")
    lo, hi = int(stats["min"]) - 2, int(stats["max"]) + 2   # whole numbers so the filters are exact
    if limit:
        ranges = [(lo, hi, min(listed, CAP))]
    else:
        missing, _ = api.count(filters=f"NOT price:{lo} TO {hi}")
        if missing:
            raise RuntimeError(f"{missing} products have no price in [{lo}, {hi}); "
                               "price ranges would miss them")
        ranges = plan(api, lo, hi, listed)
        planned = sum(n for _, _, n in ranges)
        print(f"{len(ranges)} price ranges, {planned} products (categories list {listed})",
              flush=True)

    by_id = {}
    for r_lo, r_hi, _ in ranges:
        if limit:
            data = api.query({"hitsPerPage": min(limit, CAP), "page": 0,
                              "attributesToRetrieve": RETRIEVE})
            hits = data.get("hits") or []
        else:
            hits = api.fetch(price_filter(r_lo, r_hi))
        for h in hits:
            by_id[str(h["objectID"])] = h
    hits = list(by_id.values())[:limit] if limit else list(by_id.values())
    return [build_row(h, scraped_at) for h in hits], listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
