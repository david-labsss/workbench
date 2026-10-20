"""Scrape pood.datafox.ee (Estonian retailer, custom Nuxt 3 storefront on a Supabase backend):
every run's full snapshot of the server / storage / networking categories, appended to Postgres.

The storefront is a Nuxt 3 app. Each category page (/en/<category-slug>?page=N) is server-rendered
and embeds the catalogue API's response inside the page's __NUXT_DATA__ payload (a devalue-encoded
flat array). That response is a {products, total, page, pageSize, totalPages} object whose products
carry everything we need: id, sku (= manufacturer part number), name, brand, price and originalPrice
(euros, VAT included), inStock, stockCount, ean, mpn, the category slug and the product slug. The
products are read straight from that payload -- no private key or headless browser -- 24 per page,
paging 1..totalPages for each category and de-duplicating by id across categories (one product can
sit in several). The whole catalogue is very large (networking alone is 26k products), so only the
server / storage / networking categories are scraped; MAX_PRODUCTS caps test runs.

Prices are the euro amounts the site shows (Estonia, EUR). Run steps, changes and env vars:
common/runner.py. Columns: common/schema.py.
"""
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor

from common import http, runner, schema

SITE = "datafox"
BASE = "https://pood.datafox.ee"
WORKERS = 3
PAGE_DELAY = 0.25          # polite pause inside each page fetch

# datafox's own URL slugs for the server / storage / networking part of the catalogue
CATEGORIES = [
    "salvestusseadmed",      # storage devices (SSD / HDD / NAS)
    "operatiivmalud-ram",    # memory (RAM)
    "vorguseadmed",          # networking devices
    "emaplaadid",            # motherboards
    "videokaardid",          # graphics cards
]
NUXT_RE = re.compile(r'id="__NUXT_DATA__"[^>]*>(.*?)</script>', re.S)
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b", re.I)


def _make_resolver(flat):
    """Resolve a Nuxt __NUXT_DATA__ devalue array: every value is either a literal or an index
    into the array. Memoised; genuine cycles resolve to None."""
    memo, stack = {}, set()

    def r(i):
        if not isinstance(i, int):
            return i
        if i < 0 or i >= len(flat):
            return None
        if i in memo:
            return memo[i]
        if i in stack:
            return None
        v = flat[i]
        if isinstance(v, dict):
            stack.add(i)
            out = {k: r(x) for k, x in v.items()}
            stack.discard(i)
        elif isinstance(v, list):
            stack.add(i)
            out = [r(x) for x in v]
            stack.discard(i)
        else:
            out = v
        memo[i] = out
        return out
    return r


def fetch_catalog(cat, page):
    """The catalogue response object embedded in one category page, or raises."""
    url = f"{BASE}/en/{cat}" + (f"?page={page}" if page > 1 else "")
    r = http.session().get(url, timeout=60)
    r.raise_for_status()
    time.sleep(PAGE_DELAY)
    m = NUXT_RE.search(r.text)
    if not m:
        raise RuntimeError(f"no __NUXT_DATA__ payload in {url}")
    flat = json.loads(m.group(1))
    resolve = _make_resolver(flat)
    for idx, v in enumerate(flat):
        if isinstance(v, dict) and "products" in v and "total" in v and "pageSize" in v:
            return resolve(idx)
    raise RuntimeError(f"no catalogue object in {url}")


def to_float(v):
    return float(v) if isinstance(v, (int, float)) else None


def build_row(p, fallback_cat, scraped_at):
    price = to_float(p.get("price"))
    compare = to_float(p.get("originalPrice"))
    cat = p.get("category") or fallback_cat
    slug = p.get("slug") or ""
    name = p.get("name") or ""
    ean = str(p.get("ean") or "").strip()
    cap = CAPACITY_RE.search(name)
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": str(p.get("id")),
        "sku": str(p.get("sku") or "").strip(),
        "part_number": str(p.get("mpn") or p.get("sku") or "").strip(),
        "title": name,
        "brand": (p.get("brand") or "").strip(),
        "category": cat,
        "capacity": (cap.group(1) + cap.group(2).upper()) if cap else "",
        "other_specs": f"EAN: {ean}" if ean else "",
        "price": price,
        "compare_at_price": compare if (compare and price and compare > price) else None,
        "available": bool(p.get("inStock")),
        "stock_qty": to_float(p.get("stockCount")),
        "image_url": (p.get("thumbnail") or "").strip() or None,  # absolute images.datafox.ee URL
        "description": None,  # catalogue payload carries no description field
        "url": f"{BASE}/en/{cat}/{slug}" if slug else f"{BASE}/en/{cat}",
    })


def scrape(limit, scraped_at):
    by_id = {}
    planned = 0
    for cat in CATEGORIES:
        first = fetch_catalog(cat, 1)
        total = first.get("total") or 0
        total_pages = first.get("totalPages") or 1
        planned += total
        print(f"{cat}: {total} products across {total_pages} pages", flush=True)
        for p in first.get("products") or []:
            if p.get("id") is not None:
                by_id[str(p["id"])] = (p, cat)
        if limit and len(by_id) >= limit:
            break
        pages = list(range(2, total_pages + 1))
        with ThreadPoolExecutor(WORKERS) as pool:
            for cobj in pool.map(lambda pg: fetch_catalog(cat, pg), pages):
                for p in cobj.get("products") or []:
                    if p.get("id") is not None:
                        by_id[str(p["id"])] = (p, cat)
        print(f"  {cat}: {len(by_id)} unique products so far", flush=True)
        if limit and len(by_id) >= limit:
            break
    items = list(by_id.values())
    if limit:
        items = items[:limit]
    rows = [build_row(p, cat, scraped_at) for p, cat in items]
    print(f"planned {planned} (with cross-category duplicates), {len(rows)} unique", flush=True)
    # products overlap categories, so the de-duplicated count is below the sum of category totals;
    # a failed page raises above rather than quietly shortening the catalogue
    return rows, None


if __name__ == "__main__":
    runner.run(SITE, scrape)
