"""Scrape soliddiskdirect.com (~200k products): every run's full snapshot appended to Postgres.

The site is a JavaScript app whose data comes from its own open API,
admin.soliddiskdirect.com/api (Laravel). Its product search (/products/search2) takes a LIKE
pattern, so "%" matches every product; sorted by id it pages through the whole catalogue,
1,000 per page (~4 s a page from a GitHub runner, ~200 pages). The listing has MPN, title,
category, condition and price, but no brand, so brand stays blank for now.

The store publishes no stock: "available" and stock_qty stay blank, so only new products and
price changes show up in prices. unit_price 0 means no price is shown ("call for price"):
saved as empty. discount_price is the higher, struck-through price, saved as compare_at_price.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
import time
from concurrent.futures import ThreadPoolExecutor

from common import http, runner, schema

SITE = "soliddiskdirect"
BASE = "https://soliddiskdirect.com"
API = "https://admin.soliddiskdirect.com/api"
PAGE = 1000
WORKERS = 3              # the API runs on shared hosting: keep it gentle
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b", re.I)


def get(path, params):
    for attempt in range(5):
        try:
            r = http.session().get(f"{API}/{path}", params=params, timeout=300)
            r.raise_for_status()
            data = r.json()
            if data.get("status"):
                return data
            error = data.get("message")
        except ValueError as e:  # a cut-off JSON body
            error = e
        if attempt < 4:
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"{path} {params}: {error}")


def search_page(page, per_page=PAGE):
    return get("products/search2", {"search": "%", "per_page": per_page, "page": page,
                                    "sort_field": "id", "sort_direction": "asc"})


def fetch_catalog(limit):
    first = search_page(1, min(limit, PAGE) if limit else PAGE)
    listed = first["pagination"]["total"]
    pages = 1 if limit else first["pagination"]["last_page"]
    print(f"catalogue: {listed} products, {pages} pages", flush=True)
    by_id = {p["id"]: p for p in first["data"]}
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, data in enumerate(pool.map(search_page, range(2, pages + 1)), 2):
            for p in data["data"]:
                by_id[p["id"]] = p
            if i % 25 == 0 or i == pages:
                print(f"page {i}/{pages}: {len(by_id)} unique products", flush=True)
    return list(by_id.values()), listed


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def clean(v):
    return re.sub(r"\s+", " ", str(v or "")).strip()


def build_row(p, scraped_at):
    price = to_float(p.get("unit_price"))
    struck = to_float(p.get("discount_price"))
    title = clean(p.get("title"))
    cap = CAPACITY_RE.search(title)
    mpn = clean(p.get("mpn"))
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": str(p["id"]),
        "sku": clean(p.get("sku")) or mpn,
        "part_number": mpn,
        "title": title,
        "category": clean((p.get("category") or {}).get("name")),
        "condition": clean(p.get("condition")),
        "capacity": (cap.group(1) + cap.group(2).upper()) if cap else "",
        "price": price or None,                                  # 0 = no price shown
        "compare_at_price": struck if struck and price and struck > price else None,
        "available": None,                                       # not published
        "stock_qty": None,
        "url": f"{BASE}/products/{p.get('category_url')}/{mpn}" if mpn else "",
    })


def scrape(limit, scraped_at):
    items, listed = fetch_catalog(limit)
    items = items[:limit] if limit else items
    return [build_row(p, scraped_at) for p in items], listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
