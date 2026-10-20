"""Scrape pcserverandparts.com (BigCommerce, ~2.5k products): every run's full snapshot appended
to Postgres.

The store's own API, search page and sitemap sit behind a Cloudflare challenge, but its search
box is Doofinder, a separate service (us1-search.doofinder.com) that answers the same public
queries the site's search makes. One query returns at most 1,000 results (100 per page,
10 pages), so the catalogue is split into best_price ranges, halved until each range has at
most 1,000 products. Doofinder also indexes the store's categories (type "category", no
price); only type "product" is kept. The product count to check against is the price
facet's count over the whole index.

Fields: title, brand, categories, best_price (what the customer pays), price (the list price,
saved as compare_at_price when higher), stock_quantity, availability, mpn (part number),
custom_model_number (model number), gtin (holds the store's SKU) and custom_* specs.
Condition is read from the title (New / Refurbished / Used / Open Box).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re

from common import http, runner, schema

SITE = "pcserverandparts"
BASE = "https://www.pcserverandparts.com"
API = "https://us1-search.doofinder.com/5/search"
HASHID = "34913f4c8b49ebec09afc685dbd2ee4f"   # the store's search engine (from its Doofinder config)
PAGE = 100
CAP = 1000                                   # Doofinder won't page past this per query

SPEC_COLUMNS = {"custom_form_factor": "form_factor"}
CONDITIONS = [
    (re.compile(r"\bopen\s*box\b", re.I), "Open Box"),
    (re.compile(r"\brefurbished\b", re.I), "Refurbished"),
    (re.compile(r"\brecertified\b", re.I), "Recertified"),
    (re.compile(r"\bused\b", re.I), "Used"),
    (re.compile(r"\bnew\b", re.I), "New"),
]
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b", re.I)
STORAGE_CATEGORY_RE = re.compile(r"storage|drive|ssd|memory|ram\b", re.I)


def search(price_range, page=1, rpp=PAGE):
    lo, hi = price_range
    params = {"hashid": HASHID, "query": "", "rpp": rpp, "page": page,
              "filter[best_price][gte]": lo}
    if hi is not None:
        params["filter[best_price][lt]"] = hi
    r = http.session().get(API, params=params, timeout=60,
                           headers={"Origin": BASE, "Referer": BASE + "/"})
    r.raise_for_status()
    return r.json()


def price_stats():
    """(product count, highest price) over the whole index."""
    data = search((0, None), rpp=1)
    stats = data["facets"]["best_price"]["range"]["buckets"][0]["stats"]
    return int(stats["count"]), float(stats["max"])


def plan(price_range, found=None):
    """[(price range, product count)] with at most CAP products each."""
    if found is None:
        found = search(price_range, rpp=1)["total_found"]
    lo, hi = price_range
    if found <= CAP:
        return [(price_range, found)] if found else []
    if hi is None or hi - lo < 0.02:
        print(f"WARN {found} products in price range {price_range}; only {CAP} are reachable",
              flush=True)
        return [(price_range, CAP)]
    mid = round((lo + hi) / 2, 2)
    return plan((lo, mid)) + plan((mid, hi))


def fetch_range(listing):
    price_range, found = listing
    results = []
    for page in range(1, -(-min(found, CAP) // PAGE) + 1):
        results += search(price_range, page)["results"]
    return [r for r in results if r.get("type") == "product"]


def clean(v):
    return re.sub(r"\s+", " ", str(v or "")).strip()


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def condition_of(title):
    for rx, label in CONDITIONS:
        if rx.search(title):
            return label
    return ""


def build_row(p, scraped_at):
    title = clean(p.get("title"))
    paths = p.get("categories") or []
    category_path = max(paths, key=lambda c: c.count(">"), default="")
    category = clean(category_path.split(">")[-1])
    specs, other = {}, []
    for key in sorted(k for k in p if k.startswith("custom_") and k != "custom_model_number"):
        value = clean(p.get(key))
        if not value:
            continue
        if key in SPEC_COLUMNS:
            specs[SPEC_COLUMNS[key]] = value
        else:
            other.append(f"{key[len('custom_'):].replace('_', ' ').capitalize()}: {value}")
    cap = CAPACITY_RE.search(title) if STORAGE_CATEGORY_RE.search(category_path) else None
    price = to_float(p.get("best_price"))
    listed = to_float(p.get("price"))
    compare = listed if listed and price and listed > price else None
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": str(p["id"]),
        "sku": clean(p.get("gtin")),          # the store's SKU travels in Doofinder's gtin field
        "part_number": clean(p.get("mpn")),
        "model_number": clean(p.get("custom_model_number")),
        "title": title,
        "brand": clean(p.get("brand")),
        "category": category,
        "condition": condition_of(title),
        "capacity": (cap.group(1) + cap.group(2).upper()) if cap else "",
        "form_factor": specs.get("form_factor", ""),
        "other_specs": "; ".join(other),
        "price": price,
        "compare_at_price": compare,
        "discount_pct": round((compare - price) / compare * 100, 1) if compare else None,
        "available": p.get("availability") == "in stock",
        "stock_qty": to_float(p.get("stock_quantity")),
        "image_url": clean(p.get("image_link")) or None,   # Doofinder indexes an absolute cdn url
        "description": schema.plain_text(p.get("description")),
        "url": clean(p.get("link")),
    })


def scrape(limit, scraped_at):
    listed, top = price_stats()
    if limit:
        listings = [((0, None), min(limit, CAP))]
    else:
        listings = plan((0, top + 1))
        print(f"{len(listings)} price ranges, {sum(n for _, n in listings)} products "
              f"(index has {listed})", flush=True)
    by_id = {}
    for listing in listings:
        for p in fetch_range(listing):
            by_id[p["id"]] = p
    items = list(by_id.values())[:limit] if limit else list(by_id.values())
    return [build_row(p, scraped_at) for p in items], listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
