"""Scrape boosthardware.com (custom Laravel store on Cloudflare, ~61k product pages): every run's
full snapshot appended to Postgres.

The store has no product API, so products are discovered from the paginated category sitemaps
(sitemap_<category>_page_<n>.xml). The sitemap index (/sitemap.xml) is NOT used: it leaves pages
out (e.g. it lists memory pages 1-4 while page 5 also has products). Instead each category's
pages are fetched in order until one comes back empty (HTTP 200 with no <loc>), which marks the
end of that category.

Each product page carries a JSON-LD Product whose offers[] holds one Offer per condition
(New / Refurbished), each with price, itemCondition and availability, plus sku, mpn and brand.
A page yields one row per offer. Most product pages (about two thirds of the catalogue) have an
empty offers[], no add-to-cart button and no price: those parts are not currently sellable, so
they produce no rows. They are picked up as "new"/"back_in_stock" once they get an offer and as
"out_of_stock" when an offer disappears (see common/changes.py).

Because only a fraction of the ~61k pages are priced, the row count is far below the page count,
so scrape() reports listed=None (the runner's count guard does not apply) and instead guards a
partial crawl itself: a full run aborts if more than MAX_FAILED of the product pages fail to
fetch. The on-page "was" price (#price_new / #price_ref .original-price) becomes compare_at_price
when it is above the live price. The internal product id is the add-to-cart button's data-id; it
is shared by a product's conditions, so product_id is "<data-id>-<new|ref>" to stay unique.

This is a large store: a full run fetches tens of thousands of pages and takes a long time. Run
steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import zip_longest

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "boosthardware"
BASE = "https://boosthardware.com"
# Category sitemaps to page through. The index (/sitemap.xml) is skipped because it omits pages.
CATEGORIES = ["processors", "memory", "hard_drives", "solid_state_drives", "networking",
              "power_supplies", "switches"]
CATEGORY_NAMES = {
    "processors": "Processors", "memory": "Memory", "hard_drives": "Hard Drives",
    "solid_state_drives": "Solid State Drives", "networking": "Networking",
    "power_supplies": "Power Supplies", "switches": "Switches",
}
WORKERS = int(os.getenv("BOOST_WORKERS") or 4)     # polite: low concurrency
DELAY = float(os.getenv("BOOST_DELAY") or 0.15)    # small pause after each request
MAX_FAILED = 0.01   # share of product pages that may fail before a full run stops without saving

LOC_RE = re.compile(r"<loc>([^<]+)</loc>", re.I)
LD_RE = re.compile(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
                   re.S | re.I)
CAPACITY_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(TB|GB)", re.I)

# schema.org itemCondition -> (condition label, product_id suffix, matching #price_<suffix> id)
CONDITIONS = {
    "NewCondition": ("New", "new"),
    "RefurbishedCondition": ("Refurbished", "ref"),
    "UsedCondition": ("Used", "used"),
    "DamagedCondition": ("Damaged", "dmg"),
}
# additionalProperty name (lowercased) -> standard column; the rest go into other_specs
SPEC_COLUMNS = {
    "capacity": "capacity",
    "interface": "interface", "interface type": "interface",
    "form factor": "form_factor", "memory form factor": "form_factor",
    "spindle speed": "spindle_speed", "rotational speed": "spindle_speed",
    "cache": "cache",
}
SKIP_SPECS = {"brand"}
# price_per_tb is only meaningful for storage, not RAM/CPUs/etc.
DRIVE_CATEGORIES = {"Hard Drives", "Solid State Drives"}


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def money(text):
    """First number in a price string like '$1,234.50' -> float, else None."""
    m = re.search(r"[\d,]+(?:\.\d+)?", text or "")
    return float(m.group(0).replace(",", "")) if m else None


def to_float(v):
    try:
        return float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def tb_size(capacity):
    """Capacity string ('2TB', '480GB') -> size in TB, for price_per_tb."""
    m = CAPACITY_RE.search(capacity or "")
    if not m:
        return None
    n = float(m.group(1))
    return n if m.group(2).upper() == "TB" else n / 1024.0


def sitemap_urls(category, max_pages=None):
    """Every product URL in a category, paging until an empty sitemap marks the end (or until
    max_pages, used only to keep capped test runs quick)."""
    urls, page = [], 1
    while not max_pages or page <= max_pages:
        url = f"{BASE}/sitemap_{category}_page_{page}.xml"
        locs = LOC_RE.findall(http.session().get(url, timeout=120).text)
        if not locs:
            # One confirming re-fetch: a transient empty page must not truncate a category.
            time.sleep(2)
            locs = LOC_RE.findall(http.session().get(url, timeout=120).text)
            if not locs:
                break
        urls += locs
        page += 1
        time.sleep(DELAY)
    return urls


def discover(limit=0):
    """(url, category) for every product, interleaved across categories so that a capped test
    run still samples every category. A capped run reads only the first sitemap page per
    category (plenty of urls) instead of paging the whole store."""
    max_pages = 1 if limit else None
    by_cat = []
    for c in CATEGORIES:
        found = sitemap_urls(c, max_pages)
        print(f"  {c}: {len(found)} urls", flush=True)
        by_cat.append([(u, CATEGORY_NAMES[c]) for u in found])
    pairs, seen = [], set()
    for group in zip_longest(*by_cat):
        for item in group:
            if item and item[0] not in seen:
                seen.add(item[0])
                pairs.append(item)
    return pairs


def find_product(html):
    """The JSON-LD Product object on the page, or None."""
    for block in LD_RE.findall(html):
        try:
            data = json.loads(block)
        except ValueError:
            continue
        items = data.get("@graph", [data]) if isinstance(data, dict) else data
        for obj in items if isinstance(items, list) else [items]:
            if isinstance(obj, dict):
                types = obj.get("@type")
                if "Product" in (types if isinstance(types, list) else [types]):
                    return obj
    return None


def parse(url, html, category, scraped_at):
    """Rows (one per condition offer) for a product page; [] when nothing is for sale."""
    prod = find_product(html)
    if not prod:
        return []
    offers = prod.get("offers") or []
    if isinstance(offers, dict):
        offers = [offers]
    if not offers:                         # listed but not currently sellable: no price, no row
        return []

    soup = BeautifulSoup(html, "html.parser")
    title = clean(prod.get("name"))
    mpn = clean(prod.get("mpn") or prod.get("sku"))
    brand = prod.get("brand")
    brand = clean(brand.get("name")) if isinstance(brand, dict) else clean(brand)

    specs, other = {}, []
    for p in prod.get("additionalProperty") or []:
        name, value = clean(p.get("name")), clean(p.get("value"))
        if not name or not value:
            continue
        column = SPEC_COLUMNS.get(name.lower())
        if column:
            specs.setdefault(column, value)
        elif name.lower() not in SKIP_SPECS:
            other.append(f"{name}: {value}")
    capacity = specs.get("capacity", "")
    tb = tb_size(capacity) if category in DRIVE_CATEGORIES else None

    # data-id is the internal product id, shared by a product's conditions (one add-to-cart
    # button per condition, same data-id).
    id_by_cond = {b.get("data-condition"): clean(b.get("data-id"))
                  for b in soup.select("button.add-to-cart[data-id]")}
    fallback_id = next((v for v in id_by_cond.values() if v), "") or mpn or url.rstrip("/").rsplit(
        "/", 1)[-1]

    rows, seen = [], set()
    for offer in offers:
        cond = (offer.get("itemCondition") or "").rstrip("/").rsplit("/", 1)[-1]
        label, suffix = CONDITIONS.get(
            cond, (cond.replace("Condition", "") or "", (cond.replace("Condition", "") or "na").lower()))
        block = soup.select_one(f"#price_{suffix}")

        price = to_float(offer.get("price"))
        if price is None and block:        # fall back to the on-page current price
            current = block.select_one(".current-price")
            price = money(current.get_text()) if current else None
        if price is None:
            continue

        was = money(block.select_one(".original-price").get_text()) if (
            block and block.select_one(".original-price")) else None
        avail = (offer.get("availability") or "").rstrip("/").rsplit("/", 1)[-1]
        product_id = f"{id_by_cond.get(suffix) or fallback_id}-{suffix}"
        if product_id in seen:
            continue
        seen.add(product_id)

        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": product_id,
            "sku": mpn,
            "part_number": mpn,
            "title": title,
            "brand": brand,
            "category": category,
            "condition": label,
            "capacity": capacity,
            "interface": specs.get("interface", ""),
            "form_factor": specs.get("form_factor", ""),
            "spindle_speed": specs.get("spindle_speed", ""),
            "cache": specs.get("cache", ""),
            "other_specs": "; ".join(other),
            "price": price,
            "compare_at_price": was if was and was > price else None,
            "price_per_tb": round(price / tb, 2) if price and tb else None,
            "available": avail == "InStock",
            "url": offer.get("url") or prod.get("url") or url,
        }))
    return rows


def fetch(pair):
    url, category = pair
    try:
        r = http.session().get(url, timeout=60)
        if DELAY:
            time.sleep(DELAY)
        if r.status_code == 404:
            return "gone", url, category, None
        r.raise_for_status()
        return "ok", url, category, r.text
    except Exception as e:  # noqa: BLE001 - one bad page must not stop the run
        print(f"WARN {url}: {e}", flush=True)
        return "fail", url, category, None


def scrape(limit, scraped_at):
    pairs = discover(limit)
    print(f"discovered {len(pairs)} product urls across {len(CATEGORIES)} categories", flush=True)
    todo = pairs[:limit] if limit else pairs

    rows, seen, failed, gone, priced = [], set(), [], 0, 0

    def handle(result):
        nonlocal gone, priced
        status, url, category, html = result
        if status == "fail":
            failed.append((url, category))
        elif status == "gone":
            gone += 1
        else:
            product_rows = parse(url, html, category, scraped_at)
            if product_rows:
                priced += 1
            for row in product_rows:
                pid = row[schema.PRODUCT_COLUMNS.index("product_id")]
                if pid not in seen:
                    seen.add(pid)
                    rows.append(row)

    with ThreadPoolExecutor(WORKERS) as pool:
        for i, result in enumerate(pool.map(fetch, todo), 1):
            handle(result)
            if i % 500 == 0 or i == len(todo):
                print(f"{i}/{len(todo)} pages, {priced} priced, {len(rows)} rows, "
                      f"{gone} gone, {len(failed)} failed", flush=True)

    # One more try for the pages that failed (e.g. a dropped connection) before deciding.
    if failed:
        retry, failed = failed, []
        for result in (fetch(p) for p in retry):
            if result[0] == "fail":
                failed.append((result[1], result[2]))
            else:
                handle(result)

    full_run = not limit
    if full_run and len(failed) > MAX_FAILED * len(todo):
        sys.exit(f"{len(failed)} of {len(todo)} product pages failed to fetch; "
                 "not saving a partial snapshot")
    print(f"{priced} of {len(todo)} pages priced -> {len(rows)} offer rows "
          f"({gone} gone, {len(failed)} failed)", flush=True)
    return rows, None


if __name__ == "__main__":
    runner.run(SITE, scrape)
