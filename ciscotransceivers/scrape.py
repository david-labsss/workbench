"""Scrape ciscotransceivers.com: every run's full snapshot appended to Postgres.

ciscotransceivers.com (compatible Cisco transceivers / optics) is an old OpenCart 1.5 storefront
behind Cloudflare, but the product HTML is served to a plain browser User-Agent (no challenge).
There is no usable XML sitemap (the OpenCart google_sitemap feed and /sitemap.xml both 404), so the
catalogue is enumerated from the "Categories" box in the left column of every page: nine SEO
category URLs (/sfp, /sfp+, /xfp, /xenpak, /x2, /cwdm-sfp, /dwdm-sfp, /gbic, /cisco-qsfp+). Each
category is one listing page (OpenCart prints "Showing 1 to N of N (1 Pages)"); pagination
(?page=N) is still walked defensively in case a category ever grows past one page.

The listing page carries everything we need per product, so the (identical) product pages are not
fetched: the product block (div.product-list > div) has the image, the name (= the Cisco part
code, e.g. GLC-SX-MM), the descriptive blurb, the price (<div class="price">$22.00</div>, with
OpenCart's price-old/price-new spans handled for specials) and an addToCart('<id>') call that gives
the OpenCart product_id. There is no JSON-LD on this theme. product_id is unique; a product is kept
under the first category it is seen in (no cross-category duplicates were observed: 389 distinct
ids across the nine categories, each category fully on one page). Currency is USD.

Stock: the store shows no per-product quantity and every listed item is an orderable "Ships in 24
hours" compatible module, so available = True for everything listed and stock_qty stays blank.
brand is "Cisco" (the whole store is Cisco-part-numbered transceivers); condition is left blank
(the store states none per product). Prices are AS-IS in USD; a missing / $0 price -> price = None.

Completeness: every category must yield at least its reported product count (MIN_COVERAGE), else the
run stops rather than save a partial snapshot. listed = the sum of the categories' reported totals.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.

Env vars:
  DATABASE_URL     Postgres URL for the scraper_writer role
  SCRAPE_WORKERS   optional, categories fetched at once (default 4)
  MAX_PRODUCTS     optional, stop after about N products (test runs, not compared or counted)
  DRY_RUN          optional, "1" = print sample rows, don't touch the database
"""
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "ciscotransceivers"
BASE = "https://www.ciscotransceivers.com"
WORKERS = int(os.getenv("SCRAPE_WORKERS") or 4)
DELAY = 0.4                 # polite pause after each request (seconds)
MAX_PAGES = 50             # a category with more pages than this means paging is broken
MIN_COVERAGE = 0.98        # each category must yield at least this share of its reported total

HOST = urlparse(BASE).netloc
SHOWING_RE = re.compile(r"Showing\s+\d+\s+to\s+\d+\s+of\s+(\d+)\s+\((\d+)\s+Pages?\)", re.I)
ADD_TO_CART_RE = re.compile(r"addToCart\('(\d+)'\)")
PRICE_RE = re.compile(r"([\d,]+(?:\.\d+)?)")


def get(url):
    r = http.session().get(url, timeout=60)
    r.raise_for_status()
    time.sleep(DELAY)
    return r.text


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def to_price(text):
    """A USD price from a '$22.00' / '$2,345.00' string, or None when missing / zero."""
    m = PRICE_RE.search(text or "")
    if not m:
        return None
    try:
        p = round(float(m.group(1).replace(",", "")), 2)
    except ValueError:
        return None
    return p if p > 0 else None


def discover_categories():
    """[(slug, name)] from the left-column "Categories" box (same-domain links only, so the
    "Partners" box of external links in the same column is left out)."""
    soup = BeautifulSoup(get(BASE + "/"), "html.parser")
    cats, seen = [], set()
    for a in soup.select("#column-left .box-category a[href]"):
        href = a["href"].strip()
        if urlparse(href).netloc != HOST:
            continue                                   # skip the external "Partners" links
        slug = urlparse(href).path.strip("/")
        if slug and slug not in seen:
            seen.add(slug)
            cats.append((slug, clean(a.get_text())))
    if not cats:
        raise RuntimeError("no categories found in the left-column Categories box")
    return cats


def listing_meta(soup):
    """(reported total, page count) from the "Showing 1 to N of N (P Pages)" line, or (None, 1)."""
    m = SHOWING_RE.search(soup.get_text(" "))
    return (int(m.group(1)), int(m.group(2))) if m else (None, 1)


def parse_products(soup, slug, name):
    """Product dicts from one listing page's div.product-list (in page order)."""
    out = []
    box = soup.select_one("div.product-list")
    if not box:
        return out
    for block in box.find_all("div", recursive=False):
        m = ADD_TO_CART_RE.search(block.decode())
        name_a = block.select_one("div.name a")
        if not m or not name_a:
            continue
        image = block.select_one("div.image img")
        price_div = block.select_one("div.price")
        price, compare_at = None, None
        if price_div:
            new = price_div.select_one(".price-new")
            old = price_div.select_one(".price-old")
            if new:
                price = to_price(new.get_text())
                compare_at = to_price(old.get_text()) if old else None
            else:
                price = to_price(price_div.get_text())
        out.append({
            "id": m.group(1),
            "title": clean(name_a.get_text()),
            "url": name_a["href"].strip(),
            "description": clean(block.select_one("div.description").get_text())
                           if block.select_one("div.description") else "",
            "image": image["src"].strip() if image and image.get("src") else None,
            "price": price,
            "compare_at": compare_at,
            "category": name,
        })
    return out


def crawl_category(cat):
    """(reported total, [product dicts]) for one category, walking ?page=N defensively."""
    slug, name = cat
    soup = BeautifulSoup(get(f"{BASE}/{slug}"), "html.parser")
    total, pages = listing_meta(soup)
    products = parse_products(soup, slug, name)
    page = 1
    while page < pages:
        page += 1
        if page > MAX_PAGES:
            raise RuntimeError(f"category {slug}: more than {MAX_PAGES} pages")
        more = parse_products(BeautifulSoup(get(f"{BASE}/{slug}?page={page}"), "html.parser"),
                              slug, name)
        if not more:
            break
        products += more
    print(f"category {slug} ({name}): reported {total}, scraped {len(products)}", flush=True)
    return total, products


def build_row(p, scraped_at):
    code = p["title"]                                  # the listing name IS the Cisco part code
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": p["id"],
        "sku": code,
        "part_number": code,
        "title": code,
        "brand": "Cisco",
        "category": p["category"],
        "price": p["price"],
        "compare_at_price": p["compare_at"],
        "available": True,                             # every listed item is orderable (no stock shown)
        "stock_qty": None,
        "image_url": p["image"],
        "description": schema.plain_text(p["description"]),
        "url": p["url"],
    })


def scrape(limit, scraped_at):
    cats = discover_categories()
    print(f"{len(cats)} categories: {', '.join(s for s, _ in cats)}", flush=True)

    by_id = {}
    if limit:
        # Test run: walk categories until we have about `limit` products.
        for cat in cats:
            _, products = crawl_category(cat)
            for p in products:
                by_id.setdefault(p["id"], p)
            if len(by_id) >= limit:
                break
        rows = [build_row(p, scraped_at) for p in list(by_id.values())[:limit]]
        return rows, None

    with ThreadPoolExecutor(WORKERS) as pool:
        results = list(pool.map(crawl_category, cats))

    listed = 0
    low = []
    for (slug, name), (total, products) in zip(cats, results):
        for p in products:
            by_id.setdefault(p["id"], p)               # keep the first category a product is seen in
        if total:
            listed += total
            if len(products) < MIN_COVERAGE * total:
                low.append(f"{slug} {len(products)}/{total}")
    if low:
        sys.exit(f"categories below their reported totals ({', '.join(low)}): the listing paging "
                 "broke; not trusting this run")

    rows = [build_row(p, scraped_at) for p in by_id.values()]
    print(f"{len(rows)} unique products across {len(cats)} categories "
          f"(categories report {listed} in total)", flush=True)
    return rows, (listed or None)


if __name__ == "__main__":
    runner.run(SITE, scrape)
