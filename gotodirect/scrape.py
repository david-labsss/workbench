"""Scrape gotodirect.com: its server / storage / networking / compute catalogue, every run's
full snapshot appended to Postgres.

PLATFORM (recon 2026-10-21): gotodirect.com is a custom Laravel storefront (csrf-token meta,
XSRF-TOKEN + "..._session" cookies, "mage/"/Bootstrap assets) behind Cloudflare. Cloudflare does
NOT block a polite plain client (every request below returns HTTP 200). There is no Shopify
/products.json, no Storefront/Magento GraphQL (/graphql is 404), no WooCommerce Store API, and no
on-site search JSON API: /search?q=, /catalog-search/filter/q/<q> and /search/<q> all return the
same 12-card HTML listing, and every autocomplete/api path is 404. No Searchanise/Algolia. So the
fastest path that carries price + stock is the category listing page itself.

FASTEST PATH: each category URL (https://www.gotodirect.com/<category-slug>) server-renders a grid
of 12 product cards, paged by ?page=N (the page size is fixed at 12 - no limit/per_page/show/size
param changes it). One card carries everything we store except a description: the product URL
slug, a stable numeric product id (data-id on the wishlist button), the full title (the image
`alt`; the visible .h6 link text is ellipsis-truncated), MPN, brand, condition, in/out of stock,
the image URL, the selling price (.price .line-through) and a higher struck "was" price
(.price .color). Verified against the product page: .line-through == the product page's
.product-price and its JSON-LD "price"; .color is the higher compare price (despite the class
names). A description is NOT on the card (only the product page has it, which would be 1 request
per product == ~1.1M requests), so description stays blank. Deep pagination works to the full
depth of every category (page 4518 of a 54,217-product category returns distinct products; no 25k
Shopify-style cap), so page-level sharding reaches the whole catalogue. Currency is USD (US store:
San Jose CA); prices are saved as the raw number. Out-of-stock cards show no price (verified: of
240 sampled cards, all 163 in-stock had a real price and all 77 without a price were out of
stock) -> price=None, never 0.

SCOPE & SIZE (census 2026-10-21): the category sitemap lists 201 categories; 175 render a grid
(the other 26 are top-level landing pages with no grid / no "Results" count and are skipped
automatically because they report no total). The grids sum to 1,085,945 products (the product
sitemap lists 1,140,229 in all - ~54k products sit under no leaf grid and are simply not reachable
by category, which is the completeness ceiling of this method). EXCLUDE below is a denylist of
consumer / peripheral leaf categories (printers, monitors, audio, cameras, input devices, consumer
systems/cases, optical media, software); it removes ~109k products, leaving the SCOPED set of
~976,930 products in ~97 categories = ~81,455 pages @12/page (counts drift daily). EXCLUDE is a
denylist so new server/storage categories the store adds are covered automatically.

TOO BIG FOR ONE RUN -> SHARDING. The origin throttles throughput to ~2 pages/s per process/IP
(measured: 1.6 pages/s at 6 workers, 2.0 at 12; it resets connections above ~20 workers), so one
process would take ~11 h for the scoped set - past GitHub Actions' 6 h hard cap. The run is
therefore sharded page-level by env var (a single category can be 100k+ products / 8k+ pages, so
whole-category sharding cannot split the giants; page-level can, because deep pagination works):
  SHARD_COUNT  number of shards the parent dispatches (default 1 = the whole scoped catalogue)
  SHARD_INDEX  this shard's number, 0 .. SHARD_COUNT-1 (default 0)
Every shard fetches page 1 of each scoped category (to learn the per-category totals -> the same
deterministic global list of (category, page) units in every shard), then fetches only the units
where unit_index % SHARD_COUNT == SHARD_INDEX. At ~2 pages/s a 6-shard run is ~1.9 h/shard
(~2.5 h at a conservative 1.5 pages/s); recommend SHARD_COUNT=6 and a 4 h (240 min) job timeout.
The parent re-picks SHARD_COUNT from the live page total each run (pages/ (budget_s * ~1.8)).

COMPLETENESS. Two independent guards so a truncated run is never saved as complete:
  1. `listed` is the sum of the EXPECTED card count of every page in THIS shard (12 per page
     except each category's last page). runner.py fails the run if the deduped rows fall below
     98% of it. For a limited (MAX_PRODUCTS) test run `listed` is None (not a full run).
  2. Any page before a category's last page that returns fewer than 12 cards raises (a short /
     empty page mid-stream must never look like the end of the catalogue).

COORDINATOR NOTE (sharded deploys). With SHARD_COUNT=1 this is a normal, complete (if slow) full
run that runner.py change-tracks and saves on its own. With SHARD_COUNT>1 each shard is a PARTIAL
snapshot: do NOT let each shard run runner.py's change-tracking against the full previous run
(common/changes.find_changes would treat every product owned by the OTHER shards as removed). The
orchestration layer owns assembling the shards for one scraped_at and running change-tracking /
the final save once over the union. This module only fetches and maps; it does not write the DB.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.

Env vars:
  DATABASE_URL   Postgres URL for the scraper_writer role
  SHARD_COUNT    optional, number of shards (default 1 = whole scoped catalogue)
  SHARD_INDEX    optional, this shard's index 0..SHARD_COUNT-1 (default 0)
  MAX_PRODUCTS   optional, stop after about N products (test runs; not a full run)
  DRY_RUN        optional, "1" = print sample rows, don't write anything
"""
import math
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup

from common import bigcommerce, http, runner, schema

SITE = "gotodirect"
BASE = "https://www.gotodirect.com"
CURRENCY = "USD"                       # documented only; prices are saved as the raw number
CATEGORY_SITEMAP = BASE + "/a564ac/sitemap_categories.xml"
PAGE_SIZE = 12                         # products per listing page (fixed; no per-page param works)
WORKERS = 8                            # the origin throttles / resets connections above this
TRIES = 3                              # attempts per request when the response is cut off / times out
RETRY_PAUSE = 20
PID = schema.PRODUCT_COLUMNS.index("product_id")

COUNT_RE = re.compile(r"of\s+([\d,]+)\s+Results", re.I)
MONEY_RE = re.compile(r"[\d,]+(?:\.\d+)?")

# Consumer / peripheral leaf categories scoped OUT (matched case-insensitively against the
# sitemap slugs). Everything else that reports a product count is kept, so new server / storage /
# networking categories are covered automatically.
EXCLUDE = {
    # printers / imaging
    "others-printer-accessories", "toner-cartridges", "ink-cartridges", "fuser-maintenance-kits",
    "printer-rollers", "multifunction-printers", "laser-printers", "label-printers",
    "thermal-printers", "inkjet-printers", "all-in-one-printers", "dot-matrix-printers",
    "receipt-printers", "print-server", "printer-memory", "printer-hard-drives",
    "printer-accessories", "printer-cartridges", "printhead", "formatter-board", "paper-tray",
    "automatic-document-feeder", "duplexer", "transfer-belt", "maintenance-kits",
    "scanner-fax-copiers-accessories", "scanners", "barcode-scanners", "plotters",
    # monitors / display
    "lcd-led-monitor", "crt-monitors", "monitor-accessories", "laptop-replacement-screen",
    # audio
    "speakers", "headphone-headsets", "sound-card", "microphones",
    # cameras
    "surveillance-cameras", "digital-cameras", "camera-parts-accessories", "webcams",
    # input devices
    "keyboards", "mouse", "keyboard-mouse-combo", "input-device-accessories",
    # consumer systems / cases / laptop & desktop peripherals
    "laptop-systems", "laptop-accessories", "laptop-case", "laptop-battery",
    "other-laptop-accessories", "desktop-systems", "desktop-accessories",
    "other-desktop-accessories", "workstation-systems", "workstation-accessories", "tablet-pc",
    "computer-cases", "hinges", "docking-station-port-replicator", "power-bank",
    # optical / media
    "cd-dvd-burners", "dvd-drives", "cd-drives", "blu-ray-drives", "blu-ray-burners",
    "optical-drives-burners", "external-cd-dvd-blu-ray-drives", "cd-dvd-blu-ray-accessories",
    "dvd-disk", "cd-disk", "optical-media",
    # video non-gpu
    "projectors", "projector-accessories", "video-conference", "video-accessories",
    # software / telecom
    "software-license", "network-software", "operating-systems", "softwares", "pabx-system",
}


def shard():
    idx = int(os.getenv("SHARD_INDEX") or 0)
    cnt = int(os.getenv("SHARD_COUNT") or 1)
    if cnt < 1 or not 0 <= idx < cnt:
        raise ValueError(f"bad shard: SHARD_INDEX={idx} SHARD_COUNT={cnt}")
    return idx, cnt


def get(url):
    """HTML for one URL. A failed request raises so a page is never mistaken for the end of a
    category; a response the server cuts off part-way is retried a few times first."""
    for attempt in range(1, TRIES + 1):
        try:
            r = http.session().get(url, timeout=90)
            r.raise_for_status()
            return r.text
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            if attempt == TRIES:
                raise
            print(f"WARN GET {url}: {e.__class__.__name__}, retrying in {RETRY_PAUSE}s", flush=True)
            time.sleep(RETRY_PAUSE)


def category_slugs():
    """Every leaf category slug in the sitemap, minus the EXCLUDE denylist (case-insensitive)."""
    text = get(CATEGORY_SITEMAP)
    slugs = [s for s in re.findall(r"<loc>https://www\.gotodirect\.com/([^<]+)</loc>", text)]
    return [s for s in slugs if s.lower() not in EXCLUDE]


def fetch_page(cat, pg):
    """(product cards, the category's "of N Results" total) for one listing page."""
    url = f"{BASE}/{cat}" if pg == 1 else f"{BASE}/{cat}?page={pg}"
    page_html = get(url)
    soup = BeautifulSoup(page_html, "html.parser")
    cards = soup.select(".product-shortcode.style-1")
    m = COUNT_RE.search(page_html)
    total = int(m.group(1).replace(",", "")) if m else 0
    return cards, total


def text_of(el):
    return re.sub(r"\s+", " ", el.get_text()).strip() if el else ""


def money(el):
    m = MONEY_RE.search(el.get_text()) if el else None
    return float(m.group(0).replace(",", "")) if m else None


def parse_card(cat, card, scraped_at):
    link = card.select_one("a.list-image-link") or card.select_one(".h6 a")
    href = ((link.get("href") if link else "") or "").strip()
    if not href:
        return None
    slug = href.lstrip("/")
    # Stable numeric product id (wishlist button data-id); fall back to the URL slug.
    tag = card.select_one("[data-id]")
    product_id = (tag.get("data-id") if tag else "").strip() or slug
    sku = (tag.get("data-sku") if tag else "").strip()

    img = card.select_one("a.list-image-link img")
    title_link = card.select_one(".h6 a")
    title = ((img.get("alt") if img else "") or (title_link.get("title") if title_link else "")
             or text_of(title_link)).strip()
    image_url = (img.get("src") if img else "") or None

    mpn = brand = ""
    for p in card.select(".mnp p"):
        a = p.select_one("a")
        val = ((a.get("title") or text_of(a)) if a else "").strip()
        label = text_of(p).lower()
        if label.startswith("mpn"):
            mpn = val
        elif label.startswith("brand"):
            brand = val

    cond_el = card.select_one(".shipping p[title]") or card.select_one(".shipping p")
    condition = (cond_el.get("title") if cond_el and cond_el.get("title") else text_of(cond_el))
    available = bool(card.select_one(".in_stock")) and not card.select_one(".out_of_stock")

    # Selling price = .line-through; .color is a higher list/compare price (verified against the
    # product page). Out-of-stock cards have no .price box -> price stays None (never 0).
    box = card.select_one(".price")
    price = money(box.select_one(".line-through")) if box else None
    compare = money(box.select_one(".color")) if box else None
    if price is None and compare is not None:    # only the .color number present -> it is the price
        price, compare = compare, None
    if price is None and box:
        price = money(box.select_one(".simple-article"))
    if price == 0:
        price = None

    tb = bigcommerce.capacity_tb(title)
    cap = re.search(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b", title)
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": product_id,                 # stable numeric id, deduped across categories
        "variant_id": product_id,
        "sku": sku,
        "part_number": mpn,
        "title": title,
        "brand": brand,
        "category": cat.replace("-", " ").strip().title(),
        "condition": (condition or "").title(),
        "capacity": (cap.group(1) + cap.group(2).upper()) if cap else "",
        "price": price,
        "compare_at_price": compare if compare and price and compare > price else None,
        "price_per_tb": round(price / tb, 2) if price and tb else None,
        "available": available,
        "stock_qty": None,                         # the listing shows no stock count
        "image_url": image_url,
        "description": None,                       # not on the listing card; product page only
        "url": BASE + (href if href.startswith("/") else "/" + href),
    })


def collect(cards, cat, scraped_at, by_id):
    for card in cards:
        row = parse_card(cat, card, scraped_at)
        if row:
            by_id.setdefault(row[PID], row)        # first category a product is seen in wins


def scrape_limited(limit, scraped_at):
    """A quick MAX_PRODUCTS test run: page through scoped categories until `limit` products."""
    by_id = {}
    for cat in category_slugs():
        cards, total = fetch_page(cat, 1)
        if not total:                               # landing page / empty
            continue
        pg, last = 1, math.ceil(total / PAGE_SIZE)
        while True:
            collect(cards, cat, scraped_at, by_id)
            if len(by_id) >= limit or pg >= last:
                break
            pg += 1
            cards = fetch_page(cat, pg)[0]
        if len(by_id) >= limit:
            break
    return list(by_id.values())[:limit], None


def scrape(limit, scraped_at):
    if limit:
        return scrape_limited(limit, scraped_at)

    idx, cnt = shard()
    cats = category_slugs()

    # Page 1 of every scoped category gives each one's total (and its first page of cards).
    with ThreadPoolExecutor(WORKERS) as pool:
        first = dict(zip(cats, pool.map(lambda c: fetch_page(c, 1), cats)))
    plan, page1 = [], {}
    for cat in cats:
        cards, total = first[cat]
        if not total:                               # top-level landing page, or an empty category
            continue
        plan.append((cat, total, math.ceil(total / PAGE_SIZE)))
        page1[cat] = cards
    products = sum(total for _, total, _ in plan)
    # Deterministic global unit list: (category, page, last_page, expected cards on this page).
    units = [(cat, pg, last, PAGE_SIZE if pg < last else total - PAGE_SIZE * (last - 1))
             for cat, total, last in plan for pg in range(1, last + 1)]
    print(f"{len(plan)} scoped categories, {products} products, {len(units)} pages", flush=True)

    mine = [u for i, u in enumerate(units) if i % cnt == idx]
    listed = sum(expected for _, _, _, expected in mine)
    print(f"shard {idx}/{cnt}: {len(mine)} of {len(units)} pages, {listed} expected products",
          flush=True)

    def work(unit):
        cat, pg, last, expected = unit
        cards = page1[cat] if pg == 1 else fetch_page(cat, pg)[0]
        if pg < last and len(cards) < PAGE_SIZE:    # a short page before the end = truncated fetch
            raise RuntimeError(f"{cat} page {pg}/{last}: {len(cards)} cards, expected {PAGE_SIZE}")
        return cat, cards

    by_id, done = {}, 0
    with ThreadPoolExecutor(WORKERS) as pool:
        for cat, cards in pool.map(work, mine):
            collect(cards, cat, scraped_at, by_id)
            done += 1
            if done % 500 == 0 or done == len(mine):
                print(f"{done}/{len(mine)} pages, {len(by_id)} products", flush=True)
    # `listed` = expected products for THIS shard's pages, so runner.py fails a truncated run.
    return list(by_id.values()), listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
