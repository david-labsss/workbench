"""Scrape compeve.com (Zen Cart store, ~7k products): every run's full snapshot appended to
Postgres.

Zen Cart is slow (2-5 s a page) and has no API, so the scraper walks its advanced-search listing
one page at a time (12 products a page). The listing is asked for everything priced from $0.01
up, which returns the whole catalogue including sold-out items:

  index.php?main_page=advanced_search_result&search_in_description=0&keyword=&categories_id=
  &inc_subcat=1&pfrom=0.01&pto=10000000&page=N

Each product card gives the id (from its -p-{id}.html URL), title, URL, the normal and sale
price, and whether it is in stock (an "Add" quantity box) or "Sold Out". Condition, brand and
model appear in the truncated description snippet when present and are read best-effort. The
page's "Displaying 1 to 12 (of N products)" is the count checked against. The product pages hold
cleaner Model / Manufacturer / Units in Stock, but at 2-5 s each for ~7k products that is hours,
so they are left for an optional later pass (product-page fields are optional here).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
import sys
import time

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "compeve"
BASE = "https://www.compeve.com"
PER_PAGE = 12
DELAY = 0.5              # polite pause between page requests (seconds)
PRICE_RE = re.compile(r"[\d,]+(?:\.\d+)?")
PID_RE = re.compile(r"-p-(\d+)\.html")
COUNT_RE = re.compile(r"of\s+([\d,]+)\s+products", re.I)
COND_RE = re.compile(
    r"Condition:?\s+(New Open Box|Factory Refurbished|Open Box|Refurbished|New|Used)", re.I)
BRAND_RE = re.compile(r"Manufactur(?:er|ed by):?\s+([A-Za-z0-9][\w.&\-]*)")


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def money(tag):
    m = PRICE_RE.search(tag.get_text() if tag else "")
    return float(m.group(0).replace(",", "")) if m else None


def listing_url(page):
    return (f"{BASE}/index.php?main_page=advanced_search_result&search_in_description=0"
            f"&keyword=&categories_id=&inc_subcat=1&pfrom=0.01&pto=10000000&page={page}")


def get(url):
    r = http.session().get(url, timeout=90)
    r.raise_for_status()
    return r.text


def parse_page(html, scraped_at):
    """[(product_row)] for the products on one listing page."""
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for card in soup.select("tr.productListing-odd, tr.productListing-even"):
        link = card.select_one("h3.itemTitle a")
        if not link:
            continue
        url = link.get("href", "")
        m = PID_RE.search(url)
        if not m:
            continue
        pid = m.group(1)
        normal = money(card.select_one(".normalprice"))
        special = money(card.select_one(".productSpecialPrice"))
        price = special if special is not None else normal
        compare = normal if (special is not None and normal and normal > special) else None
        in_stock = bool(card.select_one('input[name^="products_id["]')) \
            and "sold out" not in card.get_text().lower()
        desc = clean(card.select_one(".listingDescription").get_text(" ")) \
            if card.select_one(".listingDescription") else ""
        cond = COND_RE.search(desc)
        brand = BRAND_RE.search(desc)
        # part_number / model live only in the product page's clean fields (optional pass);
        # the truncated listing snippet is too inconsistent to read them from reliably.
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": pid,
            "sku": pid,
            "title": clean(link.get_text()),
            "brand": brand.group(1) if brand else "",
            "condition": cond.group(1).title() if cond else "",
            "price": price,
            "compare_at_price": compare,
            "available": in_stock,
            "url": url,
        }))
    return rows


def listed_count(html):
    m = COUNT_RE.search(re.sub(r"<[^>]+>", " ", html))   # the count is wrapped in <strong>
    return int(m.group(1).replace(",", "")) if m else None


def scrape(limit, scraped_at):
    first = get(listing_url(1))
    listed = listed_count(first)
    last_page = (listed + PER_PAGE - 1) // PER_PAGE if listed else None
    print(f"listing reports {listed} products ({last_page} pages)", flush=True)

    rows, seen = [], set()

    def add(page_rows):
        for row in page_rows:
            pid = row[schema.PRODUCT_COLUMNS.index("product_id")]
            if pid not in seen:
                seen.add(pid)
                rows.append(row)

    add(parse_page(first, scraped_at))
    page = 2
    while not (limit and len(rows) >= limit):
        if last_page and page > last_page:
            break
        try:
            page_rows = parse_page(get(listing_url(page)), scraped_at)
        except Exception as e:                       # noqa: BLE001
            sys.exit(f"listing page {page} failed: {e}")
        if not page_rows:
            break
        add(page_rows)
        if page % 25 == 0 or page == last_page:
            print(f"page {page}/{last_page}: {len(rows)} products", flush=True)
        page += 1
        time.sleep(DELAY)

    rows = rows[:limit] if limit else rows
    return rows, (listed if not limit else None)


if __name__ == "__main__":
    runner.run(SITE, scrape)
