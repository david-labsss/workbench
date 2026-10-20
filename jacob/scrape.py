"""Scrape jacob.de (JACOB, Shopware 6, Germany): every run's full snapshot appended to Postgres.
Prices are in euros (EUR), stored as-is.

jacob.de is a ~1,000,000-SKU GENERAL electronics catalogue (phones, TVs, appliances, ...), so this
scraper is SCOPED to the server / storage / networking categories only -- never the whole shop.
The scope is the fixed list of Shopware category slugs in CATEGORIES below (servers, storage /
SSD / HDD, server memory, and networking: infrastructure, cards, switches, cabling). Add or remove
a slug there to widen or narrow the scope.

ROBOTS-CLEAN PATH. jacob's robots.txt has `Disallow: /*?`, so every URL with a query string is
forbidden -- including the storefront's own `?p=N` listing pagination. We therefore never use a
`?` URL. Shopware's Store API (/store-api/product) would be ideal but is key-gated (returns 401
"Header sw-access-key is required") and jacob exposes no sw-access-key in its HTML or theme JS, so
that path is unavailable. Instead we use the storefront's AJAX listing endpoint, which robots.txt
explicitly allows (`Allow: /widgets/cms/`) and which takes the page number in the POST *body*
(no query string):

  1. GET /kategorie/<slug>/            (robots-allowed, no `?`) -- a server-rendered Shopware
     listing whose <script> sets `window.activeNavigationId = '<32-hex category id>'` and whose
     headline states the category size ("414 Produkte" / "414 Artikel"), fixing the page count.
  2. POST /widgets/cms/navigation/<navigationId>  with form body {p:N, order:name-asc,
     no-aggregations:1} -- returns the listing fragment for page N (24 product cards). `order` is
     fixed to name-asc so pagination is deterministic and gap-free. Pages past the last wrap back
     to page 1, so we bound strictly by ceil(count / 24) and also stop a category as soon as a
     whole batch of pages adds no new product ids.

Every product card (.product-box) carries a data-product-information JSON blob (Shopware id, name,
brand, price as a number) plus a /produkt/<slug>-<id>/ link whose trailing <id> is the stable
article number used as product_id, and a .jacob-delivery-text stock line. Products are
de-duplicated by that id across the (overlapping) scoped categories.

There is no single reliable "server/storage" total, so `listed` is None; the completeness guard in
common/runner.py is therefore skipped for this site (expected for a scoped crawl).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import html as html_lib
import json
import math
import re
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "jacob"
BASE = "https://www.jacob.de"
WORKERS = 6
PER_PAGE = 24
PID_IDX = schema.PRODUCT_COLUMNS.index("product_id")
# robots-clean listing fragment endpoint; page number goes in the POST body, never a query string
LISTING_HEADERS = {"X-Requested-With": "XMLHttpRequest"}

# Server / storage / networking scope only -- NOT the full jacob.de catalogue.
CATEGORIES = [
    "server",                       # servers
    "storage",                      # storage systems
    "speicher-festplatten",         # storage & hard drives (SSD / HDD)
    "festplatten-ssd-outlet",       # HDD / SSD outlet
    "arbeitsspeicher-ram",          # memory / RAM
    "arbeitsspeicher-outlet",       # memory / RAM outlet
    "netzwerk-serverinfrastruktur",  # network & server infrastructure
    "netzwerkkarten",               # network cards
    "switches",                     # switches
    "netzwerkgeraete-signaltechnik",  # network devices
    "netzwerkkabel",                # network cabling
    "speicherkabel",                # storage cabling
]

ID_RE = re.compile(r"-(\d+)/?$")
NAV_RE = re.compile(r"activeNavigationId\s*=\s*'([0-9a-f]{32})'")
COUNT_RE = re.compile(r"([\d.]+)\s*(?:&nbsp;|\s)*(?:Produkte|Artikel)", re.I)
EUR_RE = re.compile(r"(\d{1,3}(?:\.\d{3})*|\d+),(\d{2})")   # German "1.234,56"


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def eur(text):
    m = EUR_RE.search(text or "")
    return float(m.group(1).replace(".", "") + "." + m.group(2)) if m else None


def get(url):
    try:
        r = http.session().get(url, timeout=90)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.text
    except Exception as e:  # noqa: BLE001 - one bad page must not stop the run
        print(f"WARN {url}: {e}", flush=True)
        return None


def fetch_listing(args):
    """POST the robots-allowed listing endpoint for one page (page number in the body)."""
    nav, page = args
    url = f"{BASE}/widgets/cms/navigation/{nav}"
    try:
        r = http.session().post(url, headers=LISTING_HEADERS, timeout=90,
                                data={"p": page, "order": "name-asc", "no-aggregations": 1})
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.text
    except Exception as e:  # noqa: BLE001 - one bad page must not stop the run
        print(f"WARN listing {nav} p{page}: {e}", flush=True)
        return None


def parse_count(html):
    m = COUNT_RE.search(html or "")
    return int(m.group(1).replace(".", "")) if m else None


def parse_boxes(html, category, scraped_at):
    soup = BeautifulSoup(html or "", "html.parser")
    rows = []
    for box in soup.select(".product-box"):
        info = box.get("data-product-information")
        data = {}
        if info:
            try:
                data = json.loads(html_lib.unescape(info))
            except ValueError:
                data = {}
        a = box.select_one("a.jacob-product-name") or box.select_one('a[href*="/produkt/"]')
        url = a["href"] if a and a.get("href") else ""
        m = ID_RE.search(url)
        product_id = m.group(1) if m else clean(str(data.get("id") or ""))
        if not product_id:
            continue
        price = data.get("price")
        if price is None:
            price = eur(box.select_one(".product-price").get_text()
                        if box.select_one(".product-price") else "")
        delivery = box.select_one(".jacob-delivery-text")
        dtext = clean(delivery.get_text()).lower() if delivery else ""
        available = True if not dtext else ("lieferbar" in dtext and "nicht" not in dtext)
        title = clean(data.get("name")) or (clean(a.get("title")) if a else "")
        rows.append(schema.product_row({
            "scraped_at": scraped_at, "website": SITE,
            "product_id": product_id, "sku": product_id,
            "title": title,
            "brand": clean(data.get("brand")),
            "category": category,
            "price": price,
            "available": available,
            "other_specs": dtext,
            "url": url,
        }))
    return rows


def scrape(limit, scraped_at):
    rows, seen = [], set()
    with ThreadPoolExecutor(WORKERS) as pool:
        for slug in CATEGORIES:
            meta = get(f"{BASE}/kategorie/{slug}/")
            if meta is None:
                print(f"WARN category {slug}: listing page unreachable", flush=True)
                continue
            nav = NAV_RE.search(meta)
            if not nav:
                print(f"WARN category {slug}: no navigation id found", flush=True)
                continue
            nav = nav.group(1)
            count = parse_count(meta)
            pages = math.ceil(count / PER_PAGE) if count else 10_000
            added = 0
            # pages 1..N via the robots-clean POST endpoint, a chunk at a time, stopping as soon as
            # a whole batch adds no new ids (overflow pages wrap back to page 1, so never go empty)
            page, done = 1, False
            while page <= pages and not done and not (limit and len(rows) >= limit):
                batch = range(page, min(page + WORKERS, pages + 1))
                batch_new = 0
                for html in pool.map(fetch_listing, [(nav, p) for p in batch]):
                    for row in parse_boxes(html, slug, scraped_at):
                        pid = row[PID_IDX]
                        if pid and pid not in seen:
                            seen.add(pid)
                            rows.append(row)
                            added += 1
                            batch_new += 1
                if batch_new == 0:
                    done = True
                page += WORKERS
            print(f"{slug}: {count} listed, +{added} new (total {len(rows)})", flush=True)
            if limit and len(rows) >= limit:
                break
    return (rows[:limit] if limit else rows), None


if __name__ == "__main__":
    runner.run(SITE, scrape)
