"""Scrape ipc-computer.eu (IPC-Computer Deutschland, DE/EU laptop spare parts): every run's full
snapshot appended to Postgres.

ipc-computer.eu is a custom PHP shop (nginx, no Cloudflare) whose English catalogue is browsed as
category listing pages that are fully server-rendered: each product card carries the product URL
(ending in a numeric id), the title (part number + brand), the price as <meta itemprop="price">
in euros, and an availability block. There is no public product API and the .eu site has no XML
sitemap, so the fastest complete public path is the category-listing crawl: discover the
categories under /laptop-spare-parts/, then page through each (?page=N) reading 50 cards a page,
deduping by product id. No per-product page fetch is needed, which keeps it efficient.

Per card: product_id = the trailing numeric id in the URL; part_number and brand are read from the
title; price from the itemprop meta (EUR); available is true only for in-stock cards (green
quantity badge) -- "order related" items are listed with a price but not in stock, and the badge
text is kept in other_specs. category is the listing category's name.

NOTE ON SCALE: the catalogue is very large (a part per laptop model; keyboards alone list ~270k,
spareparts ~107k). A full run pages through every category and is only practical at a slow cadence.
Categories are discovered from the /laptop-spare-parts/ hub plus any sibling links found while
crawling, seeded with the "spareparts" catch-all (which the hub nav omits). Prices are in euros
(EUR); the price number is stored as-is.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import html
import re
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "ipccomputer"
BASE = "https://www.ipc-computer.eu"
HUB = "/laptop-spare-parts"
WORKERS = 6
CURRENCY = "EUR"

CAT_RE = re.compile(r'/laptop-spare-parts/([a-z0-9][a-z0-9-]*)(?=["/?#])', re.I)
PAGE_RE = re.compile(r"[?&]page=(\d+)")
ID_RE = re.compile(r"-(\d+)(?:[/?#]|$)")
BRANDS = [
    "Acer", "ASUS", "Asus", "Dell", "HP", "Hewlett-Packard", "Lenovo", "IBM", "Toshiba", "Sony",
    "Samsung", "MSI", "Fujitsu Siemens", "Fujitsu", "Apple", "Medion", "Packard Bell", "Gigabyte",
    "Razer", "LG", "Huawei", "Microsoft", "Panasonic", "Clevo", "Gateway", "eMachines", "Compaq",
    "Terra", "Wortmann", "Dynabook", "Vaio",
]
BRAND_RE = re.compile(r"\b(" + "|".join(re.escape(b) for b in BRANDS) + r")\b", re.I)
# titles read "<part> <brand> ..." or "Alternative/Original for <part> <brand> ..."
PART_PREFIX_RE = re.compile(r"^(?:alternative|original|replacement|compatible)(?:\s+for)?\s+", re.I)


def clean(text):
    return re.sub(r"\s+", " ", html.unescape((text or "").replace("\xa0", " "))).strip()


def to_float(v):
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


def category_paths(html_text):
    """`/laptop-spare-parts/<cat>` listing paths linked on a page (not product URLs, which have a
    further /sparepart-... segment)."""
    return {f"{HUB}/{m.group(1)}" for m in CAT_RE.finditer(html_text)
            if m.group(1).lower() != "sparepart"}


def page_url(cat_path, n):
    # robots disallows ?page=0 and ?page=1$, so page 1 is the bare category URL
    return BASE + cat_path if n <= 1 else f"{BASE}{cat_path}?page={n}"


def max_page(html_text):
    pages = [int(x) for x in PAGE_RE.findall(html_text)]
    return max(pages) if pages else 1


def brand_of(title):
    m = BRAND_RE.search(title or "")
    if not m:
        return ""
    b = m.group(1)
    return {"HP": "HP", "IBM": "IBM", "MSI": "MSI", "LG": "LG", "ASUS": "ASUS"}.get(
        b.upper(), b if b.isupper() else b.title())


def parse_cards(html_text, category, scraped_at):
    soup = BeautifulSoup(html_text, "html.parser")
    rows = []
    for item in soup.select("div.product-item"):
        a = item.find("a", href=True)
        if not a:
            continue
        href = a["href"]
        m = ID_RE.search(href)
        if not m:
            continue
        pid = m.group(1)
        title_el = item.select_one(".title")
        title = clean(title_el.get_text(" ")) if title_el else clean(a.get("title"))
        price_meta = item.select_one('meta[itemprop="price"]')
        price = to_float(price_meta.get("content")) if price_meta else None
        avail_el = item.select_one(".availability")
        avail_text = clean(avail_el.get_text(" ")) if avail_el else ""
        in_stock = bool(item.select_one(".qty-green")) or "in stock" in avail_text.lower()
        url = href if href.startswith("http") else BASE + href
        part = PART_PREFIX_RE.sub("", title).split(" ")[0] if title else ""
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": pid,
            "part_number": part,
            "title": title,
            "brand": brand_of(title),
            "category": category,
            "other_specs": f"Availability: {avail_text}" if avail_text else "",
            "price": price,
            "available": in_stock,
            "url": url,
        }))
    return rows


def fetch_page(url):
    try:
        r = http.session().get(url, timeout=60)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return url, r.text
    except Exception as e:  # noqa: BLE001 - one bad page must not stop the run
        print(f"WARN {url}: {e}", flush=True)
        return None


def discover_categories():
    r = http.session().get(BASE + HUB, timeout=60)
    r.raise_for_status()
    cats = category_paths(r.text)
    cats.add(f"{HUB}/spareparts")        # catch-all: present in product URLs, not the hub nav
    cats.discard(HUB)
    return cats


def scrape(limit, scraped_at):
    queue = sorted(discover_categories())
    print(f"starting from {len(queue)} categories: "
          f"{[c.rsplit('/', 1)[-1] for c in queue]}", flush=True)
    done, rows, seen = set(), [], set()
    pid_idx = schema.PRODUCT_COLUMNS.index("product_id")

    def add(cards):
        for row in cards:
            if row[pid_idx] not in seen:
                seen.add(row[pid_idx])
                rows.append(row)

    while queue:
        cat = queue.pop(0)
        if cat in done:
            continue
        done.add(cat)
        name = cat.rsplit("/", 1)[-1]
        first = fetch_page(page_url(cat, 1))
        if not first:
            continue
        for c in category_paths(first[1]):          # discover sibling categories as we go
            if c != HUB and c not in done and c not in queue:
                queue.append(c)
        add(parse_cards(first[1], name, scraped_at))
        last = max_page(first[1])
        print(f"category {name}: {last} pages", flush=True)
        if limit and len(rows) >= limit:
            break
        pages = list(range(2, last + 1))
        with ThreadPoolExecutor(WORKERS) as pool:
            for page in pool.map(lambda n: fetch_page(page_url(cat, n)), pages):
                if page:
                    add(parse_cards(page[1], name, scraped_at))
                if limit and len(rows) >= limit:
                    break
        if limit and len(rows) >= limit:
            break
    print(f"{len(rows)} products from {len(done)} categories", flush=True)
    # Overlapping categories and an unknown exact total: rely on dedup + raise-on-fetch-error
    return rows, None


if __name__ == "__main__":
    runner.run(SITE, scrape)
