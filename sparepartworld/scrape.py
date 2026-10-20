"""Scrape www.sparepartworld.com (IPC-Computer's English spare-parts store, Gambio/osCommerce):
every run's full snapshot of the storage / server / component categories, appended to Postgres.

The store has no public product API and no product sitemap, so each category's listing pages are
read directly. A category page (/laptop-spare-parts/<subcategory>?page=N) renders its real products
inside <div id="product-list"> -- 20 per page, each a <div class="product-item"> tile with the
product URL (its trailing number is the product id), name, price (a <meta itemprop="price">),
currency symbol and in-stock state. (Other .product-item tiles on the page are "recommended" /
"recently viewed" carousels and are ignored -- only #product-list's direct children are products.)
A hidden .product-listing-count gives the subcategory's total, used to page 1..N and to confirm the
scrape is complete. Only storage / server / component subcategories are scraped; MAX_PRODUCTS caps
test runs.

Prices are the amounts the site serves us; it is an EU store (VAT shown) that quotes international
visitors in US dollars, so the currency symbol is read per tile and reported. Run steps, changes and
env vars: common/runner.py. Columns: common/schema.py.
"""
import math
import re
import time

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "sparepartworld"
BASE = "https://www.sparepartworld.com"
HUB = "/laptop-spare-parts"
PAGE_DELAY = 0.4

# storage / server / component subcategories (of 44 under /laptop-spare-parts)
SUBCATS = [
    "server-hdd", "server-parts", "hard-drives", "memory",
    "optical-drives", "graphics-cards", "mainboards",
]
CURRENCY = {"$": "USD", "€": "EUR", "£": "GBP"}
ID_RE = re.compile(r"-(\d+)(?:/nup)?/?$")
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB|MB)\b", re.I)
ALT_PREFIX_RE = re.compile(r"^(?:alternative|substitute|replacement)?\s*(?:for|für)\s+", re.I)
QTY_RE = re.compile(r"(\d+)\s*pieces", re.I)
# Manufacturers common to server/storage/component parts; title word order varies (brand may
# come before or after the part number), so the title is scanned for a known brand.
KNOWN_BRANDS = {
    "dell", "hp", "hpe", "hewlett", "lenovo", "ibm", "fujitsu", "toshiba", "samsung", "intel",
    "micron", "kingston", "crucial", "seagate", "western", "wd", "hgst", "sandisk", "hynix",
    "supermicro", "cisco", "netapp", "emc", "qnap", "synology", "asus", "asrock", "gigabyte",
    "msi", "nvidia", "amd", "broadcom", "lsi", "avago", "adaptec", "mellanox", "dahua", "kioxia",
    "transcend", "adata", "corsair", "pny", "zotac", "sapphire", "acer", "apacer", "innodisk",
}


def clean(v):
    return re.sub(r"\s+", " ", str(v or "")).strip()


def brand_and_part(title):
    """Guess the manufacturer and part number from a tile title like "Dell 400-AZTN 1.92TB SSD...",
    "Alternative for Samsung MZ-77Q1T0BW SSD 1TB" or "S26361-F5543-L112 Fujitsu Server hard drive"
    -- brand = the first known manufacturer in the title, part = the first part-number-like word."""
    toks = ALT_PREFIX_RE.sub("", title).split()
    brand = next((t for t in toks[:5] if t.lower().strip(".,") in KNOWN_BRANDS), "")
    part = next((t for t in toks[:5]
                 if re.search(r"\d", t) and re.fullmatch(r"[A-Za-z0-9./\-]{3,}", t)
                 and not CAPACITY_RE.fullmatch(t) and t.lower() not in KNOWN_BRANDS), "")
    return brand, part


def fetch(url):
    r = http.session().get(url, timeout=60)
    r.raise_for_status()
    time.sleep(PAGE_DELAY)
    return BeautifulSoup(r.text, "html.parser")


def listing_total(soup):
    el = soup.select_one(".product-listing-count")
    m = re.search(r"\d[\d,]*", el.get_text()) if el else None
    return int(m.group(0).replace(",", "")) if m else None


def tiles(soup):
    """The real product tiles: direct children of #product-list only."""
    pl = soup.select_one("#product-list")
    return pl.select(":scope > div.product-item") if pl else []


def parse_tile(tile, subcat, scraped_at):
    a = tile.select_one("a.stretched-link") or tile.select_one("a[href]")
    if not a or not a.get("href"):
        return None
    url = a["href"]
    if url.startswith("/"):
        url = BASE + url
    m = ID_RE.search(url.rstrip("/"))
    if not m:
        return None
    pid = m.group(1)
    h4 = tile.select_one("h4.title")
    title = clean(h4.get_text()) if h4 else clean(a.get("title"))
    pm = tile.select_one('meta[itemprop="price"]')
    price = None
    if pm and pm.get("content"):
        try:
            price = float(pm["content"])
        except ValueError:
            price = None
    cur_el = tile.select_one(".price-currency")
    cur_sym = clean(cur_el.get_text()) if cur_el else ""
    currency = CURRENCY.get(cur_sym, cur_sym)
    classes = tile.get("class") or []
    av_text = clean((tile.select_one(".availability") or tile).get_text(" ")).lower()
    if "out of stock" in av_text or "not available" in av_text or "not-available" in classes:
        available = False
    elif "in stock" in av_text or "available" in classes:
        available = True
    else:
        available = "available" in classes
    qty = QTY_RE.search(av_text)
    brand, part = brand_and_part(title)
    cap = CAPACITY_RE.search(title)
    img_el = tile.select_one(".image img") or tile.select_one("img")
    image = (img_el.get("src") or img_el.get("data-src")) if img_el else None
    if image:
        image = "https:" + image if image.startswith("//") else (
            BASE + image if image.startswith("/") else image)
    # The listing tile shows only the title (no description block), so description is None.
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": pid,
        "part_number": part,
        "title": title,
        "brand": brand,
        "category": subcat,
        "capacity": (cap.group(1) + cap.group(2).upper()) if cap else "",
        "other_specs": f"Price currency: {currency}" if (price is not None and currency) else "",
        "price": price,
        "available": available,
        "stock_qty": float(qty.group(1)) if qty else None,
        "image_url": image,
        "description": None,
        "url": url,
    })


def scrape_subcat(subcat, limit, scraped_at):
    """(rows, total) for one subcategory, paging until the listing is exhausted."""
    first = fetch(f"{BASE}{HUB}/{subcat}")
    total = listing_total(first)
    first_tiles = tiles(first)
    if not first_tiles:
        print(f"WARN {subcat}: no products on page 1", flush=True)
        return [], 0
    per_page = len(first_tiles)
    max_page = math.ceil(total / per_page) if total else 1
    print(f"{subcat}: {total} products, ~{per_page}/page, {max_page} pages", flush=True)
    rows, seen = [], set()

    def take(tile_list):
        for t in tile_list:
            row = parse_tile(t, subcat, scraped_at)
            if row:
                pid = row[schema.PRODUCT_COLUMNS.index("product_id")]
                if pid not in seen:
                    seen.add(pid)
                    rows.append(row)

    take(first_tiles)
    page = 2
    while page <= max_page + 1 and not (limit and len(rows) >= limit):
        batch = tiles(fetch(f"{BASE}{HUB}/{subcat}?page={page}"))
        before = len(seen)
        take(batch)
        if not batch or len(seen) == before:     # empty page or no new products -> done
            break
        page += 1
    if total and not limit and len(rows) < 0.9 * total:
        raise RuntimeError(f"{subcat}: only {len(rows)} of {total} products; likely a bad fetch")
    return rows, (total or len(rows))


def scrape(limit, scraped_at):
    rows, listed, seen = [], 0, set()
    for subcat in SUBCATS:
        sub_rows, total = scrape_subcat(subcat, limit, scraped_at)
        listed += total
        for row in sub_rows:
            pid = row[schema.PRODUCT_COLUMNS.index("product_id")]
            if pid not in seen:                   # de-dupe across subcategories
                seen.add(pid)
                rows.append(row)
        if limit and len(rows) >= limit:
            rows = rows[:limit]
            break
    # listed = sum of each subcategory's own count; per-subcategory completeness is checked above
    return rows, (None if limit else listed)


if __name__ == "__main__":
    runner.run(SITE, scrape)
