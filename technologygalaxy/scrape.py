"""Scrape technologygalaxy.com (ColdFusion, ~40,120 products): every run's full snapshot appended
to Postgres.

The store has no public product API. /sitemap.xml lists every product (links of the form
/<slug>/p/<id>) and gives the authoritative product count; /sitemap2.xml lists every category
(/<slug>/c/<id>). Only leaf categories list products - parent categories show sub-category
tiles and no items - so the whole catalogue is covered by walking every category: parents
return nothing, leaves return their products. Each category page shows up to 60 products and a
total ("<n> products found"); page 2 onwards is the same URL with &pag=<n> appended, walked
until a page returns no new products. Products are deduped by id across categories.

Each product card gives the product id and slug (the brand is the slug's first segment), the
title, the SKU (= manufacturer part number), the price, and the stock line ("Stock: YES, 5
Units" when in stock, with the quantity, or "Stock: Backordered" when not).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "technologygalaxy"
BASE = "https://www.technologygalaxy.com"
WORKERS = 4
MAX_PAGES = 200  # safety cap per category (the largest categories are far smaller)
PID = schema.PRODUCT_COLUMNS.index("product_id")

PID_RE = re.compile(r"/p/(\d+)")
STOCK_RE = re.compile(r"Stock:\s*(.+?)\s*$", re.I)
QTY_RE = re.compile(r"\byes\b.*?(\d[\d,]*)\s*units?", re.I)
PRICE_RE = re.compile(r"[\d,]+\.\d{2}")


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def to_float(text):
    m = PRICE_RE.search(text or "")
    return float(m.group(0).replace(",", "")) if m else None


def absolute(u):
    """Absolute https URL for a possibly relative/protocol-relative one, else None."""
    u = (u or "").strip()
    if not u:
        return None
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return BASE + u
    return u


def card_image(item):
    """The listing card's thumbnail URL, absolute, or None. The .product_image div is populated
    client-side on most pages, so the fetched HTML usually has no <img> and this returns None."""
    img = item.find("img")
    if not img:
        return None
    src = img.get("data-src") or img.get("src") or ""
    return None if src.startswith("data:") else absolute(src)


def sitemap_count():
    """How many products the catalogue lists (the authoritative total)."""
    r = http.session().get(f"{BASE}/sitemap.xml", timeout=120)
    r.raise_for_status()
    return len(set(re.findall(r"/p/(\d+)</loc>", r.text)))


def category_urls():
    r = http.session().get(f"{BASE}/sitemap2.xml", timeout=120)
    r.raise_for_status()
    return sorted(set(re.findall(r"<loc>([^<]+/c/\d+)</loc>", r.text)))


def breadcrumb_category(soup):
    """The top-level category (first breadcrumb step after Home) of a category page."""
    loc = soup.select_one(".location")
    if not loc:
        return ""
    crumbs = [clean(a.get_text()) for a in loc.find_all("a")]
    crumbs = [c for c in crumbs if c and c.lower() != "home"]
    return crumbs[0] if crumbs else ""


def parse_card(item, scraped_at, category=""):
    desc = item.select_one(".product_desc")
    links = desc.find_all("a") if desc else []
    if not links:
        return None
    href = links[0].get("href", "")
    m = PID_RE.search(href)
    if not m:
        return None
    slug = href.strip("/").split("/")[0]
    stock_m = STOCK_RE.search(clean(desc.get_text(" ")))
    stock_text = stock_m.group(1) if stock_m else ""
    qty_m = QTY_RE.search(stock_text)
    price_el = item.select_one(".product_price")
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": m.group(1),
        "sku": clean(links[1].get_text()) if len(links) > 1 else "",
        "part_number": clean(links[1].get_text()) if len(links) > 1 else "",
        "title": clean(links[0].get_text()),
        "brand": slug.split("-")[0] if slug else "",
        "category": category,
        "price": to_float(price_el.get_text()) if price_el else None,
        "available": "yes" in stock_text.lower(),
        "stock_qty": float(qty_m.group(1).replace(",", "")) if qty_m else None,
        "image_url": card_image(item),
        # description lives only on the product page; the listing (all we fetch) carries none,
        # so it is left blank - fetching ~40k product pages just for it would be far too slow.
        "description": None,
        "url": href if href.startswith("http") else BASE + href,
    })


def walk_category(url, scraped_at):
    """Every product card in a category, across its pages. Stops at the first page that adds
    no new product (an out-of-range &pag returns an empty listing)."""
    rows, seen, category = [], set(), ""
    for pg in range(1, MAX_PAGES + 1):
        page_url = url + (f"&pag={pg}" if pg > 1 else "")
        try:
            r = http.session().get(page_url, timeout=60)
            r.raise_for_status()
        except Exception as e:  # noqa: BLE001 - one bad page must not stop the run
            print(f"WARN {page_url}: {e}", flush=True)
            break
        soup = BeautifulSoup(r.text, "html.parser")
        items = soup.select(".product_item")
        if pg == 1:
            category = breadcrumb_category(soup)
        added = 0
        for item in items:
            row = parse_card(item, scraped_at, category)
            if row and row[PID] not in seen:
                seen.add(row[PID])
                rows.append(row)
                added += 1
        if added == 0:
            break
    return rows


def scrape(limit, scraped_at):
    listed = sitemap_count()
    print(f"sitemap lists {listed} products", flush=True)
    cats = category_urls()
    print(f"{len(cats)} categories", flush=True)

    rows, seen = [], set()

    def collect(cat_rows):
        for row in cat_rows:
            if row[PID] not in seen:
                seen.add(row[PID])
                rows.append(row)

    if limit:
        # Test runs: walk categories one at a time and stop as soon as we have enough.
        for cat in cats:
            collect(walk_category(cat, scraped_at))
            if len(rows) >= limit:
                break
        return rows[:limit], listed

    with ThreadPoolExecutor(WORKERS) as pool:
        for i, cat_rows in enumerate(pool.map(lambda c: walk_category(c, scraped_at), cats), 1):
            collect(cat_rows)
            if i % 200 == 0 or i == len(cats):
                print(f"{i}/{len(cats)} categories, {len(rows)} products", flush=True)
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
