"""Scrape serverparts4less.com (BigCommerce store, ~5,140 products): every run's full snapshot
appended to Postgres.

The store has no storefront GraphQL token in its pages (so common/bigcommerce.py cannot be used)
and its /compare/ comparison page is switched off, so products are read from the category
listing pages. Its category URLs come from the store's sitemap
(xmlsitemap.php?type=categories&page=N); each category is paged with ?limit=100&page=N (100 is
the store's max per page) until the last page. Products are deduped by their BigCommerce product
id, because a product can sit in more than one category.

Each listing card (li.product) gives the product id, URL, title (which starts with the part
number), brand, the price and any compare/sale price, and the buy button:
  "Add to Cart" link present     in stock
  "...currently out of stock"    out of stock
SKU/MPN and specs are not on the listing (the store leaves them blank); the part number is read
from the start of the title. The number of products to check the snapshot against is the product
count of the store's sitemap.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "serverparts4less"
BASE = "https://www.serverparts4less.com"
PAGE_LIMIT = 100              # products per listing page (the store's maximum)
MAX_PAGES = 300               # per-category safety cap
WORKERS = 4
PRICE_RE = re.compile(r"[\d,]+(?:\.\d+)?")
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b", re.I)
# the first title token that looks like a manufacturer part number (has a digit, 4+ chars)
PART_TOKEN_RE = re.compile(r"^[A-Za-z]*\d[A-Za-z0-9]*(?:[-/.][A-Za-z0-9]+)*$")
CONDITION_RE = re.compile(r"\b(new|refurbished|refurb|renew(?:ed)?|used|pre[- ]?owned|pull(?:ed)?)\b", re.I)


def get(url):
    r = http.session().get(url, timeout=90)
    r.raise_for_status()
    return r


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def sitemap_pages(kind):
    """The page numbers the sitemap index lists for one entry type."""
    text = get(f"{BASE}/xmlsitemap.php").text
    pages = re.findall(rf"xmlsitemap\.php\?type={kind}&(?:amp;)?page=(\d+)", text)
    return sorted(set(pages), key=int)


def sitemap_count():
    """Product URLs across the store's product sitemap pages (its full catalogue size)."""
    total = 0
    for page in sitemap_pages("products"):
        total += get(f"{BASE}/xmlsitemap.php?type=products&page={page}").text.count("<loc>")
    return total


def categories():
    """Every category URL in the store's category sitemap."""
    urls = []
    for page in sitemap_pages("categories"):
        text = get(f"{BASE}/xmlsitemap.php?type=categories&page={page}").text
        urls += [u.replace("&amp;", "&") for u in re.findall(r"<loc>(.*?)</loc>", text)]
    return urls


def money(card, attr):
    el = card.select_one(f"[{attr}]")
    m = PRICE_RE.search(el.get_text() if el else "")
    return float(m.group(0).replace(",", "")) if m else None


def product_id(card):
    el = card.select_one("[data-product-id]")
    if el and (el.get("data-product-id") or "").isdigit():
        return el["data-product-id"]
    m = re.search(r"product_id=(\d+)", str(card))        # cart / out-of-stock links
    if m:
        return m.group(1)
    m = re.search(r"/products/(\d+)/", str(card))         # image path
    return m.group(1) if m else None


def part_number(title):
    for token in title.split():
        t = token.strip("*()[]:,")
        if len(t) >= 4 and any(c.isdigit() for c in t) and PART_TOKEN_RE.match(t):
            return t
    return ""


def parse_cards(html):
    """[{fields}] for every product card on a listing page."""
    soup = BeautifulSoup(html, "html.parser")
    products = []
    for card in soup.select("li.product"):
        pid = product_id(card)
        link = card.select_one("h4.card-title a")
        if not pid or not link:
            continue
        title = clean(link.get_text())
        price = money(card, "data-product-price-without-tax")
        compare = max((x for x in (money(card, "data-product-rrp-price-without-tax"),
                                   money(card, "data-product-non-sale-price-without-tax")) if x),
                      default=None)
        brand = card.select_one('[data-test-info-type="brandName"]')
        in_stock = bool(card.select_one('a[href*="cart.php?action=add"]'))
        cond = CONDITION_RE.search(title)
        products.append({
            "id": pid,
            "title": title,
            "url": clean(link.get("href")),
            "brand": clean(brand.get_text()) if brand else "",
            "price": price,
            "compare": compare if compare and price and compare > price else None,
            "available": in_stock,
            "condition": cond.group(0).title() if cond else "",
        })
    return products


def fetch_category(url, limit):
    """Every product card in one category, paging until the last page."""
    found = {}
    for page in range(1, MAX_PAGES + 1):
        html = get(f"{url}?limit={PAGE_LIMIT}&page={page}").text
        cards = parse_cards(html)
        new = [c for c in cards if c["id"] not in found]
        for c in new:
            found[c["id"]] = c
        if not cards or not new or len(cards) < PAGE_LIMIT:
            break
        if limit and len(found) >= limit:
            break
    return found


def build_row(p, scraped_at):
    cap = CAPACITY_RE.search(p["title"])
    capacity = f"{cap.group(1)}{cap.group(2).upper()}" if cap else ""
    tb = None
    if cap:
        tb = float(cap.group(1)) if cap.group(2).upper() == "TB" else float(cap.group(1)) / 1000
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": str(p["id"]),
        "part_number": part_number(p["title"]),
        "title": p["title"],
        "brand": p["brand"],
        "category": "",
        "condition": p["condition"],
        "capacity": capacity,
        "price": p["price"],
        "compare_at_price": p["compare"],
        "price_per_tb": round(p["price"] / tb, 2) if p["price"] and tb else None,
        "available": p["available"],
        "stock_qty": None,                 # not shown on the listing
        "url": p["url"],
    })


def scrape(limit, scraped_at):
    listed = sitemap_count()
    print(f"sitemap lists {listed} products", flush=True)
    cats = categories()
    print(f"{len(cats)} categories", flush=True)
    by_id = {}
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, found in enumerate(pool.map(lambda c: fetch_category(c, limit), cats), 1):
            by_id.update(found)
            if i % 5 == 0 or i == len(cats):
                print(f"{i}/{len(cats)} categories, {len(by_id)} unique products", flush=True)
            if limit and len(by_id) >= limit:
                break
    items = list(by_id.values())[:limit] if limit else list(by_id.values())
    return [build_row(p, scraped_at) for p in items], listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
