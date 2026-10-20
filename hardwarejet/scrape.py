"""Scrape hardwarejet.com (OpenCart, ~57,653 products): every run's full snapshot appended to Postgres.

The catalogue listing (route=product/catalog) returns 500 products a page and, for each, the
product id, name, URL, manufacturer and price - everything we map here. The page header
("Showing X to Y of Z (N Pages)") gives the total and the number of pages to walk.

Only the listing is read: model number, condition and the real stock quantity live on the
individual product pages (and the listing's "In Stock" label is not reliable), so those are
left blank. $0.00 means the item is quote-only, so its price is left empty too. A few cards
also print the manufacturer part number and product type inside the description; those are
picked up when present.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re

from common import http, runner, schema

SITE = "hardwarejet"
BASE = "https://www.hardwarejet.com"
# 500 per page keeps the whole walk to ~116 polite requests.
LISTING = BASE + "/en?route=product/catalog&limit=500&page={}"

CARD_RE = re.compile(r'data-product-id="(\d+)"')
SHOWING_RE = re.compile(r"Showing\s+[\d,]+\s+to\s+[\d,]+\s+of\s+([\d,]+)\s+\((\d+)\s+Pages?\)", re.I)
NAME_RE = re.compile(r'class="name">\s*<a\s+href="([^"]+)"[^>]*?title="([^"]*)"', re.S | re.I)
MANUF_RE = re.compile(r'Manufacturer:\s*</span>\s*<span>\s*<a[^>]*>([^<]+)</a>', re.S | re.I)
PRICE_NEW_RE = re.compile(r'price-new">\s*([^<]+?)\s*</span>', re.I)
PRICE_OLD_RE = re.compile(r'price-old">\s*([^<]+?)\s*</span>', re.I)
PRICE_NORM_RE = re.compile(r'price-normal">\s*([^<]+?)\s*</span>', re.I)
DESC_MPN_RE = re.compile(r'Manufacturer Part Number:\s*([^\n<]+)', re.I)
DESC_TYPE_RE = re.compile(r'Product Type:\s*([^\n<]+)', re.I)
# The card's lazy-loaded thumbnail keeps the real image in data-src (src is a data: placeholder).
IMG_RE = re.compile(r'<img[^>]+(?:data-src|src)="(https?://[^"]+)"', re.I)


def clean(text):
    import html
    text = html.unescape(text or "").replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def to_price(text):
    """A float, or None for quote-only items ($0.00 / no price)."""
    if not text:
        return None
    m = re.search(r"[\d,]+\.\d{2}", text)
    if not m:
        return None
    val = float(m.group(0).replace(",", ""))
    return val or None  # 0.00 -> quote-only


def cards(html_text):
    """Each product card block (from one data-product-id up to the next)."""
    starts = [m.start() for m in CARD_RE.finditer(html_text)]
    for i, start in enumerate(starts):
        # back up to the opening <div of the product-layout wrapper the id sits on
        open_div = html_text.rfind("<div", 0, start)
        end = starts[i + 1] if i + 1 < len(starts) else len(html_text)
        yield html_text[open_div:end]


def parse_card(block, scraped_at):
    pid = CARD_RE.search(block)
    name = NAME_RE.search(block)
    if not pid or not name:
        return None
    url = name.group(1).split("?")[0]
    manuf = MANUF_RE.search(block)
    new, norm = PRICE_NEW_RE.search(block), PRICE_NORM_RE.search(block)
    old = PRICE_OLD_RE.search(block)
    if new:
        price, compare = to_price(new.group(1)), to_price(old.group(1) if old else "")
    else:
        price, compare = to_price(norm.group(1) if norm else ""), None
    mpn = DESC_MPN_RE.search(block)
    ptype = DESC_TYPE_RE.search(block)
    img = IMG_RE.search(block)
    image_url = img.group(1) if img else None
    if image_url and "placeholder" in image_url.lower():
        image_url = None                 # the store's "no image" placeholder, not a real photo
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": pid.group(1),
        "title": clean(name.group(2)),
        "brand": clean(manuf.group(1)) if manuf else "",
        "category": clean(ptype.group(1)) if ptype else "",
        "sku": clean(mpn.group(1)) if mpn else "",
        "part_number": clean(mpn.group(1)) if mpn else "",
        "price": price,
        "compare_at_price": compare if (price and compare and compare > price) else None,
        "image_url": image_url,
        # description lives only on the product page; the listing (all we fetch) has none, so
        # it is left blank - fetching 57k product pages just for it would be far too slow.
        "description": None,
        "url": url,
        # stock_qty / condition / in_stock only on the product pages, left blank on purpose
    })


def get(url):
    r = http.session().get(url, timeout=90)
    r.raise_for_status()
    return r.text


def scrape(limit, scraped_at):
    first = get(LISTING.format(1))
    m = SHOWING_RE.search(first)
    listed = int(m.group(1).replace(",", "")) if m else None
    pages = int(m.group(2)) if m else None
    print(f"catalogue lists {listed} products across {pages} pages", flush=True)

    rows, seen = [], set()

    def take(html_text):
        for block in cards(html_text):
            row = parse_card(block, scraped_at)
            if not row:
                continue
            pid = row[schema.PRODUCT_COLUMNS.index("product_id")]
            if pid not in seen:
                seen.add(pid)
                rows.append(row)
                if limit and len(rows) >= limit:
                    return True
        return False

    if take(first):
        return rows, listed
    # Pages walked one at a time (be polite; the listing is heavy at 500/page). Stop at the
    # reported page count, or - if that header ever changes - when a page adds no new product.
    pg = 2
    while pg <= (pages or 2000):
        before = len(rows)
        if take(get(LISTING.format(pg))):
            break
        if len(rows) == before:  # past the last page (empty or repeated listing)
            break
        if pg % 10 == 0 or pg == pages:
            print(f"page {pg}/{pages}, {len(rows)} products", flush=True)
        pg += 1
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
