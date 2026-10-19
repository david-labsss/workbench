"""Scrape flagshiptech.com (BigCommerce store at store.flagshiptech.com, ~16k products): every
run's full snapshot appended to Postgres.

The store has no storefront API token in its pages and its Doofinder search engine is switched
off, so products are read from BigCommerce's own product comparison page, which renders any
list of product ids given in its URL: /compare/1/2/3/... Product ids run from 1 to ~17k, so
the scraper asks for 200 ids at a time, from 1 up until 2,000 ids in a row come back empty.

Each column of the comparison table is one product. Its id is the one missing from its own
"remove" link (that link lists every other product on the page). Columns give the brand,
title, link, price, the buy button and the description:
  "Add to Cart" / "Choose Options"   in stock
  "Call for Availability"            out of stock
  no button and no price             not for sale (saved as out of stock, no price)
The part number is read from the description ("Part Number(s): A, B"). The page shows no
SKU, category, condition or stock count, so those stay blank. The number of products to
check against is the product count of the store's sitemap.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "flagshiptech"
BASE = "https://store.flagshiptech.com"
CHUNK = 200               # product ids per comparison page
EMPTY_STOP = 10           # stop after this many empty chunks in a row (2,000 ids)
WORKERS = 3
IN_STOCK_BUTTONS = {"add to cart", "choose options"}
PART_RE = re.compile(r"Part\s+Number(?:\(s\)|s)?\s*:\s*([A-Z0-9][A-Z0-9#/.\-]*(?:\s*,\s*[A-Z0-9][A-Z0-9#/.\-]*)*)",
                     re.I)
PRICE_RE = re.compile(r"[\d,]+(?:\.\d+)?")


def sitemap_count():
    """Product URLs in the store's sitemap (all its product pages)."""
    r = http.session().get(f"{BASE}/xmlsitemap.php", timeout=60)
    r.raise_for_status()
    pages = re.findall(r"xmlsitemap\.php\?type=products&(?:amp;)?page=(\d+)", r.text)
    total = 0
    for page in sorted(set(pages), key=int):
        p = http.session().get(f"{BASE}/xmlsitemap.php",
                               params={"type": "products", "page": page}, timeout=120)
        p.raise_for_status()
        total += p.text.count("<loc>")
    return total


def clean(text):
    # descriptions hold some doubly escaped spaces: a literal "&nbsp;" in the text
    text = (text or "").replace("\xa0", " ").replace("&nbsp;", " ")
    return re.sub(r"\s+", " ", text).strip()


def money(tag):
    m = PRICE_RE.search(tag.get_text() if tag else "")
    return float(m.group(0).replace(",", "")) if m else None


def parse_compare(html):
    """[{column fields}] for every product column of a comparison page."""
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("tr.compareTable-row")
    if not rows:
        return []
    cards = rows[0].select("th.compareTable-product")
    if not cards:
        return []
    actions = rows[1].select("td.compareTable-action") if len(rows) > 1 else []
    details = {clean(r.select_one(".compareTable-headingText").get_text()):
               r.select("td.compareTable-item")
               for r in rows if r.select_one(".compareTable-headingText")}

    def link_ids(card):
        a = card.select_one("a.compareTable-removeProduct")
        tail = (a.get("href") or "").split("/compare/", 1)[-1] if a else ""
        return {x for x in tail.strip("/").split("/") if x.isdigit()}

    links = [link_ids(c) for c in cards]
    on_page = set().union(*links)
    products = []
    for i, card in enumerate(cards):
        button = actions[i] if i < len(actions) else None
        mine = on_page - links[i]
        if len(mine) == 1:
            pid = mine.pop()
        else:  # a page with a single product: take the id from its buy button
            b = button.select_one("[data-product-id]") if button else None
            pid = b.get("data-product-id") if b else None
        if not pid:
            print("WARN skipped a comparison column whose product id is unknown", flush=True)
            continue
        title = card.select_one(".card-title a")
        brand = card.select_one(".card-text a")
        description = details.get("Description", [])
        products.append({
            "id": pid,
            "title": clean(title.get_text()) if title else "",
            "url": (title.get("href") or "") if title else "",
            "brand": clean(brand.get_text()) if brand else "",
            "price": money(card.select_one("[data-product-price-without-tax]")),
            "rrp": money(card.select_one("[data-product-rrp-price-without-tax]")),
            "non_sale": money(card.select_one("[data-product-non-sale-price-without-tax]")),
            "button": clean(button.get_text()).lower() if button else "",
            "description": clean(description[i].get_text(" ")) if i < len(description) else "",
        })
    return products


def fetch_chunk(start):
    ids = "/".join(str(i) for i in range(start, start + CHUNK))
    r = http.session().get(f"{BASE}/compare/{ids}", timeout=180)
    r.raise_for_status()
    return parse_compare(r.text)


def fetch_all(limit):
    by_id, start, empty_run = {}, 1, 0
    with ThreadPoolExecutor(WORKERS) as pool:
        while empty_run < EMPTY_STOP and not (limit and len(by_id) >= limit):
            starts = [start + k * CHUNK for k in range(WORKERS * 2)]
            for s, products in zip(starts, pool.map(fetch_chunk, starts)):
                for p in products:
                    by_id[p["id"]] = p
                empty_run = 0 if products else empty_run + 1
            start = starts[-1] + CHUNK
            print(f"ids up to {start - 1}: {len(by_id)} products", flush=True)
    return list(by_id.values())


def build_row(p, scraped_at):
    parts = []
    m = PART_RE.search(p["description"])
    if m:
        parts = [x.strip() for x in m.group(1).split(",") if x.strip()]
    price = p["price"]
    compare = max((x for x in (p["rrp"], p["non_sale"]) if x), default=None)
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": p["id"],
        "part_number": parts[0] if parts else "",
        "title": p["title"],
        "brand": p["brand"],
        "other_specs": f"Part numbers: {', '.join(parts)}" if len(parts) > 1 else "",
        "price": price,
        "compare_at_price": compare if compare and price and compare > price else None,
        "available": p["button"] in IN_STOCK_BUTTONS,
        "stock_qty": None,       # not shown on the comparison page
        "url": p["url"],
    })


def scrape(limit, scraped_at):
    listed = sitemap_count()
    print(f"sitemap lists {listed} products", flush=True)
    items = fetch_all(limit)
    items = items[:limit] if limit else items
    return [build_row(p, scraped_at) for p in items], listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
