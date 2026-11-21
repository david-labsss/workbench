"""Scrape novianetworks.com (BigCommerce, ~475 networking products): every run's full snapshot
appended to Postgres.

The store has no storefront GraphQL token in its pages (so common/bigcommerce.py cannot be
used), so every product page listed in its sitemap (xmlsitemap.php?type=products&page=N) is
read, 2 at a time with a short pause (robots.txt allows product pages and the sitemap; its
crawl-delay applies only to named AI bots). Each page carries the theme's inline
`var BCData = {...}` JSON, whose product_attributes give:
  sku, mpn (-> part_number), upc (-> other_specs)
  price.without_tax.value (USD) and the RRP / non-sale price when set (-> compare_at_price);
    "call for price" items have no price there and are saved with an empty price
  stock (the inventory count, only when the store tracks it), instock (-> available)
    "call for price" items (purchasable false, no stock count) have no Add to Cart and a blank
    "Current Stock:" on the page, though instock is true: available is saved as None (unknown)
The rest comes from the page: product id (hidden input product_id), title, brand and condition
(schema.org microdata), the category path from the breadcrumbs (e.g. "Wireless > Access
Points"; the catch-all "Shop All Products" is skipped), image_url (og:image) and description
(the Description tab as plain text). The number to check the snapshot
against is the sitemap's product count; a page that fails (after common/http retries) stops
the run, and a sitemap URL that is gone (404) is skipped. A page that comes back with no
product on it is tried again 3 times, a few minutes apart, before it stops the run (fetch).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "novianetworks"
BASE = "https://www.novianetworks.com"
WORKERS = 2
DELAY = 1.0                   # pause after each page, per worker
NOT_PRODUCT_WAITS = (30, 90, 180)   # seconds before each new try of a page that came back with no product
BCDATA_RE = re.compile(r"var BCData = (\{.*?\});\s*$", re.S | re.M)
GENERIC_CATEGORIES = {"home", "shop all products", "shop all accessories"}
EMPTY_VALUES = {"", "n/a", "na", "none", "-"}


def get(url, **kw):
    r = http.session().get(url, timeout=90, **kw)
    r.raise_for_status()
    return r


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def sitemap_urls():
    """Every product URL across the store's product sitemap pages."""
    index = get(f"{BASE}/xmlsitemap.php").text
    pages = sorted(set(re.findall(r"type=products&(?:amp;)?page=(\d+)", index)), key=int)
    if not pages:
        raise RuntimeError("no product sitemap in xmlsitemap.php")
    urls = []
    for page in pages:
        text = get(f"{BASE}/xmlsitemap.php", params={"type": "products", "page": page}).text
        urls += [clean(u).replace("&amp;", "&") for u in re.findall(r"<loc>([^<]+)</loc>", text)]
    return urls


def is_product_page(html):
    """The parts parse() needs: the theme's BCData, the hidden product_id input and the title."""
    return "var BCData" in html and 'name="product_id"' in html and "productView-title" in html


def fetch(url):
    """(url, html), or None when the product is gone (404). Other failures raise.

    A 200 page with no product on it is fetched again after NOT_PRODUCT_WAITS (11-19 to 11-21 a
    different product page each day, after 50 to 200 pages, came back like that to GitHub's runner
    and stopped the whole run, though the same page was fine before and after). Still no product
    after the last wait: it raises as before, never a run with a product left out (the master
    table would list it as gone)."""
    for wait in (*NOT_PRODUCT_WAITS, None):
        r = http.session().get(url, timeout=90)
        time.sleep(DELAY)
        if r.status_code == 404:
            print(f"WARN {url}: 404, listed in the sitemap but gone", flush=True)
            return None
        r.raise_for_status()
        if is_product_page(r.text):
            return url, r.text
        title = re.search(r"<title[^>]*>([^<]*)", r.text, re.I)
        what = f"{r.status_code}, title {clean(title.group(1))[:80]!r}" if title else f"{r.status_code}, no title"
        what += f", {len(r.text)} bytes, starts {clean(r.text[:120])!r}"
        if wait is None:
            raise RuntimeError(f"{url}: no product on the page after {len(NOT_PRODUCT_WAITS)} retries ({what})")
        print(f"WARN {url}: no product on the page ({what}); trying again in {wait}s", flush=True)
        time.sleep(wait)


def num(value):
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def parse(url, html, scraped_at):
    m = BCDATA_RE.search(html)
    if not m:
        raise RuntimeError(f"{url}: no BCData on the page")
    attrs = json.loads(m.group(1)).get("product_attributes") or {}
    soup = BeautifulSoup(html, "html.parser")
    pid = soup.select_one("input[name=product_id]")
    title = soup.select_one("h1.productView-title") or soup.select_one(".productView-title")
    if not pid or not pid.get("value") or not title:
        raise RuntimeError(f"{url}: no product id or title on the page")

    prices = attrs.get("price") or {}
    price = num((prices.get("without_tax") or {}).get("value"))
    price = price or None                                        # $0 = call for price
    compare = max((x for x in (num((prices.get(k) or {}).get("value"))
                               for k in ("rrp_without_tax", "non_sale_price_without_tax")) if x),
                  default=None)
    brand = soup.select_one("[itemprop=brand] [itemprop=name]")
    condition = ""
    for el in soup.select("[itemprop=itemCondition]"):
        text = clean(el.get("content") or el.get_text())
        if text and not text.startswith("http"):
            condition = text
            break
    crumbs = [clean(a.get_text()) for a in soup.select(".breadcrumbs .breadcrumb-label")][:-1]
    category = " > ".join(c for c in crumbs if c.lower() not in GENERIC_CATEGORIES)
    sku = clean(attrs.get("sku"))
    mpn = clean(attrs.get("mpn"))
    upc = clean(attrs.get("upc"))
    stock = num(attrs.get("stock"))
    available = bool(attrs.get("instock"))
    if available and attrs.get("purchasable") is False and stock is None:
        available = None   # call for price: no Add to Cart, no stock shown
    image = soup.select_one("meta[property='og:image']")
    description = soup.select_one("#tab-description") or soup.select_one("[itemprop=description]")
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": pid.get("value"),
        "sku": sku,
        "part_number": mpn if mpn.lower() not in EMPTY_VALUES else sku,
        "title": clean(title.get_text()),
        "brand": clean(brand.get_text()) if brand else "",
        "category": category,
        "condition": condition,
        "other_specs": f"UPC: {upc}" if upc.lower() not in EMPTY_VALUES else "",
        "price": price,
        "compare_at_price": compare if compare and price and compare > price else None,
        "available": available,
        "stock_qty": stock,
        "image_url": (image.get("content") or None) if image else None,
        "description": schema.plain_text(description.get_text(" ")) if description else None,
        "url": url,
    })


def scrape(limit, scraped_at):
    urls = list(dict.fromkeys(sitemap_urls()))
    print(f"sitemap lists {len(urls)} products", flush=True)
    todo = urls[:limit] if limit else urls
    rows, seen = [], set()
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, page in enumerate(pool.map(fetch, todo), 1):
            row = parse(page[0], page[1], scraped_at) if page else None
            pid = row[schema.PRODUCT_COLUMNS.index("product_id")] if row else None
            if row and pid not in seen:      # two URLs can lead to one product
                seen.add(pid)
                rows.append(row)
            if i % 100 == 0 or i == len(todo):
                print(f"{i}/{len(todo)} pages, {len(rows)} products", flush=True)
    return rows, len(urls)


if __name__ == "__main__":
    runner.run(SITE, scrape)
