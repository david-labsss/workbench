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
The rest comes from the page: product id (hidden input product_id), title, brand and condition
(schema.org microdata), and the category path from the breadcrumbs (e.g. "Wireless > Access
Points"; the catch-all "Shop All Products" is skipped). The number to check the snapshot
against is the sitemap's product count; a page that fails (after common/http retries) stops
the run, and a sitemap URL that is gone (404) is skipped.

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


def fetch(url):
    """(url, html), or None when the product is gone (404). Other failures raise."""
    r = http.session().get(url, timeout=90)
    time.sleep(DELAY)
    if r.status_code == 404:
        print(f"WARN {url}: 404, listed in the sitemap but gone", flush=True)
        return None
    r.raise_for_status()
    return url, r.text


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
        "available": bool(attrs.get("instock")),
        "stock_qty": num(attrs.get("stock")),
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
