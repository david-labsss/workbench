"""Scrape www.pbtech.com (PB Tech, ~43k products): every run's full snapshot appended to Postgres.

Prices are in AUD and stored as-is. This is PB Tech's Australian site (www.pbtech.com), which is
open; the New Zealand site (pbtech.co.nz) sits behind a Cloudflare challenge and is NOT used.

Side door (no API needed): the product sitemap lists every product URL, and each product page
carries a clean schema.org Product JSON-LD block (name, sku, mpn, brand, price, priceCurrency
"AUD", availability). Same idiom as the sitemap -> product-page JSON-LD scrapers:
  /sitemap.xml -> /sitemaps/products-global.xml (~43k product URLs, SKU embedded in the URL)
  -> each product page -> parse the Product JSON-LD (price from the <... data-price> attr as a
  fallback when the JSON-LD has none, e.g. "call for price" products).

www.pbtech.com still fronts Cloudflare, which occasionally answers a request with a "Just a
moment..." interstitial (HTTP 403). It clears on retry with the same cookie-bearing session, so
get() retries those; a product that keeps failing is tried again in a second pass and otherwise
dropped (runner.py refuses to save a snapshot that falls short of the sitemap count).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "pbtech"
BASE = "https://www.pbtech.com"
SITEMAP_INDEX = f"{BASE}/sitemap.xml"
PRODUCTS_SITEMAP = f"{BASE}/sitemaps/products-global.xml"
CURRENCY = "AUD"            # prices are Australian dollars, stored as-is
WORKERS = 6                 # polite; Cloudflare starts challenging above this
TRIES = 6                   # per URL, to ride out the occasional Cloudflare interstitial

LOC_RE = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>")
SKU_RE = re.compile(r"/product/([^/]+)/")
LDJSON_RE = re.compile(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', re.S | re.I)
CHALLENGE = ("just a moment", "challenge-platform", "cf-browser-verification", "attention required")


def get(url):
    """Page text, or None for a 404. Retries the Cloudflare interstitial (HTTP 403 / challenge
    HTML), which clears once the session holds a clearance cookie."""
    last = ""
    for i in range(TRIES):
        r = http.session().get(url, timeout=90)
        if r.status_code == 404:
            return None
        body = r.text
        if r.status_code == 403 or any(c in body[:2000].lower() for c in CHALLENGE):
            last = f"HTTP {r.status_code} Cloudflare challenge"
            time.sleep(1.5 * (i + 1))
            continue
        r.raise_for_status()
        return body
    raise RuntimeError(f"{url}: {last}")


def ld_items(html):
    """Every object in the page's JSON-LD blocks (a block may be a single object or a list)."""
    for raw in LDJSON_RE.findall(html):
        try:
            data = json.loads(raw.strip())
        except ValueError:
            continue
        yield from (data if isinstance(data, list) else [data])


def is_type(obj, name):
    t = obj.get("@type")
    return t == name or (isinstance(t, list) and name in t)


def category(items):
    """The leaf category from the breadcrumb trail (Home > ... > leaf > product name)."""
    for obj in items:
        if is_type(obj, "BreadcrumbList"):
            names = [e.get("name") for e in obj.get("itemListElement", []) if e.get("name")]
            trail = [n for n in names if n.lower() != "home"][:-1]  # drop Home and the product
            if trail:
                return trail[-1]
    return ""


def condition(offer):
    """schema.org itemCondition -> New / Refurbished / Used / ..."""
    token = str(offer.get("itemCondition") or "").rstrip("/").rsplit("/", 1)[-1]
    return token[:-len("Condition")] if token.endswith("Condition") else token


def to_price(value):
    if value in (None, "", 0, "0"):
        return None
    try:
        return float(str(value).replace(",", "").replace("$", "").strip())
    except ValueError:
        return None


def data_price(html):
    """Fallback price from the first element carrying a data-price attribute."""
    el = BeautifulSoup(html, "html.parser").select_one("[data-price]")
    return to_price(el.get("data-price")) if el else None


def parse(html, url, url_sku):
    """A product row dict from the page's Product JSON-LD, or None if the page has no product."""
    items = list(ld_items(html))
    product = next((o for o in items if is_type(o, "Product")), None)
    if not product:
        return None
    offers = product.get("offers")
    offer = (offers[0] if isinstance(offers, list) and offers else offers) or {}
    brand = product.get("brand")
    brand = brand.get("name") if isinstance(brand, dict) else brand
    availability = str(offer.get("availability") or "")
    price = to_price(offer.get("price"))
    if price is None:
        price = data_price(html)
    image = product.get("image")
    if isinstance(image, list):
        image = image[0] if image else ""
    return {
        "scraped_at": None,  # filled by scrape()
        "website": SITE,
        "product_id": product.get("sku") or url_sku,
        "sku": product.get("sku") or url_sku,
        "part_number": product.get("mpn") or "",
        "title": product.get("name") or "",
        "brand": brand or "",
        "category": category(items),
        "condition": condition(offer),
        "price": price,
        "available": "instock" in availability.lower(),
        "image_url": (str(image).strip() if image else "") or None,
        "description": schema.plain_text(product.get("description")),
        "url": url,
    }


def scrape(limit, scraped_at):
    index = get(SITEMAP_INDEX) or ""
    products_sitemap = next((u for u in LOC_RE.findall(index) if "products" in u), PRODUCTS_SITEMAP)
    urls = LOC_RE.findall(get(products_sitemap) or "")

    # one URL per SKU (the SKU is the product_id, which must be unique)
    todo = {}
    for url in urls:
        m = SKU_RE.search(url)
        if m:
            todo.setdefault(m.group(1), url)
    listed = len(todo)
    items = list(todo.items())
    if limit:
        items = items[:limit]
    print(f"{len(urls)} product URLs in the sitemap, {listed} unique SKUs", flush=True)

    def fetch(item):
        sku, url = item
        html = get(url)             # raises on a persistent challenge -> retried below
        if html is None:            # 404: delisted since the sitemap was built
            return None
        row = parse(html, url, sku)
        if row is None:
            raise RuntimeError(f"{url}: no Product JSON-LD")
        row["scraped_at"] = scraped_at
        return schema.product_row(row)

    rows, failed = [], []
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, (item, result) in enumerate(zip(items, pool.map(_safe(fetch), items)), 1):
            ok, value = result
            if not ok:
                failed.append(item)
                print(f"WARN {item[1]}: {value}", flush=True)
            elif value is not None:
                rows.append(value)
            if i % 2000 == 0 or i == len(items):
                print(f"{i}/{len(items)} pages, {len(rows)} products, {len(failed)} failed",
                      flush=True)

    # second pass for the pages that failed (dropped connection, stubborn challenge)
    for item in failed:
        ok, value = _safe(fetch)(item)
        if ok and value is not None:
            rows.append(value)

    return rows, listed


def _safe(fn):
    """Wrap fetch so one bad page can't stop the pool: (True, row) or (False, error)."""
    def wrapped(item):
        try:
            return True, fn(item)
        except Exception as e:  # noqa: BLE001 - one bad page must not stop the run
            return False, str(e)
    return wrapped


if __name__ == "__main__":
    runner.run(SITE, scrape)
