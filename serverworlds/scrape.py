"""Scrape serverworlds.com (BigCommerce, ~44k products): every run's full snapshot appended to
Postgres.

The store has no storefront API token in its pages, its product comparison page is switched
off, and its category listings only reach ~75% of the products (the rest aren't in any
category). Its pages are ~1 MB each. So every product page in the sitemap is read the way the
theme's own quick-view requests do: with the stencil-utils headers, which make BigCommerce
render only the product view (~27 KB). 16 pages are read at a time: the store answers each in
~0.4 s whether 4 or 20 are asked at once (tested 2026-10-13), so the run takes ~20 min, not 75.

Each product view gives the product id (the add-to-cart form), title, brand, SKU (= the part
number), condition, MPN / UPC when filled, price, the struck-through "Was" / MSRP price, the
stock count when the store shows it, and the buy button ("Add to Cart" / "Pre-Order" = in
stock, "Out of stock" otherwise). The number of products to check against is the product count
of the store's sitemap.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import json
import re
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "serverworlds"
BASE = "https://www.serverworlds.com"
WORKERS = 16
VIEW_HEADERS = {
    "stencil-options": json.dumps({"render_with": "products/product-view"}),
    "x-requested-with": "stencil-utils",
}
IN_STOCK_BUTTONS = ("add to cart", "pre-order")
SKIP_DETAILS = {"availability", "shipping", "sku", "condition", "mpn", "brand", "minimum purchase",
                "maximum purchase"}
PRICE_RE = re.compile(r"[\d,]+(?:\.\d+)?")


def sitemap_urls(kind):
    """Every URL of one kind (products, categories) in the store's sitemap."""
    r = http.session().get(f"{BASE}/xmlsitemap.php", timeout=60)
    r.raise_for_status()
    pages = sorted(set(re.findall(rf"type={kind}&(?:amp;)?page=(\d+)", r.text)), key=int)
    urls = []
    for page in pages:
        p = http.session().get(f"{BASE}/xmlsitemap.php", params={"type": kind, "page": page},
                               timeout=120)
        p.raise_for_status()
        urls += re.findall(r"<loc>([^<]+)</loc>", p.text)
    return urls


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def money(tag):
    m = PRICE_RE.search(tag.get_text() if tag else "")
    return float(m.group(0).replace(",", "")) if m else None


def fetch_view(url):
    try:
        r = http.session().get(url, headers=VIEW_HEADERS, timeout=90)
        if r.status_code == 404:
            return None  # listed in the sitemap but gone
        r.raise_for_status()
        return url, r.text
    except Exception as e:  # noqa: BLE001 - one bad page must not stop the run
        print(f"WARN {url}: {e}", flush=True)
        return None


def parse(url, html, scraped_at):
    soup = BeautifulSoup(html, "html.parser")
    pid = soup.select_one("input[name=product_id]")
    title = soup.select_one(".productView-title")
    if not pid or not pid.get("value") or not title:
        return None
    details = {}
    for dt in soup.select("dt"):
        dd = dt.find_next_sibling("dd")
        if dd:
            details[clean(dt.get_text()).rstrip(":").lower()] = clean(dd.get_text(" "))
    other = [f"{k.title()}: {v}" for k, v in details.items() if v and k not in SKIP_DETAILS]
    brand = soup.select_one(".productView-brand a")
    button = soup.select_one("#form-action-addToCart")
    stock = soup.select_one("[data-product-stock]")
    stock_text = clean(stock.get_text()) if stock else ""
    price = money(soup.select_one("[data-product-price-with-tax], "
                                  "[data-product-price-without-tax]"))
    compare = max((x for x in (
        money(soup.select_one("[data-product-rrp-with-tax], [data-product-rrp-price-without-tax]")),
        money(soup.select_one("[data-product-non-sale-price-with-tax], "
                              "[data-product-non-sale-price-without-tax]"))) if x), default=None)
    sku = details.get("sku", "")
    in_stock = bool(button) and (button.get("value") or "").lower().startswith(IN_STOCK_BUTTONS)
    image = soup.select_one(".productView-image img")
    image_url = (image.get("src") or image.get("data-src")) if image else ""   # cdn11 absolute url
    desc = soup.select_one("#tab-description")
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": pid.get("value"),
        "sku": sku,
        "part_number": details.get("mpn") or sku,
        "title": clean(title.get_text()),
        "brand": clean(brand.get_text()) if brand else details.get("brand", ""),
        "condition": details.get("condition", ""),
        "other_specs": "; ".join(other),
        "price": price,
        "compare_at_price": compare if compare and price and compare > price else None,
        "available": in_stock,
        "stock_qty": float(stock_text) if stock_text.isdigit() else None,
        "image_url": image_url or None,
        "description": schema.plain_text(desc.get_text(" ")) if desc else None,
        "url": url,
    })


def scrape(limit, scraped_at):
    urls = sorted(set(sitemap_urls("products")))
    print(f"sitemap lists {len(urls)} products", flush=True)
    todo = urls[:limit] if limit else urls
    rows, seen = [], set()
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, page in enumerate(pool.map(fetch_view, todo), 1):
            row = parse(page[0], page[1], scraped_at) if page else None
            pid = row[schema.PRODUCT_COLUMNS.index("product_id")] if row else None
            if row and pid not in seen:      # two URLs can lead to one product
                seen.add(pid)
                rows.append(row)
            if i % 2000 == 0 or i == len(todo):
                print(f"{i}/{len(todo)} pages, {len(rows)} products", flush=True)
    return rows, len(urls)


if __name__ == "__main__":
    runner.run(SITE, scrape)
