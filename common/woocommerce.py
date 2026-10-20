"""Shared client and mapping for stores on the WooCommerce Store API (public, no key).

GET https://<domain>/wp-json/wc/store/v1/products?per_page=100&page=N returns a JSON array
of products. Paging stops when a page comes back empty (a page past the last one answers 200
with []). Prices are integers in the currency's minor unit (usually cents), so divide by
10**currency_minor_unit -- and the unit varies by store (some use 0, i.e. whole dollars). A
price of "0" means "call for price" (quote-only) in this market, so it maps to an empty price.

A per-site scrape.py only needs to fetch and map:

    from common import runner, woocommerce as woo
    SITE, BASE = "examplestore", "https://examplestore.com"

    def scrape(limit, scraped_at):
        products, listed = woo.fetch_products(BASE, limit)
        rows = [woo.product_row(SITE, p, scraped_at) for p in products]
        return rows, listed

    if __name__ == "__main__":
        runner.run(SITE, scrape)

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import html
import re
import time

from common import http, schema

STORE_PRODUCTS = "/wp-json/wc/store/v1/products"
PAGE = 100
PAGE_DELAY = 0.5           # polite pause between pages
TAG_RE = re.compile(r"<[^>]+>")
NUM_RE = re.compile(r"\d[\d,]*")


def clean(value):
    """Plain text: decode HTML entities, drop any tags, collapse whitespace."""
    text = html.unescape(TAG_RE.sub(" ", str(value))) if value is not None else ""
    return re.sub(r"\s+", " ", text).strip()


def _amount(prices, key):
    """prices[key] (minor units, a string) as dollars, or None. "0"/blank -> None, so a
    quote-only item (price "0") leaves the column empty."""
    if not prices:
        return None
    raw = prices.get(key)
    if raw in (None, "", "0"):
        return None
    try:
        minor = int(prices.get("currency_minor_unit", 2) or 0)
        return round(float(raw) / (10 ** minor), 2)
    except (TypeError, ValueError):
        return None


def _stock_qty(product):
    """A number from stock_availability.text when the store prints one (e.g. "7 in stock"),
    else None. The text is sometimes wrapped in HTML."""
    text = clean((product.get("stock_availability") or {}).get("text"))
    m = NUM_RE.search(text)
    return float(m.group(0).replace(",", "")) if m else None


def _names(items):
    """Unique, order-preserving names from a brands[]/categories[] list."""
    out = []
    for it in items or []:
        name = clean(it.get("name"))
        if name and name not in out:
            out.append(name)
    return out


def fetch_products(base, limit=0, per_page=PAGE, delay=PAGE_DELAY, params=None):
    """Every product from the Store API as a list of dicts, paging until a page is empty.
    Returns (products, listed) where listed is the X-WP-Total the store reports. A failed
    page raises (common/http retries 429/5xx first): it must never look like the end."""
    products, listed, page = [], None, 1
    while True:
        r = http.session().get(base + STORE_PRODUCTS,
                               params={"per_page": per_page, "page": page, **(params or {})},
                               timeout=60)
        r.raise_for_status()
        if listed is None:
            try:
                listed = int(r.headers.get("X-WP-Total"))
            except (TypeError, ValueError):
                listed = None
        batch = r.json()
        if not batch:
            break
        products.extend(batch)
        print(f"page {page}: {len(batch)} products (total {len(products)} of {listed})",
              flush=True)
        if limit and len(products) >= limit:
            return products[:limit], listed
        page += 1
        time.sleep(delay)
    return products, listed


def product_row(site, product, scraped_at, condition=""):
    """One product table row (schema.product_row) from a Store API product dict. `condition`
    is passed in by stores that read it from elsewhere (e.g. the WP class_list)."""
    prices = product.get("prices") or {}
    price = _amount(prices, "price")               # current/effective price
    regular = _amount(prices, "regular_price")     # list price; a sale -> old_price
    on_sale = price is not None and regular is not None and regular > price
    parent = product.get("parent") or 0
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": site,
        "product_id": str(product["id"]),
        "parent_id": str(parent) if parent else "",
        "sku": clean(product.get("sku")),
        "title": clean(product.get("name")),
        "brand": ", ".join(_names(product.get("brands"))),
        "category": ", ".join(_names(product.get("categories"))),
        "condition": condition,
        "price": price,
        "compare_at_price": regular if on_sale else None,
        "available": bool(product.get("is_in_stock")),
        "stock_qty": _stock_qty(product),
        "image_url": (product.get("images") or [{}])[0].get("src") or None,
        "description": schema.plain_text(product.get("short_description") or product.get("description")),
        "url": product.get("permalink") or "",
    })
