"""Scrape datadirectglobal.com (Shopify, ~1,160 products): every run's full snapshot appended to Postgres.

Data Direct Global is a Hong Kong / China networking reseller (Fortinet, Meraki, Aruba, Cisco,
H3C licences and hardware) selling in USD - not a US store.

/products.json, 250 per page, paged until an empty page. One row per variant (product_id =
variant id, parent_id = product id):
  price             the variant price; a $0 price is saved empty (the page shows no price then),
                    never as 0. Saved empty for every product while the store is quote-only
                    (below).
  compare_at_price  only when it is above the price (many products repeat the price there)
  available         the in-stock flag (the store marks everything available); None while the
                    store is quote-only, since its pages then show no stock either
  image_url         the variant's featured image, else the product's first image
  description       body_html as plain text
  sku               the manufacturer part number; part_number = sku
  brand             vendor; category = product_type
  condition         blank: the store has no per-product condition (only a site-wide
                    "Brand New & Original" banner)
  capacity          drives / memory only, from the title (upper-case GB/TB: "8Gb" is a link speed)
The public endpoint carries no stock count, so stock_qty stays blank.

Quote-only: the store's quote app (QuoteSnap, "OMGRFQConfigs" in every page) hides the price and
the Add-to-cart button in the browser and shows "Request a Quote" instead. The price is still in
the page HTML and in /products.json, so only a rendered page shows it's hidden (checked
2026-10-20: 10 of 10 sampled pages show no price). The app's settings in the page say which
products it covers: hide_add_cart_setting.hide_add_cart = 1 with hide_price_settings.follow_quote
= 1 (the price follows the hidden button), or hide_price = 1, and
condition_product_rule.enable_all_products = 1 (every product). Each run reads those settings
from a few product pages: all products hidden -> every price (and the in-stock flag, which the
pages don't show either) is saved empty (products kept); app
off -> prices saved as before; any other case (only some products covered, or no page readable)
-> prices left out with a warning, so a hidden price is never saved as a selling price.

The product count comes from the store's product sitemaps (every file counted: they don't hold
exactly 1,000 each) as a completeness check, so a truncated crawl is never saved as the full
catalogue.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import random
import re
import sys
import time

from common import http, runner, schema

SITE = "datadirectglobal"
BASE = "https://www.datadirectglobal.com"
PAGE_DELAY = 1.0
QUOTE_SAMPLE = 3      # product pages read each run for the quote app's settings

CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b")   # upper-case only: "8Gb" is a link speed
SIZED_RE = re.compile(r"HDD|SSD|Drive|Memory|RAM", re.I)      # product types where capacity applies


def fetch_catalog(limit):
    """Every product. A failed page raises: it must never look like the end of the list."""
    products, page = [], 1
    while True:
        r = http.session().get(f"{BASE}/products.json", params={"limit": 250, "page": page},
                               timeout=30)
        r.raise_for_status()
        batch = r.json().get("products", [])
        if not batch:
            return products
        products.extend(batch)
        print(f"page {page}: {len(batch)} products (total {len(products)})", flush=True)
        if limit and len(products) >= limit:
            return products[:limit]
        page += 1
        time.sleep(PAGE_DELAY)


def listed_total():
    """Product count from the product sitemaps, a completeness check only (None if unreadable)."""
    try:
        s = http.session()
        index = s.get(f"{BASE}/sitemap.xml", timeout=30)
        index.raise_for_status()
        subs = [u.replace("&amp;", "&")
                for u in re.findall(r"<loc>([^<]*sitemap_products_[^<]*)</loc>", index.text)]
        total = 0
        for url in subs:
            r = s.get(url, timeout=30)
            r.raise_for_status()
            total += len(re.findall(r"<loc>[^<]*/products/[^<]*</loc>", r.text))
        return total or None
    except Exception as e:  # noqa: BLE001 - only a sanity check
        print(f"WARN couldn't read the sitemap product count: {e}", flush=True)
        return None


def quote_app_mode(html):
    """What the quote app's settings in a page say: "off" (prices shown), "all" (price hidden
    on every product) or "some" (hidden on a subset we can't tell apart)."""
    if "OMGRFQConfigs" not in html:
        return "off"
    hide_atc = re.search(r'"hide_add_cart_setting":\{"hide_add_cart":1\b', html)
    follow = re.search(r'"follow_quote":1\b', html)
    hide_price = re.search(r'"hide_price_settings":\{[^{}]*"hide_price":1\b', html)
    if not (hide_price or (hide_atc and follow)):
        return "off"
    rule = re.search(r'"condition_product_rule":\{[^{}]*"enable_all_products":(\d)', html)
    return "all" if rule and rule.group(1) == "1" else "some"


def prices_hidden(catalog):
    """Read the quote app's settings from a few random product pages: True (save no prices)
    unless they say the app hides no prices. Mixed / partial / unreadable also give True, with
    a warning: a hidden price must never be saved as a selling price."""
    handles = [p["handle"] for p in catalog if p.get("handle")]
    modes = []
    for handle in random.sample(handles, min(QUOTE_SAMPLE, len(handles))):
        try:
            r = http.session().get(f"{BASE}/products/{handle}", timeout=30)
            r.raise_for_status()
            modes.append(quote_app_mode(r.text))
        except Exception as e:  # noqa: BLE001 - one page less in the sample
            print(f"WARN couldn't open /products/{handle}: {e}", flush=True)
        time.sleep(PAGE_DELAY)
    print(f"price check: quote app on sampled pages = {modes}", flush=True)
    if modes and all(m == "off" for m in modes):
        return False
    if not modes:
        print("WARN no product page could be read; saving no prices this run.", flush=True)
    elif not all(m == "all" for m in modes):
        print("WARN the quote app hides prices on only some products (or pages disagree); "
              "its rule needs a re-check. Saving no prices this run.", flush=True)
    return True


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def build_rows(product, scraped_at, hide_prices=False):
    title = re.sub(r"\s+", " ", product.get("title") or "").strip()
    category = product.get("product_type") or ""
    cap = CAPACITY_RE.search(title) if SIZED_RE.search(category) else None
    description = schema.plain_text(product.get("body_html"))
    product_image = (product.get("images") or [{}])[0].get("src")
    variants = product.get("variants", [])
    rows = []
    for v in variants:
        sku = (v.get("sku") or "").strip()
        price = None if hide_prices else (to_float(v.get("price")) or None)   # $0 -> empty
        compare = None if hide_prices else to_float(v.get("compare_at_price"))
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": str(v["id"]),        # unique: one row per variant
            "parent_id": str(product["id"]),
            "variant_id": str(v["id"]),
            "sku": sku,
            "part_number": sku,
            "title": title + ("" if v.get("title") in (None, "Default Title") else f" - {v['title']}"),
            "brand": product.get("vendor", ""),
            "category": category,
            "capacity": (cap.group(1) + cap.group(2)) if cap else "",
            "price": price,
            "compare_at_price": compare if price and compare and compare > price else None,
            "available": None if hide_prices else bool(v.get("available")),
            "stock_qty": None,  # not published by the public /products.json endpoint
            "image_url": (v.get("featured_image") or {}).get("src") or product_image,
            "description": description,
            "url": f"{BASE}/products/{product['handle']}"
            + (f"?variant={v['id']}" if len(variants) > 1 else ""),
        }))
    return rows


def scrape(limit, scraped_at):
    catalog = fetch_catalog(limit)
    hide = prices_hidden(catalog)
    rows = [row for product in catalog for row in build_rows(product, scraped_at, hide)]
    listed = None if limit else listed_total()
    if not limit and listed and len(catalog) < 0.98 * listed:
        sys.exit(f"Only fetched {len(catalog)} of {listed} products ({len(catalog) / listed:.0%}); "
                 "not saving a partial snapshot")
    print(f"fetched {len(catalog)} products -> {len(rows)} variant rows (sitemap lists {listed})",
          flush=True)
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
