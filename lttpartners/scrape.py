"""Scrape lttpartners.com (Shopify, ~470 products): every run's full snapshot appended to Postgres.

LTT Partners sells Verkada and other physical-security / networking gear - a small catalogue
(~470 products, well under Shopify's 25,000-item deep-paging cap), so the public /products.json
endpoint reaches everything: 250 per page, paged until a page comes back empty. No GraphQL or
Storefront token needed. Many products have several variants (e.g. a camera in different storage
sizes); each variant is its own row.

One row per variant: product_id = variant id, parent_id = product id. Standard Shopify mapping -
sku, title, price, compare_at_price, available, brand = vendor, category = product_type, url.
Condition and capacity are read from the title / variant title when present. The store doesn't
publish stock counts in /products.json, so stock_qty stays blank. Prices are the store's USD
prices and are saved as-is.

A $0.00 / "0.00" price in /products.json means the item is quote/hidden, not free, so it is
saved as blank (never 0); the same guard applies to compare_at_price.

The product count comes from the store's product sitemap as a completeness check, so a truncated
crawl is never saved as the full catalogue. Shopify's /products.json refuses page 101, so if the
catalogue ever grows past 25k the walk fails loudly rather than silently truncating.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
import sys
import time

from common import http, runner, schema

SITE = "lttpartners"
BASE = "https://lttpartners.com"
PAGE_DELAY = 0.5
MAX_PAGE = 100            # Shopify's public /products.json refuses pages past 100 (25k items)

CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB|MB)\b", re.I)
CONDITIONS = [
    (re.compile(r"\brefurbished\b", re.I), "Refurbished"),
    (re.compile(r"\brecertified\b", re.I), "Recertified"),
    (re.compile(r"\brenewed\b", re.I), "Renewed"),
    (re.compile(r"\bopen\s*box\b", re.I), "Open Box"),
    (re.compile(r"\bused\b", re.I), "Used"),
    (re.compile(r"\bnew\b", re.I), "New"),
]


def price_of(value):
    """A positive USD price, or None. Shopify sends price as a string; "0.00" means the item is
    quote-only/hidden, not free, so it becomes None rather than 0."""
    try:
        return float(value) or None
    except (TypeError, ValueError):
        return None


def condition_of(title):
    for rx, label in CONDITIONS:
        if rx.search(title or ""):
            return label
    return ""


def fetch_catalog(limit):
    """Every product, deduped by product id. A failed page raises (handled by the retrying
    session): an empty page is the only thing that ends the walk, so a crash never looks like the
    end of the list. Pages past Shopify's 25k cap raise rather than silently truncate."""
    by_id, page = {}, 1
    while True:
        r = http.session().get(f"{BASE}/products.json", params={"limit": 250, "page": page},
                               timeout=30)
        r.raise_for_status()
        batch = r.json().get("products", [])
        if not batch:
            return list(by_id.values())
        for p in batch:
            by_id.setdefault(p["id"], p)
        print(f"page {page}: {len(batch)} products (total {len(by_id)})", flush=True)
        if limit and len(by_id) >= limit:
            return list(by_id.values())[:limit]
        if page >= MAX_PAGE:
            raise RuntimeError(f"Reached page {page} (Shopify's /products.json cap); the catalogue "
                               "exceeds 25k and needs the Storefront GraphQL method instead")
        page += 1
        time.sleep(PAGE_DELAY)


def listed_total():
    """Product count from the sitemap, a completeness check only. Shopify's product sitemaps hold
    1,000 URLs each except the last, so the total is read from just the index and the last file.
    Returns None if the layout isn't what we expect (then the check is skipped)."""
    try:
        s = http.session()
        index = s.get(f"{BASE}/sitemap.xml", timeout=30).text
        subs = [u.replace("&amp;", "&")
                for u in re.findall(r"<loc>([^<]*sitemap_products_[^<]*)</loc>", index)]
        if not subs:
            return None

        def count(url):
            return len(re.findall(r"<loc>[^<]*/products/[^<]*</loc>", s.get(url, timeout=30).text))

        first = count(subs[0])
        if first != 1000 and len(subs) > 1:
            return None
        last = count(subs[-1]) if len(subs) > 1 else first
        return (len(subs) - 1) * 1000 + last
    except Exception as e:  # noqa: BLE001 - only a sanity check
        print(f"WARN couldn't read the sitemap product count: {e}", flush=True)
        return None


def build_rows(product, scraped_at):
    """One row per variant (product_id = the variant id; parent_id = the product id)."""
    title = (product.get("title") or "").strip()
    variants = product.get("variants", [])
    first_word = title.split(" ", 1)[0] if title else ""
    image_url = (product.get("images") or [{}])[0].get("src") or None
    description = schema.plain_text(product.get("body_html"))
    multi = len(variants) > 1
    rows = []
    for v in variants:
        vtitle = v.get("title")
        full_title = title + ("" if vtitle in (None, "Default Title") else f" - {vtitle}")
        cap = CAPACITY_RE.search(full_title)
        sku = (v.get("sku") or "").strip()
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": str(v["id"]),        # unique: one row per variant
            "parent_id": str(product["id"]),
            "variant_id": str(v["id"]),
            "sku": sku,
            "part_number": sku or (first_word if re.search(r"\d", first_word) else ""),
            "title": full_title,
            "brand": product.get("vendor", ""),
            "category": product.get("product_type", ""),
            "condition": condition_of(full_title),
            "capacity": (cap.group(1) + cap.group(2).upper()) if cap else "",
            "price": price_of(v.get("price")),                      # $0.00 -> blank (quote)
            "compare_at_price": price_of(v.get("compare_at_price")),  # $0.00 -> blank
            "available": bool(v.get("available")),
            "stock_qty": None,                 # not published in /products.json
            "image_url": image_url,
            "description": description,
            "url": f"{BASE}/products/{product['handle']}"
                   + (f"?variant={v['id']}" if multi else ""),
        }))
    return rows


def scrape(limit, scraped_at):
    catalog = fetch_catalog(limit)
    rows = [row for product in catalog for row in build_rows(product, scraped_at)]
    listed = None if limit else listed_total()
    # Product-level completeness guard (rows are per variant; products are the right unit here)
    if not limit and listed and len(catalog) < 0.98 * listed:
        sys.exit(f"Only fetched {len(catalog)} of {listed} products ({len(catalog) / listed:.0%}); "
                 "not saving a partial snapshot")
    print(f"fetched {len(catalog)} unique products -> {len(rows)} variant rows "
          f"(sitemap lists {listed})", flush=True)
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
