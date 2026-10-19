"""Scrape core4solutions.com (Shopify, ~8,450 products): every run's full snapshot appended to Postgres.

Two public sources, joined by product handle:
  1. /collections/all/products.json, 250 per page (~34 pages): ids, title, vendor, product
     type, tags, SKU, price, in-stock flag (same data as /products.json, in the listing's order).
  2. The /collections/all listing (24 cards per page, ~352 pages), fetched one page at a time
     with a pause, because the product cards carry two things the JSON doesn't:
       - "Pricing on request": about 1 in 7 products show this instead of a price (with a
         "Request a quote" button) although the JSON still has a price. No tag, product field or
         price threshold explains which ones (same tags, same price range as priced products),
         so the listing is the only reliable source. Those products are saved with an empty
         price, never the JSON price.
       - the real stock count: "In stock (12 units), ready to be shipped" -> stock_qty 12.
     The listing must cover at least 98% of the JSON catalogue or the run fails, so a short or
     broken crawl never saves JSON prices for hidden-price products. A product missing from the
     listing (e.g. added mid-run) keeps its JSON price and gets no stock count.

Mapping (one row per variant; every product has one):
  sku          the raw SKU, e.g. "hp 489864-002 refurbished" (vendor + part number + condition)
  part_number  the manufacturer part number parsed from the SKU, in the title's spelling
               (489864-002); left blank when it can't be confirmed in the title
  condition    the listing card's badge (Refurbished / New / New Factory Sealed / New Bulk /
               New Open Box), else the condition tags (NOB / REF / NEW ...) or the SKU's last
               word (refurbished / new / renew / nob); "renew" SKUs carry the "New" badge
  available    products.json "available". Checked against the pages: the products it marks
               available show "In stock (N units)" + Add to cart (or Request a quote); the few it
               marks unavailable show "Sold out". Storefront availableForSale agrees.
  brand        vendor; category = product_type
  capacity     drives / memory only, from the title (upper-case GB/TB: "8Gb" is a link speed)
The product count comes from the store's product sitemaps (all files counted) as a completeness
check.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
import sys
import time
from urllib.parse import unquote

from common import http, runner, schema

SITE = "core4solutions"
BASE = "https://www.core4solutions.com"
JSON_DELAY = 1.0      # pause between /products.json pages
LIST_DELAY = 1.0      # pause between listing pages (the store answers 429 to faster crawls)
COVERAGE = 0.98       # the listing must show at least this share of the JSON catalogue

CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b")   # upper-case only: "8Gb" is a link speed
SIZED_RE = re.compile(r"Hard drive|SSD|Memory", re.I)         # product types where capacity applies
CARD_SPLIT = '<div class="product-card">'
CARD_HANDLE_RE = re.compile(r'href="/products/([^"?#]+)')
CARD_PRICE_RE = re.compile(r'class="product-card__price">(.*?)</p>', re.S)
CARD_STOCK_RE = re.compile(r'class="product-card__stock">(.*?)</p>', re.S)
CARD_BADGE_RE = re.compile(r'class="product-card__badge[^"]*">\s*([^<]*?)\s*<')
UNITS_RE = re.compile(r"\((\d[\d,]*)\s+units?\)", re.I)
ON_REQUEST = "pricing on request"

# Condition from the tags (first match wins), then from the SKU's last word
TAG_CONDITIONS = [("NOB", "New Open Box"), ("OPEN BOX", "New Open Box"),
                  ("FACTORY SEALED", "New Factory Sealed"), ("REF", "Refurbished"),
                  ("REFURBISHED", "Refurbished"), ("NEW", "New")]
SKU_CONDITIONS = {"refurbished": "Refurbished", "ref": "Refurbished", "new": "New",
                  "renew": "New", "nob": "New Open Box", "ob": "New Open Box"}
COND_WORDS = set(SKU_CONDITIONS)


# --- fetching ----------------------------------------------------------------
def fetch_catalog(limit):
    """Every product from /collections/all/products.json (the same products as /products.json,
    in the listing's order). A failed page raises: it must never look like the end of the list."""
    products, page = [], 1
    while True:
        r = http.session().get(f"{BASE}/collections/all/products.json",
                               params={"limit": 250, "page": page},
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
        time.sleep(JSON_DELAY)


def parse_cards(html):
    """{handle: {"on_request": bool, "stock_qty": int|None, "stock_text": str, "badge": str}}"""
    cards = {}
    for chunk in html.split(CARD_SPLIT)[1:]:
        handle = CARD_HANDLE_RE.search(chunk)
        price = CARD_PRICE_RE.search(chunk)
        if not handle or not price:
            continue
        stock = CARD_STOCK_RE.search(chunk)
        stock_text = re.sub(r"\s+", " ", stock.group(1)).strip() if stock else ""
        units = UNITS_RE.search(stock_text)
        badge = CARD_BADGE_RE.search(chunk)
        cards[unquote(handle.group(1))] = {   # hrefs are percent-encoded ("rtx%E2%84%A2")
            "on_request": ON_REQUEST in price.group(1).lower(),
            "stock_qty": int(units.group(1).replace(",", "")) if units else None,
            "stock_text": stock_text,
            # "New Factory sealed" -> "New Factory Sealed"
            "badge": " ".join(w.capitalize() for w in badge.group(1).split()) if badge else "",
        }
    return cards


def fetch_listing(wanted=None, max_pages=0):
    """Every product card of /collections/all, one page at a time until a page has no cards.
    Test runs: stop once all the wanted handles are seen, or after max_pages."""
    cards, page = {}, 1
    while not max_pages or page <= max_pages:
        r = http.session().get(f"{BASE}/collections/all", params={"page": page}, timeout=60)
        r.raise_for_status()
        found = parse_cards(r.text)
        if not found:
            return cards
        cards.update(found)
        if page % 25 == 0:
            print(f"listing page {page}: {len(cards)} cards", flush=True)
        if wanted and wanted <= cards.keys():
            return cards
        page += 1
        time.sleep(LIST_DELAY)
    return cards


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


# --- mapping -----------------------------------------------------------------
def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def part_number_of(sku, vendor, title):
    """The manufacturer part number inside the SKU, spelled as in the title, or ""."""
    sku = (sku or "").strip()
    toks = sku.split()
    if len(toks) == 1 and "-" in sku:
        # Internal style SKUs: "HP-663656-001-", "INTEL-SR1AZ-REF"
        code = sku.strip("-").partition("-")[2] or sku
        head, _, tail = code.rpartition("-")
        cand = head if head and tail.lower() in COND_WORDS else code
    else:
        # "dell st6000nm0034 refurbished", "palo alto pa-3060 refurbished", "lsi lsi 49571-01 ..."
        vend = (vendor or "").lower().split()
        i = 0
        while i < len(toks) - 1 and vend and toks[i].lower() == vend[i % len(vend)]:
            i += 1
        cand = toks[i] if i < len(toks) else ""
        if cand.lower() in COND_WORDS:
            cand = ""
    if not cand:
        return ""
    m = re.search(r"(?<![\w-])" + re.escape(cand) + r"(?![\w-])", title or "", re.I)
    return m.group(0) if m else ""


def condition_of(card, tags, sku):
    if card and card["badge"]:
        return card["badge"]
    upper = {t.upper() for t in tags}
    for tag, label in TAG_CONDITIONS:
        if tag in upper:
            return label
    last = (sku or "").lower().split()[-1:] or [""]
    return SKU_CONDITIONS.get(last[0], "")


def build_rows(product, card, scraped_at):
    title = re.sub(r"\s+", " ", product.get("title") or "").strip()
    category = product.get("product_type") or ""
    cap = CAPACITY_RE.search(title) if SIZED_RE.search(category) else None
    tags = product.get("tags") or []
    variants = product.get("variants", [])
    rows = []
    for v in variants:
        sku = (v.get("sku") or "").strip()
        price = to_float(v.get("price")) or None        # $0 is not a price -> empty
        if card and card["on_request"]:
            price = None                                # the store shows "Pricing on request"
        compare = to_float(v.get("compare_at_price"))
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": str(v["id"]),        # unique: one row per variant
            "parent_id": str(product["id"]),
            "variant_id": str(v["id"]),
            "sku": sku,
            "part_number": part_number_of(sku, product.get("vendor"), title),
            "title": title + ("" if v.get("title") in (None, "Default Title") else f" - {v['title']}"),
            "brand": product.get("vendor", ""),
            "category": category,
            "condition": condition_of(card, tags, sku),
            "capacity": (cap.group(1) + cap.group(2)) if cap else "",
            "price": price,
            "compare_at_price": compare if price and compare and compare > price else None,
            "available": bool(v.get("available")),
            # The card's count is per product; every product here has one variant
            "stock_qty": card["stock_qty"] if card and len(variants) == 1 else None,
            "url": f"{BASE}/products/{product['handle']}"
            + (f"?variant={v['id']}" if len(variants) > 1 else ""),
        }))
    return rows


def scrape(limit, scraped_at):
    catalog = fetch_catalog(limit)
    handles = {p["handle"] for p in catalog}
    # Test runs: the JSON is in listing order, so the first few listing pages cover it
    cards = fetch_listing(wanted=handles if limit else None,
                          max_pages=limit // 24 + 3 if limit else 0)
    seen = handles & cards.keys()
    print(f"listing: {len(cards)} cards, {len(seen)} of {len(handles)} products matched, "
          f"{sum(1 for h in seen if cards[h]['on_request'])} 'Pricing on request'", flush=True)
    if not limit and len(seen) < COVERAGE * len(handles):
        sys.exit(f"The listing only showed {len(seen)} of {len(handles)} products; not saving "
                 "JSON prices for products whose price may be hidden")
    rows = [row for p in catalog for row in build_rows(p, cards.get(p["handle"]), scraped_at)]
    listed = None if limit else listed_total()
    if not limit and listed and len(catalog) < 0.98 * listed:
        sys.exit(f"Only fetched {len(catalog)} of {listed} products ({len(catalog) / listed:.0%}); "
                 "not saving a partial snapshot")
    print(f"fetched {len(catalog)} products -> {len(rows)} variant rows (sitemap lists {listed})",
          flush=True)
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
