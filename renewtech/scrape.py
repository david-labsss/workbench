"""Scrape renewtech.com (RenewTech, Magento 2 store, refurbished servers / storage / components):
every run's full snapshot appended to Postgres.

Prices are assumed to be in EUR and stored as-is. RenewTech is an EU reseller; the currency could
not be confirmed from here because www.renewtech.com returns a plain nginx 403 to our scraping IP
(an IP/geo firewall block, not a JS challenge). The scraper logs the priceCurrency it actually
finds on the first product (JSON-LD offer / itemprop / Open Graph), so a US run can confirm the
assumption.

The robots-clean sitemap path is used (no product API assumed): robots.txt -> Sitemap: lines, or
/sitemap.xml by default. A Magento sitemap is usually an index of child sitemaps; the product
sitemaps (loc contains "product") are followed when present, otherwise every child sitemap is
followed. Child sitemaps may be gzipped. Each product page is read a few at a time and mapped from
its Product JSON-LD when present, falling back to Magento's own markup:
  * product id   - the add-to-cart form's hidden "product" input, or an inline current_product_id,
                   or data-product-id (stable Magento entity id); falls back to the SKU, then the
                   last URL segment;
  * sku / part#  - JSON-LD sku / mpn, or the [itemprop=sku] / .product.attribute.sku value;
  * title        - JSON-LD name or the h1 page title;
  * price        - JSON-LD offer price, or the price box's data-price-amount (finalPrice), which is
                   always a machine dot-decimal; the oldPrice amount becomes compare_at_price;
  * availability - JSON-LD availability, or the stock markup / add-to-cart button.
A page that is not a product page (no catalog-product-view body class, no Product JSON-LD and no
add-to-cart form) is a category / CMS page and is skipped.

If the store turns out to expose Magento GraphQL (/graphql) from a US IP, the avoira / teqex idiom
(POST /graphql, split by price under the 10k cap) would be a cleaner path; the sitemap path here is
the safer default because it is robots-clean and needs no attributes we cannot confirm.

NOTE: untested locally - renewtech.com 403s our IP from Pakistan, so this has only been
py_compile-checked here; the parent smoke-tests it on the US GitHub runner.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import gzip
import json
import re
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "renewtech"
BASE = "https://www.renewtech.com"
WORKERS = 5
CONDITIONS = {"newcondition": "New", "usedcondition": "Used",
              "refurbishedcondition": "Refurbished", "damagedcondition": "Damaged"}
OOS_WORDS = ("out of stock", "out-of-stock", "sold out", "nicht verfügbar", "ausverkauft",
             "notify me", "unavailable")
# Magento entity id on a product page: the add-to-cart form input, an inline JS id, or a data attr.
ID_RES = [
    re.compile(r'name=["\']product["\']\s+value=["\'](\d+)["\']', re.I),
    re.compile(r'value=["\'](\d+)["\']\s+name=["\']product["\']', re.I),
    re.compile(r'"current_product_id"\s*:\s*"?(\d+)"?', re.I),
    re.compile(r'data-product-id=["\'](\d+)["\']', re.I),
]


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def to_float(v):
    try:
        return float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _locs(xml):
    return re.findall(r"<loc>([^<]+)</loc>", xml)


def _fetch_xml(url):
    r = http.session().get(url, timeout=120)
    r.raise_for_status()
    try:
        return gzip.decompress(r.content).decode("utf-8", "replace")
    except (OSError, EOFError):
        return r.text


def _seed_sitemaps():
    """Sitemap URLs from robots.txt (Sitemap: lines), or /sitemap.xml as the default."""
    try:
        r = http.session().get(f"{BASE}/robots.txt", timeout=60)
        if r.ok:
            seeds = re.findall(r"(?im)^\s*Sitemap:\s*(\S+)\s*$", r.text)
            if seeds:
                return seeds
    except Exception as e:  # noqa: BLE001 - robots is only a hint; fall back to the default
        print(f"WARN robots.txt: {e}", flush=True)
    return [f"{BASE}/sitemap.xml"]


def product_urls():
    """Every product page URL, plus whether that list is reliably product-only.

    Returns (urls, product_only). When the sitemap index has product-named child sitemaps we
    follow only those and the count is a reliable product total; otherwise we follow every child
    (or a flat sitemap) and skip non-product pages while parsing, so no count is trusted.
    """
    pages, children = [], []
    for sm in _seed_sitemaps():
        try:
            xml = _fetch_xml(sm)
        except Exception as e:  # noqa: BLE001 - one bad sitemap must not stop the others
            print(f"WARN sitemap {sm}: {e}", flush=True)
            continue
        for loc in _locs(xml):
            (children if ".xml" in loc.lower() else pages).append(loc)

    product_only = False
    if children:
        product_children = [c for c in children if "product" in c.lower()]
        chosen = product_children or children
        product_only = bool(product_children)
        for sm in sorted(set(chosen)):
            try:
                xml = _fetch_xml(sm)
            except Exception as e:  # noqa: BLE001
                print(f"WARN sitemap {sm}: {e}", flush=True)
                continue
            for loc in _locs(xml):
                if ".xml" not in loc.lower():   # ignore a second level of nesting (rare)
                    pages.append(loc)

    urls = sorted(u for u in set(pages) if u.rstrip("/") != BASE)
    return urls, product_only


def product_jsonld(soup):
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except (ValueError, TypeError):
            continue
        nodes = data.get("@graph", data) if isinstance(data, dict) else data
        for node in nodes if isinstance(nodes, list) else [nodes]:
            t = node.get("@type") if isinstance(node, dict) else None
            if t == "Product" or (isinstance(t, list) and "Product" in t):
                return node
    return None


def _entity_id(html):
    for rx in ID_RES:
        m = rx.search(html)
        if m:
            return m.group(1)
    return ""


def _price_amount(soup, price_type):
    el = soup.select_one(f'[data-price-type="{price_type}"][data-price-amount]')
    return to_float(el.get("data-price-amount")) if el else None


def build_row(url, html, scraped_at, currency_seen):
    soup = BeautifulSoup(html, "html.parser")
    ld = product_jsonld(soup)
    body_classes = (soup.body.get("class") or []) if soup.body else []
    is_product = (ld is not None or "catalog-product-view" in body_classes
                  or "product_addtocart_form" in html)
    if not is_product:
        return None                   # category / CMS page

    entity_id = _entity_id(html)
    sku = clean(ld.get("sku")) if ld else ""
    if not sku:
        node = soup.select_one('[itemprop="sku"], .product.attribute.sku .value, .product-sku')
        sku = clean(node.get_text()) if node else ""
    mpn = (clean(ld.get("mpn")) if ld else "") or sku

    title = clean(ld.get("name")) if ld else ""
    if not title:
        h1 = soup.select_one("h1.page-title .base, h1.page-title, h1 .base, h1")
        title = clean(h1.get_text()) if h1 else ""

    brand = (ld or {}).get("brand")
    brand = clean(brand.get("name")) if isinstance(brand, dict) else clean(brand)

    offers = (ld or {}).get("offers") or {}
    offer = offers[0] if isinstance(offers, list) and offers else offers
    offer = offer if isinstance(offer, dict) else {}
    cur = offer.get("priceCurrency") or _meta(soup, itemprop="priceCurrency") \
        or _meta(soup, property="product:price:currency") or _meta(soup, property="og:price:currency")
    if cur and not currency_seen.get("value"):
        currency_seen["value"] = clean(cur)

    price = to_float(offer.get("price") or offer.get("lowPrice"))
    if price is None:
        price = _price_amount(soup, "finalPrice")
    if price is None:
        any_amt = soup.select_one("[data-price-amount]")
        price = to_float(any_amt.get("data-price-amount")) if any_amt else None
    compare = _price_amount(soup, "oldPrice")

    cond = str(offer.get("itemCondition") or "").lower()
    condition = next((v for k, v in CONDITIONS.items() if k in cond), "")

    avail_text = str(offer.get("availability") or "").lower()
    if avail_text:
        available = "instock" in avail_text or "limitedavailability" in avail_text \
            or "presale" in avail_text or "backorder" in avail_text
    else:
        low = html.lower()
        available = ("product_addtocart_form" in html or "available" in body_classes) \
            and not any(w in low for w in OOS_WORDS)

    product_id = entity_id or sku or clean(url.split("?")[0].rstrip("/").split("/")[-1])
    if not product_id:
        return None
    return schema.product_row({
        "scraped_at": scraped_at, "website": SITE,
        "product_id": product_id,
        "sku": sku, "part_number": mpn, "model_number": mpn,
        "title": title, "brand": brand,
        "condition": condition,
        "price": price,
        "compare_at_price": compare if (price and compare and compare > price) else None,
        "available": available,
        "url": url.split("?")[0],
    })


def _meta(soup, **attrs):
    tag = soup.find("meta", attrs=attrs)
    return tag.get("content") if tag and tag.get("content") else ""


def fetch(url):
    try:
        r = http.session().get(url, timeout=60)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return url, r.text
    except Exception as e:  # noqa: BLE001 - one bad page must not stop the run
        print(f"WARN {url}: {e}", flush=True)
        return None


def scrape(limit, scraped_at):
    urls, product_only = product_urls()
    print(f"sitemap lists {len(urls)} URLs (product-only sitemap: {product_only})", flush=True)
    todo = urls[:limit] if limit else urls
    rows, seen, currency_seen = [], set(), {}
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, page in enumerate(pool.map(fetch, todo), 1):
            if page:
                row = build_row(page[0], page[1], scraped_at, currency_seen)
                if row:
                    pid = row[schema.PRODUCT_COLUMNS.index("product_id")]
                    if pid and pid not in seen:
                        seen.add(pid)
                        rows.append(row)
            if i % 200 == 0 or i == len(todo):
                print(f"{i}/{len(todo)} pages, {len(rows)} products", flush=True)
    print(f"price currency seen on site: {currency_seen.get('value') or 'unknown'} "
          f"(stored as EUR by assumption)", flush=True)
    # Only trust a product count when the URLs came from a product-only sitemap.
    listed = len(urls) if (product_only and not limit) else None
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
