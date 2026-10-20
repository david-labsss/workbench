"""Scrape bluelink.nl (Bluelink B.V., Lenovo/IBM Platinum Business Partner, custom nginx/PHP store,
~3,450 products: Lenovo & IBM server parts, options and warranties): every run's full snapshot
appended to Postgres. Prices are in EUR (net / excl. VAT, as the store publishes them).

The store's on-site search renders HTML (no JSON API) and its listings are capped, so the whole
catalogue is read from /sitemap.xml instead: the Dutch /product/<code> URLs (the /en/ copies are
skipped). Each product page carries a clean Product JSON-LD block (a one-element array) with the
name, the product code ("sku", = the manufacturer part number), the brand and an offer (price, EUR,
valueAddedTaxIncluded=false, availability, item condition). Pages are read a few at a time.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import json
import re
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "bluelink"
BASE = "https://www.bluelink.nl"
WORKERS = 5
PRODUCT_RE = re.compile(r"^https://www\.bluelink\.nl/product/([^/?#]+)$", re.I)
CONDITIONS = {"newcondition": "New", "usedcondition": "Used",
              "refurbishedcondition": "Refurbished", "damagedcondition": "Damaged"}


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def to_float(v):
    try:
        return float(str(v).replace(" ", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def product_urls():
    """Every Dutch /product/<code> URL from the sitemap (the /en/ copies are skipped)."""
    r = http.session().get(f"{BASE}/sitemap.xml", timeout=120)
    r.raise_for_status()
    urls = [loc for loc in re.findall(r"<loc>([^<]+)</loc>", r.text) if PRODUCT_RE.match(loc)]
    return sorted(set(urls))


def product_jsonld(soup):
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except (ValueError, TypeError):
            continue
        nodes = data if isinstance(data, list) else [data]
        for node in nodes:
            t = node.get("@type") if isinstance(node, dict) else None
            if t == "Product" or (isinstance(t, list) and "Product" in t):
                return node
    return None


def build_row(url, html, scraped_at):
    soup = BeautifulSoup(html, "html.parser")
    ld = product_jsonld(soup)
    if not ld:
        return None
    code = PRODUCT_RE.match(url).group(1)
    brand = ld.get("brand")
    brand = clean(brand.get("name")) if isinstance(brand, dict) else clean(brand)
    sku = clean(ld.get("sku")) or code
    mpn = clean(ld.get("mpn"))
    offers = ld.get("offers") or {}
    offer = offers[0] if isinstance(offers, list) and offers else offers
    offer = offer if isinstance(offer, dict) else {}
    price = to_float(offer.get("price") or offer.get("lowPrice"))
    avail = str(offer.get("availability") or "").lower()
    cond = str(offer.get("itemCondition") or "").lower()
    condition = next((v for k, v in CONDITIONS.items() if k in cond), "")
    return schema.product_row({
        "scraped_at": scraped_at, "website": SITE,
        "product_id": code,
        "sku": sku, "part_number": mpn or sku, "model_number": mpn,
        "title": clean(ld.get("name")), "brand": brand,
        "condition": condition,
        "price": price,
        "available": "instock" in avail or "limitedavailability" in avail,
        "url": url,
    })


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
    urls = product_urls()
    print(f"sitemap lists {len(urls)} products", flush=True)
    todo = urls[:limit] if limit else urls
    rows, seen = [], set()
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, page in enumerate(pool.map(fetch, todo), 1):
            if page:
                row = build_row(page[0], page[1], scraped_at)
                if row:
                    pid = row[schema.PRODUCT_COLUMNS.index("product_id")]
                    if pid and pid not in seen:
                        seen.add(pid)
                        rows.append(row)
            if i % 200 == 0 or i == len(todo):
                print(f"{i}/{len(todo)} products, {len(rows)} rows", flush=True)
    return rows, len(urls)


if __name__ == "__main__":
    runner.run(SITE, scrape)
