"""Scrape ram-koenig.de (RAM-König, Shopware 6, ~2,450 products, refurbished PCs / servers /
memory): every run's full snapshot appended to Postgres. The English sales channel
(en.ram-koenig.de) is used, so prices are in EUR.

Shopware's Store API needs a sales-channel access key that this storefront does not expose, so
every product in the sitemap (/sitemap.xml -> a gzipped product sitemap; product URLs have no
trailing slash) is read a few at a time. Each product page is server-rendered:
  * the price is the Open Graph product meta (<meta property="product:price:amount">), present
    even when the item is out of stock;
  * the store's product number (.product-detail-ordernumber) is the id, and also the last URL
    segment;
  * the title is .product-detail-name and availability is .delivery-information
    ("No longer available" -> out of stock);
  * a Product JSON-LD block, when present, adds the brand and the manufacturer part number.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import gzip
import json
import re
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "ramkoenig"
BASE = "https://en.ram-koenig.de"
WORKERS = 5
# product URLs have no trailing slash; category / CMS pages end with "/"
PRODUCT_RE = re.compile(r"^https://en\.ram-koenig\.de/.+[^/]$")
NOT_AVAILABLE = ("no longer available", "not available", "sold out", "nicht verfügbar")
CONDITIONS = {"newcondition": "New", "usedcondition": "Used",
              "refurbishedcondition": "Refurbished", "damagedcondition": "Damaged"}


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def to_float(v):
    try:
        return float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _locs(xml):
    return re.findall(r"<loc>([^<]+)</loc>", xml)


def product_urls():
    r = http.session().get(f"{BASE}/sitemap.xml", timeout=120)
    r.raise_for_status()
    urls = []
    for sm in _locs(r.text):
        rs = http.session().get(sm, timeout=120)
        rs.raise_for_status()
        try:
            xml = gzip.decompress(rs.content).decode("utf-8", "replace")
        except (OSError, EOFError):
            xml = rs.text            # already-decompressed XML
        urls += [loc for loc in _locs(xml)
                 if PRODUCT_RE.match(loc) and loc.rstrip("/") != BASE]
    return sorted(set(urls))


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


def _meta(soup, **attrs):
    tag = soup.find("meta", attrs=attrs)
    return clean(tag.get("content")) if tag and tag.get("content") else ""


def build_row(url, html, scraped_at):
    soup = BeautifulSoup(html, "html.parser")
    onum = soup.select_one(".product-detail-ordernumber")
    product_id = clean(onum.get_text()) if onum else ""
    if not product_id:
        return None                   # not a product page (category / CMS)
    name = soup.select_one(".product-detail-name")
    delivery = soup.select_one(".delivery-information, .product-detail-delivery-information")
    delivery_text = clean(delivery.get_text()) if delivery else ""
    price = to_float(_meta(soup, property="product:price:amount")) or \
        to_float(_meta(soup, itemprop="price"))

    ld = product_jsonld(soup) or {}
    brand = ld.get("brand")
    brand = clean(brand.get("name")) if isinstance(brand, dict) else clean(brand)
    title = clean(name.get_text()) if name else clean(ld.get("name"))
    if not brand and title:
        brand = title.split()[0]
    mpn = clean(ld.get("mpn"))
    offers = ld.get("offers") or {}
    offer = offers[0] if isinstance(offers, list) and offers else offers
    cond = str((offer or {}).get("itemCondition") or "").lower()

    available = bool(delivery_text) and not any(p in delivery_text.lower() for p in NOT_AVAILABLE)
    return schema.product_row({
        "scraped_at": scraped_at, "website": SITE,
        "product_id": product_id, "sku": product_id,
        "part_number": mpn, "model_number": mpn,
        "title": title, "brand": brand,
        "condition": next((v for k, v in CONDITIONS.items() if k in cond), ""),
        "price": price,
        "available": available,
        "other_specs": delivery_text,
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
