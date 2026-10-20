"""Scrape gekko-computer.de (GEKKO, custom "ForSellers" PHP store, ~930 products, refurbished
servers / components): every run's full snapshot appended to Postgres.

Prices are in EUR. The store has no public product API, so every product in its sitemap
(/sitemapindex.xml -> sitemap-Articles-*.xml, links of the form /p/<slug>) is read a few at a
time. Each product page carries a clean Product JSON-LD block with the name, the store's internal
id ("sku"), the manufacturer part number ("mpn"), the brand and an offer (price, EUR,
availability, item condition). The manufacturer part number falls back to the last URL segment.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import json
import re
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "gekko"
BASE = "https://www.gekko-computer.de"
WORKERS = 5
PRODUCT_RE = re.compile(r"^https?://[^/]+/p/(.+)$", re.I)
CONDITIONS = {"newcondition": "New", "usedcondition": "Used",
              "refurbishedcondition": "Refurbished", "damagedcondition": "Damaged"}


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def to_float(v):
    try:
        return float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def abs_image(img):
    """First image URL from a schema.org image value (str / list / ImageObject), absolute."""
    if isinstance(img, list):
        img = img[0] if img else None
    if isinstance(img, dict):
        img = img.get("url") or img.get("contentUrl")
    if not isinstance(img, str) or not img.strip():
        return None
    return urljoin(BASE, img.strip())


def product_urls():
    """Every /p/<slug> URL from the Articles sitemaps (via the sitemap index)."""
    r = http.session().get(f"{BASE}/sitemapindex.xml", timeout=120)
    r.raise_for_status()
    sitemaps = [loc for loc in re.findall(r"<loc>([^<]+)</loc>", r.text) if "Articles" in loc]
    urls = []
    for sm in sitemaps:
        rs = http.session().get(sm, timeout=120)
        rs.raise_for_status()
        urls += [loc for loc in re.findall(r"<loc>([^<]+)</loc>", rs.text) if PRODUCT_RE.match(loc)]
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


def build_row(url, html, scraped_at):
    soup = BeautifulSoup(html, "html.parser")
    ld = product_jsonld(soup)
    if not ld:
        return None
    pn_from_url = clean(PRODUCT_RE.match(url).group(1)).split("-")[-1]
    brand = ld.get("brand")
    brand = clean(brand.get("name")) if isinstance(brand, dict) else clean(brand)
    internal = clean(ld.get("sku"))
    mpn = clean(ld.get("mpn")) or pn_from_url
    offers = ld.get("offers") or {}
    offer = offers[0] if isinstance(offers, list) and offers else offers
    offer = offer if isinstance(offer, dict) else {}
    price = to_float(offer.get("price") or offer.get("lowPrice"))
    avail = str(offer.get("availability") or "").lower()
    cond = str(offer.get("itemCondition") or "").lower()
    condition = next((v for k, v in CONDITIONS.items() if k in cond), "")
    return schema.product_row({
        "scraped_at": scraped_at, "website": SITE,
        "product_id": internal or pn_from_url,
        "sku": internal, "part_number": mpn, "model_number": mpn,
        "title": clean(ld.get("name")), "brand": brand,
        "condition": condition,
        "price": price,
        "available": "instock" in avail or "limitedavailability" in avail,
        "image_url": abs_image(ld.get("image")),
        "description": schema.plain_text(ld.get("description")),
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
