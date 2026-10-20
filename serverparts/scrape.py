"""Scrape serverparts.pl (ITCare, custom Apache/PHP store, ~4,900 products: new server parts --
LSI/Broadcom, Supermicro, Intel, HPE, Dell): every run's full snapshot appended to Postgres.
Prices are in PLN.

The store has no public product API, so every product in its XML sitemap is read a few at a time.
/sitemap.xml is an index pointing at /sitemap-products.xml, which lists the live catalogue as URLs
of the form /<slug>-i<id> (the trailing -i<id> is the store's internal product id). Each product
page carries a single clean Product JSON-LD block with the name, the manufacturer part number
("sku", e.g. P4X-UPE51620V2-SR1AR), the store's numeric id ("mpn"), the brand and an offer (price,
PLN, availability, item condition). The end-of-life sitemap (sitemap-products-eol.xml) is skipped.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import json
import re
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "serverparts"
BASE = "https://www.serverparts.pl"
WORKERS = 5
ID_RE = re.compile(r"-i(\d+)/?$", re.I)
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
    """Every product URL from sitemap-products.xml (found via the sitemap index)."""
    r = http.session().get(f"{BASE}/sitemap.xml", timeout=120)
    r.raise_for_status()
    sitemaps = [loc for loc in re.findall(r"<loc>([^<]+)</loc>", r.text)
                if re.search(r"sitemap-products\.xml", loc)]
    urls = []
    for sm in sitemaps:
        rs = http.session().get(sm, timeout=120)
        rs.raise_for_status()
        urls += [loc for loc in re.findall(r"<loc>([^<]+)</loc>", rs.text) if ID_RE.search(loc)]
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
    m = ID_RE.search(url)
    url_id = m.group(1) if m else ""
    brand = ld.get("brand")
    brand = clean(brand.get("name")) if isinstance(brand, dict) else clean(brand)
    sku = clean(ld.get("sku"))
    internal = clean(ld.get("mpn")) or url_id          # numeric store id
    offers = ld.get("offers") or {}
    offer = offers[0] if isinstance(offers, list) and offers else offers
    offer = offer if isinstance(offer, dict) else {}
    price = to_float(offer.get("price") or offer.get("lowPrice"))
    avail = str(offer.get("availability") or "").lower()
    cond = str(offer.get("itemCondition") or "").lower()
    condition = next((v for k, v in CONDITIONS.items() if k in cond), "")
    # JSON-LD "image" is the real product photo when the product has one, and the site logo
    # (/media/img/logo.png) when it doesn't -- store None in that case, not the logo.
    image = ld.get("image")
    image = image[0] if isinstance(image, list) and image else image
    image = clean(image) if isinstance(image, str) else None
    if image and "/media/img/" in image:
        image = None
    return schema.product_row({
        "scraped_at": scraped_at, "website": SITE,
        "product_id": url_id or internal or sku,
        "sku": sku, "part_number": sku, "model_number": internal,
        "title": clean(ld.get("name")), "brand": brand,
        "condition": condition,
        "price": price,
        "available": "instock" in avail or "limitedavailability" in avail,
        "image_url": image,
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
