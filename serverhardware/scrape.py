"""Scrape server-hardware.com (Happyware Server Europe GmbH, Shopware 6, ~11,700 products: new
server hardware - CPUs, memory, storage, barebones, components): every run's full snapshot
appended to Postgres. The English sales channel (the site root) is scraped, so titles are in
English; prices are in EUR.

robots.txt disallows every query URL ("Disallow: /*/?"), so pagination / listing filters are off
limits. The sitemap and product pages are robots-clean, so every product in /sitemap.xml (an
index -> one gzipped sub-sitemap per sales channel) is read a few at a time. Product URLs have the
form /<brand>/<slug>_<ordernumber>-<id>; category / CMS pages sit at the site root and are left
out by PRODUCT_RE (they do not end with "-<digits>").

Each product page is server-rendered. There is no Product JSON-LD (only a BreadcrumbList), so the
fields come from the Open Graph product meta and schema.org microdata:
  * price is the Open Graph product meta (<meta property="product:price:amount">, EUR), the
    "from" price; it is present but 0 for sold-out items with no price (stored empty);
  * the store order number (itemprop="sku" / .product-detail-ordernumber, e.g. "SH1C7537-110979")
    is the id, unique per product;
  * the manufacturer part number is itemprop="mpn" and the brand is the product:brand meta;
  * the title is the <h1> (falls back to og:title);
  * availability comes from <link itemprop="availability"> - in stock when any offer is InStock,
    otherwise out of stock (OutOfStock / SoldOut);
  * the category is the deepest named level of the BreadcrumbList JSON-LD.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import gzip
import json
import re
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "serverhardware"
BASE = "https://www.server-hardware.com"
WORKERS = 5
# products are /<brand>/<slug>-<id>; root-level category / CMS pages are excluded
PRODUCT_RE = re.compile(r"^https://www\.server-hardware\.com/[^/]+/.+-\d+$")


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def to_float(v):
    try:
        f = float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    return f


def _locs(xml):
    return re.findall(r"<loc>([^<]+)</loc>", xml)


def product_urls():
    """Every product URL from the (gzipped) sales-channel sitemap behind /sitemap.xml."""
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
        urls += [loc for loc in _locs(xml) if PRODUCT_RE.match(loc)]
    return sorted(set(urls))


def _meta(soup, prop):
    tag = soup.find("meta", property=prop) or soup.find("meta", attrs={"itemprop": prop})
    return clean(tag.get("content")) if tag and tag.get("content") else ""


def _breadcrumb_category(soup):
    """The deepest named level of the BreadcrumbList JSON-LD (the product's own level has no URL)."""
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except (ValueError, TypeError):
            continue
        if isinstance(data, dict) and data.get("@type") == "BreadcrumbList":
            named = [clean(e.get("name")) for e in data.get("itemListElement", [])
                     if e.get("item") and e.get("name")]
            if named:
                return named[-1]
    return ""


def build_row(url, html, scraped_at):
    soup = BeautifulSoup(html, "html.parser")
    sku_tag = soup.find(attrs={"itemprop": "sku"})
    order_no = soup.select_one(".product-detail-ordernumber")
    product_id = (clean(sku_tag.get("content") or sku_tag.get_text()) if sku_tag else "") or \
        (clean(order_no.get_text()) if order_no else "")
    if not product_id:
        return None                    # not a buyable product page

    h1 = soup.select_one("h1")
    title = clean(h1.get_text()) if h1 else _meta(soup, "og:title")
    brand = _meta(soup, "product:brand")
    mpn = _meta(soup, "mpn")
    price = to_float(_meta(soup, "product:price:amount"))
    if price == 0:
        price = None                   # sold-out items carry a 0 "price", i.e. no price

    avail = {(l.get("href") or "").rsplit("/", 1)[-1].lower()
             for l in soup.select("link[itemprop='availability']")}
    available = "instock" in avail

    return schema.product_row({
        "scraped_at": scraped_at, "website": SITE,
        "product_id": product_id, "sku": product_id,
        "part_number": mpn, "model_number": mpn,
        "title": title, "brand": brand,
        "category": _breadcrumb_category(soup),
        "price": price,
        "available": available,
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
