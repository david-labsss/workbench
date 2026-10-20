"""Scrape vertical-enterprise.com's priced shop (shop.vertical-enterprise.com, ~165 products):
every run's full snapshot appended to Postgres.

Vertical Enterprise has two storefronts. The main site's /store/ is quote-only (no prices), so it
is ignored. The priced shop lives on the shop. subdomain, a small custom catalogue listed 60 to a
page at /products/?page=N. The catalogue is tiny, so the scraper collects the product URLs from
the listing pages and then opens each product page, which carries clean, labelled fields:

  <div class="detail-line">Part Number: <strong>817718-B21</strong></div>
  ... Manufacturer, Category, Model Number, Price, Condition ...
  <input type="hidden" name="node_id" value="274678554188588747">   (the product id)

Relative links resolve against the page's <base href> (the shop root). The shop does not expose
stock counts; every priced item is treated as available. The listed count is the number of
distinct product URLs found across the listing pages.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "verticalenterprise"
BASE = "https://shop.vertical-enterprise.com/"
LISTING = urljoin(BASE, "products/")
WORKERS = 4
MAX_PAGES = 50                      # safety stop for the listing walk
PRICE_RE = re.compile(r"[\d,]+(?:\.\d+)?")
NODE_ID_RE = re.compile(r'name="node_id"[^>]*value="(\d+)"')


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def get(url):
    r = http.session().get(url, timeout=60)
    r.raise_for_status()
    return r.text


def listing_urls(limit):
    """Distinct product-page URLs from the listing pages, in order."""
    urls, page = [], 1
    while page <= MAX_PAGES:
        soup = BeautifulSoup(get(f"{LISTING}?page={page}"), "html.parser")
        found = []
        for a in soup.find_all("a", href=True):
            if re.search(r"/products/.+\.html", a["href"]) and a.select_one(".title"):
                found.append(urljoin(BASE, a["href"]))
        new = [u for u in dict.fromkeys(found) if u not in urls]
        if not new:
            break
        urls += new
        print(f"listing page {page}: {len(urls)} products so far", flush=True)
        if limit and len(urls) >= limit:
            break
        page += 1
    return urls


def detail_lines(soup):
    """{label: value} from the product page's <div class="detail-line">Label: <strong>v</strong>."""
    out = {}
    for line in soup.select(".detail-line"):
        strong = line.find("strong")
        if not strong:
            continue
        label = clean(line.get_text().split(":", 1)[0])
        out[label.lower()] = clean(strong.get_text())
    return out


def build_row(url, html, scraped_at):
    soup = BeautifulSoup(html, "html.parser")
    fields = detail_lines(soup)
    m = NODE_ID_RE.search(html)
    node_id = m.group(1) if m else None
    if not node_id:
        print(f"WARN no node_id on {url}", flush=True)
        return None
    h1 = soup.find("h1")
    price_text = fields.get("price", "")
    pm = PRICE_RE.search(price_text)
    part = fields.get("part number", "")
    model = fields.get("model number", "")

    # Description: the product page's #description tab (drop its "Description" sub-heading),
    # falling back to the short .description block.
    desc_el = soup.find(id="description") or soup.select_one(".description")
    if desc_el:
        for heading in desc_el.select(".mobile-title"):
            heading.extract()
        description = schema.plain_text(desc_el.get_text(" "))
    else:
        description = None
    # Image: the shop renders product images client-side - the fetched HTML has no <img> and no
    # og:image - so image_url is usually None (nothing to read without a JS render).
    og = soup.find("meta", property="og:image")
    image_url = urljoin(BASE, og["content"]) if og and og.get("content") else None

    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": node_id,
        "sku": part,
        "part_number": part,
        "model_number": model if model and model != part else "",
        "title": clean(h1.get_text()) if h1 else "",
        "brand": fields.get("manufacturer", ""),
        "category": fields.get("category", ""),
        "condition": fields.get("condition", "").title(),
        "price": float(pm.group(0).replace(",", "")) if pm else None,
        "available": True,          # priced shop; stock not exposed
        "image_url": image_url,
        "description": description,
        "url": url,
    })


def fetch(url, scraped_at):
    try:
        return build_row(url, get(url), scraped_at)
    except Exception as e:          # noqa: BLE001 - one bad page must not stop the run
        print(f"WARN {url}: {e}", flush=True)
        return None


def scrape(limit, scraped_at):
    urls = listing_urls(limit)
    urls = urls[:limit] if limit else urls
    print(f"{len(urls)} product pages to fetch", flush=True)
    with ThreadPoolExecutor(WORKERS) as pool:
        rows = [r for r in pool.map(lambda u: fetch(u, scraped_at), urls) if r is not None]
    return rows, (len(urls) if not limit else None)


if __name__ == "__main__":
    runner.run(SITE, scrape)
