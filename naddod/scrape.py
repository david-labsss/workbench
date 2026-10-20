"""Scrape naddod.com (HK/CN optical networking vendor): every run's full snapshot appended to Postgres.

naddod.com is a custom React/Vue storefront behind Cloudflare, but every product page ships a
clean schema.org JSON-LD Product block (sku, mpn, brand, offers) plus a BreadcrumbList, and the
HTML is served to a normal browser client without a challenge. So the fastest complete public
path is the product sitemap (/sitemaps/products.xml, ~8.7k English product URLs) + each product
page's JSON-LD -- no API key, no browser, no Cloudflare bypass.

Per product the JSON-LD gives: name, sku (naddod's numeric product id = product_id), mpn
(manufacturer part number), brand, description, and an Offer with priceCurrency ("USD"), price,
itemCondition and availability (schema.org/InStock, PreOrder, ...). category is the second-to-last
BreadcrumbList entry (the leaf category, the last entry being the product itself). The store shows
no per-product stock count, so stock_qty stays blank; availability is the in-stock flag.

Prices are in US dollars (USD); the price number is stored as-is.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import html
import json
import re
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

from common import http, runner, schema

SITE = "naddod"
BASE = "https://www.naddod.com"
PRODUCT_SITEMAP = f"{BASE}/sitemaps/products.xml"
WORKERS = 6
CURRENCY = "USD"

LOC_RE = re.compile(r"<loc>\s*([^<]+?)\s*</loc>", re.I)
LD_RE = re.compile(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', re.S | re.I)
# Manufacturer brand from the product title (names lead with the maker), else JSON-LD brand
BRANDS = [
    "NVIDIA", "Mellanox", "Cisco", "Arista", "Juniper", "HPE", "HP", "Dell EMC", "Dell",
    "Intel", "Broadcom", "Finisar", "Brocade", "Lenovo", "IBM", "Huawei", "H3C", "Supermicro",
    "Marvell", "Fortinet", "Extreme", "Netgear", "Ubiquiti", "Ruijie", "Zyxel", "D-Link",
    "TP-Link", "Palo Alto", "Avago", "QLogic", "Molex", "Amphenol", "Nokia", "ZTE", "Fujitsu",
]
BRAND_RE = re.compile(r"\b(" + "|".join(re.escape(b) for b in BRANDS) + r")\b", re.I)


def clean(text):
    return re.sub(r"\s+", " ", html.unescape((text or "").replace("\xa0", " "))).strip()


def to_float(v):
    try:
        return float(str(v).replace(",", ""))
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
    r = http.session().get(PRODUCT_SITEMAP, timeout=120)
    r.raise_for_status()
    return sorted(set(LOC_RE.findall(r.text)))


def iter_ld(html):
    """Every JSON-LD object on the page, walking into @graph and arrays."""
    for block in LD_RE.findall(html):
        try:
            data = json.loads(block.strip())
        except ValueError:
            continue
        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                yield node
                graph = node.get("@graph")
                if isinstance(graph, list):
                    stack.extend(graph)
            elif isinstance(node, list):
                stack.extend(node)


def node_of(nodes, type_name):
    for n in nodes:
        t = n.get("@type")
        if t == type_name or (isinstance(t, list) and type_name in t):
            return n
    return None


def available_from(availability):
    leaf = str(availability or "").rsplit("/", 1)[-1].lower()
    return leaf in ("instock", "limitedavailability", "onlineonly", "instoreonly")


def category_from(breadcrumb):
    if not breadcrumb:
        return ""
    items = breadcrumb.get("itemListElement") or []
    names = []
    for it in items:
        name = it.get("name") or (it.get("item") or {}).get("name")
        if name:
            names.append(clean(name))
    # drop a leading "Home" and the trailing product-name entry
    names = [n for n in names if n.lower() not in ("home", "naddod")]
    return names[-2] if len(names) >= 2 else (names[0] if names else "")


def brand_from(title, ld_brand):
    m = BRAND_RE.search(title or "")
    if m:
        b = m.group(1)
        return {"HP": "HP", "HPE": "HPE", "IBM": "IBM", "ZTE": "ZTE", "H3C": "H3C"}.get(
            b.upper(), b if b.isupper() else b.title())
    return clean(ld_brand)


def parse(url, html, scraped_at):
    nodes = list(iter_ld(html))
    product = node_of(nodes, "Product")
    if not product:
        return None
    offers = product.get("offers")
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    offers = offers or {}
    title = clean(product.get("name"))
    sku = clean(product.get("sku"))
    if not sku:
        return None
    ld_brand = (product.get("brand") or {}).get("name") if isinstance(product.get("brand"), dict) \
        else product.get("brand")
    condition = str(offers.get("itemCondition") or "").rsplit("/", 1)[-1].replace("Condition", "")
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": sku,
        "sku": sku,
        "part_number": clean(product.get("mpn")),
        "title": title,
        "brand": brand_from(title, ld_brand),
        "category": category_from(node_of(nodes, "BreadcrumbList")),
        "condition": condition,
        "price": to_float(offers.get("price")),
        "available": available_from(offers.get("availability")),
        "image_url": abs_image(product.get("image")),
        "description": schema.plain_text(product.get("description")),
        "url": url,
    })


def fetch_page(url):
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
        for i, page in enumerate(pool.map(fetch_page, todo), 1):
            row = parse(page[0], page[1], scraped_at) if page else None
            pid = row[schema.PRODUCT_COLUMNS.index("product_id")] if row else None
            if row and pid not in seen:
                seen.add(pid)
                rows.append(row)
            if i % 200 == 0 or i == len(todo):
                print(f"{i}/{len(todo)} pages, {len(rows)} products", flush=True)
    return rows, len(urls)


if __name__ == "__main__":
    runner.run(SITE, scrape)
