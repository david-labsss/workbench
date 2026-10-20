"""Scrape the server / storage / networking slice of industry-electronics.com (Lieske Elektronik,
DE broadline electronics B2B): every run's full snapshot appended to Postgres.

The store (nginx/PHP, custom "shopcms", no Cloudflare) publishes a schema.org JSON-LD Product with
an Offer on every product page. No API key, no browser.

SCOPING -- why we don't walk the artikel sitemaps. The full catalogue is ~0.9M products (18
"lieskesitemap_artikel-*" sitemaps), far too big for one daily run. industry-electronics is a
broadline electronics distributor, so we restrict to its IT / EDP server-relevant tree only. The
category sitemap (/sitemaps/lieskesitemap_kategorien.xml) lists ~1.6k category pages with clean
hierarchical paths; SCOPE_PREFIXES picks the server / storage / networking subtrees under
/it-equipment/ (19-inch racks & cabinets, computer hardware incl. servers/workstations/barebones,
computer components incl. HDD/SSD/RAM/CPU/mainboards/controllers/PSU, networking, storage,
storage media and UPS). Deliberately excluded: printers-scanners, software, office-equipment,
consumables, monitors, mobile-computing and the non-IT top categories (electrics, electronics,
light-sound, technology). This is ~34k products (~4% of the catalogue), which fits one run.

How products are found: a leaf category page lists 24 products and paginates with ?eintrag=<offset>
(step 24); an out-of-range offset returns an empty page (clean stop). Mid-level category pages only
show sub-category navigation (no products), so paginating every scoped category URL -- leaves
included -- covers the whole slice. Product links are "/<manufacturer>/<slug>-lieske_<id>.htm"
(the "<id>" is Lieske's article number = the JSON-LD sku = product_id); we dedupe on it so a
product listed under several categories is fetched once. The per-leaf "Top sellers" carousel holds
extra in-category products, so they are kept. We then fetch each product page for its JSON-LD.
robots.txt only disallows cart/search/account/json endpoints, not these category or product pages.

Per product the JSON-LD gives: name, sku (= product_id, matches the trailing _<id> in the URL),
mpn (manufacturer part number), brand, and an Offer with priceCurrency ("EUR"), price, availability
and itemCondition (New/Used/Refurbished). category comes from the BreadcrumbList leaf when present.
The store shows no per-product stock count, so stock_qty stays blank; availability is the in-stock
flag. Prices are in euros (EUR); the price number is stored as-is.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import html
import json
import re
from concurrent.futures import ThreadPoolExecutor

from common import http, runner, schema

SITE = "industryelectronics"
BASE = "https://industry-electronics.com"
CATEGORY_SITEMAP = f"{BASE}/sitemaps/lieskesitemap_kategorien.xml"
WORKERS = 6
CURRENCY = "EUR"
PAGE_STEP = 24           # products per category listing page (?eintrag=<offset>)
MAX_PAGES_PER_CAT = 1000  # safety stop (1000 * 24 = 24k, well above the largest leaf)

# server / storage / networking category subtrees we keep (substring match on the category URL)
SCOPE_PREFIXES = (
    "/it-equipment/19-inch-technology/",
    "/it-equipment/computer-hardware/",
    "/it-equipment/computer-components/",
    "/it-equipment/networks/",
    "/it-equipment/storage/",
    "/it-equipment/storage-media/",
    "/it-equipment/ups/",
)

LOC_RE = re.compile(r"<loc>\s*([^<]+?)\s*</loc>", re.I)
LD_RE = re.compile(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', re.S | re.I)
# product links on category listing pages: /<manufacturer>/<slug>-lieske_<id>.htm
PRODUCT_LINK_RE = re.compile(r'href="(/[^"]*?lieske_(\d+)\.htm)"', re.I)


def clean(text):
    return re.sub(r"\s+", " ", html.unescape((text or "").replace("\xa0", " "))).strip()


def to_float(v):
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


def scoped_categories():
    """The server / storage / networking category pages from the category sitemap."""
    r = http.session().get(CATEGORY_SITEMAP, timeout=120)
    r.raise_for_status()
    return [u for u in LOC_RE.findall(r.text)
            if any(prefix in u for prefix in SCOPE_PREFIXES)]


def category_products(cat_url):
    """Paginate one category's listing pages and return {product_id: absolute_url}. Mid-level
    category pages carry no products (just sub-category links) and yield {}. Stops as soon as a
    page adds no new product -- an out-of-range ?eintrag offset returns an empty listing."""
    found = {}
    for page in range(MAX_PAGES_PER_CAT):
        url = cat_url if page == 0 else f"{cat_url}?eintrag={page * PAGE_STEP}"
        try:
            r = http.session().get(url, timeout=60)
            if r.status_code == 404:
                break
            r.raise_for_status()
        except Exception as e:  # noqa: BLE001 - a bad listing page must not stop the run
            print(f"WARN {url}: {e}", flush=True)
            break
        new = 0
        for href, pid in PRODUCT_LINK_RE.findall(r.text):
            if pid not in found:
                found[pid] = BASE + href
                new += 1
        if new == 0:  # empty / repeated page -> end of this category
            break
    return found


def product_urls(limit=0):
    """Unique scoped product URLs, found by crawling the server / storage / networking category
    listing pages. When `limit` is set (test runs), crawl categories one at a time and stop as
    soon as enough products are collected; otherwise crawl categories in parallel."""
    cats = scoped_categories()
    print(f"{len(cats)} scoped category pages", flush=True)
    merged = {}
    if limit:
        for cat in cats:
            merged.update(category_products(cat))
            if len(merged) >= limit:
                break
    else:
        with ThreadPoolExecutor(WORKERS) as pool:
            for part in pool.map(category_products, cats):
                merged.update(part)
    return list(merged.values())


def iter_ld(html_text):
    for block in LD_RE.findall(html_text):
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
    names = []
    for it in breadcrumb.get("itemListElement") or []:
        name = it.get("name") or (it.get("item") or {}).get("name")
        if name:
            names.append(clean(name))
    names = [n for n in names if n.lower() not in ("home", "startseite", "")]
    return names[-2] if len(names) >= 2 else (names[0] if names else "")


def parse(url, html_text, scraped_at):
    nodes = list(iter_ld(html_text))
    product = node_of(nodes, "Product")
    if not product:
        return None
    sku = clean(product.get("sku"))
    if not sku:
        return None
    offers = product.get("offers")
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    offers = offers or {}
    brand = product.get("brand")
    brand = clean(brand.get("name")) if isinstance(brand, dict) else clean(brand)
    condition = str(offers.get("itemCondition") or "").rsplit("/", 1)[-1].replace("Condition", "")
    image = product.get("image")
    if isinstance(image, list):
        image = image[0] if image else ""
    image_url = clean(image) or None
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": sku,
        "sku": sku,
        "part_number": clean(product.get("mpn")),
        "title": clean(product.get("name")),
        "brand": brand,
        "category": category_from(node_of(nodes, "BreadcrumbList")),
        "condition": condition,
        "price": to_float(offers.get("price")),
        "available": available_from(offers.get("availability")),
        "image_url": image_url,
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
    urls = product_urls(limit)
    print(f"scoped categories list {len(urls)} products{' (capped for test)' if limit else ''}",
          flush=True)
    todo = urls[:limit] if limit else urls
    rows, seen = [], set()
    pid_idx = schema.PRODUCT_COLUMNS.index("product_id")
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, page in enumerate(pool.map(fetch_page, todo), 1):
            row = parse(page[0], page[1], scraped_at) if page else None
            if row and row[pid_idx] not in seen:
                seen.add(row[pid_idx])
                rows.append(row)
            if i % 500 == 0 or i == len(todo):
                print(f"{i}/{len(todo)} pages, {len(rows)} products", flush=True)
    return rows, len(urls)


if __name__ == "__main__":
    runner.run(SITE, scrape)
