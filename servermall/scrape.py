"""Scrape servermall.com (UAB ServerMall, EU refurbished servers & parts): every run's full
snapshot appended to Postgres.

servermall.com is a 1C-Bitrix storefront behind Cloudflare, but the HTML is served to a normal
browser client without a challenge and every product page ships a schema.org JSON-LD Product with
a nested Offer and a rich additionalProperty spec list. The store is multilingual (en + de/es/fr/
it/pl); the English pages carry no language prefix. The fastest complete public path is the two
XML sitemaps -- product-sitemap.xml (ready-made servers & parts, /catalog/<category>/<slug>/) and
config-sitemap.xml (build-to-config base servers, /config/<slug>/) -- each English URL fetched and
its JSON-LD parsed. No API key, no browser.

Per product the JSON-LD gives: name, sku, category, brand, additionalProperty (Condition, Form
factor, CPU, RAM, PSU, ...) and an Offer with priceCurrency ("EUR"), price, availability and
itemCondition. product_id is the URL path (stable and unique across catalog + config). The store
publishes no per-product stock count, so stock_qty stays blank; availability is the in-stock flag.
Configurable servers list their base configuration price.

Prices are in euros (EUR); the price number is stored as-is.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import html
import json
import re
from concurrent.futures import ThreadPoolExecutor

from common import http, runner, schema

SITE = "servermall"
BASE = "https://servermall.com"
SITEMAPS = [f"{BASE}/product-sitemap.xml", f"{BASE}/config-sitemap.xml"]
WORKERS = 5
CURRENCY = "EUR"

LOC_RE = re.compile(r"<loc>\s*([^<]+?)\s*</loc>", re.I)
LD_RE = re.compile(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', re.S | re.I)
# English product/config URL (other languages carry a /de//es//fr//it//pl/ prefix we skip)
PRODUCT_URL_RE = re.compile(r"^https://servermall\.com/(catalog|config)/(.+)$")

# additionalProperty name (lowercase) -> standard column; the rest go to other_specs
PROP_COLUMNS = {
    "condition": "condition",
    "form factor": "form_factor", "form-factor": "form_factor",
    "model": "model_number", "model number": "model_number",
    "interface": "interface",
    "series": "series", "product line": "series",
}
SKIP_PROPS = {"manufacturer", "brand"}


def clean(text):
    return re.sub(r"\s+", " ", html.unescape((text or "").replace("\xa0", " "))).strip()


def to_float(v):
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


def is_product(url):
    m = PRODUCT_URL_RE.match(url)
    if not m:
        return False
    rest = m.group(2).strip("/")
    if not rest:                                   # the bare /catalog/ or /config/ root
        return False
    if m.group(1) == "catalog" and "/" not in rest:  # a /catalog/<category>/ listing page
        return False
    return True


def product_urls():
    urls = set()
    for sm in SITEMAPS:
        r = http.session().get(sm, timeout=120)
        r.raise_for_status()
        urls.update(u for u in LOC_RE.findall(r.text) if is_product(u))
    return sorted(urls)


def iter_ld(html):
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


def product_node(nodes):
    """The Product node that carries an Offer (some pages list a trimmed Product too)."""
    best = None
    for n in nodes:
        t = n.get("@type")
        if t == "Product" or (isinstance(t, list) and "Product" in t):
            if n.get("offers"):
                return n
            best = best or n
    return best


def available_from(availability):
    leaf = str(availability or "").rsplit("/", 1)[-1].lower()
    return leaf in ("instock", "limitedavailability", "onlineonly", "instoreonly")


def specs_from(product):
    specs, other = {}, []
    props = product.get("additionalProperty") or []
    for p in props if isinstance(props, list) else [props]:
        if not isinstance(p, dict):
            continue
        name, value = clean(p.get("name")), clean(p.get("value"))
        if not name or not value:
            continue
        col = PROP_COLUMNS.get(name.lower())
        if col:
            specs.setdefault(col, value)
        elif name.lower() not in SKIP_PROPS:
            other.append(f"{name}: {value}")
    return specs, "; ".join(other)


def parse(url, html, scraped_at):
    nodes = list(iter_ld(html))
    product = product_node(nodes)
    if not product:
        return None
    offers = product.get("offers")
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    offers = offers or {}
    specs, other = specs_from(product)
    brand = product.get("brand")
    brand = clean(brand.get("name")) if isinstance(brand, dict) else clean(brand)
    item_condition = str(offers.get("itemCondition") or "").rsplit("/", 1)[-1].replace("Condition", "")
    path = url.replace(BASE + "/", "").strip("/")   # stable, unique product_id
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": path,
        "sku": clean(product.get("sku")),
        "model_number": specs.get("model_number", ""),
        "title": clean(product.get("name")),
        "brand": brand,
        "category": clean(product.get("category")),
        "condition": specs.get("condition") or item_condition,
        "series": specs.get("series", ""),
        "interface": specs.get("interface", ""),
        "form_factor": specs.get("form_factor", ""),
        "other_specs": other,
        "price": to_float(offers.get("price")),
        "available": available_from(offers.get("availability")),
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
    print(f"sitemaps list {len(urls)} products", flush=True)
    todo = urls[:limit] if limit else urls
    rows, seen = [], set()
    pid_idx = schema.PRODUCT_COLUMNS.index("product_id")
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, page in enumerate(pool.map(fetch_page, todo), 1):
            row = parse(page[0], page[1], scraped_at) if page else None
            if row and row[pid_idx] not in seen:
                seen.add(row[pid_idx])
                rows.append(row)
            if i % 100 == 0 or i == len(todo):
                print(f"{i}/{len(todo)} pages, {len(rows)} products", flush=True)
    return rows, len(urls)


if __name__ == "__main__":
    runner.run(SITE, scrape)
