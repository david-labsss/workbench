"""Scrape edgeoptic.com (EDGE Optical Solutions, UA/EU optical transceivers): every run's full
snapshot appended to Postgres.

edgeoptic.com is a Next.js storefront on Vercel (no Cloudflare), and every product page ships a
clean schema.org JSON-LD Product with an Offer. The fastest complete public path is the product
sitemaps (sitemap index -> /sitemap-products/0..N, ~5.9k products) + each page's JSON-LD -- no API
key, no browser.

Per product the JSON-LD gives: name, sku (EDGE's own code = product_id), mpn (the OEM part it is
compatible with, e.g. "SFP-10G-SR"), brand ("EDGE Optical Solutions"), a product grouping in
"category" (e.g. "Cisco", "100G SFP-DD"), additionalProperty specs (form factor, connector, data
rate, wavelength, reach, ...) and an Offer with priceCurrency ("EUR"), price, itemCondition and
availability. Tunable CWDM/DWDM parts are published as a ProductGroup whose hasVariant lists one
Product per wavelength channel, each with its own sku and Offer; those are emitted one row per
variant (product_id = variant sku, parent_id = the group's productGroupID).

Prices are in euros (EUR); the price number is stored as-is.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import html
import json
import re
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

from common import http, runner, schema

SITE = "edgeoptic"
BASE = "https://edgeoptic.com"
SITEMAP = f"{BASE}/sitemap.xml"
WORKERS = 6
CURRENCY = "EUR"

LOC_RE = re.compile(r"<loc>\s*([^<]+?)\s*</loc>", re.I)
LD_RE = re.compile(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', re.S | re.I)

# additionalProperty name (lowercase) -> standard column
SPEC_COLUMNS = {
    "form factor": "form_factor", "formfactor": "form_factor",
    "connector": "interface", "interface": "interface", "media": "interface",
    "data rate": "other_specs", "wavelength": "other_specs", "reach": "other_specs",
}


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
    r = http.session().get(SITEMAP, timeout=60)
    r.raise_for_status()
    subs = [u for u in LOC_RE.findall(r.text) if "/sitemap-products/" in u]
    urls = set()
    for sub in subs:
        rr = http.session().get(sub, timeout=120)
        rr.raise_for_status()
        urls.update(LOC_RE.findall(rr.text))
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


def node_of(nodes, type_name):
    for n in nodes:
        t = n.get("@type")
        if t == type_name or (isinstance(t, list) and type_name in t):
            return n
    return None


def available_from(availability):
    leaf = str(availability or "").rsplit("/", 1)[-1].lower()
    return leaf in ("instock", "limitedavailability", "onlineonly", "instoreonly")


def breadcrumb_leaf(breadcrumb):
    if not breadcrumb:
        return ""
    names = []
    for it in breadcrumb.get("itemListElement") or []:
        name = it.get("name") or (it.get("item") or {}).get("name")
        if name:
            names.append(clean(name))
    names = [n for n in names if n.lower() not in ("home", "products")]
    # the last entry is the product itself; the one before it is the leaf category
    return names[-2] if len(names) >= 2 else (names[0] if names else "")


def specs_from(product):
    specs, other = {}, []
    props = product.get("additionalProperty") or []
    for p in props if isinstance(props, list) else [props]:
        if not isinstance(p, dict):
            continue
        name, value = clean(p.get("name")), clean(p.get("value"))
        if not name or not value:
            continue
        col = SPEC_COLUMNS.get(name.lower())
        if col and col != "other_specs":
            specs.setdefault(col, value)
        else:
            other.append(f"{name}: {value}")
    return specs, "; ".join(other)


def brand_of(node, fallback=""):
    b = node.get("brand")
    if isinstance(b, dict):
        return clean(b.get("name")) or fallback
    if isinstance(b, str):
        return clean(b) or fallback
    return fallback


def build_row(node, page_url, scraped_at, parent_id="", leaf="", fallback=None):
    """One product row from a Product node (standalone, or a variant of a ProductGroup).
    `fallback` carries group-level brand/category/specs that a variant may omit."""
    fallback = fallback or {}
    sku = clean(node.get("sku")) or clean(node.get("mpn"))
    if not sku:
        return None
    offers = node.get("offers")
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    offers = offers or {}
    specs, other = specs_from(node)
    url = clean(offers.get("url")) or clean(node.get("url")) or page_url
    condition = str(offers.get("itemCondition") or "").rsplit("/", 1)[-1].replace("Condition", "")
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": sku,
        "parent_id": parent_id,
        "variant_id": sku if parent_id else "",
        "sku": sku,
        "part_number": clean(node.get("mpn")),
        "title": clean(node.get("name")),
        "brand": brand_of(node, fallback.get("brand", "")),
        "category": clean(node.get("category")) or fallback.get("category") or leaf,
        "condition": condition,
        "interface": specs.get("interface") or fallback.get("interface", ""),
        "form_factor": specs.get("form_factor") or fallback.get("form_factor", ""),
        "other_specs": other or fallback.get("other", ""),
        "price": to_float(offers.get("price")),
        "available": available_from(offers.get("availability")),
        "image_url": abs_image(node.get("image")) or fallback.get("image"),
        "description": schema.plain_text(node.get("description")) or fallback.get("description"),
        "url": url,
    })


def parse_rows(page_url, html, scraped_at):
    """Zero or more rows from a product page: one Product, or a ProductGroup's variants."""
    nodes = list(iter_ld(html))
    leaf = breadcrumb_leaf(node_of(nodes, "BreadcrumbList"))
    group = node_of(nodes, "ProductGroup")
    if group and isinstance(group.get("hasVariant"), list) and group["hasVariant"]:
        gspecs, gother = specs_from(group)
        fallback = {"brand": brand_of(group), "category": clean(group.get("category")) or leaf,
                    "interface": gspecs.get("interface", ""),
                    "form_factor": gspecs.get("form_factor", ""), "other": gother,
                    "image": abs_image(group.get("image")),
                    "description": schema.plain_text(group.get("description"))}
        parent = clean(group.get("productGroupID")) or clean(group.get("sku")) or page_url
        rows = [build_row(v, page_url, scraped_at, parent, leaf, fallback)
                for v in group["hasVariant"] if isinstance(v, dict)]
        return [r for r in rows if r]
    product = node_of(nodes, "Product")
    row = build_row(product, page_url, scraped_at, leaf=leaf) if product else None
    return [row] if row else []


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
    pid_idx = schema.PRODUCT_COLUMNS.index("product_id")
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, page in enumerate(pool.map(fetch_page, todo), 1):
            for row in (parse_rows(page[0], page[1], scraped_at) if page else []):
                pid = row[pid_idx]
                if pid not in seen:      # variant skus are unique across the catalogue
                    seen.add(pid)
                    rows.append(row)
            if i % 200 == 0 or i == len(todo):
                print(f"{i}/{len(todo)} pages, {len(rows)} products", flush=True)
    # ProductGroup pages expand to many variant rows, so rows can exceed the URL count
    return rows, len(urls)


if __name__ == "__main__":
    runner.run(SITE, scrape)
