"""Scrape senetic.co.uk (Senetic, UK arm of the Senetic IT distributor): every run's full snapshot
appended to Postgres.

Senetic runs a custom PHP storefront with no public product API (robots.txt disallows /api/,
/graphql/ and /ws/), but every product page ships a clean schema.org Product JSON-LD block, and an
XML sitemap index lists every product page. The fastest complete public path is therefore the
sitemap index (/sitemap_files/co_uk-MainSiteMap.xml, which points at ~490 per-vendor sub-sitemaps)
-> the /product/<code> URLs in each -> each page's JSON-LD. No key, no browser.

Per product the JSON-LD gives: name, sku (the vendor part code = product_id, and the last URL
segment), brand, gtin13, and an Offer with price, priceCurrency ("GBP"), itemCondition and
availability. Prices are the pound amounts the site shows; the price number is stored as-is.

The catalogue is large and split across many vendor sub-sitemaps, so product pages are fetched a few
at a time (threaded) and de-duplicated by sku; a page that 404s or carries no Product JSON-LD is
skipped. Under a test limit, only enough sub-sitemaps are read to cover the limit.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import html as html_mod
import json
import re
from concurrent.futures import ThreadPoolExecutor

from common import http, runner, schema

SITE = "senetic"
BASE = "https://www.senetic.co.uk"
MAIN_SITEMAP = f"{BASE}/sitemap_files/co_uk-MainSiteMap.xml"
WORKERS = 3          # Senetic rate-limits (HTTP 429) under heavy concurrency; stay gentle

LOC_RE = re.compile(r"<loc>\s*([^<]+?)\s*</loc>", re.I)
LD_RE = re.compile(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', re.S | re.I)
PRODUCT_RE = re.compile(r"^https?://www\.senetic\.co\.uk/product/.+", re.I)

CONDITIONS = {"newcondition": "New", "usedcondition": "Used",
              "refurbishedcondition": "Refurbished", "damagedcondition": "Damaged"}


def clean(text):
    return re.sub(r"\s+", " ", html_mod.unescape((text or "").replace("\xa0", " "))).strip()


def to_float(v):
    try:
        return float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def product_urls(limit):
    """Every /product/<code> URL from the per-vendor sub-sitemaps (via the main sitemap index).
    Under a test limit, stop reading sub-sitemaps once enough URLs are collected."""
    r = http.session().get(MAIN_SITEMAP, timeout=120)
    r.raise_for_status()
    locs = LOC_RE.findall(r.text)
    subs = [u for u in locs if u.lower().endswith(".xml") and "blog" not in u.lower()]
    urls, seen = [], set()

    def add(loc):
        if PRODUCT_RE.match(loc) and loc not in seen:
            seen.add(loc)
            urls.append(loc)

    for loc in locs:               # any product URLs listed directly in the index
        add(loc)
    for sub in subs:
        try:
            rr = http.session().get(sub, timeout=120)
            rr.raise_for_status()
        except Exception as e:  # noqa: BLE001 - one bad sub-sitemap must not stop the run
            print(f"WARN sub-sitemap {sub}: {e}", flush=True)
            continue
        for loc in LOC_RE.findall(rr.text):
            add(loc)
        if limit and len(urls) >= limit:
            break
    return urls


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
    return leaf in ("instock", "limitedavailability", "onlineonly", "instoreonly", "presale",
                    "backorder")


def breadcrumb_leaf(breadcrumb):
    if not breadcrumb:
        return ""
    names = []
    for it in breadcrumb.get("itemListElement") or []:
        name = it.get("name") or (it.get("item") or {}).get("name")
        if name:
            names.append(clean(name))
    names = [n for n in names if n.lower() not in ("home", "products")]
    return names[-2] if len(names) >= 2 else ""


def brand_of(node):
    b = node.get("brand")
    if isinstance(b, dict):
        return clean(b.get("name"))
    if isinstance(b, str):
        return clean(b)
    return ""


def first(value):
    """JSON-LD fields are sometimes single values, sometimes lists; take the first non-empty."""
    if isinstance(value, list):
        return value[0] if value else ""
    return value


def build_row(page_url, html, scraped_at):
    nodes = list(iter_ld(html))
    product = node_of(nodes, "Product")
    if not product:
        return None
    sku = clean(first(product.get("sku")) or first(product.get("mpn")))
    if not sku:
        return None
    offers = product.get("offers")
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    offers = offers or {}
    gtin = clean(first(product.get("gtin13")) or first(product.get("gtin")) or "")
    category = clean(product.get("category")) or breadcrumb_leaf(node_of(nodes, "BreadcrumbList"))
    condition_raw = str(offers.get("itemCondition") or "").rsplit("/", 1)[-1].lower()
    url = clean(offers.get("url")) or clean(product.get("url")) or page_url
    image = first(product.get("image"))     # string, ImageObject, or list of either
    if isinstance(image, dict):
        image = image.get("contentUrl") or image.get("url")
    image = clean(image) or None if isinstance(image, str) else None
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": sku,
        "sku": sku,
        "part_number": sku,          # the vendor part code is the store's one identifier
        "model_number": sku,
        "title": clean(product.get("name")),
        "brand": brand_of(product),
        "category": category,
        "condition": CONDITIONS.get(condition_raw, ""),
        "other_specs": f"GTIN: {gtin}" if gtin else "",
        "price": to_float(offers.get("price")),
        "available": available_from(offers.get("availability")),
        "image_url": image,
        "description": schema.plain_text(product.get("description")),
        "url": url,
    })


def fetch(url):
    """('ok', url, html) for a 200, ('gone',) for a 404 (delisted sitemap entry), or ('fail', url)
    for a blocked / errored fetch after the shared session's own retries."""
    try:
        r = http.session().get(url, timeout=60)
    except Exception as e:  # noqa: BLE001 - a network error is a fetch failure, handled below
        print(f"WARN fetch failed {url}: {e}", flush=True)
        return ("fail", url)
    if r.status_code == 404:
        return ("gone",)
    if r.status_code != 200:
        print(f"WARN HTTP {r.status_code} {url}", flush=True)
        return ("fail", url)
    return ("ok", url, r.text)


def scrape(limit, scraped_at):
    urls = product_urls(limit)
    print(f"sitemap lists {len(urls)} product URLs", flush=True)
    todo = urls[:limit] if limit else urls
    rows, seen = [], set()
    pid_idx = schema.PRODUCT_COLUMNS.index("product_id")
    ok = gone = fail = no_product = 0
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, res in enumerate(pool.map(fetch, todo), 1):
            kind = res[0]
            if kind == "ok":
                ok += 1
                row = build_row(res[1], res[2], scraped_at)
                if row:
                    pid = row[pid_idx]
                    if pid and pid not in seen:
                        seen.add(pid)
                        rows.append(row)
                else:
                    no_product += 1     # a real page with no Product JSON-LD
            elif kind == "gone":
                gone += 1
            else:
                fail += 1
            if i % 500 == 0 or i == len(todo):
                print(f"{i}/{len(todo)} fetched: {len(rows)} products, "
                      f"{gone} gone(404), {fail} failed", flush=True)
    print(f"fetched ok={ok} gone={gone} failed={fail}; {len(rows)} product rows, "
          f"{no_product} pages without a Product", flush=True)
    # listed is None: the sitemap URL count is not a clean product count (some entries are delisted
    # 404s or pages without a Product JSON-LD), so the runner's count-based completeness gate can't
    # apply. Instead guard against the real failure mode - getting blocked and saving a tiny
    # snapshot as if it were the full catalogue - by aborting when too many pages hard-fail or the
    # product yield collapses. (Out-of-stock items legitimately carry no price; that is not a miss.)
    if not limit:
        if fail > max(20, int(0.01 * len(todo))):
            raise RuntimeError(f"{fail}/{len(todo)} product pages failed to fetch; "
                               "aborting rather than saving a partial snapshot")
        if ok and len(rows) < 0.5 * ok:
            raise RuntimeError(f"only {len(rows)} products from {ok} fetched pages "
                               "(<50%); aborting (likely blocked)")
    return rows, None


if __name__ == "__main__":
    runner.run(SITE, scrape)
