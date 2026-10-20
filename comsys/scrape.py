"""Scrape comsys.co.nz (Comsys NZ, Auckland IT hardware retailer): every run's full snapshot
appended to Postgres.

The storefront is a server-rendered Next.js app. There is no public catalogue API (robots.txt
disallows /api/), but every product page ships a clean schema.org Product JSON-LD block, and the XML
sitemap index lists every product page, so the fastest complete public path is the product sitemaps
(/sitemap-index.xml -> product-sitemap-1..N, ~157k product URLs, links of the form /product/<slug>)
+ each page's JSON-LD -- no key, no browser.

Per product the JSON-LD gives: name, sku (Comsys's own code = product_id), mpn (the manufacturer
part number), gtin13, brand, category (a string, e.g. "Toner & Ink") plus a BreadcrumbList, and an
Offer with priceCurrency ("NZD"), price (incl. GST), itemCondition and availability. Prices are the
New Zealand dollar amounts the site shows; the price number is stored as-is.

The catalogue is large, so product pages are fetched a few at a time (threaded) and de-duplicated by
sku; a page that 404s or carries no Product JSON-LD is skipped.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import html as html_mod
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor

from common import http, runner, schema

SITE = "comsys"
BASE = "https://comsys.co.nz"
SITEMAP_INDEX = f"{BASE}/sitemap-index.xml"
# The whole catalogue (~157k product pages) is scraped every run, so wall time is what decides
# whether a single GitHub Actions job (<6h) finishes. comsys can't be cheaply scoped down to just
# the server/storage/networking lines: the only categorised, robots-allowed surface is the
# /category/<slug> listing pages, and their pagination (?page=N) is Disallowed by robots.txt
# (as are /api/ and /search), so page 1 of a category gives only its first 24 product URLs plus a
# total count -- not an enumerable scoped URL set. There is no public search/catalogue API and the
# product sitemaps aren't grouped by category, so the full product sitemap stays the only complete,
# polite source, and the in-scope subset (servers/storage/networking) is tens of thousands anyway.
# That leaves fetch concurrency as the one safe lever in this file: raise it (env-overridable) to
# bring the full crawl inside the job budget. If a run still overruns, chunk the product sitemaps
# across runs -- that needs runner/DB support for per-chunk change detection (common/runner.py,
# common/changes.py), so it is deliberately not done here.
WORKERS = int(os.getenv("WORKERS") or 12)
CURRENCY = "NZD"

LOC_RE = re.compile(r"<loc>\s*([^<]+?)\s*</loc>", re.I)
LD_RE = re.compile(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', re.S | re.I)
PRODUCT_RE = re.compile(r"^https?://comsys\.co\.nz/product/.+", re.I)

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
    """Every /product/<slug> URL from the product sitemaps (via the sitemap index). Under a test
    limit, stop fetching sub-sitemaps once enough URLs are collected."""
    r = http.session().get(SITEMAP_INDEX, timeout=120)
    r.raise_for_status()
    subs = [u for u in LOC_RE.findall(r.text) if "product-sitemap" in u]
    urls, seen = [], set()
    for sub in subs:
        rr = http.session().get(sub, timeout=120)
        rr.raise_for_status()
        for loc in LOC_RE.findall(rr.text):
            if PRODUCT_RE.match(loc) and loc not in seen:
                seen.add(loc)
                urls.append(loc)
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
    # the last entry is the product itself; the one before it is the leaf category
    return names[-2] if len(names) >= 2 else ""


def brand_of(node):
    b = node.get("brand")
    if isinstance(b, dict):
        return clean(b.get("name"))
    if isinstance(b, str):
        return clean(b)
    return ""


def build_row(page_url, html, scraped_at):
    nodes = list(iter_ld(html))
    product = node_of(nodes, "Product")
    if not product:
        return None
    sku = clean(product.get("sku")) or clean(product.get("mpn"))
    if not sku:
        return None
    offers = product.get("offers")
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    offers = offers or {}
    gtin = clean(product.get("gtin13") or product.get("gtin") or "")
    category = clean(product.get("category")) or breadcrumb_leaf(node_of(nodes, "BreadcrumbList"))
    condition_raw = str(offers.get("itemCondition") or "").rsplit("/", 1)[-1].lower()
    url = clean(offers.get("url")) or clean(product.get("url")) or page_url
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
        "model_number": clean(product.get("mpn")),
        "title": clean(product.get("name")),
        "brand": brand_of(product),
        "category": category,
        "condition": CONDITIONS.get(condition_raw, ""),
        "other_specs": f"GTIN: {gtin}" if gtin else "",
        "price": to_float(offers.get("price")),
        "available": available_from(offers.get("availability")),
        "image_url": image_url,
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
                    no_product += 1     # a real page with no Product JSON-LD (service / care pack)
            elif kind == "gone":
                gone += 1
            else:
                fail += 1
            if i % 500 == 0 or i == len(todo):
                print(f"{i}/{len(todo)} fetched: {len(rows)} products, "
                      f"{gone} gone(404), {fail} failed", flush=True)
    print(f"fetched ok={ok} gone={gone} failed={fail}; {len(rows)} product rows, "
          f"{no_product} pages without a Product", flush=True)
    # listed is None: the sitemap URL count is not the product count (a stable share of /product/
    # pages are services / care packs with no Product JSON-LD, plus some delisted 404s), so the
    # runner's count-based completeness gate can't apply. Instead, guard against the real failure
    # mode - getting blocked and saving a tiny snapshot as if it were the full catalogue - by
    # aborting when too many pages hard-fail or the product yield collapses.
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
