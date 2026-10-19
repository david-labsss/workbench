"""Scrape itcreations.com (IT Creations, Laravel store, ~19.4k parts): every run's full snapshot
appended to Postgres.

The store's /sitemap.xml is a single urlset listing every page, including ~19,371 /product/{id}
parts and ~13,665 /server/p{id} built-to-order servers. This scraper reads the /product/{id}
pages only (the priced parts); the servers are skipped for now (SERVERS_LISTED is reported so the
count is on record). There is an on-site search JSON endpoint, but it is a keyword search capped
at ~300 hits per query, so it can't enumerate the catalogue - the sitemap is the complete list.

Each product page carries everything in three places:
  - JSON-LD (@type Product): name, mpn (part number), brand, offers.price, itemCondition. Its
    availability is unreliable, so it is ignored.
  - window.itcData = {maxQty: N, ...}: maxQty is the stock quantity.
  - window.__itc_products = {item_id, item_brand, ...}: a fallback for part number and brand.
A price of 0 (or no offer) means quote-only: saved with no price. Stock comes from maxQty, so
available = maxQty > 0.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor

from common import http, runner, schema

SITE = "itcreations"
BASE = "https://www.itcreations.com"
SITEMAP = f"{BASE}/sitemap.xml"
WORKERS = 6
MAX_FAILED = 0.01        # share of product pages that may fail before the run stops

PRODUCT_RE = re.compile(r"https://www\.itcreations\.com/product/(\d+)")
SERVER_RE = re.compile(r"https://www\.itcreations\.com/server/p\d+")
LD_RE = re.compile(r'<script type="application/ld\+json">(.*?)</script>', re.S)
MAXQTY_RE = re.compile(r"itcData\s*=\s*\{[^}]*maxQty:\s*(\d+)")
ITC_ITEMID_RE = re.compile(r"__itc_products\s*=\s*\{.*?item_id:\s*\"([^\"]*)\"", re.S)
ITC_BRAND_RE = re.compile(r"__itc_products\s*=\s*\{.*?item_brand:\s*\"([^\"]*)\"", re.S)
CONDITIONS = {"NewCondition": "New", "UsedCondition": "Used",
              "RefurbishedCondition": "Refurbished", "DamagedCondition": "Damaged"}


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def condition(url):
    """schema.org itemCondition URL -> New / Used / Refurbished."""
    for key, label in CONDITIONS.items():
        if url and key in url:
            return label
    return ""


def product_ids():
    """The /product/{id} ids from the sitemap, and the /server/ count (reported, not scraped)."""
    r = http.session().get(SITEMAP, timeout=120)
    r.raise_for_status()
    ids = list(dict.fromkeys(PRODUCT_RE.findall(r.text)))   # de-duplicate, keep order
    servers = len(set(SERVER_RE.findall(r.text)))
    print(f"sitemap lists {len(ids)} /product/ parts and {servers} /server/ servers "
          f"(servers skipped)", flush=True)
    return ids, servers


def product_json(html):
    """The @type Product object from the page's JSON-LD blocks, or None."""
    for block in LD_RE.findall(html):
        try:
            data = json.loads(block)
        except ValueError:
            continue
        graph = data.get("@graph", [data]) if isinstance(data, dict) else []
        for node in graph:
            if isinstance(node, dict) and node.get("@type") == "Product":
                return node
    return None


def parse(pid, html):
    product = product_json(html) or {}
    offers = product.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    brand = product.get("brand") or {}
    brand_name = brand.get("name") if isinstance(brand, dict) else brand

    mpn = clean(product.get("mpn"))
    if not mpn:
        m = ITC_ITEMID_RE.search(html)
        mpn = clean(m.group(1)) if m else ""
    if not brand_name:
        m = ITC_BRAND_RE.search(html)
        brand_name = m.group(1) if m else ""

    m = MAXQTY_RE.search(html)
    max_qty = int(m.group(1)) if m else None

    try:
        price = float(offers.get("price"))
    except (TypeError, ValueError):
        price = None
    if not price:                       # 0 or missing = quote-only
        price = None

    return schema.product_row({
        "scraped_at": SCRAPED_AT,
        "website": SITE,
        "product_id": pid,
        "sku": mpn,
        "part_number": mpn,
        "title": clean(product.get("name")),
        "brand": clean(brand_name),
        "condition": condition(offers.get("itemCondition")),
        "price": price,
        "available": None if max_qty is None else max_qty > 0,
        "stock_qty": max_qty,
        "url": f"{BASE}/product/{pid}",
    })


def fetch(pid):
    try:
        r = http.session().get(f"{BASE}/product/{pid}", timeout=60)
        if r.status_code == 404:
            return pid, None            # gone from the catalogue since the sitemap was built
        r.raise_for_status()
        return pid, parse(pid, r.text)
    except Exception as e:              # noqa: BLE001 - one bad page must not stop the run
        print(f"WARN product {pid}: {e}", flush=True)
        return pid, "error"


SCRAPED_AT = None


def scrape(limit, scraped_at):
    global SCRAPED_AT
    SCRAPED_AT = scraped_at
    ids, servers = product_ids()
    ids = ids[:limit] if limit else ids

    rows, failed, gone = [], [], 0
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, (pid, row) in enumerate(pool.map(fetch, ids), 1):
            if row == "error":
                failed.append(pid)
            elif row is None:
                gone += 1                 # 404: removed from the catalogue since the sitemap
            else:
                rows.append(row)
            if i % 2000 == 0 or i == len(ids):
                print(f"{i}/{len(ids)} pages, {len(rows)} products, {gone} gone, "
                      f"{len(failed)} failed", flush=True)

    # One more try for pages that failed (e.g. a dropped connection)
    if failed:
        retry = [fetch(pid) for pid in failed]
        failed = [pid for pid, row in retry if row == "error"]
        gone += sum(1 for _, row in retry if row is None)
        rows += [row for _, row in retry if row not in (None, "error")]
    if len(failed) > MAX_FAILED * max(len(ids), 1):
        sys.exit(f"{len(failed)} of {len(ids)} product pages failed; not saving a partial snapshot")

    # listed = the parts the sitemap promised that still exist (404s are genuine removals, not
    # fetch failures, so they must not trip the runner's completeness gate). Servers are skipped.
    return rows, (len(ids) - gone if not limit else None)


if __name__ == "__main__":
    runner.run(SITE, scrape)
