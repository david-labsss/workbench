"""Scrape sabrepc.com (Angular storefront on NetSuite, ~33.7k products): every run's full snapshot
appended to Postgres.

The store's search and category pages run on Searchspring (site id m2xvgv, set in the store's own
chunk-*.js as searchspring:{siteId:"m2xvgv"}). Searchspring's public search API answers without
any key, so the data comes straight from it (https://m2xvgv.a.searchspring.io/api/search/
search.json; that host's robots.txt only blocks one bot, "wotbox") - the store itself is not
touched. An empty search lists the whole catalogue (33,748 products on 2026-10-20 = the product
sitemap's count).

The API gives at most 100 products per page, and page 101+ answers HTTP 400, so one query reaches
10,000 products. So the catalogue is split by manufacturer: one request reads the manufacturer
facet (305 makers summing to the total; the biggest, Supermicro, has 3,495), the makers are packed
into buckets of <= 8,000 products (repeating filter.manufacturer = OR, the facet is
"multiple-union") with a query string under 4 KB, and each bucket is paged at 100 per page sorted
by name (sort.sabrepc_description, one of the store's own sort options; uid / sku sorts are
ignored by the API), ~345 requests, 3 at a time. Every page must report the same total, and a
bucket must yield exactly its total of unique products (by uid); otherwise the bucket is read
again (the index can change mid-run), and after 3 tries the run raises, so a bad fetch never
looks like the end of a list. A request that stalls or is cut off part-way is retried 3x
(requests does not retry those); 429 / 5xx retries come from common/http.py.

Fields (the store's own product model in its JS maps them the same way):
  uid / sku (NetSuite id)            -> product_id, sku
  mpn                                -> part_number
  name, manufacturer (brand fallback) -> title, brand
  sabrepc_breadcrumb ("Products&gt;Storage Devices&gt;Hard Drives") -> category = the deepest level
  condition                          -> condition (New, Used, Refurbished, New Open Box, ...)
  sabrepc_price                      -> price. The plain "price" field is 0 for most products
                                        that are out of stock, so it is only a fallback.
  sabrepc_inventory                  -> stock_qty (a real count; 0 = none)
  url (/<part>-<brand>-S<id>)        -> url (absolute)
price is None when don_t_show_sabrepc_price = "Yes" (the page hides the price: "Contact sales for
pricing"; ~950 products, sabrepc_price is then missing) and when sabrepc_price is 0 (the page
shows the same "Contact sales for pricing"). show_price_in_cart = "Yes" would mean "See price in
cart" (0 products on 2026-10-20); that price is still saved and flagged in other_specs.

available follows the store's badge and buy button (its JS: getAvailability / getAction):
  stock > 0                                  "In Stock"       -> available
  special_order = "Yes" with a shown price   "Special Order"  -> available: the page has Add to
      Cart and "Ships in <lead_time>" (ordered from the supplier; ~1,000 products, most with
      stock 0, so stock_qty stays the real 0)
  anything else                              "Out of Stock" / "Contact sales" -> not available
other_specs keeps the flags: Special Order, Ships in <lead time>, Pre-order date, NCNR
(non-cancelable non-returnable), Contact sales for pricing, Price shown in cart.

Drives and memory: the result records carry form_factor, memory_size, product_line / series, but
not the capacity / interface / RPM facets, so for hard drives and SSDs those are read from the
title, which the store builds the same way for every drive ("... Hard Drive - 1 TB - 64 MB - 3.5 -
SATA - 7200", "... SSD - 240 GB - Internal - 2.5 - SATA"). price_per_tb for drives with a price.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import html
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from urllib.parse import urlencode

import requests

from common import http, runner, schema

SITE = "sabrepc"
BASE = "https://www.sabrepc.com"
SITE_ID = "m2xvgv"
API = f"https://{SITE_ID}.a.searchspring.io/api/search/search.json"
PER_PAGE = 100            # the API's largest page
MAX_PAGE = 100            # page 101+ answers HTTP 400 -> at most 10,000 products per query
BUCKET_ROWS = 8000        # products per manufacturer bucket (room to grow under 10,000)
MAX_QUERY = 4000          # characters of filter.manufacturer=... per bucket
SORT = ("sort.sabrepc_description", "asc")   # name A-Z: a stable order to page through
NO_FACETS = ("includedFacets", "none")       # product pages don't need the ~180 KB of facets
WORKERS = 3               # requests in flight
DELAY = 0.2               # pause after each request, per worker
TRIES = 3                 # attempts for a stalled / cut-off download
RETRY_PAUSE = 20
BUCKET_TRIES = 3          # reads of a bucket whose pages don't add up

DRIVE_CATEGORIES = {"Hard Drives", "Solid State Drives"}
CAPACITY_RE = re.compile(r"^(\d+(?:\.\d+)?)\s?(TB|GB)$")
CACHE_RE = re.compile(r"^(\d+(?:\.\d+)?)\s?(MB|GB)$")
RPM_RE = re.compile(r"^(\d{4,5})(?:\s?RPM)?$", re.I)
INTERFACE_RE = re.compile(r"^(?:SATA|SAS|NL-SAS|Near Line SAS|NVMe|PCI Express|PCIe|U\.2|U\.3|IDE|"
                          r"SCSI|Ultra320|Fibre Channel|USB|Thunderbolt|M\.2 NVMe)\b", re.I)


# --- fetching ----------------------------------------------------------------
def get(params):
    """One API call (JSON). http.session() retries 429 / 5xx; this retries a download that
    stalls or is cut off part-way (requests does not). The last failure raises."""
    query = [("siteId", SITE_ID), ("resultsFormat", "native")] + params
    for attempt in range(1, TRIES + 1):
        try:
            r = http.session().get(API, params=query, timeout=90)
            r.raise_for_status()
            data = r.json()
            time.sleep(DELAY)
            return data
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError, ValueError) as e:
            if attempt == TRIES:
                raise
            print(f"WARN {e.__class__.__name__} ({str(e)[:120]}), retrying in {RETRY_PAUSE}s",
                  flush=True)
            time.sleep(RETRY_PAUSE)


def manufacturers():
    """(products the store lists, {manufacturer: product count}) from one facet request."""
    data = get([("resultsPerPage", 0), ("page", 1), ("includedFacets", "manufacturer")])
    listed = int(data["pagination"]["totalResults"])
    facet = next((f for f in data.get("facets") or [] if f.get("field") == "manufacturer"), None)
    if not facet or not facet.get("values"):
        raise RuntimeError("Searchspring returned no manufacturer facet")
    if facet.get("multiple") != "multiple-union":
        # repeated filter.manufacturer would no longer mean OR; buckets would come back short
        raise RuntimeError(f"manufacturer facet is {facet.get('multiple')!r}, not multiple-union")
    counts = {v["value"]: int(v["count"]) for v in facet["values"]}
    return listed, counts


def make_buckets(counts):
    """Pack manufacturers (biggest first) into buckets of <= BUCKET_ROWS products and a query
    string <= MAX_QUERY characters. [(names, planned products)]."""
    buckets = []
    for name, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        if n > MAX_PAGE * PER_PAGE:
            raise RuntimeError(f"{name} has {n} products, more than one query can page through")
        chars = len(urlencode([("filter.manufacturer", name)])) + 1
        for b in buckets:
            if b["rows"] + n <= BUCKET_ROWS and b["chars"] + chars <= MAX_QUERY:
                break
        else:
            b = {"names": [], "rows": 0, "chars": 0}
            buckets.append(b)
        b["names"].append(name)
        b["rows"] += n
        b["chars"] += chars
    return [(b["names"], b["rows"]) for b in buckets]


def page_params(names, page):
    return ([("resultsPerPage", PER_PAGE), ("page", page), SORT, NO_FACETS]
            + [("filter.manufacturer", n) for n in names])


def read_pages(pool, names, first):
    """Every result of a query whose page 1 is `first`; (results, totals the pages reported)."""
    info = first["pagination"]
    total = int(info["totalResults"])
    if int(info.get("perPage") or 0) != PER_PAGE and total:
        raise RuntimeError(f"API pages hold {info.get('perPage')} products, not {PER_PAGE}")
    pages = math.ceil(total / PER_PAGE)
    if pages > MAX_PAGE:
        raise RuntimeError(f"{total} products in one query; more than {MAX_PAGE} pages")
    results, totals = list(first.get("results") or []), {total}
    for data in pool.map(lambda p: get(page_params(names, p)), range(2, pages + 1)):
        results.extend(data.get("results") or [])
        totals.add(int(data["pagination"]["totalResults"]))
    return results, totals


def fetch_bucket(pool, names):
    """{uid: result} for one bucket of manufacturers. Re-reads the bucket when its pages don't
    add up to exactly its total; raises after BUCKET_TRIES."""
    for attempt in range(1, BUCKET_TRIES + 1):
        first = get(page_params(names, 1))
        total = int(first["pagination"]["totalResults"])
        results, totals = read_pages(pool, names, first)
        by_uid = {str(r["uid"]): r for r in results}
        if totals == {total} and len(results) == total and len(by_uid) == total:
            return by_uid
        problem = (f"{len(results)} results, {len(by_uid)} unique, pages reported totals "
                   f"{sorted(totals)}")
        if attempt == BUCKET_TRIES:
            raise RuntimeError(f"bucket {names[:3]}...: {problem}; not saving a partial snapshot")
        print(f"WARN bucket {names[:3]}... ({total} products): {problem}; reading it again",
              flush=True)
        time.sleep(RETRY_PAUSE)


# --- mapping -----------------------------------------------------------------
def text(v):
    if isinstance(v, list):
        v = " | ".join(str(x) for x in v if x not in (None, ""))
    return re.sub(r"\s+", " ", html.unescape(str(v if v is not None else ""))).strip()


def yes(v):
    return text(v).lower() in ("yes", "true", "1")


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def stock_of(v):
    try:
        n = int(float(str(v).strip()))
    except (TypeError, ValueError):
        return None
    return n if n >= 0 else None


def category_of(r):
    crumb = r.get("sabrepc_breadcrumb") or ""
    crumb = crumb[0] if isinstance(crumb, list) and crumb else crumb
    levels = [text(x) for x in str(crumb).split("&gt;")]
    levels = [x for x in levels if x and x != "Products"]
    return levels[-1] if levels else ""


def drive_specs(title):
    """capacity, cache, interface, rpm from a drive title's ' - ' parts."""
    specs = {"capacity": "", "cache": "", "interface": "", "spindle_speed": "", "tb": None}
    for part in [p.strip() for p in title.split(" - ")[1:]]:
        cap = CAPACITY_RE.match(part)
        if cap and not specs["capacity"]:
            specs["capacity"] = f"{cap.group(1)}{cap.group(2).upper()}"
            size = float(cap.group(1))
            specs["tb"] = size if cap.group(2).upper() == "TB" else size / 1000
            continue
        cache = CACHE_RE.match(part)
        if cache and specs["capacity"] and not specs["cache"]:
            specs["cache"] = f"{cache.group(1)}{cache.group(2).upper()}"
            continue
        if not specs["interface"] and INTERFACE_RE.match(part):
            specs["interface"] = part
            continue
        rpm = RPM_RE.match(part)
        if rpm and not specs["spindle_speed"] and 3000 <= int(rpm.group(1)) <= 20000:
            specs["spindle_speed"] = f"{rpm.group(1)} RPM"
    return specs


def preorder_of(r):
    d = text(r.get("pre_order_date"))[:10]
    try:
        return d if date.fromisoformat(d) >= date.today() else ""
    except ValueError:
        return ""


def build_row(r, scraped_at):
    uid = str(r["uid"])
    title = text(r.get("name")) or text(r.get("sabrepc_description"))
    category = category_of(r)
    shown = not yes(r.get("don_t_show_sabrepc_price"))
    price = to_float(r.get("sabrepc_price"))
    if price is None:                      # only missing when hidden, but just in case
        price = to_float(r.get("price"))
    price = price if shown and price and price > 0 else None
    qty = stock_of(r.get("sabrepc_inventory"))
    special = yes(r.get("special_order"))
    available = bool(qty and qty > 0) or (special and price is not None)

    flags = []
    if special:
        flags.append("Special Order")
    if text(r.get("lead_time")) and ((qty or 0) > 0 or special):
        lead = text(r.get("lead_time"))
        flags.append(lead if lead == "Call for availability" else f"Ships in {lead}")
    if preorder_of(r):
        flags.append(f"Pre-order, ships {preorder_of(r)}")
    if yes(r.get("ncnr")):
        flags.append("NCNR")
    if price is None:
        flags.append("Contact sales for pricing")
    elif yes(r.get("show_price_in_cart")):
        flags.append("Price shown in cart")

    specs = drive_specs(title) if category in DRIVE_CATEGORIES else {}
    capacity = specs.get("capacity", "")
    if not capacity and text(r.get("memory_size")):
        capacity = text(r.get("memory_size")).replace(" ", "")
    tb = specs.get("tb")
    url = text(r.get("url")) or "/" + text(r.get("sabrepc_url_component"))
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": uid,
        "sku": text(r.get("sku")) or uid,
        "part_number": text(r.get("mpn")),
        "title": title,
        "brand": text(r.get("manufacturer")) or text(r.get("brand")),
        "category": category,
        "condition": text(r.get("condition")),
        "series": text(r.get("product_line")) or text(r.get("product_series")),
        "capacity": capacity,
        "interface": specs.get("interface", ""),
        "form_factor": text(r.get("form_factor")),
        "spindle_speed": specs.get("spindle_speed", ""),
        "cache": specs.get("cache", ""),
        "other_specs": "; ".join(flags),
        "price": price,
        "price_per_tb": round(price / tb, 2) if price and tb else None,
        "available": available,
        "stock_qty": qty,
        "url": url if url.startswith("http") else BASE + url,
    })


def scrape(limit, scraped_at):
    listed, counts = manufacturers()
    if not listed:
        raise RuntimeError("Searchspring lists no products")
    by_uid = {}
    with ThreadPoolExecutor(WORKERS) as pool:
        if limit and limit <= MAX_PAGE * PER_PAGE:
            # test run: the first pages of the whole catalogue, no buckets
            first = get(page_params([], 1))
            pages = math.ceil(min(limit, listed) / PER_PAGE)
            results = list(first.get("results") or [])
            for data in pool.map(lambda p: get(page_params([], p)), range(2, pages + 1)):
                results.extend(data.get("results") or [])
            by_uid = {str(r["uid"]): r for r in results}
        else:
            planned = sum(counts.values())
            buckets = make_buckets(counts)
            print(f"{listed} products listed; {len(counts)} manufacturers ({planned} products) "
                  f"in {len(buckets)} buckets", flush=True)
            if planned < listed:
                print(f"WARN {listed - planned} products have no manufacturer and can't be "
                      "reached by the buckets", flush=True)
            for i, (names, rows) in enumerate(buckets, 1):
                started = time.time()
                found = fetch_bucket(pool, names)
                by_uid.update(found)
                print(f"bucket {i}/{len(buckets)}: {len(names)} manufacturers, {len(found)} "
                      f"products (planned {rows}) in {time.time() - started:.0f}s; "
                      f"{len(by_uid)} so far", flush=True)
    items = list(by_uid.values())[:limit] if limit else list(by_uid.values())
    return [build_row(r, scraped_at) for r in items], listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
