"""Scrape cdw.com (CDW, US IT reseller; new products), server / storage / component / networking
categories only (~9.2k products): every run's full snapshot appended to Postgres.

Data path: the public category listing pages, plain HTML:
  /category/<path>/?w=<code>&maxrecords=72&pcurrent=<n>          (b=<brand code> to filter)
robots.txt (User-agent: *) disallows /search/, the account / cart pages and a list of query
parameters (SortBy=, page=, pfm=, ...). None of the parameters used here is on that list, so the
"Sort by" parameter is left out: the default order ("Best Match") was checked to page stably
(2026-10-20: transceivers / ProLine, 686 of 686 unique over 10 pages; server memory 357 of 357).
Product URLs are saved without the tile link's "?pfm=srh" (also disallowed).
72 is the biggest page size the site accepts (100 falls back to 24). The sitemaps are behind a
bot check (HTTP 429) and the stock / price GraphQL on dc-origin.cdw.com is the site's private
backend, so neither is used.

Scope (codes from the site's own header menu hardwarel3Menu-min.json; listing totals on
2026-10-20 in brackets):
  Servers     SA1 Server Accessories (137), SA2 Server Memory (357), SB1 Blade Servers (1),
              SB2 Rack Servers (114)
  Storage     KA2 Flash Arrays (129), KA3 Hard Drive Arrays (95), KA4 Hybrid Arrays (18),
              KA5 Tape Drives (85), KB1 Internal Hard Drives (354), KD Network Attached Storage
              (294), KE Solid State Drives (680), KG Storage Mounts & Enclosures (296, incl.
              storage controllers), KF3 Tape Cartridges (64)
  Components  AA2 Computer Memory (218), AA3 CPUs (185), AA4 Graphic Cards (109), AA5
              Motherboards (55), AA7 System Cooling Fans (118), AA8 Power Supplies (96)
  Networking  RB1 Ethernet Adapters (395), RB2 Host Bus Adapters (51), RB4 Transceiver Modules
              (1,699), RJ1 Ethernet Switches (1,410), RJ2 Fiber Channel Switches (22), RJ3 Modular
              Switches (310), RJ4 PoE Injectors (154), RG3 Router Modules (20), RG4 Data Routers
              (45), RF1 Firewalls & VPN (824), RF2 Security Appliances (47), RD1 Console & Device
              Servers (194), RD2 Load Balancers (32), RK Wireless Access Points (579)
Left out: desktops / laptops, monitors, printers, cables, UPS (LA2, ~1k) and racks (LE3, ~800).

A listing only pages through its first ~1,000 results (page 14 at 72 a page jumps back to page 1),
so a category over CAP (936 = 13 pages) is split by its Brand facet (b=<code>): each product has
exactly one brand, and the facet counts add up to the category total. A brand still over CAP would
be split again by the Price facet (p=[a TO b}); "Request Pricing" products are in no price bucket,
so such a split may come up short, and the run then stops (see the coverage check) instead of
saving a partial snapshot. On 2026-10-20 the biggest brand in one category had 686 products.

Every page is checked: its "A - B of N" label must start at (page - 1) * 72 + 1, end at
min(page * 72, N) and show B - A + 1 tiles, or the page raises (after the download retries). A
page past the end because the listing shrank mid-run is accepted with a warning. Each category's
unique products must reach its page-1 total (up to CHURN_SLACK short, for products that disappear
mid-run). Products are deduped by CDW# (the first category listing one names its category).
2026-10-20: 33 first pages + 206 more (114 of them brand listings), 3 at a time: 9,187 of 9,187
products in ~3 min.

Tile fields:
  data-product-code (CDW#)    product_id and sku
  "MFG#: ..."                  part_number
  product link text            title; url = https://www.cdw.com/product/<slug>/<CDW#>
  data-brand-code              brand = the Brand facet's label for that code ("CIS" -> Cisco)
  .price-type-price            price, the "Advertised Price" a visitor sees ("$652.99");
                               "Request Pricing" tiles -> None (never 0)
  .price-msrp                  the struck-through list price -> compare_at_price when higher
  "Main Features" specs        capacity / interface / form_factor / spindle_speed (drives,
                               memory), the rest in other_specs
  tile image                   image_url (https://webobjects2.cdw.com/is/image/CDW/<CDW#>);
                               description stays empty (only on the product pages, one request
                               each, not fetched)
Stock is not on the tile: the "In Stock" line is filled in by the browser from the private
dc-origin backend, so available / stock_qty are left empty. Condition is blank (CDW sells new)
unless the name says refurbished / recertified / renewed / pre-owned / open box. Category = the
listing's own name. Capacity of memory comes from the name ("64 GB", kits "128 GB: 4 x 32 GB").

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import html
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlencode

import requests

from common import http, runner, schema

SITE = "cdw"
BASE = "https://www.cdw.com"
CATEGORIES = [   # (code, path, name)
    ("SA1", "servers-server-management/server-components/server-accessories", "Server Accessories"),
    ("SA2", "servers-server-management/server-components/server-memory", "Server Memory"),
    ("SB1", "servers-server-management/servers/blade-servers", "Blade Servers"),
    ("SB2", "servers-server-management/servers/rack-servers", "Rack Servers"),
    ("KA2", "storage-hard-drives/drive-arrays/flash-arrays", "Flash Arrays"),
    ("KA3", "storage-hard-drives/drive-arrays/hard-drive-arrays", "Hard Drive Arrays"),
    ("KA4", "storage-hard-drives/drive-arrays/hybrid-arrays", "Hybrid Arrays"),
    ("KA5", "storage-hard-drives/drive-arrays/tape-drives", "Tape Drives"),
    ("KB1", "storage-hard-drives/hard-drives/internal-hard-drives", "Internal Hard Drives"),
    ("KD", "storage-hard-drives/network-attached-storage", "Network Attached Storage"),
    ("KE", "storage-hard-drives/solid-state-drives", "Solid State Drives"),
    ("KG", "storage-hard-drives/storage-mounts-enclosures", "Storage Mounts & Enclosures"),
    ("KF3", "storage-hard-drives/storage-media/tape-cartridges", "Tape Cartridges"),
    ("AA2", "computer-accessories/computer-components/computer-memory", "Computer Memory"),
    ("AA3", "computer-accessories/computer-components/cpus", "CPUs"),
    ("AA4", "computer-accessories/computer-components/graphic-cards", "Graphic Cards"),
    ("AA5", "computer-accessories/computer-components/motherboards", "Motherboards"),
    ("AA7", "computer-accessories/computer-components/system-cooling-fans", "System Cooling Fans"),
    ("AA8", "computer-accessories/computer-components/power-supplies", "Power Supplies"),
    ("RB1", "networking/network-adapters/ethernet-adapters", "Ethernet Adapters"),
    ("RB2", "networking/network-adapters/host-bus-adapters", "Host Bus Adapters"),
    ("RB4", "networking/network-adapters/transceiver-modules", "Transceiver Modules"),
    ("RJ1", "networking/switches/ethernet-switches", "Ethernet Switches"),
    ("RJ2", "networking/switches/fiber-channel-switches", "Fiber Channel Switches"),
    ("RJ3", "networking/switches/modular-switches", "Modular Switches"),
    ("RJ4", "networking/switches/poe-injectors", "PoE Injectors"),
    ("RG3", "networking/routers/router-modules-accessories", "Router Modules & Accessories"),
    ("RG4", "networking/routers/data-routers", "Data Routers"),
    ("RF1", "networking/network-security/firewalls-vpn", "Firewalls & VPN"),
    ("RF2", "networking/network-security/security-appliances", "Security Appliances"),
    ("RD1", "networking/network-management/console-device-servers", "Console & Device Servers"),
    ("RD2", "networking/network-management/load-balancers", "Load Balancers"),
    ("RK", "networking/wireless-access-points", "Wireless Access Points"),
]
MEMORY_CODES = {"SA2", "AA2"}
PAGE_SIZE = 72                 # the biggest page size the site accepts
CAP = 936                      # 13 pages of 72: deeper pages jump back to page 1
WORKERS = 3                    # requests in flight
DELAY = 1.0                    # per worker, after each request
TRIES = 3                      # attempts per request for stalled / cut-off downloads
RETRY_PAUSE = 30
CHURN_SLACK = 10               # products a category may come up short when some vanish mid-run
RANGE_RE = re.compile(r'search-pagination-range[^>]*>\s*<strong>([\d,]+) - ([\d,]+)</strong>'
                      r'\s*of\s*([\d,]+)')
TILE_SPLIT = '<div class="search-result coupon-check"'
TILE_CODE_RE = re.compile(r'^\s*data-product-code="(\d+)"[^>]*?data-brand-code="([^"]*)"')
LINK_RE = re.compile(r'<a class="search-result-product-url"[^>]*?href="([^"?]+)[^"]*"[^>]*>'
                     r'([^<]*)</a>')
MFG_RE = re.compile(r'<span class="mfg-code">\s*MFG#:\s*([^<]*?)\s*</span>')
PRICE_RE = re.compile(r'class="price-type-price"[^>]*>\s*\$\s*([\d,]+(?:\.\d+)?)\s*<')
MSRP_RE = re.compile(r'class="price-msrp[^"]*">(?:<em></em>)?\s*\$\s*([\d,]+(?:\.\d+)?)')
PRICE_TYPE_RE = re.compile(r'class="price-type (price-type-[a-z-]+)"')
IMAGE_RE = re.compile(r'<img src="(https://webobjects\d*\.cdw\.com/is/image/CDW/\d+)')
SPEC_RE = re.compile(r'<div class="extended-specs-key">\s*([^<]*?):?\s*</div>\s*'
                     r'<div class="extended-specs-value">\s*([^<]*?)\s*</div>')
FACET_SPLIT = '<div class="filter-item '
FACET_RE = re.compile(r'data-facet-key="([^"]+)" data-facet-value="([^"]*)".*?'
                      r'filter-item-display-name-text">\s*([^<]*?)\s*</label>.*?'
                      r'filter-item-display-count[^>]*>\(([\d,]+)\)', re.S)
MEMORY_CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s?(TB|GB)\b")
CONDITIONS = [(re.compile(r"\brefurb", re.I), "Refurbished"),
              (re.compile(r"\brecertified\b", re.I), "Recertified"),
              (re.compile(r"\brenewed\b", re.I), "Renewed"),
              (re.compile(r"\bpre-?owned\b", re.I), "Used"),
              (re.compile(r"\bopen[\s-]?box\b", re.I), "Open Box")]
# Tile spec label -> column (first label found wins); every other spec goes to other_specs
SPEC_COLUMNS = {
    "capacity": ["Hard Drive Capacity"],
    "interface": ["Storage Interface", "HD Interface"],
    "form_factor": ["Form Factor (Short)", "Hard Drive Form Factor", "Form Factor (RAM)"],
    "spindle_speed": ["Spindle Speed (rpm)"],
}
SKIP_SPECS = {"TCO Certified", "EPEAT Compliant", "ENERGY STAR Certified", "Color Category"}


def get(url):
    """GET with http's 429 / 5xx retries, plus a few tries for a download the server stalls or
    cuts off part-way (requests does not retry those). The last failure raises."""
    for attempt in range(1, TRIES + 1):
        try:
            r = http.session().get(url, timeout=(30, 120))
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            if attempt == TRIES:
                raise
            print(f"WARN {url[:110]}: {e.__class__.__name__}, retrying in {RETRY_PAUSE}s",
                  flush=True)
            time.sleep(RETRY_PAUSE)


def clean(text):
    return re.sub(r"\s+", " ", html.unescape(text or "").replace("\xa0", " ")).strip()


def money(text):
    try:
        value = float((text or "").replace(",", ""))
    except ValueError:
        return None
    return value or None                       # $0 = no real price


def listing_url(listing, page):
    """listing = (category, filters): filters is a tuple of (param, value) pairs (b= / p=)."""
    (code, path, _), filters = listing
    query = [("w", code), *filters, ("maxrecords", PAGE_SIZE), ("pcurrent", page)]
    return f"{BASE}/category/{path}/?{urlencode(query)}"


def parse_facets(text):
    """{facet key: [(value, label, count)]} from a listing page's filter sidebar."""
    facets = {}
    for chunk in text.split(FACET_SPLIT)[1:]:
        m = FACET_RE.search(chunk)
        if m:
            key, value, label, count = m.groups()
            facets.setdefault(key, []).append(
                (html.unescape(value), clean(label), int(count.replace(",", ""))))
    return facets


def parse_tiles(text, cat):
    tiles = []
    for chunk in text.split(TILE_SPLIT)[1:]:
        head, link = TILE_CODE_RE.search(chunk), LINK_RE.search(chunk)
        if not head or not link:
            raise RuntimeError(f"a product tile without a CDW# / link ({chunk[:120]!r})")
        mfg, price, msrp = MFG_RE.search(chunk), PRICE_RE.search(chunk), MSRP_RE.search(chunk)
        ptype = PRICE_TYPE_RE.search(chunk)
        image = IMAGE_RE.search(chunk)
        tiles.append({
            "code": head.group(1),
            "brand_code": head.group(2),
            "title": clean(link.group(2)),
            "url": BASE + link.group(1),
            "mfg": clean(mfg.group(1)) if mfg else "",
            "price": money(price.group(1)) if price else None,
            "msrp": money(msrp.group(1)) if msrp else None,
            "price_type": ptype.group(1) if ptype else "none",
            "image": image.group(1) if image else "",
            "specs": [(clean(k), clean(v)) for k, v in SPEC_RE.findall(chunk)],
            "cat_code": cat[0],
            "category": cat[2],
        })
    return tiles


def fetch_page(listing, page):
    """(tiles, total, facets) of one listing page; a page whose tiles don't match its label
    raises."""
    url = listing_url(listing, page)
    text = get(url).text
    time.sleep(DELAY)
    tiles = parse_tiles(text, listing[0])
    m = RANGE_RE.search(text)
    if not m:
        if not tiles and "search-result" not in text:
            return [], 0, {}                    # a filter that matches nothing any more
        raise RuntimeError(f"{url}: no 'A - B of N' label on the page")
    first, last, total = (int(x.replace(",", "")) for x in m.groups())
    start = (page - 1) * PAGE_SIZE + 1
    if start > total and first == 1:            # past the end: the listing shrank mid-run
        print(f"WARN {url}: past the end now ({total} products)", flush=True)
        return [], total, {}
    if (first, last, len(tiles)) != (start, min(page * PAGE_SIZE, total), last - first + 1):
        raise RuntimeError(f"{url}: {len(tiles)} tiles, label says {first}-{last} of {total}, "
                           f"page {page} should show {start}-{min(page * PAGE_SIZE, total)}")
    return tiles, total, parse_facets(text) if page == 1 else {}


def plan(cat, total, facets):
    """[(listing, expected count)] covering a category: the category itself when it fits in
    CAP, else one listing per brand (a brand over CAP split again by price bucket)."""
    if total <= CAP:
        return [((cat, ()), total)]
    brands = facets.get("b") or []
    if sum(n for _, _, n in brands) < total:
        raise RuntimeError(f"{cat[0]}: {total} products, but its brand facet only adds up to "
                           f"{sum(n for _, _, n in brands)}")
    listings = []
    for value, label, n in brands:
        if n <= CAP:
            listings.append(((cat, (("b", value),)), n))
            continue
        print(f"WARN {cat[0]} brand {label}: {n} products, more than {CAP}: splitting by price",
              flush=True)
        for price, _, pn in facets.get("p") or []:
            listings.append(((cat, (("b", value), ("p", price))), pn))
    return listings


def walk(pool, cats, limit):
    """{CDW#: tile} from every listing, plus the categories' own totals and the brand names."""
    firsts = list(pool.map(lambda c: fetch_page((c, ()), 1), cats))
    pages, totals, brand_names = {}, {}, {}      # pages: {listing: {page: (tiles, total)}}
    jobs = []
    for cat, (tiles, total, facets) in zip(cats, firsts):
        totals[cat[0]] = total
        pages[(cat, ())] = {1: (tiles, total)}   # kept even when the category is split by brand
        for value, label, _ in facets.get("b") or []:
            brand_names[value] = label
        listings = plan(cat, total, facets)
        print(f"{cat[0]} {cat[2]}: {total} products in {len(listings)} listing(s)", flush=True)
        for listing, n in listings:
            jobs += [(listing, p) for p in range(1, math.ceil(n / PAGE_SIZE) + 1)
                     if p not in pages.get(listing, {})]
    rounds = 0
    while jobs and not limit:                    # test runs read the first pages only
        print(f"{len(jobs)} pages to read", flush=True)
        for done, ((listing, page), (tiles, total, _)) in enumerate(
                zip(jobs, pool.map(lambda j: fetch_page(*j), jobs)), 1):
            pages.setdefault(listing, {})[page] = (tiles, total)
            if done % 25 == 0 or done == len(jobs):
                print(f"{done}/{len(jobs)} pages read", flush=True)
        # A listing that grew mid-run gets the pages its labels now add (once)
        jobs, rounds = [], rounds + 1
        for listing, got in pages.items():
            need = math.ceil(max(t for _, t in got.values()) / PAGE_SIZE)
            if listing[1] == () and totals[listing[0][0]] > CAP:
                continue                         # split by brand: its own pages are not read
            jobs += [(listing, p) for p in range(1, min(need, CAP // PAGE_SIZE) + 1)
                     if p not in got]
        if rounds == 2:
            jobs = []

    by_code, found_in = {}, {}
    for listing, got in pages.items():
        for tiles, _ in got.values():
            for t in tiles:
                found_in.setdefault(listing[0][0], set()).add(t["code"])
                by_code.setdefault(t["code"], t)
    for cat in cats:
        found, total = len(found_in.get(cat[0], ())), totals[cat[0]]
        if limit:
            continue
        if total - found > CHURN_SLACK:
            raise RuntimeError(f"{cat[0]} {cat[2]}: only {found} unique products of {total}")
        if found < total:
            print(f"WARN {cat[0]}: {found} unique products of {total} (some vanished mid-run?)",
                  flush=True)
    return by_code, totals, brand_names


def condition_of(title):
    return next((c for rx, c in CONDITIONS if rx.search(title)), "")


def build_row(t, brand_names, scraped_at):
    specs = dict(t["specs"])
    cols = {col: next((specs[k] for k in keys if specs.get(k)), "")
            for col, keys in SPEC_COLUMNS.items()}
    used = {k for keys in SPEC_COLUMNS.values() for k in keys} | SKIP_SPECS
    other = "; ".join(f"{k}: {v}" for k, v in t["specs"] if k not in used and v)
    capacity = cols["capacity"].replace(" ", "")
    if not capacity and t["cat_code"] in MEMORY_CODES:
        m = MEMORY_CAPACITY_RE.search(t["title"])
        capacity = m.group(1) + m.group(2) if m else ""
    tb = None
    if cols["capacity"]:
        m = MEMORY_CAPACITY_RE.search(cols["capacity"])
        tb = (float(m.group(1)) if m.group(2) == "TB" else float(m.group(1)) / 1000) if m else None
    spindle = re.sub(r"\s*revolutions per minute", " rpm", cols["spindle_speed"])
    price, msrp = t["price"], t["msrp"]
    compare = msrp if price and msrp and msrp > price else None
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": t["code"],
        "sku": t["code"],
        "part_number": t["mfg"],
        "title": t["title"],
        "brand": brand_names.get(t["brand_code"], ""),
        "category": t["category"],
        "condition": condition_of(t["title"]),
        "capacity": capacity,
        "interface": cols["interface"],
        "form_factor": cols["form_factor"],
        "spindle_speed": spindle,
        "other_specs": other[:2000],
        "price": price,
        "compare_at_price": compare,
        "price_per_tb": round(price / tb, 2) if price and tb else None,
        "discount_pct": round((compare - price) / compare * 100, 1) if compare else None,
        "available": None,                     # stock is only in the private dc-origin backend
        "stock_qty": None,
        "image_url": t["image"] or None,
        "description": None,                   # tiles carry no description (product pages do)
        "url": t["url"],
    })


def scrape(limit, scraped_at):
    cats = CATEGORIES
    with ThreadPoolExecutor(WORKERS) as pool:
        if limit:                              # test run: page 1 of a spread of categories
            cats = [c for c in CATEGORIES if c[0] in ("SA2", "KB1", "KE", "RB4", "RJ1")]
        by_code, totals, brand_names = walk(pool, cats, limit)
    types = {}
    for t in by_code.values():
        types[t["price_type"]] = types.get(t["price_type"], 0) + 1
    print(f"{len(by_code)} unique products (category totals add up to {sum(totals.values())}); "
          f"price types {types}", flush=True)
    items = list(by_code.values())
    if limit:
        step = max(1, len(items) // limit)     # spread the sample over the categories
        items = items[::step][:limit]
    return [build_row(t, brand_names, scraped_at) for t in items], (
        None if limit else sum(totals.values()))


if __name__ == "__main__":
    runner.run(SITE, scrape)
