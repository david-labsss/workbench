"""Scrape aloinfousa.com (Shopify, ~229k products): every run's full snapshot appended to Postgres.

aloinfousa.com has far more products than Shopify's 25,000-item deep-paging cap, so /products.json
and collection listings can't reach the whole catalogue. The Storefront GraphQL API
(/api/<version>/graphql.json, with the public storefront token the theme ships in its HTML, read
fresh every run) has a top-level products search that takes a created_at range and sorts by
CREATED_AT. We walk each calendar month oldest first, 250 per page, a few months in parallel; when
a listing reaches the 25k cap we restart it from the last createdAt seen. Products are deduped by
id. Same approach as esaitech / orangehardwares. Prices are asked for the US market
(@inContext(country: US)), which is also the store's default market and what its pages show.

One row per variant: product_id is the variant id, parent_id the product id. Price is the variant
price, compare_at_price its compare-at price (only when above the price; the store often sends
0.0), in stock = availableForSale. The store doesn't publish stock counts (quantityAvailable is
access-denied), so stock_qty stays blank.

Fields are messy (the catalogue is merged from several feeds), so:
  brand      the "Brand_<name>" tag when there is one, else the vendor. The vendor is often the
             placeholder "Major Brand" -> then the title's first word if it is a real vendor name
             seen elsewhere in the catalogue, else blank.
  category   product type (when the product type is a condition such as "Factory New", the
             "Type_<name>" tag instead).
  condition  the "Condition_<name>" tag, else the title (Refurbished / Recertified / Open Box /
             Used / New), else a condition code on the SKU (_NB / _MR / _SR suffixes, -NEW,
             -REF, -NOB). Blank when none says.
  sku        the variant SKU as published. It is often an internal code (5-digit numbers,
             Amazon ASINs, distributor prefixes), so it is not used blindly as the part number.
  part_number  the manufacturer part number, only where it is clear: the SKU's base when the SKU
             is "<MPN>_<condition code>"; the SKU (or a part of it) when it also appears in the
             title; else a part-number-shaped word right after the brand at the start of the
             title; else a single part number in brackets at the end of the title. Words after
             "for ..." (compatibility) are never used. Blank otherwise.
  capacity   first upper-case GB/TB size in the title, for drives / memory only (judged by the
             product type, or by the title when the product type is blank).

The product count comes from the store's root (English) product sitemaps (1,000 per file; the
sitemap index also lists 9 locale copies, which are ignored), read from the index plus the first
and last file: 3 requests. It is only a completeness check, so a truncated crawl is never saved
as the full catalogue. (collections/all.json reports a larger products_count, ~256k, that the
sitemap and Storefront don't reach, so it isn't used.)

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.

Env vars:
  DATABASE_URL        Postgres URL for the scraper_writer role
  STOREFRONT_TOKEN    optional, overrides the public token read from the site's HTML
  MAX_PRODUCTS        optional, stop after about N products (test runs)
  DRY_RUN             optional, "1" = print sample rows, don't write anything
"""
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common import runner, schema

SITE = "aloinfousa"
BASE = "https://www.aloinfousa.com"
API = BASE + "/api/2025-07/graphql.json"
FALLBACK_TOKEN = "fd3ad96582ee257a669235d19af4a14f"   # public token seen in the theme, 2026-10
PAGE = 250            # max per request
VARIANTS = 20         # variants fetched with each product (most have 1, a few up to 5)
CAP = 25000           # Shopify won't page deeper than this per listing
WORKERS = 4           # low concurrency: be polite
PAGE_DELAY = 0.3      # small pause between pages within a window
USER_AGENT = "aloinfousa-scraper/1.0 (price monitoring)"
PLACEHOLDER_VENDORS = {"", "major brand", "n/a", "na", "none", "unknown", "generic"}

CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b")   # upper-case only: "8Gb" is a link speed
STORAGE_RE = re.compile(r"\b(hdd|ssd|drives?|hard\s*drives?|memory|ram|dimms?|rdimm|lrdimm|"
                        r"sodimm|udimm|flash|nvme|storage|ddr\d|sas|sata|m\.2|u\.2)\b", re.I)
CONDITIONS = [
    (re.compile(r"\b(refurb(ished)?|renewed)\b", re.I), "Refurbished"),
    (re.compile(r"\brecertified\b", re.I), "Recertified"),
    (re.compile(r"\bopen[\s-]+box\b", re.I), "Open Box"),
    (re.compile(r"\b(used|pre-?owned)\b", re.I), "Used"),
    (re.compile(r"\bnew\b", re.I), "New"),
]
SKU_CONDITIONS = [
    (re.compile(r"_NB(?:_\d+)?$"), "New"),
    (re.compile(r"_MR(?:_\d+)?$"), "Recertified"),
    (re.compile(r"_SR(?:_\d+)?$"), "Refurbished"),
    (re.compile(r"[-_]NOB(?:$|[-_])", re.I), "Open Box"),
    (re.compile(r"[-_](?:REF|RFB)(?:$|[-_])", re.I), "Refurbished"),
    (re.compile(r"[-_]NEW(?:$|[-_])", re.I), "New"),
    (re.compile(r"[-_]USED(?:$|[-_])", re.I), "Used"),
]
SKU_CODE_RE = re.compile(r"^([A-Za-z0-9][A-Za-z0-9-]{3,})(?:_[A-Z0-9]+)*_(?:NB|MR|SR)(?:_\d+)?$")
# Words that look like part numbers but are specs: 480GB, 10K, 512e, 2x8GB, PC4-19200R, DDR4,
# Cat6, G14, Gen9, 128-Channel ...
UNITS = ("TB|GB|MB|KB|TIB|GIB|W|KW|V|VAC|VDC|A|MA|MAH|AH|VA|KVA|K|M|U|G|X|P|E|T|C|S|R|L|D|HZ|"
         "KHZ|MHZ|GHZ|RPM|FT|MM|CM|IN|NM|GBE|GBPS|MBPS|GBIT|MT|GT|PIN|PORTS?|PCS|PK|PACK|TH|ST|ND|"
         "RD|BIT|CORES?|AWG|DWPD|LBS?|OZ|IOPS|LFF|SFF|NITS?|CFM|GBASE")
SPEC_RE = re.compile(r"\d+(?:\.\d+)?(?i:%s)?|\d{1,3}[xX]\d{1,3}(?:\.\d+)?(?:[xX]\d+)*[A-Za-z]{0,4}"
                     r"|\d[Rr][xX]\d+|(?i:PC|DDR|CAT|GEN|PCIE|USB)\d.*|[Gg]\d{1,2}|\d+-[A-Za-z][a-z]+"
                     r"|\d+(?:\.\d+)?[A-Za-z]*/[sS]|\d+[Kk]\.\d+" % UNITS)
NAME_RE = re.compile(r"[A-Z][a-z]+[A-Za-z]*-\d{1,2}")    # family names: ConnectX-6, Spectrum-4
STORAGE_CATEGORY_RE = re.compile(r"drive|ssd|hdd|memory|\bram\b|dimm|flash|nvme", re.I)
NOT_STORAGE_RE = re.compile(r"enclosure|cage|tray|caddy|bracket|adapter|cable|controller|"
                            r"backplane|tape|duplicator|dock", re.I)
PN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9\-./#+]*[A-Za-z0-9#+]")
FOR_RE = re.compile(r"\s(?:for|fits|compatible\s+with)\s", re.I)
THIRD_PARTY_RE = re.compile(r"\b(remanufactured|compatible|replacement|generic|equivalent)\b", re.I)
END_PAREN_RE = re.compile(r"\(([^()]+)\)\s*$")
STRIP = "()[]{},;:|\"'*"

_local = threading.local()
_token = None

VARIANT_FIELDS = "id sku title price { amount } compareAtPrice { amount } availableForSale"
PRODUCT_FIELDS = """
  id title handle vendor productType tags availableForSale createdAt
  variants(first: %d) { pageInfo { hasNextPage endCursor } nodes { %s } }
""" % (VARIANTS, VARIANT_FIELDS)
LIST_QUERY = """
query($first: Int!, $after: String, $query: String!) @inContext(country: US) {
  products(first: $first, after: $after, query: $query, sortKey: CREATED_AT) {
    pageInfo { hasNextPage endCursor }
    nodes { %s }
  }
}""" % PRODUCT_FIELDS
MORE_VARIANTS_QUERY = """
query($id: ID!, $after: String) @inContext(country: US) {
  node(id: $id) { ... on Product {
    variants(first: 250, after: $after) { pageInfo { hasNextPage endCursor } nodes { %s } }
  } }
}""" % VARIANT_FIELDS
EARLIEST_QUERY = "{ products(first: 1, sortKey: CREATED_AT, reverse: false) { nodes { createdAt } } }"


# --- fetching ----------------------------------------------------------------
def storefront_token():
    """The public Storefront token: env override, else read from the theme's HTML."""
    global _token
    if _token:
        return _token
    _token = os.getenv("STOREFRONT_TOKEN")
    if not _token:
        try:
            r = requests.get(BASE + "/", headers={"User-Agent": USER_AGENT}, timeout=60)
            r.raise_for_status()
            m = re.search(r'"accessToken"\s*:\s*"([0-9a-f]{32})"', r.text)
            _token = m.group(1) if m else None
        except Exception as e:  # noqa: BLE001 - fall back to the known token
            print(f"WARN couldn't read the storefront token from the site: {e}", flush=True)
        if not _token:
            print("WARN storefront token not found in the HTML; using the known one", flush=True)
            _token = FALLBACK_TOKEN
    return _token


def session():
    s = getattr(_local, "s", None)
    if s is None:
        s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT, "Content-Type": "application/json",
                          "X-Shopify-Storefront-Access-Token": storefront_token()})
        retry = Retry(total=6, backoff_factor=2, status_forcelist=[429, 430, 500, 502, 503, 504],
                      allowed_methods=["POST"], respect_retry_after_header=True)
        s.mount("https://", HTTPAdapter(max_retries=retry))
        _local.s = s
    return s


def gql(query, variables=None):
    """Run a query; retry GraphQL-level throttling / internal errors, raise on anything else."""
    for attempt in range(6):
        r = session().post(API, json={"query": query, "variables": variables or {}}, timeout=90)
        r.raise_for_status()
        data = r.json()
        errors = data.get("errors")
        if not errors:
            return data["data"]
        codes = {(e.get("extensions") or {}).get("code") for e in errors}
        if codes & {"THROTTLED", "INTERNAL_SERVER_ERROR"} and attempt < 5:
            time.sleep(5 * (attempt + 1))
            continue
        raise RuntimeError(f"Storefront API error: {errors[0].get('message')}")
    raise RuntimeError("Storefront API kept failing")


def earliest_year():
    """The year the oldest product was created, so the month windows start there."""
    nodes = gql(EARLIEST_QUERY)["products"]["nodes"]
    return int(nodes[0]["createdAt"][:4]) if nodes else datetime.now(timezone.utc).year


def month_windows(now, first_year):
    """[start, end) pairs, one per calendar month from first_year to next month."""
    edges, y, m = [], first_year, 1
    while True:
        edges.append(f"{y:04d}-{m:02d}-01T00:00:00Z")
        if (y, m) > (now.year, now.month):
            break
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return list(zip(edges, edges[1:]))


def complete_variants(n):
    """Fetch the rest of a product's variants when it has more than VARIANTS."""
    variants = n.get("variants") or {}
    info = variants.get("pageInfo") or {}
    while info.get("hasNextPage"):
        page = gql(MORE_VARIANTS_QUERY, {"id": n["id"], "after": info["endCursor"]})
        more = ((page.get("node") or {}).get("variants")) or {}
        variants["nodes"] = (variants.get("nodes") or []) + (more.get("nodes") or [])
        info = more.get("pageInfo") or {}
    return n


def fetch_window(window, limit=0):
    """Every product created in [start, end), oldest first. A listing stops at 25k, so when it
    gets there we start a new listing from the last createdAt seen (">=" so products sharing that
    second aren't skipped; the overlap is removed by product id). Any failed page raises."""
    start, end = window
    by_id, after, frm, depth = {}, None, start, 0
    while True:
        q = f"created_at:>='{frm}' AND created_at:<'{end}'"
        page = gql(LIST_QUERY, {"first": PAGE, "after": after, "query": q})["products"]
        for n in page["nodes"]:
            by_id[n["id"]] = complete_variants(n)
        depth += len(page["nodes"])
        if not page["pageInfo"]["hasNextPage"] or (limit and len(by_id) >= limit):
            break
        if depth + PAGE > CAP:
            last = page["nodes"][-1]["createdAt"]
            if last == frm:
                raise RuntimeError(f"25k+ products created at {last}; can't reach them all")
            frm, after, depth = last, None, 0
        else:
            after = page["pageInfo"]["endCursor"]
        time.sleep(PAGE_DELAY)
    if by_id:
        print(f"created {start[:7]}: {len(by_id)} products", flush=True)
    return list(by_id.values())


def fetch_all(limit=0):
    windows = month_windows(datetime.now(timezone.utc), earliest_year())
    print(f"{len(windows)} monthly windows", flush=True)
    by_id = {}
    if limit:
        # Test runs: one window at a time until there are enough products
        for w in windows:
            for n in fetch_window(w, limit):
                by_id[n["id"]] = n
            if len(by_id) >= limit:
                break
        return list(by_id.values())[:limit]
    with ThreadPoolExecutor(WORKERS) as pool:
        for nodes in pool.map(fetch_window, windows):
            for n in nodes:
                by_id[n["id"]] = n
            if nodes:
                print(f"  unique so far: {len(by_id)}", flush=True)
    return list(by_id.values())


def listed_total():
    """Product count from the root (English) product sitemaps, a completeness check only.
    Shopify's product sitemaps hold 1,000 URLs each except the last, so the total is read from
    the index, the first and the last file. None if the layout isn't what we expect (then the
    check is skipped)."""
    try:
        s = requests.Session()
        s.headers["User-Agent"] = USER_AGENT
        r = s.get(f"{BASE}/sitemap.xml", timeout=60)
        r.raise_for_status()
        root = re.compile(re.escape(BASE) + r"/sitemap_products_\d+\.xml")
        subs = [u.replace("&amp;", "&") for u in re.findall(r"<loc>([^<]+)</loc>", r.text)
                if root.match(u)]   # skips the /es/, /pt-br/, ... locale copies
        if not subs:
            return None

        def count(url):
            page = s.get(url, timeout=60)
            page.raise_for_status()
            return len(re.findall(r"<loc>[^<]*/products/[^<]*</loc>", page.text))

        first = count(subs[0])
        if first != 1000 and len(subs) > 1:
            return None
        last = count(subs[-1]) if len(subs) > 1 else first
        return (len(subs) - 1) * 1000 + last
    except Exception as e:  # noqa: BLE001 - only a sanity check
        print(f"WARN couldn't read the sitemap product count: {e}", flush=True)
        return None


# --- mapping -----------------------------------------------------------------
def _to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def money(m):
    return _to_float((m or {}).get("amount"))


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def tag_value(tags, prefix):
    for t in tags or []:
        if t.startswith(prefix) and clean(t[len(prefix):]):
            return clean(t[len(prefix):])
    return ""


def condition_label(text):
    for rx, label in CONDITIONS:
        if rx.search(text or ""):
            return label
    return ""


def condition_of(tags, title, sku):
    tag = tag_value(tags, "Condition_")
    if tag:
        return condition_label(tag) or tag
    found = condition_label(title)
    if found:
        return found
    for rx, label in SKU_CONDITIONS:
        if rx.search(sku or ""):
            return label
    return ""


def alnum(text):
    return re.sub(r"[^a-z0-9]", "", (text or "").lower())


def vendor_of(n):
    """The vendor without feed junk such as a "Brand: " prefix."""
    return re.sub(r"^brand\s*:\s*", "", clean(n.get("vendor")), flags=re.I)


def brand_of(n, title, vendors):
    tag = tag_value(n.get("tags"), "Brand_")
    if tag:
        return tag
    vendor = vendor_of(n)
    if vendor.lower() not in PLACEHOLDER_VENDORS:
        return vendor
    first = alnum(title.split(" ", 1)[0]) if title else ""
    return vendors.get(first, "")


def is_brand_word(token, brand_words):
    t = alnum(token)
    return len(t) >= 2 and any(t == w or (len(t) >= 2 and w.startswith(t)) or
                               (len(w) >= 2 and t.startswith(w)) for w in brand_words)


def pn_like(token):
    return (len(token) >= 4 and re.search(r"\d", token) and PN_RE.fullmatch(token)
            and (re.search(r"[A-Za-z]", token) or "-" in token) and not SPEC_RE.fullmatch(token)
            and not NAME_RE.fullmatch(token))


def part_number_of(sku, title, brand, vendor):
    """The manufacturer part number where it is clear, else ""."""
    sku = clean(sku)
    m = SKU_CODE_RE.match(sku)
    if m:                                    # <MPN>_DELLG14_SR_11, <MPN>_NB ...
        return m.group(1)
    head = FOR_RE.split(" " + title + " ", 1)[0]       # never words after "for ..."
    tokens = [t.strip(STRIP) for t in head.split()]
    tokens = [t for t in tokens if t]
    if len(sku) >= 4 and re.search(r"\d", sku) and \
            any(t.upper() == sku.upper() for t in tokens):          # the whole SKU is in the title
        return sku
    base = re.split(r"[_\s]", sku)[0] if sku else ""
    if base:
        if len(base) >= 4 and (re.search(r"\d", base) or "-" in base) and \
                any(t.upper() == base.upper() for t in tokens):
            return base
        # a title word that is a hyphen-separated part of the SKU (HP-QK735A-REF-K, PVDM4-128-R)
        parts = [t for t in tokens if len(t) >= 5 and pn_like(t) and re.search(
            r"(?:^|[-_])" + re.escape(t) + r"(?:$|[-_])", base, re.I)]
        if parts:
            return max(parts, key=len)
    brand_words = {alnum(w) for w in f"{brand} {vendor}".split() if len(alnum(w)) >= 2}
    i = 0
    while i < len(tokens) and i < 3 and is_brand_word(tokens[i], brand_words):
        i += 1
    for t in tokens[i:i + 4]:
        if pn_like(t):
            return t
    if not THIRD_PARTY_RE.search(title):
        m = END_PAREN_RE.search(title)
        inner = m.group(1).strip(STRIP + " ") if m else ""
        if inner and " " not in inner and pn_like(inner):
            return inner
    return ""


def category_of(n):
    ptype = clean(n.get("productType"))
    if ptype and condition_label(ptype) and len(ptype.split()) <= 3:   # e.g. "Factory New"
        return tag_value(n.get("tags"), "Type_")
    return ptype


def build_rows(n, scraped_at, vendors):
    """One row per variant of a product (product_id = variant id, parent_id = product id)."""
    title = clean(n.get("title"))
    product_id = n["id"].rsplit("/", 1)[-1]
    vendor = vendor_of(n)
    brand = brand_of(n, title, vendors)
    category = category_of(n)
    capacity = ""
    # drives / memory only: by category when there is one, else by the title
    storage = (STORAGE_CATEGORY_RE.search(category) and not NOT_STORAGE_RE.search(category)) \
        if category else STORAGE_RE.search(title)
    if storage:
        cap = CAPACITY_RE.search(title)
        capacity = (cap.group(1) + cap.group(2)) if cap else ""
    nodes = (n.get("variants") or {}).get("nodes") or []
    multi = len(nodes) > 1
    rows = []
    for v in nodes:
        vid = v["id"].rsplit("/", 1)[-1]
        sku = clean(v.get("sku"))
        vtitle = clean(v.get("title"))
        full_title = title + ("" if vtitle in ("", "Default Title") else f" - {vtitle}")
        price = money(v.get("price"))
        compare = money(v.get("compareAtPrice"))
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": vid,        # unique: one row per variant
            "parent_id": product_id,
            "variant_id": vid,
            "sku": sku,
            "part_number": part_number_of(sku, title, brand, vendor),
            "title": full_title,
            "brand": brand,
            "category": category,
            "condition": condition_of(n.get("tags"), full_title, sku),
            "capacity": capacity,
            "price": price if price else None,          # $0 = no price, never 0
            "compare_at_price": compare if (compare and price and compare > price) else None,
            "available": bool(v.get("availableForSale")),
            "stock_qty": None,   # not published by the store
            "url": f"{BASE}/products/{n.get('handle')}" + (f"?variant={vid}" if multi else ""),
        }))
    return rows


def known_vendors(items):
    """Real vendor names seen in the catalogue, keyed by their letters/digits, so a "Major Brand"
    title that starts with one of them gets that brand."""
    vendors = {}
    for n in items:
        v = vendor_of(n)
        if v.lower() not in PLACEHOLDER_VENDORS and len(alnum(v)) >= 2:
            vendors.setdefault(alnum(v), v)
    return vendors


def scrape(limit, scraped_at):
    items = fetch_all(limit)
    vendors = known_vendors(items)
    rows = [row for n in items for row in build_rows(n, scraped_at, vendors)]
    listed = None if limit else listed_total()
    # Product-level completeness guard (rows are per variant; products are the right unit here)
    if not limit and listed and len(items) < 0.98 * listed:
        sys.exit(f"Only fetched {len(items)} of {listed} products ({len(items) / listed:.0%}); "
                 "not saving a partial snapshot")
    print(f"fetched {len(items)} unique products -> {len(rows)} variant rows "
          f"(sitemap lists {listed})", flush=True)
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
