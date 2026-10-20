"""Scrape pcnation.com, IT categories only (~161k products): every run's full snapshot appended
to Postgres.

PCNation is a Next.js (App Router) + Payload CMS store on Vercel, with no bot challenge. Its
category pages get their products from Elasticsearch through the page's own Next.js server action
"searchAction": the browser builds an Elasticsearch body (category filter, facet filters, from /
size paging, sort, facet aggregations) and POSTs [m, cacheKey] to the category page's own URL with
the headers Next-Action: <action id> and Next-Router-State-Tree. The server passes the body to
Elasticsearch and answers with an RSC flight; the row that row 0 points to ("a":"$@1") is
{"ok": true, "data": {"hits", "total", "aggregations"}}. robots.txt allows the category pages (it
disallows /api/, /search, /cart, /account, /checkout and /sandbox/). This scraper sends the same
request from the same pages.

The action id changes with every deploy, so each run reads it from the site's JS: the Storage
page's <script> chunks are fetched (1934-*.js first, where it was on 2026-10-20, then the others
from the page-specific end of the list) until one contains
createServerReference)("<id>", ..., "searchAction"). If the store deploys during a run, the old id
gets HTTP 404 "Server action not found" (x-nextjs-action-not-found: 1); the id is then read again
once and the request retried.

Scope: the four IT trees under Hardware (the category ids the site itself filters on):
  15 Storage (drives, SSDs, SAN / NAS, controllers, backup), 11 Networking (switches, routers,
  wireless, transceivers, NICs, network security, also physical security and communication
  devices), 109 Systems (servers, desktops, laptops, workstations, thin clients), 136 Components
  (processors, memory, motherboards, power supplies, video cards).
Peripherals (incl. Power & Rack), Services and Software are left out.

Query shape: the body the site's own code sends from a category page, with a bigger page:
  {"query": {"bool": {
       "must":   [{"term": {"categories": "<root id>"}}],
       "filter": [{"range": {"RetailRate": {"gte": lo, "lte": hi}}},      # the price facet *
                  {"term": {"enabled": true}}, {"exists": {"field": "base_image"}}],
       "should": [{"term": {"StockStatus": {"value": true, "boost": 10}}},  # the site's scoring
                  {"terms": {"BrandName.keyword": [featured brands], "boost": 15}}]}},
   "from": 0 / 1000 / ... / 9000, "size": 1000,
   "sort": [{"BrandName.keyword": "asc"}, {"identifier.keyword": "asc"}],  # "Sort by Brand"
   "_source": [the 15 fields used below]}
* The server does not filter on the RetailRate field itself: it applies the range to the price
  the site shows (PCNationRate, or RetailRate when that is 0), the same price as its own
  "RetailRate_brackets" facet script (checked 2026-10-20: "<= 65" returned PCNationRate up to
  64.99 but RetailRate up to 243,250). Both ends are inclusive.
Elasticsearch only pages through the first 10,000 hits of a query (from + size <= 10,000), so each
category is cut into price slices of at most 9,000 products. The cuts come from one plan request
per category (size 0, aggregations only, like the site's own price-bracket request): the site's
in / out of stock aggregation gives the exact product count (the hit total stops at "10,000+"),
and the site's RetailRate_brackets range aggregation (same price script) gives the count in each
price band. Neighbouring bands are joined into slices that cover all prices with no gap (a product
priced exactly on a cut is fetched twice and kept once). Each slice must report an exact total
under 10,000, and its pages are fetched; a slice that comes back short is fetched again once.
Not used: search_after, scroll, point-in-time, track_total_hits (the site's code never sends them).
Differences from the browser's body, all to make each request lighter for the store: page
requests leave out the facet aggregations (the browser asks for ~30 facets on every page), and
_source lists the fields used: whole documents are ~7 KB each (7 MB and 188 s per 1,000 from
Pakistan on 2026-10-20), the field list cuts that to ~0.95 MB and ~7 s.

The server caches action results per request (an identical repeat returned the earlier bytes), so
every request carries a page number of its own (m.page = run start in seconds * 10000 + n).

Fields: identifier (PCNation's SKU, "PNA10KCNM", = the Elasticsearch _id, unique) -> product_id
and sku; mpn -> part_number; BrandName; name -> title; the deepest category_text node ("...||Solid
State Disks||SATA Solid State Disks") -> category; Condition, the store's text ("New and Factory
Fresh", "Manufacturer Refurbished"); PCNationRate -> price (the price on the product page and in
its JSON-LD; None when 0 / missing); RetailRate (list price) -> compare_at_price when higher (it is
lower on ~24% of products); StockStatus -> available: the product page's "In Stock" / "Out of
Stock" and JSON-LD availability follow StockStatus alone (checked 2026-10-20: StockStatus false
with Quantity 18 shows "Out of Stock", true with Quantity 0 shows "In Stock"); Quantity -> stock_qty
(the store's unit count, not shown to shoppers; left empty when it says 0 for a product shown in
stock); url = https://www.pcnation.com/<pcn_url_key> (the product page's canonical URL).
base_image -> image_url (media.pcnation.net, 800 px); product_highlights -> description (the full
"description", a ~3.7 KB spec-table HTML per product, would make every page ~7x bigger).
listed = the sum of the four categories' exact counts.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import itertools
import json
import re
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

import requests

from common import http, runner, schema

SITE = "pcnation"
BASE = "https://www.pcnation.com"
ROOTS = [("15", "hardware/storage"), ("11", "hardware/networking"),
         ("109", "hardware/systems"), ("136", "hardware/components")]
PAGE = 1000                   # products per request
WINDOW = 10000                # Elasticsearch's from + size limit
SLICE = 9000                  # most products per price slice (room for cut prices / new products)
WORKERS = 3
DELAY = 0.5                   # before each request, to stay polite
TRIES = 3                     # attempts per request for stalled / cut-off downloads
RETRY_PAUSE = 30
SOURCE = ["identifier", "mpn", "BrandName", "name", "PCNationRate", "RetailRate", "Quantity",
          "StockStatus", "Condition", "pcn_url_key", "category_text", "categories", "updated",
          "base_image", "product_highlights"]
# The product page's own image URL: the store's image CDN resizing base_image to 800 px wide
IMAGE_BASE = "https://media.pcnation.net/cdn-cgi/image/width=800,format=auto/"
FLIGHT_REF_RE = re.compile(r"\$[0-9a-f]+")   # a string moved to its own RSC row (>= 1 KB long)
FEATURED = ["Lenovo", "HP", "Dell", "Microsoft", "Samsung", "Cisco", "APC", "Tripp Lite"]
SORT = [{"BrandName.keyword": "asc"}, {"identifier.keyword": "asc"}]
# The site's own in / out of stock facet (every product lands in one of the two buckets)
STOCK_AGG = {"terms": {"script": {"source": """
          if (doc['StockStatus'].size() == 0) return 'out_of_stock';
          if (doc['StockStatus'].value == true) return 'in_stock';
          return 'out_of_stock';
        """, "lang": "painless"}, "size": 2}}
# The price the site shows and filters on (its own script, as in its RetailRate_brackets facet)
PRICE_SCRIPT = """
  double retail = doc['RetailRate'].size() != 0 ? doc['RetailRate'].value : 0;
  double pcn = doc['PCNationRate'].size() != 0 ? doc['PCNationRate'].value : 0;
  double price = pcn > 0 ? pcn : retail;
 return price;"""
# Price band edges for the plan request (USD)
EDGES =([0.01] + list(range(5, 100, 5)) + list(range(100, 500, 10)) + list(range(500, 1000, 25))
         + list(range(1000, 5000, 100)) + list(range(5000, 20000, 500))
         + list(range(20000, 100000, 5000)) + [100000, 250000, 1000000])
ACTION_RE = re.compile(r'createServerReference\)\("([0-9a-f]{20,})",[^)]*?"searchAction"\)')
CHUNK_RE = re.compile(r'<script src="(/_next/static/chunks/[^"]+?\.js[^"]*)"')
ROW_RE = re.compile(rb"([0-9a-f]+):")
RESULT_REF_RE = re.compile(rb'"a":"\$@([0-9a-f]+)"')

_action = {"id": None, "refreshed": False}
_action_lock = threading.Lock()
_pages = itertools.count()
_run = int(time.time())


class ActionNotFound(Exception):
    """The store deployed and the searchAction id changed."""


def retrying(what, call):
    """call() with http's 429 / 5xx retries, plus a few tries for a download the server stalls or
    cuts off part-way (requests does not retry those). The last failure raises."""
    for attempt in range(1, TRIES + 1):
        try:
            return call()
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            if attempt == TRIES:
                raise
            print(f"WARN {what}: {e.__class__.__name__}, retrying in {RETRY_PAUSE}s", flush=True)
            time.sleep(RETRY_PAUSE)


def get(url):
    def call():
        r = http.session().get(url, timeout=(30, 180))
        r.raise_for_status()
        return r.text
    return retrying(url[:90], call)


def find_action_id():
    """The searchAction id of the live deploy, read from the Storage page's JS chunks."""
    html = get(f"{BASE}/{ROOTS[0][1]}")
    chunks = list(dict.fromkeys(CHUNK_RE.findall(html)))[::-1]   # page-specific chunks first
    chunks.sort(key=lambda c: 0 if "/1934-" in c else 1)
    for chunk in chunks:
        m = ACTION_RE.search(get(BASE + chunk))
        if m:
            print(f"searchAction id {m.group(1)} (in {chunk.split('?')[0].rsplit('/', 1)[-1]})",
                  flush=True)
            return m.group(1)
    raise RuntimeError(f"searchAction id not found in the {len(chunks)} scripts of the Storage page")


def action_id(stale=None):
    with _action_lock:
        if _action["id"] is None:
            _action["id"] = find_action_id()
        elif stale and _action["id"] == stale:
            if _action["refreshed"]:
                raise RuntimeError("searchAction id changed twice during the run")
            print("WARN searchAction id is stale (new deploy), reading it again", flush=True)
            _action["id"], _action["refreshed"] = find_action_id(), True
        return _action["id"]


def router_tree(handle):
    """The Next-Router-State-Tree header a browser on this category page sends."""
    tree = ["", {"children": ["(storefront)", {"children": ["(templates)", {"children": ["(main)", {
        "children": ["category-page", {"children": [["handle", handle, "c"], {
            "children": ["__PAGE__", {}]}]}]}]}]}]}, None, None, True]
    return urllib.parse.quote(json.dumps(tree, separators=(",", ":")), safe="")


def flight_rows(content):
    """{row id: bytes} of an RSC flight response (text rows "id:T<hex length>,<bytes>" are read by
    their declared length, the rest one line each)."""
    rows, i = {}, 0
    while i < len(content):
        m = ROW_RE.match(content, i)
        if not m:
            j = content.find(b"\n", i)
            i = len(content) if j < 0 else j + 1
            continue
        key, i = m.group(1).decode(), m.end()
        if content[i:i + 1] == b"T":
            comma = content.find(b",", i)
            n = int(content[i + 1:comma], 16)
            rows[key], i = content[comma + 1:comma + 1 + n], comma + 1 + n
            continue
        j = content.find(b"\n", i)
        j = len(content) if j < 0 else j
        rows[key], i = content[i:j], j + 1
    return rows


def search(cat, handle, frm=0, size=PAGE, price=None, aggs=None):
    """One searchAction call shaped like the category page's own. price = (lo, hi), either may be
    None for an open end. Returns data {hits, total, aggregations}."""
    clauses, facet = [], {}
    if price:
        lo, hi = price
        bounds = {k: v for k, v in (("gte", lo), ("lte", hi)) if v is not None}
        clauses = [{"range": {"RetailRate": bounds}}]
        facet = {"RetailRate": {k: v for k, v in (("from", lo), ("to", hi)) if v is not None}}
    body = {"query": {"bool": {
                "must": [{"term": {"categories": cat}}],
                "filter": clauses + [{"term": {"enabled": True}}, {"exists": {"field": "base_image"}}],
                "should": [{"term": {"StockStatus": {"value": True, "boost": 10}}},
                           {"terms": {"BrandName.keyword": FEATURED, "boost": 15}}]}},
            "from": frm, "size": size}
    if aggs:
        body["aggs"] = aggs
    if size:
        body["sort"] = SORT
        body["_source"] = SOURCE
    m = {"categoryId": cat, "type": "category", "page": _run * 10000 + next(_pages),
         "filters": facet, "sort": {"field": "brand", "direction": "asc"} if size else {},
         "filterClauses": clauses, "preference": "_local", "body": body}
    payload = json.dumps([m, f"search:category:{cat}"], separators=(",", ":"))
    url = f"{BASE}/{handle}"

    def call():
        time.sleep(DELAY)
        aid = action_id()
        r = http.session().post(url, data=payload, timeout=(30, 180), headers={
            "Accept": "text/x-component", "Content-Type": "text/plain;charset=UTF-8",
            "Next-Action": aid, "Next-Router-State-Tree": router_tree(handle),
            "Origin": BASE, "Referer": url})
        if r.status_code == 404 and r.headers.get("x-nextjs-action-not-found"):
            raise ActionNotFound(aid)
        r.raise_for_status()
        rows = flight_rows(r.content)
        ref = RESULT_REF_RE.search(rows.get("0", b""))
        if not ref or ref.group(1).decode() not in rows:
            raise RuntimeError(f"no searchAction result in the response ({len(r.content)} bytes)")
        return json.loads(rows[ref.group(1).decode()])

    what = f"/{handle} price={price} from={frm}"
    try:
        result = retrying(what, call)
    except ActionNotFound as e:
        action_id(stale=e.args[0])
        result = retrying(what, call)
    if not result.get("ok"):
        raise RuntimeError(f"searchAction failed for {what}: {result.get('errorCode')}")
    return result["data"]


def plan(cat, handle):
    """(exact product count, [price slices of <= SLICE products]) for one category."""
    ranges = ([{"to": EDGES[0]}] + [{"from": a, "to": b} for a, b in zip(EDGES, EDGES[1:])]
              + [{"from": EDGES[-1]}])
    data = search(cat, handle, size=0, aggs={"Quantity": STOCK_AGG, "RetailRate_brackets": {
        "range": {"script": {"source": PRICE_SCRIPT, "lang": "painless"}, "keyed": False,
                  "ranges": ranges}}})
    aggs = data["aggregations"]
    total = sum(b["doc_count"] for b in aggs["Quantity"]["buckets"])
    bands = aggs["RetailRate_brackets"]["buckets"]
    banded = sum(b["doc_count"] for b in bands)
    if banded != total:
        print(f"WARN category {cat}: price bands hold {banded} of {total} products", flush=True)
    slices, lo, count = [], None, 0
    for b in bands:
        if b["doc_count"] > SLICE:
            raise RuntimeError(f"category {cat}: {b['doc_count']} products priced {b['key']}, "
                               f"more than one {SLICE}-product slice; add finer EDGES")
        if count and count + b["doc_count"] > SLICE:
            slices.append((lo, b["from"], count))       # cut at this band's lower edge
            lo, count = b["from"], 0
        count += b["doc_count"]
    slices.append((lo, None, count))
    return total, slices


def fetch_pages(pool, jobs):
    """Fetch [(slice, from)] pages, WORKERS at a time. Returns {slice: [reported total,
    {product key: _source}]}; a slice whose total is not exact and under WINDOW raises."""
    def one(job):
        (cat, handle, lo, hi), frm = job
        return job, search(cat, handle, frm, PAGE, (lo, hi))

    out = {}
    for done, ((sl, frm), data) in enumerate(pool.map(one, jobs), 1):
        if done % 25 == 0 or done == len(jobs):
            print(f"  {done} of {len(jobs)} pages", flush=True)
        total = data["total"]
        if total["relation"] != "eq" or total["value"] >= WINDOW:
            raise RuntimeError(f"category {sl[0]} price {sl[2]}..{sl[3]}: {total} products, "
                               "too many for one slice")
        entry = out.setdefault(sl, [0, {}])
        entry[0] = max(entry[0], total["value"])
        for h in data["hits"]:
            src = dict(h.get("_source") or {}, _id=h["_id"])
            entry[1][src.get("identifier") or h["_id"]] = src
    return out


def fetch_slices(pool, slices):
    """{slice: [reported total, {product key: _source}]} for [(slice, planned count)]: the planned
    pages first (all slices in one queue), then any page the reported totals add, then one more
    try for a slice that came back short (products moving between pages mid-run)."""
    got = fetch_pages(pool, [(sl, f) for sl, n in slices for f in range(0, max(n, 1), PAGE)])
    planned = {sl: max(n, 1) for sl, n in slices}
    extra = [(sl, f) for sl, (reported, _) in got.items()
             for f in range(0, reported, PAGE) if f >= planned[sl]]
    for sl, (reported, found) in fetch_pages(pool, extra).items():
        got[sl][1].update(found)
    short = [sl for sl, (reported, found) in got.items() if len(found) < reported]
    for sl in short:
        print(f"WARN category {sl[0]} price {sl[2]}..{sl[3]}: {len(got[sl][1])} of {got[sl][0]} "
              "products, fetching the slice again", flush=True)
    again = fetch_pages(pool, [(sl, f) for sl in short for f in range(0, got[sl][0], PAGE)])
    for sl, (reported, found) in again.items():
        got[sl][1].update(found)
        got[sl][0] = reported
    return got


def leaf_category(paths):
    if isinstance(paths, str):
        paths = [paths]
    nodes = max(((p or "").split("||") for p in paths or []), key=len, default=[""])
    return nodes[-1].strip()


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def build_row(src, scraped_at):
    price = num(src.get("PCNationRate")) or None          # $0 = no real price
    retail = num(src.get("RetailRate"))
    compare = retail if price and retail and retail > price else None
    available = src.get("StockStatus") is True
    qty = src.get("Quantity")
    qty = int(qty) if isinstance(qty, (int, float)) else None
    if qty == 0 and available:
        qty = None                                       # shown in stock: 0 is not a real count
    sku = src.get("identifier") or ""
    key = (src.get("pcn_url_key") or "").strip().strip("/")
    image = (src.get("base_image") or "").strip().lstrip("/")      # "a/9/2/2/<hash>_<id>.jpg"
    if image and not image.startswith("http"):
        image = IMAGE_BASE + image
    highlights = src.get("product_highlights")
    if not isinstance(highlights, str) or FLIGHT_REF_RE.fullmatch(highlights):
        highlights = None             # missing, or a long text left in another flight row
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": sku or src.get("_id"),
        "sku": sku,
        "part_number": (src.get("mpn") or "").strip(),
        "title": (src.get("name") or "").strip(),
        "brand": (src.get("BrandName") or "").strip(),
        "category": leaf_category(src.get("category_text")),
        "condition": (src.get("Condition") or "").strip(),
        "price": price,
        "compare_at_price": compare,
        "discount_pct": round((compare - price) / compare * 100, 1) if compare else None,
        "available": available,
        "stock_qty": qty,
        "image_url": image or None,
        "description": schema.plain_text(highlights),
        "url": f"{BASE}/{key}" if key else "",
    })


def scrape(limit, scraped_at):
    action_id()
    by_id = {}
    if limit:                                    # test run: first page of the first categories
        for cat, handle in ROOTS:
            for h in search(cat, handle, 0, min(limit, PAGE))["hits"]:
                src = dict(h.get("_source") or {}, _id=h["_id"])
                by_id[src.get("identifier") or h["_id"]] = src
            if len(by_id) >= limit:
                break
        items = list(by_id.values())[:limit]
        return [build_row(s, scraped_at) for s in items], None

    listed, totals, slices = 0, {}, []
    for cat, handle in ROOTS:
        total, cuts = plan(cat, handle)
        listed += total
        totals[cat] = total
        slices += [((cat, handle, lo, hi), n) for lo, hi, n in cuts]
        print(f"category {cat} /{handle}: {total} products in {len(cuts)} price slices "
              f"{[(lo, hi, n) for lo, hi, n in cuts]}", flush=True)
    print(f"{listed} products listed, {len(slices)} slices, ~{sum(-(-n // PAGE) for _, n in slices)}"
          f" pages", flush=True)
    with ThreadPoolExecutor(WORKERS) as pool:
        got = fetch_slices(pool, slices)

    by_id, roots_of = {}, {}
    for (cat, handle, lo, hi), (reported, found) in got.items():
        by_id.update(found)
        for pid in found:
            roots_of.setdefault(pid, set()).add(cat)
    for cat, total in totals.items():
        mine = sum(1 for cats in roots_of.values() if cat in cats)
        print(f"{'WARN ' if mine < total else ''}category {cat}: {mine} of {total} products",
              flush=True)
    shared = sum(1 for cats in roots_of.values() if len(cats) > 1)
    print(f"{len(by_id)} unique products, {shared} in more than one category", flush=True)
    return [build_row(s, scraped_at) for s in by_id.values()], listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
