"""Scrape bulkdevices.com (Magento 2, IT parts / printers / peripherals): every run's full snapshot
of its ~147k catalog-visible products appended to Postgres.

The store's pages, sitemap, robots.txt and GraphQL sit behind a Cloudflare challenge, but
Magento's anonymous REST API is not challenged: /rest/V1/products-render-info (the product cards
Magento renders for widgets) answers without a key or cookie, in USD, 1,000 products per call.
robots.txt (User-agent: *) does not disallow /rest/ and asks for Crawl-delay: 5, so there is
only ever ONE request in flight and a 5 s pause after each (~150 calls of ~12 s + the pause ->
~44 min, measured 2026-10-20).

Paging is by id (keyset): entity_id > last id seen, sorted by entity_id, 1,000 per call, until a
call comes back empty. A product that disappears mid-run can't shift the pages, and new products
(higher ids) land at the end. At the start one call reads the highest entity_id; an empty page
before reaching it is re-checked (that product may have gone) and otherwise raises, so a bad
fetch never looks like the end of the catalogue.

Coverage - IMPORTANT: Magento's products-render-info only returns products whose visibility is
"Catalog, Search" (its ProductVisibilityCondition is hard-coded). The store's search index
(/rest/V1/search, also anonymous) holds ~402k products: ~147k "Catalog, Search" (= what this
scraper gets) and ~255k "Search" only (not in the category menus; found through the site search
or a direct link). Those search-only products can't be read for free (GraphQL and the pages are
behind Cloudflare; /V1/search returns ids only). Each run logs both counts. listed = the
"Catalog, Search" count from the search index (catalog_view_container, root category 2,
visibility 4), so the runner's 98% check compares like with like.

Fields:
  id (Magento entity id)                    -> product_id
  name ("012970-001 - HP 64MB 40-Bit DDR Battery Backed-Write Cache Memory", the page's h1)
                                            -> title; part number = text before " - " -> sku,
                                               part_number (the page's "SKU" and "Manufacturer
                                               Part No." show the same); brand = first word after
                                               " - " (a few two-word brands are kept whole)
  price_info.final_price                    -> price (USD). 0 = no price: the page shows "Request
      For Quotation" / "Request a Quote" instead of a price (seen on live pages 2026-10-20)
      -> None. regular_price above final_price (a special price) -> compare_at_price.
      Magento MAP (msrp.is_applicable) would hide the price on the page -> None + flagged
      (0 of 2,000 sampled products on 2026-10-20).
  is_salable                                -> available (the page's "Availability In stock").
      No stock count is published -> stock_qty None.
  url, images (first non-placeholder)       -> url, image_url
Not in this API: category, condition (the page shows e.g. "Condition Refurbished"), description
and specs -> blank (they would need the product pages, which are behind Cloudflare). Condition
is set only when the title says so (~100 "Refurbished"); capacity is read from the title for
drives and memory. is_salable was true for all 147,420 products on 2026-10-20 (Magento leaves
out-of-stock products out of this list, or the store keeps everything salable), so available
carries little information here.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import html
import re
import time

import requests

from common import http, runner, schema

SITE = "bulkdevices"
BASE = "https://bulkdevices.com"
RENDER = BASE + "/rest/V1/products-render-info"
SEARCH = BASE + "/rest/V1/search"
PAGE_SIZE = 1000          # accepted by the API; ~280 KB with the fields below
CRAWL_DELAY = 5           # robots.txt Crawl-delay; one request at a time
TRIES = 3                 # attempts for a stalled / cut-off download
RETRY_PAUSE = 30
ROOT_CATEGORY = "2"       # Magento's default root category ("Default Category")
FIELDS = ("items[id,name,url,type,is_salable,images[url],price_info[final_price,regular_price,"
          "special_price,minimal_price,extension_attributes[msrp[is_applicable]]]]")

TWO_WORD_BRANDS = {"Western Digital", "Hewlett Packard", "Super Micro", "Allied Telesis",
                   "Extreme Networks", "Juniper Networks", "Palo Alto", "Silicon Power",
                   "Smart Modular", "Tandberg Data", "Black Box", "SK Hynix", "Hitachi Vantara",
                   "American Power", "Kingston Technology", "Quantum Corporation", "Mean Well"}
# a size or count where the brand should be ("500GB - ...", "2-Port", "2.5-inch")
NOT_BRAND_RE = re.compile(r"^\d+(?:\.\d+)?\s?-?(?:TB|GB|MB|KB|port|ports|inch|in)$", re.I)
STORAGE_OR_MEMORY = re.compile(r"hard drive|\bhdd\b|\bssd\b|solid state|memory|\bdimm\b|\bram\b|"
                               r"flash|nvme", re.I)
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s?(TB|GB)\b")   # upper case only ("6Gb/s" = speed)
# condition is only in ~100 titles ("... (Refurbished / Grade-A)"); "Mixed Used" is an SSD class
CONDITION_RES = [(re.compile(r"\brefurbished\b", re.I), "Refurbished"),
                 (re.compile(r"\b(?:renewed|recertified)\b", re.I), "Refurbished"),
                 (re.compile(r"\bopen[- ]box\b", re.I), "Open Box")]


# --- fetching ----------------------------------------------------------------
def get(url, params):
    """One API call (JSON), then the robots.txt crawl delay. http.session() retries 429 / 5xx;
    this retries a download that stalls or is cut off part-way (requests does not). The last
    failure raises."""
    for attempt in range(1, TRIES + 1):
        try:
            r = http.session().get(url, params=params, timeout=180)
            r.raise_for_status()
            data = r.json()
            time.sleep(CRAWL_DELAY)
            return data
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError, ValueError) as e:
            if attempt == TRIES:
                raise
            print(f"WARN {e.__class__.__name__} ({str(e)[:120]}), retrying in {RETRY_PAUSE}s",
                  flush=True)
            time.sleep(RETRY_PAUSE)


def search_count(visibility):
    """Products of the given visibility in the store's search index (4 = Catalog, Search;
    3 = Search only), via the anonymous /V1/search with the category page's request."""
    data = get(SEARCH, {
        "searchCriteria[requestName]": "catalog_view_container",
        "searchCriteria[filterGroups][0][filters][0][field]": "category_ids",
        "searchCriteria[filterGroups][0][filters][0][value]": ROOT_CATEGORY,
        "searchCriteria[filterGroups][0][filters][1][field]": "visibility",
        "searchCriteria[filterGroups][0][filters][1][value]": visibility,
        "searchCriteria[pageSize]": 1,
    })
    return int(data.get("total_count") or 0)


def render_page(after_id=0, size=PAGE_SIZE, descending=False):
    """Up to `size` product cards with entity_id > after_id, by entity_id."""
    params = {
        "storeId": 1, "currencyCode": "USD",
        "searchCriteria[pageSize]": size, "searchCriteria[currentPage]": 1,
        "searchCriteria[sortOrders][0][field]": "entity_id",
        "searchCriteria[sortOrders][0][direction]": "DESC" if descending else "ASC",
        "fields": FIELDS,
    }
    if after_id:
        params.update({
            "searchCriteria[filterGroups][0][filters][0][field]": "entity_id",
            "searchCriteria[filterGroups][0][filters][0][value]": after_id,
            "searchCriteria[filterGroups][0][filters][0][conditionType]": "gt",
        })
    return get(RENDER, params).get("items") or []     # no match -> {"items": null}


def max_id():
    items = render_page(size=1, descending=True)
    return int(items[0]["id"]) if items else 0


def read_catalogue(limit):
    """{id: item} for every product card, paging by id. Raises when the list ends before the
    highest id known to exist (unless that product is gone by then)."""
    top = max_id()
    by_id, last, pages, started = {}, 0, 0, time.time()
    while True:
        size = min(PAGE_SIZE, limit - len(by_id)) if limit else PAGE_SIZE
        items = render_page(last, size)
        for attempt in range(1, TRIES + 1):
            if items or limit or last >= top:
                break
            now_top = max_id()
            if now_top <= last:            # the highest products were removed mid-run
                break
            if attempt == TRIES:
                raise RuntimeError(f"empty page after id {last}, but ids up to {now_top} exist; "
                                   "not saving a partial snapshot")
            print(f"WARN empty page after id {last} (ids up to {now_top} exist), reading it "
                  f"again in {RETRY_PAUSE}s", flush=True)
            time.sleep(RETRY_PAUSE)
            items = render_page(last, size)
        if not items:
            break
        for it in items:
            by_id[int(it["id"])] = it
        last = max(int(it["id"]) for it in items)
        pages += 1
        if pages % 10 == 0 or limit:
            print(f"{len(by_id)} products (id {last} of {top}) in {time.time() - started:.0f}s",
                  flush=True)
        if limit and len(by_id) >= limit:
            break
    return by_id


# --- mapping -----------------------------------------------------------------
def clean(v):
    return re.sub(r"\s+", " ", html.unescape(str(v if v is not None else ""))).strip()


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def split_name(name):
    """(part number, brand) from "MPN - Brand description". ~2% of names have no " - " part
    number ("Dell 10 Gigabit AT2 Dual Port Server Adapter"): part number blank (the URL slug is
    not reliable for it), brand = the first word."""
    part, sep, rest = name.partition(" - ")
    if not sep:
        part, rest = "", name
    words = rest.split()
    if len(words) >= 2 and " ".join(words[:2]) in TWO_WORD_BRANDS:
        return part.strip(), " ".join(words[:2])
    brand = words[0] if words else ""
    if not re.search(r"[A-Za-z]{2}", brand) or NOT_BRAND_RE.match(brand):
        brand = ""
    return part.strip(), brand


def image_of(item):
    for img in item.get("images") or []:
        url = clean(img.get("url"))
        if url and "/placeholder/" not in url:
            return url
    return None


def build_row(item, scraped_at):
    title = clean(item.get("name"))
    part, brand = split_name(title)
    info = item.get("price_info") or {}
    final = to_float(info.get("final_price"))
    price = final if final and final > 0 else None          # 0 = "Request a Quote"
    msrp = (info.get("extension_attributes") or {}).get("msrp") or {}
    hidden = str(msrp.get("is_applicable") or "").lower() in ("1", "true")
    if hidden:
        price = None        # MAP: the page hides the price ("see price in cart")
    regular = to_float(info.get("regular_price"))
    compare = regular if price and regular and regular > price else None
    flags = []
    if hidden:
        flags.append("Price hidden (MAP), shown in cart")
    elif price is None:
        flags.append("Request a Quote")
    if item.get("type") and item["type"] != "simple":
        flags.append(f"Product type: {item['type']}")
    capacity = ""
    if STORAGE_OR_MEMORY.search(title):
        m = CAPACITY_RE.search(title.partition(" - ")[2] or title)
        capacity = m.group(1) + m.group(2) if m else ""
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": str(item["id"]),
        "sku": part,
        "part_number": part,
        "title": title,
        "brand": brand,
        "category": "",                 # not in this API
        "condition": next((c for rx, c in CONDITION_RES if rx.search(title)), ""),
        "capacity": capacity,
        "other_specs": "; ".join(flags),
        "price": price,
        "compare_at_price": compare,
        "discount_pct": round((compare - price) / compare * 100, 1) if compare else None,
        "available": str(item.get("is_salable")) in ("1", "True", "true"),
        "stock_qty": None,              # not published
        "image_url": image_of(item),
        "description": None,            # not in this API (product pages are behind Cloudflare)
        "url": clean(item.get("url")),
    })


def scrape(limit, scraped_at):
    listed = search_count(4)
    search_only = search_count(3)
    print(f"search index: {listed} catalog-visible products (scraped here), {search_only} "
          "search-only products (not reachable through this API)", flush=True)
    items = read_catalogue(limit)
    rows = [build_row(it, scraped_at) for _, it in sorted(items.items())]
    return rows, listed or None


if __name__ == "__main__":
    runner.run(SITE, scrape)
