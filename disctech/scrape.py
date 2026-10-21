"""Scrape disctech.com (NetSuite SuiteCommerce, ~17k products): every run's full snapshot appended
to Postgres.

The store's own search API (/api/items, JSON, 100 items per call) returns every field we need:
manufacturer part number, condition, capacity, interface, form factor, price, in stock and the
exact quantity available. One listing can't be paged past 5,000 items ("Search failed"), so
the catalogue is split by product type, and a type with more than 5,000 items by condition,
then by manufacturer. Items are deduped by internalid and the total is checked against the
unfiltered listing's count (a few dozen items have no product type and can't be reached).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import html
import re
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote

from common import http, runner, schema

SITE = "disctech"
BASE = "https://www.disctech.com"
API = BASE + "/api/items"
PAGE = 100
MAX_OFFSET = 5000                 # the API fails past this
WORKERS = 4
SPLIT_BY = ["custitemproducttypebroad", "custitemcondition_basic", "custitemmanufacturer"]

# Field -> label, for other_specs
OTHER_FIELDS = {
    "custitemalt_mpn": "Alt part numbers",
    "custitemcompatibility": "Compatibility",
    "custitemwarranty": "Warranty",
    "custitemram": "Memory/cache",
    "custitembays": "Bays",
    "custitemwattage": "Wattage",
    "custitemdwpd": "DWPD",
    "custitemsector_size": "Sector size",
    "custitemlead_time": "Lead time",
}


def get(params):
    """One call to the items API. It sometimes answers 400 "Search failed" for a page that
    works on a retry, so retry that too before giving up."""
    for attempt in range(5):
        r = http.session().get(API, params={"fieldset": "search", **params}, timeout=60)
        if r.status_code == 400 and attempt < 4:
            time.sleep(3 * (attempt + 1))
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError("unreachable")


def facet_values(filters, facet):
    data = get({**filters, "limit": 1, "include": "facets"})
    for f in data.get("facets", []):
        if f.get("id") == facet:
            # values come URL-encoded ("7.2K-%28SSD...%29"); requests encodes them again
            return [unquote(v["url"]) for v in f.get("values", [])]
    return []


def plan(filters=None, depth=0):
    """[(filters, total)] listings that each have at most MAX_OFFSET items."""
    filters = filters or {}
    total = get({**filters, "limit": 1})["total"]
    if total <= MAX_OFFSET or depth == len(SPLIT_BY):
        if total > MAX_OFFSET:
            print(f"WARN {filters} has {total} items; only {MAX_OFFSET} are reachable", flush=True)
        return [(filters, total)] if total else []
    listings = []
    for v in facet_values(filters, SPLIT_BY[depth]):
        listings += plan({**filters, SPLIT_BY[depth]: v}, depth + 1)
    return listings


def fetch_listing(listing):
    filters, total = listing
    items = []
    for offset in range(0, min(total, MAX_OFFSET), PAGE):
        items += get({**filters, "limit": PAGE, "offset": offset}).get("items", [])
    return items


def clean(v):
    """Text without HTML (some fields hold tables) or runs of whitespace."""
    v = "" if v is None else html.unescape(re.sub(r"<[^>]+>", " ", str(v)))
    v = re.sub(r"\s+", " ", v).strip()
    return "" if v in ("", "- None -") else v


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def first_image(item):
    """The main product image url (absolute). itemimages_detail is usually {group: {urls: [{url}]}},
    but a group can also be a list of such entries (a run crashed on one on 2026-10-21), so the
    first "url" is found whatever the nesting."""
    def urls(node):
        if isinstance(node, dict):
            if isinstance(node.get("url"), str) and node["url"]:
                yield node["url"]
            for v in node.values():
                yield from urls(v)
        elif isinstance(node, list):
            for v in node:
                yield from urls(v)
    return next(urls(item.get("itemimages_detail") or {}), None)


def build_row(item, scraped_at):
    price = to_float((item.get("onlinecustomerprice_detail") or {}).get("onlinecustomerprice")
                     or item.get("onlinecustomerprice"))
    tbw = clean(item.get("custitemssdtbw"))
    other = [f"{label}: {clean(item.get(f))}" for f, label in OTHER_FIELDS.items()
             if clean(item.get(f))]
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": str(item["internalid"]),
        "sku": clean(item.get("itemid")),
        "part_number": clean(item.get("custitemmanufacturer_pn")),
        "title": clean(item.get("storedisplayname2") or item.get("pagetitle")
                       or item.get("displayname")),
        "brand": clean(item.get("custitemmanufacturer")),
        "category": clean(item.get("custitemproducttypebroad")),
        "condition": clean(item.get("custitemcondition") or item.get("custitemcondition_basic")),
        "capacity": clean(item.get("custitemdisk_capacity")),
        "interface": clean(item.get("custiteminterface_speed")
                           or item.get("custiteminterfacetypes")),
        "form_factor": clean(item.get("custitemdisk_size")),
        "spindle_speed": clean(item.get("custitemdisk_rpm")),
        "endurance": f"{tbw} TBW" if tbw else "",
        "other_specs": "; ".join(other),
        "price": price,
        "available": bool(item.get("isinstock")),
        "stock_qty": to_float(item.get("quantityavailable")),
        "image_url": first_image(item),
        "description": schema.plain_text(item.get("storedescription")
                                         or item.get("description")),
        "url": f"{BASE}/{item.get('urlcomponent')}" if item.get("urlcomponent") else "",
    })


def scrape(limit, scraped_at):
    listed = get({"limit": 1})["total"]
    if limit:
        listings = [({}, min(limit, MAX_OFFSET))]
    else:
        listings = plan()
        print(f"{len(listings)} listings, {sum(t for _, t in listings)} items "
              f"(catalogue lists {listed})", flush=True)
    by_id = {}
    with ThreadPoolExecutor(WORKERS) as pool:
        for (filters, total), items in zip(listings, pool.map(fetch_listing, listings)):
            for item in items:
                by_id[item["internalid"]] = item
            print(f"{filters or 'all'}: {len(items)} of {total} (unique so far {len(by_id)})",
                  flush=True)
    items = list(by_id.values())[:limit] if limit else list(by_id.values())
    return [build_row(i, scraped_at) for i in items], listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
