"""Scrape staples.com server-related categories: every run's full snapshot appended to Postgres.

staples.com itself blocks non-US / bot traffic, but its Bloomreach Discovery search API
(core.dxpapi.com) is open and answers from anywhere, so the data is read from there. We fetch
each server-related category by its Bloomreach "class id", 200 products per request (the API
max), paged by the `start` offset (the API refuses an offset past 10,000 - none of these
categories are that big). The categories overlap, so products are deduped by pid.

is_sellable (in stock / not) is a filter, not a returned field, so each category is fetched
twice: is_sellable "true" (available) and "false" (unavailable). The in-stock pass runs first,
so a product listed in more than one category keeps its available=True and first category.

Fields the API returns: pid, title, brand, price, sale_price, url. There is no condition and no
stock count. The manufacturer part number (MPN) is often the bracketed token at the end of the
title, e.g. "... AC Adapter (170717-000)"; it is read from there when present. pid is Staples'
own item number and also the URL slug (/product_<pid>).

NOTE: the Bloomreach API price can differ slightly from the live staples.com product page;
prices should be spot-checked from a US connection.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
import time

from common import http, runner, schema

SITE = "staples"
API = "https://core.dxpapi.com/api/v1/core/"
ACCOUNT_ID = "5073"
DOMAIN_KEY = "staples_com"
ROWS = 200           # the API returns at most 200 products per request
OFFSET_CAP = 10000   # the API refuses a `start` offset past this
PAGE_DELAY = 0.3     # polite pause between requests

# Bloomreach category class id -> readable category name (server-related, from recon 2026-10-19)
CATEGORIES = {
    "CL165750": "Ethernet Switches",
    "CL165822": "Networking Accessories",
    "CL168260": "Transceivers",
    "CL163882": "Solid State Drives",
    "CL167422": "Network Adapters",
    "CL167348": "CPUs / Processors",
    "CL215749": "Graphics Cards (GPUs)",
    "CL165749": "Routers",
    "CL142707": "Memory",
    "CL207763": "Servers",
    "CL211163": "Internal Hard Drives",
    "CL167352": "Motherboards",
    "CL166252": "Firewalls",
    "CL141992": "Power Supplies",
    "CL140474": "Computer Cards",
    "CL167414": "RAID Controllers",
}

# MPN is often the last parenthesised token of the title, e.g. "... Flash Drive (PE246945)"
MPN_RE = re.compile(r"\(([A-Za-z0-9][A-Za-z0-9/.\- ]{2,})\)\s*$")


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def search(class_id, sellable, start):
    """One API page (a category filtered by is_sellable). Any error raises, so a failed request
    is never mistaken for the end of the category."""
    params = {
        "account_id": ACCOUNT_ID, "domain_key": DOMAIN_KEY,
        "request_type": "search", "search_type": "category",
        "q": class_id, "rows": ROWS, "start": start,
        "sort": "price asc",
        "fl": "pid,title,brand,price,sale_price,url",
        "fq": f'is_sellable:"{sellable}"',
        "url": "https://www.staples.com/",
        "_br_uid_2": "uid=7797686432023:v=11.5:ts=1428617911187:hc=55",
        "request_id": "1",
    }
    r = http.session().get(API, params=params, timeout=30)
    r.raise_for_status()
    resp = r.json().get("response") or {}
    # Bloomreach returns the products under "docs" or "products" depending on the account
    docs = resp.get("docs")
    if docs is None:
        docs = resp.get("products") or []
    return docs, int(resp.get("numFound") or 0)


def fetch_category(class_id, sellable):
    """Every product in one category for the given is_sellable flag, paged by offset."""
    docs, start = [], 0
    while True:
        batch, found = search(class_id, sellable, start)
        docs.extend(batch)
        start += ROWS
        if not batch or start >= found or start >= OFFSET_CAP:
            if found > OFFSET_CAP:
                print(f"WARN {class_id} {sellable}: {found} products > offset cap {OFFSET_CAP}; "
                      f"only {len(docs)} reachable", flush=True)
            return docs, found
        time.sleep(PAGE_DELAY)


def build_row(doc, category, available, scraped_at):
    title = re.sub(r"\s+", " ", doc.get("title") or "").strip()
    m = MPN_RE.search(title)
    mpn = m.group(1).strip() if m else ""
    pid = str(doc.get("pid") or "")
    sale = to_float(doc.get("sale_price"))
    listed = to_float(doc.get("price"))
    price = sale if sale is not None else listed
    url = (doc.get("url") or f"https://www.staples.com/product_{pid}").replace("http://", "https://")
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": pid,
        "sku": pid,                 # pid is Staples' own item number
        "part_number": mpn,
        "model_number": mpn,
        "title": title,
        "brand": doc.get("brand") or "",
        "category": category,
        "price": price,
        "compare_at_price": listed if (listed and price and listed > price) else None,
        "available": available,
        "stock_qty": None,          # not exposed by the search API
        "url": url,
    })


def scrape(limit, scraped_at):
    by_id = {}
    # in-stock pass first, so a product in two categories keeps available=True and its first category
    for sellable, available in (("true", True), ("false", False)):
        for class_id, category in CATEGORIES.items():
            docs, found = fetch_category(class_id, sellable)
            new = 0
            for doc in docs:
                pid = str(doc.get("pid") or "")
                if not pid or pid in by_id:
                    continue
                by_id[pid] = build_row(doc, category, available, scraped_at)
                new += 1
            print(f"{class_id} {category!r} sellable={sellable}: {found} found, "
                  f"{len(docs)} fetched, {new} new ({len(by_id)} unique total)", flush=True)
            if limit and len(by_id) >= limit:
                return list(by_id.values())[:limit], None
    # No single catalogue total to compare against; each category is paged to completion above
    return list(by_id.values()), None


if __name__ == "__main__":
    runner.run(SITE, scrape)
