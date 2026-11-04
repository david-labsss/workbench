"""Scrape usithardware.com (Next.js storefront on Medusa, 438k+ parts): every run's full snapshot
appended to Postgres.

Its Medusa store API (api.usithardware.com/store) needs a publishable key the storefront keeps
on its server. The store listing (/store) renders 24 parts and loads the rest of its 120-part
page with the storefront's own "loadMoreProducts" server action (POST /store with a Next-Action
header; arguments: the listing's filters, offset, limit). That action takes any offset and up
to 1,000 parts a call. Its id changes when the site is rebuilt, so it is read from the listing's
scripts every run.

The listing shows at most 200,000 parts for any one filter, newest first, and then says "of
200,000" whatever the real number is. So the catalogue is read one price range at a time, each
holding fewer than that (the page's minPrice and maxPrice give the same range as the action's),
and then one brand at a time. Each finds what the other can't: quote-only parts with no price
record are in no range (13,000+ on 2026-11-04), and a brand is only known from the parts already
read. Reading twice also fills what a read misses when the list changes under it.

The list's prices are a copy, not the live ones. Its records have few fields and made-up variant
ids ("<product id>-v"), the store updates them in batches, and a price update can miss them: on
2026-11-04 about 1 in 10 list prices (and of the Google feed's, which is no better) was behind the
product page, the price a buyer pays. There is no faster source of live prices to read 400k+ parts
from, so the dashboard's QA checks saved prices against the pages, and ops/health_check.py flags a
site whose QA keeps finding other prices while its runs log no price change.

Each part gives its Medusa product id, handle, title, part number, brand, condition, tags (type,
interface, form factor, capacity) and, unless it is quote-only ("Request quote", no price), one
variant with its price. The store shows no stock, so "available" and stock_qty stay blank. The
total to check against is the sum of the ranges' "of N" counts, plus the parts only the brands
have; the run also has to reach most of the last run's parts (min_of_previous below), since the
listing's own count can't show a catalogue shrinking.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import json
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "usithardware"
BASE = "https://usithardware.com"
BATCH = 1000                  # parts per call (the most the action returns)
WORKERS = 4
# The price ranges to read, split further while one still reaches the listing's cap. The edges fall
# between cents, so a part is in exactly one range and their counts add up to the whole catalogue.
EDGES = [0, 20.005, 40.005, 60.005, 80.005, 100.005, 150.005, 200.005, 300.005, 500.005, 1000.005,
         3000.005, None]
TOTAL_RE = re.compile(r"Showing\s+[\d,]+\s*-\s*[\d,]+\s+of\s+([\d,]+)")
SCRIPT_RE = re.compile(r'src="(/_next/static/chunks/[^"]+\.js)"')
ACTION_RE = re.compile(r'createServerReference\)\("([0-9a-f]{40,})"[^)]*"loadMoreProducts"\)')
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b", re.I)
# The listing's filters as the page passes them ("$undefined" is how React sends undefined)
PARAMS = {"countryCode": "us", "categoryId": "$undefined", "extraCategoryIds": "$undefined",
          "q": "$undefined", "facetValues": [], "minPrice": "$undefined",
          "maxPrice": "$undefined", "sort": "created_at"}


def fetch(method, url, **kwargs):
    """A reply from the store. A connection dropped mid-reply is tried again, which the session's
    own retries don't cover: one dropped reply in a run's hundreds of calls must not lose the run."""
    for wait in (5, 20, 60):
        try:
            r = http.session().request(method, url, **kwargs)
            r.raise_for_status()
            return r
        except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            print(f"retrying in {wait}s: {type(e).__name__}", flush=True)
            time.sleep(wait)
    r = http.session().request(method, url, **kwargs)
    r.raise_for_status()
    return r


def listing():
    """(store total, loadMoreProducts action id) from the listing page and its scripts."""
    r = fetch("GET", f"{BASE}/store", timeout=60)
    m = TOTAL_RE.search(BeautifulSoup(r.text, "html.parser").get_text(" "))
    total = int(m.group(1).replace(",", "")) if m else 0
    for src in dict.fromkeys(SCRIPT_RE.findall(r.text)):
        js = http.session().get(urljoin(BASE, src), timeout=60)
        found = ACTION_RE.search(js.text) if js.ok else None
        if found:
            return total, found.group(1)
    raise RuntimeError("loadMoreProducts not found in the listing's scripts")


def range_count(action, low, high):
    """How many parts the listing page says it has in a price range (its cap means at least that)."""
    query = {"minPrice": low} if high is None else {"minPrice": low, "maxPrice": high}
    r = fetch("GET", f"{BASE}/store", params=query, timeout=60)
    m = TOTAL_RE.search(BeautifulSoup(r.text, "html.parser").get_text(" "))
    if m:
        return int(m.group(1).replace(",", ""))
    # An empty range's page has no count; any other page without one must not read as empty
    if load(action, 0, 1, low, high):
        raise RuntimeError(f"no count on the listing page for prices {low} to {high}")
    return 0


def price_ranges(action, cap):
    """(low, high, count) price ranges that together hold the whole catalogue, each under cap."""
    todo = list(zip(EDGES, EDGES[1:]))
    done = []
    while todo:
        low, high = todo.pop()
        count = range_count(action, low, high)
        if count < cap:
            done.append((low, high, count))
            continue
        top = low * 2 if high is None else high
        middle = round(math.floor((low + top) * 50) / 100 + 0.005, 3)
        if not low < middle < top:
            raise RuntimeError(f"{cap} or more parts priced {low} to {high}")
        todo += [(low, middle), (middle, high)]
    return done


def load(action, offset, limit=BATCH, low=None, high=None, brand=None):
    """Parts offset .. offset+limit of the listing, or of one price range or brand of it, as the
    action returns them."""
    params = dict(PARAMS)
    if brand:
        params["facetValues"] = [f"brand:{brand}"]
    if low is not None:
        params["minPrice"] = low
    if high is not None:
        params["maxPrice"] = high
    r = fetch("POST", f"{BASE}/store", data=json.dumps([params, offset, limit]),
              headers={"Next-Action": action, "Accept": "text/x-component",
                       "Content-Type": "text/plain;charset=UTF-8"}, timeout=120)
    # The reply is React's wire format: line "1:" holds the action's return value
    for line in r.text.splitlines():
        if line.startswith("1:"):
            return json.loads(line[2:])
    raise RuntimeError(f"no parts in the reply at offset {offset}: {r.text[:200]}")


def build_row(p, scraped_at):
    meta = p.get("metadata") or {}
    tags = {}
    for t in p.get("tags") or []:
        name, _, value = (t.get("value") or "").partition(":")
        tags.setdefault(name, value)
    variant = (p.get("variants") or [{}])[0]
    amounts = variant.get("calculated_price") or {}
    price = None if meta.get("quote_only") else amounts.get("calculated_amount")
    original = amounts.get("original_amount")
    title = re.sub(r"\s+", " ", p.get("title") or "").strip()
    cap = CAPACITY_RE.search(title)
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": p["id"],
        "sku": variant.get("sku") or meta.get("part_number") or "",
        "part_number": meta.get("part_number") or "",
        "title": title,
        "brand": meta.get("brand") or "",
        "category": tags.get("type", ""),
        "condition": meta.get("condition") or "",
        "capacity": tags.get("cap") or ((cap.group(1) + cap.group(2).upper()) if cap else ""),
        "interface": tags.get("iface", ""),
        "form_factor": tags.get("form", ""),
        "price": price,                   # None = quote only ("Request quote")
        "compare_at_price": original if price is not None and original and original > price else None,
        "available": None,                # the store doesn't show stock
        "stock_qty": None,
        "image_url": p.get("thumbnail") or None,   # absolute Medusa cdn url; None for some parts
        # loadMoreProducts returns no description (only metadata: brand/condition/part_number/specs)
        "description": None,
        "url": f"{BASE}/products/{p['handle']}",
    })


def read_all(action, calls):
    """The parts of (offset, low, high) calls, by product id."""
    by_id = {}
    with ThreadPoolExecutor(WORKERS) as pool:
        results = pool.map(lambda c: load(action, c[0], BATCH, c[1], c[2]), calls)
        for i, parts in enumerate(results, 1):
            for p in parts:
                by_id[p["id"]] = p
            if i % 100 == 0 or i == len(calls):
                print(f"{i}/{len(calls)} calls, {len(by_id)} unique parts", flush=True)
    return by_id


def read_brands(action, brands, cap):
    """Every part of these brands, by product id."""
    def of_brand(brand):
        found, offset = {}, 0
        while offset < cap:
            parts = load(action, offset, brand=brand)
            found |= {p["id"]: p for p in parts}
            offset += BATCH
            if len(parts) < BATCH:
                return found
        # Its priced parts are in the ranges; only its parts with no price record can be lost
        print(f"::warning::{brand} reaches the listing's cap of {cap}: some parts may be missing",
              flush=True)
        return found

    by_id = {}
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, found in enumerate(pool.map(of_brand, brands), 1):
            by_id |= found
            if i % 200 == 0 or i == len(brands):
                print(f"{i}/{len(brands)} brands, {len(by_id)} unique parts", flush=True)
    return by_id


def scrape(limit, scraped_at):
    total, action = listing()
    print(f"its listing says {total} parts; loadMoreProducts = {action}", flush=True)
    if limit:  # a test run only needs the newest parts
        newest = [(offset, None, None) for offset in range(0, min(limit, total), BATCH)]
        items = list(read_all(action, newest).values())[:limit]
        return [build_row(p, scraped_at) for p in items], total
    if not total:
        raise RuntimeError("no count on the listing page")
    # The unfiltered listing's own number is its cap when the catalogue is bigger than that, so a
    # lower cap can't pass for a complete range
    ranges = price_ranges(action, total)
    in_ranges = sum(count for _, _, count in ranges)
    print(f"{in_ranges} parts in {len(ranges)} price ranges", flush=True)
    by_id = read_all(action, [(offset, low, high) for low, high, count in ranges
                              for offset in range(0, count, BATCH)])
    read = len(by_id)
    brands = sorted({(p.get("metadata") or {}).get("brand") for p in by_id.values()} - {None, ""})
    by_id |= read_brands(action, brands, total)
    listed = in_ranges + len(by_id) - read
    print(f"read {read} of them; {len(by_id) - read} more parts only under {len(brands)} brands",
          flush=True)
    if listed < total:
        raise RuntimeError(f"found {listed} parts in all, but the listing alone says {total}")
    return [build_row(p, scraped_at) for p in by_id.values()], listed


if __name__ == "__main__":
    runner.run(SITE, scrape, min_of_previous=0.9)
