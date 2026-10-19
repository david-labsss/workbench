"""Scrape usithardware.com (Next.js storefront on Medusa, ~151k parts): every run's full snapshot
appended to Postgres.

Its Medusa store API (api.usithardware.com/store) needs a publishable key the storefront keeps
on its server. The store listing (/store) renders 24 parts and loads the rest of its 120-part
page with the storefront's own "loadMoreProducts" server action (POST /store with a Next-Action
header; arguments: the listing's filters, offset, limit). That action takes any offset and up
to 1,000 parts a call, so the whole catalogue (newest first by created_at) is read in ~150 calls.
Its id changes when the site is rebuilt, so it is read from the listing's scripts every run.

Each part gives its Medusa product id, handle, title, part number, brand, condition, tags (type,
interface, form factor, capacity) and, unless it is quote-only ("Request quote", no price), one
variant with its price. The store shows no stock, so "available" and stock_qty stay blank. The
total to check against is the listing's own "of N" count.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import json
import re
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "usithardware"
BASE = "https://usithardware.com"
BATCH = 1000                  # parts per call (the most the action returns)
WORKERS = 4
TOTAL_RE = re.compile(r"Showing\s+[\d,]+\s*-\s*[\d,]+\s+of\s+([\d,]+)")
SCRIPT_RE = re.compile(r'src="(/_next/static/chunks/[^"]+\.js)"')
ACTION_RE = re.compile(r'createServerReference\)\("([0-9a-f]{40,})"[^)]*"loadMoreProducts"\)')
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b", re.I)
# The listing's filters as the page passes them ("$undefined" is how React sends undefined)
PARAMS = {"countryCode": "us", "categoryId": "$undefined", "extraCategoryIds": "$undefined",
          "q": "$undefined", "facetValues": [], "minPrice": "$undefined",
          "maxPrice": "$undefined", "sort": "created_at"}


def listing():
    """(store total, loadMoreProducts action id) from the listing page and its scripts."""
    r = http.session().get(f"{BASE}/store", timeout=60)
    r.raise_for_status()
    m = TOTAL_RE.search(BeautifulSoup(r.text, "html.parser").get_text(" "))
    total = int(m.group(1).replace(",", "")) if m else 0
    for src in dict.fromkeys(SCRIPT_RE.findall(r.text)):
        js = http.session().get(urljoin(BASE, src), timeout=60)
        found = ACTION_RE.search(js.text) if js.ok else None
        if found:
            return total, found.group(1)
    raise RuntimeError("loadMoreProducts not found in the listing's scripts")


def load(action, offset, limit=BATCH):
    """Parts offset .. offset+limit of the whole listing, as the action returns them."""
    r = http.session().post(f"{BASE}/store", data=json.dumps([PARAMS, offset, limit]),
                            headers={"Next-Action": action, "Accept": "text/x-component",
                                     "Content-Type": "text/plain;charset=UTF-8"}, timeout=120)
    r.raise_for_status()
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
        "url": f"{BASE}/products/{p['handle']}",
    })


def scrape(limit, scraped_at):
    total, action = listing()
    print(f"store lists {total} parts; loadMoreProducts = {action}", flush=True)
    wanted = min(limit, total) if limit else total
    offsets = range(0, wanted, BATCH)
    by_id = {}
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, parts in enumerate(pool.map(lambda o: load(action, o), offsets), 1):
            for p in parts:
                by_id[p["id"]] = p
            if i % 25 == 0 or i == len(offsets):
                print(f"{i}/{len(offsets)} calls, {len(by_id)} unique parts", flush=True)
    items = list(by_id.values())[:limit] if limit else list(by_id.values())
    return [build_row(p, scraped_at) for p in items], total


if __name__ == "__main__":
    runner.run(SITE, scrape)
