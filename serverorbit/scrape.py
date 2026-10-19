"""Scrape serverorbit.com: every run's full snapshot appended to Postgres.

serverorbit.com is CS-Cart behind Cloudflare, so the HTML is not reachable from a
plain client. But the site's own search box is powered by Searchanise, whose public
Search API (searchserverapi.com) answers with the site's public key and is NOT behind
Cloudflare. It returns the whole in-stock catalogue with price, part number and URL.
No browser, no Cloudflare bypass, no product-page crawl.

The API caps each query at 10,000 items deep, so we page through in price order and use
a price lower-bound as a cursor, deduping by product_id. ~6 buckets * ~40 pages.

Fields the API gives per product: title, price, list_price, quantity, product_code
(manufacturer part number = SKU), link, product_type, discount.

Stock: the search lists ONLY in-stock products, and "quantity" is 1 for all of them (checked
2026-10-09: no listed product has 0 or 2+). It's an in-stock flag, and the store publishes no
exact counts anywhere, so stock_qty stays blank. Stock changes show up as products leaving the
list (out_of_stock) or rejoining it (back_in_stock -> Restocked), see find_changes().
brand / condition / capacity / model are parsed from the (very spec-rich) title.
category is derived from title keywords (the search API does not expose per-product
breadcrumb categories).

Every run's rows are INSERTed into the "serverorbit" table (common/db.py), changes against the
previous full run into the shared prices table (website = serverorbit). Columns:
common/schema.py (the API's list_price is compare_at_price).

Env vars:
  DATABASE_URL                 Postgres URL for the scraper_writer role
  SEARCHANISE_API_KEY          optional, defaults to the site's public key
  MAX_PRODUCTS                 optional, stop after N products (test runs)
  DRY_RUN                      optional, "1" = print sample rows, don't save anything
"""
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common import db, schema

API = "https://searchserverapi.com/getresults"
API_KEY = os.getenv("SEARCHANISE_API_KEY", "1D2R1g8S8q")
PAGE = 250           # max the API allows per request
DEPTH_CAP = 10000    # the API will not page deeper than this per query
PAGE_DELAY = 0.4     # seconds between requests (polite)
USER_AGENT = "serverorbit-scraper/1.0 (price monitoring)"

SITE = "serverorbit"
COLUMNS = schema.PRODUCT_COLUMNS
# Columns read back from the previous run (enough for an out_of_stock prices row)
PREVIOUS_COLUMNS = ["sku", "part_number", "model_number", "title", "brand", "category",
                    "condition", "price", "stock_qty", "available", "url"]

# --- title parsing -----------------------------------------------------------
BRANDS = [
    "HPE", "HP", "Dell", "EMC", "IBM", "Lenovo", "Cisco", "Intel", "AMD", "Juniper",
    "NetApp", "Seagate", "Samsung", "Western Digital", "WD", "Toshiba", "Hitachi",
    "Supermicro", "Fujitsu", "Oracle", "Sun", "Brocade", "Mellanox", "Nvidia", "NVIDIA",
    "Arista", "Fortinet", "Micron", "Kingston", "Crucial", "Hynix", "SanDisk", "QLogic",
    "Emulec", "Emulex", "Broadcom", "Pure Storage", "Quantum", "Aruba", "Nutanix",
]
BRAND_RE = re.compile(r"^(?:cheap\s+)?(" + "|".join(re.escape(b) for b in BRANDS) + r")\b", re.I)
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB|MB)\b", re.I)
RPM_RE = re.compile(r"\b(\d[\d,]*)\s*[- ]?\s*(?:RPM|K\s*RPM)\b", re.I)
IFACE_RE = re.compile(r"\b(SAS|SATA|NVMe|NVME|PCIe|PCI-E|Fibre Channel|FC|SCSI|iSCSI|Ultra320)\b", re.I)
FORM_RE = re.compile(r"\b(2\.5[\"-]?inch|3\.5[\"-]?inch|2\.5\"|3\.5\"|SFF|LFF|M\.2|U\.2)\b", re.I)
SPEED_RE = re.compile(r"\b(\d{1,2}\s?GBPS|\d{1,2}\s?GB/s|6G|12G)\b", re.I)

CONDITIONS = [
    (re.compile(r"\bnew\s+sealed\b", re.I), "New Sealed"),
    (re.compile(r"\bbrand\s+new\b", re.I), "New"),
    (re.compile(r"\bnew\s+open\s+box\b", re.I), "New Open Box"),
    (re.compile(r"\bnew\b", re.I), "New"),
    (re.compile(r"\brenewed\b", re.I), "Renewed"),
    (re.compile(r"\brecertified\b", re.I), "Recertified"),
    (re.compile(r"\brefurbished\b", re.I), "Refurbished"),
    (re.compile(r"\bused\b", re.I), "Used"),
]
# coarse category from title keywords (search API has no per-product breadcrumb)
CATEGORY_RULES = [
    (re.compile(r"\bssd|solid.?state\b", re.I), "Solid State Drives"),
    (re.compile(r"\b(hdd|hard\s*(disk|drive))\b", re.I), "Hard Disk Drives"),
    (re.compile(r"\b(memory|dimm|sdram|dram|\bram\b|pc[0-9])\b", re.I), "Memory"),
    (re.compile(r"\b(xeon|processor|\bcpu\b|core\s*2|epyc|opteron)\b", re.I), "Processors"),
    (re.compile(r"\b(power\s*supply|psu|power\s*supplies)\b", re.I), "Power Supplies"),
    (re.compile(r"\b(switch|catalyst|nexus)\b", re.I), "Switches"),
    (re.compile(r"\b(router|asr|isr)\b", re.I), "Routers"),
    (re.compile(r"\b(firewall|asa|fortigate)\b", re.I), "Firewalls"),
    (re.compile(r"\b(transceiver|sfp|qsfp|gbic)\b", re.I), "Transceivers"),
    (re.compile(r"\b(raid|controller|hba)\b", re.I), "Controllers"),
    (re.compile(r"\b(server|blade|poweredge|proliant)\b", re.I), "Servers"),
    (re.compile(r"\b(tape|lto|ultrium)\b", re.I), "Tape & Backup"),
    (re.compile(r"\b(gpu|graphics|tesla|quadro|geforce|radeon)\b", re.I), "Graphics Cards"),
    (re.compile(r"\b(motherboard|system\s*board|mainboard)\b", re.I), "Motherboards"),
    (re.compile(r"\b(network|nic|ethernet|adapter)\b", re.I), "Network Adapters"),
]


def session():
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"], respect_retry_after_header=True)
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def api_page(s, start_index, price_min):
    params = {
        "api_key": API_KEY, "output": "json", "q": "",
        "items": "true", "pages": "false", "suggestions": "false",
        "facets": "false", "categories": "false",
        "restrictBy[status]": "A",
        "sortBy": "price", "sortOrder": "asc",
        "maxResults": PAGE, "startIndex": start_index,
    }
    if price_min:
        params["restrictBy[price][min]"] = price_min
    r = s.get(API, params=params, timeout=40)
    r.raise_for_status()
    return r.json()


def fetch_all(limit=0):
    """Whole catalogue via a price-ascending cursor.

    The API caps paging at 10,000 items deep and its price *upper* bound is broken, so we
    sort by price ascending and walk a price *lower* bound (restrictBy[price][min]): page a
    bucket to the depth cap, then set min to the highest price seen and continue. Products
    at the boundary price repeat, so we dedup by product_id. A failed page raises (it must
    never look like the end of the list). We stop when a bucket returns fewer than the cap,
    and verify the unique count against the API's reported total before trusting the result.
    """
    s = session()
    by_id = {}
    price_min = 0
    bucket = 0
    total_reported = None
    while True:
        bucket += 1
        got_in_bucket = 0
        before_bucket = len(by_id)
        last_page_prices = []
        for start in range(0, DEPTH_CAP, PAGE):
            data = api_page(s, start, price_min)
            if total_reported is None:
                total_reported = data.get("totalItems")
            items = data.get("items", [])
            for it in items:
                by_id[it["product_id"]] = it
            prices = sorted(p for p in (_to_float(it.get("price")) for it in items) if p is not None)
            if prices:
                last_page_prices = prices
            got_in_bucket += len(items)
            print(f"bucket {bucket} min={price_min} start={start}: +{len(items)} (unique {len(by_id)})",
                  flush=True)
            if limit and len(by_id) >= limit:
                return list(by_id.values())[:limit], total_reported
            if len(items) < PAGE:
                break
            time.sleep(PAGE_DELAY)
        # bucket returned fewer than the depth cap -> reached the top of the catalogue
        if got_in_bucket < DEPTH_CAP or len(by_id) == before_bucket:
            break
        # Advance by the LOWEST price on the deepest page (the true depth boundary). We sort
        # ascending, so the deepest page holds the highest prices; its minimum is where the
        # next bucket must resume. Using the min (not the bucket max) keeps a stray early
        # outlier from skipping the catalogue, and the overlap is removed by dedup.
        boundary = min(last_page_prices) if last_page_prices else price_min
        price_min = boundary if boundary > price_min else round(price_min + 0.01, 2)
        time.sleep(PAGE_DELAY)
    return list(by_id.values()), total_reported


# --- mapping -----------------------------------------------------------------
def _to_float(v):
    try:
        f = float(v)
        return f
    except (TypeError, ValueError):
        return None


def clean(t):
    return re.sub(r"\s+", " ", (t or "").replace("cheap ", "", 1)).strip()


def brand_of(title):
    m = BRAND_RE.search(title or "")
    if not m:
        return ""
    b = m.group(1)
    return {"WD": "Western Digital", "HP": "HP"}.get(b.upper(), b if b.isupper() else b.title())


def condition_of(title):
    for rx, label in CONDITIONS:
        if rx.search(title or ""):
            return label
    return ""


def category_of(title):
    for rx, label in CATEGORY_RULES:
        if rx.search(title or ""):
            return label
    return ""


def first(rx, title):
    m = rx.search(title or "")
    return m.group(0).strip() if m else ""


def map_item(it, scraped_at):
    title = clean(it.get("title"))
    code = it.get("product_code") or ""
    qty = it.get("quantity")
    qty = int(qty) if isinstance(qty, (int, float)) or (isinstance(qty, str) and qty.isdigit()) else ""
    price = _to_float(it.get("price"))
    list_price = _to_float(it.get("list_price"))
    discount = _to_float(it.get("discount"))
    url = (it.get("link") or "").replace("http://", "https://")
    cap = CAPACITY_RE.search(title)
    other = "; ".join(x for x in [
        ("Interface: " + first(IFACE_RE, title)) if IFACE_RE.search(title) else "",
        ("Speed: " + first(RPM_RE, title)) if RPM_RE.search(title) else "",
        ("Form Factor: " + first(FORM_RE, title)) if FORM_RE.search(title) else "",
        ("Transfer: " + first(SPEED_RE, title)) if SPEED_RE.search(title) else "",
    ] if x)
    row = {
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": str(it.get("product_id") or ""),
        "title": title,
        "brand": brand_of(title),
        "category": category_of(title),
        "condition": condition_of(title),
        "sku": code,
        "part_number": code,
        "model_number": code,
        "capacity": (cap.group(1) + cap.group(2).upper()) if cap else "",
        "price": price if price is not None else "",
        "compare_at_price": list_price if list_price is not None else "",
        "discount_pct": round(discount, 2) if discount is not None else "",
        # The search only lists in-stock products, and its "quantity" is 1 for every one of them
        # (an in-stock flag, not a count). The store doesn't publish exact stock anywhere.
        "available": "Yes" if (isinstance(qty, int) and qty > 0) else "No",
        "stock_qty": "",
        "other_specs": other,
        "url": url,
    }
    return schema.product_row(row)


# --- changes -----------------------------------------------------------------
def same_num(a, b):
    return _to_float(a) == _to_float(b)


def seen_before(product_ids):
    """Product ids that were in an earlier full run (the database keeps every run)."""
    if not product_ids:
        return set()
    try:
        with db.connect() as conn:
            found = conn.execute(
                "SELECT DISTINCT product_id FROM serverorbit WHERE full_run AND product_id = ANY(%s)",
                (list(product_ids),)).fetchall()
        return {r[0] for r in found}
    except Exception as e:  # noqa: BLE001 - without the database, a return looks like "new"
        print(f"WARN can't check earlier runs, logging reappearing products as new: {e}", flush=True)
        return set()


def find_changes(rows, previous, scraped_at, full_run):
    """ServerOrbit's search lists only in-stock products, so stock changes show up as products
    leaving or rejoining the list:
      listed last run, gone now           -> out_of_stock (or removed from the store)
      not listed last run, seen earlier   -> back_in_stock (restocked)
      never seen before                   -> new
      listed both times, different price  -> price
    Must run before this run's snapshot is saved to the database.
    Returns prices rows (common/schema.py)."""
    current = {r[COLUMNS.index("product_id")]: schema.as_dict(r) for r in rows}
    # Only compare listings against a full previous snapshot (not after a test run)
    compare_listing = len(previous) >= 0.9 * len(current)
    returning = seen_before([p for p in current if p not in previous]) if compare_listing else set()

    prices = []
    for pid, product in current.items():
        before = previous.get(pid)
        if before is None:
            if previous and not compare_listing:
                continue
            change, old_price = (schema.BACK_IN_STOCK if pid in returning else schema.NEW), ""
        elif not same_num(before.get("price"), product["price"]):
            change, old_price = schema.PRICE, before.get("price", "")
        else:
            continue
        prices.append(schema.price_row(product, change, old_price))

    if full_run and compare_listing:
        for pid, before in previous.items():
            if pid not in current:
                prices.append(schema.price_row(
                    dict(before, website=SITE), schema.OUT_OF_STOCK, before.get("price", ""),
                    available=False, stock_qty=""))
    return prices


def set_output(name, value):
    path = os.getenv("GITHUB_OUTPUT")
    if path:
        with open(path, "a") as f:
            f.write(f"{name}={value}\n")


def main():
    limit = int(os.getenv("MAX_PRODUCTS") or 0)
    dry_run = os.getenv("DRY_RUN") == "1"
    scraped_dt = datetime.now(timezone.utc)
    scraped_at = scraped_dt.strftime("%Y-%m-%d %H:%M:%S UTC")

    # Read the previous run before scraping, so bad credentials fail in seconds
    previous = {}
    if not dry_run:
        db.check()
        previous = db.previous_run(SITE, "product_id", PREVIOUS_COLUMNS)

    items, total_reported = fetch_all(limit)
    print(f"fetched {len(items)} unique products (API reported total {total_reported})", flush=True)
    if not items:
        sys.exit("No products returned by the search API")
    # Never save a partial snapshot as if it were the full catalogue.
    if not limit and total_reported and len(items) < 0.98 * total_reported:
        sys.exit(f"Only fetched {len(items)} of {total_reported} products "
                 f"({len(items) / total_reported:.0%}); not saving a partial snapshot")

    rows = [map_item(it, scraped_at) for it in items]
    for c in ("sku", "brand", "category", "condition", "capacity", "price", "stock_qty"):
        filled = sum(1 for r in rows if r[COLUMNS.index(c)] not in ("", None))
        print(f"  {c}: filled in {filled}/{len(rows)} rows", flush=True)

    if dry_run:
        for row in rows[:5]:
            print(json.dumps(dict(zip(COLUMNS, row)), indent=2))
    db_error = None
    if not dry_run:
        # Changes first: spotting returning products needs the database before today is added
        prices = find_changes(rows, previous, scraped_at, full_run=not limit)
        db_error = db.save_run(SITE, rows, scraped_dt, len(items), full_run=not limit)
        counts = {}
        for p in prices:
            change = p[schema.PRICE_COLUMNS.index("change")]
            counts[change] = counts.get(change, 0) + 1
        restocked = schema.restocked_count(prices)
        print(f"{len(prices)} changes {counts}, {restocked} back in stock", flush=True)
        set_output("restocked", restocked)
        db_error = db_error or db.save_prices(prices, scraped_dt, full_run=not limit)
    set_output("rows", len(rows))
    set_output("products", len(items))
    set_output("in_stock", sum(1 for r in rows if r[COLUMNS.index("available")] == "Yes"))
    if db_error:
        sys.exit(f"Saving to the database failed: {db_error}")


if __name__ == "__main__":
    main()
