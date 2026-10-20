"""Scrape serverpartdeals.com (Shopify): every run's full snapshot appended to Postgres.

Every run covers the whole catalog (~17.6k products) in two steps:
  1. /products.json, 250 products per page (~71 requests): title, price, SKU, in stock yes/no,
     and the specs table (model / part number, capacity, ...) embedded in each description.
  2. ?view=stock for in-stock products only (~700 requests): exact stock count. Sold-out
     products are 0.
Rows are INSERTed into the "serverpartdeals" table (common/db.py); changes against the previous
full run go to the shared prices table (website = serverpartdeals). Columns:
common/schema.py.

Env vars:
  DATABASE_URL                 Postgres URL for the scraper_writer role
  MAX_PRODUCTS                 optional, only list the first N products (test runs)
  WORKERS                      optional, stock requests in parallel (default 2)
  DRY_RUN                      optional, "1" = print sample rows, don't save anything
"""
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common import db, schema

SITE = "serverpartdeals"
BASE = "https://serverpartdeals.com"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)
# The storefront localises prices to the caller's IP (PKR from Pakistan): always ask for USD
CURRENCY = {"currency": "USD", "country": "US"}
CATALOG_DELAY = 2.0  # seconds between /products.json pages
STOCK_DELAY = 1.0  # seconds each worker waits after a stock request
MAX_CONSECUTIVE_FAILURES = 20  # site is blocking us: stop stock requests, keep what we have

COLUMNS = schema.PRODUCT_COLUMNS

# Spec table label (lowercase) -> column
SPEC_ALIASES = {
    "model number": "model_number", "model #": "model_number", "model no": "model_number",
    "model no.": "model_number",
    "part number": "part_number", "part #": "part_number", "part no": "part_number",
    "part no.": "part_number", "p/n": "part_number",
    "series": "series",
    "capacity": "capacity",
    "interface": "interface",
    "form factor": "form_factor",
    "spindle speed": "spindle_speed", "rotational speed": "spindle_speed", "rpm": "spindle_speed",
    "cache": "cache", "cache size": "cache", "buffer": "cache", "buffer size": "cache",
    "endurance": "endurance",
    "condition": "condition",
    "type": "category",
}
# Labels already covered by other columns
SKIP_LABELS = {"brand", "manufacturer"}

SKU_CONDITIONS = {"NB": "New", "MR": "Manufacturer Recertified", "SR": "Seller Refurbished"}

CAPACITY_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(TB|GB)\b", re.I)
CACHE_RE = re.compile(r"\b(\d+)\s*MB\b", re.I)  # "256MB" in titles

_local = threading.local()
_stop = threading.Event()
_fail_lock = threading.Lock()
_consecutive_failures = 0


def session():
    """One requests session per thread, with retries on 429/5xx (honours Retry-After)."""
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers["User-Agent"] = USER_AGENT
        retry = Retry(
            total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        s.mount("https://", HTTPAdapter(max_retries=retry))
        _local.session = s
    return _local.session


def fetch_catalog(limit):
    """Every product. A failed page raises: it must never look like the end of the list."""
    products, page = [], 1
    while True:
        r = session().get(
            f"{BASE}/products.json", params={"limit": 250, "page": page, **CURRENCY}, timeout=30,
        )
        r.raise_for_status()
        batch = r.json().get("products", [])
        if not batch:
            break
        products.extend(batch)
        print(f"catalog page {page}: {len(batch)} products (total {len(products)})", flush=True)
        if limit and len(products) >= limit:
            return products[:limit]
        page += 1
        time.sleep(CATALOG_DELAY)
    return products


def record_result(ok):
    global _consecutive_failures
    with _fail_lock:
        _consecutive_failures = 0 if ok else _consecutive_failures + 1
        if _consecutive_failures >= MAX_CONSECUTIVE_FAILURES and not _stop.is_set():
            print(f"WARN {MAX_CONSECUTIVE_FAILURES} failures in a row, site is likely "
                  "rate limiting: stopping stock requests for this run", flush=True)
            _stop.set()


def fetch_stock(product):
    """{variant_id: qty} from the theme's own ?view=stock endpoint (the one the product page
    calls to render "60 in stock"), or None if not fetched.
    Response: {"variants":[{"id":..,"available":true,"tracked":true,"qty":60}]}
    """
    if _stop.is_set():
        return None
    try:
        r = session().get(
            f"{BASE}/products/{product['handle']}", params={"view": "stock"},
            headers={"X-Requested-With": "XMLHttpRequest"}, timeout=30,
        )
        r.raise_for_status()
        stock = {}
        for v in r.json().get("variants", []):
            if v.get("tracked") and isinstance(v.get("qty"), int):
                stock[v["id"]] = v["qty"]
        record_result(True)
        return stock
    except (requests.RequestException, ValueError) as e:
        print(f"WARN stock failed: {product['handle']}: {e}", flush=True)
        record_result(False)
        return None
    finally:
        time.sleep(STOCK_DELAY)


def clean(text):
    return re.sub(r"\s+", " ", text or "").strip()


def parse_specs(body_html):
    """Label -> value from the specs table embedded in the product description."""
    specs = {}
    for tr in BeautifulSoup(body_html or "", "html.parser").select("tr"):
        cells = tr.find_all(["th", "td"], recursive=False)
        if len(cells) == 2:
            label = clean(cells[0].get_text(" ")).rstrip(":").strip()
            value = clean(cells[1].get_text(" "))
            if label and value and len(label) <= 60:
                specs.setdefault(label, value)
    return specs


def map_specs(product):
    mapped, other = {}, []
    for label, value in parse_specs(product.get("body_html")).items():
        key = SPEC_ALIASES.get(label.lower())
        if key:
            if value.upper() not in ("N/A", "NA", "-"):
                mapped.setdefault(key, value)
        elif label.lower() not in SKIP_LABELS:
            other.append(f"{label}: {value}")
    return mapped, "; ".join(other)


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def capacity_tb(*texts):
    for text in texts:
        m = CAPACITY_RE.search(text or "")
        if m:
            size = float(m.group(1))
            return size if m.group(2).upper() == "TB" else size / 1000
    return None


def cache_from_title(title):
    m = CACHE_RE.search(title or "")
    return f"{m.group(1)}MB" if m else ""


def condition_from(product, variant, mapped):
    if mapped.get("condition"):
        return mapped["condition"]
    for opt in product.get("options", []):
        if "condition" in opt.get("name", "").lower():
            return variant.get(f"option{opt['position']}") or ""
    # SKUs end in a condition code, e.g. ST1000VX013_NB / _MR / _SR
    suffix = (variant.get("sku") or "").rsplit("_", 1)[-1].upper()
    if suffix in SKU_CONDITIONS:
        return SKU_CONDITIONS[suffix]
    title = product.get("title", "").lower()
    if "recertified" in title:
        return "Recertified"
    if "refurbished" in title:
        return "Refurbished"
    if re.search(r"\bnew\b", title):
        return "New"
    return ""


def build_rows(product, stock_by_variant, scraped_at, prev_by_id):
    """stock_by_variant: fresh counts for this product, or None to keep the last known ones."""
    mapped, other = map_specs(product)
    variants = product.get("variants", [])
    tb = capacity_tb(mapped.get("capacity"), product.get("title"))
    rows = []
    for v in variants:
        price = to_float(v.get("price")) or None
        if not v.get("available"):
            stock = 0
        elif stock_by_variant is not None:
            stock = stock_by_variant.get(v["id"])
        else:
            # Not refreshed (yet) this run: keep the last known count unless it said sold out
            old = (prev_by_id.get(str(v["id"])) or {}).get("stock_qty", "")
            stock = old if old not in ("", "0") else None
        row = {
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": str(v["id"]),       # unique: one row per variant
            "parent_id": str(product["id"]),  # the Shopify product the variant belongs to
            "variant_id": str(v["id"]),
            "title": product.get("title", "") + ("" if v.get("title") in (None, "Default Title") else f" - {v['title']}"),
            "brand": product.get("vendor", ""),
            "category": product.get("product_type") or mapped.get("category", ""),
            "condition": condition_from(product, v, mapped),
            "sku": v.get("sku") or "",
            "model_number": mapped.get("model_number", ""),
            "part_number": mapped.get("part_number", ""),
            "series": mapped.get("series", ""),
            "capacity": mapped.get("capacity", ""),
            "interface": mapped.get("interface", ""),
            "form_factor": mapped.get("form_factor", ""),
            "spindle_speed": mapped.get("spindle_speed", ""),
            "cache": mapped.get("cache") or cache_from_title(product.get("title")),
            "endurance": mapped.get("endurance", ""),
            "price": price,
            "compare_at_price": to_float(v.get("compare_at_price")),
            "price_per_tb": round(price / tb, 2) if price and tb else None,
            "available": bool(v.get("available")),
            "stock_qty": stock,
            "other_specs": other,
            "url": f"{BASE}/products/{product['handle']}"
            + (f"?variant={v['id']}" if len(variants) > 1 else ""),
        }
        rows.append(schema.product_row(row))
    return rows


# ---------------------------------------------------------------- changes

def same_number(a, b):
    return to_float(a) == to_float(b)


def find_changes(rows, previous):
    """Changes against the previous full run, as prices rows (common/schema.py).
    previous is {product_id: {column: text}} from db.previous_run()."""
    prices = []
    for r in rows:
        product = schema.as_dict(r)
        before = previous.get(product["product_id"])
        if before is None:
            change, old_price = schema.NEW, ""
        else:
            old_price = before.get("price", "")
            changed = []
            if not same_number(old_price, product["price"]):
                changed.append(schema.PRICE)
            if not same_number(before.get("stock_qty"), product["stock_qty"]):
                changed.append(schema.STOCK)
            was_available = before.get("available", "").upper() == "TRUE"
            if was_available != product["available"]:
                changed.append(schema.BACK_IN_STOCK if product["available"]
                               else schema.OUT_OF_STOCK)
            if not changed:
                continue
            change = "+".join(changed)
        prices.append(schema.price_row(product, change, old_price))
    print(f"{len(prices)} changes, {schema.restocked_count(prices)} back in stock", flush=True)
    return prices


def set_output(name, value):
    path = os.getenv("GITHUB_OUTPUT")
    if path:
        with open(path, "a") as f:
            f.write(f"{name}={value}\n")


def main():
    limit = int(os.getenv("MAX_PRODUCTS") or 0)
    workers = int(os.getenv("WORKERS") or 2)
    dry_run = os.getenv("DRY_RUN") == "1"
    scraped_dt = datetime.now(timezone.utc)
    scraped_at = scraped_dt.strftime("%Y-%m-%d %H:%M:%S UTC")

    # Read the previous run before scraping, so bad credentials fail in seconds
    previous = {}
    if not dry_run:
        db.check()
        previous = db.previous_run(SITE, "product_id", ["price", "stock_qty", "available"])

    # Step 1: whole catalog with specs
    catalog = fetch_catalog(limit)
    if not catalog:
        sys.exit("No products returned by /products.json")
    stock = {}  # product id -> fresh {variant_id: qty}

    def all_rows():
        rows = []
        for product in catalog:
            rows.extend(build_rows(product, stock.get(product["id"]), scraped_at, previous))
        return rows

    # Step 2: exact stock for in-stock products only
    in_stock = [p for p in catalog if any(v.get("available") for v in p.get("variants", []))]
    print(f"fetching stock for {len(in_stock)} in-stock products with {workers} workers...",
          flush=True)
    with ThreadPoolExecutor(workers) as pool:
        for p, s in zip(in_stock, pool.map(fetch_stock, in_stock)):
            if s is not None:
                stock[p["id"]] = s
    print(f"stock: {len(stock)} ok, {len(in_stock) - len(stock)} not fetched", flush=True)

    rows = all_rows()
    print(f"built {len(rows)} rows", flush=True)
    for c in ("model_number", "part_number", "condition", "capacity", "price_per_tb", "stock_qty"):
        filled = sum(1 for r in rows if r[COLUMNS.index(c)] != "")
        print(f"  {c}: filled in {filled}/{len(rows)} rows", flush=True)

    if dry_run:
        avail = [r for r in rows if r[COLUMNS.index("available")] is True]
        for row in (avail[:3] + rows[:2]):
            print(json.dumps(dict(zip(COLUMNS, row)), indent=2))
    db_error = None
    if not dry_run:
        db_error = db.save_run(SITE, rows, scraped_dt, len(catalog), full_run=not limit)
        prices = find_changes(rows, previous)
        set_output("restocked", schema.restocked_count(prices))
        db_error = db_error or db.save_prices(prices, scraped_dt, full_run=not limit)
    set_output("rows", len(rows))
    set_output("products", len(catalog))
    set_output("in_stock", len(in_stock))
    if db_error:
        sys.exit(f"Saving to the database failed: {db_error}")


if __name__ == "__main__":
    main()
