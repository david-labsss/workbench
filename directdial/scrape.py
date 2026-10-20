"""Scrape directdial.com's server / IT parts (~92k of its ~1.35M US catalogue): every run's full
snapshot appended to Postgres.

The store itself (www.directdial.com) sits behind a Cloudflare challenge, but its search and
category pages are rendered in the browser from a self-hosted Typesense server,
https://searchqd.directdial.com (not behind Cloudflare; no robots.txt). The search-only key is
public: it is in every store page's source as window.TYPESENSE_SERVER_CONFIG. The scraper tries to
read it fresh from the home page each run; while Cloudflare blocks that, it uses the saved copy
below (a wrong key fails loudly at the first query with HTTP 401). The key is sent as a header,
never in the URL, so it can't leak into logs. Collection idx_ddus = the US store (USD);
idx_ddca is Canada (CAD) and is not used.

Scope: the 35 IT categories in categories.lvl1 below (servers, drives, memory, CPUs, networking,
controllers, SAN, storage, GPUs, power, ...), ~92.3k products, ~16.6k in stock (2026-10-20). The
big "Uncategorized" / Software / Services / Office trees are left out.

Paging: one count query (per_page 0) gives `found` = the listed count; then pages of 250 (the
Typesense maximum) sorted by _seq_id (the index's own insertion id: a stable order), 3 at a time.
Every page must report the same `found`, and the unique ids must add up to exactly `found`;
otherwise the whole scope is read again (the index changed mid-run), and after 3 tries the run
raises, so a bad fetch never looks like the end of the list. The whole index is re-pushed once a
day at ~12:48-12:51 UTC, so a run never starts between 12:42 and 12:56 UTC (it waits instead).
~370 requests, ~3-4 minutes.

Fields (the store's grid renders the same documents, with price level 0 = guest):
  id (manufacturer part no., "/" -> "-", unique) -> product_id, sku
  name (the real part number, e.g. MZ-V9P4T0B/AM) -> part_number, model_number
  description ("Samsung 990 PRO 4 TB Solid State Drive - M.2 2280 Internal - ...") -> title
  bullets ("<li>4TB Storage Capacity<li>...") as plain text -> description (else the spec line)
  image (/dd2/img/item/...) -> image_url (None when hasNoImage: the site then shows the brand
      logo); url (/item/<slug>/<part>) -> url
  brand -> brand; productFamily -> series
  productType (= the 3rd category level, e.g. "Solid State Drive", "SFP+") -> category; when
      missing, that level if a brand level follows it, else the lvl1 category
  price -> price (USD, the guest price; price1..price3 are logged-in tier prices and are ignored).
      It is already net of any instant rebate (badge "Instant Rebate | $51 (7%)": 7% = 51 /
      (price + 51)). Every document has a price > 0; 0 / missing would be saved as None.
  stock -> stock_qty (a real count; -1 = no count -> None); instock (== stock > 0) -> available.
      The ~750 products with stock -1 carry the badge "Available" but instock false (the store's
      own "In stock" filter leaves them out), so they are saved as not available.
  badges (In Stock - Great Price / Limited Stock at this price / Backordered / Available /
      Heavy Freight / Instant Rebate) -> other_specs
  attrib ("Storage Capacity > 4 TB", "Spindle Speed (rpm) > 7200", "Buffer > 256 MB", "Memory
      Size > 64 GB", "Form Factor > 2.5\"", "Drive Interface > SATA", ...) -> capacity, cache,
      spindle_speed, form_factor, interface
  condition: no field (a new-goods distributor). Set from the description when it says so
      (Refurbished / Remanufactured / Certified Pre-Owned, ~400 products); otherwise blank.
  msrp, the stock badge and the UPC are kept in other_specs. msrp is NOT saved as
      compare_at_price: whether the page shows it could not be checked (the page is behind
      Cloudflare).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import html
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import requests

from common import http, runner, schema

SITE = "directdial"
BASE = "https://www.directdial.com"
HOST = "https://searchqd.directdial.com"          # fallback, from the store's page source
API_KEY = "hYDsFlzrUCuL15lO5XY0LyeuXCJDcwvA"  # public search-only key, same source (2026-02-28)
COLLECTION = "idx_ddus"                           # the US store, prices in USD
PER_PAGE = 250            # Typesense's largest page
WORKERS = 3               # requests in flight
DELAY = 0.3               # pause after each request, per worker
TRIES = 3                 # attempts for a stalled / cut-off download
RETRY_PAUSE = 20
SCOPE_TRIES = 3           # reads of the scope whose pages don't add up
SCOPE_PAUSE = 60
QUIET = ((12, 42), (12, 56))   # UTC; the daily index re-push runs ~12:48-12:51

CATEGORIES = [
    "Computer Systems > Servers",
    "Computer Systems > Barebone Systems",
    "Storage Drives > Hard Drives/Solid State Drives",
    "Storage Drives > Tape Drives",
    "Memory > RAM Modules",
    "Memory > Cache Memory",
    "Memory > Memory Expansion Boards",
    "Electronic Components > Microprocessors",
    "Electronic Components > Motherboards",
    "Network & Communication > Switches & Bridges",
    "Network & Communication > Network Interface Cards",
    "Network & Communication > Transceivers/Media Converters",
    "Network & Communication > Routers & Gateways",
    "Network & Communication > Modules",
    "Network & Communication > Network Security & Firewalls",
    "Network & Communication > Server Load Balancers",
    "Network & Communication > Network Fabric Extenders",
    "I/O & Storage Controllers > SCSI/RAID Controllers",
    "I/O & Storage Controllers > Hard Drive Adapters",
    "I/O & Storage Controllers > Serial/Ultra ATA Controllers",
    "SAN Devices > SAN Switches",
    "SAN Devices > Host Bus Adapters",
    "Storage Systems > Storage Arrays & Servers",
    "Storage Autoloaders & Libraries > Tape Autoloader/Libraries",
    "Storage Media > Tape Media",
    "Video Cards > Graphic Cards",
    "Power Equipment > Power Supplies",
    "Power Equipment > UPS",
    "Power Equipment > PDUs",
    "Power Equipment > Power Modules",
    "Power Equipment > UPS Batteries",
    "Accessories > Drive Cabinets",
    "Accessories > PC Fans/Heat Sinks",
    "Accessories > Computer Components/Replacement Parts & Accessories",
    "Accessories > Expansion Chassis",
]
SCOPE = "categories.lvl1:=[" + ",".join(f"`{c}`" for c in CATEGORIES) + "]"
FIELDS = ("id,name,description,bullets,brand,categories,productType,productFamily,price,msrp,"
          "stock,instock,badges,UPC,attrib,url,image,hasNoImage")

CONFIG_RE = re.compile(r"TYPESENSE_SERVER_CONFIG\s*=\s*\{(.*?)\n\s*\}", re.S)
CONDITION_RES = [(re.compile(r"\brefurbished\b", re.I), "Refurbished"),
                 (re.compile(r"\bremanufactured\b", re.I), "Remanufactured"),
                 (re.compile(r"\bcertified pre-owned\b", re.I), "Pre-Owned"),
                 (re.compile(r"\bopen[- ]box\b", re.I), "Open Box")]
SIZE_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*(TB|GB|MB)$", re.I)
DRIVE_TYPES = re.compile(r"hard drive|solid state|ssd", re.I)


# --- fetching ----------------------------------------------------------------
def search_config():
    """(host, key) from the store's home page (window.TYPESENSE_SERVER_CONFIG); the saved ones
    when the page can't be read (Cloudflare challenges plain requests) or changed shape."""
    try:
        # one plain try (no 429 / 5xx retries): Cloudflare answers this page with a challenge
        r = requests.get(BASE + "/", headers={"User-Agent": http.USER_AGENT}, timeout=30)
        block = CONFIG_RE.search(r.text) if r.ok else None
        if block:
            key = re.search(r'"apiKey"\s*:\s*"([A-Za-z0-9]{16,64})"', block.group(1))
            host = re.search(r'"host"\s*:\s*"([a-z0-9.-]+)"', block.group(1))
            if key and host:
                return f"https://{host.group(1)}", key.group(1)
        print(f"WARN search key not readable from the store page (HTTP {r.status_code}); "
              "using the saved one", flush=True)
    except requests.RequestException as e:
        print(f"WARN store page unreachable ({e.__class__.__name__}); using the saved key",
              flush=True)
    return HOST, API_KEY


class Typesense:
    def __init__(self, host, key):
        self.url = f"{host}/collections/{COLLECTION}/documents/search"
        self.headers = {"X-TYPESENSE-API-KEY": key}

    def search(self, params):
        """One search (JSON). http.session() retries 429 / 5xx; this retries a download that
        stalls or is cut off part-way (requests does not). The last failure raises."""
        query = {"q": "*", "query_by": "concat", "filter_by": SCOPE, **params}
        for attempt in range(1, TRIES + 1):
            try:
                r = http.session().get(self.url, params=query, headers=self.headers, timeout=90)
                if r.status_code == 401:
                    raise RuntimeError("Typesense refused the search key (401): it was rotated; "
                                       "copy the new apiKey from the store's page source "
                                       "(window.TYPESENSE_SERVER_CONFIG) into API_KEY")
                r.raise_for_status()
                data = r.json()
                time.sleep(DELAY)
                return data
            except (requests.ConnectionError, requests.Timeout,
                    requests.exceptions.ChunkedEncodingError, ValueError) as e:
                if attempt == TRIES:
                    raise
                print(f"WARN {e.__class__.__name__} ({str(e)[:120]}), retrying in {RETRY_PAUSE}s",
                      flush=True)
                time.sleep(RETRY_PAUSE)

    def count(self):
        return int(self.search({"per_page": 0})["found"])

    def page(self, n, per_page=PER_PAGE):
        return self.search({"per_page": per_page, "page": n, "sort_by": "_seq_id:asc",
                            "include_fields": FIELDS})


def wait_for_quiet():
    """Don't read the index while it is being re-pushed (~12:48-12:51 UTC every day)."""
    now = datetime.now(timezone.utc)
    start = now.replace(hour=QUIET[0][0], minute=QUIET[0][1], second=0, microsecond=0)
    end = now.replace(hour=QUIET[1][0], minute=QUIET[1][1], second=0, microsecond=0)
    if start <= now < end:
        print(f"index re-push window: waiting until {end:%H:%M} UTC", flush=True)
        time.sleep((end - now + timedelta(seconds=5)).total_seconds())


def read_scope(api, pool):
    """{id: document} for the whole scope. Re-reads it when the pages don't add up to exactly
    the reported count; raises after SCOPE_TRIES."""
    for attempt in range(1, SCOPE_TRIES + 1):
        first = api.page(1)
        found = int(first["found"])
        pages = math.ceil(found / PER_PAGE)
        docs, totals = [h["document"] for h in first.get("hits") or []], {found}
        for data in pool.map(api.page, range(2, pages + 1)):
            docs.extend(h["document"] for h in data.get("hits") or [])
            totals.add(int(data["found"]))
        by_id = {str(d["id"]): d for d in docs}
        if totals == {found} and len(docs) == found and len(by_id) == found:
            return by_id
        problem = f"{len(docs)} hits, {len(by_id)} unique, pages reported {sorted(totals)}"
        if attempt == SCOPE_TRIES:
            raise RuntimeError(f"{problem} of {found}; not saving a partial snapshot")
        print(f"WARN {problem} of {found}; reading the scope again in {SCOPE_PAUSE}s", flush=True)
        time.sleep(SCOPE_PAUSE)
        wait_for_quiet()


# --- mapping -----------------------------------------------------------------
def clean(v):
    return re.sub(r"\s+", " ", html.unescape(str(v if v is not None else ""))).strip()


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def attributes(doc):
    """{name: value} from attrib ["Storage Capacity > 4 TB", ...]."""
    out = {}
    for a in doc.get("attrib") or []:
        name, _, value = clean(a).partition(" > ")
        if value and name not in out:
            out[name] = value
    return out


def size(value):
    """'4 TB' -> ('4TB', 4.0 TB); anything else -> ('', None)."""
    m = SIZE_RE.match(value or "")
    if not m:
        return "", None
    n, unit = float(m.group(1)), m.group(2).upper()
    text = f"{m.group(1)}{unit}"
    return text, {"TB": n, "GB": n / 1000, "MB": n / 1e6}[unit]


def condition_of(text):
    for rx, name in CONDITION_RES:
        if rx.search(text):
            return name
    return ""


def category_of(doc):
    """The product type ("Solid State Drive", "SFP+", ...): productType, which is the third
    category level; when it is missing, that level if the path still has a brand after it
    (4+ levels), else the lvl1 category ("Tape Media")."""
    cats = [clean(c) for c in doc.get("categories") or []]
    if clean(doc.get("productType")):
        return clean(doc.get("productType"))
    if len(cats) >= 4:
        return cats[2]
    return cats[1] if len(cats) > 1 else ""


def description_of(doc):
    """The bullet list ("<li>4TB Storage Capacity<li>M.2 2280 Form Factor...") as plain text,
    else the spec line."""
    bullets = [b for b in re.split(r"<li[^>]*>", str(doc.get("bullets") or ""), flags=re.I)
               if schema.plain_text(b)]
    if bullets:
        return schema.plain_text("; ".join(schema.plain_text(b) for b in bullets))
    return schema.plain_text(doc.get("description"))


def build_row(doc, scraped_at):
    title = clean(doc.get("description")) or clean(doc.get("name"))
    category = category_of(doc)
    attrs = attributes(doc)
    price = to_float(doc.get("price"))
    price = price if price and price > 0 else None
    stock = doc.get("stock")
    stock_qty = int(stock) if isinstance(stock, (int, float)) and not isinstance(stock, bool) \
        and stock >= 0 else None
    available = doc.get("instock") is True

    capacity, tb = size(attrs.get("Storage Capacity"))
    if not capacity:
        capacity, _ = size(attrs.get("Memory Size"))
    is_drive = bool(DRIVE_TYPES.search(category))
    rpm = attrs.get("Spindle Speed (rpm)", "")
    cache, _ = size(attrs.get("Buffer"))
    interface = attrs.get("Drive Interface") or attrs.get("Host Interface") or ""

    msrp = to_float(doc.get("msrp"))
    # "Heavy Freight|Instant Rebate | $51 (7%)|In Stock - Great Price|" -> one readable list.
    # price is already net of an instant rebate (the badge % = rebate / (price + rebate)).
    badges = [clean(b) for b in re.split(r"\|(?! \$)", str(doc.get("badges") or ""))
              if clean(b)]
    flags = ["Badges: " + ", ".join(b.replace(" | ", " ") for b in badges) if badges else "",
             f"MSRP: {msrp:g}" if msrp and price and msrp > price else "",
             f"UPC: {clean(doc.get('UPC'))}" if clean(doc.get("UPC")) else ""]
    for name in ("Memory Technology", "Memory Speed", "Error Checking", "Signal Processing",
                 "Processor Socket", "Total Number of Ports"):
        if attrs.get(name):
            flags.append(f"{name}: {attrs[name]}")

    url = clean(doc.get("url"))
    images = doc.get("image") or []
    image = clean(images[0] if isinstance(images, list) and images else images)
    if doc.get("hasNoImage") is True:
        image = ""                 # the site shows the brand logo instead (.../-/EATON.jpg)
    part = clean(doc.get("name"))
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": str(doc["id"]),
        "sku": str(doc["id"]),
        "part_number": part,
        "model_number": part,
        "title": title,
        "brand": clean(doc.get("brand")),
        "category": category,
        "condition": condition_of(title),
        "series": clean(doc.get("productFamily")),
        "capacity": capacity,
        "interface": interface if is_drive else "",
        "form_factor": attrs.get("Form Factor", ""),
        "spindle_speed": f"{rpm} RPM" if rpm.isdigit() else "",
        "cache": cache if is_drive else "",
        "other_specs": "; ".join(f for f in flags if f),
        "price": price,
        "price_per_tb": round(price / tb, 2) if price and tb and is_drive else None,
        "available": available,
        "stock_qty": stock_qty,
        "image_url": ((BASE + image) if image.startswith("/") else image) or None,
        "description": description_of(doc),
        "url": (BASE + url) if url.startswith("/") else url,
    })


def scrape(limit, scraped_at):
    wait_for_quiet()
    api = Typesense(*search_config())
    listed = api.count()
    if not listed:
        raise RuntimeError("the search index lists no products in the IT categories")
    print(f"{listed} products in {len(CATEGORIES)} categories", flush=True)
    if limit:
        # test run: the first page(s) only, no completeness check
        docs = []
        for n in range(1, math.ceil(limit / PER_PAGE) + 1):
            docs.extend(h["document"] for h in api.page(n).get("hits") or [])
        docs = docs[:limit]
    else:
        with ThreadPoolExecutor(WORKERS) as pool:
            docs = list(read_scope(api, pool).values())
    return [build_row(d, scraped_at) for d in docs], listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
