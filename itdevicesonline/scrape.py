"""Scrape itdevicesonline.com: every run's full snapshot appended to Postgres.

itdevicesonline.com (IT Devices, a US reseller) is a Next.js App Router storefront over a custom
backend. It lists ~655k products, but most of them are non-IT bulk (software licences ~250k,
plus office supplies, medical, kitchen, sewing, ...). Only ~12 products show per listing page and
the server is slow, so the whole store is far too big for one run. This scraper is SCOPED to the
IT / server / storage / networking section (see CATEGORIES): ~165k listing-reported products,
~13.7k listing pages, which one GitHub Actions job finishes in roughly 1.5-3 h.

Fastest allowed path: the category listing pages. robots.txt allows everything except /api/
(the price "skeleton" you see is a client-side /api/ call we must not use), /cart, /login,
/signup and /account-details, and it publishes a sitemap. Each listing page
(/category/<slug>?page=N, 12 products a page) embeds the full product JSON in its React Server
Component payload (self.__next_f.push(...)): id, title, mpn (= manufacturer part number), brand,
price, originalPrice, inStock and quoteOnly. So we read price AND stock straight from the
listing and never fetch the (108 KB, ~5 s) product pages, nor touch /api/. Currency is USD
(the product pages' JSON-LD says priceCurrency USD).

Per-category pagination is deterministic (the same page always returns the same products), so we
page every category 1..totalPages and dedupe globally by product id. Two quirks we handle:
  - the listing's "total" is NOT reliable - it is inflated, badly so for tiny categories
    (e.g. power-conditioning claims 227 but only 143 distinct products are reachable), and big
    categories page cleanly to ~95%+ of their total. The reachable distinct set IS the catalogue
    the site exposes, so we take it as complete and return listed=None (no reliable total). As a
    safety net we fail the run if too many pages stay empty after retries.
  - a page occasionally returns 200 with an empty listing (a transient server hiccup), so an
    empty page is retried before being accepted.

Scoping: CATEGORIES is the fixed set of IT/server/storage/networking category slugs (servers,
storage, components/RAM, networking and their accessories). Override it with the CATEGORIES env
var (comma-separated slugs; the full list is on /categories). The non-IT bulk and software are
left out on purpose.

Chunking (optional, default = whole scope in one run): set SHARD_COUNT=N and SHARD_INDEX=i
(0-based) to crawl only every N-th (category, page) task, so the scope can be split across N
parallel jobs if the server is slow on a given day. Each shard is then a PARTIAL snapshot, so run
shards only as test/one-off crawls, not as the scheduled full run (a partial snapshot saved as a
full run would log every missing product as out_of_stock). A lighter daily mode could instead
diff the sitemap (storage/sitemap/sitemapN.xml) for new/removed product ids; not implemented here.

Changes are found against the previous full run in the database (common/changes.py): new
products, price changes, out_of_stock / back_in_stock. They go to the shared prices table
(website = itdevicesonline). Columns: common/schema.py.

Env vars:
  DATABASE_URL     Postgres URL for the scraper_writer role
  CATEGORIES       optional, comma-separated category slugs to scrape (default: the IT set below)
  SCRAPE_WORKERS   optional, listing pages fetched at once (default 8)
  SHARD_COUNT      optional, split the crawl into this many shards (default 1 = whole scope)
  SHARD_INDEX      optional, which shard to run, 0-based (default 0)
  MAX_PRODUCTS     optional, stop after about N products (test runs, not compared or counted)
  DRY_RUN          optional, "1" = print sample rows, don't touch the database
"""
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from common import http, runner, schema

SITE = "itdevicesonline"
BASE = "https://www.itdevicesonline.com"
PER_PAGE = 12                 # the listing is fixed at 12 products a page (no page-size override)
WORKERS = int(os.getenv("SCRAPE_WORKERS") or 8)
EMPTY_RETRIES = 3             # a 200-with-no-products page is a transient hiccup: retry it
MAX_EMPTY_SHARE = 0.02        # fail the run if more than this share of pages stay empty

# The IT / server / storage / networking section: servers, storage (HDD/SSD), components (RAM,
# CPUs, GPUs, boards), networking and their accessories. Everything else the store sells
# (software licences ~250k, office, medical, kitchen, sewing, ...) is left out. Full list: /categories.
DEFAULT_CATEGORIES = [
    "computers",                               # servers, workstations, desktops, laptops
    "computer-components",                     # RAM, CPUs, GPUs, motherboards, controllers
    "computer-spare-parts-and-accessories",    # server/rack spares and accessories
    "computer-cables",                         # SAS/SATA/network/power/fibre cabling
    "data-storage",                            # HDDs, SSDs, flash, tape, arrays
    "data-input-devices",                      # keyboards, mice, KVM, scanners
    "networking",                              # switches, NICs, transceivers, routers, firewalls
    "monitors-and-accessories",                # displays and mounts
    "power-conditioning",                      # UPS, PDUs
    "telecommunication-equipment-accessories", # telecom/VoIP accessories
]

CONDITIONS = [
    (re.compile(r"\brefurb", re.I), "Refurbished"),
    (re.compile(r"\brecertified\b", re.I), "Recertified"),
    (re.compile(r"\brenewed\b", re.I), "Renewed"),
    (re.compile(r"\bopen\s*box\b", re.I), "Open Box"),
    (re.compile(r"\bpre[- ]?owned\b", re.I), "Used"),
    (re.compile(r"\bused\b", re.I), "Used"),
    (re.compile(r"\bbrand\s*new\b|\bnew\b", re.I), "New"),
]
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB|MB)\b", re.I)
STORAGE_SLUGS = {"data-storage", "computer-components"}

# RSC payloads arrive as self.__next_f.push([1,"<json-string-chunk>"]); the chunks concatenate
# into the real server-component text, which holds "listing":{"products":[ {...}, {...} ]}.
PUSH_RE = re.compile(r'self\.__next_f\.push\(\[1,("(?:[^"\\]|\\.)*")\]\)')


def category_name(slug):
    return slug.replace("-and-", " & ").replace("-", " ").title()


def fetch(url):
    """GET a listing page as text, retrying empty/failed responses a few times."""
    for attempt in range(EMPTY_RETRIES):
        try:
            r = http.session().get(url, timeout=60)
            if r.status_code == 200 and "__next_f" in r.text:
                return r.text
        except Exception:
            pass
        time.sleep(2 * (attempt + 1))
    return ""


def decode_rsc(html):
    out = []
    for m in PUSH_RE.finditer(html):
        try:
            out.append(json.loads(m.group(1)))
        except ValueError:
            pass
    return "".join(out)


def parse_products(html):
    """The product objects from one listing page's RSC payload (first "products":[...] array)."""
    rsc = decode_rsc(html)
    start = rsc.find('"products":[')
    if start < 0:
        return []
    i = start + len('"products":[')
    products = []
    while i < len(rsc):
        c = rsc[i]
        if c == "{":
            depth, j, instr, esc = 0, i, False, False
            while j < len(rsc):
                ch = rsc[j]
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    instr = not instr
                elif not instr and ch == "{":
                    depth += 1
                elif not instr and ch == "}":
                    depth -= 1
                    if depth == 0:
                        j += 1
                        break
                j += 1
            try:
                products.append(json.loads(rsc[i:j]))
            except ValueError:
                pass
            i = j
        elif c == "]":
            break
        else:
            i += 1
    return products


def total_pages(html):
    m = re.search(r'"totalPages":\s*(\d+)', decode_rsc(html))
    return int(m.group(1)) if m else None


def page_url(slug, page):
    base = f"{BASE}/category/{slug}"
    return base if page == 1 else f"{base}?page={page}"


def to_price(value):
    try:
        p = round(float(value), 2)
    except (TypeError, ValueError):
        return None
    return p if p > 0 else None


def condition_of(title):
    for pattern, label in CONDITIONS:
        if pattern.search(title or ""):
            return label
    return ""


def build_row(p, slug, scraped_at):
    title = (p.get("title") or "").strip()
    mpn = (p.get("mpn") or "").strip()
    quote_only = bool(p.get("quoteOnly"))
    price = None if quote_only else to_price(p.get("displayPrice") or p.get("price"))
    capacity = ""
    if slug in STORAGE_SLUGS:
        m = CAPACITY_RE.search(title)
        capacity = m.group(1) + m.group(2).upper() if m else ""
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": str(p["id"]),
        "sku": mpn,
        "part_number": mpn,                 # the store's mpn is the manufacturer part number
        "title": title,
        "brand": (p.get("brand") or "").strip(),
        "category": category_name(slug),
        "condition": condition_of(title),
        "capacity": capacity,
        "price": price,
        "compare_at_price": to_price(p.get("originalPrice")),
        "available": bool(p.get("inStock")),
        "url": f"{BASE}/product-details/{p['id']}",
    })


def plan_tasks(slugs):
    """One (slug, page) task per listing page of every scoped category."""
    with ThreadPoolExecutor(WORKERS) as pool:
        firsts = list(pool.map(lambda s: (s, fetch(page_url(s, 1))), slugs))
    tasks = []
    for slug, html in firsts:
        tp = total_pages(html)
        if not tp:
            print(f"WARN category {slug}: no listing found (skipped)", flush=True)
            continue
        print(f"category {slug}: {tp} pages", flush=True)
        tasks += [(slug, page) for page in range(1, tp + 1)]
    return tasks


def shard(tasks):
    count = int(os.getenv("SHARD_COUNT") or 1)
    index = int(os.getenv("SHARD_INDEX") or 0)
    if count > 1:
        tasks = [t for n, t in enumerate(tasks) if n % count == index]
        print(f"shard {index}/{count}: {len(tasks)} of the scope's pages", flush=True)
    return tasks


def scrape(limit, scraped_at):
    slugs = [s.strip() for s in (os.getenv("CATEGORIES") or "").split(",") if s.strip()]
    slugs = slugs or DEFAULT_CATEGORIES

    by_id = {}
    empty = [0]

    if limit:
        # Test run: walk the first category's pages until we have about `limit` products.
        slug = slugs[0]
        page = 1
        while len(by_id) < limit:
            products = parse_products(fetch(page_url(slug, page)))
            if not products:
                break
            for p in products:
                by_id[str(p["id"])] = (p, slug)
            page += 1
        items = list(by_id.values())[:limit]
        return [build_row(p, slug, scraped_at) for p, slug in items], None

    tasks = shard(plan_tasks(slugs))
    if not tasks:
        return [], None

    def do(task):
        slug, page = task
        products = parse_products(fetch(page_url(slug, page)))
        return slug, products

    with ThreadPoolExecutor(WORKERS) as pool:
        for i, (slug, products) in enumerate(pool.map(do, tasks), 1):
            if not products:
                empty[0] += 1
            for p in products:
                pid = str(p["id"])
                if pid not in by_id:            # keep the first category a product is seen in
                    by_id[pid] = (p, slug)
            if i % 500 == 0 or i == len(tasks):
                print(f"{i}/{len(tasks)} pages, {len(by_id)} unique products, "
                      f"{empty[0]} empty", flush=True)

    if empty[0] > MAX_EMPTY_SHARE * len(tasks):
        sys.exit(f"{empty[0]}/{len(tasks)} listing pages were empty after retries; "
                 "the site is likely erroring - not trusting this run")

    rows = [build_row(p, slug, scraped_at) for p, slug in by_id.values()]
    return rows, None            # the site's "total" is unreliable, so no completeness target


if __name__ == "__main__":
    runner.run(SITE, scrape)
