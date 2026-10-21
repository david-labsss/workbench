"""Scrape owl360it's USA store (storeusa.owl360it.com), IT categories only: every run's full
snapshot appended to Postgres.

owl360it.com itself is a Wix site of OWL360IT, an IT services company (London, Ontario and
Michigan) with no products of its own (no store sitemap; site currency CAD). Its "Store" menu links
two separate Kaseya Quote Manager (formerly Datto Commerce) storefronts on Azure Front Door:
  store.owl360it.com     "OWL360IT Inc."  Canada, CAD   -> not scraped
  storeusa.owl360it.com  "OWL360IT LLC"   Michigan, USD -> scraped here
The USA store's product pages carry schema.org priceCurrency "USD" next to the visible "$" price
(<meta itemprop="priceCurrency" content="USD"/>), so its prices are saved as they are; nothing is
converted. The two stores have separate catalogues (product 1121713 is a Cisco drive at USD
1,355.00 on the USA store and a Microsoft Azure VM at CAD 983.17 on the Canada one); the Canada
store shows CAD only (priceCurrency "CAD"), so it is left out.

The scraper says who it is: User-Agent "SPD-price-monitor/1.0" (USER_AGENT), not a browser's.
The store's Azure Front Door WAF answers 403 "The request is blocked." to generic tool names
(curl, python-requests) and headless Chrome, and lets a named bot through (checked 2026-10-21).
robots.txt disallows only /account, /checkout, /cloudstorage, /quotes and /error (no Crawl-delay).

Data path: the category pages (/find/<path>) are server-rendered. The page's own script (keystone.
search in core.js) re-renders the result list by POSTing its state model (the JSON in the result
panel's ks-state attribute) to /search/refresh, /search/changepage, /search/filter ...; the answer
is {"success": true, "html": "<the result panel>"}. This scraper POSTs the same model to
/search/refresh:
  {"CategoryID": <leaf id>, "Page": 1, "PageSize": n, "IncludeInStockOnly": true|false,
   "IncludeVisibleOnly": true, "SortBy": 1, "SortOrder": 1,           # "Lowest Price"
   "Filters": [{"Type": "Brand", "Key": "brands", "Value": "<brand id>"},
               {"Type": "Price", "Key": "price", "Value": "<lo> - <hi>"}], ...}
The page's own "In Stock" / "All Stock" toggle is IncludeInStockOnly (default In Stock); the
"Brands" refine list and the price ranges are the page's own filters. The UI offers PageSize 24 /
48 / 60; the server returns up to 1000 cards per request (more is cut to 1000).

Gotchas (all checked 2026-10-20):
 - "showing 4.48k products" is rounded. The pager is exact: with PageSize=1, "Page 1 of N" means
   N products. No pager = no products.
 - Paging is unstable: under "Best Match" and "Oldest", page 2 repeats / skips products of page 1
   (500 products paged by 60 gave 493-497 unique). So no slice is ever paged: every slice holds
   at most 1000 products and is fetched in ONE request (PageSize=1000), and its card count must
   equal its exact count.
 - The search window stops at 10,000 ("10k"), another reason to keep slices small.
 - Price range filters split cleanly (59 + 1326 = 1385) and include both ends ("544 - 544" finds
   the two $544.00 products). Cuts are made at half-cent values (x.xx5), which no price equals.
 - The "Brands" refine list shows at most 50 brands (ShowAllFilters does not change it), and its
   counts are for the whole leaf whatever price filter is set. 5 leaves have more than 50 brands
   (48 products past the list on 2026-10-20); see step 2.
 - Listing cards look the same in and out of stock (price and "Add to cart" on both); stock shows
   only on the product page ("In Stock" / "Out of Stock"), which follows IncludeInStockOnly.

Scope: the IT trees of the store's own menu (ROOTS): Storage (drives, SSDs, arrays, tape,
enclosures, media, flash), Networking (wired / wireless, rack & cabling, KVM switchboxes,
collaboration & VoIP), Computers > Servers, Components (CPUs, memory, motherboards, HBAs, PSUs,
cases, video cards, risers, fans) and Computer Systems (desktops, laptops, workstations, ...).
Left out: Software, Services, Printing, Electronics (TVs, phones, cameras, power, audio, GPS ...),
Displays, Peripherals, Computer Accessories, Cables & Adapters. The leaf categories under ROOTS
are read from the category landing pages every run (a landing page lists its sub-categories; a
leaf page has the result panel with its CategoryID).

Per leaf category:
 1. plan: All Stock, PageSize=1 -> exact count (pager) and the "Brands" refine list: brand id,
    exact count and name of the leaf's (up to) 50 biggest brands.
 2. products: one slice per brand (brand filter). A brand over 1000 is cut by price range into
    halves until each is at most 1000 (counts from PageSize=1 requests). When the brand slices
    hold fewer than the leaf's count (brands past the list's 50), the whole leaf is fetched in
    price slices too and the products it adds get their brand from their product page's "Brand
    Name" line (one page each, ~48 per run).
 3. stock: In Stock, same price cutting, no brand filter -> the leaf's in-stock product ids.
Every slice's cards are checked against its exact count (one retry, then WARN), every leaf's
products against the leaf's count.

Fields, from the listing card: <a href="/products/<slug>-<number>">, <img src>, product-title,
the line under the title (the product page's "Model:"), product-price.
  product_id, sku: the number at the end of the URL (the product page's "Part:", the store's
    catalogue number; unique);
  part_number: the "Model" line (the manufacturer part number, e.g. UCS-SD16TK3X-EP);
  title; brand: the brand slice's name from the store's Brands list (product page "Brand Name"
    past the list's 50); category: the leaf category's title ("Hard Drives & SSDs");
  price: the card price (= the product page's price and its meta price); $0 / missing -> None;
  compare_at_price: None (no struck-through price in listings or product pages);
  available: True when the product is in the leaf's In Stock results, else False;
  stock_qty: None (no count shown);
  image_url: the card image without its ?width=250&height=250&fit=pad resize (the full-size
    file); None when the card shows the brand-logo placeholder (/brandimages/), not a photo;
  description: None (only on the product page: one more page per product);
  condition: empty (not shown); url: https://storeusa.owl360it.com/products/<slug>-<number>.
listed = the sum of the leaf categories' exact counts (no product sat in two leaves on
2026-10-20: 49,077 unique = 49,077 listed, 53 leaves, 12,559 in stock, ~33 min with 3 workers).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import html
import json
import math
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

from common import http, runner, schema

SITE = "owl360it"
BASE = "https://storeusa.owl360it.com"
ROOTS = ["/find/storage", "/find/networking", "/find/computers/servers",
         "/find/computers/components", "/find/computers/computer-systems"]
SLICE = 1000                   # most cards the server returns in one request
MAX_PRICE = "79228162514264337593543950335"   # the site's own open upper end (.NET decimal max)
TOP = 10_000_000.0             # upper end used when cutting a price range
WORKERS = 3
DELAY = 1.0                    # before each request, per worker
TRIES = 3
RETRY_PAUSE = 30
USER_AGENT = "SPD-price-monitor/1.0"   # an honest bot name (see the docstring)

FIND_RE = re.compile(r'href="(?:https://storeusa\.owl360it\.com)?(/find/[^"#?]+)"')
STATE_RE = re.compile(r'ks-content="search-result"[^>]*?ks-state="([^"]*)"')
PAGES_RE = re.compile(r"Page\s+\d+\s+of\s+([\d,]+)")
CARD_SPLIT = 'data-component="ProductPanel"'
HREF_RE = re.compile(r'<a href="(/products/[^"]+?-(\d+))">')
IMG_RE = re.compile(r'<img src="([^"]+)"')
TITLE_RE = re.compile(r'<div class="product-title">(.*?)</div>', re.S)
MODEL_RE = re.compile(r'<div class="mt-1 hide-overflow">(.*?)</div>', re.S)
PRICE_RE = re.compile(r'<span class="product-price">([^<]*)</span>')
BRAND_PAGE_RE = re.compile(r"<li>Brand Name</li>\s*<li>(.*?)</li>", re.S)
BRAND_RE = re.compile(r'ks-function="refine-more-search-item" ks-search-value="([^"]*)">\s*'
                      r'<div[^>]*>\s*<span class="badge[^"]*">([\d,]+)</span>\s*</div>\s*'
                      r'<div[^>]*>\s*(?:<i[^>]*></i>\s*)?<span[^>]*>(.*?)</span>', re.S)

_warned = set()
_lock = threading.Lock()


def warn_once(key, msg):
    with _lock:
        if key not in _warned:
            _warned.add(key)
            print(f"WARN {msg}", flush=True)


def retrying(what, call):
    """call() with http's 429 / 5xx retries, plus a few tries for stalled / cut-off downloads."""
    for attempt in range(1, TRIES + 1):
        try:
            return call()
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError, ValueError) as e:
            if attempt == TRIES:
                raise
            print(f"WARN {what}: {e.__class__.__name__}, retrying in {RETRY_PAUSE}s", flush=True)
            time.sleep(RETRY_PAUSE)


def session():
    """This thread's http session, introducing itself as USER_AGENT."""
    s = http.session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def check_blocked(r):
    if r.status_code == 403 and "request is blocked" in r.text:
        raise RuntimeError(f"Azure Front Door WAF blocked {r.url} (HTTP 403)")
    r.raise_for_status()


def get(path):
    def call():
        time.sleep(DELAY)
        r = session().get(BASE + path, timeout=(30, 180))
        check_blocked(r)
        return r.text
    return retrying(path, call)


def search(cat, size, instock=False, brand=None, price=None):
    """The result panel HTML for one /search/refresh call (the page's own state model)."""
    filters = []
    if brand:
        filters.append({"Type": "Brand", "Key": "brands", "Value": brand})
    if price:
        filters.append({"Type": "Price", "Key": "price", "Value": price})
    model = {"Title": None, "PageMetaTitle": None, "Children": [], "Page": 1, "PageSize": size,
             "Query": "", "BrandID": None, "CategoryID": cat, "IncludeInStockOnly": instock,
             "IncludeVisibleOnly": True, "SortDisplayName": "Lowest Price", "SortBy": 1,
             "SortOrder": 1, "Filters": filters, "ShowAllFilters": False, "ResultTitle": None}
    body = json.dumps(model)

    def call():
        time.sleep(DELAY)
        r = session().post(BASE + "/search/refresh", data=body, timeout=(30, 180),
                                headers={"Content-Type": "application/json"})
        check_blocked(r)
        data = r.json()
        if data.get("success") is not True:
            raise ValueError(f"search not successful: {str(data)[:200]}")
        return data.get("html") or ""
    return retrying(f"search cat={cat} brand={brand} price={price} instock={instock}", call)


def exact_count(html_):
    """Products in a PageSize=1 result: the pager's page count (no pager = none)."""
    m = PAGES_RE.search(html_)
    if m:
        return int(m.group(1).replace(",", ""))
    return len(html_.split(CARD_SPLIT)) - 1


def parse_cards(html_):
    """{product number: card fields} of a result panel."""
    out = {}
    for card in html_.split(CARD_SPLIT)[1:]:
        card = card.split('class="product-buttons')[0]
        href = HREF_RE.search(card)
        if not href:
            warn_once("nohref", "a product card without a /products/ link")
            continue
        title = TITLE_RE.search(card)
        model = MODEL_RE.search(card)
        prices = PRICE_RE.findall(card)
        if len(prices) > 1:
            warn_once("2prices", f"a card with {len(prices)} prices: {href.group(1)} {prices}")
        img = IMG_RE.search(card)
        out[href.group(2)] = {
            "path": href.group(1),
            "title": text(title.group(1)) if title else "",
            "model": text(model.group(1)) if model else "",
            "price": prices[0] if prices else None,
            "image": html.unescape(img.group(1)) if img else "",
        }
    return out


def text(fragment):
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", fragment))).strip()


def money(s):
    try:
        v = float(re.sub(r"[^\d.]", "", s or ""))
    except ValueError:
        return None
    return v or None                      # $0 = no real price


def cut(lo, hi):
    """A half-cent price between lo and hi (geometric middle, so wide ranges halve fast)."""
    a, b = max(lo, 1.0), min(hi, TOP)
    mid = math.sqrt(a * b) if b > a * 4 else (lo + hi) / 2
    mid = math.floor(mid * 100) / 100 + 0.005
    if not lo < mid < hi:
        raise RuntimeError(f"cannot cut price range {lo}..{hi}")
    return mid


def price_value(lo, hi):
    hi_s = MAX_PRICE if hi >= TOP else f"{hi:.3f}"
    return f"{lo:.3f} - {hi_s}"


def collect(cat, count, instock=False, brand=None, lo=None, hi=None, depth=0):
    """{product number: card} of one slice (leaf + optional brand + optional price range) whose
    exact count is known: one PageSize=1000 request when it fits, else cut by price."""
    price = price_value(lo, hi) if lo is not None else None
    if count == 0:
        return {}
    if count <= SLICE:
        for attempt in (1, 2):
            found = parse_cards(search(cat, SLICE, instock, brand, price))
            if len(found) == count:
                return found
            if attempt == 1:
                count = exact_count(search(cat, 1, instock, brand, price))  # it may have moved
        warn_once(("short", cat, brand, price, instock),
                  f"cat {cat} brand {brand} price {price} instock {instock}: "
                  f"{len(found)} cards for {count} products")
        return found
    if depth > 40:
        raise RuntimeError(f"cat {cat} brand {brand}: cannot cut {count} products under {SLICE}")
    lo, hi = (0.0, TOP) if lo is None else (lo, hi)
    mid = cut(lo, hi)
    out = {}
    for a, b in ((lo, mid), (mid, hi)):
        n = exact_count(search(cat, 1, instock, brand, price_value(a, b)))
        out.update(collect(cat, n, instock, brand, a, b, depth + 1))
    got = len(out)
    if got < count:
        warn_once(("cutshort", cat, brand, price, instock),
                  f"cat {cat} brand {brand} price {price} instock {instock}: price cuts hold "
                  f"{got} of {count} products")
    return out


def leaves(pool):
    """[(path, CategoryID, title)] of every leaf category under ROOTS, from the landing pages."""
    found, seen, level = [], set(), list(ROOTS)
    while level:
        pages = list(pool.map(get, level))
        nxt = []
        for path, page in zip(level, pages):
            seen.add(path)
            m = STATE_RE.search(page)
            if m:
                state = json.loads(html.unescape(m.group(1)))
                found.append((path, state["CategoryID"], state.get("ResultTitle") or ""))
                continue
            main = page[page.find('id="main"'):]
            kids = [k for k in dict.fromkeys(FIND_RE.findall(main))
                    if k.startswith(path + "/") and k.count("/") == path.count("/") + 1]
            if not kids:
                warn_once(("nokids", path), f"{path}: neither a result list nor sub-categories")
            nxt += [k for k in kids if k not in seen]
        level = list(dict.fromkeys(nxt))
    return found


def brands_of(html_):
    out = []
    for value, count, name in BRAND_RE.findall(html_):
        v = json.loads(html.unescape(value))
        if v.get("Type") == "Brand":
            out.append((str(v["Value"]), int(count.replace(",", "")), text(name)))
    return out


def label_brands(cat, extra):
    """Brands of products outside the leaf's Brands list (it shows at most 50 brands, and its
    counts do not narrow under a price filter): the product page's "Brand Name" line."""
    for card in extra.values():
        m = BRAND_PAGE_RE.search(get(card["path"]))
        card["brand"] = text(m.group(1)) if m else ""
    missing = sum(1 for v in extra.values() if not v["brand"])
    if missing:
        warn_once(("nobrand", cat), f"cat {cat}: {missing} products without a Brand Name")


def scrape_leaf(leaf, limit=0):
    """(leaf, total, {number: card with brand}, in-stock numbers) for one leaf category."""
    path, cat, title = leaf
    plan = search(cat, 1)
    total = exact_count(plan)
    brands = brands_of(plan)
    products = {}
    for bid, n, name in brands:
        for num, card in collect(cat, n, brand=bid).items():
            card["brand"] = name
            products.setdefault(num, card)
        if limit and len(products) >= limit:
            break
    if not limit and len(products) < total:
        # brands past the Brands list's 50 (or none): the whole leaf, keep the ones the brand
        # slices missed, then read their brands from their product pages
        rest = collect(cat, total)
        extra = {k: dict(v, brand="") for k, v in rest.items() if k not in products}
        label_brands(cat, extra)
        print(f"  {path}: {sum(n for _, n, _ in brands)} in the {len(brands)} listed brands of "
              f"{total}, {len(extra)} more from category slices "
              f"({sum(1 for v in extra.values() if v['brand'])} of them branded)", flush=True)
        products.update(extra)
    instock_n = exact_count(search(cat, 1, instock=True))
    instock = set(collect(cat, instock_n, instock=True))
    if len(instock) < instock_n:
        warn_once(("instock", cat), f"{path}: {len(instock)} of {instock_n} in-stock products")
    flag = "" if limit or len(products) >= total else "WARN "
    print(f"{flag}{path} (cat {cat}, {title}): {len(products)} of {total} products, "
          f"{len(brands)} brands, {len(instock)} in stock", flush=True)
    return leaf, total, products, instock


def build_row(num, card, category, available, scraped_at):
    image = card["image"].split("?")[0]
    if "/brandimages/" in image:
        image = ""                        # the brand's logo, shown when there is no photo
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": num,
        "sku": num,
        "part_number": card["model"],
        "title": card["title"],
        "brand": card.get("brand") or "",
        "category": category,
        "price": money(card["price"]),
        "available": available,
        "image_url": image or None,
        "url": BASE + card["path"],
    })


def scrape(limit, scraped_at):
    with ThreadPoolExecutor(WORKERS) as pool:
        cats = leaves(pool)
        print(f"{len(cats)} leaf categories under {len(ROOTS)} roots", flush=True)
        if limit:                                   # test run: the first leaf, first brands
            results = [scrape_leaf(cats[0], limit)]
        else:
            results = list(pool.map(scrape_leaf, cats))

    rows, seen, listed, dupes, in_stock = [], set(), 0, 0, 0
    for (path, cat, title), total, products, instock in results:
        listed += total
        for num, card in products.items():
            if num in seen:
                dupes += 1
                continue
            seen.add(num)
            available = num in instock
            in_stock += available
            rows.append(build_row(num, card, title, available, scraped_at))
    print(f"{len(rows)} unique products ({dupes} also in another category), {in_stock} in stock, "
          f"{listed} listed", flush=True)
    if limit:
        return rows[:limit], None
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
