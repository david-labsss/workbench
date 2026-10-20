"""Scrape ipcstore.com (Magento 2, US store, USD; ~66k products, a general store): only its IT /
server-parts categories, every run's full snapshot appended to Postgres.

The store's /graphql is switched off (404) and robots.txt disallows every query string except
?p=N (so no product_list_limit, no /catalogsearch/), so the chosen category listing pages are read
page by page with ?p=N (36 products per page). Categories (path, "N items" on 2026-10-19):
  computers-electronics/data-storage                    742  (HDDs, SSDs, NAS, enclosures, media)
  computers-electronics/computer-components           1,877  (memory, CPUs, NICs, RAID, motherboards,
                                                              GPUs, cooling, racks, drive enclosures)
  computers-electronics/networking                    1,229  (switches, transceivers, firewalls, APs)
  computers-electronics/computers/servers                10
  .../power-conditioning/power-supply-units              49  (server and PC power supplies)
Memory Modules (255), Processors (156), Internal Hard Drives (98), Internal SSDs (198), Network
Switches (429) and Transceiver Modules (156) are walked first, only so their products get that
more specific category name (~39 extra pages). Left out: notebooks / PCs / tablets, cables,
monitors, printers, office, input devices, UPS / PDUs and every non-IT department.

The listing renders only products that can be bought: out-of-stock products are counted in the
category's "N items" (~15% of it) but never shown (their product pages say "Out of stock", no
Add to Cart). So every scraped card is in stock, and a product that goes out of stock drops out
of the listing (common/changes.py logs that as out_of_stock). For the same reason the "N items"
totals can't be the snapshot's target count (a full run would always look ~85% complete), so
listed is None and completeness is checked page by page instead: every page from 1 to
ceil(N / page size) is read (the page count comes from the total, not from what is shown), and
each page must show exactly as many cards as its own toolbar says ("Items 37-65 of 742" = 29), or
the run fails (a one-page listing only says "N Items" incl. hidden ones, so there it must not
show more cards than that). Products are deduped by Magento product id (categories overlap).

Each card gives the product id (price box / cart form), name (ends with "(MPN)"), brand, final
price (data-price-amount) and the old price when on sale (compare_at_price), SKU (the store's
own, e.g. "6_P02468-B21_1") and MPN (data-product-mpn -> part_number), and the Add to Cart
button. Condition is read from the title when it says so, capacity from drive / memory titles
(upper-case GB / TB, not GB/s; not from whole computers' titles); category = the walked
category's name. No stock counts.
image_url = the card's photo; description stays empty (the listing cards carry none).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "ipcstore"
BASE = "https://ipcstore.com"
CATEGORIES = [
    # Subcategories first, so their products get the more specific category name (the first
    # category a product is found in wins); the parents below cover everything else.
    "computers-electronics/computer-components/system-components/memory-modules",
    "computers-electronics/computer-components/system-components/processors",
    "computers-electronics/data-storage/data-storage-devices/internal-hard-drives",
    "computers-electronics/data-storage/data-storage-devices/internal-solid-state-drives",
    "computers-electronics/networking/network-switches",
    "computers-electronics/networking/network-transceiver-modules",
    "computers-electronics/data-storage",
    "computers-electronics/computer-components",
    "computers-electronics/networking",
    "computers-electronics/computers/servers",
    "industrial-lab-equipment/electronic-components-supplies/electrical-equipment-supplies/"
    "power-conditioning/power-supply-units",
]
PAGE_SIZE = 36                # the store's default listing page size
WORKERS = 2
DELAY = 0.7                   # between pages of one category (the store answers in ~3-5 s)
TOTAL_RE = re.compile(r"of\s+([\d,]+)|^([\d,]+)\s+items?$", re.I)
RANGE_RE = re.compile(r"items\s+([\d,]+)\s*-\s*([\d,]+)", re.I)
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s?(TB|GB)\b(?!\s*/\s*s|ps)")
DRIVE_RE = re.compile(r"hard (?:disk )?drive|\bhdd\b|\bssd\b|solid state|\bnvme\b", re.I)
MEMORY_RE = re.compile(r"\bmemory\b|\bdimm\b|\brdimm\b|\blrdimm\b|\bsdram\b|\bddr\d", re.I)
# Whole computers ("Core i5 ... 8 GB RAM - 256 SSD") are not drives / memory
COMPUTER_RE = re.compile(r"\bGB\s+RAM\b|\b(?:xeon|core i\d|core ultra|ryzen|epyc|celeron|"
                         r"pentium|athlon|opteron|a-series)\b", re.I)
CONDITIONS = [(re.compile(r"\brefurbished\b|\brefurb\b|\brenewed\b", re.I), "Refurbished"),
              (re.compile(r"\bused\b|\bpre-?owned\b", re.I), "Used")]


def get(url):
    r = http.session().get(url, timeout=120)
    r.raise_for_status()
    return r.text


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def num(text):
    return int(text.replace(",", "")) if text else None


def toolbar(soup):
    """(total items, (first, last) shown on this page or None) from the listing toolbar."""
    el = soup.select_one(".toolbar-amount")
    text = clean(el.get_text()) if el else ""
    m = TOTAL_RE.search(text)
    total = num(m.group(1) or m.group(2)) if m else None
    r = RANGE_RE.search(text)
    return total, ((num(r.group(1)), num(r.group(2))) if r else None)


def price_of(card, kind):
    el = card.select_one(f'[data-price-type="{kind}"][data-price-amount]')
    try:
        return float(el["data-price-amount"]) if el else None
    except ValueError:
        return None


def parse_cards(soup, category):
    products = []
    for card in soup.select("li.product-item"):
        box = card.select_one("[data-price-box][data-product-id]")
        link = card.select_one("a.product-item-link")
        if not box or not link:
            continue                     # widgets such as the "recently viewed" names list
        form = card.select_one("form[data-role=tocart-form]") or {}
        price = price_of(card, "finalPrice")
        old = price_of(card, "oldPrice")
        brand = card.select_one(".list-brand")
        img = card.select_one("img.product-image-photo") or card.find("img")
        image = clean(img.get("src") or img.get("data-src")) if img else ""
        if image.startswith("//"):
            image = "https:" + image
        elif image.startswith("/"):
            image = BASE + image
        if "placeholder" in image.lower() or image.startswith("data:"):
            image = ""                   # Magento's "no image" placeholder, not a real photo
        products.append({
            "id": box["data-product-id"],
            "title": clean(link.get_text()),
            "url": clean(link.get("href")),
            "brand": clean(brand.get_text()) if brand else "",
            "sku": clean(form.get("data-product-sku")) if form else "",
            "mpn": clean(form.get("data-product-mpn")) if form else "",
            "category": category,
            "price": price or None,                     # $0 = no real price
            "compare": old if old and price and old > price else None,
            "available": bool(card.select_one("button.tocart")),
            "image": image or None,
        })
    return products


def fetch_page(path, page):
    """(soup, cards) of one listing page; a page whose cards don't match its toolbar raises."""
    url = f"{BASE}/{path}" + (f"?p={page}" if page > 1 else "")
    soup = BeautifulSoup(get(url), "html.parser")
    h1 = soup.select_one("h1.page-title, h1")
    cards = parse_cards(soup, clean(h1.get_text()) if h1 else "")
    total, shown = toolbar(soup)
    if shown:                           # "Items 37-65 of 742": 29 shown on this page
        if len(cards) != shown[1] - shown[0] + 1:
            raise RuntimeError(f"{url}: {len(cards)} product cards, the page says "
                               f"{shown[1] - shown[0] + 1}")
        if shown[0] != (page - 1) * PAGE_SIZE + 1:
            raise RuntimeError(f"{url}: page starts at item {shown[0]}, not page {page}")
    elif total is not None:             # one-page listing: "10 Items" counts hidden ones too
        if len(cards) > total:
            raise RuntimeError(f"{url}: {len(cards)} product cards, the page says {total}")
    elif cards or not soup.select_one(".message.info.empty"):
        raise RuntimeError(f"{url}: no item count on the page")
    else:
        print(f"WARN {url}: no products shown (all out of stock?)", flush=True)
    return soup, cards, total


def walk(path, limit=0):
    """Every product card of one category, pages 1..ceil(total / page size)."""
    _, cards, total = fetch_page(path, 1)
    pages = math.ceil((total or 0) / PAGE_SIZE)
    found = {c["id"]: c for c in cards}
    for page in range(2, pages + 1):
        if limit and len(found) >= limit:
            break
        time.sleep(DELAY)
        for c in fetch_page(path, page)[1]:
            found.setdefault(c["id"], c)
    print(f"{path}: {total} items, {pages} pages, {len(found)} shown", flush=True)
    return found


def build_row(p, scraped_at):
    title = p["title"]
    part = (DRIVE_RE.search(title) or MEMORY_RE.search(title)) and not COMPUTER_RE.search(title)
    cap = CAPACITY_RE.search(title) if part else None
    tb = None
    if cap and DRIVE_RE.search(title):
        tb = float(cap.group(1)) if cap.group(2) == "TB" else float(cap.group(1)) / 1000
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": str(p["id"]),
        "sku": p["sku"],
        "part_number": p["mpn"],
        "title": title,
        "brand": p["brand"],
        "category": p["category"],
        "condition": next((c for rx, c in CONDITIONS if rx.search(title)), ""),
        "capacity": f"{cap.group(1)}{cap.group(2)}" if cap else "",
        "price": p["price"],
        "compare_at_price": p["compare"],
        "price_per_tb": round(p["price"] / tb, 2) if p["price"] and tb else None,
        "available": p["available"],
        "stock_qty": None,                 # not shown on the listing
        "image_url": p["image"],
        # description lives only on the product page; the listing cards (all we fetch) carry none
        "description": None,
        "url": p["url"],
    })


def scrape(limit, scraped_at):
    by_id = {}
    cats = CATEGORIES[:1] if limit else CATEGORIES
    with ThreadPoolExecutor(WORKERS) as pool:
        for found in pool.map(lambda c: walk(c, limit), cats):
            for pid, p in found.items():
                by_id.setdefault(pid, p)
            print(f"{len(by_id)} unique products so far", flush=True)
    items = list(by_id.values())[:limit] if limit else list(by_id.values())
    # The category totals include out-of-stock products the listing never shows (see above)
    return [build_row(p, scraped_at) for p in items], None


if __name__ == "__main__":
    runner.run(SITE, scrape)
