"""Scrape aztekcomputers.com (Aztek Computers, custom PHP store): every run's snapshot appended to
Postgres.

The old aztekc.com store moved here. The catalogue is huge (4M+ SKUs the site claims), far too
large to take whole, so this scraper walks a fixed set of the main server and storage leaf
categories. Each category lists 50 products a page at /{slug} (page 1) and /{slug}/{N} (page N),
and every product is rendered twice - a grid card and a list card. The list card
(.wa-list-caption) carries every field cleanly:

  MFG: Intel · MFG. Part: BX8071514100 · SKU: UETFYNWZP6 · UPC: 735858547512 · Condition: New
  price (with an optional <del> "was" price)  ·  h2 > a = title + product URL

The internal SKU is the product id. A product can sit in more than one category, so rows are
de-duplicated by SKU. Because this is a curated slice of the catalogue (not the whole store) and
rows are de-duplicated across categories, there is no single trustworthy "listed" total to gate a
full run on, so the scraper returns listed = None and instead walks each category to its reported
last page and aborts on any page that fails after retries. MAX_PRODUCTS caps a test run.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import json
import math
import re
import sys
import time

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "aztekc"
BASE = "https://www.aztekcomputers.com"
PER_PAGE = 50
DELAY = 0.3
PRICE_RE = re.compile(r"[\d,]+(?:\.\d+)?")
COUNT_RE = re.compile(r"of\s+([\d,]+)\s+item", re.I)

# The main server / storage leaf categories (slugs under aztekcomputers.com/{slug}).
CATEGORIES = [
    "computers-servers-server-components-server-cpus-processors",
    "computers-servers-server-components-server-memory-ram",
    "computers-servers-server-components-disk-controllers-raid-cards",
    "computers-storage-hard-drives-internal-hard-disk-drives",
    "computers-storage-hard-drives-solid-state-drives",
    "computers-networking",
]


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def money(text):
    m = PRICE_RE.search(text or "")
    return float(m.group(0).replace(",", "")) if m else None


def absolute(u):
    """Absolute https URL for a possibly relative/protocol-relative one, else None."""
    u = (u or "").strip()
    if not u or u.startswith("data:"):
        return None
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return BASE + u
    return u


def get(url):
    r = http.session().get(url, timeout=60)
    r.raise_for_status()
    return r.text


def page_url(slug, page):
    return f"{BASE}/{slug}" if page == 1 else f"{BASE}/{slug}/{page}"


def category_name(html, slug):
    """The category's human name from the page's BreadcrumbList JSON-LD, or a cleaned slug."""
    for block in re.findall(r'<script type="application/ld\+json">(.*?)</script>', html, re.S):
        try:
            data = json.loads(block)
        except ValueError:
            continue
        if isinstance(data, dict) and data.get("@type") == "BreadcrumbList":
            items = data.get("itemListElement") or []
            if items:
                return clean(items[-1].get("name"))
    return slug.split("-")[-1].title()


def parse_cards(html, scraped_at, category=""):
    """[product_row] for the list cards on one category page."""
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for card in soup.select(".wa-list-caption"):
        a = card.select_one("h2 a")
        if not a:
            continue
        price_el = card.select_one(".price")
        compare = None
        if price_el:
            del_el = price_el.select_one("del")      # a visible "was" price
            if del_el:
                compare = money(del_el.get_text())
                del_el.extract()
            price = money(price_el.get_text())
        else:
            price = None
        fields = {}
        for span in card.select("p span"):
            txt = clean(span.get_text())
            if ":" in txt:
                k, v = txt.split(":", 1)
                fields[k.strip().lower()] = v.strip()
        sku = fields.get("sku", "")
        url = a.get("href", "")
        pid = sku or url.rstrip("/").rsplit("/", 1)[-1]
        if not pid:
            continue
        # The product thumbnail sits in the same .wa-products wrapper as this list caption.
        wrapper = card.find_parent(class_="wa-products")
        img = wrapper.find("img") if wrapper else None
        image_url = absolute(img.get("src")) if img else None
        if image_url and "no-image" in image_url.lower():
            image_url = None             # the store's "no image" placeholder, not a real photo
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": SITE,
            "product_id": pid,
            "sku": sku,
            "part_number": fields.get("mfg. part", ""),
            "title": clean(a.get("title") or a.get_text()),
            "brand": fields.get("mfg", ""),
            "category": category,
            "condition": fields.get("condition", ""),
            "other_specs": f"UPC: {fields['upc']}" if fields.get("upc") else "",
            "price": price,
            "compare_at_price": compare if compare and price and compare > price else None,
            "available": price is not None,
            "image_url": image_url,
            # description lives only on the product page; these category listings (all we fetch)
            # carry none, and this is a huge catalogue, so a per-product fetch is out.
            "description": None,
            "url": url,
        }))
    return rows


def scrape_category(slug, scraped_at, limit, seen, rows):
    first = get(page_url(slug, 1))
    m = COUNT_RE.search(re.sub(r"<[^>]+>", " ", first))
    total = int(m.group(1).replace(",", "")) if m else None
    last_page = math.ceil(total / PER_PAGE) if total else None
    category = category_name(first, slug)
    print(f"[{slug}] {total} items ({last_page} pages) as '{category}'", flush=True)

    def add(page_rows):
        for row in page_rows:
            pid = row[schema.PRODUCT_COLUMNS.index("product_id")]
            if pid not in seen:
                seen.add(pid)
                rows.append(row)

    add(parse_cards(first, scraped_at, category))
    page = 2
    while not (limit and len(rows) >= limit):
        if last_page and page > last_page:
            break
        try:
            page_rows = parse_cards(get(page_url(slug, page)), scraped_at, category)
        except Exception as e:                       # noqa: BLE001
            sys.exit(f"[{slug}] page {page} failed: {e}")
        if not page_rows:
            break
        add(page_rows)
        page += 1
        time.sleep(DELAY)


def scrape(limit, scraped_at):
    rows, seen = [], set()
    for slug in CATEGORIES:
        if limit and len(rows) >= limit:
            break
        scrape_category(slug, scraped_at, limit, seen, rows)
        print(f"  running total: {len(rows)} unique products", flush=True)
    rows = rows[:limit] if limit else rows
    # No reliable global count for a curated, de-duplicated slice (see module docstring).
    return rows, None


if __name__ == "__main__":
    runner.run(SITE, scrape)
