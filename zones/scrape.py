"""Scrape zones.com (Zones, a US IT reseller; Java storefront behind Akamai, search by Lucidworks),
IT categories only (~5-9k products): every run's full snapshot appended to Postgres.

US ONLY: every zones.com host answers HTTP 403 "Access Denied" (Akamai) from Pakistan; from US
networks the pages load with no challenge. So this only works on a US runner (GitHub Actions).
It was built from archived copies of the pages (Wayback Machine captures of the listings,
2026-03..06; a Common Crawl capture of a product page, 2026-09), so it checks the live format
hard and stops with a clear message (plus a raw snippet in the log) when anything is off.

Listing = the store's own browse page (robots.txt allows /site/locate/refine.html; it disallows
/site/productinventory/ and any URL with "zone=", neither is used):
  /site/locate/refine.html?page_number=N&txt_search=*
      &fq=categoryName_hier_ss:<facet>&sort_by=Mfr_part_no_s|asc
20 products per page (there is no page-size option). sort_by is the page's own "Manufacturer
Part#: A-Z" option (its pagination links append &sort_by=... the same way), so the order does not
shift between pages the way "Best Match" might. Scope (the category facets the site itself shows,
facet counts on 2026-02..04 in brackets; the US research on 2026-10-20 saw Memory 997,
Networking 6,524, Servers 153, Storage & Backup 1,106):
  1//Products//Memory (626)            1//Products//Networking (3,290)
  1//Products//Servers (152)           1//Products//Storage & Backup (1,135)
  2//Products//Accessories//Computer Components (510: processors, motherboards, GPUs, fans ...)
  2//Products//Accessories//Server & Networking Accessories
Products in more than one category are kept once (the first category in CATEGORIES names it).
A core category (the first four) that shows "We Found 0 Results" stops the run; the two Accessories
sub-categories are skipped with a warning instead.

Every page is checked: its "Showing: A - B of N items" label must start at (page - 1) * 20 + 1,
the page must show product cards (and no more than B - A + 1). A page may show fewer cards than
its label (the 2026-04 Storage Media capture showed 19 for "1 - 20"): those missing slots are
counted and printed. After each category the unique products found must reach the smallest "of N"
the category showed, within max(SHORT_MIN, 2%); else the category is walked a second time and the
two walks are merged, and if it is still short the run stops. listed = the sum of the categories'
"of N" minus the products found in more than one category.

Each card gives: the Zones product id (the ?id= of its link, e.g. "105415317" / "301261062NEW",
the same as its hidden EDP number) -> product_id; "Item #" (Zones SKU) -> sku; "Mfr #" ->
part_number; the name "<Brand> - <name>" -> title and brand (when the name is empty or only repeats the brand,
"Cisco - Cisco", the description names the product and becomes the title); the card's short description -> description; the image -> image_url; the unit price ("$1,026.99" -> price;
anything without a $ amount, e.g. a quote / call text -> price empty, never 0, and the texts are
printed at the end). Zones sells new stock: condition stays empty unless the name / description
says refurbished. Capacity is read from Memory / Storage names.

Self-checks that stop the run: a 403 / "Access Denied" page; a page without the "Showing" label or
without cards; a card without a product link; Mfr # missing on more than 10% of a page's cards;
no $ price on most cards of a page; the category breadcrumb not matching the facet asked for.
Test runs (MAX_PRODUCTS) read page 1 of every category and then compare the listing price of the
first PRICE_CHECKS products with their product pages (div.prod-price); any mismatch stops the run.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import html as htmllib
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import parse_qs, quote_plus, urlsplit

import requests
from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "zones"
BASE = "https://www.zones.com"
LIST = f"{BASE}/site/locate/refine.html"
CATEGORIES = [                       # (categoryName_hier_ss facet value, category name)
    ("1//Products//Memory", "Memory"),
    ("1//Products//Networking", "Networking"),
    ("1//Products//Servers", "Servers"),
    ("1//Products//Storage & Backup", "Storage & Backup"),
    ("2//Products//Accessories//Computer Components", "Computer Components"),
    ("2//Products//Accessories//Server & Networking Accessories",
     "Server & Networking Accessories"),
]
CORE = CATEGORIES[:4]                # a missing core category stops the run; the others warn
SORT = "Mfr_part_no_s|asc"           # "Manufacturer Part#: A-Z"; None = the site's "Best Match"
PAGE_SIZE = 20
WORKERS = 3                          # requests in flight
DELAY = 1.0                          # per worker, after each request
TRIES = 3                            # attempts for stalled / cut-off downloads
RETRY_PAUSE = 30
SHORT_MIN = 5                        # a category may come up this many (or 2%) short
PRICE_CHECKS = 3                     # test runs: listing price vs product page
COUNT_RE = re.compile(r"Showing:\s*([\d,]+)\s*-\s*([\d,]+)\s*of\s*([\d,]+)", re.I)
PRICE_RE = re.compile(r"\$\s*([\d,]+(?:\.\d+)?)")
CAPACITY_RE = re.compile(r"(?<![\d.])(\d+(?:\.\d+)?)\s?(TB|GB)\b(?!\s*/\s*s|ps)")
DRIVE_RE = re.compile(r"hard (?:disk )?drive|\bhdd\b|\bssd\b|solid state|\bnvme\b", re.I)
REFURB_RE = re.compile(r"\brefurb", re.I)
CAPACITY_CATEGORIES = {"Memory", "Storage & Backup"}


class Blocked(RuntimeError):
    """zones.com refused this network (Akamai 403)."""


class NoResults(RuntimeError):
    """A category facet that matches nothing (renamed / removed)."""


def get(url):
    """GET with http's 429 / 5xx retries, plus a few tries for a download the server stalls or
    cuts off part-way. A 403 / Access Denied page raises at once (it is not worked around)."""
    for attempt in range(1, TRIES + 1):
        try:
            r = http.session().get(url, timeout=(30, 120))
            if r.status_code == 403 or "<TITLE>Access Denied</TITLE>" in r.text[:500]:
                raise Blocked(f"{url}: HTTP {r.status_code} Access Denied (Akamai) - zones.com "
                              "blocks this network/country; run this scraper on a US runner. "
                              f"Page starts: {snippet(r.text, 300)}")
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            if attempt == TRIES:
                raise
            print(f"WARN {url[:120]}: {e.__class__.__name__}, retrying in {RETRY_PAUSE}s",
                  flush=True)
            time.sleep(RETRY_PAUSE)


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def snippet(html, n=1500):
    """The start of the page body as plain-ish text, for error messages in the GitHub log."""
    body = re.split(r"<body[^>]*>", html, maxsplit=1, flags=re.I)[-1]
    return clean(htmllib.unescape(re.sub(r"<script.*?</script>|<style.*?</style>|<[^>]+>", " ",
                                         body, flags=re.S | re.I)))[:n]


def money(text):
    m = PRICE_RE.search(text or "")
    value = float(m.group(1).replace(",", "")) if m else None
    return value or None                         # no $ amount / $0 = no real price


def page_url(facet, page):
    parts = [f"page_number={page}"] if page > 1 else []
    parts += ["txt_search=*", "fq=" + quote_plus(f"categoryName_hier_ss:{facet}")]
    if SORT:
        parts.append("sort_by=" + quote_plus(SORT))
    return f"{LIST}?{'&'.join(parts)}"


def text_of(card, selector):
    el = card.select_one(selector)
    return clean(el.get_text(" ")) if el else ""


def parse_card(card):
    link = card.select_one("a[href*='/site/product/index.html?id=']")
    if not link:
        raise RuntimeError(f"product card without a product link: {clean(card.get_text(' '))[:300]}")
    url = link["href"].strip()
    if url.startswith("/"):
        url = BASE + url
    pid = (parse_qs(urlsplit(url).query).get("id") or [""])[0].strip()
    if not pid:
        raise RuntimeError(f"product link without an id: {url}")
    desc_el = card.select_one(".product-description")
    if desc_el:
        for more in desc_el.select("a.more-details, a.less-details"):
            more.extract()
    img = card.select_one("a.product-image img")
    image = (img.get("src") or "").strip() if img else ""
    if image.startswith("//"):
        image = "https:" + image
    elif image.startswith("/"):
        image = BASE + image
    return {
        "id": pid,
        "edp": text_of(card, "a.zcd-edp-number"),
        "url": f"{BASE}/site/product/index.html?id={pid}",
        "title": text_of(card, "section a.title") or text_of(card, "a.ga_ec_p_name"),
        "mfr": text_of(card, "a.mfr_text"),
        "item": text_of(card, "a.zcd-sku-number"),
        "description": clean(desc_el.get_text(" ")) if desc_el else "",
        "price_text": text_of(card, ".product-price"),
        "image": "" if "generic200" in image else image,
    }


def parse_page(html):
    """{label: (A, B, N) or None, cards: [card dicts], crumbs: [...], sort: str, zero: bool}."""
    soup = BeautifulSoup(html, "html.parser")
    # real cards carry ga_ec_p; a hidden display:none template card ("ga_detector") does not
    cards = [parse_card(c) for c in soup.select("div.product.js-product.ga_ec_p")]
    label = soup.select_one(".serp-item-count")
    m = COUNT_RE.search(clean(label.get_text(" "))) if label else None
    sort_el = soup.select_one(".serp-sort .select-styled")
    return {
        "label": tuple(int(x.replace(",", "")) for x in m.groups()) if m else None,
        "cards": cards,
        "crumbs": [clean(c.get_text(" ")) for c in soup.select(".serp-breadcrumbs .dimval_crumb")],
        "sort": clean(sort_el.get_text(" ")) if sort_el else "",
        "zero": "We Found 0 Results" in html,
    }


def check_page(url, html, parsed, page, facet):
    """Raise when the page is not the listing page asked for; returns the number of card slots
    the label promises but the page does not show."""
    label, cards = parsed["label"], parsed["cards"]
    if parsed["zero"]:
        raise NoResults(f"{url}: 'We Found 0 Results' - the category facet {facet!r} matched "
                           "nothing (renamed?)")
    if not label:
        raise RuntimeError(f"{url}: no 'Showing: A - B of N items' label ({len(cards)} cards); "
                           f"page format changed? Page text: {snippet(html)}")
    first, last, total = label
    start = (page - 1) * PAGE_SIZE + 1
    if first != start or last < first:
        raise RuntimeError(f"{url}: label says {first}-{last} of {total}, page {page} should "
                           f"start at {start}")
    if not cards:
        raise RuntimeError(f"{url}: label says {first}-{last} of {total} but no product cards "
                           f"parsed; card markup changed? Page text: {snippet(html)}")
    if len(cards) > last - first + 1:
        raise RuntimeError(f"{url}: {len(cards)} cards but the label says {first}-{last}")
    want = facet.rsplit("//", 1)[-1]
    if page == 1 and parsed["crumbs"] and want.lower() not in [c.lower() for c in parsed["crumbs"]]:
        raise RuntimeError(f"{url}: breadcrumb {parsed['crumbs']} does not show {want!r}; the "
                           "category filter was not applied")
    no_mfr = sum(1 for c in cards if not c["mfr"])
    if no_mfr > max(1, len(cards) // 10):
        raise RuntimeError(f"{url}: {no_mfr} of {len(cards)} cards without 'Mfr #'; card markup "
                           f"changed? First card: {cards[0]}")
    no_price = sum(1 for c in cards if money(c["price_text"]) is None)
    if no_price > len(cards) // 2:
        raise RuntimeError(f"{url}: {no_price} of {len(cards)} cards without a $ price; price "
                           f"markup changed? Price texts: {[c['price_text'] for c in cards[:5]]}")
    return last - first + 1 - len(cards)


def fetch_page(cat, page):
    """(cards, total, missing slots, sort label) of one listing page."""
    facet, _ = cat
    url = page_url(facet, page)
    html = get(url).text
    parsed = parse_page(html)
    try:
        missing = check_page(url, html, parsed, page, facet)
    finally:
        time.sleep(DELAY)
    return parsed["cards"], parsed["label"][2], missing, parsed["sort"]


def walk_category(pool, cat, first=None):
    """{product id: card} of one category, plus the smallest "of N" it showed and the missing
    card slots. first = page 1's result when it is already fetched."""
    facet, name = cat
    first = first or fetch_page(cat, 1)
    results = [first]
    total = first[1]
    pages = math.ceil(total / PAGE_SIZE)
    results += list(pool.map(lambda p: fetch_page(cat, p), range(2, pages + 1)))
    grown = max(r[1] for r in results)
    if math.ceil(grown / PAGE_SIZE) > pages:     # products added while walking
        extra = range(pages + 1, math.ceil(grown / PAGE_SIZE) + 1)
        results += list(pool.map(lambda p: fetch_page(cat, p), extra))
    found = {}
    for cards, _, _, _ in results:
        for c in cards:
            found.setdefault(c["id"], c)
    return found, min(r[1] for r in results), sum(r[2] for r in results)


def walk(pool, limit):
    """[(category name, {id: card}, shown total)] for every category."""
    def first_page(cat):
        try:
            return fetch_page(cat, 1)
        except NoResults as e:
            if cat in CORE:
                raise
            print(f"WARN skipping {cat[1]}: {e}", flush=True)
            return None

    pairs = [(c, f) for c, f in zip(CATEGORIES, pool.map(first_page, CATEGORIES)) if f]
    for (facet, name), (cards, total, missing, sort) in pairs:
        print(f"{name}: {total} products, {math.ceil(total / PAGE_SIZE)} pages "
              f"(sort shown: {sort!r}); first: {[(c['mfr'], c['price_text']) for c in cards[:3]]}",
              flush=True)
    out = []
    for cat, first in pairs:
        name = cat[1]
        if limit:
            out.append((name, {c["id"]: c for c in first[0]}, first[1]))
            continue
        found, shown, missing = walk_category(pool, cat, first)
        slack = max(SHORT_MIN, int(shown * 0.02))
        if shown - len(found) > slack:
            print(f"WARN {name}: {len(found)} unique of {shown} (pages short {missing} cards); "
                  "walking the category again", flush=True)
            again, shown2, missing = walk_category(pool, cat)
            found.update({k: v for k, v in again.items() if k not in found})
            shown = min(shown, shown2)
        print(f"{name}: {len(found)} unique products of {shown} (pages short {missing} cards)",
              flush=True)
        if shown - len(found) > slack:
            raise RuntimeError(f"{name}: only {len(found)} unique products of {shown} after two "
                               "walks; the listing order / paging is not reliable")
        out.append((name, found, shown))
    return out


def product_page_price(url):
    """The price on a product page (div.prod-price in its price box), or None."""
    soup = BeautifulSoup(get(url).text, "html.parser")
    el = soup.select_one(".price-box-list .prod-price") or soup.select_one(".prod-price")
    return money(el.get_text(" ")) if el else None


def price_check(items):
    """Test runs: the listing price of the first priced products must equal their product page's."""
    checked = 0
    for c in items:
        price = money(c["price_text"])
        if price is None:
            continue
        page_price = product_page_price(c["url"])
        time.sleep(DELAY)
        print(f"price check {c['url']}: listing {price}, product page {page_price}", flush=True)
        if page_price != price:
            raise RuntimeError(f"{c['url']}: listing shows {price} but the product page shows "
                               f"{page_price}; the listing price is not the price a visitor "
                               "sees on the product page")
        checked += 1
        if checked >= PRICE_CHECKS:
            break


def brand_of(title):
    """Names are "<Brand> - <name>"."""
    if " - " in title:
        brand = title.split(" - ", 1)[0].strip()
        if brand and len(brand.split()) <= 5:
            return brand
    return ""


def title_of(c):
    """The card name; when it is empty or only repeats the brand ("Cisco - Cisco", "IBM - IBM"),
    the description (which then names the product) prefixed with the brand."""
    title, desc = c["title"], c["description"]
    brand = brand_of(title)
    name = title.split(" - ", 1)[1].strip() if brand else title
    if desc and (not name or (len(name.split()) <= 2 and len(desc) > len(name) + 10)):
        if brand and not desc.lower().startswith(brand.lower()):
            return f"{brand} - {desc}"
        return desc
    return title


def build_row(c, category, scraped_at):
    title = title_of(c)
    price = money(c["price_text"])
    text = f"{title} {c['description']}"
    cap = CAPACITY_RE.search(title) if category in CAPACITY_CATEGORIES else None
    tb = None
    if cap and category == "Storage & Backup" and DRIVE_RE.search(text):
        tb = float(cap.group(1)) if cap.group(2) == "TB" else float(cap.group(1)) / 1000
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": c["id"],
        "sku": c["item"],
        "part_number": c["mfr"],
        "title": title,
        "brand": brand_of(c["title"]),
        "category": category,
        "condition": "Refurbished" if REFURB_RE.search(text) else "",
        "capacity": f"{cap.group(1)}{cap.group(2)}" if cap else "",
        "price": price,
        "price_per_tb": round(price / tb, 2) if price and tb else None,
        "available": None,                   # stock is only in the robots-disallowed inventory call
        "stock_qty": None,
        "image_url": c["image"] if c["image"].startswith("http") else None,
        "description": schema.plain_text(c["description"]),     # the card's (short) description
        "url": c["url"],
    })


def scrape(limit, scraped_at):
    with ThreadPoolExecutor(WORKERS) as pool:
        cats = walk(pool, limit)
    by_id, category_of, overlap = {}, {}, 0
    for name, found, _ in cats:
        for pid, c in found.items():
            if pid in by_id:
                overlap += 1
                continue
            by_id[pid] = c
            category_of[pid] = name
    listed = sum(shown for _, _, shown in cats) - overlap
    texts = {}
    for c in by_id.values():
        if money(c["price_text"]) is None:
            texts[c["price_text"]] = texts.get(c["price_text"], 0) + 1
    print(f"{len(by_id)} unique products ({overlap} in more than one category); "
          f"cards without a $ price: {texts or 'none'}", flush=True)
    for c in list(by_id.values())[:3]:
        print(f"sample: {c}", flush=True)
    items = list(by_id.values())
    if limit:
        items = items[:limit]
        price_check(items)
        listed = None
    return [build_row(c, category_of[c["id"]], scraped_at) for c in items], listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
