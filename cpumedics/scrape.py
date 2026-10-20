"""Scrape cpumedics.com (BigCommerce store, ~197k products): every run's full snapshot appended
to Postgres.

The bare domain is Cloudflare-blocked from some hosts, so the store is used over www
(https://www.cpumedics.com). It has no storefront GraphQL token in its pages (so
common/bigcommerce.py cannot be used), and its category tree is ~6,700 deeply nested categories,
so products are read the same way as flagshiptech: from BigCommerce's own product comparison page,
which renders any list of product ids given in its URL (/compare/1/2/3/...). The store's product
ids are spread over roughly 118,000-355,000 with gaps, so the scraper asks for 200 ids at a time
across that whole range; the top is found each run by probing upwards (the catalogue can grow),
and nothing is skipped in the middle, so a gap in the id space cannot end the scan early.

Each comparison column is one product. Its id is the one missing from its own "remove" link (that
link lists every other product on the page), with the product image path (/products/<id>/) as a
fallback. The column gives the title (which starts with or contains the part number), URL and
price; the buy button says whether it is in stock:
  "Add to Cart" / "Choose Options"   in stock
  anything else / none               out of stock
Many products are priced on request ("Request a Quote"): they show no price (saved empty) but are
still in stock. The page shows no brand, SKU, category or stock count, so those stay blank;
capacity and price-per-TB are read from the title when present. The snapshot is checked against
the product count of the store's sitemap.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
import time
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "cpumedics"
BASE = "https://www.cpumedics.com"
CHUNK = 200                   # product ids per comparison page
WORKERS = 3
DELAY = 0.3                   # between batches of requests, to stay polite
START = 90000                 # first id to scan on a full run (ids begin ~118,000)
SEED = 120000                 # a known-populated id, used to find the top and for test runs
IN_STOCK_BUTTONS = {"add to cart", "choose options", "buy now"}
PRICE_RE = re.compile(r"[\d,]+(?:\.\d+)?")
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b", re.I)
PART_TOKEN_RE = re.compile(r"^[A-Za-z]*\d[A-Za-z0-9]*(?:[-/.][A-Za-z0-9]+)*$")


def get(url):
    r = http.session().get(url, timeout=180)
    r.raise_for_status()
    return r


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ").replace("&nbsp;", " ")).strip()


def sitemap_count():
    """Product URLs across the store's product sitemap pages (its full catalogue size)."""
    index = get(f"{BASE}/xmlsitemap.php").text
    pages = sorted(set(re.findall(r"xmlsitemap\.php\?type=products&(?:amp;)?page=(\d+)", index)),
                   key=int)
    return sum(get(f"{BASE}/xmlsitemap.php?type=products&page={p}").text.count("<loc>")
               for p in pages)


def money(card, attr):
    el = card.select_one(f"[{attr}]")
    m = PRICE_RE.search(el.get_text() if el else "")
    return float(m.group(0).replace(",", "")) if m else None


def part_number(title):
    for token in title.split():
        t = token.strip("*()[]:,")
        if len(t) >= 4 and any(c.isdigit() for c in t) and PART_TOKEN_RE.match(t):
            return t
    return ""


def parse_compare(html):
    """[{fields}] for every product column of a comparison page."""
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("tr.compareTable-row")
    if not rows:
        return []
    cards = rows[0].select("th.compareTable-product")
    if not cards:
        return []
    actions = rows[1].select("td.compareTable-action") if len(rows) > 1 else []

    def link_ids(card):
        a = card.select_one("a.compareTable-removeProduct")
        tail = (a.get("href") or "").split("/compare/", 1)[-1] if a else ""
        return {x for x in tail.strip("/").split("/") if x.isdigit()}

    links = [link_ids(c) for c in cards]
    on_page = set().union(*links) if links else set()
    products = []
    for i, card in enumerate(cards):
        mine = on_page - links[i]
        if len(mine) == 1:
            pid = mine.pop()
        else:                                   # single-product page: id from the image path
            m = re.search(r"/products/(\d+)/", str(card))
            pid = m.group(1) if m else None
        if not pid:
            print("WARN skipped a comparison column whose product id is unknown", flush=True)
            continue
        link = card.select_one(".card-title a")
        if link:
            title, url = clean(link.get_text()), clean(link.get("href"))
        else:
            fig = card.select_one("a.card-figure__link")
            title = clean((fig.get("aria-label") or "").split(",$")[0]) if fig else ""
            url = clean(fig.get("href")) if fig else ""
        price = money(card, "data-product-price-without-tax")
        compare = max((x for x in (money(card, "data-product-rrp-price-without-tax"),
                                   money(card, "data-product-non-sale-price-without-tax")) if x),
                      default=None)
        button = clean(actions[i].get_text()).lower() if i < len(actions) else ""
        products.append({
            "id": pid,
            "title": title,
            "url": url,
            "price": price,
            "compare": compare if compare and price and compare > price else None,
            "available": button in IN_STOCK_BUTTONS,
        })
    return products


def fetch_chunk(start):
    ids = "/".join(str(i) for i in range(start, start + CHUNK))
    return parse_compare(get(f"{BASE}/compare/{ids}").text)


def populated(start, n=60):
    ids = "/".join(str(i) for i in range(start, start + n))
    return len(parse_compare(get(f"{BASE}/compare/{ids}").text)) > 0


def find_ceiling(seed=SEED, step=20000, clear=3):
    """Highest id to scan: probe upwards from a populated seed until `clear` empty windows in a
    row (wide enough to step over any gap in the id space); the catalogue can grow between runs."""
    hi, k, empty = seed, 1, 0
    while empty < clear:
        start = seed + k * step
        if populated(start):
            hi, empty = start, 0
        else:
            empty += 1
        k += 1
    return hi + step


def build_row(p, scraped_at):
    cap = CAPACITY_RE.search(p["title"])
    capacity = f"{cap.group(1)}{cap.group(2).upper()}" if cap else ""
    tb = None
    if cap:
        tb = float(cap.group(1)) if cap.group(2).upper() == "TB" else float(cap.group(1)) / 1000
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": str(p["id"]),
        "part_number": part_number(p["title"]),
        "title": p["title"],
        "category": "",
        "capacity": capacity,
        "price": p["price"],
        "compare_at_price": p["compare"],
        "price_per_tb": round(p["price"] / tb, 2) if p["price"] and tb else None,
        "available": p["available"],
        "stock_qty": None,                 # not shown on the comparison page
        "url": p["url"],
    })


def scrape(limit, scraped_at):
    listed = sitemap_count()
    print(f"sitemap lists {listed} products", flush=True)
    if limit:
        lo, hi = SEED, None                # test run: from a populated id until limit is reached
    else:
        lo, hi = START, find_ceiling()
        print(f"scanning ids {lo}..{hi}", flush=True)
    by_id, start = {}, lo
    with ThreadPoolExecutor(WORKERS) as pool:
        while True:
            starts = [start + k * CHUNK for k in range(WORKERS * 2)]
            if hi is not None:
                starts = [s for s in starts if s <= hi]
            if not starts:
                break
            for products in pool.map(fetch_chunk, starts):
                for p in products:
                    by_id[p["id"]] = p
            start = starts[-1] + CHUNK
            print(f"ids up to {start - 1}: {len(by_id)} products", flush=True)
            if limit and len(by_id) >= limit:
                break
            time.sleep(DELAY)
    items = list(by_id.values())[:limit] if limit else list(by_id.values())
    return [build_row(p, scraped_at) for p in items], listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
