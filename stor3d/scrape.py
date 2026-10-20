"""Scrape 3dstor-shop.com (3DSTOR, custom ASP.NET store: data-center GPUs, server memory and
storage): every run's full snapshot appended to Postgres. Prices are in US dollars (USD),
stored as-is.

NOTE: the package is "stor3d" because a Python module name cannot start with a digit; the site is
3dstor-shop.com. The store is US-hosted and can be slow / unreachable from some regions -- if a
run fails to reach it, it needs a US runner.

The catalogue is small and fully server-rendered under /Product/List, paged with ?page=N (10
products a page, a <a class="product-list-item"> card each). Page 1 declares the total in the
pager script (totalList: N), used as the completeness count. Each card carries the
/Product/Details/<id> link (the id is product_id), the <h3> title, a "P/N : ..." highlight, an
"Availability: In Stock" line and the price block (.a = list price, .b = selling price).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "stor3d"
BASE = "https://www.3dstor-shop.com"
PID_IDX = schema.PRODUCT_COLUMNS.index("product_id")
ID_RE = re.compile(r"/Product/Details/(\d+)")
USD_RE = re.compile(r"\$?\s*([\d,]+(?:\.\d+)?)")
TOTAL_RE = re.compile(r"totalList:\s*(\d+)")
PN_RE = re.compile(r"P/?N\s*[:：]\s*(.+)", re.I)


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def usd(text):
    m = USD_RE.search((text or "").replace(",", ""))
    try:
        return float(m.group(1)) if m else None
    except (TypeError, ValueError):
        return None


def parse_card(card, scraped_at):
    href = card.get("href") or ""
    m = ID_RE.search(href)
    if not m:
        return None
    product_id = m.group(1)
    h3 = card.select_one("h3")
    title = clean(h3.get_text()) if h3 else ""

    price_box = card.select_one(".product-list-item-price")
    price = compare = None
    if price_box:
        b = price_box.select_one("p.b")
        a = price_box.select_one("p.a")
        price = usd(b.get_text()) if b else None
        compare = usd(a.get_text()) if a else None
    if price is None:
        price = compare                  # some cards show a single price (.a only)

    right = card.select_one(".product-list-item-right")
    avail_text = ""
    if right:
        for p in right.find_all("p"):
            if "availab" in p.get_text().lower():
                avail_text = p.get_text().lower()
                break

    part_number, specs = "", []
    for span in card.select(".product-list-item-info span"):
        t = clean(span.get_text()).lstrip("• ").strip()
        if not t:
            continue
        pn = PN_RE.match(t)
        if pn and not part_number:
            part_number = clean(pn.group(1)).split(",")[0]
        else:
            specs.append(t)

    return schema.product_row({
        "scraped_at": scraped_at, "website": SITE,
        "product_id": product_id, "sku": product_id,
        "part_number": part_number,
        "title": title,
        "brand": title.split()[0] if title else "",
        "price": price,
        "compare_at_price": compare,
        "available": "in stock" in avail_text,
        "other_specs": "; ".join(specs),
        "url": BASE + href if href.startswith("/") else href,
    })


def scrape(limit, scraped_at):
    rows, seen, listed = [], set(), None
    page = 1
    while page <= 200:
        try:
            r = http.session().get(f"{BASE}/Product/List?page={page}", timeout=90)
            r.raise_for_status()
        except Exception as e:  # noqa: BLE001
            print(f"WARN page {page}: {e}", flush=True)
            break
        if listed is None:
            m = TOTAL_RE.search(r.text)
            listed = int(m.group(1)) if m else None
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select("a.product-list-item")
        if not cards:
            break
        for card in cards:
            row = parse_card(card, scraped_at)
            if row:
                pid = row[PID_IDX]
                if pid not in seen:
                    seen.add(pid)
                    rows.append(row)
        print(f"page {page}: {len(cards)} cards, {len(rows)} products", flush=True)
        if limit and len(rows) >= limit:
            break
        page += 1
    return (rows[:limit] if limit else rows), listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
