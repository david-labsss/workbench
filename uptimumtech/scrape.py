"""Scrape uptimumtech.in (Uptimum Tech, custom Laravel store, India): every run's full snapshot
appended to Postgres. Prices are in Indian rupees (INR), stored as-is.

The store sells a wide range (IT hardware, networking, devices) alongside quote-only service
offerings (AI, automation, managed IT, ...). There is no sitemap or product API, so the shop is
enumerated from its navigation: every category / subcategory page (/p/<cat> and /p/<cat>/<sub>,
linked from every page's menu) is crawled, following Laravel pagination (?page=N, 30 products a
page, a <a rel="next"> link while more pages remain). Each product card (.single-product-wrap)
carries everything needed:
  * the product link /p/d/<slug>-<id> -> the trailing <id> is the product_id;
  * the title (<h2> link) and the brand (.product-category);
  * the price (.product-price, "Rs5,999.00") and the struck-through .old-price.
Service / quote-only offerings render as plain category pages with NO product cards (and no
price), so they drop out naturally. A card without a firm rupee price is skipped.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "uptimumtech"
BASE = "https://www.uptimumtech.in"
WORKERS = 6
PID_IDX = schema.PRODUCT_COLUMNS.index("product_id")
# /p/<cat> or /p/<cat>/<sub>, but never a /p/d/<...> product page
CATEGORY_RE = re.compile(r"https?://(?:www\.)?uptimumtech\.in(/p/(?!d/)[a-z0-9][a-z0-9/-]*)", re.I)
ID_RE = re.compile(r"-(\d+)/?$")
AMOUNT_RE = re.compile(r"([\d,]+(?:\.\d+)?)")
OOS = ("out of stock", "sold out", "unavailable")


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def inr(text):
    """A rupee amount ("Rs5,999.00" / "5999.00") as a float, or None."""
    m = AMOUNT_RE.search((text or "").replace(",", ""))
    try:
        return float(m.group(1)) if m else None
    except (TypeError, ValueError):
        return None


def category_urls():
    """Every /p/<cat>[/<sub>] category page linked from the site navigation (on the home page)."""
    r = http.session().get(f"{BASE}/", timeout=60)
    r.raise_for_status()
    paths = {m.group(1).rstrip("/") for m in CATEGORY_RE.finditer(r.text)}
    return sorted(BASE + p for p in paths)


def parse_card(card, category, scraped_at):
    a = card.select_one('h2 a[href*="/p/d/"]') or card.select_one('a[href*="/p/d/"]')
    if not a or not a.get("href"):
        return None
    url = a["href"]
    m = ID_RE.search(url)
    if not m:
        return None
    product_id = m.group(1)

    pp = card.select_one(".product-price")
    if not pp:
        return None                       # quote-only: no firm price
    old_el = pp.select_one(".old-price")
    old_price = inr(old_el.get_text()) if old_el else None
    if old_el:
        old_el.extract()                  # leave only the current-price text behind
    price = inr(pp.get_text())
    if price is None:
        return None                       # quote-only: no firm price

    brand_el = card.select_one(".product-category")
    text = card.get_text(" ", strip=True).lower()
    return schema.product_row({
        "scraped_at": scraped_at, "website": SITE,
        "product_id": product_id, "sku": product_id,
        "title": clean(a.get_text()),
        "brand": clean(brand_el.get_text()) if brand_el else "",
        "category": category,
        "price": price,
        "compare_at_price": old_price,
        "available": not any(s in text for s in OOS),
        "url": url,
    })


def scrape_category(cat_url, scraped_at):
    """All product cards in a category, across its Laravel pages."""
    category = cat_url.split("/p/", 1)[-1]
    rows, page = [], 1
    while page <= 200:
        url = cat_url if page == 1 else f"{cat_url}?page={page}"
        try:
            r = http.session().get(url, timeout=60)
            if r.status_code == 404:
                break
            r.raise_for_status()
        except Exception as e:  # noqa: BLE001 - one bad page must not stop the run
            print(f"WARN {url}: {e}", flush=True)
            break
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".single-product-wrap")
        if not cards:
            break
        for card in cards:
            row = parse_card(card, category, scraped_at)
            if row:
                rows.append(row)
        if not soup.select_one('a[rel="next"]'):
            break
        page += 1
    return rows


def scrape(limit, scraped_at):
    cats = category_urls()
    print(f"navigation lists {len(cats)} category pages", flush=True)
    rows, seen = [], set()
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, cat_rows in enumerate(pool.map(lambda c: scrape_category(c, scraped_at), cats), 1):
            for row in cat_rows:
                pid = row[PID_IDX]
                if pid and pid not in seen:
                    seen.add(pid)
                    rows.append(row)
            if i % 50 == 0 or i == len(cats):
                print(f"{i}/{len(cats)} categories, {len(rows)} priced products", flush=True)
            if limit and len(rows) >= limit:
                break
    # No reliable site-wide total (most categories are quote-only services): listed unknown.
    return (rows[:limit] if limit else rows), None


if __name__ == "__main__":
    runner.run(SITE, scrape)
