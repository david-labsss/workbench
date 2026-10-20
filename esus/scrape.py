"""Scrape esus-it.com (ESUS IT, IdoSell/IAI platform, large catalogue: memory, CPUs, drives and
other enterprise IT parts): every run's full snapshot appended to Postgres. Prices are in EUR.

IdoSell renders its listings server-side as `div.product` cards that already carry everything we
need -- the product id (data-product_id), name, link, condition, availability and price -- so the
cards are read straight off the listing pages; no product-detail fetch is needed. The catalogue is
walked producer by producer: /producers.php lists every brand's listing page (/firm-eng-<id>-<name>
.html), and each is paged through the platform's own `?counter=N` paging by following the
<link rel="next"> the page emits, de-duplicating products by id across producers. The store is slow
to respond, so pages are fetched one at a time with a short pause; MAX_PRODUCTS caps test runs.

The gross ("incl. VAT") price is taken as the price (the net price is also shown on each card).
Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
import time

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "esus"
BASE = "https://www.esus-it.com"
PAGE_DELAY = 0.4            # polite pause between listing-page fetches (the store is slow)
MAX_PAGES = 1000           # per-producer safety cap against a paging loop
EUR_RE = re.compile(r"(\d{1,3}(?:[.\s]\d{3})*|\d+),(\d{2})")   # European "1.234,56"
CONDITIONS = {"new": "New", "used": "Used", "refurbished": "Refurbished",
              "damaged": "Damaged", "nowy": "New"}


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def eur(text):
    """The first European-formatted euro amount in `text` as a float, or None."""
    m = EUR_RE.search(text or "")
    if not m:
        return None
    return float(m.group(1).replace(".", "").replace(" ", "") + "." + m.group(2))


def get(url):
    r = http.session().get(url, timeout=120)
    r.raise_for_status()
    time.sleep(PAGE_DELAY)
    return r.text


def producer_urls():
    """Every producer listing page (/firm-eng-<id>-<name>.html) from /producers.php."""
    soup = BeautifulSoup(get(f"{BASE}/producers.php"), "html.parser")
    urls = []
    for a in soup.select('a[href*="/firm-eng-"]'):
        href = a.get("href") or ""
        if href.endswith(".html"):
            urls.append(href if href.startswith("http") else BASE + href)
    return sorted(set(urls))


def card_price(card):
    brutto = card.select_one(".price__brutto")
    netto = card.select_one(".price__netto")
    return eur(brutto.get_text()) if brutto else (eur(netto.get_text()) if netto else None)


def card_available(card, price):
    imgs = card.select("img[src*='dostepnosc']")
    srcs = " ".join(im.get("src") or "" for im in imgs)
    if "disabled" in srcs:
        return False
    if "enabled" in srcs:
        return True
    return price is not None


def card_condition(card):
    for el in card.select(".additional_traits_search strong, .additional_traits_search"):
        t = el.get_text(" ", strip=True).lower()
        if "condition" in t:
            word = t.split("condition", 1)[1].lstrip(": ").split()[:1]
            if word:
                return CONDITIONS.get(word[0], word[0].capitalize())
    return ""


def build_row(card, brand, scraped_at):
    pid = card.get("data-product_id") or card.get("data-product-id")
    a = card.select_one("a.product__name") or card.select_one("h3 a")
    if not pid or not a:
        return None
    url = a.get("href") or ""
    if url and not url.startswith("http"):
        url = BASE + url
    title = clean(a.get("title") or a.get_text())
    price = card_price(card)
    # product thumbnail on the card (skip the availability "dostepnosc" status icon)
    img = next((im for im in card.find_all("img")
                if "dostepnosc" not in (im.get("src") or im.get("data-src") or "")), None)
    image = (img.get("data-src") or img.get("src")) if img else None
    if image:
        image = "https:" + image if image.startswith("//") else (
            BASE + image if image.startswith("/") else image)
    # The listing cards carry no product description; a description would need a per-product
    # page fetch, which this scraper deliberately avoids. Left None.
    return schema.product_row({
        "scraped_at": scraped_at, "website": SITE,
        "product_id": str(pid),
        "title": title, "brand": brand,
        "condition": card_condition(card),
        "price": price,
        "available": card_available(card, price),
        "image_url": image,
        "description": None,
        "url": url,
    })


def scrape(limit, scraped_at):
    producers = producer_urls()
    print(f"{len(producers)} producer listing pages", flush=True)
    rows, seen = [], set()
    for pi, start in enumerate(producers, 1):
        brand = ""
        url, pages = start, 0
        while url and pages < MAX_PAGES:
            try:
                html = get(url)
            except Exception as e:  # noqa: BLE001 - one bad page must not stop the run
                print(f"WARN {url}: {e}", flush=True)
                break
            soup = BeautifulSoup(html, "html.parser")
            if not brand:
                h1 = soup.select_one("h1")
                brand = clean(h1.get_text()) if h1 else ""
            for card in soup.select("div.product"):
                row = build_row(card, brand, scraped_at)
                if row:
                    pid = row[schema.PRODUCT_COLUMNS.index("product_id")]
                    if pid not in seen:
                        seen.add(pid)
                        rows.append(row)
            if limit and len(rows) >= limit:
                rows = rows[:limit]
                print(f"reached MAX_PRODUCTS={limit}", flush=True)
                return rows, None
            nxt = soup.find("link", rel="next")
            href = nxt.get("href") if nxt else None
            url = (href if not href or href.startswith("http") else BASE + href) if href else None
            pages += 1
        if pi % 10 == 0 or pi == len(producers):
            print(f"{pi}/{len(producers)} producers, {len(rows)} products", flush=True)
    return rows, None


if __name__ == "__main__":
    runner.run(SITE, scrape)
