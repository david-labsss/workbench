"""Scrape serverparts4less.com (BigCommerce store, ~5,140 products): every run's full snapshot
appended to Postgres.

The store has no storefront GraphQL token in its pages (so common/bigcommerce.py cannot be used)
and its /compare/ page is switched off. Walking the category listings misses the products that
sit in no browsable category (~566 of 5,140), so the run can never reach them and the runner's
completeness guard aborts. Instead the scrape is driven off the PRODUCT SITEMAP
(xmlsitemap.php?type=products&page=N), which lists a <loc> for every product, so nothing is
missed: each product page is fetched and parsed on its own.

Each product page gives, from its main productView block:
  product id   the add-to-cart form's hidden product_id (BigCommerce entity id)
  title        h1.productView-title (starts with the manufacturer part number)
  brand        h2.productView-brand
  price        the offer's itemprop="price" (USD); $0 / "call for price" / quote -> None
  compare_at   the RRP / non-sale price when the product is on sale
  available    itemprop="availability" (In/OutOfStock), falling back to og:availability
  image_url    og:image
  description   the #tab-description tab (HTML -> schema.plain_text)
SKU/MPN and specs are not on the page (the store leaves them blank); the part number and any
capacity are read from the title. The snapshot is checked against the sitemap product count,
minus any URLs that are genuinely gone (HTTP 404/410) so a dead product is not counted as a
truncation.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
import time
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "serverparts4less"
BASE = "https://www.serverparts4less.com"
WORKERS = 8                   # product pages fetched in parallel
DELAY = 0.1                   # polite pause per request, per worker (seconds)
ATTEMPTS = 4                  # extra retries around a transient blip / partial page
PRICE_RE = re.compile(r"[\d,]+(?:\.\d+)?")
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b", re.I)
# the first title token that looks like a manufacturer part number (has a digit, 4+ chars)
PART_TOKEN_RE = re.compile(r"^[A-Za-z]*\d[A-Za-z0-9]*(?:[-/.][A-Za-z0-9]+)*$")
CONDITION_RE = re.compile(r"\b(new|refurbished|refurb|renew(?:ed)?|used|pre[- ]?owned|pull(?:ed)?)\b", re.I)
# price shown as a quote instead of a number -> no price
QUOTE_RE = re.compile(r"call for price|request a quote|request quote|call for availability", re.I)


def get(url):
    r = http.session().get(url, timeout=90)
    return r


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def sitemap_pages(kind):
    """The page numbers the sitemap index lists for one entry type."""
    text = http.session().get(f"{BASE}/xmlsitemap.php", timeout=90).text
    pages = re.findall(rf"xmlsitemap\.php\?type={kind}&(?:amp;)?page=(\d+)", text)
    return sorted(set(pages), key=int)


def product_urls():
    """Every product URL across the store's product sitemap pages (its full catalogue)."""
    urls = []
    for page in sitemap_pages("products"):
        text = http.session().get(f"{BASE}/xmlsitemap.php?type=products&page={page}",
                                  timeout=90).text
        urls += [u.replace("&amp;", "&") for u in re.findall(r"<loc>(.*?)</loc>", text)]
    # one URL per product; keep order, drop any accidental duplicate loc
    return list(dict.fromkeys(urls))


def money(text):
    m = PRICE_RE.search(text or "")
    return float(m.group(0).replace(",", "")) if m else None


def part_number(title):
    for token in title.split():
        t = token.strip("*()[]:,")
        if len(t) >= 4 and any(c.isdigit() for c in t) and PART_TOKEN_RE.match(t):
            return t
    return ""


def parse(url, html):
    """Fields for one product page, or None if it has no main product block."""
    soup = BeautifulSoup(html, "html.parser")
    pv = soup.select_one("div.productView")
    if not pv:
        return None
    title_el = pv.select_one("h1.productView-title")
    title = clean(title_el.get_text()) if title_el else ""
    if not title:
        return None

    # product id: the add-to-cart / wishlist form's hidden input (the main product's own id),
    # with the product image path as a fallback
    pid_el = pv.select_one("input[name='product_id']") or soup.select_one("input[name='product_id']")
    pid = (pid_el.get("value") or "").strip() if pid_el else ""
    if not pid.isdigit():
        m = re.search(r"/products/(\d+)/", html)
        pid = m.group(1) if m else ""
    if not pid.isdigit():
        return None

    brand_el = (pv.select_one("h2.productView-brand [itemprop='name']")
                or pv.select_one("h2.productView-brand a")
                or pv.select_one("h2.productView-brand"))
    brand = clean(brand_el.get_text()) if brand_el else ""

    # price / compare-at from the main offer block only (card-prices on related products differ)
    pb = pv.select_one("div.productView-price")
    price = compare = None
    if pb:
        meta = pb.select_one("[itemprop='price']")
        if meta and meta.get("content"):
            price = money(meta["content"])
        if price is None:
            span = pb.select_one("[data-product-price-without-tax]")
            price = money(span.get_text()) if span else None
        rrp = pb.select_one("[data-product-rrp-price-without-tax]")
        nonsale = pb.select_one("[data-product-non-sale-price-without-tax]")
        compare = max((x for x in (money(rrp.get_text() if rrp else ""),
                                   money(nonsale.get_text() if nonsale else "")) if x),
                      default=None)
        if QUOTE_RE.search(pb.get_text(" ", strip=True)):
            price = None
    if price == 0:                       # $0 / quote-only is not a real price
        price = None

    # availability: the offer microdata, falling back to the og:availability meta
    avail = None
    avail_meta = pv.select_one("[itemprop='availability']")
    if avail_meta and avail_meta.get("content"):
        avail = "instock" in avail_meta["content"].lower()
    if avail is None:
        og = soup.select_one("meta[property='og:availability']")
        avail = (og.get("content") == "instock") if og else False

    stock_el = pv.select_one("[data-product-stock]")
    stock_txt = clean(stock_el.get_text()) if stock_el else ""
    stock_qty = int(stock_txt) if stock_txt.isdigit() else None

    og_img = soup.select_one("meta[property='og:image']")
    image = og_img.get("content") if og_img and og_img.get("content") else None
    desc_el = soup.select_one("#tab-description") or soup.select_one("article.productView-description")
    description = schema.plain_text(desc_el.decode_contents()) if desc_el else None

    cond = CONDITION_RE.search(title)
    return {
        "id": pid,
        "title": title,
        "url": url,
        "brand": brand,
        "price": price,
        "compare": compare if compare and price and compare > price else None,
        "available": bool(avail),
        "condition": cond.group(0).title() if cond else "",
        "stock_qty": stock_qty,
        "image_url": image,
        "description": description,
    }


def fetch_product(url):
    """(status, fields) for one product URL.
    status "ok": parsed; "dead": gone (HTTP 404/410); "fail": still broken after retries.
    Transient errors / partial pages are retried so a blip never drops a product."""
    for attempt in range(ATTEMPTS):
        try:
            time.sleep(DELAY)
            r = get(url)
            if r.status_code in (404, 410):
                return "dead", None
            r.raise_for_status()
            fields = parse(url, r.text)
            if fields:
                return "ok", fields
        except Exception as e:                       # noqa: BLE001 - retry any transient error
            if attempt == ATTEMPTS - 1:
                print(f"WARN {url} failed: {type(e).__name__}", flush=True)
            else:
                time.sleep(2 * (attempt + 1))
            continue
        # 200 but no product block: retry a couple of times, then give up on this URL
        if attempt < ATTEMPTS - 1:
            time.sleep(2 * (attempt + 1))
    return "fail", None


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
        "brand": p["brand"],
        "category": "",
        "condition": p["condition"],
        "capacity": capacity,
        "price": p["price"],
        "compare_at_price": p["compare"],
        "price_per_tb": round(p["price"] / tb, 2) if p["price"] and tb else None,
        "available": p["available"],
        "stock_qty": p["stock_qty"],
        "image_url": p["image_url"],
        "description": p["description"],
        "url": p["url"],
    })


def scrape(limit, scraped_at):
    urls = product_urls()
    print(f"sitemap lists {len(urls)} products", flush=True)
    if limit:
        urls = urls[:limit]

    by_id, dead, failed = {}, 0, 0
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, (status, p) in enumerate(pool.map(fetch_product, urls), 1):
            if status == "ok":
                by_id.setdefault(p["id"], p)       # one row per product id
            elif status == "dead":
                dead += 1
            else:
                failed += 1
            if i % 500 == 0 or i == len(urls):
                print(f"{i}/{len(urls)} pages, {len(by_id)} products, "
                      f"{dead} gone (404), {failed} failed", flush=True)

    # A product that is genuinely gone (404/410) is not a truncation, so drop it from the
    # count the completeness guard checks against.
    listed = len(urls) - dead
    rows = [build_row(p, scraped_at) for p in by_id.values()]
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
