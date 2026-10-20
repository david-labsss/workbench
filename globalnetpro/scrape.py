"""Scrape globalnetpro.com (Laravel, ~2,283 products): every run's full snapshot appended to Postgres.

The store has no public product API, so every product in /sitemap.xml (links of the form
/products/<family>/<part-number>) is read, a few at a time. Each product page carries:
  * a Product JSON-LD block with the name, the store's internal product id ("sku"), the real
    manufacturer part number ("mpn") and the brand;
  * a <select id="cond"> whose options are the sellable conditions, each with data-qty (units
    available), data-retail (price), data-msrp (compare-at, "0.00" when none) and data-avail
    (a shipping/availability note).
One row per condition option (its own variant): product_id is "<internal id>-<option value>",
parent_id the internal product id. Quote-only products have no <select> and no price; they get
a single row (part number from the URL, title/brand from the page) with the price left empty.
The family segment of the URL is used as the category.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import json
import re
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "globalnetpro"
BASE = "https://www.globalnetpro.com"
WORKERS = 4
# /products/<family>/<part-number>  (two segments => a product, one segment => a family page)
PRODUCT_RE = re.compile(r"^https?://[^/]+/products/([^/]+)/([^/]+)$", re.I)


def clean(text):
    text = (text or "").replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def to_float(v):
    try:
        return float(str(v).replace(",", "").replace("$", "").strip())
    except (TypeError, ValueError):
        return None


def to_price(v):
    """A positive price, or None (a "0.00" retail means the condition is quote-only)."""
    f = to_float(v)
    return f if f and f > 0 else None


def absolute(u):
    """Absolute https URL for a possibly relative/protocol-relative one, else None."""
    u = (u or "").strip()
    if not u:
        return None
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return BASE + u
    return u


def first_image(value):
    """First image URL from a JSON-LD "image" value (str / list / {url}), absolute, or None."""
    if isinstance(value, list):
        value = value[0] if value else None
    if isinstance(value, dict):
        value = value.get("url")
    return absolute(value)


def product_urls():
    r = http.session().get(f"{BASE}/sitemap.xml", timeout=120)
    r.raise_for_status()
    urls = []
    for loc in re.findall(r"<loc>([^<]+)</loc>", r.text):
        if PRODUCT_RE.match(loc):
            urls.append(loc)
    return sorted(set(urls))


def product_jsonld(soup):
    """The Product JSON-LD block, if the page has one."""
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except (ValueError, TypeError):
            continue
        for node in data if isinstance(data, list) else [data]:
            if isinstance(node, dict) and node.get("@type") == "Product":
                return node
    return None


def condition_rows(url, html, scraped_at):
    soup = BeautifulSoup(html, "html.parser")
    family = PRODUCT_RE.match(url).group(1)
    pn_from_url = PRODUCT_RE.match(url).group(2)
    ld = product_jsonld(soup) or {}
    brand = ""
    if isinstance(ld.get("brand"), dict):
        brand = clean(ld["brand"].get("name"))
    if not brand:
        meta = soup.find("meta", property="product:brand")
        brand = clean(meta.get("content")) if meta else ""
    mpn = clean(ld.get("mpn")) or pn_from_url
    internal = clean(ld.get("sku"))  # the store's internal product id
    title = clean(ld.get("name"))
    if not title:
        h1 = soup.find("h1")
        title = clean(h1.get_text()) if h1 else ""

    select = soup.find("select", id="cond")
    options = select.find_all("option") if select else []

    image_url = first_image(ld.get("image"))
    if not image_url:
        og = soup.find("meta", property="og:image")
        image_url = absolute(og.get("content")) if og else None
    description = schema.plain_text(ld.get("description"))
    if not description:
        md = soup.find("meta", attrs={"name": "description"})
        description = schema.plain_text(md.get("content")) if md else None

    common = {
        "scraped_at": scraped_at, "website": SITE,
        "sku": mpn, "part_number": mpn, "model_number": mpn,
        "title": title, "brand": brand, "category": family,
        "image_url": image_url, "description": description, "url": url,
    }

    # Quote-only product: no sellable conditions, no price.
    if not options:
        pid = internal or pn_from_url
        return [schema.product_row({**common, "product_id": pid, "available": False})]

    rows = []
    for opt in options:
        val = clean(opt.get("value"))
        if not val:
            continue
        qty = to_float(opt.get("data-qty"))
        price = to_price(opt.get("data-retail"))
        msrp = to_price(opt.get("data-msrp"))
        # option text e.g. "Used / Refurbished (983 available)" -> condition label
        condition = re.sub(r"\s*\(\d[\d,]*\s*available\)\s*$", "", clean(opt.get_text()))
        rows.append(schema.product_row({
            **common,
            "product_id": f"{internal}-{val}" if internal else f"{pn_from_url}-{val}",
            "parent_id": internal,
            "variant_id": val,
            "condition": condition,
            "price": price,
            "compare_at_price": msrp if (price and msrp and msrp > price) else None,
            "available": bool(qty and qty > 0),
            "stock_qty": qty,
        }))
    return rows


def fetch(url):
    try:
        r = http.session().get(url, timeout=60)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return url, r.text
    except Exception as e:  # noqa: BLE001 - one bad page must not stop the run
        print(f"WARN {url}: {e}", flush=True)
        return None


def scrape(limit, scraped_at):
    urls = product_urls()
    print(f"sitemap lists {len(urls)} products", flush=True)
    todo = urls[:limit] if limit else urls
    rows, seen = [], set()
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, page in enumerate(pool.map(fetch, todo), 1):
            if page:
                for row in condition_rows(page[0], page[1], scraped_at):
                    pid = row[schema.PRODUCT_COLUMNS.index("product_id")]
                    if pid and pid not in seen:
                        seen.add(pid)
                        rows.append(row)
            if i % 200 == 0 or i == len(todo):
                print(f"{i}/{len(todo)} products, {len(rows)} rows", flush=True)
    return rows, len(urls)


if __name__ == "__main__":
    runner.run(SITE, scrape)
