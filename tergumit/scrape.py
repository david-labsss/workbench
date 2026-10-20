"""Scrape tergumit.com (BigCommerce, ~826 products: HPE / Dell spare parts, drives, memory, CPUs):
every run's full snapshot appended to Postgres.

The store has no storefront GraphQL token in its pages (so common/bigcommerce.py cannot be used),
so every product page listed in its sitemap (xmlsitemap.php?type=products&page=N) is read, 2 at a
time with a 1 s pause after each page (~0.7 pages/s, a full run takes ~19 min; robots.txt allows
product pages and the sitemap, its crawl-delay applies only to named AI bots). Each page carries the theme's inline
`var BCData = {...}` JSON, whose product_attributes give:
  sku, mpn (-> part_number)
  price.without_tax.value (USD; with_tax when the store shows tax-inclusive prices) and the
    RRP / non-sale price when set (-> compare_at_price, only when above the price);
    no price or $0 -> empty price
  instock (-> available) and available_to_sell (-> stock_qty, the store's real stock count)
The rest comes from the page: product id (hidden input product_id), title (h1), brand and
condition (the schema.org JSON-LD Product; the "Condition:" line of the page wins when shown),
and the category path from the breadcrumbs (e.g. "Spare Parts > Memory (DIMM)"). Capacity is
read from the title (upper-case GB/TB only) for drives and memory.
image_url / description: the JSON-LD image (placeholder -> None) and description (URL-decoded).

The number to check the snapshot against is the sitemap's product count; a page that fails
(after common/http retries) stops the run, and a sitemap URL that is gone (404) is skipped.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "tergumit"
BASE = "https://tergumit.com"
WORKERS = 2
DELAY = 1.0                   # pause after each page, per worker
BCDATA_RE = re.compile(r"var BCData = (\{.*?\});\s*$", re.S | re.M)
GENERIC_CATEGORIES = {"home", "shop all", "shop all products"}
EMPTY_VALUES = {"", "n/a", "na", "none", "-"}
CONDITIONS = {"newcondition": "New", "refurbishedcondition": "Refurbished",
              "usedcondition": "Used", "damagedcondition": "Damaged"}
CAPACITY_RE = re.compile(r"(?<![A-Za-z0-9.])(\d+(?:\.\d+)?)\s*(TB|GB)\b")   # upper case only
CAPACITY_HINT = re.compile(r"drive|ssd|hdd|memory|dimm|storage|flash", re.I)


def get(url, **kw):
    r = http.session().get(url, timeout=90, **kw)
    r.raise_for_status()
    return r


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def sitemap_urls():
    """Every product URL across the store's product sitemap pages."""
    index = get(f"{BASE}/xmlsitemap.php").text
    pages = sorted(set(re.findall(r"type=products&(?:amp;)?page=(\d+)", index)), key=int)
    if not pages:
        raise RuntimeError("no product sitemap in xmlsitemap.php")
    urls = []
    for page in pages:
        text = get(f"{BASE}/xmlsitemap.php", params={"type": "products", "page": page}).text
        urls += [clean(u).replace("&amp;", "&") for u in re.findall(r"<loc>([^<]+)</loc>", text)]
    return urls


def fetch(url):
    """(url, html), or None when the product is gone (404). Other failures raise."""
    r = http.session().get(url, timeout=90)
    time.sleep(DELAY)
    if r.status_code == 404:
        print(f"WARN {url}: 404, listed in the sitemap but gone", flush=True)
        return None
    r.raise_for_status()
    return url, r.text


def num(value):
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def json_ld_product(soup):
    for tag in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(tag.string or "")
        except ValueError:
            continue
        for item in data if isinstance(data, list) else [data]:
            if isinstance(item, dict) and item.get("@type") == "Product":
                return item
    return {}


def page_details(soup):
    """{"condition": "New", "warranty": "1 Year", ...} from the product's dt/dd info list."""
    details = {}
    for dt in soup.select("dl.productView-info dt, .productView-info dt"):
        dd = dt.find_next_sibling("dd")
        if dd:
            details.setdefault(clean(dt.get_text()).rstrip(":").strip().lower(),
                               clean(dd.get_text(" ")))
    return details


def parse(url, html, scraped_at):
    m = BCDATA_RE.search(html)
    if not m:
        raise RuntimeError(f"{url}: no BCData on the page")
    attrs = json.loads(m.group(1)).get("product_attributes") or {}
    soup = BeautifulSoup(html, "html.parser")
    pid = soup.select_one("input[name=product_id]")
    title = soup.select_one("h1.productView-title") or soup.select_one(".productView-title")
    if not pid or not pid.get("value") or not title:
        raise RuntimeError(f"{url}: no product id or title on the page")
    title = clean(title.get_text())

    prices = attrs.get("price") or {}
    price = num((prices.get("without_tax") or prices.get("with_tax") or {}).get("value"))
    price = price or None                                        # $0 / none = call for price
    compare = max((x for x in (num((prices.get(k) or {}).get("value"))
                               for k in ("rrp_without_tax", "rrp_with_tax",
                                         "non_sale_price_without_tax",
                                         "non_sale_price_with_tax")) if x), default=None)

    ld = json_ld_product(soup)
    details = page_details(soup)
    brand = (ld.get("brand") or {}).get("name") if isinstance(ld.get("brand"), dict) else ""
    if not brand:
        el = soup.select_one(".productView-brand a, .productView-brand")
        brand = clean(el.get_text()) if el else ""
    offers = ld.get("offers") or {}
    offers = offers[0] if isinstance(offers, list) and offers else offers
    ld_condition = str((offers or {}).get("itemCondition") or "").rstrip("/").split("/")[-1]
    condition = details.get("condition") or CONDITIONS.get(ld_condition.lower(), "")
    image = ld.get("image")                 # JSON-LD image: a URL, a list of URLs or {url}
    image = image[0] if isinstance(image, list) and image else image
    image = image.get("url") if isinstance(image, dict) else image
    image = clean(image) if isinstance(image, str) else ""
    if image.startswith("//"):
        image = "https:" + image
    elif image.startswith("/"):
        image = BASE + image
    if "productdefault" in image.lower():
        image = ""                          # the theme's "no image" placeholder, not a photo
    # The theme URL-encodes the JSON-LD description ("SPS-DRV%20SSD%20960GB...")
    description = schema.plain_text(unquote(str(ld.get("description") or "")))

    crumbs = [clean(a.get_text()) for a in soup.select(".breadcrumbs .breadcrumb-label")][:-1]
    category = " > ".join(c for c in crumbs if c and c.lower() not in GENERIC_CATEGORIES)

    sku = clean(attrs.get("sku"))
    mpn = clean(attrs.get("mpn"))
    capacity = ""
    cap = CAPACITY_RE.search(title)
    if cap and CAPACITY_HINT.search(f"{category} {title}"):
        capacity = cap.group(1) + cap.group(2)
    stock = attrs.get("available_to_sell")
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": pid.get("value"),
        "sku": sku,
        "part_number": mpn if mpn.lower() not in EMPTY_VALUES else "",
        "title": title,
        "brand": clean(brand),
        "category": category,
        "condition": condition,
        "capacity": capacity,
        "other_specs": f"Warranty: {details['warranty']}" if details.get("warranty") else "",
        "price": price,
        "compare_at_price": compare if compare and price and compare > price else None,
        "available": bool(attrs.get("instock")),
        "stock_qty": num(stock) if isinstance(stock, (int, float)) else None,
        "image_url": image or None,
        "description": description,
        "url": url,
    })


def scrape(limit, scraped_at):
    urls = list(dict.fromkeys(sitemap_urls()))
    print(f"sitemap lists {len(urls)} products", flush=True)
    todo = urls[:limit] if limit else urls
    rows, seen = [], set()
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, page in enumerate(pool.map(fetch, todo), 1):
            row = parse(page[0], page[1], scraped_at) if page else None
            pid = row[schema.PRODUCT_COLUMNS.index("product_id")] if row else None
            if row and pid not in seen:      # two URLs can lead to one product
                seen.add(pid)
                rows.append(row)
            if i % 100 == 0 or i == len(todo):
                print(f"{i}/{len(todo)} pages, {len(rows)} products", flush=True)
    return rows, len(urls)


if __name__ == "__main__":
    runner.run(SITE, scrape)
