"""Scrape it-market.com (IT-Market, Shopware 6 store, refurbished servers / storage / networking):
every run's full snapshot appended to Postgres. Prices are in EUR, stored as-is.

Primary path - the Shopware 6 Store API side door:
  POST https://it-market.com/store-api/product
       headers: sw-access-key: <KEY>, Content-Type/Accept: application/json
       body:    {"limit":100,"page":N,"total-count-mode":1,
                 "associations":{"seoUrls":{},"manufacturer":{}}}
  returns {"total":N,"elements":[...]}, each element giving:
    translated.name (-> title), calculatedPrice.unitPrice (EUR -> price),
    productNumber (-> sku / part#), availableStock + available (-> stock / availability),
    id (uuid -> product_id), parentId (-> parent_id), seoUrls[].seoPathInfo (-> url),
    manufacturer.translated.name (-> brand).

The sw-access-key is not public config; it is embedded in the storefront HTML/JS. Because the
storefront cannot be loaded from here (it-market.com serves an IP-reputation "under attack"
challenge to our IP, HTTP 503), the key is discovered at RUNTIME: fetch the homepage, regex out
every plausible access-key candidate (window.accessKey / "accessKey": / data-access-key /
sw-access-key / a bare SWSC... token), then probe each candidate against /store-api/product and
keep the first that returns products. This avoids hard-coding a key that may rotate.

Fallback, if no working key is found: the robots-clean sitemap path, like the ramkoenig Shopware
scraper - /sitemap.xml -> gzipped product sitemaps -> each product page, read from its Open Graph
price meta, Product JSON-LD and .product-detail-ordernumber.

NOTE: untested locally - it-market.com serves a 503 "under attack" page to our IP from Pakistan,
so this has only been py_compile-checked here; the parent smoke-tests it on the US GitHub runner.
The sw-access-key extraction is the main uncertainty: the runtime probe should find it if it is in
the homepage markup, but if Shopware hides it the scraper falls back to the sitemap path.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import gzip
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "itmarket"
BASE = "https://it-market.com"
STORE_API = f"{BASE}/store-api/product"
PAGE = 100            # products per Store API page
WORKERS = 6           # <= 6, polite
DELAY = 0.2
NOT_AVAILABLE = ("no longer available", "not available", "sold out", "nicht verfügbar",
                 "nicht mehr verfügbar", "ausverkauft")
CONDITIONS = {"newcondition": "New", "usedcondition": "Used",
              "refurbishedcondition": "Refurbished", "damagedcondition": "Damaged"}

# Access-key candidates in the storefront markup, most specific first. Shopware keys are ~20-32
# upper-alphanumeric chars (older ones start with "SWSC").
KEY_RES = [
    re.compile(r'["\']?(?:sw-?access-?key|accessKey)["\']?\s*[:=]\s*["\']([A-Za-z0-9]{16,40})["\']', re.I),
    re.compile(r'data-access-key=["\']([A-Za-z0-9]{16,40})["\']', re.I),
    re.compile(r'\b(SWSC[A-Z0-9]{12,36})\b'),
]


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def to_float(v):
    try:
        return float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------- access key
def _homepage_html():
    s = http.session()
    r = s.get(BASE + "/", timeout=60, headers={
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
    })
    r.raise_for_status()
    return r.text


def access_key_candidates(html):
    out = []
    for rx in KEY_RES:
        for m in rx.findall(html):
            key = m if isinstance(m, str) else m[0]
            if key and key not in out:
                out.append(key)
    return out


def store_api_page(key, page, total_count_mode=0):
    """One /store-api/product page. Returns the parsed JSON, or raises."""
    body = {"limit": PAGE, "page": page, "total-count-mode": total_count_mode,
            "associations": {"seoUrls": {}, "manufacturer": {}}}
    r = http.session().post(STORE_API, json=body, timeout=120, headers={
        "sw-access-key": key, "Content-Type": "application/json", "Accept": "application/json",
    })
    r.raise_for_status()
    return r.json()


def find_access_key(html):
    """The first candidate key that the Store API actually accepts, or None."""
    for key in access_key_candidates(html):
        try:
            data = store_api_page(key, 1)
            if isinstance(data.get("elements"), list) and data["elements"]:
                print(f"store-api access key accepted (len {len(key)})", flush=True)
                return key
        except Exception as e:  # noqa: BLE001 - try the next candidate
            print(f"WARN access-key candidate rejected: {e}", flush=True)
    return None


# ---------------------------------------------------------------- Store API mapping
def _seo_url(element):
    for seo in element.get("seoUrls") or []:
        path = seo.get("seoPathInfo")
        if path and seo.get("isCanonical"):
            return f"{BASE}/{path.lstrip('/')}"
    for seo in element.get("seoUrls") or []:
        path = seo.get("seoPathInfo")
        if path:
            return f"{BASE}/{path.lstrip('/')}"
    pid = element.get("id")
    return f"{BASE}/detail/{pid}" if pid else BASE


def _price(element):
    calc = element.get("calculatedPrice") or {}
    price = to_float(calc.get("unitPrice"))
    if price is None:                 # fall back to the stored gross price
        plist = element.get("price") or []
        if isinstance(plist, list) and plist:
            price = to_float(plist[0].get("gross"))
    return round(price, 2) if price is not None else None


def api_row(element, scraped_at):
    pid = element.get("id")
    if not pid:
        return None
    tr = element.get("translated") or {}
    name = clean(tr.get("name") or element.get("name"))
    number = clean(element.get("productNumber"))
    stock = element.get("availableStock")
    available = element.get("available")
    if available is None:
        available = bool(stock and stock > 0)
    manufacturer = element.get("manufacturer") or {}
    mtr = (manufacturer.get("translated") or {}) if isinstance(manufacturer, dict) else {}
    brand = clean(mtr.get("name") or (manufacturer.get("name") if isinstance(manufacturer, dict) else ""))
    blob = f"{name} {clean(tr.get('description') or '')}".lower()
    condition = next((v for k, v in {"refurbished": "Refurbished", "renewed": "Renewed",
                                     "used": "Used", "new": "New"}.items() if k in blob), "")
    return schema.product_row({
        "scraped_at": scraped_at, "website": SITE,
        "product_id": pid,
        "parent_id": element.get("parentId") or "",
        "variant_id": pid if element.get("parentId") else "",
        "sku": number, "part_number": number, "model_number": number,
        "title": name, "brand": brand,
        "condition": condition,
        "price": _price(element),
        "available": bool(available),
        "stock_qty": stock if isinstance(stock, int) else None,
        "url": _seo_url(element),
    })


def scrape_store_api(key, limit, scraped_at):
    first = store_api_page(key, 1, total_count_mode=1)
    total = first.get("total") or 0
    print(f"store-api lists {total} products", flush=True)
    rows, seen = [], set()

    def add(elements):
        for el in elements or []:
            row = api_row(el, scraped_at)
            if row:
                pid = row[schema.PRODUCT_COLUMNS.index("product_id")]
                if pid not in seen:
                    seen.add(pid)
                    rows.append(row)

    add(first.get("elements"))
    pages = (total + PAGE - 1) // PAGE if total else 1
    if limit:
        pages = min(pages, (limit + PAGE - 1) // PAGE)

    def fetch_page(page):
        for attempt in range(1, 4):
            try:
                time.sleep(DELAY)
                return store_api_page(key, page)["elements"]
            except Exception as e:  # noqa: BLE001
                if attempt == 3:
                    print(f"WARN store-api page {page} failed: {e}", flush=True)
                    return []
                time.sleep(2 * attempt)

    if pages > 1:
        with ThreadPoolExecutor(WORKERS) as pool:
            for i, elements in enumerate(pool.map(fetch_page, range(2, pages + 1)), 1):
                add(elements)
                if i % 20 == 0 or i == pages - 1:
                    print(f"{len(rows)} products so far", flush=True)
                if limit and len(rows) >= limit:
                    break

    if limit:
        return rows[:limit], None
    return rows, (total or None)


# ---------------------------------------------------------------- sitemap fallback
PRODUCT_RE = re.compile(r"^https://it-market\.com/.+[^/]$", re.I)


def _locs(xml):
    return re.findall(r"<loc>([^<]+)</loc>", xml)


def product_urls():
    r = http.session().get(f"{BASE}/sitemap.xml", timeout=120)
    r.raise_for_status()
    urls = []
    for sm in _locs(r.text):
        try:
            rs = http.session().get(sm, timeout=120)
            rs.raise_for_status()
            try:
                xml = gzip.decompress(rs.content).decode("utf-8", "replace")
            except (OSError, EOFError):
                xml = rs.text
        except Exception as e:  # noqa: BLE001
            print(f"WARN sitemap {sm}: {e}", flush=True)
            continue
        urls += [loc for loc in _locs(xml) if PRODUCT_RE.match(loc) and loc.rstrip("/") != BASE]
    return sorted(set(urls))


def product_jsonld(soup):
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except (ValueError, TypeError):
            continue
        nodes = data.get("@graph", data) if isinstance(data, dict) else data
        for node in nodes if isinstance(nodes, list) else [nodes]:
            t = node.get("@type") if isinstance(node, dict) else None
            if t == "Product" or (isinstance(t, list) and "Product" in t):
                return node
    return None


def _meta(soup, **attrs):
    tag = soup.find("meta", attrs=attrs)
    return clean(tag.get("content")) if tag and tag.get("content") else ""


def page_row(url, html, scraped_at):
    soup = BeautifulSoup(html, "html.parser")
    onum = soup.select_one(".product-detail-ordernumber")
    product_id = clean(onum.get_text()) if onum else ""
    if not product_id:
        return None                   # not a product page
    name = soup.select_one(".product-detail-name")
    delivery = soup.select_one(".delivery-information, .product-detail-delivery-information")
    delivery_text = clean(delivery.get_text()) if delivery else ""
    price = to_float(_meta(soup, property="product:price:amount")) or \
        to_float(_meta(soup, itemprop="price"))
    ld = product_jsonld(soup) or {}
    brand = ld.get("brand")
    brand = clean(brand.get("name")) if isinstance(brand, dict) else clean(brand)
    title = clean(name.get_text()) if name else clean(ld.get("name"))
    if not brand and title:
        brand = title.split()[0]
    mpn = clean(ld.get("mpn"))
    offers = ld.get("offers") or {}
    offer = offers[0] if isinstance(offers, list) and offers else offers
    cond = str((offer or {}).get("itemCondition") or "").lower()
    available = bool(delivery_text) and not any(p in delivery_text.lower() for p in NOT_AVAILABLE)
    return schema.product_row({
        "scraped_at": scraped_at, "website": SITE,
        "product_id": product_id, "sku": product_id,
        "part_number": mpn, "model_number": mpn,
        "title": title, "brand": brand,
        "condition": next((v for k, v in CONDITIONS.items() if k in cond), ""),
        "price": price,
        "available": available,
        "other_specs": delivery_text,
        "url": url,
    })


def fetch(url):
    try:
        r = http.session().get(url, timeout=60)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return url, r.text
    except Exception as e:  # noqa: BLE001
        print(f"WARN {url}: {e}", flush=True)
        return None


def scrape_sitemap(limit, scraped_at):
    urls = product_urls()
    print(f"sitemap lists {len(urls)} product URLs", flush=True)
    todo = urls[:limit] if limit else urls
    rows, seen = [], set()
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, page in enumerate(pool.map(fetch, todo), 1):
            if page:
                row = page_row(page[0], page[1], scraped_at)
                if row:
                    pid = row[schema.PRODUCT_COLUMNS.index("product_id")]
                    if pid and pid not in seen:
                        seen.add(pid)
                        rows.append(row)
            if i % 200 == 0 or i == len(todo):
                print(f"{i}/{len(todo)} pages, {len(rows)} products", flush=True)
    return rows, None                 # sitemap may include a few non-product pages


# ---------------------------------------------------------------- run
def scrape(limit, scraped_at):
    try:
        html = _homepage_html()
        key = find_access_key(html)
    except Exception as e:  # noqa: BLE001 - homepage blocked -> go straight to the fallback
        print(f"WARN could not load storefront for the access key: {e}", flush=True)
        key = None
    if key:
        return scrape_store_api(key, limit, scraped_at)
    print("no working sw-access-key found; falling back to the sitemap path", flush=True)
    return scrape_sitemap(limit, scraped_at)


if __name__ == "__main__":
    runner.run(SITE, scrape)
