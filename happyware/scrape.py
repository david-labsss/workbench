"""Scrape happyware.com: every run's full snapshot appended to Postgres.

happyware.com is a German server/hardware reseller on Shopware 5. Cloudflare is in front of
the site but lets a normal browser User-Agent through (200 OK), so no bypass is needed. Prices
are in EUR and public on each product page as schema.org microdata (itemprop=...), there is NO
JSON-LD Product block. Fastest allowed path: read every product page once, straight from the
sitemap (robots.txt allows product pages; only /checkout, /account, /listing, ... are disallowed).

ENUMERATION: the catalogue is the Shopware sitemap. https://happyware.com/sitemap.xml is a
PixupSitemap index whose "articles" area sitemap lists every product-page URL (898 on
2026-10-20). The index references a single articles sitemap (articles-0); the server returns
that same file for any page index, so there is no hidden pagination. We parse the index, fetch
every "articles" sitemap it names, collect the <loc> product URLs and crawl each once. The store
is configure-to-order: its category pages are small curated/configurator pages (e.g.
server-discount lists 4), so the ~898 sitemap articles ARE the full catalogue, not a subset.
We print the URL count and fail the run (via common/runner.py's 98% guard) rather than save a
partial snapshot if too many pages fail to fetch.

PER PRODUCT PAGE (microdata, parsed with BeautifulSoup):
  name          <h1 class="product--title"> (fallback <title itemprop="name">)
  price         the VISIBLE EUR price in .price--content, German format "2.299,00" -> 2299.00.
                This is the authoritative source: many in-stock products show a real price here
                but carry NO <meta itemprop="price"> (configurable base price), so reading the
                meta alone would wrongly blank them. The meta (already machine format) is only a
                defensive fallback. "auf Anfrage" / "on request" items have no price here, and
                the placeholder 99.999,99 (meta 99999.99) both map to price=None.
  availability  itemprop="availability" -> schema.org/InStock|OutOfStock (content or href).
  sku/part no.  "Hersteller SKU" base-info row = manufacturer part number (e.g. P71-09025);
                falls back to the store article number ("Artikel-Nr." / itemprop="sku").
  brand         <meta itemprop="brand"> (fallback the "Hersteller" base-info row).
  category      deepest breadcrumb (itemprop=itemListElement), minus the "Produkte" root.
  image         first itemprop="image" URL that is not the happyware logo (the first such meta
                is always the site logo; the real product image/gallery follows).
  description   itemprop="description" -> schema.plain_text.
  product_id    the product URL's path (e.g. "microsoft/p71-09025"): stable and unique per page.

German number format: display uses "." for thousands and "," for decimals, so "2.299,00" is
2299.00 and "840,00" is 840.00. parse_price_text strips "." and swaps "," for ".".

Changes are found against the previous full run (common/changes.py) and written to the shared
prices table (website = happyware). Columns: common/schema.py.

Be polite: this is ~900 one-shot GETs (1 sitemap index + 1 article sitemap + ~898 product
pages). SCRAPE_WORKERS defaults to 6 with a small per-request delay; a full run takes roughly
5-10 minutes, so a workflow timeout of ~25-30 min is comfortable.

Env vars:
  DATABASE_URL     Postgres URL for the scraper_writer role
  SCRAPE_WORKERS   optional, product pages fetched at once (default 6, kept low on purpose)
  MAX_PRODUCTS     optional, stop after about N products (test runs, not compared or counted)
  DRY_RUN          optional, "1" = print sample rows, don't touch the database
"""
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit
from xml.etree import ElementTree

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "happyware"
BASE = "https://happyware.com"
SITEMAP = f"{BASE}/sitemap.xml"
WORKERS = int(os.getenv("SCRAPE_WORKERS") or 6)   # kept low: polite on a big one-page-each crawl
PAGE_DELAY = 0.3                                   # seconds before each product fetch (polite)
FETCH_RETRIES = 3                                  # network errors only (session retries 429/5xx)
PLACEHOLDER_PRICE = 99999.99                       # "price on request" sentinel -> price=None
MAX_FAILED_SHARE = 0.02                            # abort if more than this share of pages fail
LOGO_MARKER = "happyware-main-logo"                # the first itemprop=image is always the logo
PRODUCT_MARKER = "product--title"                  # every real product page has this; a 200 that
#                                                    lacks it (a Cloudflare challenge / error page)
#                                                    is NOT a product and is treated as a failure

COLUMNS = schema.PRODUCT_COLUMNS

# first number in a German price string: "2.299,00", "840,00", "99.999,99", "965"
PRICE_RE = re.compile(r"\d[\d.]*(?:,\d+)?")
CONDITIONS = [
    (re.compile(r"\brefurb", re.I), "Refurbished"),
    (re.compile(r"\brecertified\b", re.I), "Recertified"),
    (re.compile(r"\brenewed\b", re.I), "Renewed"),
    (re.compile(r"\bopen[- ]?box\b", re.I), "Open Box"),
    (re.compile(r"\b(gebraucht|used|pre[- ]?owned)\b", re.I), "Used"),
]


# --- fetching ----------------------------------------------------------------
def fetch(url, require=None):
    """GET a page. Returns (status, text): (200, html), (404, "") for a gone product, or
    (0, "") after retries for a transient/network failure (session already retries 429/5xx).
    `require`, when set, is a marker a 200 body must contain to count as success: a product 200
    without it is a Cloudflare challenge / error page, so it is retried rather than accepted."""
    for attempt in range(FETCH_RETRIES):
        try:
            r = http.session().get(url, timeout=45)
            if r.status_code == 404:
                return 404, ""
            if r.status_code == 200 and r.text:
                r.encoding = "utf-8"                # server declares UTF-8; force it
                if require is None or require in r.text:
                    return 200, r.text
        except Exception:
            pass
        time.sleep(2 * (attempt + 1))
    return 0, ""


def _locs(xml_text):
    """Every <loc> URL in a sitemap XML, namespace-agnostic."""
    root = ElementTree.fromstring(xml_text)
    return [el.text.strip() for el in root.iter()
            if el.tag.endswith("}loc") and el.text and el.text.strip()]


def product_urls():
    """All product-page URLs from the sitemap index's 'articles' area sitemap(s)."""
    status, text = fetch(SITEMAP)
    if status != 200:
        sys.exit(f"sitemap index {SITEMAP} unreachable (status {status}); not scraping")
    article_maps = [u for u in _locs(text) if "articles" in u.lower()]
    if not article_maps:
        sys.exit("sitemap index has no 'articles' product sitemap; not scraping")

    urls, seen = [], set()
    for sm in article_maps:
        st, body = fetch(sm)
        if st != 200:
            sys.exit(f"article sitemap {sm} unreachable (status {st}); not scraping partial")
        for u in _locs(body):
            if u not in seen:
                seen.add(u)
                urls.append(u)
    return urls


# --- parsing -----------------------------------------------------------------
def parse_price_text(text):
    """A German-format price string -> float EUR, or None. '2.299,00' -> 2299.00. Placeholder
    (>= 99999.99) and non-positive values become None."""
    if not text:
        return None
    m = PRICE_RE.search(text)
    if not m:
        return None
    try:
        v = float(m.group(0).replace(".", "").replace(",", "."))
    except ValueError:
        return None
    if v <= 0 or v >= PLACEHOLDER_PRICE:
        return None
    return round(v, 2)


def parse_price(offers):
    """Price from the offers block: the visible .price--content text first (authoritative and
    present even when the price meta is not), the <meta itemprop=price> as a fallback."""
    if offers is None:
        return None
    pc = offers.find(class_="price--content")
    if pc is not None:
        v = parse_price_text(pc.get_text(" ", strip=True))
        if v is not None:
            return v
    meta = offers.find("meta", attrs={"itemprop": "price"})
    if meta is not None and meta.get("content"):
        try:
            v = float(meta["content"])
            return None if v <= 0 or v >= PLACEHOLDER_PRICE else round(v, 2)
        except ValueError:
            pass
    return None


def is_available(offers):
    if offers is None:
        return False
    a = offers.find(attrs={"itemprop": "availability"})
    val = ((a.get("content") or a.get("href") or "") if a is not None else "")
    return "instock" in val.lower()


def base_info(soup):
    """The product's base-info rows as {label-without-colon: value}, e.g.
    {'Hersteller': 'Microsoft', 'Artikel-Nr.': '1248170', 'Hersteller SKU': 'P71-09025'}."""
    out = {}
    for li in soup.select("li.base-info--entry"):
        label = li.find("strong", class_="entry--label")
        value = li.find("span", class_="entry--content")
        if label and value:
            key = label.get_text(" ", strip=True).rstrip(":").strip()
            out[key] = value.get_text(" ", strip=True)
    return out


def product_image(soup):
    """First itemprop=image URL that isn't the site logo (the first such meta always is)."""
    for el in soup.find_all(attrs={"itemprop": "image"}):
        src = el.get("content") or el.get("src")
        if src and LOGO_MARKER not in src:
            return src
    return None


def category_of(soup):
    names = []
    for el in soup.find_all(attrs={"itemprop": "itemListElement"}):
        nm = el.find(attrs={"itemprop": "name"})
        if nm:
            t = (nm.get("content") or nm.get_text(" ", strip=True)).strip()
            if t and t.lower() not in ("produkte", "home", "startseite"):
                names.append(t)
    return names[-1] if names else ""


def condition_of(title):
    for pattern, label in CONDITIONS:
        if pattern.search(title or ""):
            return label
    return ""


def path_of(url):
    return urlsplit(url).path.strip("/")


def build_row(url, html, scraped_at):
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.find("h1", class_="product--title")
    title = (h1.get_text(" ", strip=True) if h1
             else (soup.title.get_text(strip=True) if soup.title else ""))

    offers = soup.find(attrs={"itemprop": "offers"})
    price = parse_price(offers)
    available = is_available(offers)

    info = base_info(soup)
    mpn = info.get("Hersteller SKU") or ""
    sku_meta = soup.find(attrs={"itemprop": "sku"})
    store_no = info.get("Artikel-Nr.") or (sku_meta.get("content") if sku_meta else "") or \
        (sku_meta.get_text(" ", strip=True) if sku_meta else "")
    part_number = (mpn or store_no or "").strip()

    brand_meta = soup.find("meta", attrs={"itemprop": "brand"})
    brand = (brand_meta.get("content").strip() if brand_meta and brand_meta.get("content")
             else info.get("Hersteller", "")).strip()

    desc_el = soup.find(attrs={"itemprop": "description"})
    description = schema.plain_text(desc_el.decode_contents()) if desc_el else None

    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": path_of(url),
        "sku": part_number,
        "part_number": part_number,
        "model_number": store_no.strip(),
        "title": title,
        "brand": brand,
        "category": category_of(soup),
        "condition": condition_of(title),
        "price": price,                       # EUR, as-is; None when on request / placeholder
        "available": available,
        "stock_qty": "",                      # happyware doesn't publish exact quantities
        "image_url": product_image(soup),
        "description": description,
        "url": url,
    })


# --- orchestration -----------------------------------------------------------
def scrape(limit, scraped_at):
    urls = product_urls()
    print(f"sitemap lists {len(urls)} product URLs", flush=True)
    if not urls:
        sys.exit("no product URLs found in sitemap")

    if limit:
        # Test run: crawl just enough pages (+margin for the odd 404) to reach ~limit products.
        urls = urls[:limit + 30]

    rows = {}
    gone = [0]
    failed = []

    def do(url):
        time.sleep(PAGE_DELAY)
        status, html = fetch(url, require=PRODUCT_MARKER)
        return url, status, html

    with ThreadPoolExecutor(WORKERS) as pool:
        for i, (url, status, html) in enumerate(pool.map(do, urls), 1):
            if status == 404:
                gone[0] += 1
            elif status == 200 and html:
                try:
                    rows[path_of(url)] = build_row(url, html, scraped_at)
                except Exception as e:  # noqa: BLE001 - one bad page shouldn't kill the run
                    failed.append(url)
                    print(f"WARN parse failed {url}: {e}", flush=True)
            else:
                failed.append(url)
            if i % 100 == 0 or i == len(urls):
                print(f"{i}/{len(urls)} pages: {len(rows)} products, "
                      f"{gone[0]} gone (404), {len(failed)} failed", flush=True)

    # Retry transient failures once, slowly and one at a time
    if failed and not limit:
        retry, failed = failed, []
        print(f"retrying {len(retry)} failed pages one at a time", flush=True)
        for url in retry:
            time.sleep(1.0)
            status, html = fetch(url, require=PRODUCT_MARKER)
            if status == 404:
                gone[0] += 1
            elif status == 200 and html:
                try:
                    rows[path_of(url)] = build_row(url, html, scraped_at)
                except Exception as e:  # noqa: BLE001
                    failed.append(url)
                    print(f"WARN parse failed {url}: {e}", flush=True)
            else:
                failed.append(url)

    live = len(urls) - gone[0]            # products still on the site (404s are gone, not misses)
    print(f"parsed {len(rows)} products; {gone[0]} gone (404); {len(failed)} still failed "
          f"of {live} live URLs", flush=True)

    if limit:
        out = [row for _, row in list(rows.items())[:limit]]
        return out, None                  # test run: not compared, no completeness target

    # Don't ship a partial snapshot: bail if too many live pages failed to fetch.
    if failed and len(failed) > MAX_FAILED_SHARE * max(live, 1):
        sys.exit(f"{len(failed)}/{live} product pages failed after retry; the site is likely "
                 "blocking or erroring - not trusting this run")
    return list(rows.values()), live


if __name__ == "__main__":
    runner.run(SITE, scrape)
