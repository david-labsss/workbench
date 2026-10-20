"""Scrape serverflow.ru (ServerFlow, a Russian server-hardware store on 1C-Bitrix): every
run's full snapshot appended to Postgres.

Bitrix exposes no public JSON API, but every product page carries schema.org microdata: the
main offer is a <div class="card__right-container" itemtype=".../Offer"> holding itemprop
price / priceCurrency (RUB) / availability. Prices are public, no login. Product URLs come
from the XML sitemap index (/sitemap.xml -> sitemap-iblock-*.xml). Those list both section
(category) and element (product) URLs under /catalog/...; products are the "leaf" URLs (not a
prefix of any other URL), and any leaf that turns out to be a childless category has no main
offer and is skipped. Condition is read from the offer text ("Б/У" = used).

Currency: RUB. Titles/categories are Russian (Cyrillic); json.dumps keeps them as \\uXXXX in
DRY_RUN output and Postgres stores UTF-8.

Reachability: the site is behind DDoS-Guard and is very slow from some regions (large ~400 KB
pages, frequent timeouts from Pakistan). The data shape is confirmed; a full run needs a
patient RU/EU runner -- FLAG: needs an RU/EU runner + smoke test. No reliable store-wide
product count is available, so the 98% completeness gate is skipped (listed = None).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
Env: WORKERS (parallel product fetches, default 3), plus MAX_PRODUCTS / DRY_RUN (runner).
"""
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "serverflow"
BASE = "https://serverflow.ru"
SITEMAP = f"{BASE}/sitemap.xml"
WORKERS = int(os.getenv("WORKERS") or 3)
FETCH_RETRIES = 3
PAGE_DELAY = 0.3
TIMEOUT = 90               # pages are large and the origin is slow
NUM_RE = re.compile(r"\d[\d\s .,]*")


def fetch(url, timeout=TIMEOUT):
    """GET text, or None after repeated failures (the caller skips one URL; the run as a
    whole still reports how many were skipped)."""
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            r = http.session().get(url, timeout=timeout,
                                   headers={"Accept-Language": "ru,en;q=0.9"})
            if r.status_code == 200 and r.text:
                return r.text
            print(f"WARN {url} HTTP {r.status_code} (attempt {attempt})", flush=True)
        except Exception as e:
            print(f"WARN {url} {type(e).__name__} (attempt {attempt})", flush=True)
        time.sleep(3 * attempt)
    return None


def locs(xml):
    return re.findall(r"<loc>\s*([^<]+?)\s*</loc>", xml or "")


def iblock_sitemaps():
    """Product/section sitemaps from the index (the files sitemap is skipped)."""
    xml = fetch(SITEMAP)
    if not xml:
        raise RuntimeError("could not fetch sitemap index")
    return [u for u in locs(xml) if "sitemap-iblock" in u]


def leaf_products(paths):
    """Leaf /catalog/ paths: those that are not a prefix of any other path (sections are
    prefixes of the products beneath them)."""
    paths = sorted(p for p in set(paths) if "/catalog/" in p)
    pset = set(paths)
    return [p for p in paths
            if not any(o != p and o.startswith(p) for o in pset)]


def product_urls(limit):
    """Candidate product URLs from the sitemaps. For a test run, stop pulling sitemaps once
    there are plainly enough candidates, then keep the first `limit`."""
    accumulated = []
    for sm in iblock_sitemaps():
        xml = fetch(sm)
        if not xml:
            print(f"WARN skipping unreadable sitemap {sm}", flush=True)
            continue
        paths = [re.sub(r"^https?://[^/]+", "", u) for u in locs(xml)]
        accumulated += [p for p in paths if "/catalog/" in p]
        print(f"{sm.rsplit('/', 1)[-1]}: {len(paths)} urls (catalog total {len(accumulated)})",
              flush=True)
        if limit and len(accumulated) >= limit * 4:
            break
    leaves = leaf_products(accumulated)
    print(f"{len(leaves)} leaf product candidates", flush=True)
    urls = [BASE + p for p in leaves]
    return urls[:limit] if limit else urls


def to_float(text):
    if text is None:
        return None
    m = NUM_RE.search(str(text))
    if not m:
        return None
    digits = re.sub(r"[\s ]", "", m.group(0)).replace(",", ".").rstrip(".")
    try:
        return float(digits)
    except ValueError:
        return None


def prop(scope, name):
    el = scope.find(attrs={"itemprop": name}) if scope else None
    if not el:
        return None
    return (el.get("content") or el.get_text(" ", strip=True) or "").strip() or None


def parse_product(html, url, scraped_at):
    """One product row from a product page, or None if it has no main public offer (a
    category page or a layout we don't recognise)."""
    soup = BeautifulSoup(html, "html.parser")
    offer = soup.select_one('div.card__right-container[itemtype*="Offer"]')
    if not offer:
        return None
    price = to_float(prop(offer, "price"))
    if price is None:
        return None
    avail = (prop(offer, "availability") or "").lower()
    available = ("instock" in avail.replace("/", "").replace(" ", "")
                 or ("налич" in avail and "нет" not in avail))
    h1 = soup.find("h1")
    name = (h1.get_text(" ", strip=True) if h1 else "") or (
        (soup.find("meta", property="og:title") or {}).get("content", "")).split(" купить")[0]
    old = offer.select_one('[class*="old"]')
    old_price = to_float(old.get_text(" ", strip=True)) if old else None
    card = offer.find_parent(class_="cardcard") or soup
    card_text = card.get_text(" ", strip=True).lower()
    condition = "Used (Б/У)" if "б/у" in card_text else ""
    path = re.sub(r"^https?://[^/]+", "", url).strip("/")
    og_image = soup.find("meta", property="og:image")
    image = og_image.get("content") if og_image else None
    if not image:
        img_el = soup.find(attrs={"itemprop": "image"})
        image = (img_el.get("content") or img_el.get("src")) if img_el else None
    if image:
        image = "https:" + image if image.startswith("//") else (
            BASE + image if image.startswith("/") else image)
    desc_el = soup.find(attrs={"itemprop": "description"})
    description = (desc_el.get("content") or desc_el.get_text(" ", strip=True)) if desc_el else None
    if not description:
        og_desc = soup.find("meta", property="og:description")
        description = og_desc.get("content") if og_desc else None
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": path,                 # the catalog path, unique per product
        "sku": prop(offer, "sku") or "",
        "title": name,
        "brand": "",   # not in clean microdata; a page-wide guess was unreliable, left blank
        "category": " > ".join(path.split("/")[1:-1]),  # /catalog/<cats...>/<slug>
        "condition": condition,
        "price": price,
        "compare_at_price": old_price if old_price and old_price > price else None,
        "available": available,
        "stock_qty": None,
        "image_url": image,
        "description": schema.plain_text(description),
        "url": url,
    })


def scrape(limit, scraped_at):
    urls = product_urls(limit)
    print(f"{len(urls)} product URLs to fetch", flush=True)
    rows, done, failed, skipped = [], 0, 0, 0
    with ThreadPoolExecutor(WORKERS) as pool:
        def work(u):
            time.sleep(PAGE_DELAY)
            return u, fetch(u)

        futures = [pool.submit(work, u) for u in urls]
        for fut in as_completed(futures):
            url, html = fut.result()
            done += 1
            if not html:
                failed += 1
            else:
                row = parse_product(html, url, scraped_at)
                if row:
                    rows.append(row)
                else:
                    skipped += 1       # a category leaf without a product offer
            if done % 100 == 0 or done == len(urls):
                print(f"fetched {done}/{len(urls)} ({len(rows)} rows, {failed} failed, "
                      f"{skipped} non-product)", flush=True)
    # No reliable store-wide product count; skip the completeness gate.
    return rows, None


if __name__ == "__main__":
    runner.run(SITE, scrape)
