"""Scrape www.redcorp.com (European B2B IT reseller, custom ASP.NET webshop): every run's full
snapshot of the server / storage / networking catalogue, appended to Postgres.

redcorp has no public product API, but it publishes XML product sitemaps (listed in robots.txt).
The English ("UK") sitemap index /UK-Products-SitemapIndex.xml links ~436 per-category sitemaps,
each listing product-page URLs of the form /en/product/<category>/<brand>/<slug>/<id>. Prices are
public (no login needed): every product page carries a schema.org/Product JSON-LD block with name,
mpn, sku (= redcorp's own item id), brand, condition, availability and the price in EUR. The site
is read the same way as goharddrive: collect the product URLs from the sitemaps, then read each
product page. The catalogue is large, so only product URLs whose category segment is about
server / storage / networking hardware are kept; MAX_PRODUCTS caps test runs.

redcorp sits behind Cloudflare and its origin intermittently answers 522/52x; get() retries those
and connection errors so a flaky response never looks like a gone product. Prices are euros (EUR).
Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

import requests

from common import http, runner, schema

SITE = "redcorp"
BASE = "https://www.redcorp.com"
SITEMAP_INDEX = f"{BASE}/UK-Products-SitemapIndex.xml"
WORKERS = 4
RETRY_STATUS = {429, 500, 502, 503, 504, 520, 521, 522, 523, 524}

LOC_RE = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>", re.I)
LD_RE = re.compile(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', re.S | re.I)
CATEGORY_RE = re.compile(r"/en/product/([^/]+)/")
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b", re.I)

# Product URLs are kept when their category segment looks like server / storage / networking gear
# (SCOPE_RE) and is not printer/consumer tat that happens to share a word (EXCLUDE_RE, e.g. the
# "tape" in printer label "supplies-tape-cartridge").
SCOPE_RE = re.compile(
    r"ssd|hdd|sata|\bsas\b|scsi|nvme|hard-dis|hard-driv|storage|\bnas\b|storage-area|raid|\blto\b|"
    r"tape-driv|memory|\bram\b|dimm|server|\brack|\bblade|workstation|network|\bswitch|router|"
    r"firewall|transceiver|\bsfp\b|gbic|fibre|fiber|ethernet|wireless|wi-fi|access-point|\bmodem|"
    r"\bkvm\b|processor|\bcpu\b|motherboard|mainboard|power-sup|\bpsu\b|graphic|\bgpu\b|controller|"
    r"host-bus|\bhba\b", re.I)
EXCLUDE_RE = re.compile(
    r"suppl|\bink\b|toner|ribbon|printer|cartridge|label|\bpaper\b|projector|scanner|lamp|headset|"
    r"webcam|keyboard|\bmouse\b|speaker|monitor|tablet|smartphone|\bphone\b|camera|battery|charger",
    re.I)

CONDITIONS = [("refurbish", "Refurbished"), ("used", "Used"), ("damaged", "Damaged"),
              ("new", "New")]


def get(url, timeout=60):
    """A GET that retries Cloudflare 52x / 5xx / connection errors (common/http handles 429/5xx
    at the adapter level, but not Cloudflare's 522). Returns the response (incl. 404) or raises."""
    last = None
    for attempt in range(6):
        try:
            r = http.session().get(url, timeout=timeout)
            if r.status_code in RETRY_STATUS:
                last = f"HTTP {r.status_code}"
            else:
                return r
        except requests.RequestException as e:
            last = str(e)
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"{url}: {last}")


def sub_sitemaps():
    r = get(SITEMAP_INDEX)
    r.raise_for_status()
    return [u for u in LOC_RE.findall(r.text) if u.lower().endswith(".xml")]


def scoped_urls(sitemap_url):
    """Product URLs in one sitemap whose category segment is in scope."""
    r = get(sitemap_url)
    r.raise_for_status()
    out = []
    for u in LOC_RE.findall(r.text):
        m = CATEGORY_RE.search(u)
        if m and SCOPE_RE.search(m.group(1)) and not EXCLUDE_RE.search(m.group(1)):
            out.append(u)
    return out


def find_product_ld(html):
    """The schema.org Product object from a page's JSON-LD blocks, or None."""
    for block in LD_RE.findall(html):
        try:
            data = json.loads(block.strip())
        except ValueError:
            continue
        for node in (data if isinstance(data, list) else
                     (data.get("@graph") if isinstance(data, dict) and "@graph" in data else [data])):
            if isinstance(node, dict) and "Product" in str(node.get("@type", "")):
                return node
    return None


def clean(v):
    return re.sub(r"\s+", " ", str(v or "")).strip()


def to_float(v):
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


def abs_image(img):
    """First image URL from a schema.org image value (str / list / ImageObject), absolute."""
    if isinstance(img, list):
        img = img[0] if img else None
    if isinstance(img, dict):
        img = img.get("url") or img.get("contentUrl")
    if not isinstance(img, str) or not img.strip():
        return None
    return urljoin(BASE, img.strip())


def parse(url, scraped_at):
    r = get(url)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    node = find_product_ld(r.text)
    if not node:
        print(f"WARN no Product JSON-LD: {url}", flush=True)
        return None
    offer = node.get("offers") or {}
    if isinstance(offer, list):
        offer = offer[0] if offer else {}
    brand = node.get("brand")
    brand = brand.get("name") if isinstance(brand, dict) else brand
    cond_src = (str(offer.get("itemCondition", "")) + " " + clean(node.get("name"))).lower()
    condition = next((label for key, label in CONDITIONS if key in cond_src), "")
    title = clean(node.get("name"))
    cat = CATEGORY_RE.search(url)
    cap = CAPACITY_RE.search(title)
    avail = str(offer.get("availability", "")).lower()
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": clean(node.get("sku")) or url.rsplit("/", 1)[-1],
        "sku": clean(node.get("sku")),
        "part_number": clean(node.get("mpn")),
        "title": title,
        "brand": clean(brand),
        "category": cat.group(1) if cat else "",
        "condition": condition,
        "capacity": (cap.group(1) + cap.group(2).upper()) if cap else "",
        "price": to_float(offer.get("price")),
        "available": "instock" in avail or "preorder" in avail or "limitedavailability" in avail,
        "image_url": abs_image(node.get("image")),
        "description": schema.plain_text(node.get("description")),
        "url": clean(offer.get("url")) or url,
    })


def scrape(limit, scraped_at):
    sitemaps = sub_sitemaps()
    print(f"{len(sitemaps)} product sitemaps in the UK index", flush=True)
    urls, seen = [], set()
    if limit:
        # test run: read sitemaps one at a time and stop as soon as we have enough in scope
        for sm in sitemaps:
            for u in scoped_urls(sm):
                if u not in seen:
                    seen.add(u)
                    urls.append(u)
            if len(urls) >= limit:
                break
        urls = urls[:limit]
    else:
        with ThreadPoolExecutor(WORKERS) as pool:
            for found in pool.map(scoped_urls, sitemaps):
                for u in found:
                    if u not in seen:
                        seen.add(u)
                        urls.append(u)
    print(f"{len(urls)} in-scope product pages to read", flush=True)
    rows = []
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, row in enumerate(pool.map(lambda u: parse(u, scraped_at), urls), 1):
            if row:
                rows.append(row)
            if i % 200 == 0 or i == len(urls):
                print(f"{i}/{len(urls)} pages, {len(rows)} products", flush=True)
    # listed = in-scope URLs found; the runner refuses a run that reaches < 98% of them
    return rows, (None if limit else len(urls))


if __name__ == "__main__":
    runner.run(SITE, scrape)
