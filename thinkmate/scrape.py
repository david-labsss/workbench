"""Scrape thinkmate.com (custom PHP store, ~36 plain component products): every run's full
snapshot appended to Postgres.

Covered: ONLY the plain component products, i.e. the /product/<brand>/<mpn> URLs in
https://www.thinkmate.com/sitemap.xml (drives, SSDs, memory, CPUs, PDUs, ...). Each product page
is read one at a time (~1 request/s; robots.txt only blocks /images, /download, /account, /email,
/gallery, /order and /bg-):
  title         the page's h1 (the JSON-LD breadcrumb's last item when the h1 is missing)
  brand / MPN   the "<Brand> part #: <MPN>" heading (falls back to the URL path)
  product_id    Thinkmate's own product id (the /email/p/<id> link), else the URL path
  category      the breadcrumb (Home > Hardware > <category>)
  price         span.price-nice-dollars + span.price-nice-cents (USD); none shown -> empty
  available     the price box has an "Add To Order" link and its status line does not say the
                item is out of stock / discontinued / unavailable. Seen statuses (2026-10-19):
                "Limited Quantity" and "Active" -> in stock (both are orderable). The status
                text is kept in other_specs ("Status: ...").
  condition     blank: Thinkmate sells new parts only
There is no stock count, compare-at price or SKU separate from the MPN.

NOT covered: the /system/ configurator pages (servers / workstations). Their prices are computed
in JavaScript from the chosen options and were not verified, so they are not scraped.

The number of products to check against is the number of /product/ URLs in the sitemap.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import json
import re
import time

import requests
from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "thinkmate"
BASE = "https://www.thinkmate.com"
DELAY = 1.0                      # one page at a time, ~1 request/s
TRIES = 3                        # the server sometimes stalls mid-download (sitemap.xml, 2026-10-19)
RETRY_PAUSE = 30
PRODUCT_URL_RE = re.compile(r"^https://www\.thinkmate\.com/product/([^/]+)/([^/?#]+)$")
PART_HEADING_RE = re.compile(r"^(.*?)\s+part\s*#\s*:\s*(\S.*)$", re.I)
NOT_AVAILABLE_RE = re.compile(r"out of stock|discontinued|unavailable|end of life|\beol\b|"
                              r"not available|sold out|call for", re.I)
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b")      # upper case only ("8Gb" = speed)
CAPACITY_CATEGORIES = re.compile(r"drive|ssd|memory|storage", re.I)


def get(url):
    """GET with http's 429 / 5xx retries, plus a few tries for a download the server stalls or
    cuts off part-way (requests does not retry those). The last failure raises."""
    for attempt in range(1, TRIES + 1):
        try:
            r = http.session().get(url, timeout=60)
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            if attempt == TRIES:
                raise
            print(f"WARN {url}: {e.__class__.__name__}, retrying in {RETRY_PAUSE}s", flush=True)
            time.sleep(RETRY_PAUSE)


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def product_urls():
    """Every /product/<brand>/<mpn> URL in the sitemap (the /system/ pages are left out)."""
    text = get(f"{BASE}/sitemap.xml").text
    urls = [u.strip() for u in re.findall(r"<loc>([^<]+)</loc>", text)]
    return sorted({u for u in urls if PRODUCT_URL_RE.match(u)})


def breadcrumb_ld(soup):
    """[names] of the JSON-LD BreadcrumbList (the last one is the product itself)."""
    for tag in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(tag.string or "")
        except ValueError:
            continue
        if isinstance(data, dict) and data.get("@type") == "BreadcrumbList":
            items = sorted(data.get("itemListElement") or [], key=lambda i: i.get("position", 0))
            return [clean(i.get("name")) for i in items]
    return []


def price_of(box):
    dollars = box.select_one(".price-nice-dollars") if box else None
    if not dollars:
        return None
    cents = box.select_one(".price-nice-cents")
    text = clean(dollars.get_text()).replace(",", "") + (clean(cents.get_text()) if cents else "")
    try:
        value = float(text)
    except ValueError:
        return None
    return value or None                    # $0 = no real price


def parse(url, html, scraped_at):
    soup = BeautifulSoup(html, "html.parser")
    url_brand, url_mpn = PRODUCT_URL_RE.match(url).groups()
    crumbs_ld = breadcrumb_ld(soup)
    h1 = soup.select_one("h1")
    title = clean(h1.get_text()) if h1 else ""
    if crumbs_ld and (not title or "�" in title):     # the LD copy keeps symbols like (TM)
        title = crumbs_ld[-1]
    if not title:
        raise RuntimeError(f"{url}: no product title (page layout changed?)")

    brand, mpn = "", ""
    for h2 in soup.select("h2"):
        m = PART_HEADING_RE.match(clean(h2.get_text()))
        if m:
            brand, mpn = m.group(1), m.group(2)
            break
    brand = brand or url_brand.replace("-", " ").title()
    mpn = mpn or url_mpn.upper()

    crumbs = [clean(a.get_text()) for a in soup.select(".breadcrumbs a")]
    crumbs = [c for c in crumbs if c and c.lower() not in ("home", "hardware")]
    if not crumbs and len(crumbs_ld) > 1:
        crumbs = [c for c in crumbs_ld[:-1] if c.lower() != "hardware"]
    category = crumbs[-1] if crumbs else ""

    box = soup.select_one(".pricebox")
    price = price_of(box)
    status = ""
    if box:
        for div in box.find_all("div", recursive=False):
            text = clean(div.get_text(" "))
            if text and not div.find("a"):
                status = text
                break
    order = box.find("a", string=re.compile(r"add to order", re.I)) if box else None
    available = bool(order) and price is not None and not NOT_AVAILABLE_RE.search(status)

    email = soup.find("a", href=re.compile(r"^/email/p/\d+"))
    pid = re.search(r"/email/p/(\d+)", email["href"]).group(1) if email else f"{url_brand}/{url_mpn}"

    capacity = ""
    cap = CAPACITY_RE.search(title)
    if cap and CAPACITY_CATEGORIES.search(category):
        capacity = cap.group(1) + cap.group(2)
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": pid,
        "sku": mpn,
        "part_number": mpn,
        "title": title,
        "brand": brand,
        "category": category,
        "condition": "",                    # new parts only
        "capacity": capacity,
        "other_specs": f"Status: {status}" if status else "",
        "price": price,
        "available": available,
        "stock_qty": None,                  # not published
        "url": url,
    })


def scrape(limit, scraped_at):
    urls = product_urls()
    print(f"sitemap lists {len(urls)} component products", flush=True)
    rows = []
    for i, url in enumerate(urls[:limit] if limit else urls, 1):
        r = get(url)                        # a failed page raises (after http's retries)
        rows.append(parse(url, r.content, scraped_at))
        if i % 10 == 0:
            print(f"{i} pages", flush=True)
        time.sleep(DELAY)
    return rows, len(urls)


if __name__ == "__main__":
    runner.run(SITE, scrape)
