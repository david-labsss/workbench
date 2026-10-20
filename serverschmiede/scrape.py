"""Scrape serverschmiede.com (Serverschmiede.com GmbH, custom "SEO:mercari" PHP store, ~2,800
products: refurbished enterprise servers, storage and components): every run's full snapshot
appended to Postgres. Prices are in EUR.

The store has no product API, so every product in /sitemap.xml (links of the form /de/<slug>) is
read a few at a time. Each product page is server-rendered:
  * the store's article id is the hidden "products_id" input -> product_id;
  * the gross price is the single .basis_top element, shown German-formatted ("5.221,00 €");
  * the title is the <h1> and the brand is taken from its first word.
A page without a products_id / price is a category or info page and is skipped. Configurable
servers show their configured ("from") price; components show their fixed price.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "serverschmiede"
BASE = "https://www.serverschmiede.com"
WORKERS = 5
PRODUCT_RE = re.compile(r"^https://www\.serverschmiede\.com/de/.+", re.I)
EUR_RE = re.compile(r"(\d{1,3}(?:\.\d{3})*|\d+),(\d{2})")   # German "5.221,00"
ID_RE = re.compile(r'name=["\']products_id["\']\s+value=["\'](\d+)["\']', re.I)
OOS = ("ausverkauft", "nicht verfügbar", "nicht lieferbar", "sold out", "out of stock")

# The title often starts with a dimension ("0,15m ...") not the brand, so match a known brand
# anywhere in it; the longer, more specific names are listed first.
BRANDS = ("HPE", "HP", "Dell EMC", "Dell", "Lenovo", "IBM", "Fujitsu", "Supermicro",
          "Super Micro", "Intel", "AMD", "Cisco", "Juniper", "Mellanox", "NVIDIA", "Broadcom",
          "LSI", "Samsung", "Seagate", "Toshiba", "Western Digital", "WD", "Micron", "Kingston",
          "Crucial", "SK Hynix", "Hynix", "NetApp", "EMC", "Quantum", "Adaptec", "QLogic",
          "Emulex", "Brocade", "Arista", "Netgear", "Aruba", "Hitachi", "SanDisk", "Kioxia",
          "Oracle", "Sun", "Inphi", "Nutanix")


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def brand(title):
    low = f" {title.lower()} "
    for b in BRANDS:
        if re.search(r"(?<![a-z])" + re.escape(b.lower()) + r"(?![a-z])", low):
            return b
    return ""


def eur(text):
    """A German-formatted euro amount ("5.221,00 €") as a float, or None."""
    m = EUR_RE.search(text or "")
    return float(m.group(1).replace(".", "") + "." + m.group(2)) if m else None


def product_urls():
    r = http.session().get(f"{BASE}/sitemap.xml", timeout=120)
    r.raise_for_status()
    urls = [loc for loc in re.findall(r"<loc>([^<]+)</loc>", r.text) if PRODUCT_RE.match(loc)]
    return sorted(set(urls))


def build_row(url, html, scraped_at):
    m = ID_RE.search(html)
    if not m:
        return None                   # not a buyable product page
    product_id = m.group(1)
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.select_one("h1")
    title = clean(h1.get_text()) if h1 else ""
    basis = soup.select_one(".basis_top")
    price = eur(basis.get_text()) if basis else None
    text = html.lower()
    available = price is not None and not any(s in text for s in OOS)
    # og:image is only the site logo; the product photo is the /product_images/ <img>
    img = soup.find("img", src=re.compile(r"/product_images/"))
    image_url = urljoin(BASE, img.get("src")) if img and img.get("src") else None
    meta_desc = soup.find("meta", attrs={"name": "description"})
    description = schema.plain_text(meta_desc.get("content")) if meta_desc else None
    return schema.product_row({
        "scraped_at": scraped_at, "website": SITE,
        "product_id": product_id, "sku": product_id,
        "title": title,
        "brand": brand(title),
        "price": price,
        "available": available,
        "image_url": image_url,
        "description": description,
        "url": url,
    })


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
    print(f"sitemap lists {len(urls)} product URLs", flush=True)
    todo = urls[:limit] if limit else urls
    rows, seen = [], set()
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, page in enumerate(pool.map(fetch, todo), 1):
            if page:
                row = build_row(page[0], page[1], scraped_at)
                if row:
                    pid = row[schema.PRODUCT_COLUMNS.index("product_id")]
                    if pid and pid not in seen:
                        seen.add(pid)
                        rows.append(row)
            if i % 200 == 0 or i == len(todo):
                print(f"{i}/{len(todo)} products, {len(rows)} rows", flush=True)
    # listed left as the product-URL count; non-product /de/ pages (few) are skipped above
    return rows, len(urls)


if __name__ == "__main__":
    runner.run(SITE, scrape)
