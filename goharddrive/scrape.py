"""Scrape goharddrive.com (Volusion, ~950 products): every run's full snapshot appended to Postgres.

Volusion has no public product API, and the store's search listing redirects, so every product
page in the sitemap (/sitemap.xml, "-p/<code>.htm" links) is read, a few at a time. Each page
gives the product code (unique, used as product_id and SKU), name, manufacturer, price
(itemprop "price"), breadcrumb (category = its first level after Home), whether it is in stock
(itemprop "availability" = InStock; out-of-stock pages show "(Out of Stock)" instead) and a
spec table (Manufacturer Part #, capacity, interface, ...). The store shows no stock count for
items in stock, so stock_qty is only filled when a number is shown. Condition is read from the
title (Refurbished / Recertified / New / Used).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "goharddrive"
BASE = "https://www.goharddrive.com"
WORKERS = 4
PRODUCT_URL_RE = re.compile(r"<loc>([^<]+-p/[^<]+\.htm)</loc>", re.I)

# Spec table label (lowercase, without ":" / "#") -> column
SPEC_COLUMNS = {
    "manufacturer part": "part_number", "mfr part": "part_number", "part number": "part_number",
    "model": "model_number", "model number": "model_number",
    "capacity": "capacity",
    "interface": "interface",
    "form factor": "form_factor",
    "rotation speed": "spindle_speed", "rotational speed": "spindle_speed",
    "spindle speed": "spindle_speed", "rpm": "spindle_speed",
    "cache": "cache", "buffer": "cache", "cache size": "cache",
}
SKIP_SPECS = {"manufacturer", "brand"}
CONDITIONS = [
    (re.compile(r"\brefurbished\b", re.I), "Refurbished"),
    (re.compile(r"\brecertified\b", re.I), "Recertified"),
    (re.compile(r"\bused\b", re.I), "Used"),
    (re.compile(r"\bnew\b", re.I), "New"),
]
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b", re.I)


def product_urls():
    r = http.session().get(f"{BASE}/sitemap.xml", timeout=120)
    r.raise_for_status()
    return sorted(set(PRODUCT_URL_RE.findall(r.text)))


def clean(text):
    text = (text or "").replace("\xa0", " ").replace("&nbsp;", " ")
    return re.sub(r"\s+", " ", text).strip()


def fetch_page(url):
    try:
        r = http.session().get(url, timeout=60)
        if r.status_code == 404:
            return None  # listed in the sitemap but gone
        r.raise_for_status()
        return url, r.text
    except Exception as e:  # noqa: BLE001 - one bad page must not stop the run
        print(f"WARN {url}: {e}", flush=True)
        return None


def spec_table(soup):
    """Label -> value pairs of the spec table in the product description."""
    specs, other = {}, []
    area = soup.select_one("#ProductDetail_ProductDetails_div") or soup
    for th in area.select("th"):
        td = th.find_next_sibling("td")
        if not td:
            continue
        label = clean(th.get_text()).rstrip(":").replace("#", "").strip()
        value = clean(td.get_text(" "))
        if not label or not value or len(label) > 40:
            continue
        column = SPEC_COLUMNS.get(label.lower())
        if column:
            specs.setdefault(column, value)
        elif label.lower() not in SKIP_SPECS:
            other.append(f"{label}: {value}")
    return specs, "; ".join(other)


def to_float(v):
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


def parse(url, html, scraped_at):
    soup = BeautifulSoup(html, "html.parser")
    code = soup.select_one("span.product_code")
    if not code or not clean(code.get_text()):
        return None
    name = soup.select_one(".productnamecolorLARGE [itemprop=name]") or soup.select_one(
        "[itemprop=name]")
    title = clean(name.get_text()) if name else ""
    brand = soup.select_one("meta[itemprop=manufacturer]")
    price = soup.select_one("[itemprop=price]")
    availability = soup.select_one("meta[itemprop=availability]")
    in_stock = bool(availability and "instock" in (availability.get("content") or "").lower())
    stock = None
    qty = soup.select_one("[class^=StockQuantity]")
    if qty:
        m = re.search(r"\d+", qty.get_text())
        stock = float(m.group(0)) if m else (0.0 if "out of stock" in qty.get_text().lower() else None)
    crumbs = [clean(a.get_text()) for a in soup.select("td.vCSS_breadcrumb_td a")]
    crumbs = [c for c in crumbs if c and c.lower() != "home"]
    specs, other = spec_table(soup)
    condition = next((label for rx, label in CONDITIONS if rx.search(title)), "")
    cap = CAPACITY_RE.search(specs.get("capacity") or title)
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": clean(code.get_text()).upper(),
        "sku": clean(code.get_text()).upper(),
        "part_number": specs.get("part_number", ""),
        "model_number": specs.get("model_number", ""),
        "title": title,
        "brand": clean(brand.get("content")) if brand else "",
        "category": crumbs[0] if crumbs else "",
        "condition": condition,
        "capacity": specs.get("capacity") or ((cap.group(1) + cap.group(2).upper()) if cap else ""),
        "interface": specs.get("interface", ""),
        "form_factor": specs.get("form_factor", ""),
        "spindle_speed": specs.get("spindle_speed", ""),
        "cache": specs.get("cache", ""),
        "other_specs": other,
        "price": to_float(price.get("content") or price.get_text()) if price else None,
        "available": in_stock,
        "stock_qty": stock,
        "url": url,
    })


def scrape(limit, scraped_at):
    urls = product_urls()
    print(f"sitemap lists {len(urls)} products", flush=True)
    todo = urls[:limit] if limit else urls
    rows, seen = [], set()
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, page in enumerate(pool.map(fetch_page, todo), 1):
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
