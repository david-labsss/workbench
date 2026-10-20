"""Scrape dellpartsdirect.com (Dell / HP server drives, SSDs, memory, CPUs, controllers, PSUs,
networking; old osCommerce store, server-rendered HTML, public prices; ~30-60k listing rows):
every run's full snapshot appended to Postgres.

US / CANADA ONLY: the site is behind a Cloudflare WAF rule that answers "Sorry, you have been
blocked" (HTTP 403) to other countries (Pakistan) and to some hosting networks; plain requests
from US / Canadian datacenter IPs got 200 (check-host.net, 2026-10-20). So this only works on a
US runner. It was built from archived copies of the pages (Wayback Machine: category listing
2020-02, model listing 2023-08, product page 2023-08, home page 2025-03), so it checks the live
format hard and stops with a clear message (plus a raw snippet in the log) when anything is off.
robots.txt (2022 capture): "User-agent: * / Allow: /".

Categories: the home page's category menu (default.php?cPath=N links) is read every run; these
are expected (2025-03 names) and new ones found there are added automatically:
  150 Solid State Drives, 130 SAS/SATA Drives, 124 U320 Drives, 131 Fibre Drives, 100 Memory,
  101 Controllers, 102 Processors, 125 Power Supply, 200 Networking, 113 Tape Drives, 129 Misc
A category listing shows every product of the category, 50 rows a page, in the store's default
order (by description, the same sort=4a its own pagination links carry):
  /default.php?cPath=<id>&sort=4a&page=N
Each row: stock ("245 In Stock"), part #, pack size ("5 Pack"), the name ("<part#> <Brand>
<description>"), "Products ID", ships, condition, warranty, manufacturer, server / model,
generation, and the price ("$1,485.95"; "Special" prices are shown the same way). Pack variants
are separate products (own Products ID) and their price is the price of the whole pack, which is
the price the product page shows (product page 2023: "2 Pack $755.95 [ $377.98 ea ]").

Every page is checked: its "Displaying A to B (of N products)" label must start at
(page - 1) * 50 + 1 and the page must show exactly B - A + 1 product rows (a page past the end
because the catalogue shrank mid-run is accepted with a warning). The listing sorts by
description, which repeats (pack variants), and MySQL may order equal values differently from one
page to the next; so after each category the unique Products IDs found must reach the smallest
"of N" within max(SHORT_MIN, 2%), else the category is walked again in part-number order
(sort=3a) and the two walks merged; still short -> the run stops. listed = the sum of the
categories' "of N" minus products found in more than one category.

Fields: product_id = Products ID (unique per row, pack variants included); sku / part_number =
the part #; title = the name; brand = "Manufacturer:" (or the word after the part # in the name);
condition; capacity from drive / memory names ("600-GB" -> 600GB); price = the row's $ price
(no $ amount -> empty, never 0); compare_at_price = a struck-through price if a row shows one;
available / stock_qty from "N In Stock" (no number -> available from the text, count empty);
other_specs = pack, generation, warranty, ships, server / model, "Special"; image_url = the row's
thumbnail; description stays empty (the listing has only the name, and reading every product page
would multiply the requests ~50x). Prices must be USD:
a page whose title says CAD / EUR (the store also has a /canada/ store) stops the run.

Test runs (MAX_PRODUCTS) read page 1 of every category and then compare the first PRICE_CHECKS
listing prices with their product pages (itemprop="price"); any mismatch stops the run.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import html as htmllib
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import parse_qs, urlsplit

import requests
from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "dellpartsdirect"
BASE = "https://www.dellpartsdirect.com"
CATEGORIES = {                  # cPath -> name (home page menu, 2025-03); the menu adds new ones
    "150": "Solid State Drives", "130": "SAS/SATA Drives", "124": "U320 Drives",
    "131": "Fibre Drives", "100": "Memory", "101": "Controllers", "102": "Processors",
    "125": "Power Supply", "200": "Networking", "113": "Tape Drives", "129": "Misc",
}
DRIVE_CATEGORIES = {"150", "130", "124", "131"}
CAPACITY_CATEGORIES = DRIVE_CATEGORIES | {"100"}
SORTS = ["4a", "3a"]            # description A-Z (the store's default), then part # A-Z
PAGE_SIZE = 50
WORKERS = 3                     # requests in flight
DELAY = 1.0                     # per worker, after each request
TRIES = 3                       # attempts for stalled / cut-off downloads
RETRY_PAUSE = 30
SHORT_MIN = 5
PRICE_CHECKS = 3
LABEL_RE = re.compile(r"Displaying\s+([\d,]+)\s+to\s+([\d,]+)\s+\(of\s+([\d,]+)\s+products\)", re.I)
PRICE_RE = re.compile(r"\$\s*([\d,]+\.\d{2})")
STOCK_RE = re.compile(r"^([\d,]+)\s*(.*)$")
PACK_RE = re.compile(r"(\d+)\s*-?\s*Pack\b", re.I)
FIELD_NAMES = ["Products ID", "Ships", "Condition", "Warranty", "Manufacturer", "Server", "Model",
               "Generation", "Shipping"]
FIELD_RE = re.compile(r"(%s):\s*(.*?)\s*(?=\||(?:%s):|Detailed Product Description|$)"
                      % ("|".join(FIELD_NAMES), "|".join(FIELD_NAMES)))
CAPACITY_RE = re.compile(r"(?<![\d.])(\d+(?:\.\d+)?)\s?-?\s?(TB|GB)\b")
MENU_RE = re.compile(r"(?:^|/)default\.php\?cPath=(\d+)$")
ROW_START_RE = re.compile(r"<tr\s+class=[\"']?rounded_new2(?![\w-])", re.I)


class Blocked(RuntimeError):
    """dellpartsdirect.com refused this network (Cloudflare WAF 403)."""


def get(url):
    """GET with http's 429 / 5xx retries, plus a few tries for a download the server stalls or
    cuts off part-way. A 403 (Cloudflare block) raises at once; it is not worked around."""
    for attempt in range(1, TRIES + 1):
        try:
            r = http.session().get(url, timeout=(30, 180))
            if r.status_code == 403:
                raise Blocked(f"{url}: HTTP 403 - dellpartsdirect.com (Cloudflare) blocks this "
                              "network/country; run this scraper on a US runner. Page says: "
                              f"{snippet(r.text, 300)}")
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            if attempt == TRIES:
                raise
            print(f"WARN {url[:120]}: {e.__class__.__name__}, retrying in {RETRY_PAUSE}s",
                  flush=True)
            time.sleep(RETRY_PAUSE)


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def snippet(html, n=1500):
    body = re.split(r"<body[^>]*>", html, maxsplit=1, flags=re.I)[-1]
    return clean(htmllib.unescape(re.sub(r"<script.*?</script>|<style.*?</style>|<[^>]+>", " ",
                                         body, flags=re.S | re.I)))[:n]


def money(text):
    m = PRICE_RE.search(text or "")
    value = float(m.group(1).replace(",", "")) if m else None
    return value or None


def page_url(cid, page, sort):
    return f"{BASE}/default.php?cPath={cid}&sort={sort}&page={page}"


def decode(r):
    """osCommerce pages are ISO-8859-1."""
    return r.content.decode(r.encoding or "iso-8859-1", errors="replace")


def menu_categories(html):
    """{cPath: name} from the home page's category menu."""
    soup = BeautifulSoup(html, "html.parser")
    found = {}
    for a in soup.find_all("a", href=True):
        m = MENU_RE.search(a["href"].strip())
        name = clean(a.get_text(" "))
        if m and name:
            found.setdefault(m.group(1), name)
    return found


def categories():
    """{cPath: name}: the expected ones plus any new ones in the home page menu."""
    html = decode(get(f"{BASE}/"))
    menu = menu_categories(html)
    print(f"home page menu: {menu}", flush=True)
    if not set(menu) & set(CATEGORIES):
        raise RuntimeError(f"none of the expected categories {sorted(CATEGORIES)} is in the home "
                           f"page menu (found {menu}); page format changed? {snippet(html)}")
    missing = sorted(set(CATEGORIES) - set(menu))
    if missing:
        print(f"WARN categories not in the home page menu (still read): {missing}", flush=True)
    cats = dict(CATEGORIES)
    for cid, name in menu.items():
        if cid not in cats:
            print(f"NEW category in the home page menu, added: {cid} {name}", flush=True)
            cats[cid] = name
    return cats


def fields(text):
    return {k: v.strip(" |") for k, v in FIELD_RE.findall(text)}


def product_url(row_tr, pid):
    for a in row_tr.find_all("a", href=True):
        if "product_info.php" in a["href"]:
            value = (parse_qs(urlsplit(a["href"]).query).get("products_id") or [""])[0]
            if value.split("_")[0] == pid:
                return f"{BASE}/product_info.php?products_id={value}"
    return f"{BASE}/product_info.php?products_id={pid}"


def parse_row(tr):
    pid = clean(tr.find("input", attrs={"name": "products_id"}).get("value"))
    tds = tr.find_all("td", recursive=False)
    if len(tds) < 5 or not pid.isdigit():
        raise RuntimeError(f"product row {pid!r} with {len(tds)} cells: {clean(tr.get_text(' '))[:300]}")
    desc_td = next((td for td in tds if "Products ID" in td.get_text()), None)
    if desc_td is None:
        raise RuntimeError(f"product row {pid} without 'Products ID': {clean(tr.get_text(' '))[:300]}")
    stock_td, part_td, price_td = tds[1], tds[2], tds[-1]
    info = fields(clean(desc_td.get_text(" ")))
    if info.get("Products ID") and info["Products ID"] != pid:
        raise RuntimeError(f"row says Products ID {info['Products ID']} but its cart form {pid}")
    title_el = desc_td.find("span")
    part_el = part_td.find("span")
    struck = [clean(s.get_text(" ")) for s in price_td.find_all(["s", "del", "strike"])]
    for s in price_td.find_all(["s", "del", "strike"]):
        s.extract()
    price_text = clean(price_td.get_text(" "))
    amounts = PRICE_RE.findall(price_text)
    special = re.search(r"special\s*\$\s*([\d,]+\.\d{2})", price_text, re.I)
    if special:
        price = float(special.group(1).replace(",", ""))
    else:
        price = float(amounts[-1].replace(",", "")) if amounts else None
    pack = PACK_RE.search(" ".join(clean(td.get_text(" ")) for td in (part_td, desc_td, price_td)))
    img = tds[0].find("img")
    image = (img.get("src") or img.get("data-src") or "").strip() if img else ""
    if image.startswith("//"):
        image = "https:" + image
    elif image and not image.startswith("http"):
        image = f"{BASE}/{image.lstrip('/')}"
    return {
        "image": image,
        "id": pid,
        "url": product_url(tr, pid),
        "part": clean(part_el.get_text(" ")) if part_el else "",
        "title": clean(title_el.get_text(" ")) if title_el else "",
        "info": info,
        "stock_text": clean(stock_td.get_text(" ")),
        "price_text": price_text,
        "price": price or None,
        "amounts": len(amounts),
        "compare": money(struck[0]) if struck else None,
        "special": bool(re.search(r"\bspecial\b", price_text, re.I)),
        "pack": int(pack.group(1)) if pack else 1,
    }


def row_trs(html):
    """The product rows (<tr> with a products_id cart input). A listing page is ~0.9 MB of nested
    tables that takes seconds to parse whole, so each row (<tr class=rounded_new2 ...> up to the
    next one) is parsed on its own; if no row is found that way, the whole page is parsed."""
    starts = [m.start() for m in ROW_START_RE.finditer(html)]
    trs = []
    for a, b in zip(starts, starts[1:] + [len(html)]):
        chunk = html[a:b]
        if 'name="products_id"' not in chunk:
            continue
        end = chunk.find("</table>")
        tr = BeautifulSoup(chunk[:end] if end > 0 else chunk, "html.parser").find("tr")
        if tr is not None:
            trs.append(tr)
    if trs or 'name="products_id"' not in html:
        return trs
    soup = BeautifulSoup(html, "html.parser")
    return [tr for tr in soup.find_all("tr")
            if not tr.find("tr") and tr.find("input", attrs={"name": "products_id"})]


def parse_page(html):
    """{label: (A, B, N) or None, rows: [...], title: page <title>, empty: bool}."""
    at = html.find("Displaying")
    m = LABEL_RE.search(clean(htmllib.unescape(re.sub(r"<[^>]+>", " ", html[at:at + 600]))))         if at >= 0 else None
    title = re.search(r"<title>(.*?)</title>", html, re.S | re.I)
    return {
        "label": tuple(int(x.replace(",", "")) for x in m.groups()) if m else None,
        "rows": [parse_row(tr) for tr in row_trs(html)],
        "title": clean(title.group(1)) if title else "",
        "empty": bool(re.search(r"There are no products", html, re.I)),
    }


def check_page(url, html, parsed, page):
    """Raise when the page is not the listing page asked for. Returns False for a page past the
    end (catalogue shrank), True otherwise."""
    if re.search(r"\b(CAD|EUR)\b", parsed["title"]):
        raise RuntimeError(f"{url}: page title {parsed['title']!r} - prices are not in USD")
    label, rows = parsed["label"], parsed["rows"]
    if not label:
        if parsed["empty"] and not rows:
            return True
        raise RuntimeError(f"{url}: no 'Displaying A to B (of N products)' label ({len(rows)} "
                           f"rows); page format changed? Page text: {snippet(html)}")
    first, last, total = label
    start = (page - 1) * PAGE_SIZE + 1
    if first != start:
        if first < start and last == total and start > total:
            print(f"WARN {url}: past the end now ({total} products)", flush=True)
            return False
        raise RuntimeError(f"{url}: label says {first} to {last} of {total}, page {page} should "
                           f"start at {start}")
    if len(rows) != last - first + 1:
        raise RuntimeError(f"{url}: {len(rows)} product rows but the label says {first} to {last} "
                           f"of {total}; row markup changed? Page text: {snippet(html)}")
    no_part = sum(1 for r in rows if not r["part"])
    no_price = [r for r in rows if r["price"] is None]
    if no_part > len(rows) // 10:
        raise RuntimeError(f"{url}: {no_part} of {len(rows)} rows without a part #; first row: "
                           f"{rows[0]}")
    if len(no_price) > len(rows) // 2:
        raise RuntimeError(f"{url}: {len(no_price)} of {len(rows)} rows without a $ price; price "
                           f"texts: {[r['price_text'] for r in no_price[:5]]}")
    return True


def fetch_page(cid, page, sort):
    """(rows, total) of one listing page (total None for an empty category)."""
    url = page_url(cid, page, sort)
    html = decode(get(url))
    parsed = parse_page(html)
    try:
        current = check_page(url, html, parsed, page)
    finally:
        time.sleep(DELAY)
    total = parsed["label"][2] if parsed["label"] else 0
    return (parsed["rows"] if current else []), total


def walk_category(pool, cid, sort, first=None):
    """({Products ID: row}, smallest total shown) of one category in one sort order."""
    first = first or fetch_page(cid, 1, sort)
    results = [first]
    pages = math.ceil(first[1] / PAGE_SIZE)
    results += list(pool.map(lambda p: fetch_page(cid, p, sort), range(2, pages + 1)))
    grown = max(r[1] for r in results)
    if math.ceil(grown / PAGE_SIZE) > pages:
        extra = range(pages + 1, math.ceil(grown / PAGE_SIZE) + 1)
        results += list(pool.map(lambda p: fetch_page(cid, p, sort), extra))
    found = {}
    for rows, _ in results:
        for r in rows:
            found.setdefault(r["id"], r)
    return found, min(r[1] for r in results)


def walk(pool, cats, limit):
    """[(cPath, {id: row}, shown total)] for every category."""
    ids = list(cats)
    firsts = list(pool.map(lambda c: fetch_page(c, 1, SORTS[0]), ids))
    for cid, (rows, total) in zip(ids, firsts):
        print(f"{cid} {cats[cid]}: {total} products, {math.ceil(total / PAGE_SIZE)} pages; first: "
              f"{[(r['part'], r['price'], r['stock_text']) for r in rows[:3]]}", flush=True)
    out = []
    for cid, first in zip(ids, firsts):
        if limit:
            out.append((cid, {r["id"]: r for r in first[0]}, first[1]))
            continue
        found, shown = walk_category(pool, cid, SORTS[0], first)
        slack = max(SHORT_MIN, int(shown * 0.02))
        for sort in SORTS[1:]:
            if shown - len(found) <= slack:
                break
            print(f"WARN {cid} {cats[cid]}: {len(found)} unique of {shown}; walking it again with "
                  f"sort={sort}", flush=True)
            again, shown2 = walk_category(pool, cid, sort)
            found.update({k: v for k, v in again.items() if k not in found})
            shown = min(shown, shown2)
        print(f"{cid} {cats[cid]}: {len(found)} unique products of {shown}", flush=True)
        if shown - len(found) > slack:
            raise RuntimeError(f"{cid} {cats[cid]}: only {len(found)} unique products of {shown} "
                               "after walking it in two orders; paging is not reliable")
        out.append((cid, found, shown))
    return out


def product_page_price(url):
    """The product page's itemprop="price" (USD) or its "Price: $..." text."""
    r = get(url)
    html = decode(r)
    soup = BeautifulSoup(html, "html.parser")
    cur = soup.find(attrs={"itemprop": "priceCurrency"})
    if cur and cur.get("content") and cur["content"] != "USD":
        raise RuntimeError(f"{url}: product page currency {cur['content']}")
    meta = soup.find(attrs={"itemprop": "price"})
    if meta and meta.get("content"):
        return float(meta["content"].replace(",", "")) or None
    m = re.search(r"Price:\s*\$\s*([\d,]+\.\d{2})", clean(soup.get_text(" ")))
    return float(m.group(1).replace(",", "")) if m else None


def price_check(rows):
    """Test runs: the listing price of the first products must equal their product page's."""
    checked = 0
    for r in rows:
        if r["price"] is None:
            continue
        page_price = product_page_price(r["url"])
        time.sleep(DELAY)
        print(f"price check {r['url']}: listing {r['price']}, product page {page_price}",
              flush=True)
        if page_price != r["price"]:
            raise RuntimeError(f"{r['url']}: listing shows {r['price']} but the product page "
                               f"shows {page_price}")
        checked += 1
        if checked >= PRICE_CHECKS:
            break


def stock(text):
    """(available, stock_qty) from "245 In Stock" / other texts."""
    m = STOCK_RE.match(text)
    if m:
        qty = int(m.group(1).replace(",", ""))
        words = m.group(2).lower()
        if "out of stock" in words or "call" in words:
            return False, 0 if qty == 0 else None
        return qty > 0, qty
    low = text.lower()
    return ("in stock" in low and "out of stock" not in low), None


def brand_of(r):
    brand = r["info"].get("Manufacturer", "")
    if brand:
        return brand
    words = r["title"].split()
    if len(words) > 1 and r["part"] and words[0].lower() == r["part"].lower():
        return words[1]
    return ""


def build_row(r, cid, cats, scraped_at):
    available, qty = stock(r["stock_text"])
    info = r["info"]
    cap = CAPACITY_RE.search(r["title"]) if cid in CAPACITY_CATEGORIES else None
    tb = None
    if cap and cid in DRIVE_CATEGORIES:
        tb = float(cap.group(1)) if cap.group(2).upper() == "TB" else float(cap.group(1)) / 1000
    price = r["price"]
    specs = [f"Pack: {r['pack']}" if r["pack"] > 1 else ""]
    specs += [f"{k}: {info[k]}" for k in ("Generation", "Warranty", "Ships", "Server", "Model")
              if info.get(k)]
    specs += ["Special price" if r["special"] else ""]
    compare = r["compare"] if r["compare"] and price and r["compare"] > price else None
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": r["id"],
        "sku": r["part"],
        "part_number": r["part"],
        "title": r["title"],
        "brand": brand_of(r),
        "category": cats[cid],
        "condition": info.get("Condition", ""),
        "capacity": f"{cap.group(1)}{cap.group(2).upper()}" if cap else "",
        "interface": info.get("Generation", "") if cid in DRIVE_CATEGORIES else "",
        "other_specs": " | ".join(s for s in specs if s),
        "price": price,
        "compare_at_price": compare,
        "discount_pct": round((compare - price) / compare * 100, 1) if compare else None,
        "price_per_tb": round(price / (tb * r["pack"]), 2) if price and tb else None,
        "available": available,
        "stock_qty": qty,
        "image_url": r["image"] or None,     # the listing's 60x60 thumbnail
        "description": None,                 # listings carry only the name; no extra requests
        "url": r["url"],
    })


def scrape(limit, scraped_at):
    cats = categories()
    with ThreadPoolExecutor(WORKERS) as pool:
        walked = walk(pool, cats, limit)
    by_id, cat_of, overlap = {}, {}, 0
    for cid, found, _ in walked:
        for pid, r in found.items():
            if pid in by_id:
                overlap += 1
                continue
            by_id[pid] = r
            cat_of[pid] = cid
    listed = sum(shown for _, _, shown in walked) - overlap
    texts, no_price, multi = {}, {}, 0
    for r in by_id.values():
        key = re.sub(r"\d[\d,]*", "N", r["stock_text"])
        texts[key] = texts.get(key, 0) + 1
        if r["price"] is None:
            no_price[r["price_text"]] = no_price.get(r["price_text"], 0) + 1
        multi += r["amounts"] > 1
    print(f"{len(by_id)} unique products ({overlap} in more than one category); stock texts "
          f"{texts}; rows without a $ price {no_price or 'none'}; rows with 2+ $ amounts {multi}",
          flush=True)
    if multi > max(5, len(by_id) // 20):
        raise RuntimeError(f"{multi} rows show more than one $ amount; the price cell changed")
    for r in list(by_id.values())[:3]:
        print(f"sample: {r}", flush=True)
    items = list(by_id.values())
    if limit:
        items = items[:limit]
        price_check(items)
        listed = None
    return [build_row(r, cat_of[r["id"]], cats, scraped_at) for r in items], listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
