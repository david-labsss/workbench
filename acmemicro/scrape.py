"""Scrape acmemicro.com (now www.8anet.com, a custom ASP.NET WebForms store, ~3k products):
every run's full snapshot appended to Postgres.

acmemicro.com 301-redirects to www.8anet.com. There is no sitemap or API, so the category tree is
crawled: the top menu gives the root categories (/listprod.aspx?parent=<id>), every category page
lists its own sub-categories (followed recursively, deduped by id) and the products filed directly
in it. A category with more than 40 products is paged with the page's own ASP.NET pager, which is
a form post back to the same URL (__EVENTTARGET = the pager's ">" link, plus the page's hidden
fields), exactly as a browser does it. A failed page raises; a pager that doesn't move to the next
page, or a non-last page with fewer rows than the first, also raises, so a broken crawl never
looks like the end of a list. robots.txt only disallows /Account/ and /Cart/.

Each product row gives: product id (/Product/<id>/<slug>, used as product_id), title, price
(USD, some are per "Each" unit and some are service / warranty items), the store's product code
(sku) and an Add to Cart button. The store shows no stock counts. available = True when the row
has an Add to Cart button and is not flagged "Back Order" or "Preorder" (and the title doesn't
say "(OUT OF STOCK)"); "Special Order" items stay available. Those flags (plus "On Sale") are
kept in other_specs.

Products are listed in several places (the server tree has "List By brand / chassis / CPU /
HDD bays" sub-trees). category is the path of the place with the fewest of those "List By"
levels (first in crawl order on a tie): the root's short menu name, then the sub-categories,
leaving out the "List By ..." levels.

brand and part_number are read from the title where clear: brand = a known brand name among the
first words ("A+ Server" / SuperServer / SuperStorage = Supermicro), part_number = an Intel /
AMD ordering code anywhere in the title (CD8068904570201, 100-000000339), else the first
part-number-shaped word after the brand (never words after "for ..."). condition only when the
title says it (e.g. "(OPEN BOX)"). capacity: first upper-case GB/TB size in the title, for the
Memory and Hard Drive / SSD categories only.

The site prints no product count, so listed is None (no completeness ratio check); the page checks
above guard against partial crawls instead.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "acmemicro"
BASE = "https://www.8anet.com"
WORKERS = 2              # small custom site: at most 2 requests at a time
CHUNK = 4                # categories handed to the workers per round
DELAY = 0.4              # polite pause after every request (seconds)
MAX_PAGES = 200          # a category with more pages than this means the pager is broken
MAX_CATEGORIES = 3000    # same for the category tree

PID_RE = re.compile(r"^/Product/(\d+)/", re.I)
CAT_RE = re.compile(r"listprod\.aspx\?parent=(\d+)$", re.I)
PRICE_RE = re.compile(r"\$\s*([\d,]+(?:\.\d+)?)")
POSTBACK_RE = re.compile(r"__doPostBack\('([^']+)'")
FACET_RE = re.compile(r"^\s*\*|\blist\s*by\b", re.I)
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b")   # upper-case only: "8Gb" is a link speed
STORAGE_ROOT_RE = re.compile(r"memory|hard\s*(disk|drive)|\bssd\b", re.I)   # root menu names
CONDITIONS = [
    (re.compile(r"\b(refurb(ished)?|renewed)\b", re.I), "Refurbished"),
    (re.compile(r"\brecertified\b", re.I), "Recertified"),
    (re.compile(r"\bopen[\s-]+box\b", re.I), "Open Box"),
    (re.compile(r"\b(used|pre-?owned)\b", re.I), "Used"),
    (re.compile(r"\bbrand[\s-]+new\b|\(new\)", re.I), "New"),
]
OUT_OF_STOCK_RE = re.compile(r"\bout\s+of\s+stock\b", re.I)
FLAGS = {"imgBackOrder": "Back Order", "lblPreorder": "Preorder",
         "lblSpecialOrder": "Special Order", "imgOnSale": "On Sale"}
NOT_AVAILABLE_FLAGS = {"Back Order", "Preorder"}

BRANDS = [
    "Supermicro", "Intel", "AMD", "Mellanox", "NVIDIA", "Tyan", "Acme", "ASUS", "ASRock",
    "Gigabyte", "MSI", "Adaptec", "Microchip", "Microsemi", "Broadcom", "LSI", "Avago", "Areca",
    "HighPoint", "3ware", "ATTO", "QLogic", "Emulex", "Chelsio", "Marvell", "Samsung", "Micron",
    "Crucial", "Kingston", "Hynix", "SK Hynix", "PNY", "Innodisk", "Kioxia", "Toshiba", "Seagate",
    "Western Digital", "WD", "HGST", "Hitachi", "SanDisk", "Solidigm", "Transcend", "Microsoft",
    "VMware", "Red Hat", "APC", "Eaton", "Tripp Lite", "CyberPower", "Logitech", "D-Link",
    "Netgear", "TP-Link", "Cisco", "Juniper", "Arista", "Aruba", "Ubiquiti", "LG", "ViewSonic",
    "EVGA", "Zotac", "Chenbro", "SilverStone", "iStarUSA", "Norco", "Antec", "Thermaltake",
    "Cooler Master", "Noctua", "Dynatron", "Zippy", "Delta", "Seasonic", "Corsair", "QNAP",
    "Synology", "NEC", "Lite-On", "Plextor", "Aten", "Avocent", "Raritan", "StarTech", "Dell",
    "HP", "HPE", "Lenovo", "IBM", "Matrox", "Black Box", "Belkin",
]
BRAND_ALIASES = {
    "a+": "Supermicro", "superserver": "Supermicro", "superstorage": "Supermicro",
    "superworkstation": "Supermicro", "superworksta": "Supermicro", "superblade": "Supermicro",
    "superchassis": "Supermicro", "acmeserver": "Acme", "acmemicro": "Acme",
    "wdhgst": "Western Digital", "wd": "Western Digital",
}

# Part numbers: Intel / AMD CPU ordering codes, then words shaped like part numbers that are not
# specs (480GB, 10K, 2U, 1200W, 16Cores, 2x8GB, PC4-19200R, DDR4, G14, 12Gb/s, 128-Channel ...)
CPU_CODE_RE = re.compile(r"(?:CD|CM|BXC?)8\d{5,}[A-Z0-9]*|100-\d{9}[A-Z0-9]*|OS\d{4}[A-Z0-9]{5,}")
UNITS = ("TB|GB|MB|KB|TIB|GIB|W|KW|V|VAC|VDC|A|MA|MAH|AH|VA|KVA|K|M|U|G|X|P|E|T|C|S|R|L|D|HZ|"
         "KHZ|MHZ|GHZ|RPM|FT|MM|CM|IN|NM|GBE|GBPS|MBPS|GBIT|MT|GT|PIN|PORTS?|PCS|PK|PACK|TH|ST|ND|"
         "RD|BIT|CORES?|THREADS?|AWG|DWPD|LBS?|OZ|IOPS|LFF|SFF|NITS?|CFM|GBASE|WAY|NODES?|BAYS?")
SPEC_RE = re.compile(r"\d+(?:\.\d+)?(?i:%s)?|\d{1,3}[xX]\d{1,3}(?:\.\d+)?(?:[xX]\d+)*[A-Za-z]{0,4}"
                     r"|\d[Rr][xX]\d+|(?i:PC|DDR|CAT|GEN|PCIE|USB|LGA|SAS|SATA)\d.*|[Gg]\d{1,2}"
                     r"|\d+-[A-Za-z][a-z]+|\d+(?:\.\d+)?[A-Za-z]*/[sS]|\d+[Kk]\.\d+" % UNITS)
NAME_RE = re.compile(r"[A-Z][a-z]+[A-Za-z]*-\d{1,2}")    # family names: ConnectX-6, Spectrum-4
PN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9\-./#+]*[A-Za-z0-9#+]")
FOR_RE = re.compile(r"\s(?:for|fits|compatible\s+with)\s", re.I)
STRIP = "()[]{},;:|\"'*®™�"


# --- fetching ----------------------------------------------------------------
def get(url):
    r = http.session().get(url, timeout=90)
    r.raise_for_status()
    time.sleep(DELAY)
    return r.text


def post(url, data):
    r = http.session().post(url, data=data, timeout=90)
    r.raise_for_status()
    time.sleep(DELAY)
    return r.text


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def menu_roots():
    """{root category id: short menu name} from the site's top menu."""
    soup = BeautifulSoup(get(f"{BASE}/index.aspx"), "html.parser")
    roots = {}
    for a in soup.select('a[href^="/listprod.aspx?parent="]'):
        m = CAT_RE.search(a["href"])
        if m and clean(a.get_text()):
            roots.setdefault(m.group(1), clean(a.get_text()))
    if not roots:
        raise RuntimeError("no categories found in the site menu")
    return roots


def tree_rows(soup):
    """[(category id, name, indent)] of the category tree box: the path from the root down to
    this category, then its sub-categories (deeper indent)."""
    rows = []
    for a in soup.select("span.bold_content > a[href]"):
        m = CAT_RE.search(a["href"].lstrip("./"))
        if not m:
            continue
        img = a.find_parent("td").find("img", width=True) if a.find_parent("td") else None
        try:
            indent = int(img["width"]) if img else 0
        except ValueError:
            indent = 0
        rows.append((m.group(1), clean(a.get_text(" ")), indent))
    return rows


def product_rows(soup):
    """Product dicts of one listing page (in page order, deduped by id)."""
    found, seen = [], set()
    for a in soup.select('a[href^="/Product/"]'):
        m = PID_RE.match(a["href"])
        td = a.find_parent("td")
        tr = td.find_parent("tr") if td else None
        tds = tr.find_all("td", recursive=False) if tr else []
        if not m or len(tds) < 4 or td is not tds[0] or m.group(1) in seen:
            continue
        seen.add(m.group(1))
        ids = " ".join(x.get("id", "") for x in tr.select("[id]"))
        flags = [label for key, label in FLAGS.items() if key in ids]
        price = PRICE_RE.search(tds[1].get_text(" "))
        found.append({
            "id": m.group(1),
            "title": clean(a.get_text(" ")),
            "url": BASE + a["href"],
            "price": float(price.group(1).replace(",", "")) if price else None,
            "sku": clean(tds[2].get_text(" ")),
            "cart": bool(tr.select_one('input[name$="ibAddToCart"]')),
            "flags": flags,
        })
    return found


def pager(soup):
    """(current page number, __EVENTTARGET of the enabled ">" link or None)."""
    current, target = 1, None
    for a in soup.select('a[id*="pager_rpPages"]'):
        text = clean(a.get_text())
        classes = a.get("class") or []
        if text.isdigit() and "aspNetDisabled" in classes:
            current = int(text)
        elif text == ">" and a.get("href"):
            m = POSTBACK_RE.search(a["href"].replace("&#39;", "'"))
            target = target or (m.group(1) if m else None)
    return current, target


def form_fields(soup, target):
    """The page form's fields as a browser would post them for a pager click."""
    form = soup.find("form")
    data = {}
    for i in form.find_all("input"):
        name, kind = i.get("name"), (i.get("type") or "text").lower()
        if not name or kind in ("image", "submit", "button", "file"):
            continue
        if kind in ("checkbox", "radio") and not i.has_attr("checked"):
            continue
        data[name] = i.get("value", "")
    for sel in form.find_all("select"):
        opt = sel.find("option", selected=True) or sel.find("option")
        if sel.get("name"):
            data[sel["name"]] = opt.get("value", "") if opt else ""
    data.update({"__EVENTTARGET": target, "__EVENTARGUMENT": ""})
    return urljoin(BASE + "/", form.get("action") or ""), data


def crawl_category(cid):
    """Everything on a category: its path, sub-category ids and all its products (all pages)."""
    url = f"{BASE}/listprod.aspx?parent={cid}"
    soup = BeautifulSoup(get(url), "html.parser")
    rows = tree_rows(soup)
    ids = [r[0] for r in rows]
    if cid in ids:
        at = ids.index(cid)
        path = [(r[0], r[1]) for r in rows[:at + 1]]
        children = [r[0] for r in rows[at + 1:] if r[2] > rows[at][2]]
    else:
        title = clean(soup.title.get_text()) if soup.title else ""
        path = [(cid, re.sub(r"^8ANET\s*-\s*", "", title))]
        children = []
    # any other category link on the page (safety net; the menu's roots are already known)
    children += [m.group(1) for a in soup.select("a[href]")
                 for m in [CAT_RE.search(a["href"].lstrip("./"))] if m]
    products = product_rows(soup)
    sizes = [len(products)]
    page, target = pager(soup)
    while target:
        if page >= MAX_PAGES:
            raise RuntimeError(f"category {cid}: more than {MAX_PAGES} pages")
        post_url, data = form_fields(soup, target)
        soup = BeautifulSoup(post(post_url, data), "html.parser")
        now, target = pager(soup)
        if now != page + 1:
            raise RuntimeError(f"category {cid}: pager went from page {page} to {now}")
        page = now
        found = product_rows(soup)
        if not found:
            raise RuntimeError(f"category {cid}: page {page} has no products")
        products += found
        sizes.append(len(found))
    if len(sizes) > 1 and any(s != sizes[0] for s in sizes[:-1]):
        raise RuntimeError(f"category {cid}: short page before the last one {sizes}")
    return {"id": cid, "path": path, "children": children, "products": products,
            "pages": len(sizes)}


def crawl(limit=0):
    """Breadth-first over the whole category tree (a queue, CHUNK categories at a time, WORKERS
    in parallel; the order is the same every run). Returns (roots, [results in crawl order])."""
    roots = menu_roots()
    seen, queue, results = set(roots), list(roots), []
    products = set()
    with ThreadPoolExecutor(WORKERS) as pool:
        while queue:
            chunk, queue = queue[:CHUNK], queue[CHUNK:]
            for res in pool.map(crawl_category, chunk):   # any failed page raises here
                results.append(res)
                products.update(p["id"] for p in res["products"])
                for c in res["children"]:
                    if c not in seen:
                        seen.add(c)
                        queue.append(c)
            if len(results) % 40 < CHUNK or not queue:
                print(f"{len(results)} categories crawled, {len(queue)} to go, "
                      f"{len(products)} products", flush=True)
            if len(seen) > MAX_CATEGORIES:
                raise RuntimeError(f"more than {MAX_CATEGORIES} categories; tree loop?")
            if limit and len(products) >= limit:
                break
    return roots, results


# --- mapping -----------------------------------------------------------------
def alnum(text):
    return re.sub(r"[^a-z0-9+]", "", (text or "").lower())


BRAND_KEYS = {alnum(b): b for b in BRANDS}
BRAND_KEYS.update(BRAND_ALIASES)


def tokens_of(title):
    title = OUT_OF_STOCK_RE.sub(" ", re.sub(r"\(\s*out\s+of\s+stock\s*\)", " ", title, flags=re.I))
    head = FOR_RE.split(" " + title + " ", 1)[0]       # never words after "for ..."
    return [t for t in (w.strip(STRIP) for w in head.split()) if t]


def brand_at(tokens):
    """(brand, index of its last word) among the first 3 words, else ("", -1)."""
    for i in range(min(3, len(tokens))):
        two = alnum(tokens[i] + (tokens[i + 1] if i + 1 < len(tokens) else ""))
        if i + 1 < len(tokens) and two in BRAND_KEYS and " " in BRAND_KEYS[two]:
            return BRAND_KEYS[two], i + 1
        one = alnum(tokens[i])
        if one in BRAND_KEYS:
            return BRAND_KEYS[one], i
    return "", -1


def pn_like(token):
    return (len(token) >= 4 and re.search(r"\d", token) and PN_RE.fullmatch(token)
            and (re.search(r"[A-Za-z]", token) or "-" in token) and not SPEC_RE.fullmatch(token)
            and not NAME_RE.fullmatch(token) and alnum(token) not in BRAND_KEYS)


def part_number_of(title, tokens, brand_end):
    for t in (w.strip(STRIP) for w in title.split()):
        if CPU_CODE_RE.fullmatch(t):
            return t
    for t in tokens[brand_end + 1:brand_end + 5]:
        if pn_like(t):
            return t
    return ""


def category_of(path, roots):
    names = [name for _, name in path]
    if not names:
        return ""
    root = roots.get(path[0][0], names[0])
    return " > ".join([root] + [n for n in names[1:] if not FACET_RE.search(n)])


def build_row(p, category, scraped_at):
    title = p["title"]
    tokens = tokens_of(title)
    brand, brand_end = brand_at(tokens)
    condition = next((label for rx, label in CONDITIONS if rx.search(title)), "")
    capacity = ""
    if STORAGE_ROOT_RE.search(category.split(" > ")[0]):     # Memory, Hard Drive | SSD roots
        cap = CAPACITY_RE.search(title)
        capacity = (cap.group(1) + cap.group(2)) if cap else ""
    flags = list(p["flags"]) + (["Out of Stock"] if OUT_OF_STOCK_RE.search(title) else [])
    available = p["cart"] and not (set(flags) & (NOT_AVAILABLE_FLAGS | {"Out of Stock"}))
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": p["id"],
        "sku": p["sku"],
        "part_number": part_number_of(title, tokens, brand_end),
        "title": title,
        "brand": brand,
        "category": category,
        "condition": condition,
        "capacity": capacity,
        "other_specs": "; ".join(flags),
        "price": p["price"] if p["price"] else None,     # no / $0 price = None, never 0
        "available": bool(available),
        "stock_qty": None,                                # not shown by the store
        "url": p["url"],
    })


def scrape(limit, scraped_at):
    roots, results = crawl(limit)
    best = {}    # product id -> (number of "List By" levels, crawl order, product, category)
    for order, res in enumerate(results):
        facets = sum(1 for _, name in res["path"] if FACET_RE.search(name))
        category = category_of(res["path"], roots)
        for p in res["products"]:
            key = (facets, order)
            if p["id"] not in best or key < best[p["id"]][:2]:
                best[p["id"]] = (facets, order, p, category)
    pages = sum(r["pages"] for r in results)
    print(f"{len(results)} categories, {pages} listing pages, {len(best)} unique products",
          flush=True)
    rows = [build_row(p, category, scraped_at) for _, _, p, category in best.values()]
    rows = rows[:limit] if limit else rows
    return rows, None    # the site prints no product count


if __name__ == "__main__":
    runner.run(SITE, scrape)
