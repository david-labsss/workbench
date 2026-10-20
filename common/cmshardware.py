"""Shared scraper for stores on the "cmshardware" e-commerce backend (harddiskdirect,
memoryclearance, ...).

These stores are Angular apps whose data comes from an open JSON backend, one per store group,
addressed as <api host>/<website id> (e.g. backend.cmshardware.com/5, api.memoryclearance.com/1),
with no Cloudflare challenge even when the store's www is behind one. Its catalogue search
(POST <api>/catalogue-search/products) returns 1,000 products per call with SKU (= the part
number), name, brand, category, condition, price, in stock, stock quantity (when the store
tracks it) and specs ("string_facet").

One query can't page past 10,000 products, and its "total" stops at 10,000 too. So:
  - every category of the store's category tree (<api>/category/tree) is counted on its own
    (a category filter only matches products in that exact category, not its children). It
    takes several ids and matches any of them, so smaller categories are listed several at a
    time, up to 10,000 products a listing;
  - a category with 10,000+ products is split: its products without a price apart first (often
    most of them), then price ranges halved until each is under 10,000, and a single price with
    10,000+ products by brand (brands again several to a listing). Big categories are split
    side by side.
Products are deduped by id; the planned total is the sum of the listings' exact totals.

Price 0 means the store shows no price ("request a quote"): saved as empty, not 0.
"""
import re
import time
from concurrent.futures import ThreadPoolExecutor

from common import http, schema

PAGE = 1000
CAP = 10000                  # one query can't page past this, and its total stops here
MAX_PRICE = 9999999999
NO_PRICE = 0.01              # [0, NO_PRICE]: the products without a price
WORKERS = 8                  # API calls at once

# Spec ("string_facet") name -> column
FACET_COLUMNS = {
    "storage capacity": "capacity", "memory capacity": "capacity", "capacity": "capacity",
    "interface standard": "interface", "interface": "interface",
    "form factor": "form_factor",
    "spindle speed": "spindle_speed", "rotational speed": "spindle_speed", "rpm": "spindle_speed",
    "cache": "cache", "cache size": "cache", "buffer size": "cache",
    "product series": "series", "product line": "series", "series": "series",
    "model": "model_number", "model number": "model_number",
}
# Stores without specs: capacity is read from the title, for drives and memory only
STORAGE_CATEGORY_RE = re.compile(r"drive|ssd|hdd|storage|memory|flash", re.I)
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b", re.I)


def search_body(f, page=1, limit=1):
    return {"page": page, "limit": limit, "keyword": "",
            "filters": {"attributes": [], "categories": f.get("categories", []),
                        "brands": f.get("brands", []), "minPrice": f.get("min", 0),
                        "maxPrice": f.get("max", MAX_PRICE)}}


def clean(v):
    return re.sub(r"\s+", " ", str(v or "")).strip()


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def pack(sized):
    """Group (id, count) pairs into [(ids, total)], each total under CAP, biggest first; an id
    with CAP or more stays on its own."""
    groups = []
    for i, n in sorted(sized, key=lambda x: -x[1]):
        for g in groups:
            if g[1] + n < CAP:
                g[0].append(i)
                g[1] += n
                break
        else:
            groups.append([[i], n])
    return [(ids, n) for ids, n in groups]


class Store:
    def __init__(self, site, base, api):
        self.site, self.base, self.api = site, base, api

    def post(self, path, body):
        for attempt in range(5):
            r = http.session().post(f"{self.api}/{path}", json=body, timeout=180)
            r.raise_for_status()
            data = r.json()
            if data.get("status") == "success":
                return data["data"]
            if attempt < 4:
                time.sleep(5 * (attempt + 1))
        raise RuntimeError(f"{path}: {data.get('msg')}")

    def total(self, f):
        return self.post("catalogue-search/products", search_body(f))["total"]

    def categories(self):
        """Every category id in the store's category tree."""
        r = http.session().get(f"{self.api}/category/tree", timeout=60)
        r.raise_for_status()
        ids = []

        def walk(node):
            ids.append(node["id"])
            for child in node.get("childrens") or []:
                walk(child)
        for node in r.json()["data"]:
            walk(node)
        return ids

    def split(self, f, n):
        """[(filters, exact total)] listings under CAP that cover filters f (n = its total)."""
        if n < CAP:
            return [(f, n)] if n else []
        lo, hi = f.get("min", 0), f.get("max", MAX_PRICE)
        if lo == 0 and hi > NO_PRICE:
            # The products without a price first, rather than halving the range down to 0
            parts = [{**f, "max": NO_PRICE}, {**f, "min": NO_PRICE}]
        else:
            if hi == MAX_PRICE:
                hi = self.post("catalogue-search/products/filters",
                               search_body(f))["maxPrice"] or 0
            if hi - lo < 0.02:
                return self.split_by_brand(f)
            # Two halves that share their middle price; the overlap is removed by product id
            mid = round((lo + hi) / 2, 2)
            parts = [{**f, "min": lo, "max": mid}, {**f, "min": mid, "max": hi}]
        listings = []
        for part in parts:
            listings += self.split(part, self.total(part))
        return listings

    def split_by_brand(self, f):
        """One price with CAP+ products: by brand (the store returns up to 100 brands), brands
        packed several to a listing (a brand filter matches any of its ids)."""
        sized = []
        for b in self.post("catalogue-search/products/filters", search_body(f))["brands"]:
            t = self.total({**f, "brands": [b["id"]]})
            if t >= CAP:
                print(f"WARN {f} brand {b['id']} has {t}+ products; only {CAP} are reachable",
                      flush=True)
            if t:
                sized.append((b["id"], min(t, CAP)))
        return [({**f, "brands": ids}, n) for ids, n in pack(sized)]

    def plan(self):
        cats = self.categories()
        with ThreadPoolExecutor(WORKERS) as pool:
            totals = dict(zip(cats, pool.map(lambda c: self.total({"categories": [c]}), cats)))
            big = [c for c in cats if totals[c] >= CAP]
            splits = list(pool.map(lambda c: self.split({"categories": [c]}, totals[c]), big))
        listings = [({"categories": ids}, n)
                    for ids, n in pack([(c, n) for c, n in totals.items() if 0 < n < CAP])]
        for c, part in zip(big, splits):
            print(f"category {c}: split into {len(part)} listings, "
                  f"{sum(t for _, t in part)} products", flush=True)
            listings += part
        return listings

    def fetch_listing(self, listing):
        f, n = listing
        products = []
        for page in range(1, -(-min(n, CAP) // PAGE) + 1):
            products += self.post("catalogue-search/products",
                                  search_body(f, page, PAGE))["products"]
        return products

    def build_row(self, p, scraped_at):
        specs, other = {}, []
        for facet in p.get("string_facet") or []:
            name, value = clean(facet.get("facet_name")), clean(facet.get("facet_value"))
            if not name or not value:
                continue
            column = FACET_COLUMNS.get(name.lower())
            if column:
                specs.setdefault(column, value)
            else:
                other.append(f"{name}: {value}")
        if clean(p.get("same_as_part")):
            other.append(f"Same as part: {clean(p['same_as_part'])}")
        # Some stores send brand_name / category_name, others only the brand / category objects
        brand = p.get("brand_name") or (p.get("brand") or {}).get("name")
        category = p.get("category_name") or (p.get("category") or {}).get("name")
        capacity = specs.get("capacity", "")
        if not capacity and STORAGE_CATEGORY_RE.search(clean(category)):
            m = CAPACITY_RE.search(clean(p.get("name")))
            capacity = m.group(1) + m.group(2).upper() if m else ""
        price = to_float(p.get("price"))
        images = p.get("assignedImages") or []
        image_url = (images[0].get("image_url") or images[0].get("image")) if images else None
        description = schema.plain_text(p.get("description") or p.get("short_description"))
        return schema.product_row({
            "scraped_at": scraped_at,
            "website": self.site,
            "product_id": str(p["id"]),
            "sku": clean(p.get("sku")),
            "part_number": clean(p.get("sku")),      # the store's SKU is the manufacturer part no.
            "model_number": specs.get("model_number", ""),
            "title": clean(p.get("name")),
            "brand": clean(brand),
            "category": clean(category),
            "condition": clean(p.get("product_condtion")),   # sic: the API's spelling
            "series": specs.get("series", ""),
            "capacity": capacity,
            "interface": specs.get("interface", ""),
            "form_factor": specs.get("form_factor", ""),
            "spindle_speed": specs.get("spindle_speed", ""),
            "cache": specs.get("cache", ""),
            "other_specs": "; ".join(other),
            "price": price or None,                  # 0 = no price shown
            "available": bool(p.get("is_in_stock")),
            "stock_qty": to_float(p.get("quantity")),
            "image_url": image_url,
            "description": description,
            "url": f"{self.base}/{p['url']}" if p.get("url") else "",
        })

    def scrape(self, limit, scraped_at):
        """A scrape(limit, scraped_at) function for common/runner.py."""
        if limit:
            listings = [({}, min(limit, CAP))]
            planned = None
        else:
            listings = self.plan()
            planned = sum(t for _, t in listings)
            print(f"{len(listings)} listings, {planned} products", flush=True)
        by_id = {}
        with ThreadPoolExecutor(WORKERS) as pool:
            for i, products in enumerate(pool.map(self.fetch_listing, listings), 1):
                for p in products:
                    by_id[p["id"]] = p
                if i % 20 == 0 or i == len(listings):
                    print(f"{i}/{len(listings)} listings, {len(by_id)} unique products",
                          flush=True)
        items = list(by_id.values())[:limit] if limit else list(by_id.values())
        # planned counts products on a price boundary twice, so it can be slightly above the truth
        return [self.build_row(p, scraped_at) for p in items], planned


def scraper(site, base, api):
    return Store(site, base, api).scrape
