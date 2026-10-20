"""Scrape topparagonresource.com (WooCommerce Store API, ~1,925 products: Cisco / Aruba / Juniper /
Fortinet networking, HPE / Dell parts): every run's full snapshot appended to Postgres.

The Store API feed (common/woocommerce.py, 100 per page, X-WP-Total = listed) gives the id, name,
price (minor unit 2; "0" = quote-only -> empty price), regular price (old price on sales),
categories and the in-stock flag (no stock count). On top of the shared mapping:
  part_number  the product NAME is the part number (e.g. "JL625A", "N9K-C9232C used",
               "HPE 633542 001"): the first part-number-looking token, after a leading brand
               word; HP's "633542 001" spelling is joined to "633542-001". Upper-cased.
  sku          the store's SKU (almost always empty)
  brand        from a branded category ("Cisco Switches" -> Cisco), else a leading brand word in
               the name ("HPE 572531-B21 ..." -> HPE)
  category     the store categories without the brand ("Cisco Switches" + "Switches" ->
               "Switches"); "New" and "Promotions" are not categories
  condition    the "New" category, or a word in the name: used -> Used, REF -> Refurbished,
               NOB -> New Open Box
  capacity     upper-case GB/TB in the name, for HDD / SSD / RAM / memory items only
  available    None when the store prints no stock text: it doesn't track stock, its pages show
               no stock wording, and is_in_stock is just WooCommerce's default "in stock"
               (checked 2026-10-20). Out of stock (set by hand on a few items) stays False.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re

from common import runner, schema, woocommerce as woo

SITE = "topparagonresource"
BASE = "https://topparagonresource.com"

BRANDS = {
    "cisco": "Cisco", "aruba": "Aruba", "juniper": "Juniper", "fortinet": "Fortinet",
    "huawei": "Huawei", "hpe": "HPE", "hp": "HP", "dell": "Dell", "ibm": "IBM", "lenovo": "Lenovo",
    "intel": "Intel", "amd": "AMD", "logitech": "Logitech", "pny": "PNY", "lsi": "LSI",
    "nvidia": "NVIDIA", "arista": "Arista", "samsung": "Samsung", "seagate": "Seagate",
    "micron": "Micron", "kingston": "Kingston", "avaya": "Avaya", "ubiquiti": "Ubiquiti",
    "meraki": "Meraki", "apc": "APC", "supermicro": "Supermicro", "mikrotik": "MikroTik",
    "poly": "Poly", "yealink": "Yealink", "brocade": "Brocade", "netgear": "Netgear",
}
NOT_CATEGORIES = {"new", "promotions", "uncategorized"}
CONDITION_WORDS = {"used": "Used", "ref": "Refurbished", "refurbished": "Refurbished",
                   "refurb": "Refurbished", "nob": "New Open Box", "new": "New"}
PART_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9\-/.#+_=()]*$")
NOT_PARTS = {"CFP2", "CFP4", "QSFP28", "QSFP56", "SFP28", "QSFP-DD", "OSFP", "DDR3", "DDR4",
             "DDR5", "PCIE3", "PCIE4", "WIFI6", "WIFI7"}       # form factors, not part numbers
UNIT_RE = re.compile(r"^\d{1,4}(?:\.\d+)?(?:GB|TB|MB|W|G|M|V|A|U)$", re.I)  # 40GB, 800W, 1M
HP_SPACED_RE = re.compile(r"\b([A-Za-z]?\d{6}) (\d{3}|[A-Za-z]\d{2})\b")   # "633542 001"
CAPACITY_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(TB|GB)\b")                    # upper case only
CAPACITY_CATEGORIES = {"hdd", "ssd", "ram", "network memory"}


def split_brand(category_names):
    """(brand from category, generic category names) from a product's category names."""
    brand, generic, branded = "", [], []
    for raw in category_names:
        cat = woo.clean(raw)
        if not cat or cat.lower() in NOT_CATEGORIES:
            continue
        first, _, rest = cat.partition(" ")
        if first.lower() in BRANDS and rest:
            brand = brand or BRANDS[first.lower()]
            if rest not in branded:
                branded.append(rest)
        elif cat not in generic:
            generic.append(cat)
    return brand, generic or branded


def part_number(name):
    text = HP_SPACED_RE.sub(r"\1-\2", name.replace(" ,", ","))
    text = re.sub(r"\b([A-Za-z]+-) (\d)", r"\1\2", text)          # "CP- 8865-K9"
    tokens = []
    for t in text.split():
        t = t.strip(",;:*[]")
        if t.count("(") != t.count(")"):
            t = t.strip("()")
        tokens.append(t)
    if tokens and tokens[0].lower() in BRANDS:
        tokens = tokens[1:]
    words = [t for t in tokens if t.lower() not in CONDITION_WORDS and t.upper() != "OEM"]
    for t in tokens:
        first, _, rest = t.partition("-")                         # "Cisco-GLC-LH-SMD"
        if first.lower() in BRANDS and rest:
            t = rest
        if len(t) < 4 or not PART_RE.match(t) or UNIT_RE.match(t) or t.upper() in NOT_PARTS:
            continue
        digits = sum(c.isdigit() for c in t)
        if digits:
            if digits == len(t) and len(t) < 6:                   # "9310": a model, not a part
                continue
            return t.upper()
        # no digits: Cisco-style codes ("GLC-T", "PWR-ADPT") or a lone Intel S-spec ("SRKHR")
        if t.isupper() and ("-" in t or (len(words) == 1 and len(t) >= 5)):
            return t
    return ""


def condition_of(name, category_names):
    if any(woo.clean(c).lower() == "new" for c in category_names):
        return "New"
    words = [w.lower().strip("(),") for w in name.split()[1:]]
    for word in ("ref", "refurbished", "refurb", "nob", "used"):
        if word in words:
            return CONDITION_WORDS[word]
    return ""


def build_row(product, scraped_at):
    names = [c.get("name") for c in product.get("categories") or []]
    row = schema.as_dict(woo.product_row(SITE, product, scraped_at))
    name = row["title"]
    brand, categories = split_brand(names)
    first = name.split()[0].lower() if name.split() else ""
    brand = brand or BRANDS.get(first, "") or BRANDS.get(first.split("-")[0], "")
    capacity = ""
    cap = CAPACITY_RE.search(name)
    if cap and any(c.lower() in CAPACITY_CATEGORIES for c in categories):
        capacity = cap.group(1) + cap.group(2)
    stock_text = woo.clean((product.get("stock_availability") or {}).get("text"))
    row.update(
        part_number=part_number(name),
        brand=brand,
        category=", ".join(categories),
        condition=condition_of(name, names),
        capacity=capacity,
        available=None if product.get("is_in_stock") and not stock_text else
        bool(product.get("is_in_stock")),
    )
    return schema.product_row(row)


def scrape(limit, scraped_at):
    products, listed = woo.fetch_products(BASE, limit)
    rows = [build_row(p, scraped_at) for p in products]
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
