"""Scrape serverbasket.net (WooCommerce Store API, ~750 products): every run's full snapshot
appended to Postgres.

Note: the seller is a Dubai (UAE) company, not a US store; its global site prices in USD.

Price, stock, brand and categories come from the Store API via common/woocommerce.py. This
store prices in whole dollars (currency_minor_unit 0), which the shared helper handles per
product. "External" products (a "Buy Now" button instead of add-to-cart) still carry a price
and are kept like any other product; items without a price ("Read more", quote-only) get an
empty price. The store leaves every SKU empty, so product_id (the WooCommerce id) is the key
and sku / part_number stay blank.

Specs are product attributes in the same Store API response: "Brand" fills brand when the
brands taxonomy is empty; form factor, interface, rotational speed, cache and series map to
their columns (ATTR_COLUMNS); the rest go to other_specs. The store has no condition field, so
condition is read from the title ("Refurbished ...") or the categories ("Refurbished Servers"
-> Refurbished, "Brand New SAS HDD" / "New Servers" -> New), else left blank. For drives and
memory (by category) capacity is read from the title (e.g. "12TB", "32GB"), and drives also
get price_per_tb.

robots.txt disallows /page/, filter_, orderby and add-to-cart URLs; the /wp-json Store API
used here is not disallowed.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import re

from common import runner, schema, woocommerce as woo

SITE = "serverbasket"
BASE = "https://www.serverbasket.net"

# Attribute name (lowercase) -> column; first one found wins
ATTR_COLUMNS = {
    "form factor": "form_factor", "from factor": "form_factor",
    "interface": "interface", "drive interface": "interface", "system interface": "interface",
    "rotational speed": "spindle_speed",
    "cache": "cache",
    "series": "series", "product series": "series", "storage family": "series",
}
SKIP_ATTRS = {"brand"}
EMPTY_VALUES = {"not available", "n/a", "na", "none", "-"}
DRIVE_RE = re.compile(r"\b(hdd|ssd|ssds|hard drives?|solid state)\b", re.I)
MEMORY_RE = re.compile(r"\b(memory|ram|ram's|rams|ddr\d)\b", re.I)
CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s?(TB|GB)\b")          # upper-case units only
REFURB_RE = re.compile(r"\b(refurbished|renewed|recertified|pre-owned)\b", re.I)
NEW_CATEGORY_RE = re.compile(r"\b(brand new|new servers?)\b", re.I)


def attributes(product):
    """[(attribute name, "value, value")] from the Store API product's attributes list."""
    out = []
    for a in product.get("attributes") or []:
        name = woo.clean(a.get("name"))
        values = [woo.clean(t.get("name")) for t in a.get("terms") or []]
        values = [v for v in values if v and v.lower() not in EMPTY_VALUES]
        if name and values:
            out.append((name, ", ".join(values)))
    return out


def condition_of(title, categories):
    if REFURB_RE.search(title) or any(REFURB_RE.search(c) for c in categories):
        return "Refurbished"
    if any(NEW_CATEGORY_RE.search(c) for c in categories):
        return "New"
    return ""


def build_row(product, scraped_at):
    categories = [woo.clean(c.get("name")) for c in product.get("categories") or []]
    title = woo.clean(product.get("name"))
    row = schema.as_dict(woo.product_row(SITE, product, scraped_at,
                                         condition_of(title, categories)))
    other = []
    for name, val in attributes(product):
        key = name.lower()
        column = ATTR_COLUMNS.get(key)
        if key == "brand":
            row["brand"] = row["brand"] or val
        elif column and not row.get(column):
            row[column] = val
        elif key not in SKIP_ATTRS:
            other.append(f"{name}: {val}")
    row["other_specs"] = "; ".join(other)

    is_drive = any(DRIVE_RE.search(c) for c in categories)
    if is_drive or any(MEMORY_RE.search(c) for c in categories):
        m = CAPACITY_RE.search(title)
        if m:
            row["capacity"] = f"{m.group(1)}{m.group(2)}"
            tb = float(m.group(1)) / (1 if m.group(2) == "TB" else 1000)
            price = row["price"] if row["price"] != "" else None
            if is_drive and price and tb:
                row["price_per_tb"] = round(price / tb, 2)
    return schema.product_row(row)


def scrape(limit, scraped_at):
    products, listed = woo.fetch_products(BASE, limit)
    rows = [build_row(p, scraped_at) for p in products]
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
