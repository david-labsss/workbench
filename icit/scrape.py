"""Scrape icit.net.au (WooCommerce Store API, ~3.8k products, Lenovo / HPE / networking and
IT hardware): every run's full snapshot appended to Postgres.

Prices are in AUD, delivered by the Store API in minor units (cents) and converted by
common/woocommerce.py. The brands taxonomy is empty; this store keeps the useful fields in each
product's attributes instead: "Brand" -> brand, "Condition" -> condition, "Manufacturer Part#"
-> part_number (falling back to the SKU). Any other attribute goes to other_specs. Run steps,
changes and env vars: common/runner.py. Columns: common/schema.py.
"""
from common import runner, schema, woocommerce as woo

SITE = "icit"
BASE = "https://icit.net.au"
PART_KEYS = ("manufacturer part#", "manufacturer part #", "mpn", "part#", "part number")
MAPPED_ATTRS = {"brand", "condition", *PART_KEYS}
EMPTY_VALUES = {"not available", "n/a", "na", "none", "-", ""}


def attributes(product):
    """{attribute name: "value, value"} from the Store API product's attributes list."""
    out = {}
    for a in product.get("attributes") or []:
        name = woo.clean(a.get("name")).rstrip(":").strip()
        values = [woo.clean(t.get("name")) for t in a.get("terms") or []]
        values = [v for v in values if v and v.lower() not in EMPTY_VALUES]
        if name and values:
            out.setdefault(name, ", ".join(values))
    return out


def build_row(product, scraped_at):
    attrs = attributes(product)
    by_key = {k.lower(): v for k, v in attrs.items()}
    row = schema.as_dict(woo.product_row(SITE, product, scraped_at, by_key.get("condition", "")))
    row["brand"] = row["brand"] or by_key.get("brand", "")
    part = next((by_key[k] for k in PART_KEYS if by_key.get(k)), "")
    row["part_number"] = part or row["sku"]
    row["other_specs"] = "; ".join(f"{k}: {v}" for k, v in attrs.items()
                                   if k.lower() not in MAPPED_ATTRS)
    return schema.product_row(row)


def scrape(limit, scraped_at):
    products, listed = woo.fetch_products(BASE, limit)
    rows = [build_row(p, scraped_at) for p in products]
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
