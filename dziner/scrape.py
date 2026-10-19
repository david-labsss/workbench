"""Scrape d-ziner.com (WooCommerce Store API, ~1.5k products, mostly HPE Aruba / Cisco
networking): every run's full snapshot appended to Postgres.

Price (USD, in cents), stock count ("3 in stock"), brand and categories come from the Store API
via common/woocommerce.py. The store keeps the rest in each product's own attributes (the
"attributes" list of the same Store API response): "Condition" (e.g. New) -> condition, "Brand"
-> brand when the brands taxonomy is empty, "SKU" (the manufacturer part number, same as the
product SKU) -> sku / part_number; any other attribute (e.g. GTIN) goes to other_specs.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
from common import runner, schema, woocommerce as woo

SITE = "dziner"
BASE = "https://www.d-ziner.com"
MAPPED_ATTRS = {"sku", "condition", "brand"}
EMPTY_VALUES = {"not available", "n/a", "na", "none", "-"}


def attributes(product):
    """{attribute name: "value, value"} from the Store API product's attributes list."""
    out = {}
    for a in product.get("attributes") or []:
        name = woo.clean(a.get("name"))
        values = [woo.clean(t.get("name")) for t in a.get("terms") or []]
        values = [v for v in values if v and v.lower() not in EMPTY_VALUES]
        if name and values:
            out.setdefault(name, ", ".join(values))
    return out


def build_row(product, scraped_at):
    attrs = attributes(product)
    by_key = {k.lower(): v for k, v in attrs.items()}
    row = schema.as_dict(woo.product_row(SITE, product, scraped_at, by_key.get("condition", "")))
    row["sku"] = row["sku"] or by_key.get("sku", "")
    row["part_number"] = row["sku"]
    row["brand"] = row["brand"] or by_key.get("brand", "")
    row["other_specs"] = "; ".join(f"{k}: {v}" for k, v in attrs.items()
                                   if k.lower() not in MAPPED_ATTRS)
    return schema.product_row(row)


def scrape(limit, scraped_at):
    products, listed = woo.fetch_products(BASE, limit)
    others = sum(1 for p in products if p.get("type") != "simple")
    if others:
        print(f"note: {others} products are not 'simple' (variable/grouped): one row each, "
              "at the product's listed price", flush=True)
    rows = [build_row(p, scraped_at) for p in products]
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
