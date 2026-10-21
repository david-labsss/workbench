"""Scrape insidesystems.com (WooCommerce Store API, ~35k products, EMC / Dell / HP / IBM
refurbished storage and server parts): every run's full snapshot appended to Postgres.

Prices are in DKK (Danish krone), delivered by the Store API in minor units (øre) and converted
by common/woocommerce.py. The store's YayCurrency plugin otherwise auto-switches currency by the
visitor's country (a US runner would get USD at that day's FX rate, so prices would wobble by a
dollar every run), so we pin DKK on every request with the `yay-currency=DKK` query param. The
product SKU is the manufacturer part number, so it fills
part_number too. The brands taxonomy is empty; a "Brand"/"Condition" product attribute (when a
product has one) is read from the Store API attributes list, and any other attribute goes to
other_specs. Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
from common import runner, schema, woocommerce as woo

SITE = "insidesystems"
BASE = "https://insidesystems.com"
MAPPED_ATTRS = {"brand", "condition"}
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
    row["part_number"] = row["sku"]    # the SKU is the manufacturer part number here
    row["other_specs"] = "; ".join(f"{k}: {v}" for k, v in attrs.items()
                                   if k.lower() not in MAPPED_ATTRS)
    return schema.product_row(row)


def scrape(limit, scraped_at):
    products, listed = woo.fetch_products(BASE, limit, params={"yay-currency": "DKK"})
    rows = [build_row(p, scraped_at) for p in products]
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
