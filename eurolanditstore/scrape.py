"""Scrape eurolanditstore.co.uk (WooCommerce Store API, ~18k products, HPE / Dell / IBM server
spare parts): every run's full snapshot appended to Postgres.

eurolanditstore.co.uk is WooCommerce / WordPress, so the house data path is its public,
unauthenticated WooCommerce Store API (common/woocommerce.py): /wp-json/wc/store/v1/products,
paged 100 at a time, with the product count in the X-WP-Total header.

Prices are in GBP, delivered by the Store API in minor units (pence) and converted by
common/woocommerce.py. NOTE: this store hides prices -- almost every product has price "0"
(quote-only), which the house helper maps to an empty price, and its regular_price is a uniform
placeholder (12000), so it is NOT used as a fallback (that would invent a false price for the
whole catalogue). So the price column is empty for most rows; title / part number / brand /
condition / url / availability are still captured.

The brands taxonomy is empty; this store keeps the useful fields in each product's attributes
instead: "Manufacturer" -> brand, "Condition" -> condition, "Part Number" -> part_number
(falling back to the SKU), "Model" -> model_number. The long "Description" attribute is dropped;
any other attribute goes to other_specs. Run steps, changes and env vars: common/runner.py.
Columns: common/schema.py.
"""
from common import runner, schema, woocommerce as woo

SITE = "eurolanditstore"
BASE = "https://eurolanditstore.co.uk"
CURRENCY = "GBP"          # documented only; prices are saved as the raw number
PART_KEYS = ("part number", "part#", "part #", "mpn")
# Attributes mapped to their own columns (lower-cased), so they don't repeat in other_specs.
# "description" is a long marketing blurb and is dropped rather than dumped into other_specs.
MAPPED_ATTRS = {"manufacturer", "condition", "model", "description", *PART_KEYS}
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
    row["brand"] = row["brand"] or by_key.get("manufacturer", "")
    part = next((by_key[k] for k in PART_KEYS if by_key.get(k)), "")
    row["part_number"] = part or row["sku"]
    row["model_number"] = by_key.get("model", "")
    row["other_specs"] = "; ".join(f"{k}: {v}" for k, v in attrs.items()
                                   if k.lower() not in MAPPED_ATTRS)
    return schema.product_row(row)


def scrape(limit, scraped_at):
    products, listed = woo.fetch_products(BASE, limit)
    rows = [build_row(p, scraped_at) for p in products]
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
