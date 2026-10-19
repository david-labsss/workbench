"""The standard columns every site uses, so all tables look the same.

product_id is unique within a site: one row per sellable item. On sites with variants
(serverpartdeals, directmacro) it is the variant's own id, and parent_id is the product the
variant belongs to (empty elsewhere). SKUs are not used as the key: some sites leave them
empty or repeat them.

Product tables: one per site (serverpartdeals, directmacro, ...), every run's full snapshot.
Every site's table has all PRODUCT_COLUMNS; a site leaves the ones it doesn't have empty.

prices: ONE table for all sites (the "website" column says which). A row per change against
the previous full run: new product, price change, stock change, out of stock, back in stock.
"restocked" is true when a product came back in stock (out of stock / unlisted -> in stock).
"""

PRODUCT_COLUMNS = [
    "scraped_at", "website", "product_id", "parent_id", "variant_id", "sku", "part_number",
    "model_number", "title", "brand", "category", "condition", "series", "capacity", "interface", "form_factor",
    "spindle_speed", "cache", "endurance", "other_specs", "price", "compare_at_price",
    "price_per_tb", "discount_pct", "available", "stock_qty", "url",
]

PRICE_COLUMNS = [
    "scraped_at", "website", "product_id", "parent_id", "variant_id", "sku", "part_number",
    "model_number", "title", "brand", "category", "condition", "old_price", "price", "available", "stock_qty",
    "change", "restocked", "url",
]

# change values (several joined with "+", e.g. "price+stock")
NEW, PRICE, STOCK, OUT_OF_STOCK, BACK_IN_STOCK = (
    "new", "price", "stock", "out_of_stock", "back_in_stock")


def product_row(values):
    """A product table row (list in PRODUCT_COLUMNS order) from a {column: value} dict."""
    return ["" if values.get(c) is None else values.get(c, "") for c in PRODUCT_COLUMNS]


def as_dict(row):
    """A product table row back to {column: value}."""
    return dict(zip(PRODUCT_COLUMNS, row))


def price_row(product, change, old_price="", **overrides):
    """A prices row from a product {column: value} dict (this run's row, or the previous
    run's for products that disappeared)."""
    values = {c: product.get(c, "") for c in PRICE_COLUMNS}
    values.update(change=change, old_price="" if old_price is None else old_price,
                  restocked=BACK_IN_STOCK in change.split("+"), **overrides)
    return [values[c] for c in PRICE_COLUMNS]


def restocked_count(price_rows):
    i = PRICE_COLUMNS.index("restocked")
    return sum(1 for r in price_rows if r[i] is True)
