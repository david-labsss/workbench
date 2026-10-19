"""Changes against the previous full run, the same way for every site that uses common/runner.py.

A product (by product_id) gets a prices row when it is:
  new            not in the previous run and never seen before
  back_in_stock  in stock now, after being out of stock (or unlisted) before -> restocked
  out_of_stock   out of stock now (or gone from the listing) after being in stock
  price          price changed
  stock          stock_qty changed
Several at once are joined with "+", e.g. "price+stock". The first run is the baseline:
no changes are logged until there is a previous full run to compare with.
"""
from psycopg import sql

from common import db, schema

# Columns read back from the previous run: enough for an out_of_stock row of a product that
# disappeared from the listing
PREVIOUS_COLUMNS = ["parent_id", "variant_id", "sku", "part_number", "model_number", "title",
                    "brand", "category", "condition", "price", "available", "stock_qty", "url"]


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _in_stock(v):
    return str(v).strip().lower() in db.TRUE_VALUES


def seen_before(site, product_ids):
    """Of these product ids, the ones in any earlier full run of the site."""
    if not product_ids:
        return set()
    with db.connect() as conn:
        found = conn.execute(
            sql.SQL("SELECT DISTINCT product_id FROM {} WHERE full_run AND product_id = ANY(%s)")
            .format(sql.Identifier(site)), (list(product_ids),)).fetchall()
    return {r[0] for r in found}


def find_changes(site, rows, previous, full_run):
    """rows: this run (PRODUCT_COLUMNS order); previous: {product_id: {column: text}} from
    db.previous_run(). Must run before this run is saved. Returns prices rows."""
    if not full_run or not previous:
        return []
    current = {r[schema.PRODUCT_COLUMNS.index("product_id")]: schema.as_dict(r) for r in rows}
    # A much smaller listing than last time is more likely a bad fetch than real removals
    compare_listing = len(current) >= 0.9 * len(previous)
    returning = seen_before(site, [p for p in current if p not in previous])

    prices = []
    for pid, product in current.items():
        before = previous.get(pid)
        now_in = _in_stock(product["available"])
        if before is None:
            change = schema.BACK_IN_STOCK if pid in returning and now_in else schema.NEW
            prices.append(schema.price_row(product, change))
            continue
        changed = []
        if _num(before.get("price")) != _num(product["price"]):
            changed.append(schema.PRICE)
        if _num(before.get("stock_qty")) != _num(product["stock_qty"]):
            changed.append(schema.STOCK)
        was_in = _in_stock(before.get("available"))
        if was_in != now_in:
            changed.append(schema.BACK_IN_STOCK if now_in else schema.OUT_OF_STOCK)
        if changed:
            prices.append(schema.price_row(product, "+".join(changed), before.get("price", "")))

    if compare_listing:
        for pid, before in previous.items():
            if pid not in current and _in_stock(before.get("available")):
                prices.append(schema.price_row(
                    dict(before, product_id=pid, website=site), schema.OUT_OF_STOCK,
                    before.get("price", ""), available=False))
    return prices
