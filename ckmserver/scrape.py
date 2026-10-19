"""Scrape ckmserver.com (WooCommerce, ~6k products): every run's full snapshot appended to Postgres.

ckmserver.com is NOT Shopify (it is WooCommerce / WordPress), so there is no /products.json. Its
WooCommerce Store API is public and unauthenticated, which is the house data path for WooCommerce
stores (common/woocommerce.py): /wp-json/wc/store/v1/products, paged 100 at a time, with the
product count in the X-WP-Total header.

Prices are in USD (prices.currency_code); the Store API returns money in the currency's minor
unit, which common/woocommerce.py divides down to the major unit, and the price NUMBER is saved
as-is. common/woocommerce.product_row does the standard mapping (product_id, sku, title = name,
brand(s), category(ies), price, compare_at_price = regular price on sale, available =
is_in_stock, stock_qty from the stock text, url = permalink).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
from common import runner, woocommerce as woo

SITE = "ckmserver"
BASE = "https://www.ckmserver.com"
CURRENCY = "USD"          # documented only; prices are saved as the raw number


def scrape(limit, scraped_at):
    products, listed = woo.fetch_products(BASE, limit)
    rows = [woo.product_row(SITE, p, scraped_at) for p in products]
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
