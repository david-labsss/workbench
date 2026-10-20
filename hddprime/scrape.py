"""Scrape hddprime.com (WooCommerce Store API, ~35k products, hard drives / SSDs / servers and
PC hardware): every run's full snapshot appended to Postgres.

hddprime.com is WooCommerce / WordPress (not Shopify), so the house data path is its public,
unauthenticated WooCommerce Store API (common/woocommerce.py): /wp-json/wc/store/v1/products,
paged 100 at a time, with the product count in the X-WP-Total header.

Prices are in USD (prices.currency_code); the Store API returns money in the currency's minor
unit, which common/woocommerce.py divides to the major unit, and the price NUMBER is saved as-is.
This store has no brands taxonomy and no product attributes, and its SKUs are opaque internal
ids (not manufacturer part numbers), so common/woocommerce.product_row's plain mapping is used
(product_id, sku, title = name, category(ies), price, compare_at_price = regular price on sale,
available = is_in_stock, stock_qty from the stock text, url = permalink).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
from common import runner, woocommerce as woo

SITE = "hddprime"
BASE = "https://www.hddprime.com"
CURRENCY = "USD"          # documented only; prices are saved as the raw number


def scrape(limit, scraped_at):
    products, listed = woo.fetch_products(BASE, limit)
    rows = [woo.product_row(SITE, p, scraped_at) for p in products]
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
