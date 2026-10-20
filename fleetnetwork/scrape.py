"""Scrape fleetnetwork.ca (BigCommerce, ~255k products): every run's full snapshot appended to Postgres.

fleetnetwork.ca is NOT Shopify (it is BigCommerce), so there is no /products.json. It uses the
house BigCommerce data path (common/bigcommerce.py): the Storefront GraphQL API (/graphql) with
the public, CORS-locked, read-only storefront token the theme ships in its homepage HTML (read
fresh each run), paged by cursor 50 products at a time, one row per variant.

Prices are in CAD (prices.price.currencyCode); the price NUMBER is saved as-is. Mapping is the
shared BigCommerce one: product_id = variant id, parent_id = product id, sku, mpn = part_number,
brand, category (deepest), condition, specs from custom fields, price, compare_at_price (retail /
base price when higher), available, stock_qty (when the store tracks it), url.

NOTE: this is a large dropshipping-style catalogue (~255k products, 26 sitemap pages x 10k), so
a full run makes thousands of GraphQL pages and takes roughly an hour on the US runner. Cursor
paging has no total, so a failed page raises rather than ending the list early (common/bigcommerce.py).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
from common import bigcommerce, runner

SITE = "fleetnetwork"
BASE = "https://fleetnetwork.ca"
CURRENCY = "CAD"          # documented only; prices are saved as the raw number


if __name__ == "__main__":
    runner.run(SITE, bigcommerce.scraper(SITE, BASE))
