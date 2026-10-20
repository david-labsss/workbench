"""Scrape hummingbirdnetworks.com (BigCommerce, ~18.5k products): every run's full snapshot
appended to Postgres. Fetching and mapping: common/bigcommerce.py (storefront GraphQL, site.products
by cursor; stock_qty = the store's real availableToSell count). A $0 price (e.g. the store's
"Non-Cash Fee" item) is saved empty. An in-stock item with availableToSell 0 is one whose
stock the store doesn't count (it is buyable), so its stock_qty is saved empty, not 0. Run
steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
from common import bigcommerce, runner, schema

SITE = "hummingbirdnetworks"
BASE = "https://www.hummingbirdnetworks.com"  # storefront JWT's cors origin is the www host


def scrape(limit, scraped_at):
    rows, listed = bigcommerce.scraper(SITE, BASE)(limit, scraped_at)
    price = schema.PRODUCT_COLUMNS.index("price")
    available = schema.PRODUCT_COLUMNS.index("available")
    stock = schema.PRODUCT_COLUMNS.index("stock_qty")
    for row in rows:
        if row[price] == 0:
            row[price] = ""                # $0 = no real price
        if row[available] is True and row[stock] == 0:
            row[stock] = ""                # in stock but not counted: unknown, not 0
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
