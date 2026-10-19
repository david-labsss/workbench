"""Scrape hummingbirdnetworks.com (BigCommerce, ~18.5k products): every run's full snapshot
appended to Postgres. Fetching and mapping: common/bigcommerce.py (storefront GraphQL, site.products
by cursor; stock_qty = the store's real availableToSell count). A $0 price (e.g. the store's
"Non-Cash Fee" item) is saved empty. Run steps, changes and env vars: common/runner.py.
Columns: common/schema.py.
"""
from common import bigcommerce, runner, schema

SITE = "hummingbirdnetworks"
BASE = "https://www.hummingbirdnetworks.com"  # storefront JWT's cors origin is the www host


def scrape(limit, scraped_at):
    rows, listed = bigcommerce.scraper(SITE, BASE)(limit, scraped_at)
    price = schema.PRODUCT_COLUMNS.index("price")
    for row in rows:
        if row[price] == 0:
            row[price] = ""                # $0 = no real price
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
