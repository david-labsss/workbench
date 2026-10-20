"""Scrape tekboost.com (BigCommerce, ~33 configure-to-order server products / ~59 variants):
every run's full snapshot appended to Postgres.

The store's pages carry the public storefront GraphQL token, so common/bigcommerce.py reads every
product and variant (one row per variant). Prices are the configure-to-order BASE prices (e.g.
"Dell PowerEdge R640 Configure To Order" $399); condition (REFURBISHED) comes from the BigCommerce
condition field and stock_qty from the store's tracked inventory. Items the store doesn't count
(the GPU upgrade options and build-to-order workstations: in stock and buyable, availableToSell
0, the page shows no number) get an empty stock_qty, not 0. Fetching and mapping:
common/bigcommerce.py. Run steps, changes and env vars: common/runner.py. Columns:
common/schema.py.
"""
from common import bigcommerce, runner, schema

SITE = "tekboost"
BASE = "https://tekboost.com"


def scrape(limit, scraped_at):
    rows, listed = bigcommerce.scraper(SITE, BASE)(limit, scraped_at)
    available = schema.PRODUCT_COLUMNS.index("available")
    stock = schema.PRODUCT_COLUMNS.index("stock_qty")
    for row in rows:
        if row[available] is True and row[stock] == 0:
            row[stock] = ""                # in stock but not counted: unknown, not 0
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
