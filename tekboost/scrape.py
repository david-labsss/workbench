"""Scrape tekboost.com (BigCommerce, ~33 configure-to-order server products / ~59 variants):
every run's full snapshot appended to Postgres.

The store's pages carry the public storefront GraphQL token, so common/bigcommerce.py reads every
product and variant (one row per variant). Prices are the configure-to-order BASE prices (e.g.
"Dell PowerEdge R640 Configure To Order" $399); condition (REFURBISHED) comes from the BigCommerce
condition field and stock_qty from the store's tracked inventory. Fetching and mapping:
common/bigcommerce.py. Run steps, changes and env vars: common/runner.py. Columns:
common/schema.py.
"""
from common import bigcommerce, runner

SITE = "tekboost"
BASE = "https://tekboost.com"

if __name__ == "__main__":
    runner.run(SITE, bigcommerce.scraper(SITE, BASE))
