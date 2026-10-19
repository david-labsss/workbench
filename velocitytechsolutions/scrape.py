"""Scrape velocitytechsolutions.com (BigCommerce, ~1.7k products): every run's full snapshot
appended to Postgres. The store doesn't track stock counts, so stock_qty stays blank.
Fetching and mapping: common/bigcommerce.py. Run steps, changes and env vars:
common/runner.py. Columns: common/schema.py.
"""
from common import bigcommerce, runner

SITE = "velocitytechsolutions"
BASE = "https://velocitytechsolutions.com"

if __name__ == "__main__":
    runner.run(SITE, bigcommerce.scraper(SITE, BASE))
