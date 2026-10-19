"""Scrape globalonetechnology.com (BigCommerce): every run's full snapshot appended to Postgres.
Fetching and mapping: common/bigcommerce.py. Run steps, changes and env vars: common/runner.py.
Columns: common/schema.py.
"""
from common import bigcommerce, runner

SITE = "globalonetechnology"
BASE = "https://www.globalonetechnology.com"  # storefront JWT's cors origin is the www host

if __name__ == "__main__":
    runner.run(SITE, bigcommerce.scraper(SITE, BASE))
