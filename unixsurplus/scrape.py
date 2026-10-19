"""Scrape unixsurplus.com (BigCommerce, ~720 products / ~800 variants): every run's full
snapshot appended to Postgres. Fetching and mapping: common/bigcommerce.py. Run steps, changes
and env vars: common/runner.py. Columns: common/schema.py.
"""
from common import bigcommerce, runner

SITE = "unixsurplus"
BASE = "https://unixsurplus.com"

if __name__ == "__main__":
    runner.run(SITE, bigcommerce.scraper(SITE, BASE))
