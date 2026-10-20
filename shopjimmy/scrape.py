"""Scrape shopjimmy.com (BigCommerce, store hash s-v5cphvs3s0, TV/appliance repair parts):
every run's full snapshot appended to Postgres. Some items are "make an offer"/quote and carry
no price ($0 -> None). Fetching and mapping: common/bigcommerce.py. Run steps, changes and env
vars: common/runner.py. Columns: common/schema.py.
"""
from common import bigcommerce, runner

SITE = "shopjimmy"
BASE = "https://www.shopjimmy.com"

if __name__ == "__main__":
    runner.run(SITE, bigcommerce.scraper(SITE, BASE))
