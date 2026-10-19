"""Scrape link-us-online.com (WooCommerce Store API, ~4.9k products): every run's full snapshot
appended to Postgres.

The Store API feed maps straight to the standard columns via common/woocommerce.py, including
regular_price as the old price on items that are on sale. Run steps, changes and env vars:
common/runner.py. Columns: common/schema.py.
"""
from common import runner, woocommerce as woo

SITE = "linkusonline"
BASE = "https://link-us-online.com"


def scrape(limit, scraped_at):
    products, listed = woo.fetch_products(BASE, limit)
    rows = [woo.product_row(SITE, p, scraped_at) for p in products]
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
