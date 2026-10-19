"""Scrape dedicatednetworksinc.com (WooCommerce Store API, ~5.7k products): every run's full
snapshot appended to Postgres.

The Store API feed maps straight to the standard columns via common/woocommerce.py; this store
exposes no brand, so that column stays empty. Run steps, changes and env vars:
common/runner.py. Columns: common/schema.py.
"""
from common import runner, woocommerce as woo

SITE = "dedicatednetworksinc"
BASE = "https://dedicatednetworksinc.com"


def scrape(limit, scraped_at):
    products, listed = woo.fetch_products(BASE, limit)
    rows = [woo.product_row(SITE, p, scraped_at) for p in products]
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
