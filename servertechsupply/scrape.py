"""Scrape servertechsupply.com (WooCommerce Store API, ~55k products): every run's full snapshot
appended to Postgres.

Same Store API mapping as the other WooCommerce stores (common/woocommerce.py); it is just a
larger catalogue, so more pages. This store prices in whole dollars (currency_minor_unit 0),
which woocommerce._amount handles per product. Run steps, changes and env vars:
common/runner.py. Columns: common/schema.py.
"""
from common import runner, woocommerce as woo

SITE = "servertechsupply"
BASE = "https://www.servertechsupply.com"


def scrape(limit, scraped_at):
    products, listed = woo.fetch_products(BASE, limit)
    rows = [woo.product_row(SITE, p, scraped_at) for p in products]
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
