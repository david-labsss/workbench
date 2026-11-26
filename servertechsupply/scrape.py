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
    # retry_403: the store sends 403 for a few minutes and then serves again (11-26: the
    # 7:47 AM PKT run got 403 on page 1; the same address answered 200 at about 2:30 PM).
    products, listed = woo.fetch_products(BASE, limit, retry_403=True)
    rows = [woo.product_row(SITE, p, scraped_at) for p in products]
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
