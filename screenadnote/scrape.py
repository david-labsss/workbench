"""Scrape screenadnote.com (WooCommerce Store API, ~48k products: barcode scanners, rugged
mobile computers, accessories): every run's full snapshot appended to Postgres.

Everything comes from the open WooCommerce Store API (/wp-json/wc/store/v1/products, public, no
key). Prices are in USD, delivered in minor units (cents) and converted by
common/woocommerce.py; a "0" price means "call for price" and maps to an empty price. The shared
helper also fills image_url (images[0].src) and description (short/long description as plain
text), so this site needs nothing beyond fetch + map. common/woocommerce.fetch_products walks
every page (per_page=100) until a page comes back empty, so the whole catalogue is covered.

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
from common import runner, woocommerce as woo

SITE = "screenadnote"
BASE = "https://www.screenadnote.com"


def scrape(limit, scraped_at):
    products, listed = woo.fetch_products(BASE, limit)
    rows = [woo.product_row(SITE, p, scraped_at) for p in products]
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
