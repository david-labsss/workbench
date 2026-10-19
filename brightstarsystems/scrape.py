"""Scrape brightstarsystems.com (WooCommerce Store API, ~3.1k products): every run's full
snapshot appended to Postgres.

Price, stock, brand and category come from the Store API (common/woocommerce.py). The item
condition isn't in that feed, so it is read from the WordPress REST listing (/wp-json/wp/v2/
product), where each product carries a "product-conditions-<value>" CSS class in class_list
(e.g. "product-conditions-seller-refurbished" -> "Seller Refurbished").

Note: this store has been seen to answer 403 outside the US; if this PC is blocked it must run
on a US runner. Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
"""
import time

from common import http, runner, woocommerce as woo

SITE = "brightstarsystems"
BASE = "https://brightstarsystems.com"
WP_PRODUCTS = "/wp-json/wp/v2/product"
COND_PREFIX = "product-conditions-"


def _condition(class_list):
    for cls in class_list or []:
        if cls.startswith(COND_PREFIX):
            value = cls[len(COND_PREFIX):].strip()
            if value:
                return value.replace("-", " ").title()
    return ""


def fetch_conditions(needed=None):
    """product_id (str) -> condition, from the WP REST class_list. Bounded by the listing's
    X-WP-TotalPages header, and stops early once every id in `needed` has a condition, so test
    runs don't page the whole catalogue."""
    needed = set(needed) if needed else None
    conditions, page, pages = {}, 1, 1
    while page <= pages:
        r = http.session().get(BASE + WP_PRODUCTS,
                               params={"per_page": 100, "page": page,
                                       "_fields": "id,class_list"}, timeout=60)
        r.raise_for_status()
        if page == 1:
            try:
                pages = int(r.headers.get("X-WP-TotalPages") or 1)
            except (TypeError, ValueError):
                pages = 1
        batch = r.json()
        if not batch:
            break
        for item in batch:
            cond = _condition(item.get("class_list"))
            if cond:
                conditions[str(item["id"])] = cond
        if needed and needed <= set(conditions):
            break
        page += 1
        time.sleep(woo.PAGE_DELAY)
    return conditions


def scrape(limit, scraped_at):
    products, listed = woo.fetch_products(BASE, limit)
    conditions = fetch_conditions({str(p["id"]) for p in products})
    rows = [woo.product_row(SITE, p, scraped_at, conditions.get(str(p["id"]), ""))
            for p in products]
    return rows, listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
