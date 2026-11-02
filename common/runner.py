"""The run steps every newer scraper shares: read the previous run, scrape, check the snapshot is
complete, find changes, save. A site's scrape.py only fetches and maps its products:

    def scrape(limit, scraped_at):
        ...
        return rows, listed   # rows in PRODUCT_COLUMNS order (schema.product_row), and the
                              # number of products the site says it has (None if unknown)

    if __name__ == "__main__":
        runner.run(SITE, scrape)

Env vars:
  DATABASE_URL   Postgres URL for the scraper_writer role
  MAX_PRODUCTS   optional, only scrape about N products (test runs, not compared or counted
                 as a full run)
  DRY_RUN        optional, "1" = print sample rows, don't touch the database
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

from common import changes, db, schema

COLUMNS = schema.PRODUCT_COLUMNS
COMPLETE = 0.98  # a full run must reach this share of the site's listed product count
REPORT_COLUMNS = ["sku", "part_number", "model_number", "brand", "category", "condition",
                  "capacity", "interface", "form_factor", "price", "stock_qty"]


def set_output(name, value):
    path = os.getenv("GITHUB_OUTPUT")
    if path:
        with open(path, "a") as f:
            f.write(f"{name}={value}\n")


def run(site, scrape):
    limit = int(os.getenv("MAX_PRODUCTS") or 0)
    dry_run = os.getenv("DRY_RUN") == "1"
    full_run = not limit
    scraped_dt = datetime.now(timezone.utc)
    scraped_at = scraped_dt.strftime("%Y-%m-%d %H:%M:%S UTC")

    # Read the previous run before scraping, so bad credentials fail in seconds
    previous = {}
    if not dry_run:
        db.check()
        if full_run:
            previous = db.previous_run(site, "product_id", changes.PREVIOUS_COLUMNS)

    started = time.time()
    rows, listed = scrape(limit, scraped_at)
    print(f"scraped {len(rows)} rows in {time.time() - started:.0f}s (site lists {listed})",
          flush=True)
    if not rows:
        sys.exit("No products scraped")
    ids = [r[COLUMNS.index("product_id")] for r in rows]
    if len(set(ids)) != len(ids):
        sys.exit(f"product_id is not unique: {len(ids) - len(set(ids))} duplicates")
    # Never save a partial snapshot as if it were the full catalogue
    if full_run and listed and len(rows) < COMPLETE * listed:
        sys.exit(f"Only scraped {len(rows)} of {listed} products ({len(rows) / listed:.0%}); "
                 "not saving a partial snapshot")

    for c in REPORT_COLUMNS:
        filled = sum(1 for r in rows if r[COLUMNS.index(c)] not in ("", None))
        print(f"  {c}: filled in {filled}/{len(rows)} rows", flush=True)
    in_stock = sum(1 for r in rows if r[COLUMNS.index("available")] is True)

    if dry_run:
        for row in rows[:5]:
            print(json.dumps(dict(zip(COLUMNS, row)), indent=2, default=str))
    else:
        prices = changes.find_changes(site, rows, previous, full_run)
        counts = {}
        for p in prices:
            change = p[schema.PRICE_COLUMNS.index("change")]
            counts[change] = counts.get(change, 0) + 1
        restocked = schema.restocked_count(prices)
        print(f"{len(prices)} changes {counts}, {restocked} back in stock", flush=True)
        db_error = db.save_run(site, rows, scraped_dt, len(rows), full_run)
        db_error = db_error or db.save_prices(prices, scraped_dt, full_run)
        set_output("restocked", restocked)
        if db_error:
            set_output("rows", len(rows))
            sys.exit(f"Saving to the database failed: {db_error}")
    set_output("rows", len(rows))
    set_output("products", len(rows))
    set_output("in_stock", in_stock)
