"""The run steps every scraper shares: read the previous run, scrape, check the snapshot is
complete, find changes, save. A site's scrape.py only fetches and maps its products:

    def scrape(limit, scraped_at):
        ...
        return rows, listed   # rows in PRODUCT_COLUMNS order (schema.product_row), and the
                              # number of products the site says it has (None if unknown)

    if __name__ == "__main__":
        runner.run(SITE, scrape)

A scraper that needs the last full run while scraping (serverpartdeals keeps the last known
stock count of a product whose stock request failed) passes needs_previous=True and gets it as
a third argument: scrape(limit, scraped_at, previous).

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

from common import changes, db, master, schema

COLUMNS = schema.PRODUCT_COLUMNS
COMPLETE = 0.98  # a full run must reach this share of the site's listed product count
REPORT_COLUMNS = ["sku", "part_number", "model_number", "brand", "category", "condition",
                  "capacity", "interface", "form_factor", "price", "stock_qty"]


def set_output(name, value):
    path = os.getenv("GITHUB_OUTPUT")
    if path:
        with open(path, "a") as f:
            f.write(f"{name}={value}\n")


def run(site, scrape, min_of_previous=None, needs_previous=False):
    """min_of_previous: for a site whose own count can't be trusted, the share of the last full
    run's products a full run must reach (None = don't compare).
    needs_previous: pass the last full run to scrape() as a third argument, on test runs too."""
    limit = int(os.getenv("MAX_PRODUCTS") or 0)
    dry_run = os.getenv("DRY_RUN") == "1"
    full_run = not limit
    scraped_dt = datetime.now(timezone.utc)
    scraped_at = scraped_dt.strftime("%Y-%m-%d %H:%M:%S UTC")

    # snapshot (old way), both (master + snapshot) or master (master only): ops/storage.json
    storage = master.mode(site)
    print(f"storage: {storage}", flush=True)

    # Read the previous run before scraping, so bad credentials fail in seconds. The master
    # modes compare against the master table instead, inside the save.
    previous = {}
    if not dry_run:
        db.check()
        if (full_run and storage == "snapshot") or needs_previous:
            # the master modes keep the last run in the master table, not the site's own table
            read = db.previous_run if storage == "snapshot" else db.previous_from_master
            previous = read(site, "product_id", changes.PREVIOUS_COLUMNS)
    # The last full run's size, for min_of_previous. The master modes don't read the last run's
    # rows, so they take its row count from scrape_runs; without it the rule would compare with 0
    # and let any run through.
    last_count = len(previous)
    if min_of_previous and full_run and not dry_run and storage != "snapshot":
        last_count = db.last_run_rows(site)

    started = time.time()
    rows, listed = (scrape(limit, scraped_at, previous) if needs_previous
                    else scrape(limit, scraped_at))
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
    if min_of_previous and full_run and len(rows) < min_of_previous * last_count:
        sys.exit(f"Only scraped {len(rows)} products, the last full run had {last_count}; "
                 "not saving a partial snapshot (if the store really shrank, lower "
                 f"min_of_previous in {site}/scrape.py for one run)")

    for c in REPORT_COLUMNS:
        filled = sum(1 for r in rows if r[COLUMNS.index(c)] not in ("", None))
        print(f"  {c}: filled in {filled}/{len(rows)} rows", flush=True)
    # True, or "Yes" on the scrapers that map it that way (the database stores both as true)
    in_stock = sum(1 for r in rows
                   if str(r[COLUMNS.index("available")]).strip().lower() in db.TRUE_VALUES)

    if dry_run:
        for row in rows[:5]:
            print(json.dumps(dict(zip(COLUMNS, row)), indent=2, default=str))
    elif storage != "snapshot":
        try:
            result = master.save(site, rows, scraped_dt, len(rows), full_run,
                                 snapshot=storage == "both")
        except master.Refused as e:
            set_output("rows", len(rows))
            sys.exit(f"Not saved: {e}")
        except Exception as e:  # noqa: BLE001 - report any database failure
            set_output("rows", len(rows))
            sys.exit(f"Saving to the database failed: {e}")
        set_output("restocked", result["restocked"])
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
