"""The run steps every scraper shares: scrape, check the run is complete, save it to the master
table (common/master.py), which finds and logs the changes. A site's scrape.py only fetches and
maps its products:

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

from common import db, master, schema

COLUMNS = schema.PRODUCT_COLUMNS
COMPLETE = 0.98  # a full run must reach this share of the site's listed product count
REPORT_COLUMNS = ["sku", "part_number", "model_number", "brand", "category", "condition",
                  "capacity", "interface", "form_factor", "price", "stock_qty"]
# Columns of the last full run a needs_previous scraper gets (serverpartdeals: last known stock)
PREVIOUS_COLUMNS = ["parent_id", "variant_id", "sku", "part_number", "model_number", "title",
                    "brand", "category", "condition", "price", "available", "stock_qty", "url"]


def set_output(name, value):
    path = os.getenv("GITHUB_OUTPUT")
    if path:
        with open(path, "a") as f:
            f.write(f"{name}={value}\n")


def refuse(site, scraped_dt, rows, full_run, reason, dry_run=False):
    """Stop without saving: the site keeps its last saved run, and the reason goes to
    scrape_runs (status "refused") so every run that wasn't saved is visible."""
    set_output("rows", rows)
    if not dry_run:
        try:
            db.log_run(site, scraped_dt, "refused", rows, rows, full_run, f"refused: {reason}"[:500])
        except Exception as e:  # noqa: BLE001 - the exit below still reports the reason
            print(f"WARNING: could not log the refused run: {e}", flush=True)
    sys.exit(f"Not saved (refused, the site keeps its last saved run): {reason}")


def run(site, scrape, min_of_previous=None, needs_previous=False):
    """min_of_previous: for a site whose own count can't be trusted, the share of the last full
    run's products a full run must reach (None = don't compare).
    needs_previous: pass the last full run to scrape() as a third argument, on test runs too."""
    limit = int(os.getenv("MAX_PRODUCTS") or 0)
    dry_run = os.getenv("DRY_RUN") == "1"
    full_run = not limit
    scraped_dt = datetime.now(timezone.utc)
    scraped_at = scraped_dt.strftime("%Y-%m-%d %H:%M:%S UTC")

    # Check the database before scraping, so bad credentials fail in seconds. The save compares
    # the run with the master table itself.
    previous = {}
    if not dry_run:
        db.check()
        if needs_previous:
            previous = db.previous_run(site, "product_id", PREVIOUS_COLUMNS)
    # The last full run's size, for min_of_previous (from scrape_runs)
    last_count = 0
    if min_of_previous and full_run and not dry_run:
        last_count = db.last_run_rows(site)

    started = time.time()
    rows, listed = (scrape(limit, scraped_at, previous) if needs_previous
                    else scrape(limit, scraped_at))
    print(f"scraped {len(rows)} rows in {time.time() - started:.0f}s (site lists {listed})",
          flush=True)
    if not rows:
        refuse(site, scraped_dt, 0, full_run, "No products scraped", dry_run)
    ids = [r[COLUMNS.index("product_id")] for r in rows]
    if len(set(ids)) != len(ids):
        refuse(site, scraped_dt, len(rows), full_run,
               f"product_id is not unique: {len(ids) - len(set(ids))} duplicates", dry_run)
    # Never save a partial run as if it were the full catalogue
    if full_run and listed and len(rows) < COMPLETE * listed:
        refuse(site, scraped_dt, len(rows), full_run,
               f"Only scraped {len(rows)} of {listed} products ({len(rows) / listed:.0%}); "
               "not saving a partial run", dry_run)
    if min_of_previous and full_run and len(rows) < min_of_previous * last_count:
        refuse(site, scraped_dt, len(rows), full_run,
               f"Only scraped {len(rows)} products, the last full run had {last_count}; "
               "not saving a partial run (if the store really shrank, lower "
               f"min_of_previous in {site}/scrape.py for one run)", dry_run)

    for c in REPORT_COLUMNS:
        filled = sum(1 for r in rows if r[COLUMNS.index(c)] not in ("", None))
        print(f"  {c}: filled in {filled}/{len(rows)} rows", flush=True)
    # True, or "Yes" on the scrapers that map it that way (the database stores both as true)
    in_stock = sum(1 for r in rows
                   if str(r[COLUMNS.index("available")]).strip().lower() in db.TRUE_VALUES)

    # The master table takes the run, or the run is refused: nothing is saved (the site keeps its
    # last saved run) and the reason goes to scrape_runs (status "refused").
    if dry_run:
        for row in rows[:5]:
            print(json.dumps(dict(zip(COLUMNS, row)), indent=2, default=str))
    else:
        result, reason = None, None
        try:
            result = master.save(site, rows, scraped_dt, len(rows), full_run)
        except Exception as e:  # noqa: BLE001 - refused or any error: nothing was saved
            reason = str(e) if isinstance(e, master.Refused) else f"{type(e).__name__}: {e}"
        if result is not None:
            set_output("restocked", result["restocked"])
        else:
            try:
                committed = db.run_saved(site, scraped_dt)
            except Exception:  # noqa: BLE001 - can't tell; refused below says so
                committed = False
            if committed:
                # the save's COMMIT went through, only its reply was lost
                print(f"WARNING: the save reported an error ({reason}), but the run is saved.",
                      flush=True)
            else:
                refuse(site, scraped_dt, len(rows), full_run, reason)
    set_output("rows", len(rows))
    set_output("products", len(rows))
    set_output("in_stock", in_stock)
