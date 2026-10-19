"""Append-only storage in Postgres (Railway project "server-scrapers"). The only place the
scrapers save data (Google Sheets are no longer written).

  <site>          one table per site (serverpartdeals, directmacro, ...): every run's full
                  snapshot
  prices          one table for all sites ("website" column): price / stock changes, with a
                  "restocked" column for products that came back in stock
  sites           a view: each site's last full run, product count, changes and restocks
Columns are the standard ones in common/schema.py, plus run_id and full_run. Every run
INSERTs; nothing is ever updated or deleted. The scrapers connect as the "scraper_writer" role,
which only has SELECT and INSERT rights (see db/setup.py), so even a buggy run can't change or
remove old data. The old per-site <site>_price_history / <site>_restocked tables were copied
into prices and dropped.

Env vars:
  DATABASE_URL   connection URL for the scraper_writer role
  GITHUB_RUN_ID  set by GitHub Actions; stored with every row as run_id
"""
import os
import sys
from decimal import Decimal

import psycopg
from psycopg import sql

from common.schema import PRICE_COLUMNS, PRODUCT_COLUMNS

NUMERIC = {"price", "old_price", "compare_at_price", "price_per_tb", "discount_pct", "stock_qty"}
BOOLEAN = {"available", "restocked"}
TRUE_VALUES = {"true", "yes", "1"}
EXTRA_COLUMNS = [("run_id", "text"), ("full_run", "boolean NOT NULL")]


def column_type(name):
    if name == "scraped_at":
        return "timestamptz NOT NULL"
    if name in NUMERIC:
        return "numeric"
    if name in BOOLEAN:
        return "boolean"
    return "text"


def to_db(name, value, scraped_at):
    if name == "scraped_at":
        return scraped_at
    if value is None or value == "":
        return None
    if name in NUMERIC:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    if name in BOOLEAN:
        return str(value).strip().lower() in TRUE_VALUES
    return str(value)


def connect():
    url = os.getenv("DATABASE_URL")
    if not url:
        sys.exit("DATABASE_URL is not set (add it under the repo's Settings > Secrets > Actions)")
    return psycopg.connect(url, connect_timeout=30)


def check():
    """Fail fast before scraping if the database can't be reached."""
    with connect() as conn:
        conn.execute("SELECT 1")
    print("connected to database", flush=True)


def insert_rows(table, columns, rows, scraped_at, full_run, run_id=None):
    """INSERT rows into a table in one transaction (all or nothing).
    scraped_at is either one timestamp for every row, or a list with one per row."""
    run_id = run_id or os.getenv("GITHUB_RUN_ID", "local")
    names = list(columns) + [name for name, _ in EXTRA_COLUMNS]
    copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(table), sql.SQL(", ").join(sql.Identifier(n) for n in names))
    per_row = isinstance(scraped_at, list)
    with connect() as conn:
        with conn.cursor() as cur, cur.copy(copy_sql) as copy:
            for i, row in enumerate(rows):
                ts = scraped_at[i] if per_row else scraped_at
                copy.write_row([to_db(c, v, ts) for c, v in zip(columns, row)]
                               + [run_id, full_run])
    print(f"database: inserted {len(rows)} rows into {table} (run {run_id})", flush=True)


def save_prices(price_rows, scraped_at, full_run):
    """INSERT this run's changes (rows in PRICE_COLUMNS order, see common/schema.py) into the
    shared prices table. Returns an error message or None."""
    try:
        insert_rows("prices", PRICE_COLUMNS, price_rows, scraped_at, full_run)
        return None
    except Exception as e:  # noqa: BLE001 - report any database failure, keep the run going
        print(f"ERROR database save of prices failed: {e}", flush=True)
        return str(e)


def _as_text(value):
    """A database value written the way the old sheet showed it: booleans TRUE / FALSE, numbers
    without trailing zeros, empty for NULL. Keeps the scrapers' change checks unchanged."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, Decimal):
        text = format(value, "f")
        return text.rstrip("0").rstrip(".") if "." in text else text
    return str(value)


def previous_run(site, key, columns):
    """The last saved full run's rows from the site's table as {key value: {column: text}},
    or {} if there is none. Read before this run is saved, so "previous" is never today's own
    snapshot."""
    table = site
    names = [key] + [c for c in columns if c != key]
    with connect() as conn:
        last = conn.execute(
            "SELECT run_id FROM scrape_runs WHERE site = %s AND full_run AND status = 'saved' "
            "ORDER BY logged_at DESC LIMIT 1", (site,)).fetchone()
        if not last:
            return {}
        cur = conn.execute(
            sql.SQL("SELECT {} FROM {} WHERE run_id = %s AND full_run").format(
                sql.SQL(", ").join(sql.Identifier(n) for n in names), sql.Identifier(table)),
            (last[0],))
        previous = {}
        for row in cur:
            values = dict(zip(names, (_as_text(v) for v in row)))
            previous[values[key]] = values
    print(f"previous full run (run {last[0]}): {len(previous)} rows", flush=True)
    return previous


def log_run(site, scraped_at, status, rows, products, full_run, note=""):
    run_id = os.getenv("GITHUB_RUN_ID", "local")
    with connect() as conn:
        conn.execute(
            "INSERT INTO scrape_runs (site, scraped_at, run_id, status, rows, products, full_run,"
            " note) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (site, scraped_at, run_id, status, rows, products, full_run, note),
        )


def save_run(site, rows, scraped_at, products, full_run):
    """INSERT the snapshot (rows in PRODUCT_COLUMNS order) into the site's table and log the
    run. Returns an error message, or None if it worked. Never raises, so the run can still
    report what happened."""
    try:
        insert_rows(site, PRODUCT_COLUMNS, rows, scraped_at, full_run)
        log_run(site, scraped_at, "saved", len(rows), products, full_run)
        return None
    except Exception as e:  # noqa: BLE001 - report any database failure, keep the run going
        print(f"ERROR database save failed: {e}", flush=True)
        return str(e)
