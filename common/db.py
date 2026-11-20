"""Storage in Postgres (Railway project "server-scrapers"). The only place the scrapers save
data (Google Sheets are no longer written).

  products        one row per product per site: its current values (common/master.py saves
                  every run there, and keeps every overwritten value in field_changes /
                  listing_events / run_misses, so any past run can be rebuilt: master_run())
  scrape_runs     one row per run (saved, or refused with the reason)
  sites           a view: each site's last full run, product count, changes and restocks
Columns are the standard ones in common/schema.py. The scrapers connect as the "scraper_writer"
role (see db/setup.py): it can update products and master_sync, and only add to the history
and scrape_runs.

Env vars:
  DATABASE_URL   connection URL for the scraper_writer role
  GITHUB_RUN_ID  set by GitHub Actions; stored with every run as run_id
"""
import os
import sys
from decimal import Decimal

import psycopg
from psycopg import sql

from common.schema import PRODUCT_COLUMNS

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


def run_rows(conn, site, run_id):
    """SQL FROM-item: the rows of the site's saved full run `run_id`: the master table's listed
    products when it is at that run (the site's latest), else the run rebuilt from the history
    (master_run()). Use as SELECT <columns> FROM <run_rows(...)> r."""
    at = conn.execute("SELECT run_id FROM master_sync WHERE website = %s", (site,)).fetchone()
    if at and at[0] == run_id:
        return sql.SQL("(SELECT * FROM products WHERE website = {} AND listed)").format(
            sql.Literal(site))
    return sql.SQL("(SELECT * FROM master_run({}, {}))").format(sql.Literal(site),
                                                               sql.Literal(run_id))


def last_full_run(conn, site):
    """The site's last saved full run's run_id, or None."""
    last = conn.execute(
        "SELECT run_id FROM scrape_runs WHERE site = %s AND full_run AND status = 'saved' "
        "ORDER BY logged_at DESC LIMIT 1", (site,)).fetchone()
    return last[0] if last else None


def previous_run(site, key, columns):
    """The last saved full run's rows as {key value: {column: text}}, or {} if there is none
    (see run_rows). Read before this run is saved, so "previous" is never today's own run."""
    names = [key] + [c for c in columns if c != key]
    with connect() as conn:
        run_id = last_full_run(conn, site)
        if not run_id:
            return {}
        source = run_rows(conn, site, run_id)
        cur = conn.execute(sql.SQL("SELECT {} FROM {} r").format(
            sql.SQL(", ").join(sql.Identifier(n) for n in names), source))
        previous = {}
        for row in cur:
            values = dict(zip(names, (_as_text(v) for v in row)))
            previous[values[key]] = values
        where = "master table" if "FROM products" in source.as_string(conn) else "rebuilt"
    print(f"previous full run (run {run_id}, {where}): {len(previous)} rows", flush=True)
    return previous


def log_run(site, scraped_at, status, rows, products, full_run, note=""):
    run_id = os.getenv("GITHUB_RUN_ID", "local")
    with connect() as conn:
        conn.execute(
            "INSERT INTO scrape_runs (site, scraped_at, run_id, status, rows, products, full_run,"
            " note) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (site, scraped_at, run_id, status, rows, products, full_run, note),
        )


def run_saved(site, scraped_at):
    """True if this run (this site, this exact scrape time) is already logged as saved:
    common/runner.py checks it when a save reports an error, since its COMMIT may still have gone
    through (only the reply lost), so the run is not logged as refused."""
    with connect() as conn:
        return conn.execute(
            "SELECT 1 FROM scrape_runs WHERE site = %s AND scraped_at = %s AND status = 'saved' "
            "LIMIT 1", (site, scraped_at)).fetchone() is not None


def last_run_rows(site):
    """How many rows the site's last saved full run had, or 0 if it has none (the 90% rule,
    min_of_previous in common/runner.py, compares with it)."""
    with connect() as conn:
        last = conn.execute(
            "SELECT rows FROM scrape_runs WHERE site = %s AND full_run AND status = 'saved' "
            "ORDER BY logged_at DESC LIMIT 1", (site,)).fetchone()
    return int(last[0] or 0) if last else 0


def log_run_cur(cur, site, scraped_at, run_id, status, rows, products, full_run, note="",
                counts=None):
    """log_run on an open cursor, inside the caller's transaction (common/master.py), with
    the run's new / changed / removed / unchanged counts when given."""
    counts = counts or {}
    names = ["site", "scraped_at", "run_id", "status", "rows", "products", "full_run", "note"]
    values = [site, scraped_at, run_id, status, rows, products, full_run, note]
    for name, value in counts.items():
        names.append(name)
        values.append(value)
    cur.execute(sql.SQL("INSERT INTO scrape_runs ({}) VALUES ({})").format(
        sql.SQL(", ").join(sql.Identifier(n) for n in names),
        sql.SQL(", ").join(sql.Placeholder() for _ in names)), values)
