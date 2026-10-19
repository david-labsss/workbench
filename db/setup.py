"""Create or extend the database tables and the restricted scraper role. Safe to re-run.

Run by the "Set up database" workflow with the admin connection. It only CREATEs tables, ADDs
missing columns, renames a column or table to its standard name, copies old rows into new
tables, (re)creates the sites view and GRANTs rights. Two one-time changes the user approved on 2026-10-12:
- on sites with variants, product_id became the variant's own id and the old value moved to
  parent_id (use_variant_ids), so product_id is unique on every site;
- the old per-site <site>_price_history / <site>_restocked tables were dropped once their rows
  were checked to be in prices (drop_old_tables). Nothing else is ever dropped or deleted.

Tables (all append-only, columns in common/schema.py):
  serverpartdeals, directmacro, serverorbit,
  orangehardwares                    one per site: every run's full product snapshot
  prices                             all sites ("website" column): price / stock changes, with
                                     "restocked" for products back in stock
  scrape_runs                        one row per run
  qa_checks                          the dashboard's daily QA: a sample of each new full run
                                     fetched again from the store and compared, one row a product
  sites (view)                       each site's last full run: products, changes, restocks
The old <site>_price_history and <site>_restocked tables were copied into prices
(copy_old_history), checked and dropped (drop_old_tables).

Roles:
  scraper_writer         the scrapers: SELECT + INSERT only
  scraper_writer_mirror  the same, for the scrapers that run on the other GitHub accounts
                         (README: "GitHub accounts"), so those accounts hold their own password
  scraper_reader         people and tools querying the data: read-only (SELECT, read-only sessions)
  qa_writer              the dashboard's daily QA (server-scrapers-ui): INSERT into qa_checks only

Env vars:
  DATABASE_ADMIN_URL   connection URL for the admin (postgres) user
  SCRAPER_DB_PASSWORD  password to set for the scraper_writer role
  MIRROR_DB_PASSWORD   optional, password to set for the scraper_writer_mirror role
  READER_DB_PASSWORD   optional, password to set for the scraper_reader role
  QA_DB_PASSWORD       optional, password to set for the qa_writer role
"""
import os
import sys

import psycopg
from psycopg import sql

from common.db import EXTRA_COLUMNS, column_type
from common.schema import PRICE_COLUMNS, PRODUCT_COLUMNS

SITES = ["serverpartdeals", "directmacro", "serverorbit", "orangehardwares", "waterpanther",
         "unixsurplus", "velocitytechsolutions", "disctech", "harddiskdirect",
         "soliddiskdirect", "pcserverandparts", "flagshiptech", "goharddrive",
         "memoryclearance", "serverworlds", "compudevices", "usithardware", "serversupply"]
# Sites dropped before they ever saved data: their (empty) tables are removed
REMOVED_SITES = []
# Sites with variants: product_id is the variant's own id, parent_id its product
VARIANT_SITES = ["serverpartdeals", "directmacro"]
HISTORY = "prices"  # the shared change table (briefly "price_history" on 2026-10-12)
# Old table name -> standard name
TABLE_RENAMES = {"price_history": HISTORY}
# Old column name -> standard name, per site
RENAMES = {"serverorbit": {"list_price": "compare_at_price"}}
# Old change names -> standard ones (see common/schema.py)
CHANGE_SQL = ("replace(replace(lower(trim({})), ' ', '_'), 'sold_out', 'out_of_stock')")
# Columns of an old history row filled from the site's product table when missing
LOOKUP_COLUMNS = ["product_id", "part_number", "brand", "category", "url"]
WRITER = "scraper_writer"
MIRROR_WRITER = "scraper_writer_mirror"
READER = "scraper_reader"
QA_WRITER = "qa_writer"


def ensure_role(cur, dbname, role, password):
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
    verb = "ALTER" if cur.fetchone() else "CREATE"
    cur.execute(sql.SQL(verb + " ROLE {} LOGIN PASSWORD {}").format(
        sql.Identifier(role), sql.Literal(password)))
    cur.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
        sql.Identifier(dbname), sql.Identifier(role)))
    cur.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(role)))


def table_columns(cur, table):
    cur.execute("SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s", (table,))
    return {r[0] for r in cur.fetchall()}


def ensure_table(cur, table, columns, website=None):
    """Create the table, or add the columns it's missing. website: a product table's site,
    which also fills the website column of the rows it already holds."""
    cols = [(c, column_type(c)) for c in columns] + EXTRA_COLUMNS
    cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} (id bigint GENERATED ALWAYS AS IDENTITY "
                        "PRIMARY KEY, {})").format(
        sql.Identifier(table),
        sql.SQL(", ").join(sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(t))
                           for c, t in cols)))
    existing = table_columns(cur, table)
    for old, new in RENAMES.get(table, {}).items():
        if old in existing and new not in existing:
            cur.execute(sql.SQL("ALTER TABLE {} RENAME COLUMN {} TO {}").format(
                sql.Identifier(table), sql.Identifier(old), sql.Identifier(new)))
            print(f"  {table}: renamed {old} -> {new}", flush=True)
    for c, t in cols:
        t = t.replace(" NOT NULL", "")
        if c == "website" and website:
            # Postgres fills existing rows from the default without rewriting them
            t += " DEFAULT " + sql.Literal(website).as_string(cur)
        cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
            sql.Identifier(table), sql.Identifier(c), sql.SQL(t)))
    # run_id: a UI reads one run (e.g. a site's latest); part_number: searches
    for c in ("scraped_at", "sku", "product_id", "website", "run_id", "part_number"):
        if c in columns or c == "run_id":
            cur.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                sql.Identifier(f"{table}_{c}_idx"), sql.Identifier(table), sql.Identifier(c)))


def copy_old_history(cur, site):
    """Copy <site>_price_history into prices once (skipped if its runs are already there).
    Missing columns (product id, brand, url, ...) come from the site's latest product row with
    the same SKU / product id. Change names are made standard; restocked = back_in_stock."""
    old = f"{site}_price_history"
    old_cols = table_columns(cur, old)
    if not old_cols:
        return
    cur.execute(sql.SQL("SELECT EXISTS (SELECT 1 FROM {} WHERE website = %s AND run_id IN "
                        "(SELECT DISTINCT run_id FROM {}))").format(
        sql.Identifier(HISTORY), sql.Identifier(old)), (site,))
    if cur.fetchone()[0]:
        print(f"  {old}: already copied into {HISTORY}", flush=True)
        return
    key = "product_id" if "product_id" in old_cols else "sku"
    target, select = [], []
    for c in PRICE_COLUMNS:
        if c == "website":
            expr = sql.Literal(site)
        elif c == "change":
            expr = sql.SQL(CHANGE_SQL).format(sql.SQL("h.change"))
        elif c == "restocked":
            expr = sql.SQL(CHANGE_SQL + " LIKE '%%back_in_stock%%'").format(sql.SQL("h.change"))
        elif c in old_cols and c in LOOKUP_COLUMNS:
            expr = sql.SQL("COALESCE(h.{}, p.{})").format(sql.Identifier(c), sql.Identifier(c))
        elif c in old_cols:
            expr = sql.SQL("h.{}").format(sql.Identifier(c))
        elif c in LOOKUP_COLUMNS:
            expr = sql.SQL("p.{}").format(sql.Identifier(c))
        else:
            continue
        target.append(c)
        select.append(expr)
    target += ["run_id", "full_run"]
    select += [sql.SQL("h.run_id"), sql.SQL("h.full_run")]
    cur.execute(sql.SQL(
        "INSERT INTO {} ({}) SELECT {} FROM {} h LEFT JOIN LATERAL ("
        "SELECT {} FROM {} p WHERE p.{} = h.{} ORDER BY p.scraped_at DESC LIMIT 1) p ON true"
    ).format(
        sql.Identifier(HISTORY),
        sql.SQL(", ").join(sql.Identifier(c) for c in target),
        sql.SQL(", ").join(select),
        sql.Identifier(old),
        sql.SQL(", ").join(sql.Identifier(c) for c in LOOKUP_COLUMNS),
        sql.Identifier(site), sql.Identifier(key), sql.Identifier(key)))
    print(f"  {old}: copied {cur.rowcount} rows into {HISTORY}", flush=True)


def rename_tables(cur):
    """Rename a table (with its id sequence and indexes) to its standard name, once."""
    for old, new in TABLE_RENAMES.items():
        if not table_columns(cur, old) or table_columns(cur, new):
            continue
        cur.execute(sql.SQL("ALTER TABLE {} RENAME TO {}").format(
            sql.Identifier(old), sql.Identifier(new)))
        cur.execute(sql.SQL("ALTER SEQUENCE IF EXISTS {} RENAME TO {}").format(
            sql.Identifier(f"{old}_id_seq"), sql.Identifier(f"{new}_id_seq")))
        cur.execute("SELECT indexname FROM pg_indexes WHERE schemaname = 'public' "
                    "AND tablename = %s AND indexname LIKE %s", (new, old + r"\_%"))
        for (index,) in cur.fetchall():
            cur.execute(sql.SQL("ALTER INDEX {} RENAME TO {}").format(
                sql.Identifier(index), sql.Identifier(new + index[len(old):])))
        print(f"renamed table {old} -> {new}", flush=True)


def create_sites_view(cur):
    """sites: one row per site that has a saved full run, so a UI can list the sites without
    knowing them in advance (a new site shows up after its first full run)."""
    cur.execute(sql.SQL("""
        CREATE OR REPLACE VIEW sites AS
        WITH last AS (
            SELECT DISTINCT ON (site) site, run_id, scraped_at, rows
            FROM scrape_runs WHERE full_run AND status = 'saved'
            ORDER BY site, logged_at DESC)
        SELECT l.site AS website,
               l.scraped_at AS last_scraped_at,
               l.rows AS products,
               (SELECT count(*) FROM {h} p
                WHERE p.website = l.site AND p.run_id = l.run_id) AS changes,
               (SELECT count(*) FROM {h} p
                WHERE p.website = l.site AND p.run_id = l.run_id AND p.restocked) AS restocked,
               l.run_id AS last_run_id
        FROM last l
        ORDER BY l.site""").format(h=sql.Identifier(HISTORY)))
    print("view sites: each site's last full run", flush=True)


def drop_old_tables(cur, site):
    """Drop <site>_price_history and <site>_restocked, but only after checking that prices
    holds as many rows for their runs (history) and as many restocks (restocked)."""
    old_history, old_restocked = f"{site}_price_history", f"{site}_restocked"
    checks = []
    for old, restocked_only in ((old_history, False), (old_restocked, True)):
        if not table_columns(cur, old):
            continue
        cur.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(old)))
        old_rows = cur.fetchone()[0]
        cur.execute(sql.SQL(
            "SELECT count(*) FROM {} WHERE website = %s AND (NOT %s OR restocked) "
            "AND run_id IN (SELECT DISTINCT run_id FROM {})").format(
            sql.Identifier(HISTORY), sql.Identifier(old)), (site, restocked_only))
        in_prices = cur.fetchone()[0]
        checks.append((old, old_rows, in_prices))
    for old, old_rows, in_prices in checks:
        if in_prices < old_rows:
            print(f"  NOT dropping {site}'s old tables: {old} has {old_rows} rows, {HISTORY} "
                  f"only {in_prices}", flush=True)
            return
    for old, old_rows, in_prices in checks:
        cur.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(old)))
        print(f"  dropped {old} ({old_rows} rows, all in {HISTORY}: {in_prices})", flush=True)


def drop_removed_sites(cur):
    """Drop a removed site's product table, only if it never held a row."""
    for site in REMOVED_SITES:
        if not table_columns(cur, site):
            continue
        cur.execute(sql.SQL("SELECT EXISTS (SELECT 1 FROM {})").format(sql.Identifier(site)))
        if cur.fetchone()[0]:
            print(f"  NOT dropping {site}: it has rows", flush=True)
            continue
        cur.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(site)))
        print(f"  dropped the empty table of removed site {site}", flush=True)


def use_variant_ids(cur):
    """Rows saved before product_id was unique on sites with variants: move product_id to
    parent_id and put the variant's own id in product_id. No value is lost. Only touches
    rows still in the old form, so re-running changes nothing."""
    for table, where in [(s, sql.SQL("TRUE")) for s in VARIANT_SITES] + [
            (HISTORY, sql.SQL("website = ANY({})").format(sql.Literal(VARIANT_SITES)))]:
        cur.execute(sql.SQL(
            "UPDATE {} SET parent_id = product_id, product_id = variant_id "
            "WHERE {} AND parent_id IS NULL AND variant_id IS NOT NULL AND variant_id <> '' "
            "AND product_id IS DISTINCT FROM variant_id").format(sql.Identifier(table), where))
        print(f"  {table}: {cur.rowcount} old rows now use the variant id as product_id",
              flush=True)


def main():
    admin_url = os.getenv("DATABASE_ADMIN_URL")
    password = os.getenv("SCRAPER_DB_PASSWORD")
    if not admin_url or not password:
        sys.exit("DATABASE_ADMIN_URL and SCRAPER_DB_PASSWORD must be set")

    with psycopg.connect(admin_url, connect_timeout=30) as conn, conn.cursor() as cur:
        for site in SITES:
            ensure_table(cur, site, PRODUCT_COLUMNS, website=site)
            print(f"table {site}: {len(PRODUCT_COLUMNS)} standard columns", flush=True)
        rename_tables(cur)
        ensure_table(cur, HISTORY, PRICE_COLUMNS)
        print(f"table {HISTORY}: {len(PRICE_COLUMNS)} standard columns", flush=True)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scrape_runs (
                id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                site text NOT NULL,
                scraped_at timestamptz NOT NULL,
                run_id text,
                status text NOT NULL,
                rows integer,
                products integer,
                full_run boolean NOT NULL,
                note text,
                logged_at timestamptz NOT NULL DEFAULT now()
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS qa_checks (
                id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                checked_at timestamptz NOT NULL DEFAULT now(),
                website text NOT NULL,
                run_id text NOT NULL,
                product_id text NOT NULL,
                url text,
                source text,
                saved_price numeric,
                live_price numeric,
                saved_available boolean,
                live_available boolean,
                result text NOT NULL,
                note text
            )""")
        cur.execute("CREATE INDEX IF NOT EXISTS qa_checks_website_run_idx ON qa_checks (website, run_id)")
        print("table qa_checks: QA results, one row per product checked", flush=True)
        for site in SITES:
            copy_old_history(cur, site)
        use_variant_ids(cur)
        for site in SITES + REMOVED_SITES:
            drop_old_tables(cur, site)
        drop_removed_sites(cur)
        create_sites_view(cur)

        # Restricted roles for the scrapers: read and insert only (the mirror role for the
        # scrapers on the other GitHub accounts)
        writers = [(WRITER, password)]
        if os.getenv("MIRROR_DB_PASSWORD"):
            writers.append((MIRROR_WRITER, os.getenv("MIRROR_DB_PASSWORD")))
        for role, role_password in writers:
            ensure_role(cur, conn.info.dbname, role, role_password)
            for table in SITES + [HISTORY, "scrape_runs"]:
                cur.execute(sql.SQL("REVOKE ALL ON {} FROM {}").format(
                    sql.Identifier(table), sql.Identifier(role)))
                cur.execute(sql.SQL("GRANT SELECT, INSERT ON {} TO {}").format(
                    sql.Identifier(table), sql.Identifier(role)))
            print(f"role {role}: SELECT + INSERT only (no UPDATE, DELETE or TRUNCATE)", flush=True)

        # Read-only role for people and tools that query the data (shared as a URL)
        reader_password = os.getenv("READER_DB_PASSWORD")
        if reader_password:
            ensure_role(cur, conn.info.dbname, READER, reader_password)
            cur.execute(sql.SQL("ALTER ROLE {} SET default_transaction_read_only = on").format(
                sql.Identifier(READER)))
            cur.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}").format(
                sql.Identifier(READER)))
            cur.execute(sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES "
                                "TO {}").format(sql.Identifier(READER)))
            print(f"role {READER}: read-only (SELECT only, read-only sessions)", flush=True)

        # The dashboard's daily QA logs its results, and can do nothing else
        qa_password = os.getenv("QA_DB_PASSWORD")
        if qa_password:
            ensure_role(cur, conn.info.dbname, QA_WRITER, qa_password)
            cur.execute(sql.SQL("ALTER ROLE {} CONNECTION LIMIT 5").format(sql.Identifier(QA_WRITER)))
            cur.execute(sql.SQL("REVOKE ALL ON qa_checks FROM {}").format(sql.Identifier(QA_WRITER)))
            cur.execute(sql.SQL("GRANT INSERT ON qa_checks TO {}").format(sql.Identifier(QA_WRITER)))
            print(f"role {QA_WRITER}: INSERT into qa_checks only", flush=True)


if __name__ == "__main__":
    main()
