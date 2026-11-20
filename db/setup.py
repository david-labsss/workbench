"""Create or extend the database objects and the restricted roles. Safe to re-run.

Run by the "Set up database" workflow with the admin connection. It only CREATEs tables, ADDs
missing columns, (re)creates functions and views and GRANTs rights; it never creates a per-store
table or the old prices table (both are gone: the history is in the master tables below).

Tables (columns in common/schema.py):
  products, master_sync              the master table (common/master.py): each product's current
                                     values, and the run each store's master table is at
  listing_events, field_changes,
  run_misses                         the history: products joining / leaving a listing, every
                                     value a product changed, products a partial run missed
  scrape_runs                        one row per run (saved, or refused with the reason)
  qa_checks                          the dashboard's daily QA: a sample of each new full run
                                     fetched again from the store and compared, one row a product
Views and functions (master_storage_sql): sites (each store's last full run), change_log (the
change list worked out from the history), master_run(site, run) (any past run),
master_product_runs(site, product), seed_master(site), convert_history(site), product_last_seen.

Roles:
  scraper_writer         the scrapers: add rows to scrape_runs and the history, update products
                         and master_sync (MASTER_RIGHTS); nothing else
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

from common.db import column_type
from common.schema import PRODUCT_COLUMNS

WRITER = "scraper_writer"
MIRROR_WRITER = "scraper_writer_mirror"
READER = "scraper_reader"
QA_WRITER = "qa_writer"


# ---------- Master table storage (common/master.py)
# Shanzay created products, master_sync, listing_events and seed_master() by hand on 2026-11-02;
# they are kept here, matching the live database, so they can be reviewed and created again.
SEED_CHUNK = 5000  # products of the last run per seed step
MASTER_RIGHTS = {"products": "SELECT, INSERT, UPDATE", "master_sync": "SELECT, INSERT, UPDATE",
                 "listing_events": "SELECT, INSERT", "field_changes": "SELECT, INSERT",
                 "run_misses": "SELECT, INSERT"}


def _q(name):
    return '"' + name + '"'


def master_storage_sql():
    """Every master storage object as SQL statements, in order. Safe to re-run: tables and
    columns are only added, functions replaced."""
    from common import master  # noqa: PLC0415 - only needed here

    vals = master.VALUE_COLUMNS
    returns = ", ".join(f"{_q(c)} {column_type(c).replace(' NOT NULL', '')}"
                        for c in PRODUCT_COLUMNS) + ", run_id text, full_run boolean"
    stmts = [
        "CREATE TABLE IF NOT EXISTS products (\n    website text NOT NULL,\n    "
        "product_id text NOT NULL,\n    "
        + ",\n    ".join(f"{_q(c)} {column_type(c)}" for c in vals)
        + ",\n    listed boolean NOT NULL DEFAULT true,\n    first_seen_at timestamptz NOT NULL,"
          "\n    scraped_at timestamptz NOT NULL,\n    delisted_at timestamptz,\n    run_id text,"
          "\n    PRIMARY KEY (website, product_id))",
        "CREATE INDEX IF NOT EXISTS products_website_listed_idx ON products (website) WHERE listed",
        "CREATE INDEX IF NOT EXISTS products_part_number_idx ON products (part_number)",
        "CREATE INDEX IF NOT EXISTS products_scraped_at_idx ON products (scraped_at)",
        "CREATE TABLE IF NOT EXISTS master_sync (website text PRIMARY KEY, run_id text NOT NULL, "
        "scraped_at timestamptz NOT NULL)",
        "CREATE TABLE IF NOT EXISTS listing_events (website text NOT NULL, product_id text NOT NULL, "
        "scraped_at timestamptz NOT NULL, run_id text, listed boolean NOT NULL, price numeric, "
        "stock_qty numeric, available boolean, PRIMARY KEY (website, product_id, scraped_at))",
        "CREATE TABLE IF NOT EXISTS field_changes (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
        "website text NOT NULL, product_id text NOT NULL, scraped_at timestamptz NOT NULL, "
        "run_id text, field text NOT NULL, old_value text, new_value text)",
        "CREATE INDEX IF NOT EXISTS field_changes_product_idx "
        "ON field_changes (website, product_id, scraped_at)",
        "CREATE TABLE IF NOT EXISTS run_misses (website text NOT NULL, product_id text NOT NULL, "
        "scraped_at timestamptz NOT NULL, run_id text, PRIMARY KEY (website, scraped_at, product_id))",
        # the change list (change_log) by store and time
        "CREATE INDEX IF NOT EXISTS field_changes_website_scraped_idx "
        "ON field_changes (website, scraped_at)",
        "CREATE INDEX IF NOT EXISTS listing_events_website_scraped_idx "
        "ON listing_events (website, scraped_at)",
    ]
    stmts += [f"ALTER TABLE scrape_runs ADD COLUMN IF NOT EXISTS {n} integer"
              for n in master.RUN_COUNTS]
    # set by convert_history(): the site's first converted run; from then on its whole history
    # is in the master history
    stmts.append("ALTER TABLE master_sync ADD COLUMN IF NOT EXISTS history_from timestamptz")

    # master_rebuild(site, run): one saved full run, rebuilt from the master history. Listing: the
    # products whose latest listing_events row at that time says listed, minus the ones a partial
    # run missed (run_misses). Values: each field from the FIRST change after the run (its old
    # value, field_changes), else the master row. Runs before master_sync.history_from (the
    # history kept, trim_history()) and sites without it give nothing.
    pivot_all = ",\n            ".join(
        f"bool_or(fc.field = '{c}') AS {_q('has_' + c)}, "
        f"max(fc.old_value) FILTER (WHERE fc.field = '{c}') AS {_q('old_' + c)}" for c in vals)
    exprs_all = [f"CASE WHEN fcp.{_q('has_' + c)} THEN fcp.{_q('old_' + c)}::{column_type(c)} "
                 f"ELSE m.{_q(c)} END" for c in vals]
    nl = "," + chr(10) + "           "
    stmts.append(f"""
CREATE OR REPLACE FUNCTION master_rebuild(site text, run text) RETURNS TABLE ({returns})
LANGUAGE plpgsql STABLE AS $f$
#variable_conflict use_column
DECLARE
    run_at timestamptz;
    hf timestamptz;
BEGIN
    SELECT r.scraped_at INTO run_at FROM scrape_runs r
     WHERE r.site = master_rebuild.site AND r.run_id = master_rebuild.run AND r.full_run
       AND r.status = 'saved' ORDER BY r.logged_at DESC LIMIT 1;
    IF run_at IS NULL THEN
        RETURN;
    END IF;
    SELECT s.history_from INTO hf FROM master_sync s WHERE s.website = master_rebuild.site;
    IF hf IS NULL OR run_at < hf THEN  -- before the history kept (trim_history): not rebuilt
        RETURN;
    END IF;
    RETURN QUERY
    WITH listed AS (
        SELECT DISTINCT ON (e.product_id) e.product_id, e.listed FROM listing_events e
        WHERE e.website = master_rebuild.site AND e.scraped_at <= run_at
        ORDER BY e.product_id, e.scraped_at DESC),
    fc AS (
        SELECT DISTINCT ON (f.product_id, f.field) f.product_id, f.field, f.old_value
        FROM field_changes f WHERE f.website = master_rebuild.site AND f.scraped_at > run_at
        ORDER BY f.product_id, f.field, f.scraped_at, f.id),
    fcp AS (
        SELECT fc.product_id,
        {pivot_all}
        FROM fc GROUP BY fc.product_id)
    SELECT run_at, m.website, m.product_id,
           {nl.join(exprs_all)},
           master_rebuild.run, true
    FROM products m
    JOIN listed l ON l.product_id = m.product_id AND l.listed
    LEFT JOIN fcp ON fcp.product_id = m.product_id
    WHERE m.website = master_rebuild.site
      AND NOT EXISTS (SELECT 1 FROM run_misses r WHERE r.website = m.website
                      AND r.product_id = m.product_id AND r.scraped_at = run_at);
END
$f$""")

    # master_product_runs(site, product): one product of a converted site in every saved full run
    # that had it, as master_rebuild() gives it, for a product's history without rebuilding whole
    # runs. Empty for a site that isn't converted yet.
    stmts.append(f"""
CREATE OR REPLACE FUNCTION master_product_runs(site text, product text) RETURNS TABLE ({returns})
LANGUAGE plpgsql STABLE AS $f$
#variable_conflict use_column
BEGIN
    IF NOT EXISTS (SELECT 1 FROM master_sync s WHERE s.website = master_product_runs.site
                   AND s.history_from IS NOT NULL) THEN
        RETURN;
    END IF;
    RETURN QUERY
    WITH runs AS (
        SELECT DISTINCT ON (r.run_id) r.run_id, r.scraped_at AS run_at FROM scrape_runs r
        WHERE r.site = master_product_runs.site AND r.full_run AND r.status = 'saved'
          AND r.scraped_at >= (SELECT s.history_from FROM master_sync s
                               WHERE s.website = master_product_runs.site)
        ORDER BY r.run_id, r.logged_at DESC),
    les AS MATERIALIZED (
        SELECT e.scraped_at, e.listed FROM listing_events e
        WHERE e.website = master_product_runs.site AND e.product_id = master_product_runs.product),
    fcs AS MATERIALIZED (
        SELECT f.field, f.old_value, f.scraped_at, f.id FROM field_changes f
        WHERE f.website = master_product_runs.site AND f.product_id = master_product_runs.product)
    SELECT runs.run_at, m.website, m.product_id,
           {nl.join(exprs_all)},
           runs.run_id, true
    FROM runs
    JOIN products m ON m.website = master_product_runs.site
                   AND m.product_id = master_product_runs.product
    CROSS JOIN LATERAL (SELECT les.listed FROM les WHERE les.scraped_at <= runs.run_at
                        ORDER BY les.scraped_at DESC LIMIT 1) l
    LEFT JOIN LATERAL (
        SELECT {pivot_all}
        FROM (SELECT DISTINCT ON (fcs.field) fcs.field, fcs.old_value FROM fcs
              WHERE fcs.scraped_at > runs.run_at ORDER BY fcs.field, fcs.scraped_at, fcs.id) fc
        ) fcp ON true
    WHERE l.listed
      AND NOT EXISTS (SELECT 1 FROM run_misses r WHERE r.website = m.website
                      AND r.product_id = m.product_id AND r.scraped_at = runs.run_at)
    ORDER BY runs.run_at;
END
$f$""")

    # change_log: the change list the prices table used to hold, worked out from the master
    # history of converted sites (master_sync.history_from set), with the same columns and codes:
    #   price / stock / out_of_stock / back_in_stock: price, stock and availability changes of a
    #     listed product (field_changes), as common/master.py logged them (a listing event that
    #     only repeats the product's state, as a seed's do, is no join or removal);
    #   new / back_in_stock: a product joining the listing (listing_events) that wasn't listed
    #     before (back_in_stock when seen before and in stock now), except at the site's first run;
    #   out_of_stock with available false: a listed, in-stock product leaving the listing.
    # Every other column as the product was at that time (the first change after it, else the
    # master row). old_price, old_stock_qty and old_available as common/master.py wrote them.
    view_cols = ["parent_id", "variant_id", "sku", "part_number", "model_number", "title", "brand",
                 "category", "condition", "price", "available", "stock_qty", "url"]
    later = ",\n                   ".join(
        f"bool_or(f.field = '{c}') AS {_q('has_' + c)}, "
        f"max(f.old_value) FILTER (WHERE f.field = '{c}') AS {_q('old_' + c)}" for c in view_cols)
    at = {c: f"CASE WHEN l.{_q('has_' + c)} THEN l.{_q('old_' + c)}::{column_type(c)} "
             f"ELSE m.{_q(c)} END" for c in view_cols}
    # change_codes: the change list's rows and codes only (cheap to count: the sites view), and
    # change_log: the same rows with every column, as the prices table had them
    stmts.append("""
CREATE OR REPLACE VIEW change_codes AS
SELECT * FROM (
SELECT x.website, x.product_id, x.scraped_at, x.run_id, x.kind, x.p_changed, x.p_old,
       x.s_changed, x.s_old, x.a_changed, x.a_old,
       CASE
         WHEN x.kind = 0 THEN CASE WHEN NOT x.joined_or_left THEN nullif(concat_ws('+',
              CASE WHEN x.p_changed THEN 'price' END,
              CASE WHEN x.s_changed THEN 'stock' END,
              CASE WHEN x.a_changed AND coalesce(x.a_old::boolean, false)
                                        <> coalesce(x.a_new::boolean, false)
                   THEN CASE WHEN x.a_new::boolean THEN 'back_in_stock' ELSE 'out_of_stock' END
              END), '') END
         WHEN x.kind = 1 THEN CASE WHEN prev.listed IS NOT TRUE
                                    AND NOT (prev.listed IS NULL AND x.scraped_at <= s.history_from)
              THEN CASE WHEN prev.listed IS NOT NULL AND coalesce(x.a_new::boolean, false)
                        THEN 'back_in_stock' ELSE 'new' END END
         ELSE CASE WHEN prev.listed AND coalesce(x.a_new::boolean, false) THEN 'out_of_stock' END
       END AS change
FROM (
    SELECT f.website, f.product_id, f.scraped_at, min(f.run_id) AS run_id, 0 AS kind,
           bool_or(f.field = 'price') AS p_changed,
           max(f.old_value) FILTER (WHERE f.field = 'price') AS p_old,
           bool_or(f.field = 'stock_qty') AS s_changed,
           max(f.old_value) FILTER (WHERE f.field = 'stock_qty') AS s_old,
           bool_or(f.field = 'available') AS a_changed,
           max(f.old_value) FILTER (WHERE f.field = 'available') AS a_old,
           max(f.new_value) FILTER (WHERE f.field = 'available') AS a_new,
           EXISTS (SELECT 1 FROM listing_events e WHERE e.website = f.website
                   AND e.product_id = f.product_id AND e.scraped_at = f.scraped_at
                   AND e.listed IS DISTINCT FROM (
                       SELECT b.listed FROM listing_events b WHERE b.website = e.website
                         AND b.product_id = e.product_id AND b.scraped_at < e.scraped_at
                       ORDER BY b.scraped_at DESC LIMIT 1)) AS joined_or_left
    FROM field_changes f
    WHERE f.field IN ('price', 'stock_qty', 'available')
    GROUP BY f.website, f.product_id, f.scraped_at
    UNION ALL
    SELECT e.website, e.product_id, e.scraped_at, e.run_id, CASE WHEN e.listed THEN 1 ELSE 2 END,
           NULL, NULL, NULL, NULL, NULL, NULL, e.available::text, NULL
    FROM listing_events e
) x
JOIN master_sync s ON s.website = x.website AND s.history_from IS NOT NULL
LEFT JOIN LATERAL (
    SELECT b.listed FROM listing_events b
    WHERE x.kind > 0 AND b.website = x.website AND b.product_id = x.product_id
      AND b.scraped_at < x.scraped_at
    ORDER BY b.scraped_at DESC LIMIT 1) prev ON true
) y
WHERE y.change IS NOT NULL""")

    stmts.append(f"""
CREATE OR REPLACE VIEW change_log AS
SELECT hashtextextended(y.website || ':' || y.product_id || ':' || y.scraped_at::text, 0) AS id,
       y.scraped_at, y.website, y.product_id,
       {", ".join(f"{at[c]} AS {_q(c)}" for c in view_cols[:9])},
       CASE WHEN y.kind = 0 AND y.p_changed THEN y.p_old::numeric
            WHEN y.kind = 1 THEN NULL ELSE {at["price"]} END AS old_price,
       {at["price"]} AS price,
       CASE WHEN y.kind = 2 THEN false ELSE {at["available"]} END AS available,
       {at["stock_qty"]} AS stock_qty,
       y.change, y.change LIKE '%back_in_stock%' AS restocked,
       {at["url"]} AS url, y.run_id, true AS full_run,
       CASE WHEN y.kind = 0 AND y.s_changed THEN y.s_old::numeric
            WHEN y.kind = 1 THEN NULL ELSE {at["stock_qty"]} END AS old_stock_qty,
       CASE WHEN y.kind = 0 AND y.a_changed THEN y.a_old::boolean
            WHEN y.kind = 1 THEN NULL ELSE {at["available"]} END AS old_available
FROM change_codes y
JOIN products m ON m.website = y.website AND m.product_id = y.product_id
LEFT JOIN LATERAL (
    SELECT {later}
    FROM (SELECT DISTINCT ON (f.field) f.field, f.old_value FROM field_changes f
          WHERE f.website = y.website AND f.product_id = y.product_id AND f.scraped_at > y.scraped_at
          ORDER BY f.field, f.scraped_at, f.id) f) l ON true""")

    # sites: each site's last full run (from scrape_runs), its changes and restocks counted from
    # the change list (change_codes)
    stmts.append("""
CREATE OR REPLACE VIEW sites AS
WITH last AS (
    SELECT DISTINCT ON (site) site, run_id, scraped_at, rows
    FROM scrape_runs WHERE full_run AND status = 'saved'
    ORDER BY site, logged_at DESC)
SELECT l.site AS website,
       l.scraped_at AS last_scraped_at,
       l.rows AS products,
       kc.changes,
       kc.restocked,
       l.run_id AS last_run_id
FROM last l
CROSS JOIN LATERAL (
    SELECT count(*) AS changes, count(*) FILTER (WHERE c.change LIKE '%back_in_stock%') AS restocked
    FROM change_codes c
    WHERE c.website = l.site AND c.scraped_at = l.scraped_at) kc
ORDER BY l.site""")

    # trim_history(site, keep): the history is kept for `keep` (3 months): what only older runs
    # need is deleted. The cutoff is the site's last saved full run at or before now() - keep;
    # every run from it on still rebuilds exactly: field_changes up to the cutoff go (a run
    # needs only the changes after it), each product keeps its latest listing event at or
    # before the cutoff (its listing then) and loses the older ones, run_misses before the
    # cutoff go, and master_sync.history_from moves to the cutoff (master_rebuild() returns
    # nothing for runs before it; change_log starts after it). common/master.py calls it at the
    # end of each save, so it runs every night per store. Owned by the admin (SECURITY DEFINER):
    # the scraper roles may call it but can't delete anything themselves. Converted sites only.
    stmts.append("""
CREATE OR REPLACE FUNCTION trim_history(site text, keep interval DEFAULT interval '3 months')
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $f$
DECLARE
    hf timestamptz; cutoff timestamptz; n_fields bigint; n_events bigint; n_misses bigint;
BEGIN
    SELECT s.history_from INTO hf FROM master_sync s WHERE s.website = trim_history.site;
    SELECT max(r.scraped_at) INTO cutoff FROM scrape_runs r
     WHERE r.site = trim_history.site AND r.status = 'saved' AND r.full_run
       AND r.scraped_at <= now() - keep;
    IF hf IS NULL OR cutoff IS NULL OR cutoff <= hf THEN
        RETURN 'nothing to trim';
    END IF;
    DELETE FROM field_changes f WHERE f.website = trim_history.site AND f.scraped_at <= cutoff;
    GET DIAGNOSTICS n_fields = ROW_COUNT;
    DELETE FROM listing_events e
     WHERE e.website = trim_history.site AND e.scraped_at < cutoff
       AND EXISTS (SELECT 1 FROM listing_events n WHERE n.website = e.website
                   AND n.product_id = e.product_id AND n.scraped_at > e.scraped_at
                   AND n.scraped_at <= cutoff);
    GET DIAGNOSTICS n_events = ROW_COUNT;
    DELETE FROM run_misses m WHERE m.website = trim_history.site AND m.scraped_at < cutoff;
    GET DIAGNOSTICS n_misses = ROW_COUNT;
    UPDATE master_sync SET history_from = cutoff WHERE website = trim_history.site;
    RETURN format('%s: history from %s; deleted %s field changes, %s listing events, %s run misses',
                  site, cutoff, n_fields, n_events, n_misses);
END
$f$""")
    stmts.append("REVOKE ALL ON FUNCTION trim_history(text, interval) FROM PUBLIC")
    stmts += [f"DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{r}') THEN "
              f"GRANT EXECUTE ON FUNCTION trim_history(text, interval) TO {r}; END IF; END $$"
              for r in (WRITER, MIRROR_WRITER)]

    # master_run(site, run): any saved full run of a site within the history kept, rebuilt from
    # the master history (the name the readers use; master_rebuild() does the work)
    stmts.append(f"""
CREATE OR REPLACE FUNCTION master_run(site text, run text) RETURNS TABLE ({returns})
LANGUAGE plpgsql STABLE AS $f$
BEGIN
    RETURN QUERY SELECT * FROM master_rebuild(site, run);
END
$f$""")

    # product_last_seen(site, product): when a run last found the product on the store. There is
    # no column for it on purpose: stamping every product on every run would rewrite every row,
    # which is what the master table avoids. A product row's scraped_at is always a time it was
    # found (seeded at its last time in the site's table; set again when it is new, changed or
    # back). After that it was found by every later master run (scrape_runs rows with counts)
    # until its removal, except the partial runs that missed it (run_misses).
    stmts.append("""
CREATE OR REPLACE FUNCTION product_last_seen(site text, product text) RETURNS timestamptz
LANGUAGE sql STABLE AS $f$
    SELECT greatest(p.scraped_at, (
        SELECT max(r.scraped_at) FROM scrape_runs r
        WHERE r.site = p.website AND r.full_run AND r.status = 'saved'
          AND r.unchanged_products IS NOT NULL
          AND r.scraped_at > p.scraped_at
          AND (p.listed OR r.scraped_at < p.delisted_at)
          AND NOT EXISTS (SELECT 1 FROM run_misses m WHERE m.website = p.website
                          AND m.scraped_at = r.scraped_at AND m.product_id = p.product_id)))
    FROM products p
    WHERE p.website = product_last_seen.site AND p.product_id = product_last_seen.product
$f$""")
    return stmts


def ensure_role(cur, dbname, role, password):
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
    verb = "ALTER" if cur.fetchone() else "CREATE"
    cur.execute(sql.SQL(verb + " ROLE {} LOGIN PASSWORD {}").format(
        sql.Identifier(role), sql.Literal(password)))
    cur.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
        sql.Identifier(dbname), sql.Identifier(role)))
    cur.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(role)))


def main():
    admin_url = os.getenv("DATABASE_ADMIN_URL")
    password = os.getenv("SCRAPER_DB_PASSWORD")
    if not admin_url or not password:
        sys.exit("DATABASE_ADMIN_URL and SCRAPER_DB_PASSWORD must be set")

    with psycopg.connect(admin_url, connect_timeout=30) as conn, conn.cursor() as cur:
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
        # The master table, its history, the sites and change_log views, and the functions
        for stmt in master_storage_sql():
            cur.execute(stmt)
        print("master table storage: tables, views and functions", flush=True)

        # Restricted roles for the scrapers (the mirror role for the scrapers on the other
        # GitHub accounts): scrape_runs and the history insert-only
        writers = [(WRITER, password)]
        if os.getenv("MIRROR_DB_PASSWORD"):
            writers.append((MIRROR_WRITER, os.getenv("MIRROR_DB_PASSWORD")))
        for role, role_password in writers:
            ensure_role(cur, conn.info.dbname, role, role_password)
            cur.execute(sql.SQL("REVOKE ALL ON scrape_runs FROM {}").format(sql.Identifier(role)))
            cur.execute(sql.SQL("GRANT SELECT, INSERT ON scrape_runs TO {}").format(
                sql.Identifier(role)))
            # The master table and its sync point are updated in place (each overwritten value is
            # kept in field_changes / listing_events first); the history is insert-only
            for table, rights in MASTER_RIGHTS.items():
                cur.execute(sql.SQL("REVOKE ALL ON {} FROM {}").format(
                    sql.Identifier(table), sql.Identifier(role)))
                cur.execute(sql.SQL("GRANT {} ON {} TO {}").format(
                    sql.SQL(rights), sql.Identifier(table), sql.Identifier(role)))
            print(f"role {role}: SELECT + INSERT only (no UPDATE, DELETE or TRUNCATE), plus UPDATE "
                  "on products and master_sync", flush=True)

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
