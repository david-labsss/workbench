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
  products, master_sync, listing_events,
  field_changes, run_misses          master table storage (common/master.py), used by the sites
                                     ops/storage.json switches to "both" or "master"; functions
                                     seed_master(site), master_rebuild(site, run), master_run(site, run)
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
        "CREATE INDEX IF NOT EXISTS prices_website_product_scraped_idx "
        "ON prices (website, product_id, scraped_at)",
        # the change list (change_log) by store and time
        "CREATE INDEX IF NOT EXISTS field_changes_website_scraped_idx "
        "ON field_changes (website, scraped_at)",
        "CREATE INDEX IF NOT EXISTS listing_events_website_scraped_idx "
        "ON listing_events (website, scraped_at)",
    ]
    stmts += [f"ALTER TABLE prices ADD COLUMN IF NOT EXISTS {n} {t}" for n, t in master.LOG_EXTRA]
    stmts += [f"ALTER TABLE scrape_runs ADD COLUMN IF NOT EXISTS {n} integer"
              for n in master.RUN_COUNTS]
    # set by convert_history(): the site's first converted run; from then on its whole history
    # is in the master history
    stmts.append("ALTER TABLE master_sync ADD COLUMN IF NOT EXISTS history_from timestamptz")

    # seed_master(site): the master table at the site's last saved full run, from the site's own
    # table: every product it ever had, with its latest values, listed when in that run. Steps of
    # SEED_CHUNK products (by product_id range): each only groups its range and reads each
    # product's latest row by the product_id index, so no step sorts the whole history (the
    # first version did, which needs temp disk the size of the table).
    vals_array = ", ".join(f"'{c}'" for c in vals)
    set_list = ", ".join(f"{_q(c)} = EXCLUDED.{_q(c)}" for c in vals)
    stmts.append("""
CREATE OR REPLACE FUNCTION seed_master(site text) RETURNS text LANGUAGE plpgsql AS $f$
DECLARE
    last_run text; last_at timestamptz; picks text; bounds text[]; nb int; cond text;
    n_listed bigint; n_all bigint;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('scrape:' || site, 0));
    SELECT r.run_id, r.scraped_at INTO last_run, last_at FROM scrape_runs r
     WHERE r.site = seed_master.site AND r.full_run AND r.status = 'saved'
     ORDER BY r.logged_at DESC LIMIT 1;
    IF last_run IS NULL THEN
        RAISE EXCEPTION 'no saved full run for %', site;
    END IF;
    -- A column the site's table doesn't have yet comes in empty
    SELECT string_agg(CASE WHEN EXISTS (
               SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = site AND column_name = c)
           THEN format('h.%I', c) ELSE 'NULL' END, ', ' ORDER BY i)
      INTO picks FROM unnest(ARRAY[@VALS_ARRAY@]) WITH ORDINALITY AS t(c, i);
    EXECUTE format('SELECT array_agg(product_id ORDER BY product_id) FROM (
        SELECT product_id, row_number() OVER (ORDER BY product_id) AS rn FROM %I
        WHERE run_id = %L AND full_run AND product_id IS NOT NULL) x WHERE rn %% @CHUNK@ = 0',
        site, last_run) INTO bounds;
    nb := coalesce(array_length(bounds, 1), 0);
    FOR i IN 0 .. nb LOOP
        cond := coalesce(concat_ws(' AND ',
                    CASE WHEN i > 0 THEN format('product_id >= %L', bounds[i]) END,
                    CASE WHEN i < nb THEN format('product_id < %L', bounds[i + 1]) END), '');
        IF cond = '' THEN cond := 'true'; END IF;
        EXECUTE format($q$
            INSERT INTO products (website, product_id, @VALS@, listed, first_seen_at,
                                  scraped_at, delisted_at, run_id)
            SELECT %L, h.product_id, %s, h.run_id = %L, l.first_seen, h.scraped_at,
                   CASE WHEN h.run_id = %L THEN NULL ELSE %L::timestamptz END, h.run_id
            FROM (SELECT product_id, max(scraped_at) AS last_seen, min(scraped_at) AS first_seen
                  FROM %I WHERE full_run AND product_id IS NOT NULL AND %s
                  GROUP BY product_id) l
            CROSS JOIN LATERAL (SELECT * FROM %I x WHERE x.product_id = l.product_id
                                  AND x.scraped_at = l.last_seen AND x.full_run LIMIT 1) h
            ON CONFLICT (website, product_id) DO UPDATE SET @SET_LIST@,
                listed = EXCLUDED.listed, first_seen_at = EXCLUDED.first_seen_at,
                scraped_at = EXCLUDED.scraped_at, delisted_at = EXCLUDED.delisted_at,
                run_id = EXCLUDED.run_id
            $q$, site, picks, last_run, last_run, last_at, site, cond, site);
    END LOOP;
    -- The starting point for rebuilding past runs: every product's listing and values as of
    -- the seed run (a re-seed adds a fresh starting point, older events stay)
    INSERT INTO listing_events (website, product_id, scraped_at, run_id, listed, price, stock_qty,
                                available)
    SELECT p.website, p.product_id, last_at, last_run, p.listed, p.price, p.stock_qty, p.available
    FROM products p WHERE p.website = site
    ON CONFLICT (website, product_id, scraped_at) DO UPDATE SET listed = EXCLUDED.listed,
        price = EXCLUDED.price, stock_qty = EXCLUDED.stock_qty, available = EXCLUDED.available,
        run_id = EXCLUDED.run_id;
    INSERT INTO master_sync (website, run_id, scraped_at) VALUES (site, last_run, last_at)
    ON CONFLICT (website) DO UPDATE SET run_id = EXCLUDED.run_id,
                                        scraped_at = EXCLUDED.scraped_at;
    SELECT count(*) FILTER (WHERE listed), count(*) INTO n_listed, n_all
      FROM products WHERE website = site;
    RETURN format('%s: master at run %s, %s listed, %s in all', site, last_run, n_listed, n_all);
END
$f$""".replace("@VALS_ARRAY@", vals_array).replace("@VALS@", ", ".join(_q(c) for c in vals))
                 .replace("@SET_LIST@", set_list).replace("@CHUNK@", str(SEED_CHUNK)))
    stmts.append("REVOKE EXECUTE ON FUNCTION seed_master(text) FROM PUBLIC")

    # master_rebuild(site, run): one saved full run, rebuilt from the master table. Listing: the
    # products whose latest listing_events row at that time says listed, minus the ones a partial
    # run missed (run_misses). Values: each field from the FIRST change after the run (its old
    # value), else the master row.
    #   A converted site (convert_history() set master_sync.history_from): every field's changes,
    #   price, stock and availability too, are in field_changes.
    #   Before that: field_changes for the plain fields; for price, stock and availability the
    #   change log's old_* values or a removal's listing_events row (and a blank <-> "no"
    #   availability from field_changes, when earlier).
    pivot = ",\n            ".join(
        f"bool_or(fc.field = '{c}') AS {_q('has_' + c)}, "
        f"max(fc.old_value) FILTER (WHERE fc.field = '{c}') AS {_q('old_' + c)}"
        for c in master.HISTORY_COLUMNS + ["available"])
    pivot += ",\n            max(fc.scraped_at) FILTER (WHERE fc.field = 'available') AS at_available"
    exprs = []
    for c in vals:
        t = column_type(c)
        if c in master.HISTORY_COLUMNS:
            exprs.append(f"CASE WHEN fcp.{_q('has_' + c)} THEN fcp.{_q('old_' + c)}::{t} "
                         f"ELSE m.{_q(c)} END")
        elif c == "available":
            exprs.append("CASE WHEN fcp.has_available AND (ev.product_id IS NULL "
                         "OR fcp.at_available < ev.scraped_at) THEN fcp.old_available::boolean "
                         "WHEN ev.product_id IS NOT NULL THEN ev.available ELSE m.available END")
        else:  # price, stock_qty
            exprs.append(f"CASE WHEN ev.product_id IS NOT NULL THEN ev.{_q(c)} ELSE m.{_q(c)} END")
    # converted sites: every value column from field_changes
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
BEGIN
    SELECT r.scraped_at INTO run_at FROM scrape_runs r
     WHERE r.site = master_rebuild.site AND r.run_id = master_rebuild.run AND r.full_run
       AND r.status = 'saved' ORDER BY r.logged_at DESC LIMIT 1;
    IF run_at IS NULL THEN
        RETURN;
    END IF;
    IF EXISTS (SELECT 1 FROM master_sync s WHERE s.website = master_rebuild.site
               AND s.history_from IS NOT NULL) THEN
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
        RETURN;
    END IF;
    RETURN QUERY
    WITH listed AS (
        SELECT DISTINCT ON (e.product_id) e.product_id, e.listed FROM listing_events e
        WHERE e.website = master_rebuild.site AND e.scraped_at <= run_at
        ORDER BY e.product_id, e.scraped_at DESC),
    fc AS (
        SELECT DISTINCT ON (f.product_id, f.field) f.product_id, f.field, f.old_value, f.scraped_at
        FROM field_changes f WHERE f.website = master_rebuild.site AND f.scraped_at > run_at
        ORDER BY f.product_id, f.field, f.scraped_at, f.id),
    fcp AS (
        SELECT fc.product_id,
            {pivot}
        FROM fc GROUP BY fc.product_id),
    ev AS (
        SELECT DISTINCT ON (x.product_id) x.* FROM (
            SELECT p.product_id, p.scraped_at, 1 AS src, p.id, p.old_price AS price,
                   p.old_stock_qty AS stock_qty, p.old_available AS available
            FROM prices p
            WHERE p.website = master_rebuild.site AND p.scraped_at > run_at AND p.full_run
            UNION ALL
            SELECT e.product_id, e.scraped_at, 0, 0::bigint, e.price, e.stock_qty, e.available
            FROM listing_events e
            WHERE e.website = master_rebuild.site AND e.scraped_at > run_at AND NOT e.listed) x
        ORDER BY x.product_id, x.scraped_at, x.src, x.id)
    SELECT run_at, m.website, m.product_id,
           {nl.join(exprs)},
           master_rebuild.run, true
    FROM products m
    JOIN listed l ON l.product_id = m.product_id AND l.listed
    LEFT JOIN fcp ON fcp.product_id = m.product_id
    LEFT JOIN ev ON ev.product_id = m.product_id
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

    # convert_history(site): the site's history written as master history, so master_rebuild()
    # gives every past run exactly from products + listing_events + field_changes + run_misses,
    # and the site's own table and the prices table can go. The runs in the site's own table up
    # to the master table's latest seed are replayed in order, as the master way saves a run:
    # each against every product's last values so far (a temp table): products joining the
    # listing (new, or back after being gone) and leaving it (listing_events), and every field a
    # product seen before changed, price, stock and availability included (field_changes). Run by
    # run, so the site's table is read once, each run's rows together (no lookups by product).
    # Then the master table's own log since the seed: the price, stock and availability changes
    # it kept only in prices, and those of products coming back (listing_events holds their
    # values). Sets master_sync.history_from; from then on master_rebuild() reads only the
    # master history for the site. Re-running adds nothing twice.
    col_types = ", ".join(f"'{column_type(c).replace(' NOT NULL', '')}'" for c in vals)
    stmts.append("""
CREATE OR REPLACE FUNCTION convert_history(site text) RETURNS text LANGUAGE plpgsql AS $f$
DECLARE
    mf timestamptz; n_runs int; first_at timestamptz; picks text; coldefs text; changes text;
    differ text; set_list text; rr record; got bigint;
    n_appear bigint := 0; n_gone bigint := 0; n_fields bigint := 0; n_log bigint; n_back bigint;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('scrape:' || site, 0));
    -- the latest seed: a time with a listing event for every product known then
    SELECT e.scraped_at INTO mf FROM (
        SELECT scraped_at, count(*) AS n FROM listing_events WHERE website = site GROUP BY 1) e
     WHERE e.n >= (SELECT count(*) FROM products p WHERE p.website = site
                   AND p.first_seen_at <= e.scraped_at)
     ORDER BY e.scraped_at DESC LIMIT 1;
    IF mf IS NULL THEN
        RAISE EXCEPTION 'no master table seed for %', site;
    END IF;
    -- the runs to convert: saved full runs up to the seed that the site's own table holds
    DROP TABLE IF EXISTS conv_runs;
    EXECUTE format($q$
        CREATE TEMP TABLE conv_runs AS
        SELECT run_id, run_at, row_number() OVER (ORDER BY run_at)::int AS rn FROM (
            SELECT DISTINCT ON (r.run_id) r.run_id, r.scraped_at AS run_at FROM scrape_runs r
            WHERE r.site = %L AND r.status = 'saved' AND r.full_run AND r.scraped_at <= %L
            ORDER BY r.run_id, r.logged_at DESC) x
        WHERE EXISTS (SELECT 1 FROM %I t WHERE t.run_id = x.run_id AND t.full_run)
        $q$, site, mf, site);
    SELECT count(*), min(run_at) INTO n_runs, first_at FROM conv_runs;
    IF NOT EXISTS (SELECT 1 FROM conv_runs WHERE run_at = mf) THEN
        RAISE EXCEPTION 'the seed run of % (%) is not in its own table', site, mf;
    END IF;
    -- the value columns as the site's table has them (NULL where it hasn't got one), their
    -- definitions, each field's old (s, the product's last values) and new (c, this run) value as
    -- text, whether any differs, and the update to the last values
    SELECT string_agg(CASE WHEN EXISTS (SELECT 1 FROM information_schema.columns
                               WHERE table_schema = 'public' AND table_name = site AND column_name = c)
                      THEN format('t.%I', c) ELSE format('NULL::%s', ty) END || format(' AS %I', c), ', ' ORDER BY ord),
           string_agg(format('%I %s', c, ty), ', ' ORDER BY ord),
           string_agg(format('(%L, s.%I::text, c.%I::text)', c, c, c), ', ' ORDER BY ord),
           format('(%s) IS DISTINCT FROM (%s)', string_agg(format('s.%I::text', c), ', ' ORDER BY ord),
                  string_agg(format('c.%I::text', c), ', ' ORDER BY ord)),
           string_agg(format('%I = c.%I', c, c), ', ' ORDER BY ord)
      INTO picks, coldefs, changes, differ, set_list
      FROM unnest(ARRAY[@VALS_ARRAY@], ARRAY[@COL_TYPES@]) WITH ORDINALITY AS u(c, ty, ord);
    DROP TABLE IF EXISTS conv_state;
    EXECUTE format('CREATE TEMP TABLE conv_state (product_id text PRIMARY KEY, %s, listed boolean NOT NULL)',
                   coldefs);
    FOR rr IN SELECT * FROM conv_runs ORDER BY rn LOOP
        DROP TABLE IF EXISTS conv_cur;
        EXECUTE format('CREATE TEMP TABLE conv_cur AS SELECT DISTINCT ON (t.product_id) t.product_id, %s
                        FROM %I t WHERE t.run_id = %L AND t.full_run AND t.product_id IS NOT NULL
                        ORDER BY t.product_id, t.id DESC', picks, site, rr.run_id);
        ALTER TABLE conv_cur ADD PRIMARY KEY (product_id);
        ANALYZE conv_cur;
        IF rr.run_at < mf THEN  -- the seed's own listing events hold its run
            INSERT INTO listing_events (website, product_id, scraped_at, run_id, listed, price,
                                        stock_qty, available)
            SELECT site, c.product_id, rr.run_at, rr.run_id, true, c.price, c.stock_qty, c.available
            FROM conv_cur c
            WHERE NOT EXISTS (SELECT 1 FROM conv_state s WHERE s.product_id = c.product_id AND s.listed)
            ON CONFLICT DO NOTHING;
            GET DIAGNOSTICS got = ROW_COUNT; n_appear := n_appear + got;
            INSERT INTO listing_events (website, product_id, scraped_at, run_id, listed, price,
                                        stock_qty, available)
            SELECT site, s.product_id, rr.run_at, rr.run_id, false, s.price, s.stock_qty, s.available
            FROM conv_state s
            WHERE s.listed AND NOT EXISTS (SELECT 1 FROM conv_cur c WHERE c.product_id = s.product_id)
            ON CONFLICT DO NOTHING;
            GET DIAGNOSTICS got = ROW_COUNT; n_gone := n_gone + got;
        END IF;
        EXECUTE format($q$
            INSERT INTO field_changes (website, product_id, scraped_at, run_id, field, old_value,
                                       new_value)
            SELECT %L, c.product_id, %L, %L, v.field, v.old_value, v.new_value
            FROM conv_cur c JOIN conv_state s USING (product_id)
            CROSS JOIN LATERAL (VALUES %s) AS v(field, old_value, new_value)
            WHERE %s AND v.old_value IS DISTINCT FROM v.new_value
              AND NOT EXISTS (SELECT 1 FROM field_changes f WHERE f.website = %L
                              AND f.product_id = c.product_id AND f.scraped_at = %L
                              AND f.field = v.field)
            $q$, site, rr.run_at, rr.run_id, changes, differ, site, rr.run_at);
        GET DIAGNOSTICS got = ROW_COUNT; n_fields := n_fields + got;
        -- every product's last values after this run
        EXECUTE format('UPDATE conv_state s SET %s, listed = true FROM conv_cur c
                        WHERE c.product_id = s.product_id AND (NOT s.listed OR %s)', set_list, differ);
        UPDATE conv_state s SET listed = false
        WHERE s.listed AND NOT EXISTS (SELECT 1 FROM conv_cur c WHERE c.product_id = s.product_id);
        INSERT INTO conv_state SELECT c.*, true FROM conv_cur c
        WHERE NOT EXISTS (SELECT 1 FROM conv_state s WHERE s.product_id = c.product_id);
    END LOOP;
    DROP TABLE conv_cur;
    DROP TABLE conv_state;
    -- Since the seed: price, stock and availability changes of listed products (the change
    -- log's old and new values; not its rows for new, returning or removed products, which
    -- have a listing event at the same time and no old values)
    INSERT INTO field_changes (website, product_id, scraped_at, run_id, field, old_value,
                               new_value)
    SELECT site, p.product_id, p.scraped_at, p.run_id, v.field, v.old_value, v.new_value
    FROM prices p
    CROSS JOIN LATERAL (VALUES ('price', p.old_price::text, p.price::text),
                               ('stock_qty', p.old_stock_qty::text, p.stock_qty::text),
                               ('available', p.old_available::text, p.available::text))
         AS v(field, old_value, new_value)
    WHERE p.website = site AND p.full_run AND p.scraped_at > mf
      AND v.old_value IS DISTINCT FROM v.new_value
      AND NOT EXISTS (SELECT 1 FROM listing_events e WHERE e.website = site
                      AND e.product_id = p.product_id AND e.scraped_at = p.scraped_at)
      AND NOT EXISTS (SELECT 1 FROM field_changes f WHERE f.website = site
                      AND f.product_id = p.product_id AND f.scraped_at = p.scraped_at
                      AND f.field = v.field);
    GET DIAGNOSTICS n_log = ROW_COUNT;
    -- Since the seed: a product coming back with another price, stock or availability than when
    -- it left (its listing event before holds those)
    INSERT INTO field_changes (website, product_id, scraped_at, run_id, field, old_value,
                               new_value)
    SELECT site, a.product_id, a.scraped_at, a.run_id, v.field, v.old_value, v.new_value
    FROM listing_events a
    CROSS JOIN LATERAL (SELECT b.price, b.stock_qty, b.available FROM listing_events b
                        WHERE b.website = site AND b.product_id = a.product_id
                          AND b.scraped_at < a.scraped_at
                        ORDER BY b.scraped_at DESC LIMIT 1) b
    CROSS JOIN LATERAL (VALUES ('price', b.price::text, a.price::text),
                               ('stock_qty', b.stock_qty::text, a.stock_qty::text),
                               ('available', b.available::text, a.available::text))
         AS v(field, old_value, new_value)
    WHERE a.website = site AND a.listed AND a.scraped_at > mf
      AND v.old_value IS DISTINCT FROM v.new_value
      AND NOT EXISTS (SELECT 1 FROM field_changes f WHERE f.website = site
                      AND f.product_id = a.product_id AND f.scraped_at = a.scraped_at
                      AND f.field = v.field);
    GET DIAGNOSTICS n_back = ROW_COUNT;
    UPDATE master_sync SET history_from = first_at WHERE website = site;
    DROP TABLE conv_runs;
    RETURN format('%s: %s runs from %s up to %s; %s appear, %s gone, %s field changes; since the '
                  'seed %s price/stock/availability changes, %s on coming back',
                  site, n_runs, first_at, mf, n_appear, n_gone, n_fields, n_log, n_back);
END
$f$""".replace("@VALS_ARRAY@", vals_array).replace("@COL_TYPES@", col_types))
    stmts.append("REVOKE EXECUTE ON FUNCTION convert_history(text) FROM PUBLIC")

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
FROM (
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
JOIN products m ON m.website = y.website AND m.product_id = y.product_id
LEFT JOIN LATERAL (
    SELECT {later}
    FROM (SELECT DISTINCT ON (f.field) f.field, f.old_value FROM field_changes f
          WHERE f.website = y.website AND f.product_id = y.product_id AND f.scraped_at > y.scraped_at
          ORDER BY f.field, f.scraped_at, f.id) f) l ON true
WHERE y.change IS NOT NULL""")

    # sites: each site's last full run (as create_sites_view), its changes counted from the
    # change list: change_log for a converted site, the prices table for the others
    stmts.append("""
CREATE OR REPLACE VIEW sites AS
WITH last AS (
    SELECT DISTINCT ON (site) site, run_id, scraped_at, rows
    FROM scrape_runs WHERE full_run AND status = 'saved'
    ORDER BY site, logged_at DESC)
SELECT l.site AS website,
       l.scraped_at AS last_scraped_at,
       l.rows AS products,
       CASE WHEN m.history_from IS NOT NULL
            THEN (SELECT count(*) FROM change_log c WHERE c.website = l.site AND c.scraped_at = l.scraped_at)
            ELSE (SELECT count(*) FROM prices p WHERE p.website = l.site AND p.run_id = l.run_id) END AS changes,
       CASE WHEN m.history_from IS NOT NULL
            THEN (SELECT count(*) FROM change_log c WHERE c.website = l.site AND c.scraped_at = l.scraped_at
                  AND c.restocked)
            ELSE (SELECT count(*) FROM prices p WHERE p.website = l.site AND p.run_id = l.run_id
                  AND p.restocked) END AS restocked,
       l.run_id AS last_run_id
FROM last l
LEFT JOIN master_sync m ON m.website = l.site
ORDER BY l.site""")

    # master_run(site, run): any saved full run of a site, whatever its storage: the site's own
    # rows when it has them (snapshot, both, and runs before the switch), else the rebuild
    cols_cast = ", ".join(f"{_q(c)}::{column_type(c).replace(' NOT NULL', '')}"
                          for c in PRODUCT_COLUMNS)
    stmts.append(f"""
CREATE OR REPLACE FUNCTION master_run(site text, run text) RETURNS TABLE ({returns})
LANGUAGE plpgsql STABLE AS $f$
BEGIN
    IF to_regclass(format('%I', site)) IS NOT NULL THEN
        RETURN QUERY EXECUTE format('SELECT {cols_cast}, run_id, full_run FROM %I
                                     WHERE run_id = %L AND full_run', site, run);
        IF FOUND THEN
            RETURN;
        END IF;
    END IF;
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
        # Master table storage (common/master.py): products, master_sync, listing_events,
        # field_changes, run_misses, the change log's extra columns and the seed/rebuild functions
        for stmt in master_storage_sql():
            cur.execute(stmt)
        print("master table storage: tables and functions (seed_master, master_rebuild, master_run)",
              flush=True)
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
            # Master storage: the master table and its sync point are updated in place (each
            # overwritten value is kept in field_changes / prices first); the logs are insert-only
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
