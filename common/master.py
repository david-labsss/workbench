"""Master table storage: one row per product per site, changed only when the product changes.

  products     ONE table for all sites, keyed by (website, product_id): each product's current
               values. A run UPDATEs only the products whose values changed and INSERTs new
               ones. Products gone from the listing get listed = false; nothing is deleted.
  prices       the change log, as before, now also with old_stock_qty and old_available.
               Insert only.
  scrape_runs  one row per run, now also with how many products were new, changed, removed
               and unchanged.
  listing_events  one row each time a product joins or leaves a site's listing, with its
               values then. Insert only.
  field_changes   one row per field a product changed, other than price, stock and
               availability (the change log keeps those; a blank <-> "no" availability, which
               the change log treats alike, is kept here): its old and new value. So nothing
               the master table overwrites is lost. Insert only.
  run_misses   the listed products a partial run (under 90% of the listing) did not see. They
               stay listed, as in common/changes.py; this only lets that run be rebuilt.

master_run(site, run) in the database (db/setup.py) gives any run since the site's last
seed_master(): its listing from listing_events minus run_misses, and each value from the first
change after the run (field_changes, the change log's old_* values, a removal's listing_events
row), else the master row. Runs before the seed are in the site's own table, which
seed_master() needs anyway: it builds the master from it, and a re-seed overwrites master rows
without logging them. product_last_seen(site, product) gives when a run last found a product
(a product row's scraped_at is when it last changed, not when it was last seen).

Per site, ops/storage.json says how a run saves (common/runner.py reads it):
  snapshot  the old way: every run's full snapshot into the site's own table (default)
  both      the master table and the change log, plus the snapshot as before, so the
            dashboard keeps working while the master is checked
  master    the master table and the change log only; no snapshot rows

Everything a run saves (master changes, change log rows, run totals, and the snapshot in
"both") is written in ONE transaction, so a failed save leaves nothing behind. One lock per
site stops two runs of the same site from saving at the same time, and a run older than the
site's last saved run is refused, so stale data never overwrites newer data.
"""
import json
import os

from psycopg import sql

from common import db, schema

COMPARE_LISTING = 0.9  # as common/changes.py: removals only count when the listing is this full
TABLE = "products"
CONFIG = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ops",
                      "storage.json")

# Columns kept on the master row (all product columns but scraped_at, which is when the row
# last changed)
VALUE_COLUMNS = [c for c in schema.PRODUCT_COLUMNS if c not in ("scraped_at", "website",
                                                                "product_id")]
LOG_EXTRA = [("old_stock_qty", "numeric"), ("old_available", "boolean")]
RUN_COUNTS = ["new_products", "changed_products", "removed_products", "unchanged_products"]
# Fields whose old values the change log (prices) already keeps; every other value column goes to
# field_changes when it changes
LOGGED_IN_PRICES = ("price", "stock_qty", "available")
HISTORY_COLUMNS = [c for c in VALUE_COLUMNS if c not in LOGGED_IN_PRICES]


class Refused(Exception):
    """The run looks wrong; nothing was saved."""


class NotSynced(Refused):
    """The master table is not at the site's last saved run (it needs seed_master()); nothing
    was saved. common/runner.py then saves the run the old way, so the run is not lost."""


def config():
    try:
        with open(CONFIG) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def mode(site):
    """snapshot, both or master. An env var STORAGE_MODE overrides the file (tests)."""
    m = os.getenv("STORAGE_MODE") or config().get("sites", {}).get(site) \
        or config().get("default", "snapshot")
    if m not in ("snapshot", "both", "master"):
        raise ValueError(f"unknown storage mode {m!r} for {site}")
    return m


def _ident(names):
    return sql.SQL(", ").join(sql.Identifier(n) for n in names)


def _copy(cur, table, columns, rows):
    with cur.copy(sql.SQL("COPY {} ({}) FROM STDIN").format(sql.Identifier(table),
                                                            _ident(columns))) as copy:
        for row in rows:
            copy.write_row(row)


def save(site, rows, scraped_dt, products, full_run, snapshot):
    """Save one run the master way. rows: this run in PRODUCT_COLUMNS order. snapshot: also
    insert the full snapshot into the site's table ("both" mode). Returns the run's counts
    {new, changed, removed, unchanged, restocked, changes, field_changes}. Raises Refused when
    a rule stops the run, or any database error; either way nothing is saved."""
    run_id = os.getenv("GITHUB_RUN_ID", "local")
    cols = schema.PRODUCT_COLUMNS
    # Same conversion as the snapshot path, so both store identical values
    converted = [[db.to_db(c, v, scraped_dt) for c, v in zip(cols, row)] for row in rows]

    with db.connect() as conn, conn.cursor() as cur:
        # One lock per site, held until commit
        cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"scrape:{site}",))

        # Test runs never touch the master table or the change log
        if not full_run:
            if snapshot:
                _copy(cur, site, cols + ["run_id", "full_run"],
                      (r + [run_id, False] for r in converted))
            db.log_run_cur(cur, site, scraped_dt, run_id, "saved", len(rows), products, False,
                           "test run: master table not touched")
            return {"new": 0, "changed": 0, "removed": 0, "unchanged": 0, "restocked": 0,
                    "changes": {}, "field_changes": 0, "missed": 0}

        last = cur.execute(
            "SELECT scraped_at, rows, run_id FROM scrape_runs WHERE site = %s AND full_run "
            "AND status = 'saved' ORDER BY logged_at DESC LIMIT 1", (site,)).fetchone()
        if last and last[0] >= scraped_dt:
            raise Refused(f"a newer run ({last[0]}) is already saved for {site}")
        # The master must hold exactly the site's last saved run, or every difference would
        # be logged twice (or missed). seed_master() sets this point; every save moves it.
        synced = cur.execute("SELECT run_id FROM master_sync WHERE website = %s",
                             (site,)).fetchone()
        if last and (not synced or synced[0] != last[2]):
            raise NotSynced(f"the master table is not at {site}'s last saved run "
                            f"({last[2]}); run SELECT seed_master('{site}') first")

        # This run in a temp table, typed like the product tables. A product without an id
        # can't be keyed in the master table: it stays in the snapshot only (as the old way).
        pid = cols.index("product_id")
        keyed = [r for r in converted if r[pid] is not None]
        cur.execute(sql.SQL("CREATE TEMP TABLE cur ({}) ON COMMIT DROP").format(
            sql.SQL(", ").join(sql.SQL("{} {}").format(
                sql.Identifier(c), sql.SQL(db.column_type(c).replace(" NOT NULL", "")))
                for c in cols)))
        _copy(cur, "cur", cols, keyed)
        cur.execute("ALTER TABLE cur ADD PRIMARY KEY (product_id)")
        cur.execute("ANALYZE cur")

        # Classify every product against the master: n = new, r = returning (listed = false),
        # c = values changed, t = a tracked change (price, stock, availability), x = removed
        p = {c: sql.SQL("p.{}").format(sql.Identifier(c)) for c in VALUE_COLUMNS}
        q = {c: sql.SQL("c.{}").format(sql.Identifier(c)) for c in VALUE_COLUMNS}
        values_differ = sql.SQL("({}) IS DISTINCT FROM ({})").format(
            sql.SQL(", ").join(q.values()), sql.SQL(", ").join(p.values()))
        cur.execute(sql.SQL("""
            CREATE TEMP TABLE diff ON COMMIT DROP AS
            SELECT c.product_id,
                   p.product_id IS NULL AS is_new,
                   p.product_id IS NOT NULL AND NOT p.listed AS is_returning,
                   p.product_id IS NOT NULL AND p.listed AND {differ} AS changed,
                   p.product_id IS NOT NULL AND p.listed
                     AND c.price IS DISTINCT FROM p.price AS price_changed,
                   concat_ws('+',
                     CASE WHEN c.price IS DISTINCT FROM p.price THEN 'price' END,
                     CASE WHEN c.stock_qty IS DISTINCT FROM p.stock_qty THEN 'stock' END,
                     CASE WHEN coalesce(c.available, false) <> coalesce(p.available, false)
                          THEN CASE WHEN c.available THEN 'back_in_stock'
                                    ELSE 'out_of_stock' END END) AS tracked,
                   p.price AS old_price, p.stock_qty AS old_stock_qty,
                   p.available AS old_available
            FROM cur c
            LEFT JOIN {master} p ON p.website = %s AND p.product_id = c.product_id
            """).format(differ=values_differ, master=sql.Identifier(TABLE)), (site,))

        listed_before = cur.execute(
            sql.SQL("SELECT count(*) FROM {} WHERE website = %s AND listed").format(
                sql.Identifier(TABLE)), (site,)).fetchone()[0]
        compare_listing = len(keyed) >= COMPARE_LISTING * listed_before

        # Change log rows (the same rules as common/changes.py)
        log_cols = schema.PRICE_COLUMNS + [n for n, _ in LOG_EXTRA] + ["run_id", "full_run"]
        def log_select(change, old_price, old_stock, old_avail, source="c", available=None):
            src = {c: sql.SQL("{}.{}").format(sql.Identifier(source), sql.Identifier(c))
                   for c in cols}
            out = []
            for col in schema.PRICE_COLUMNS:
                if col == "scraped_at":
                    out.append(sql.Literal(scraped_dt))
                elif col == "website":
                    out.append(sql.Literal(site))
                elif col == "change":
                    out.append(change)
                elif col == "restocked":
                    out.append(sql.SQL("({}) LIKE '%%back_in_stock%%'").format(change))
                elif col == "old_price":
                    out.append(old_price)
                elif col == "available" and available is not None:
                    out.append(available)
                else:
                    out.append(src[col])
            out += [old_stock, old_avail, sql.Literal(run_id), sql.Literal(True)]
            return sql.SQL(", ").join(out)

        # 1. new and returning products: back_in_stock when seen before and in stock now,
        #    otherwise new (as common/changes.py). A site's first saved run logs nothing (as
        #    common/changes.py, which has no previous run to compare with).
        logged = []
        if last:
            cur.execute(sql.SQL("""
                INSERT INTO prices ({cols})
                SELECT {sel} FROM cur c JOIN diff d USING (product_id)
                WHERE d.is_new OR d.is_returning RETURNING change, restocked""").format(
                cols=_ident(log_cols),
                sel=log_select(sql.SQL("CASE WHEN d.is_returning AND coalesce(c.available, false) "
                                       "THEN 'back_in_stock' ELSE 'new' END"),
                               sql.SQL("NULL::numeric"), sql.SQL("NULL::numeric"),
                               sql.SQL("NULL::boolean"))))
            logged += cur.fetchall()
        # 2. tracked changes on listed products
        cur.execute(sql.SQL("""
            INSERT INTO prices ({cols})
            SELECT {sel} FROM cur c JOIN diff d USING (product_id)
            WHERE NOT d.is_new AND NOT d.is_returning AND d.tracked <> ''
            RETURNING change, restocked""").format(
            cols=_ident(log_cols),
            sel=log_select(sql.SQL("d.tracked"), sql.SQL("d.old_price"),
                           sql.SQL("d.old_stock_qty"), sql.SQL("d.old_available"))))
        logged += cur.fetchall()
        removed = missed = 0
        if compare_listing:
            # 3. gone from the listing after being in stock: out_of_stock, from the master row
            cur.execute(sql.SQL("""
                INSERT INTO prices ({cols})
                SELECT {sel} FROM {master} p
                WHERE p.website = %s AND p.listed AND coalesce(p.available, false)
                  AND NOT EXISTS (SELECT 1 FROM cur c WHERE c.product_id = p.product_id)
                RETURNING change, restocked""").format(
                cols=_ident(log_cols), master=sql.Identifier(TABLE),
                sel=log_select(sql.SQL("'out_of_stock'"), sql.SQL("p.price"),
                               sql.SQL("p.stock_qty"), sql.SQL("p.available"), source="p",
                               available=sql.SQL("false"))), (site,))
            logged += cur.fetchall()
            # listing_events: each product leaving the listing, with its values then, so a past
            # run can be rebuilt exactly (common to every removal, in stock or not)
            cur.execute(sql.SQL("""
                INSERT INTO listing_events (website, product_id, scraped_at, run_id, listed,
                                            price, stock_qty, available)
                SELECT p.website, p.product_id, %s, %s, false, p.price, p.stock_qty, p.available
                FROM {master} p
                WHERE p.website = %s AND p.listed
                  AND NOT EXISTS (SELECT 1 FROM cur c WHERE c.product_id = p.product_id)
                """).format(master=sql.Identifier(TABLE)), (scraped_dt, run_id, site))
            cur.execute(sql.SQL("""
                UPDATE {master} p SET listed = false, delisted_at = %s, run_id = %s
                WHERE p.website = %s AND p.listed
                  AND NOT EXISTS (SELECT 1 FROM cur c WHERE c.product_id = p.product_id)
                """).format(master=sql.Identifier(TABLE)), (scraped_dt, run_id, site))
            removed = cur.rowcount
        else:
            # A partial run removes nothing (as common/changes.py), so what it missed stays
            # listed. Which products it missed is kept, so the run itself can still be rebuilt
            # exactly (master_run() leaves them out).
            cur.execute(sql.SQL("""
                INSERT INTO run_misses (website, product_id, scraped_at, run_id)
                SELECT p.website, p.product_id, %s, %s FROM {master} p
                WHERE p.website = %s AND p.listed
                  AND NOT EXISTS (SELECT 1 FROM cur c WHERE c.product_id = p.product_id)
                """).format(master=sql.Identifier(TABLE)), (scraped_dt, run_id, site))
            missed = cur.rowcount

        # 4a. field history: before the master row is updated, every value a changed or returning
        #     product is about to lose, field by field (compared typed, stored as text). The
        #     change log treats a blank and a "no" availability alike, so a change between those
        #     two is kept here too.
        fields = sql.SQL(", ").join(
            [sql.SQL("({}, p.{}::text, c.{}::text, p.{} IS DISTINCT FROM c.{})").format(
                sql.Literal(col), *[sql.Identifier(col)] * 4) for col in HISTORY_COLUMNS]
            + [sql.SQL("('available', p.available::text, c.available::text, "
                       "p.available IS DISTINCT FROM c.available "
                       "AND coalesce(p.available, false) = coalesce(c.available, false))")])
        cur.execute(sql.SQL("""
            INSERT INTO field_changes (website, product_id, scraped_at, run_id, field, old_value,
                                       new_value)
            SELECT %s, c.product_id, %s, %s, f.field, f.old_value, f.new_value
            FROM cur c JOIN diff d USING (product_id)
            JOIN {master} p ON p.website = %s AND p.product_id = c.product_id
            CROSS JOIN LATERAL (VALUES {fields}) AS f(field, old_value, new_value, differs)
            WHERE (d.changed OR d.is_returning) AND f.differs""").format(
            master=sql.Identifier(TABLE), fields=fields), (site, scraped_dt, run_id, site))
        field_changes = cur.rowcount

        # 4. master: insert new products, update changed and returning ones; unchanged rows
        #    are not touched
        set_values = sql.SQL(", ").join(
            sql.SQL("{} = c.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in VALUE_COLUMNS)
        cur.execute(sql.SQL("""
            UPDATE {master} p SET {set_values}, listed = true, delisted_at = NULL,
                   scraped_at = %s, run_id = %s
            FROM cur c JOIN diff d USING (product_id)
            WHERE p.website = %s AND p.product_id = c.product_id
              AND (d.changed OR d.is_returning)""").format(
            master=sql.Identifier(TABLE), set_values=set_values),
            (scraped_dt, run_id, site))
        cur.execute(sql.SQL("""
            INSERT INTO {master} (website, product_id, {vals}, listed, first_seen_at,
                                   scraped_at, run_id)
            SELECT %s, c.product_id, {cvals}, true, %s, %s, %s
            FROM cur c JOIN diff d USING (product_id) WHERE d.is_new""").format(
            master=sql.Identifier(TABLE), vals=_ident(VALUE_COLUMNS),
            cvals=sql.SQL(", ").join(q.values())),
            (site, scraped_dt, scraped_dt, run_id))

        # listing_events: each product joining the listing (new, or back after being gone)
        cur.execute("""
            INSERT INTO listing_events (website, product_id, scraped_at, run_id, listed, price,
                                        stock_qty, available)
            SELECT %s, c.product_id, %s, %s, true, c.price, c.stock_qty, c.available
            FROM cur c JOIN diff d USING (product_id) WHERE d.is_new OR d.is_returning""",
                    (site, scraped_dt, run_id))

        new, returning, changed = cur.execute(
            "SELECT count(*) FILTER (WHERE is_new), count(*) FILTER (WHERE is_returning), "
            "count(*) FILTER (WHERE changed) FROM diff").fetchone()
        changes = {}
        for change, _ in logged:
            changes[change] = changes.get(change, 0) + 1
        restocked = sum(1 for _, r in logged if r)
        counts = {"new": new + returning, "changed": changed, "removed": removed,
                  "unchanged": len(keyed) - new - returning - changed}

        if snapshot:
            _copy(cur, site, cols + ["run_id", "full_run"],
                  (r + [run_id, True] for r in converted))

        cur.execute("INSERT INTO master_sync (website, run_id, scraped_at) VALUES (%s, %s, %s) "
                    "ON CONFLICT (website) DO UPDATE SET run_id = EXCLUDED.run_id, "
                    "scraped_at = EXCLUDED.scraped_at", (site, run_id, scraped_dt))
        db.log_run_cur(cur, site, scraped_dt, run_id, "saved", len(rows), products, True,
                       f"storage {'both' if snapshot else 'master'}",
                       {f"{k}_products": v for k, v in counts.items()})
    print(f"master: {counts['new']} new, {counts['changed']} changed, {counts['removed']} "
          f"removed, {counts['unchanged']} unchanged; change log {changes}; "
          f"{field_changes} field changes" + (f"; partial run, {missed} not seen" if missed else ""),
          flush=True)
    return dict(counts, restocked=restocked, changes=changes, field_changes=field_changes,
                missed=missed)
