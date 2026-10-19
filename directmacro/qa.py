"""QA of a DirectMacro run, inside its own GitHub run.

The dashboard's daily QA (server-scrapers-ui, Vercel Cron) re-fetches 25 random products of each
site's newest run from the store and compares them with what was saved. DirectMacro's Cloudflare
blocks Vercel's servers, so its QA runs here instead, right after the scrape, with the same
sample size, the same GraphQL lookup and the same rules. The results go to the same qa_checks
table, so the dashboard shows "QA passed / failed" for DirectMacro like for the other sites.

  - the run checked: this GitHub run's own (GITHUB_RUN_ID), only when it saved a full run
  - reads the sample with DATABASE_URL (scraper_writer); writes with the qa_writer role, which
    can only INSERT into qa_checks (QA_DB_PASSWORD, same server as DATABASE_URL)
  - one QA is one INSERT, so its rows share checked_at (the dashboard groups them by it)
  - checking a run again adds a new QA; the dashboard shows the latest one (neither role here
    can read qa_checks, so it doesn't look for an earlier QA)

Env vars: DATABASE_URL, QA_DB_PASSWORD, GITHUB_RUN_ID (or QA_RUN_ID to check another run;
"latest" for the newest saved full run). The "QA DirectMacro" workflow runs it by hand.
"""
import base64
import json
import os
import sys
import time
from urllib.parse import unquote, urlparse

import psycopg

from common import db
from directmacro.scrape import SITE, gql

SAMPLE = 25  # products checked per run, as in the dashboard's QA
MAX_MISMATCH = 0.1  # passes when at most this share of the products it could check differ
MIN_CHECKED = 10  # below this many products checked, the run is "not checked"
MISMATCHES = ("price", "stock", "price+stock")
FIELDS = "uid stock_status price_range { minimum_price { final_price { value } } }"


class Unclear(Exception):
    """The product can't be checked (not found again): not a mismatch."""


def own_id(uid):
    """DirectMacro's product id: its GraphQL uid, base64-decoded (the scraper saves it so)."""
    return base64.b64decode(uid).decode()


def live_product(row):
    """The product as the store shows it now, found by its page's URL key: products with
    options through their parent, then the variant picked by id."""
    path = urlparse(row["url"] or "").path
    url_key = path.strip("/").removesuffix(".html")
    if not url_key:
        raise Unclear("no product page")
    data = gql(f"{{ products(filter: {{ url_key: {{ eq: {json.dumps(url_key)} }} }}) {{ items {{ {FIELDS} "
               f"... on ConfigurableProduct {{ variants {{ product {{ {FIELDS} }} }} }} }} }} }}")
    items = []
    for item in data["products"]["items"]:
        items.append(item)
        items.extend(v["product"] for v in item.get("variants") or [])
    item = next((i for i in items if own_id(i["uid"]) == row["product_id"]), None)
    if not item:
        raise Unclear("not found by its page")
    return {
        "price": item["price_range"]["minimum_price"]["final_price"]["value"] or None,
        "available": item["stock_status"] == "IN_STOCK",
    }


def compare(row, live):
    """Prices match to the cent (0 and no price are the same: "call for price"); stock is
    compared only when both sides know it."""
    saved = float(row["price"]) if row["price"] else None
    now = live["price"] or None
    price_ok = saved == now if saved is None or now is None else abs(saved - now) <= 0.01
    stock_ok = row["available"] is None or live["available"] is None or row["available"] == live["available"]
    if price_ok and stock_ok:
        return "match"
    if not price_ok and not stock_ok:
        return "price+stock"
    return "stock" if price_ok else "price"


def qa_connect():
    """The qa_writer role on the same server as DATABASE_URL."""
    password = os.getenv("QA_DB_PASSWORD")
    if not password:
        sys.exit("QA_DB_PASSWORD is not set (add it under the repo's Settings > Secrets > Actions)")
    url = urlparse(os.environ["DATABASE_URL"])
    return psycopg.connect(host=url.hostname, port=url.port or 5432, dbname=unquote(url.path.lstrip("/")),
                           user="qa_writer", password=password, connect_timeout=30)


def status_of(results):
    checked = sum(1 for r in results if r == "match" or r in MISMATCHES)
    mismatched = sum(1 for r in results if r in MISMATCHES)
    if checked < MIN_CHECKED:
        return "not_checked"
    return "passed" if mismatched / checked <= MAX_MISMATCH else "failed"


def main():
    run_id = os.getenv("QA_RUN_ID") or os.getenv("GITHUB_RUN_ID")
    if not run_id:
        sys.exit("No run to check: GITHUB_RUN_ID (or QA_RUN_ID) is not set")
    with db.connect() as conn:
        if run_id == "latest":
            last = conn.execute(
                "SELECT run_id FROM scrape_runs WHERE site = %s AND full_run AND status = 'saved' "
                "ORDER BY logged_at DESC LIMIT 1", (SITE,)).fetchone()
            if not last:
                sys.exit(f"{SITE} has no saved full run yet")
            run_id = last[0]
        saved = conn.execute(
            "SELECT 1 FROM scrape_runs WHERE site = %s AND run_id = %s AND full_run AND status = 'saved'",
            (SITE, run_id)).fetchone()
        if not saved:
            print(f"QA skipped: run {run_id} is not a saved full run of {SITE}", flush=True)
            return
        cur = conn.execute(
            f"SELECT product_id, url, price, available FROM {SITE} "
            "WHERE run_id = %s AND full_run ORDER BY random() LIMIT %s", (run_id, SAMPLE))
        names = [c.name for c in cur.description]
        sample = [dict(zip(names, r)) for r in cur]

    checks = []
    for row in sample:
        live, note = None, None
        try:
            live = live_product(row)
            result = compare(row, live)
        except Unclear as e:
            result, note = "unclear", str(e)
        except Exception as e:  # noqa: BLE001 - a store error is a result, not a crash
            result, note = "error", str(e)[:300]
        checks.append((row, live, result, note))
        now = "-" if live is None else f"{live['price']} {live['available']}"
        print(f"  {row['product_id']}: saved {row['price']} {row['available']} -> {now}: {result}"
              + (f" ({note})" if note else ""), flush=True)
        time.sleep(0.5)
    if not checks:
        print("QA skipped: the run has no products", flush=True)
        return

    # One INSERT, so every row gets the same checked_at
    values = [(SITE, run_id, row["product_id"], row["url"], "api" if live else None, row["price"],
               live["price"] if live else None, row["available"], live["available"] if live else None,
               result, note) for row, live, result, note in checks]
    placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"] * len(values))
    with qa_connect() as conn:
        conn.execute(
            "INSERT INTO qa_checks (website, run_id, product_id, url, source, saved_price, live_price,"
            f" saved_available, live_available, result, note) VALUES {placeholders}",
            [v for row in values for v in row])

    tally = {}
    for _, _, result, _ in checks:
        tally[result] = tally.get(result, 0) + 1
    print(f"QA of run {run_id}: {status_of([c[2] for c in checks])} {tally}", flush=True)


if __name__ == "__main__":
    main()
