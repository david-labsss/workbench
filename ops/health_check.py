"""Daily scraper health check: flags sites that never saved, stopped saving, have poor
price / image / description coverage, failed their latest QA, or whose prices look stale, so
problems are caught here instead of by the user.

Run by .github/workflows/health-check.yml once a day, and locally any time:
    DATABASE_URL=... .venv/Scripts/python.exe -I ops/health_check.py
It reads the DB read-only (READER_DATABASE_URL, DATABASE_URL, or the .reader-url file when
present), prints the flagged lists, and writes a GitHub step summary when run in Actions. Exits 1
when prices look stale, so the run shows red and GitHub emails the account; otherwise 0 (a report,
not a gate).
"""
import datetime
import os
import pathlib
import sys

import psycopg

# Sites that are legitimately empty/old -> not flagged.
DROP = {"interlightus", "ziestech", "zones", "serversupply", "screenadnote", "hddprime",
        "dellpartsdirect", "itmarket", "techbuyer"}
STALE_DAYS = 2
LOW_IMAGE = 80
LOW_PRICE = 40
STALE_WINDOW_DAYS = 7
STALE_QA_RUNS = 3      # runs in the window whose QA found other prices
QA_MAX_MISMATCH = 0.1  # as the dashboard's QA: a run fails when more than this share differs
QA_MIN_CHECKED = 10


def db_url():
    u = os.getenv("READER_DATABASE_URL") or os.getenv("DATABASE_URL")
    if u:
        return u
    f = pathlib.Path(__file__).resolve().parent.parent / ".reader-url"
    return f.read_text().strip() if f.exists() else None


def days_since(d, today):
    if not d:
        return 999
    y, m, dd = map(int, d.split("-"))
    return (today - datetime.date(y, m, dd)).days


def sweep(conn):
    """{site: {last, rows, price, image_url, description}}: each site's last saved full run (the
    master table, common/master.py, holds it as the site's listed products) with how full its
    price, image and description are (% of its products); rows 0 for a site that has runs logged
    but never saved one."""
    cur = conn.cursor()
    rep = {}
    for site, at in cur.execute("SELECT website, scraped_at FROM master_sync").fetchall():
        cur.execute("SELECT count(*), count(price), count(image_url), count(description) "
                    "FROM products WHERE website = %s AND listed", (site,))
        n, *filled = cur.fetchone()
        info = {"last": str(at)[:10], "rows": n}
        for c, f in zip(("price", "image_url", "description"), filled):
            info[c] = round(100 * f / n, 1) if n else None
        rep[site] = info
    for (site,) in cur.execute("SELECT DISTINCT site FROM scrape_runs").fetchall():
        rep.setdefault(site, {"last": None, "rows": 0})
    return rep


def stale_prices(conn):
    """(site, runs, products that differ) for sites whose QA keeps finding other prices on the store
    while their runs log no price change: what the scraper reads has stopped following the store.
    No price change alone means nothing: many stores really do keep their prices for weeks."""
    return conn.execute("""
        SELECT q.website, count(DISTINCT q.run_id), count(*)
        FROM qa_checks q
        WHERE q.result IN ('price', 'price+stock')
          AND q.checked_at > now() - make_interval(days => %(days)s)
          AND NOT EXISTS (SELECT 1 FROM change_log p
                          WHERE p.website = q.website AND p.change ILIKE '%%price%%'
                            AND p.scraped_at > now() - make_interval(days => %(days)s))
        GROUP BY q.website HAVING count(DISTINCT q.run_id) >= %(runs)s
        ORDER BY 3 DESC""", {"days": STALE_WINDOW_DAYS, "runs": STALE_QA_RUNS}).fetchall()


def failed_qa(conn):
    """(site, products that differ, products checked) for sites whose latest QA failed."""
    rows = conn.execute("""
        SELECT DISTINCT ON (website) website,
               count(*) FILTER (WHERE result IN ('price', 'stock', 'price+stock')),
               count(*) FILTER (WHERE result IN ('match', 'price', 'stock', 'price+stock'))
        FROM qa_checks WHERE checked_at > now() - interval '3 days'
        GROUP BY website, run_id, checked_at
        ORDER BY website, checked_at DESC""").fetchall()
    return [r for r in rows if r[2] >= QA_MIN_CHECKED and r[1] / r[2] > QA_MAX_MISMATCH]


def main():
    url = db_url()
    if not url:
        print("no DATABASE_URL / .reader-url"); return
    today = datetime.date.today()
    with psycopg.connect(url, autocommit=True) as conn:
        rep = sweep(conn)
        try:
            stale_sites, failed_sites = stale_prices(conn), failed_qa(conn)
            qa_note = ""
        except psycopg.errors.InsufficientPrivilege:
            stale_sites, failed_sites = [], []
            qa_note = "NOT CHECKED: this login can't read qa_checks (set READER_DATABASE_URL)\n"

    empty = sorted(s for s, v in rep.items() if not v.get("rows") and s not in DROP)
    stale = sorted((v["last"], s) for s, v in rep.items()
                   if v.get("rows") and STALE_DAYS <= days_since(v["last"], today) < 900 and s not in DROP)
    zero_img = sorted(s for s, v in rep.items() if v.get("image_url") == 0.0 and v.get("rows") and s not in DROP)
    low_img = sorted((v["image_url"], s) for s, v in rep.items()
                     if v.get("image_url") is not None and 0 < v["image_url"] < LOW_IMAGE)
    low_price = sorted((v["price"], s) for s, v in rep.items()
                       if v.get("price") is not None and v["price"] < LOW_PRICE and v.get("rows"))

    lines = [f"# Scraper health {today} ({len(rep)} sites)", ""]
    lines.append(f"## PRICES LOOK STALE [{len(stale_sites)}]\n{qa_note}"
                 f"QA found other prices on the store in {STALE_QA_RUNS}+ runs, and no price "
                 f"change was logged in {STALE_WINDOW_DAYS} days.")
    lines += [f"- {s}: {runs} runs, {differ} products differ"
              for s, runs, differ in stale_sites] or ["none"]
    lines.append(f"\n## QA FAILED, latest run [{len(failed_sites)}]")
    lines += [f"- {s}: {differ} of {checked} differ"
              for s, differ, checked in failed_sites] or ["none"]
    lines.append("")
    lines.append(f"## NEVER SAVED [{len(empty)}]\n{', '.join(empty) or 'none'}\n")
    lines.append(f"## STOPPED SAVING (>= {STALE_DAYS} days) [{len(stale)}]")
    lines += [f"- {d}  {s}" for d, s in stale] or ["none"]
    lines.append(f"\n## ZERO images (has data) [{len(zero_img)}]\n{', '.join(zero_img) or 'none'}\n")
    lines.append(f"## LOW image <{LOW_IMAGE}% [{len(low_img)}]\n" +
                 (", ".join(f"{s} {int(p)}%" for p, s in low_img) or "none") + "\n")
    lines.append(f"## LOW price <{LOW_PRICE}% [{len(low_price)}]\n" +
                 (", ".join(f"{s} {int(p)}%" for p, s in low_price) or "none"))
    out = "\n".join(lines)
    print(out)

    summary = os.getenv("GITHUB_STEP_SUMMARY")
    if summary:
        pathlib.Path(summary).write_text(out, encoding="utf-8")
    if stale_sites:
        sys.exit(f"Prices look stale on: {', '.join(s for s, _, _ in stale_sites)}")


if __name__ == "__main__":
    main()
