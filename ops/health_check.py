"""Daily scraper health check: flags sites that never saved, stopped saving, or have poor
price / image / description coverage, so problems are caught here instead of by the user.

Run by .github/workflows/health-check.yml once a day, and locally any time:
    DATABASE_URL=... .venv/Scripts/python.exe -I ops/health_check.py
It reads the DB read-only (DATABASE_URL, or the .reader-url file when present), prints the
flagged lists, and writes a GitHub step summary when run in Actions. Always exits 0 (a report,
not a gate).
"""
import datetime
import os
import pathlib

import psycopg

# Sites that are legitimately empty/old -> not flagged.
DROP = {"interlightus", "ziestech", "zones", "serversupply", "screenadnote", "hddprime",
        "dellpartsdirect", "itmarket", "techbuyer"}
STALE_DAYS = 2
LOW_IMAGE = 80
LOW_PRICE = 40


def db_url():
    u = os.getenv("DATABASE_URL")
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
    cur = conn.cursor()
    cur.execute("""SELECT table_name::text, array_agg(column_name::text)
                   FROM information_schema.columns WHERE table_schema='public'
                   GROUP BY table_name""")
    cols = {t: set(c) for t, c in cur.fetchall()}
    tables = sorted(t for t, cs in cols.items()
                    if {"run_id", "scraped_at"} <= cs
                    and t not in ("prices", "qa_checks", "scrape_runs"))
    rep = {}
    for t in tables:
        cur.execute(f'SELECT max(scraped_at) FROM "{t}"')
        mx = cur.fetchone()[0]
        info = {"last": str(mx)[:10] if mx else None}
        if not mx:
            info["rows"] = 0
            rep[t] = info
            continue
        cur.execute(f'SELECT run_id FROM "{t}" WHERE scraped_at=%s LIMIT 1', (mx,))
        rid = cur.fetchone()[0]
        fields = [c for c in ("price", "image_url", "description") if c in cols[t]]
        sel = ["count(*)"] + [f"count({c})" for c in fields]
        cur.execute(f'SELECT {", ".join(sel)} FROM "{t}" WHERE run_id=%s', (rid,))
        row = cur.fetchone()
        n = row[0]
        info["rows"] = n
        for i, c in enumerate(fields, 1):
            info[c] = round(100 * row[i] / n, 1) if n else None
        rep[t] = info
    return rep


def main():
    url = db_url()
    if not url:
        print("no DATABASE_URL / .reader-url"); return
    today = datetime.date.today()
    with psycopg.connect(url, autocommit=True) as conn:
        rep = sweep(conn)

    empty = sorted(s for s, v in rep.items() if not v.get("rows") and s not in DROP)
    stale = sorted((v["last"], s) for s, v in rep.items()
                   if v.get("rows") and STALE_DAYS <= days_since(v["last"], today) < 900 and s not in DROP)
    zero_img = sorted(s for s, v in rep.items() if v.get("image_url") == 0.0 and v.get("rows") and s not in DROP)
    low_img = sorted((v["image_url"], s) for s, v in rep.items()
                     if v.get("image_url") is not None and 0 < v["image_url"] < LOW_IMAGE)
    low_price = sorted((v["price"], s) for s, v in rep.items()
                       if v.get("price") is not None and v["price"] < LOW_PRICE and v.get("rows"))

    lines = [f"# Scraper health {today} ({len(rep)} sites)", ""]
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


if __name__ == "__main__":
    main()
