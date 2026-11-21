"""Dispatch this account's scrapers, spread across the morning window.

Run by scheduler.yml at 05:17 PKT (dispatched on time by the scrapers' n8n; GitHub's own late cron
is the backup and dispatches nothing once the slot has started, see slot_started). It reads ops/scrape_schedule.json (the site -> owner map),
takes the sites this account owns (heaviest first), and dispatches them one by one with a gap
between, so they start spread over the next 5 hours instead of all at once. Most sites run
through the shared scrape.yml (dispatched with the site key); a few keep their own workflow (the
"wf" field). Uses the built-in GITHUB_TOKEN (needs actions: write).

Env: GITHUB_REPOSITORY, GITHUB_REPOSITORY_OWNER, GITHUB_TOKEN (all set by Actions); ONLY = optional
comma-separated site keys to dispatch instead of the whole account (for testing); SLOT = "morning"
(05:17 PKT, the default) or "evening" (17:00 PKT): only the sites with that "slot" in the schedule
(no "slot" = morning). ONLY ignores the slot.

The evening slot (2026-11-17): our own store (usithardware) is read at 17:00 PKT, after the US IT
pricing job's 12:50 PKT run has changed its prices (done by about 15:30), so Price Monitor and the
dashboard's QA see the prices the store has that day instead of the day before. The pricing job's
list check needs this read to be under 26 hours old at its next run: about 20 hours.
"""
import json
import os
import pathlib
import time

import requests

OWNER = os.environ["GITHUB_REPOSITORY_OWNER"]
REPO = os.environ["GITHUB_REPOSITORY"]
TOKEN = os.environ["GITHUB_TOKEN"]
ONLY = {s.strip() for s in os.getenv("ONLY", "").split(",") if s.strip()}
SLOT = os.getenv("SLOT") or "morning"
WINDOW_SEC = 300 * 60           # spread over 5 h from the start (05:17-10:17 PKT when GitHub is on time)
MAX_GAP = 20 * 60

sched = json.loads((pathlib.Path(__file__).resolve().parent.parent / "ops/scrape_schedule.json").read_text())
mine = [(s, v) for s, v in sched.items() if v.get("owner") == OWNER]
if ONLY:
    mine = [(s, v) for s, v in mine if s in ONLY]
else:
    mine = [(s, v) for s, v in mine if v.get("slot", "morning") == SLOT]
mine.sort(key=lambda sv: -sv[1].get("weight", 60))     # heaviest first

n = len(mine)
if not n:
    print(f"no {SLOT} sites for {OWNER}" if not ONLY else f"no sites for {OWNER}")
    raise SystemExit(0)
hdr = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/vnd.github+json",
       "X-GitHub-Api-Version": "2022-11-28"}


def started_today(site):
    """A scrape.yml run of this site started since 07:00 UTC today and did not fail (queued, running or done).

    2026-11-18 (Shahzaib's call, user's go): the US IT pricing job starts our store's read itself the moment its run
    has written (usit-daily-prices lib/store-read.mjs, about 14:20-15:30 PKT), so the 17:00 PKT evening slot is only
    the backup: it skips a site that was already read that day. If GitHub cannot be asked, the read goes ahead."""
    since = time.strftime("%Y-%m-%dT07:00:00Z", time.gmtime())
    url = f"https://api.github.com/repos/{REPO}/actions/workflows/scrape.yml/runs"
    try:
        r = requests.get(url, headers=hdr, params={"created": f">={since}", "per_page": 100}, timeout=30)
        runs = r.json().get("workflow_runs", []) if r.status_code == 200 else []
    except (requests.RequestException, ValueError):
        return False
    return any(x.get("display_title") == f"Scrape {site}" and x.get("conclusion") in (None, "success") for x in runs)


def slot_started(hours=20):
    """An earlier Scheduler run of this slot, started in the last `hours`, that did not fail.

    2026-11-21: GitHub starts scheduled runs late (about 7 h on 3 accounts on 11-20 and 11-21), so the
    scrapers' n8n dispatches this workflow on time at 05:17 and 17:00 PKT and the crons are only the
    backup. The late cron run then finds the slot already started and dispatches nothing, so no store
    is scraped twice. Same slots are 24 h apart, so 20 h never reaches the day before. Only earlier
    runs count (a smaller run id): a later one waiting behind this run (same concurrency group) must
    not stop it. If GitHub cannot be asked, the run goes ahead."""
    me = int(os.getenv("GITHUB_RUN_ID") or 0)
    since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - hours * 3600))
    url = f"https://api.github.com/repos/{REPO}/actions/workflows/scheduler.yml/runs"
    try:
        r = requests.get(url, headers=hdr, params={"created": f">={since}", "per_page": 100}, timeout=30)
        runs = r.json().get("workflow_runs", []) if r.status_code == 200 else []
    except (requests.RequestException, ValueError):
        return None
    for x in runs:
        if (x.get("display_title") == f"Scheduler {SLOT}" and x.get("id", 0) < me
                and x.get("conclusion") in (None, "success")):
            return x
    return None


if not ONLY:
    earlier = slot_started()
    if earlier:
        print(f"the {SLOT} slot already started at {earlier.get('created_at')} "
              f"({earlier.get('event')} run {earlier.get('id')}): nothing to dispatch")
        raise SystemExit(0)

if SLOT == "evening" and not ONLY:
    before = len(mine)
    mine = [(s, v) for s, v in mine if not (v.get("wf", "scrape") == "scrape" and started_today(s))]
    if len(mine) < before:
        print(f"{before - len(mine)} evening site(s) already read today (the pricing run started them): skipped")
    n = len(mine)
    if not n:
        print(f"no {SLOT} sites left for {OWNER}")
        raise SystemExit(0)
gap = min(MAX_GAP, WINDOW_SEC // n)

print(f"{OWNER}: {n} sites, {gap // 60} min apart")
for i, (site, v) in enumerate(mine):
    wf = v.get("wf", "scrape")
    body = {"ref": "main"}
    if wf == "scrape":
        body["inputs"] = {"site": site}
    url = f"https://api.github.com/repos/{REPO}/actions/workflows/{wf}.yml/dispatches"
    try:
        r = requests.post(url, headers=hdr, json=body, timeout=30)
        print(f"  {'ok ' if r.status_code == 204 else 'ERR ' + str(r.status_code)} {site} ({wf})",
              flush=True)
    except requests.RequestException as e:
        print(f"  ERR {site}: {type(e).__name__}", flush=True)
    if i < n - 1:
        time.sleep(gap)
