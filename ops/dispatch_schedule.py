"""Dispatch this account's scrapers, spread across the morning window.

Run by scheduler.yml once at 06:00 PKT. It reads ops/scrape_schedule.json (the site -> owner map),
takes the sites this account owns (heaviest first), and dispatches them one by one with a gap
between, so they start spread across the 06:00-12:00 window instead of all at once. Most sites run
through the shared scrape.yml (dispatched with the site key); a few keep their own workflow (the
"wf" field). Uses the built-in GITHUB_TOKEN (needs actions: write).

Env: GITHUB_REPOSITORY, GITHUB_REPOSITORY_OWNER, GITHUB_TOKEN (all set by Actions); ONLY = optional
comma-separated site keys to dispatch instead of the whole account (for testing); SLOT = "morning"
(06:00 PKT, the default) or "evening" (17:00 PKT): only the sites with that "slot" in the schedule
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
WINDOW_SEC = 300 * 60           # spread over 5 h (06:00-11:00 PKT); last well inside the window
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
gap = min(MAX_GAP, WINDOW_SEC // n)
hdr = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/vnd.github+json",
       "X-GitHub-Api-Version": "2022-11-28"}

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
