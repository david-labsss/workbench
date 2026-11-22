"""Skip a scheduled run of a daily job that the scrapers' n8n already started today.

2026-11-22: GitHub starts scheduled runs hours late (the health check 7-8 h, the browser QA about
8 h, on 11-20 to 11-22), so the scrapers' n8n ("Scrapers - Scheduler on time") dispatches these
daily jobs on time and their crons stay only as the backup. A scheduled run calls this first: if an
earlier run of the same workflow started since this round's time (ROUND_UTC, less 1 h) and did not
fail, it writes skip=true to GITHUB_OUTPUT and the job's work is skipped. A dispatched run (n8n or by
hand) always goes ahead, and so does any run when GitHub cannot be asked.

Env (set by Actions): GITHUB_REPOSITORY, GITHUB_RUN_ID, GITHUB_TOKEN, GITHUB_EVENT_NAME, GITHUB_OUTPUT.
Env (set by the workflow): WORKFLOW = its file name (e.g. health-check.yml); ROUND_UTC = its daily
time in UTC ("08:00"). Standard library only, so it runs before any setup step.
"""
import calendar
import json
import os
import time
import urllib.request


def earlier_run():
    if os.getenv("GITHUB_EVENT_NAME") != "schedule":
        return None
    hh, mm = (int(x) for x in os.environ["ROUND_UTC"].split(":"))
    now = time.gmtime()
    since = calendar.timegm((now.tm_year, now.tm_mon, now.tm_mday, hh, mm, 0)) - 3600
    if since > time.time():          # started after midnight UTC: the round was yesterday's
        since -= 86400
    me = int(os.getenv("GITHUB_RUN_ID") or 0)
    url = (f"https://api.github.com/repos/{os.environ['GITHUB_REPOSITORY']}/actions/workflows/"
           f"{os.environ['WORKFLOW']}/runs?per_page=50&created=%3E%3D"
           + time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(since)))
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}",
                                               "Accept": "application/vnd.github+json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            runs = json.load(r).get("workflow_runs", [])
    except Exception as e:           # GitHub can't be asked: run rather than skip
        print(f"could not list runs ({type(e).__name__}): going ahead")
        return None
    for x in runs:
        if x.get("id", 0) < me and x.get("conclusion") in (None, "success"):
            return x
    return None


if __name__ == "__main__":
    run = earlier_run()
    if run:
        print(f"already started this round at {run.get('created_at')} ({run.get('event')} run {run.get('id')}): skipping")
    else:
        print("no earlier run this round: going ahead")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
        f.write(f"skip={'true' if run else 'false'}\n")
