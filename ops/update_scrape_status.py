r"""Write the "Scrape status" column (R) of the competitor-discovery Google Sheet: in front of
every store's domain, say whether we scrape it.

Runs in the cloud (GitHub Actions, .github/workflows/update-scrape-status.yml, daily) and locally.
No credentials or internal n8n details live in this file:
  * "which stores are live" is read from ops/live_sites.json (committed; keep in sync with the
    5 n8n tier workflows when a scraper is added to / removed from a tier).
  * the Google service account comes from the GOOGLE_SA_JSON env var (a GitHub secret in CI), or
    a local key file for hand runs.

Status per row (matched to a scraper by the scraper's REAL target domain, so e.g.
harddrivesdirect.com is never confused with the harddiskdirect scraper):
  Scraped / Blocked (...) / On hold (...) / Dropped (...) / Built, not live (...) / Not built yet

Run:  python -I ops/update_scrape_status.py            # write
      python -I ops/update_scrape_status.py --dry       # print only
"""
import glob
import json
import os
import re
import sys
from collections import Counter

sys.stdout.reconfigure(encoding="utf-8")

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.environ.get("GITHUB_WORKSPACE") or os.path.dirname(HERE)  # repo root (has */scrape.py)
LOCAL_SA = r"D:\Server Part Deals\service-account.json"             # only used for local runs
SHEET_ID = "16wJ7P8FmUG3DjOAZ6F9RnJDCb7FsXTqDb_eXdsc2boU"
STATUS_COL = 18  # column R

# Scrapers whose dir exists but are NOT scheduled, keyed by site dir name.
NOT_LIVE = {
    "gotodirect": "On hold (needs sharding)",
    "servertechcentral": "Dropped (fake data)",
    "eurolanditstore": "Built, not live (quote-only)",
    "wearesync": "Built, not live",
    "serverflake": "Built, not live (untested)",
    "cpumedics": "Built, not live (untested)",
    "serverparts4less": "Built, not live (untested)",
    "techatlantix": "Blocked (Cloudflare)",
    "ipccomputer": "Blocked",
    "itmarket": "Blocked (paid-only)",
    "techbuyer": "Blocked (paid-only)",
}
# Known blocked stores with NO scraper dir, keyed by domain.
BLOCKED_DOMAINS = {
    "memory4less.com": "Blocked (Cloudflare)",
    "servermonkey.com": "Blocked (Turnstile, no free path)",
}
# Scrapers whose target domain differs from / isn't parsed from a BASE var.
DOMAIN_OVERRIDE = {
    "staples": "staples.com",
    "prodisknetwork": "prodisknetwork.com",
    "serverorbit": "serverorbit.com",
    "aztekc": "aztekc.com",  # scraper targets aztekcomputers.com; sheet lists aztekc.com (same store)
}


def norm(host):
    host = (host or "").strip().lower()
    host = re.sub(r"^https?://", "", host).split("/")[0]
    return re.sub(r"^www\.", "", host)


def live_keys():
    data = json.load(open(os.path.join(HERE, "live_sites.json"), encoding="utf-8"))
    keys = set()
    for name, sites in data.items():
        if name.startswith("_"):
            continue
        keys.update(sites)
        print(f"  {name}: {len(sites)} live", flush=True)
    return keys


def key_domains():
    out = {}
    for path in glob.glob(os.path.join(ROOT, "*", "scrape.py")):
        key = os.path.basename(os.path.dirname(path))
        txt = open(path, encoding="utf-8", errors="ignore").read()
        m = re.search(r'^BASE\s*=\s*["\']([^"\']+)["\']', txt, re.M)
        if m:
            out[key] = norm(m.group(1))
    out.update(DOMAIN_OVERRIDE)
    return out


def open_sheet():
    import gspread
    sa_json = os.environ.get("GOOGLE_SA_JSON")
    if sa_json:
        gc = gspread.service_account_from_dict(json.loads(sa_json))
    else:
        gc = gspread.service_account(filename=LOCAL_SA)
    return gc.open_by_key(SHEET_ID).sheet1


def main():
    dry = "--dry" in sys.argv
    LIVE = live_keys()
    K2D = key_domains()
    print(f"live keys: {len(LIVE)}; scrapers with a domain: {len(K2D)}", flush=True)

    def match_key(dom):
        for key, d in K2D.items():
            if d == dom or d.endswith("." + dom) or dom.endswith("." + d):
                return key
        return None

    ws = open_sheet()
    domains = ws.col_values(3)  # column C

    rows, tally = [], Counter()
    for i in range(2, len(domains) + 1):
        dom = norm(domains[i - 1]) if i - 1 < len(domains) else ""
        if not dom:
            continue
        key = match_key(dom)
        if key and key in LIVE:
            status = "Scraped"
        elif key and key in NOT_LIVE:
            status = NOT_LIVE[key]
        elif dom in BLOCKED_DOMAINS:
            status = BLOCKED_DOMAINS[dom]
        elif key:
            status = "Built, not live"
        else:
            status = "Not built yet"
        rows.append((i, status))
        tally[status] += 1

    print("\n=== status tally ===")
    for s, c in tally.most_common():
        print(f"  {c:3}  {s}")
    print(f"  total: {sum(tally.values())}")

    if dry:
        print("\n(dry run — pass no flag to write)")
        return
    last = rows[-1][0]
    ws.update(values=[[s] for _, s in rows], range_name=f"R2:R{last}",
              value_input_option="RAW")
    print(f"\nWROTE R2:R{last} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
