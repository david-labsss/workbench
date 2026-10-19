"""Scrape harddiskdirect.com (~135k products): every run's full snapshot appended to Postgres.

The www site sits behind a Cloudflare challenge, but its data comes from an open JSON backend,
backend.cmshardware.com/5 (5 = this store's website id), with no challenge.
Fetching and mapping: common/cmshardware.py. Run steps, changes and env vars:
common/runner.py. Columns: common/schema.py.
"""
from common import cmshardware, runner

SITE = "harddiskdirect"
BASE = "https://www.harddiskdirect.com"
API = "https://backend.cmshardware.com/5"

if __name__ == "__main__":
    runner.run(SITE, cmshardware.scraper(SITE, BASE, API))
