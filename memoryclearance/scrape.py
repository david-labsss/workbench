"""Scrape memoryclearance.com: every run's full snapshot appended to Postgres.

The site is an Angular app on the same e-commerce backend as harddiskdirect: its data comes
from api.memoryclearance.com/1 (1 = this store's website id, from the app's own config).
Fetching and mapping: common/cmshardware.py. Run steps, changes and env vars:
common/runner.py. Columns: common/schema.py.
"""
from common import cmshardware, runner

SITE = "memoryclearance"
BASE = "https://www.memoryclearance.com"
API = "https://api.memoryclearance.com/1"

if __name__ == "__main__":
    runner.run(SITE, cmshardware.scraper(SITE, BASE, API))
