"""Scrape newtownspares.com (BigCommerce, ~516k products): every run's full snapshot appended
to Postgres.

The shared BigCommerce path (common/bigcommerce.py) walks the whole catalogue in one cursor
stream, 50 products at a time with a delay between pages. At ~516k products that single stream
is far too slow to finish inside one GitHub Actions run (it timed out).

This store's storefront cursor is keyset pagination by product id: `after` is
base64('{"id": N}'), and a page returns the products whose entityId is > N, in ascending id
order (verified against the live API). So the catalogue can be split into disjoint id ranges and
those ranges paged *in parallel*. That is the only change here -- the exact same GraphQL QUERY
and build_rows mapping from common/bigcommerce.py are reused, just driven by a pool of worker
threads over contiguous id ranges instead of one serial stream. Measured ~375 products/s with 6
workers, i.e. the full catalogue in well under half an hour.

Coverage is identical to the single stream: the id ranges tile the whole id space with no gap or
overlap (keyset is a strict `id >`, ascending, so each range hands off cleanly to the next), the
final range runs to the real end of the list (so products added above the sampled max id are
still caught), and a failed page raises -- common/http.py already retries 429/5xx, and a hard
failure must never look like the end of the list, so a partial snapshot can't be saved as the
full catalogue.

Optional env (all default to the whole catalogue in one polite run):
  NTS_WORKERS      parallel id-range workers (default 6)
  NTS_PAGE_DELAY   per-request courtesy delay, seconds (default 0.15)
  SHARD_INDEX /    process only shard INDEX of SHARD_COUNT equal id-space slices, so a parent can
  SHARD_COUNT      dispatch the run across several jobs (default 0/1 = everything here). NOTE: a
                   single default run already fits one job; a multi-job split would need the
                   caller to merge shards and handle change detection, since common/runner.py
                   compares each run against the previous full run.

Fetching and mapping: common/bigcommerce.py. Run steps, changes and env vars: common/runner.py.
Columns: common/schema.py.
"""
import base64
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from common import bigcommerce, http, runner, schema

SITE = "newtownspares"
BASE = "https://www.newtownspares.com"  # storefront JWT's cors origin is the www host

WORKERS = max(1, int(os.getenv("NTS_WORKERS") or 6))
PAGE_DELAY = float(os.getenv("NTS_PAGE_DELAY") or 0.15)
SHARD_INDEX = int(os.getenv("SHARD_INDEX") or 0)
SHARD_COUNT = max(1, int(os.getenv("SHARD_COUNT") or 1))
TARGET_IDS_PER_CHUNK = 10000  # ~8k products per id-range chunk; more chunks than workers balances

# A light query, used only to find the id range (min/max) before the parallel fetch.
BOUNDS_QUERY = ("query($first:Int!,$after:String){site{products(first:$first,after:$after){"
                "pageInfo{hasNextPage} edges{node{entityId}}}}}")


def cursor(product_id):
    """The store's keyset cursor for `after`: the page after it is the products with id > N."""
    return base64.b64encode(json.dumps({"id": product_id}).encode()).decode()


def headers(base):
    return {"Authorization": f"Bearer {bigcommerce.token(base)}", "Origin": base,
            "Content-Type": "application/json"}


def post(base, head, query, after, first=bigcommerce.PAGE):
    r = http.session().post(base + "/graphql", headers=head, timeout=60,
                            json={"query": query, "variables": {"first": first, "after": after}})
    r.raise_for_status()
    data = r.json()
    if data.get("errors"):
        raise RuntimeError(f"GraphQL error: {data['errors'][0].get('message')}")
    return data["data"]["site"]["products"]


def id_bounds(base, head):
    """(min id, an id >= the max id). Exponential-then-binary search on 'any product with id>x'."""
    first = post(base, head, BOUNDS_QUERY, None, first=5)
    if not first["edges"]:
        raise RuntimeError("no products returned for the first page")
    min_id = min(e["node"]["entityId"] for e in first["edges"])

    def has_after(x):
        return bool(post(base, head, BOUNDS_QUERY, cursor(x), first=1)["edges"])

    hi = 100000
    while has_after(hi):
        hi *= 2
    lo = min_id
    while lo + 1 < hi:
        mid = (lo + hi) // 2
        if has_after(mid):
            lo = mid
        else:
            hi = mid
    return min_id, hi  # no product has id > hi; the final chunk still pages to the true end


def fetch_range(base, head, lo, hi, scraped_at, progress):
    """Rows for the products with entityId in (lo, hi]; hi=None pages to the end of the list.

    A failed page raises (http.session() has already retried), so it can never look like the
    list simply ended -- a partial snapshot must not be saved as the full catalogue.
    """
    after = cursor(lo)
    rows = []
    while True:
        page = post(base, head, bigcommerce.QUERY, after)
        stop = False
        for e in page["edges"]:
            node = e["node"]
            if hi is not None and node["entityId"] > hi:
                stop = True  # belongs to the next range; its worker starts exactly here
                break
            if node["variants"]["pageInfo"]["hasNextPage"]:
                print(f"WARN product {node['entityId']} has more than 250 variants; "
                      "only the first 250 are saved", flush=True)
            rows.extend(bigcommerce.build_rows(SITE, base, node, scraped_at))
        with progress["lock"]:
            progress["products"] = len(rows) + progress.get("done", 0)
        if stop or not page["pageInfo"]["hasNextPage"]:
            break
        after = page["pageInfo"]["endCursor"]
        if PAGE_DELAY:
            time.sleep(PAGE_DELAY)
    with progress["lock"]:
        progress["done"] = progress.get("done", 0) + len(rows)
        print(f"  ~{progress['done']} products fetched", flush=True)
    return rows


def chunks(lo_edge, hi_edge, last_to_end):
    """Tile (lo_edge, hi_edge] into (lo, hi] id ranges. The last range's hi is None when it runs
    to the true end of the catalogue (so products added above the sampled max id are caught)."""
    span = hi_edge - lo_edge
    n = max(WORKERS * 4, span // TARGET_IDS_PER_CHUNK, 1)
    edges = [lo_edge + span * i // n for i in range(n + 1)]
    out = []
    for i in range(n):
        hi = edges[i + 1]
        if i == n - 1:
            hi = None if last_to_end else hi_edge
        out.append((edges[i], hi))
    return out


def scrape(limit, scraped_at):
    """A scrape(limit, scraped_at) for common/runner.py: (rows, listed)."""
    # Test runs (MAX_PRODUCTS): one short stream from the start is simplest and fast enough.
    if limit:
        nodes = bigcommerce.fetch_products(BASE, limit)
        rows = [row for n in nodes for row in bigcommerce.build_rows(SITE, BASE, n, scraped_at)]
        return rows, None

    head = headers(BASE)
    min_id, max_id = id_bounds(BASE, head)
    # The id window this process covers. SHARD_* lets a parent split the catalogue across jobs;
    # the default (0 of 1) covers everything in this one run.
    base_lo = min_id - 1
    span = max_id - base_lo
    win_lo = base_lo + span * SHARD_INDEX // SHARD_COUNT
    win_hi = base_lo + span * (SHARD_INDEX + 1) // SHARD_COUNT
    last_shard = SHARD_INDEX == SHARD_COUNT - 1
    ranges = chunks(win_lo, win_hi, last_shard)
    print(f"id range {min_id}..{max_id}; shard {SHARD_INDEX + 1}/{SHARD_COUNT} "
          f"ids ({win_lo}, {win_hi}] in {len(ranges)} chunks on {WORKERS} workers", flush=True)

    progress = {"lock": threading.Lock(), "done": 0}
    rows = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(fetch_range, BASE, head, lo, hi, scraped_at, progress)
                   for lo, hi in ranges]
        for f in as_completed(futures):
            rows.extend(f.result())  # re-raises any worker error, so the run fails and saves nothing

    # Disjoint id ranges mean no overlap; de-dupe anyway so a boundary slip can't reach the DB.
    seen, unique = set(), []
    pid = schema.PRODUCT_COLUMNS.index("product_id")
    for row in rows:
        if row[pid] not in seen:
            seen.add(row[pid])
            unique.append(row)
    if len(unique) != len(rows):
        print(f"WARN dropped {len(rows) - len(unique)} duplicate rows at range boundaries",
              flush=True)
    return unique, None


if __name__ == "__main__":
    runner.run(SITE, scrape)
