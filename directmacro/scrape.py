"""Scrape directmacro.com (Magento 2): every run's full snapshot appended to Postgres.

Everything comes from the store's public GraphQL API (POST /graphql), no product pages.
GraphQL never returns more than 10,000 results for one filter and then silently reports
total_count = 10000, so the catalog (~400k products) is split into filters that each match
fewer than 10,000 products: condition x price range, and condition x brand for the
"Call For Price" ($0) items that can't be split by price.

Every run covers the whole catalog:
  1. Tags: SKU-only lookups per brand and per spec value (capacity, interface, ...). GraphQL
     can filter by these attributes but doesn't return them on the product itself.
  2. Main sweep: title, price, stock, category, ... 1,000 products per page.
  3. Changes against the previous full run: rows whose price or availability changed, and the
     products that came back in stock.
Rows are INSERTed into the "directmacro" table (common/db.py), changes into the shared
prices table (website = directmacro). Columns: common/schema.py.

Env vars:
  DATABASE_URL                 Postgres URL for the scraper_writer role
  MAX_PRODUCTS                 optional, only scrape about N products (test runs)
  WORKERS                      optional, GraphQL requests in parallel (default 4)
  DRY_RUN                      optional, "1" = print sample rows, don't save anything
"""
import base64
import json
import math
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
import threading

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common import db, schema

BASE = "https://directmacro.com"
GRAPHQL = f"{BASE}/graphql"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)
CAP = 10000  # GraphQL never returns more results than this for one filter
PAGE = 1000  # full products per page (bigger pages are slower per row)
SKU_PAGE = 9999  # SKU-only lookups: one page covers any filter under the cap
MAX_ALIASES = 10  # the store rejects requests with more queries than this
PACK_ROWS = 1500  # small partitions are packed into one request up to this many rows
PACK_SKU_ROWS = 20000
TEST_ROWS_PER_PARTITION = 50

SITE = "directmacro"
COLUMNS = schema.PRODUCT_COLUMNS

# Layered-navigation attributes that are not specs
NOT_SPECS = {"price", "category_uid", "category_id", "condition", "brand"}
# Spec attribute -> column; every other spec attribute goes to other_specs
SPEC_COLUMNS = {
    "capacity": "capacity", "interface": "interface", "form_factor": "form_factor",
    "speed": "spindle_speed", "cache": "cache", "series": "series",
}
# Starting price ranges (USD); a range still over the cap is split in four. Edges sit on
# half cents so no product price is ever exactly on one (inclusive/exclusive can't matter).
PRICE_EDGES = [0.005] + [e + 0.005 for e in (
    25, 50, 75, 100, 150, 200, 250, 300, 400, 500, 750, 1000, 1500, 2000, 3000, 5000, 10000)]

PRODUCT_FIELDS = (
    "__typename uid sku name url_key url_suffix stock_status only_x_left_in_stock "
    "image{url} description{html} "
    "price_range{minimum_price{regular_price{value} final_price{value}}} "
    "categories{name url_path} ... on SimpleProduct{weight}"
)
# Products with options are rare (one in the whole catalog in Oct 2026), so their variants
# are fetched separately: in PRODUCT_FIELDS they push 10 aliased pages over the store's
# query complexity limit of 300.
VARIANT_FIELDS = (
    "sku ... on ConfigurableProduct{configurable_options{attribute_code label} "
    "variants{attributes{code label} product{uid sku name stock_status only_x_left_in_stock "
    "price_range{minimum_price{regular_price{value} final_price{value}}} "
    "... on SimpleProduct{weight}}}}"
)

# Upper-case only: "8Gb" in a title is a link speed (gigabit), not a capacity
CAPACITY_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(TB|GB)\b")
# Title fallbacks and price_per_tb only make sense for drives
DRIVE_CATEGORIES = (
    "Storage Devices > Solid State Drives", "Storage Devices > Internal Hard Drives",
    "Storage Devices > External Hard Drives",
)
CACHE_RE = re.compile(r"\b(\d+)\s*MB\s+Cache\b", re.I)

_local = threading.local()


# ---------------------------------------------------------------- GraphQL

def session():
    """One requests session per thread, retrying 429/502/503/504 (honours Retry-After)."""
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers["User-Agent"] = USER_AGENT
        retry = Retry(
            total=5, backoff_factor=2, status_forcelist=[429, 502, 503, 504],
            allowed_methods=["POST"],
        )
        s.mount("https://", HTTPAdapter(max_retries=retry))
        _local.session = s
    return _local.session


def gql(query):
    """Run one GraphQL request. Any error raises: a failed request must never look like
    "no more products" (the store answers some errors with HTTP 200 + an errors key)."""
    for attempt in range(1, 5):
        try:
            r = session().post(GRAPHQL, json={"query": query}, timeout=180)
            body = r.json()
            if r.status_code == 200 and body.get("data") and not body.get("errors"):
                return body["data"]
            errors = body.get("errors") or []
            message = f"HTTP {r.status_code}: {str(errors)[:300]}"
            if any((e.get("extensions") or {}).get("category") == "graphql-input"
                   for e in errors):
                raise ValueError(f"GraphQL rejected the query (retrying won't help): {message}")
            raise RuntimeError(message)
        except (requests.RequestException, RuntimeError) as e:
            if attempt == 4:
                raise RuntimeError(f"GraphQL request failed 4 times: {e}") from e
            print(f"WARN GraphQL attempt {attempt} failed: {e}", flush=True)
            time.sleep(10 * attempt)


def run_aliased(subqueries):
    """Up to MAX_ALIASES `products(...)` queries in one request; results in the same order."""
    data = gql("{" + " ".join(f"q{i}:{s}" for i, s in enumerate(subqueries)) + "}")
    return [data[f"q{i}"] for i in range(len(subqueries))]


def run_all(pool, subqueries):
    """Many queries, 10 per request, requests in parallel; results in the same order."""
    batches = [subqueries[i:i + MAX_ALIASES] for i in range(0, len(subqueries), MAX_ALIASES)]
    out = []
    for res in pool.map(run_aliased, batches):
        out.extend(res)
    return out


def gql_filter(f):
    parts = []
    for key, value in f.items():
        if key == "price":
            lo, hi = value
            rng = ([f'from:"{lo:.3f}"'] if lo is not None else []) + (
                [f'to:"{hi:.3f}"'] if hi is not None else [])
            parts.append(f"price:{{{','.join(rng)}}}")
        elif isinstance(value, list):
            parts.append(f"{key}:{{in:{json.dumps(value)}}}")
        else:
            parts.append(f"{key}:{{eq:{json.dumps(value)}}}")
    return "{" + ",".join(parts) + "}"


def products_query(f, page_size, page=1, sort="", fields="sku"):
    sort_arg = f",sort:{{name:{sort}}}" if sort else ""
    return (f"products(filter:{gql_filter(f)},pageSize:{page_size},currentPage:{page}"
            f"{sort_arg}){{total_count items{{{fields}}}}}")


def aggregations(f):
    """{attribute_code: aggregation} for the products matching f (counts are exact)."""
    data = gql("{p:products(filter:" + gql_filter(f) + ",pageSize:1){aggregations"
               "{attribute_code label count options{label value count}}}}")
    return {a["attribute_code"]: a for a in data["p"]["aggregations"]}


def exact_count(aggs):
    # Every product has exactly one condition, so the condition counts add up to the total
    return sum(o["count"] for o in aggs["condition"]["options"]) if "condition" in aggs else 0


def option_labels(codes):
    """{attribute_code: {option value: label}} for select attributes."""
    attrs = ",".join(f'{{attribute_code:"{c}",entity_type:"catalog_product"}}' for c in codes)
    data = gql(f"{{customAttributeMetadata(attributes:[{attrs}]){{items{{attribute_code "
               f"attribute_options{{label value}}}}}}}}")
    return {
        i["attribute_code"]: {o["value"]: o["label"] for o in i.get("attribute_options") or []}
        for i in data["customAttributeMetadata"]["items"]
    }


# ---------------------------------------------------------------- splitting under the cap

@dataclass
class Leaf:
    """A filter that matches fewer than CAP products (or one half of a sorted pair)."""
    filter: dict
    count: int  # products matching the filter (exact)
    rows: int  # rows to fetch
    sort: str = ""  # "ASC"/"DESC" by name: the two halves of a 10k-20k group
    group: int = 0  # sorted halves share a group, checked together


def cents_edge(x):
    return round(math.floor(x * 100) / 100 + 0.005, 3)


def price_children(lo, hi):
    """Split a price range, or [] for a single price (the $0 bucket, or one exact price)."""
    if lo is None:
        return []
    if hi is None:
        mid = cents_edge(max(lo * 4, lo + 1000))
        return [(lo, mid), (mid, None)]
    edges = sorted({cents_edge(lo + (hi - lo) * k / 4) for k in (1, 2, 3)})
    edges = [e for e in edges if lo < e < hi]
    points = [lo] + edges + [hi]
    return list(zip(points, points[1:])) if edges else []


class Planner:
    def __init__(self, pool, condition_values, brand_values):
        self.pool = pool
        self.condition_values = condition_values
        self.brand_values = brand_values
        self.unreachable = 0  # products no public filter can isolate
        self.groups = 0

    def probe(self, filters):
        res = run_all(self.pool, [f"products(filter:{gql_filter(f)},pageSize:1){{total_count}}"
                                  for f in filters])
        return [r["total_count"] for r in res]

    def plan(self, nodes, page_size):
        """Leaves covering every product matched by nodes [(filter, count or None)]."""
        leaves = []
        while nodes:
            unknown = [f for f, c in nodes if c is None]
            counts = iter(self.probe(unknown)) if unknown else iter(())
            nodes = [(f, next(counts) if c is None else c) for f, c in nodes]
            next_nodes = []
            for f, c in nodes:
                if c == 0:
                    continue
                if c < CAP:
                    leaves.append(Leaf(f, c, c))
                    continue
                kids, sorted_leaves = self.split(f, page_size)
                next_nodes.extend(kids)
                leaves.extend(sorted_leaves)
            nodes = next_nodes
        return leaves

    def split(self, f, page_size):
        """Children of a filter that hits the cap: ([(filter, count or None)], [leaves])."""
        if "condition" not in f:
            return [({**f, "condition": v}, None) for v in self.condition_values], []
        if "price" not in f:
            ranges = [(None, PRICE_EDGES[0])] + list(zip(PRICE_EDGES, PRICE_EDGES[1:])) + [
                (PRICE_EDGES[-1], None)]
            return [({**f, "price": r}, None) for r in ranges], []
        kids = price_children(*f["price"])
        if kids:
            return [({**f, "price": r}, None) for r in kids], []
        aggs = aggregations(f)
        total = exact_count(aggs)
        if "brand" not in f:
            brands = (aggs.get("brand") or {}).get("options", [])
            if len(brands) >= 500:  # aggregation lists at most 500 options: probe them all
                return [({**f, "brand": v}, None) for v in self.brand_values], []
            self.unreachable += total - sum(o["count"] for o in brands)
            return [({**f, "brand": o["value"]}, o["count"]) for o in brands if o["count"]], []
        # One price, one brand, still 10k+: the first 10k by name A-Z plus the first by Z-A
        self.groups += 1
        first = min(total, CAP - 1) if page_size == SKU_PAGE else min(total, CAP)
        rest = min(total - first + page_size, first)
        self.unreachable += max(0, total - 2 * first)
        print(f"WARN {total} products share one price and brand ({f}): fetching them "
              f"sorted A-Z and Z-A", flush=True)
        return [], [Leaf(f, total, first, "ASC", self.groups),
                    Leaf(f, total, rest, "DESC", self.groups)]


# ---------------------------------------------------------------- tags (brand, specs)

def tag_targets(base_aggs, labels, spec_attrs):
    """[(attribute, option value, count or None)] worth a lookup for these products."""
    targets = []
    for attr in ["brand"] + spec_attrs:
        agg = base_aggs.get(attr)
        if agg and len(agg["options"]) < 500:
            targets += [(attr, o["value"], o["count"]) for o in agg["options"] if o["count"]]
        elif agg or attr == "brand":  # list may be cut at 500: look up every option
            targets += [(attr, v, None) for v in labels.get(attr, {})]
    return targets


def sweep_tags(pool, planner, bases, labels, spec_attrs):
    """{sku: {attribute: [labels]}} for every brand and spec value."""
    tags = {}
    for base, base_aggs in bases:
        targets = tag_targets(base_aggs, labels, spec_attrs)
        print(f"tags: {len(targets)} brand/spec values to look up", flush=True)
        leaves_by_attr = []
        for attr in dict.fromkeys(t[0] for t in targets):
            nodes = [({**base, attr: v}, c) for a, v, c in targets if a == attr]
            leaves_by_attr += [(attr, leaf) for leaf in planner.plan(nodes, SKU_PAGE)]
        # Pack small lookups together, biggest first
        leaves_by_attr.sort(key=lambda al: -al[1].rows)
        requests_, current, rows = [], [], 0
        for al in leaves_by_attr:
            if current and (len(current) == MAX_ALIASES or rows + al[1].rows > PACK_SKU_ROWS):
                requests_.append(current)
                current, rows = [], 0
            current.append(al)
            rows += al[1].rows
        if current:
            requests_.append(current)

        def fetch(batch):
            return run_aliased([products_query(leaf.filter, SKU_PAGE, sort=leaf.sort)
                                for _, leaf in batch])

        for batch, results in zip(requests_, pool.map(fetch, requests_)):
            for (attr, leaf), res in zip(batch, results):
                label = labels.get(attr, {}).get(leaf.filter[attr], leaf.filter[attr])
                for item in res["items"]:
                    values = tags.setdefault(item["sku"], {}).setdefault(attr, [])
                    if label not in values:
                        values.append(label)
        print(f"tags: {len(requests_)} requests, {len(tags)} SKUs tagged", flush=True)
    return tags


# ---------------------------------------------------------------- rows

def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def capacity_tb(text):
    m = CAPACITY_RE.search(text or "")
    if not m:
        return None
    size = float(m.group(1))
    return size if m.group(2).upper() == "TB" else size / 1000


def category_path(categories):
    """Deepest category as "Storage Devices > Internal Hard Drives > Server Hard Drives"."""
    names = {c["url_path"]: c["name"] for c in categories or [] if c.get("url_path")}
    if not names:
        return ""
    parts = max(names, key=lambda p: p.count("/")).split("/")
    return " > ".join(
        names.get("/".join(parts[:i + 1]), parts[i].replace("-", " ").title())
        for i in range(len(parts)))


def build_rows(item, condition, tags, spec_labels, scraped_at):
    """One row per product, or one per variant for a product with options."""
    if not item.get("variants"):
        return [build_row(item, condition, tags, spec_labels, scraped_at)]
    option_names = {o["attribute_code"]: o["label"]
                    for o in item.get("configurable_options") or []}
    product_id = base64.b64decode(item["uid"]).decode()
    rows = []
    for v in item["variants"]:
        options = {a["code"]: a["label"] for a in v["attributes"]}
        variant = {
            **v["product"],
            "name": f"{item['name'].strip()} - {' / '.join(options.values())}",
            "categories": item.get("categories"),
            "url_key": item["url_key"], "url_suffix": item.get("url_suffix"),
            # variants aren't fetched with image/description: fall back to the parent's
            "image": v["product"].get("image") or item.get("image"),
            "description": v["product"].get("description") or item.get("description"),
        }
        variant_condition = options.pop("condition", condition)
        rows.append(build_row(
            variant, variant_condition, tags, spec_labels, scraped_at,
            product_id=product_id, parent_sku=item["sku"],
            options={option_names.get(c, c): label for c, label in options.items()}))
    return rows


def build_row(item, condition, tags, spec_labels, scraped_at, product_id=None,
              parent_sku=None, options=None):
    """options: a variant's own option values, e.g. {"Capacity & Color": "128GB - Silver"}."""
    sku = item["sku"]
    product_tags = {a: ", ".join(v)
                    for a, v in (tags.get(sku) or tags.get(parent_sku) or {}).items()}
    options = options or {}
    title = item.get("name") or ""
    category = category_path(item.get("categories"))
    drive = category.startswith(DRIVE_CATEGORIES)
    prices = item["price_range"]["minimum_price"]
    final = prices["final_price"]["value"] or None
    regular = prices["regular_price"]["value"] or None
    capacity = product_tags.get("capacity") or ""
    cache = product_tags.get("cache") or ""
    for name, value in options.items():
        m = CAPACITY_RE.search(value) if "capacity" in name.lower() else None
        if m and not capacity:
            capacity = f"{m.group(1)}{m.group(2)}"
    if drive and not capacity:
        m = CAPACITY_RE.search(title)
        capacity = f"{m.group(1)}{m.group(2).upper()}" if m else ""
    if drive and not cache:
        m = CACHE_RE.search(title)
        cache = f"{m.group(1)}MB" if m else ""
    tb = capacity_tb(capacity) if drive else None

    other = [f"{name}: {value}" for name, value in options.items()]
    other += [f"{spec_labels.get(a, a)}: {v}" for a, v in product_tags.items()
              if a not in SPEC_COLUMNS and a != "brand"]
    if item.get("weight"):
        other.append(f"Weight: {item['weight']:g} lbs")
    if not final:
        other.append("Price: Call For Price")

    own_id = base64.b64decode(item["uid"]).decode()
    row = {
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": own_id,       # unique: a variant's own id for products with options
        "parent_id": product_id,    # the product with options it belongs to, else empty
        "variant_id": own_id,
        "title": title,
        "brand": product_tags.get("brand", ""),
        "category": category,
        "condition": condition,
        "sku": sku,
        "model_number": sku,  # the site only has one identifier, its "Mfr Part#"
        "part_number": sku,
        "series": product_tags.get("series", ""),
        "capacity": capacity,
        "interface": product_tags.get("interface", ""),
        "form_factor": product_tags.get("form_factor", ""),
        "spindle_speed": product_tags.get("speed", ""),
        "cache": cache,
        "endurance": "",
        "price": final,
        "compare_at_price": regular if final and regular and regular > final else None,
        "price_per_tb": round(final / tb, 2) if final and tb else None,
        "available": item.get("stock_status") == "IN_STOCK",
        "stock_qty": item.get("only_x_left_in_stock"),  # null on this store: no exact count
        "other_specs": "; ".join(other),
        "image_url": (item.get("image") or {}).get("url") or None,  # Magento returns an absolute url
        "description": schema.plain_text((item.get("description") or {}).get("html")),
        "url": f"{BASE}/{item['url_key']}{item.get('url_suffix') or '.html'}",
    }
    return schema.product_row(row)


# ---------------------------------------------------------------- changes

def same_number(a, b):
    return to_float(a) == to_float(b)


def find_changes(rows, previous):
    """Every price / availability change against the previous full run, as prices rows
    (common/schema.py). previous is {product_id: {column: text}} from db.previous_run()."""
    # A first run (or one after a test run) would log every product as "new"
    log_new = len(previous) >= 0.5 * len(rows)
    prices = []
    for r in rows:
        product = schema.as_dict(r)
        before = previous.get(product["product_id"])
        if before is None:
            if not log_new:
                continue
            change, old_price = schema.NEW, ""
        else:
            old_price = before.get("price", "")
            changed = []
            if not same_number(old_price, product["price"]):
                changed.append(schema.PRICE)
            if before.get("available", "").upper() != str(product["available"]).upper():
                changed.append(schema.BACK_IN_STOCK if product["available"] is True
                               else schema.OUT_OF_STOCK)
            if not changed:
                continue
            change = "+".join(changed)
        prices.append(schema.price_row(product, change, old_price))
    print(f"{len(prices)} changes, {schema.restocked_count(prices)} back in stock"
          + ("" if log_new else " (first full run: new products not logged)"), flush=True)
    return prices


def set_output(name, value):
    path = os.getenv("GITHUB_OUTPUT")
    if path:
        with open(path, "a") as f:
            f.write(f"{name}={value}\n")


# ---------------------------------------------------------------- main sweep

def page_jobs(leaf):
    return [(leaf, page, min(PAGE, leaf.rows - (page - 1) * PAGE))
            for page in range(1, math.ceil(leaf.rows / PAGE) + 1)]


def pack(jobs):
    """Requests of up to MAX_ALIASES page jobs and about PACK_ROWS rows, biggest first."""
    jobs = sorted(jobs, key=lambda j: -j[2])
    requests_, current, rows = [], [], 0
    for job in jobs:
        if current and (len(current) == MAX_ALIASES or rows + job[2] > PACK_ROWS):
            requests_.append(current)
            current, rows = [], 0
        current.append(job)
        rows += job[2]
    if current:
        requests_.append(current)
    return requests_


def page_size(leaf, size):
    if leaf.rows > PAGE:
        return PAGE  # every page of a multi-page partition must use the same size
    # Whole small partition in one page, with room for products added since planning
    return min(PAGE, size + 25) if leaf.rows == leaf.count else size


def fetch_variants(sku):
    """configurable_options + variants of one product with options."""
    data = gql("{" + products_query({"sku": sku}, 1, fields=VARIANT_FIELDS) + "}")
    items = data["products"]["items"]
    if not items:
        raise RuntimeError(f"product with options {sku} disappeared while fetching variants")
    return items[0]


def fetch_page_batch(batch):
    return run_aliased([
        products_query(leaf.filter, page_size(leaf, size), page, leaf.sort, PRODUCT_FIELDS)
        for leaf, page, size in batch])


def main():
    limit = int(os.getenv("MAX_PRODUCTS") or 0)
    workers = int(os.getenv("WORKERS") or 4)
    dry_run = os.getenv("DRY_RUN") == "1"
    scraped_dt = datetime.now(timezone.utc)
    scraped_at = scraped_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    started = time.time()

    # Read the previous run before scraping, so bad credentials fail in seconds
    previous = {}
    if not dry_run:
        db.check()
        previous = db.previous_run(SITE, "product_id", ["price", "available"])

    pool = ThreadPoolExecutor(workers)
    try:
        root = aggregations({})
        total = exact_count(root)
        price_total = sum(o["count"] for o in root["price"]["options"])
        spec_attrs = [a for a in root if a not in NOT_SPECS]
        spec_labels = {a: root[a]["label"] for a in spec_attrs}
        labels = option_labels(["condition", "brand"] + spec_attrs)
        print(f"catalog: {total} products (price buckets add up to {price_total}); "
              f"specs: {', '.join(spec_attrs)}", flush=True)
        planner = Planner(pool, list(labels["condition"]), list(labels["brand"]))

        leaves = planner.plan([({}, None)], PAGE)
        print(f"main sweep: {len(leaves)} partitions under the {CAP} cap", flush=True)

        tags = {}
        if limit:
            # Test run: a few products from many partitions, tags looked up for those only.
            # Alternate priced and "Call For Price" partitions (there are ~10x more of the latter)
            priced = [leaf for leaf in leaves if leaf.filter["price"][0] is not None]
            zero = [leaf for leaf in leaves if leaf.filter["price"][0] is None]
            for group in (priced, zero):
                random.Random(0).shuffle(group)
            mixed = [leaf for pair in zip(priced, zero) for leaf in pair]
            jobs, picked = [], 0
            for leaf in mixed:
                if picked >= limit:
                    break
                size = min(TEST_ROWS_PER_PARTITION, leaf.count, limit - picked)
                jobs.append((Leaf(leaf.filter, leaf.count, size, leaf.sort), 1, size))
                picked += size
        else:
            tags = sweep_tags(pool, planner, [({}, root)], labels, spec_attrs)
            print(f"tags done after {time.time() - started:.0f}s", flush=True)
            jobs = [job for leaf in leaves for job in page_jobs(leaf)]

        batches = pack(jobs)
        print(f"main sweep: {len(jobs)} pages in {len(batches)} requests", flush=True)
        seen, rows = set(), []
        items = {}  # test runs only: sku -> (item, condition), rows are built after the tags
        leaf_skus, leaf_totals, types = {}, {}, {}
        futures = {pool.submit(fetch_page_batch, b): b for b in batches}
        for done, fut in enumerate(as_completed(futures), 1):
            for (leaf, page, size), res in zip(futures[fut], fut.result()):
                key = leaf.group or id(leaf)
                leaf_totals[key] = (leaf.count if leaf.group else res["total_count"])
                skus = leaf_skus.setdefault(key, set())
                condition = labels["condition"].get(leaf.filter["condition"], "")
                for item in res["items"]:
                    skus.add(item["sku"])
                    if item["sku"] in seen:
                        continue
                    seen.add(item["sku"])
                    types[item["__typename"]] = types.get(item["__typename"], 0) + 1
                    if item["__typename"] == "ConfigurableProduct":
                        item.update(fetch_variants(item["sku"]))
                    if limit:
                        items[item["sku"]] = (item, condition)
                    else:
                        rows.extend(build_rows(item, condition, tags, spec_labels, scraped_at))
            if done % 25 == 0 or done == len(batches):
                print(f"main sweep: {done}/{len(batches)} requests, {len(seen)} products, "
                      f"{time.time() - started:.0f}s", flush=True)

        if limit:
            skus = list(items)
            bases = []
            for i in range(0, len(skus), 300):
                base = {"sku": skus[i:i + 300]}
                bases.append((base, aggregations(base)))
            tags = sweep_tags(pool, planner, bases, labels, spec_attrs)
            rows = [row for item, cond in items.values()
                    for row in build_rows(item, cond, tags, spec_labels, scraped_at)]
    finally:
        pool.shutdown(wait=False, cancel_futures=True)

    # ---- report
    print(f"\nproducts: {len(seen)} unique ({len(rows)} rows incl. variants); site total "
          f"{total}" + ("" if limit else f"; missing {total - len(seen)}"), flush=True)
    print(f"product types: {types}", flush=True)
    if not limit:
        short = {k: (leaf_totals[k], len(v)) for k, v in leaf_skus.items()
                 if len(v) != leaf_totals[k]}
        print(f"partitions with a different count than the site reported: {len(short)} "
              f"(net {sum(t - g for t, g in short.values())} products, usually items that "
              f"changed price or were added/removed during the run)", flush=True)
        print(f"products no filter can reach (no brand and one shared price): "
              f"{planner.unreachable}", flush=True)
    in_stock = sum(1 for r in rows if r[COLUMNS.index("available")] is True)
    print(f"in stock: {in_stock}/{len(rows)}", flush=True)
    for c in ("brand", "category", "condition", "capacity", "interface", "form_factor",
              "spindle_speed", "cache", "price", "compare_at_price", "price_per_tb",
              "stock_qty", "other_specs"):
        filled = sum(1 for r in rows if r[COLUMNS.index(c)] != "")
        print(f"  {c}: filled in {filled}/{len(rows)} rows", flush=True)
    print(f"runtime: {time.time() - started:.0f}s", flush=True)

    if dry_run:
        for row in random.Random(1).sample(rows, min(4, len(rows))):
            print(json.dumps(dict(zip(COLUMNS, row)), indent=2))
    db_error = None
    if not dry_run:
        db_error = db.save_run(SITE, rows, scraped_dt, total, full_run=not limit)
        prices = find_changes(rows, previous)
        set_output("restocked", schema.restocked_count(prices))
        db_error = db_error or db.save_prices(prices, scraped_dt, full_run=not limit)
    set_output("rows", len(rows))
    set_output("products", total)
    set_output("in_stock", in_stock)
    if db_error:
        sys.exit(f"Saving to the database failed: {db_error}")


if __name__ == "__main__":
    main()
