"""Scrape servertechcentral.com -- the server-PARTS subset only (~132k products).

The store's full catalogue is ~554k items, but most of it is a 474k "Components" catch-all plus
servers, networking gear, racks/PDUs/cables, cooling, batteries, licences, etc. We keep ONLY the
real replaceable server-part categories:

    memory ~53k, processors ~13.6k, storage ~33k (SAS/SATA HDDs + SSDs + NVMe + tape),
    GPUs ~12.7k, power supplies ~8.4k, network cards ~4.2k, motherboards ~3.5k,
    RAID controllers ~2.8k   ->   ~132k parts total.

Platform: a custom React/Next.js storefront behind Cloudflare (a normal browser User-Agent
passes, no challenge). Its data API lives under /api/, which robots.txt disallows for every
crawler, so we do NOT touch it. We don't need to: each product page /product/<slug> is
server-rendered with a JSON-LD Product block (name, sku, mpn, brand, image, description,
offers.price in USD, offers.availability) and a BreadcrumbList that gives the product's
category path, e.g. Home > Components > Memory > <product> or Home > Storage > SSDs > <product>.

WHY we must read every product page to pick the parts (the category gotcha):
  * The category listing pages (/category/<slug>) IGNORE ?page= -- every page returns the same
    first 12 products -- so a category CANNOT be enumerated by paging it.
  * The sitemap is the only reliable full enumeration (/sitemap.xml -> 56 /sitemap-products-N.xml
    parts, ~554k <loc>+<lastmod>), but a sitemap entry carries no category.
  * So the category comes from each product page's own BreadcrumbList. We fetch every product
    page, read its breadcrumb category slugs, and keep the product only when it is a part:
        keep if  leaf slug in {memory, processors, gpus, power-supplies, network-cards,
                               motherboards, raid-controllers}   (these sit under "components")
             or  "storage" is anywhere in the path  (covers sas-sata-hdds, ssds, nvme, tape).
    Keying components on the LEAF (not on the "components" top-level) is what drops the 474k of
    fans/heatsinks/risers/batteries/cooling junk that the old "scrape everything" run pulled in.

`listed` (the completeness target the runner's 98% guard uses) is read live from the 8 category
pages' own product counts ("Browse N products"), summed -> the number of PARTS we expect to keep,
so the guard protects the parts subset, not the full 554k.

A product page is ~25 KB and answers in well under a second. A full cold crawl of all ~554k pages
(to classify them) at 16-24 workers is ~5 h, which fits one GitHub Actions job. Three run modes
share this one scrape(), all going through common/runner.py:

  FULL (default)          Fetch every product page, keep the parts. listed = live parts count, so
                          the 98%-complete guard applies. The reliable baseline / reconciliation.

  SHARD  SHARD_COUNT=N    Fetch only shard SHARD_INDEX (0-based) of N, striped across the whole
         SHARD_INDEX=i    URL list (entries[i::N]), keep the parts in it. listed = None (partial
                          snapshot, guard skipped). Spreads a one-time history backfill over jobs.
                          Run with MAX_PRODUCTS set huge so runner marks it full_run=false.

  DIFF   DIFF=1           Re-fetch only products whose sitemap <lastmod> is recent (DIFF=1 = since
         (or DIFF_DAYS=N) the last full run, DIFF_DAYS=N = last N days) or that have no <lastmod>;
                          every other part is carried forward unchanged from the previous full run,
                          and parts gone from the sitemap are dropped. Result is a complete parts
                          snapshot (listed = live parts count) built from a small number of fetches.
                          Falls back to a FULL crawl when there is no usable previous full run.
                          NOTE: trusts <lastmod> to move on a price/stock change; run a FULL crawl
                          periodically (e.g. weekly) to reconcile.

Currency is USD (offers.priceCurrency, confirmed on the pages). Prices are stored as-is; a missing
or $0 / quote-only price becomes empty (never 0). The pages don't publish a stock count or a
compare-at price, and availability is almost always InStock on this catalogue. Run steps, change
detection and env vars (DATABASE_URL, MAX_PRODUCTS, DRY_RUN): common/runner.py. Columns:
common/schema.py.
"""
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from bs4 import BeautifulSoup

from common import db, http, runner, schema

try:
    from curl_cffi import requests as cffi   # clears Cloudflare's passive bot check from runner IPs
except ImportError:
    cffi = None

SITE = "servertechcentral"
BASE = "https://servertechcentral.com"
SITEMAP = BASE + "/sitemap.xml"
WORKERS = int(os.getenv("STC_WORKERS") or 16)   # product pages fetched at once

COLUMNS = schema.PRODUCT_COLUMNS

# --- the parts filter --------------------------------------------------------
# Leaf category slugs that live under the "components" top-level and ARE parts we keep.
# (We deliberately do NOT keep on the "components" top-level itself: it is a 474k catch-all
# that also holds fans/heatsinks, boards-risers, batteries, cooling, etc. -- the junk.)
COMPONENT_LEAVES = {
    "memory", "processors", "gpus", "power-supplies",
    "network-cards", "motherboards", "raid-controllers",
}
# Anything under the "storage" top-level is a part (sas-sata-hdds, ssds, nvme, tape-backup, ...).
STORAGE_TOP = "storage"
# The 8 category pages whose live product counts sum to the expected parts total (`listed`).
LISTED_CATEGORIES = sorted(COMPONENT_LEAVES) + [STORAGE_TOP]

CAPACITY_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(TB|GB|MB)\b", re.I)
SPINDLE_RE = re.compile(r"\b(\d{4,5})\s*RPM\b", re.I)
FORM_RE = re.compile(r"\b(2\.5|3\.5|1\.8)[- ]?inch\b", re.I)
INTERFACE_RE = re.compile(
    r"\b(SAS|SATA|NVMe|PCIe|U\.2|U\.3|M\.2|FC|Fibre Channel|SCSI|ATA-\d+|IDE)\b", re.I)
CONDITION_RE = re.compile(r"\b(refurbished|recertified|renewed|open[- ]box|used|new)\b", re.I)


# --- sitemap -----------------------------------------------------------------
def _fetch(url, timeout):
    """Cloudflare 403s plain requests from datacenter IPs; curl_cffi impersonates Chrome's TLS."""
    if cffi is not None:
        return cffi.get(url, timeout=timeout, impersonate="chrome")
    return http.session().get(url, timeout=timeout)


def _get(url, timeout=120):
    r = _fetch(url, timeout)
    r.raise_for_status()
    return r.text


def product_sitemaps():
    """The product sub-sitemap URLs listed in the sitemap index."""
    text = _get(SITEMAP)
    return [u for u in re.findall(r"<loc>([^<]+)</loc>", text) if "sitemap-products" in u]


def catalogue():
    """[(slug, lastmod)] for every product in the sitemap, lastmod as 'YYYY-MM-DD' (or '')."""
    entries = []
    for sm in product_sitemaps():
        text = _get(sm)
        for block in re.findall(r"<url>(.*?)</url>", text, re.S):
            loc = re.search(r"<loc>([^<]+)</loc>", block)
            if not loc or "/product/" not in loc.group(1):
                continue
            slug = loc.group(1).rsplit("/product/", 1)[-1].strip().rstrip("/")
            lm = re.search(r"<lastmod>([^<]+)</lastmod>", block)
            entries.append((slug, (lm.group(1).strip()[:10] if lm else "")))
    # one slug per product, keep the newest lastmod seen for it
    best = {}
    for slug, lm in entries:
        if slug and (slug not in best or lm > best[slug]):
            best[slug] = lm
    return sorted(best.items())


def parts_listed():
    """Expected number of PARTS = sum of the 8 parts category pages' own product counts, read
    live. Returns None if any page can't be counted (then runner skips the completeness guard
    rather than abort a long crawl on a transient hiccup)."""
    total, got = 0, 0
    for c in LISTED_CATEGORIES:
        try:
            text = _get(f"{BASE}/category/{c}", timeout=45)
        except Exception as e:  # noqa: BLE001
            print(f"WARN category/{c} count: {e}", flush=True)
            continue
        m = re.search(r"Browse\s+([\d,]+)\s+products", text) or re.search(r'"total":(\d+)', text)
        if m:
            total += int(m.group(1).replace(",", ""))
            got += 1
        else:
            print(f"WARN category/{c}: no product count found", flush=True)
    if got < len(LISTED_CATEGORIES):
        print(f"parts count incomplete ({got}/{len(LISTED_CATEGORIES)} categories); "
              "completeness guard will be skipped", flush=True)
        return None
    print(f"parts categories list {total} products", flush=True)
    return total


# --- product pages -----------------------------------------------------------
def fetch(slug):
    url = f"{BASE}/product/{slug}"
    try:
        r = _fetch(url, 45)
        if r.status_code == 404:
            return slug, None
        r.raise_for_status()
        return slug, r.text
    except Exception as e:  # noqa: BLE001 - one bad page must not stop the crawl
        print(f"WARN {url}: {e}", flush=True)
        return slug, None


def _clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def _ld_blocks(html):
    """Every JSON-LD object on the page, flattening lists and @graph."""
    out = []
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            obj = json.loads(tag.string or "")
        except (ValueError, TypeError):
            continue
        items = obj if isinstance(obj, list) else [obj]
        for it in items:
            if isinstance(it, dict):
                out.extend(it["@graph"] if isinstance(it.get("@graph"), list) else [it])
    return out


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _first_image(value):
    if isinstance(value, list):
        value = value[0] if value else None
    if isinstance(value, dict):
        value = value.get("url")
    return _clean(value) if value else ""


def parse(slug, html, scraped_at):
    """Return (product_row, is_part). (None, False) when the page has no Product block."""
    blocks = _ld_blocks(html)
    product = next((b for b in blocks if b.get("@type") == "Product"), None)
    if not product:
        return None, False
    crumbs = next((b for b in blocks if b.get("@type") == "BreadcrumbList"), None)
    offers = product.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}

    # category path from the breadcrumb: slugs (for the parts filter) + leaf name (for the column)
    cat_slugs, cat_name = set(), ""
    if crumbs:
        items = [i for i in (crumbs.get("itemListElement") or []) if isinstance(i, dict)]
        for i in items:
            link = i.get("item") or ""
            if "/category/" in link:
                cat_slugs.add(link.rsplit("/category/", 1)[-1].strip().strip("/"))
        if len(items) >= 2:                       # the leaf is the product itself
            cat_name = _clean(items[-2].get("name"))
    is_part = bool(COMPONENT_LEAVES & cat_slugs) or (STORAGE_TOP in cat_slugs)

    title = _clean(product.get("name"))
    brand = product.get("brand")
    brand = _clean(brand.get("name") if isinstance(brand, dict) else brand)
    sku = _clean(product.get("sku"))
    mpn = _clean(product.get("mpn"))

    availability = _clean(offers.get("availability")).rsplit("/", 1)[-1].lower()
    available = availability in ("instock", "preorder", "limitedavailability", "onlineonly", "")
    price = _num(offers.get("price"))

    cond = CONDITION_RE.search(f"{title} {product.get('description', '')}")
    cap = CAPACITY_RE.search(title)
    spindle = SPINDLE_RE.search(title)
    form = FORM_RE.search(title)
    interface = INTERFACE_RE.search(title)

    row = schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": slug,
        "sku": sku,
        "part_number": mpn or sku,
        "title": title,
        "brand": brand,
        "category": cat_name,
        "condition": cond.group(1).title() if cond else "",
        "capacity": (cap.group(1) + cap.group(2).upper()) if cap else "",
        "interface": interface.group(1).upper() if interface else "",
        "form_factor": (form.group(1) + '"') if form else "",
        "spindle_speed": (spindle.group(1) + " RPM") if spindle else "",
        "price": price if price else None,        # no / $0 / quote price -> empty, not 0
        "available": available,
        "image_url": _first_image(product.get("image")),
        "description": schema.plain_text(product.get("description")),
        "url": f"{BASE}/product/{slug}",
    })
    return row, is_part


def fetch_rows(slugs, scraped_at, keep_limit=None):
    """Fetch and parse the given product slugs concurrently, keeping only PARTS. One row per
    product id. Stops early once keep_limit parts are collected (test runs)."""
    rows, seen = [], set()
    total = len(slugs)
    with ThreadPoolExecutor(WORKERS) as pool:
        for i, (slug, html) in enumerate(pool.map(fetch, slugs), 1):
            if html:
                row, is_part = parse(slug, html, scraped_at)
                if row and is_part:
                    pid = row[COLUMNS.index("product_id")]
                    if pid not in seen:
                        seen.add(pid)
                        rows.append(row)
            if i % 5000 == 0 or i == total:
                print(f"{i}/{total} pages, {len(rows)} parts kept", flush=True)
            if keep_limit and len(rows) >= keep_limit:
                print(f"{i} pages, {len(rows)} parts kept (test limit reached)", flush=True)
                break
    return rows


# --- diff (carry forward unchanged parts from the previous full run) ----------
def _as_text(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    text = str(value)
    if re.fullmatch(r"-?\d+\.\d+", text):         # trim trailing zeros like the store sheet did
        text = text.rstrip("0").rstrip(".")
    return text


def previous_full_run():
    """The last saved full run as ({product_id: {column: text}}, run_date 'YYYY-MM-DD'), or
    ({}, None) if there is none. All columns, so carried rows re-insert identically. These are
    already the PARTS subset, since that is all a full run of this scraper saves."""
    with db.connect() as conn:
        last = conn.execute(
            "SELECT run_id, scraped_at FROM scrape_runs WHERE site = %s AND full_run "
            "AND status = 'saved' ORDER BY logged_at DESC LIMIT 1", (SITE,)).fetchone()
        if not last:
            return {}, None
        run_id, scraped = last
        cols = ", ".join(f'"{c}"' for c in COLUMNS)
        cur = conn.execute(
            f'SELECT {cols} FROM "{SITE}" WHERE run_id = %s AND full_run', (run_id,))
        prev = {}
        for row in cur:
            d = {c: _as_text(v) for c, v in zip(COLUMNS, row)}
            prev[d["product_id"]] = d
    run_date = scraped.strftime("%Y-%m-%d") if hasattr(scraped, "strftime") else str(scraped)[:10]
    print(f"previous full run {run_id}: {len(prev)} parts, dated {run_date}", flush=True)
    return prev, run_date


def diff_cutoff(run_date):
    """The 'YYYY-MM-DD' on/after which a changed <lastmod> means a product must be re-fetched."""
    days = os.getenv("DIFF_DAYS")
    if days:
        return (datetime.now(timezone.utc) - timedelta(days=int(days))).strftime("%Y-%m-%d")
    # DIFF=1: everything changed on or after the previous run's day (minus a day of slack)
    base = datetime.strptime(run_date, "%Y-%m-%d") if run_date else datetime.now(timezone.utc)
    return (base - timedelta(days=1)).strftime("%Y-%m-%d")


# --- entry point -------------------------------------------------------------
def scrape(limit, scraped_at):
    entries = catalogue()                         # [(slug, lastmod)], sorted by slug
    total = len(entries)
    print(f"sitemap lists {total} products (all categories)", flush=True)

    shard_count = int(os.getenv("SHARD_COUNT") or 1)
    diff = os.getenv("DIFF") == "1" or bool(os.getenv("DIFF_DAYS"))

    # SHARD: one striped slice of the catalogue, parts kept, saved as a partial snapshot.
    if shard_count > 1:
        shard_index = int(os.getenv("SHARD_INDEX") or 0)
        slice_ = entries[shard_index::shard_count]
        if limit:
            slice_ = slice_[:limit]
        print(f"shard {shard_index + 1}/{shard_count}: {len(slice_)} products to classify",
              flush=True)
        return fetch_rows([s for s, _ in slice_], scraped_at), None

    # Test run: scan the front of the sitemap until we have `limit` parts (listed = None).
    if limit:
        cap = min(total, max(limit * 10, 300))
        print(f"test run: scanning up to {cap} pages for {limit} parts", flush=True)
        return fetch_rows([s for s, _ in entries[:cap]], scraped_at, keep_limit=limit), None

    listed = parts_listed()                       # live expected parts count (for the 98% guard)

    # DIFF: re-fetch only recently-changed products; carry the rest of the parts forward.
    if diff:
        prev, run_date = previous_full_run()
        if len(prev) > 0:
            cutoff = diff_cutoff(run_date)
            sitemap_slugs = {s for s, _ in entries}
            to_fetch = [s for s, lm in entries if (not lm) or lm >= cutoff]
            fetch_set = set(to_fetch)
            carry = [s for s in prev if s not in fetch_set and s in sitemap_slugs]
            print(f"diff since {cutoff}: {len(to_fetch)} to re-classify, "
                  f"{len(carry)} parts carried forward", flush=True)
            rows = fetch_rows(to_fetch, scraped_at)
            for slug in carry:                    # unchanged part: reuse the previous run's row
                rows.append(schema.product_row({**prev[slug], "scraped_at": scraped_at,
                                                 "website": SITE}))
            return rows, listed
        print("diff: no usable previous full run; doing a full crawl instead", flush=True)

    # FULL: classify every product page, keep the parts (listed = live parts count).
    return fetch_rows([s for s, _ in entries], scraped_at), listed


if __name__ == "__main__":
    runner.run(SITE, scrape)
