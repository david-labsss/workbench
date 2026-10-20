"""Scrape compudevices.com (custom PHP store, ~350-450k products): the products of the part
numbers the other sites carry, looked up one by one; every run's snapshot appended to Postgres.

The store has no API, product feed or product sitemap, and its category listings stop at their
first 10 pages (~90 products): deeper pages show random products whatever the sort or filter, so
the whole catalogue can't be listed. Its search finds a part number exactly, though
(/catalog-search/filter/q/<part number>, the URL its own search box builds). So every run
searches:
  - the part numbers (part_number / model_number, compared without case or punctuation) that at
    least MIN_SITES of the other sites carry in their latest full run: the products worth
    comparing (~50k searches; 91% of a sample was found);
  - the part numbers that found products in this site's last full run, so a product whose part
    number drops off that list isn't taken for out of stock.
A result is kept when its title starts with the part number, as the store's titles do
("P19907-B21 HPE 3.84TB SFF ..."). Results that only mention it ("1KK8W Dell ... QLE2662") are
other products.

Each result gives the product id (its add-to-cart button), title (the store cuts it at ~80
characters), brand, condition, price and URL. "Call For Price" results have no price or button:
they are saved as not available, with the id read from their product page. The store shows no
stock counts. There's no listed product count to check against, so a run where more than 1% of
the searches fail stops without saving.

Run steps, changes and env vars: common/runner.py (MAX_PRODUCTS = number of searches here).
Columns: common/schema.py.
"""
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote

from bs4 import BeautifulSoup
from psycopg import sql

from common import db, http, runner, schema
from db.setup import SITES

SITE = "compudevices"
BASE = "https://www.compudevices.com"
MIN_SITES = 5        # search the part numbers at least this many other sites carry
MIN_LENGTH = 5       # shorter ones are mostly model names (R640, Z440) found in many titles
WORKERS = 4
MAX_FAILED = 0.01    # share of searches that may fail before the run stops without saving
WORD_RE = re.compile(r"[\s/,;|()\[\]]+")
PRICE_RE = re.compile(r"[\d,]+(?:\.\d+)?")
PAGE_ID_RE = re.compile(r'name="Reviews\[product_id\]" value="(\d+)"')


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def key(text):
    """A part number without case or punctuation (P19907-B21 -> P19907B21)."""
    return re.sub(r"[^A-Z0-9]", "", (text or "").upper())


def part_numbers():
    """[(part number, key)] to search, the most widely carried first."""
    with db.connect() as conn:
        runs = dict(conn.execute(
            "SELECT DISTINCT ON (site) site, run_id FROM scrape_runs "
            "WHERE full_run AND status = 'saved' AND site = ANY(%s) "
            "ORDER BY site, logged_at DESC", (SITES,)).fetchall())
        own_run = runs.pop(SITE, None)
        values = sql.SQL(" UNION ALL ").join(
            sql.SQL("SELECT {} AS site, v FROM {}, unnest(ARRAY[part_number, model_number]) v "
                    "WHERE run_id = {}").format(sql.Literal(site), sql.Identifier(site),
                                                sql.Literal(run_id))
            for site, run_id in runs.items())
        # Each in the spelling most rows use: the store's search needs the dashes
        wanted = conn.execute(sql.SQL(
            "SELECT mode() WITHIN GROUP (ORDER BY v), k FROM ("
            "SELECT site, v, upper(regexp_replace(v, '[^A-Za-z0-9]', '', 'g')) AS k "
            "FROM ({}) x WHERE v <> '') y WHERE length(k) >= {} "
            "GROUP BY k HAVING count(DISTINCT site) >= {} "
            "ORDER BY count(DISTINCT site) DESC, k").format(
            values, sql.Literal(MIN_LENGTH), sql.Literal(MIN_SITES))).fetchall()
        found = conn.execute(
            sql.SQL("SELECT DISTINCT part_number FROM {} WHERE run_id = %s").format(
                sql.Identifier(SITE)), (own_run,)).fetchall() if own_run else []
    todo = {k: pn for pn, k in wanted}
    kept = 0
    for (pn,) in found:
        if pn and key(pn) not in todo:
            todo[key(pn)] = pn
            kept += 1
    print(f"{len(wanted)} part numbers on {MIN_SITES}+ of {len(runs)} other sites, "
          f"+{kept} that found products last run", flush=True)
    return [(pn, k) for k, pn in todo.items()]


def get(url):
    r = http.session().get(url, timeout=60)
    if r.status_code == 404:
        return ""
    r.raise_for_status()
    return r.text


def search_url(part_number):
    # A "/" in the path gives a 404, so it is searched as a space
    return f"{BASE}/catalog-search/filter/q/{quote(part_number.replace('/', ' '), safe='')}"


def starts_with(title, k):
    """Whether the title's first words are the part number (punctuation aside)."""
    joined = ""
    for word in WORD_RE.split(title)[:3]:
        joined += key(word)
        if joined == k:
            return True
        if len(joined) >= len(k):
            return False
    return False


def results(html):
    """The search results: [{id, title, brand, condition, price, url}], id None for "Call For
    Price" products (no add-to-cart button)."""
    start = html.find('<div class="product-default')
    if start < 0:
        return []
    soup = BeautifulSoup(html[start:html.rfind("<!-- End product default -->")], "html.parser")
    out = []
    for card in soup.select("div.product-default"):
        link = card.select_one("h3.product-title a")
        if not link:
            continue
        button = card.select_one("button[data-id]")
        brand = card.select_one(".product-card-meta a")
        condition = ""
        for li in card.select(".product-card-meta li"):
            text = clean(li.get_text(" "))
            if text.lower().startswith("condition"):
                condition = text[len("condition"):].strip()
        price = card.select_one(".product-price")
        m = PRICE_RE.search(price.get_text()) if price else None
        img = card.select_one("figure img")
        image_url = (img.get("src") or img.get("data-src")) if img else ""  # images.compudevices.com, absolute
        out.append({
            "id": button.get("data-id") if button else None,
            "title": clean(link.get_text()),
            "brand": clean(brand.get_text()) if brand else "",
            "condition": condition[:1].upper() + condition[1:],
            "price": float(m.group(0).replace(",", "")) if m else None,
            "available": bool(button),
            "image_url": image_url or "",
            "url": link.get("href", ""),
        })
    return out


def lookup(item):
    """The products whose title starts with this part number, or None if the search failed."""
    pn, k = item
    try:
        found = [r for r in results(get(search_url(pn))) if starts_with(r["title"], k)]
        for r in found:
            if not r["id"]:
                # "Call For Price": the id is in the product page's review form
                m = PAGE_ID_RE.search(get(r["url"]))
                r["id"] = m.group(1) if m else None
                if not r["id"]:
                    print(f"WARN no product id on {r['url']}", flush=True)
        return [r for r in found if r["id"]]
    except Exception as e:  # noqa: BLE001 - one bad search must not stop the run
        print(f"WARN {pn}: {e}", flush=True)
        return None


def scrape(limit, scraped_at):
    todo = part_numbers()
    todo = todo[:limit] if limit else todo
    rows, seen, failed = [], set(), []

    def add(item, found):
        for r in found:
            if r["id"] in seen:     # one product can start with two searched part numbers
                continue
            seen.add(r["id"])
            rows.append(schema.product_row({
                "scraped_at": scraped_at,
                "website": SITE,
                "product_id": r["id"],
                "part_number": item[0],
                "title": r["title"],
                "brand": r["brand"],
                "condition": r["condition"],
                "price": r["price"],
                "available": r["available"],
                "image_url": r.get("image_url") or None,
                # the search-result card has no description; the product page isn't fetched
                "description": None,
                "url": r["url"],
            }))

    with ThreadPoolExecutor(WORKERS) as pool:
        for i, (item, found) in enumerate(zip(todo, pool.map(lookup, todo)), 1):
            if found is None:
                failed.append(item)
            else:
                add(item, found)
            if i % 5000 == 0 or i == len(todo):
                print(f"{i}/{len(todo)} searches, {len(rows)} products, {len(failed)} failed",
                      flush=True)
    # One more try for the searches that failed (e.g. a dropped connection)
    retried = [(item, lookup(item)) for item in failed]
    failed = [item for item, found in retried if found is None]
    for item, found in retried:
        if found is not None:
            add(item, found)
    if len(failed) > MAX_FAILED * len(todo):
        sys.exit(f"{len(failed)} of {len(todo)} searches failed; not saving a partial snapshot")
    return rows, None


if __name__ == "__main__":
    runner.run(SITE, scrape)
