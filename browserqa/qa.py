"""QA in a real browser, from GitHub's US servers, for sites the dashboard's daily QA can't judge.

The dashboard (server-scrapers-ui, Vercel Cron, Frankfurt) re-fetches 25 random products of each
site's newest run and compares them with what was saved. Some sites it can't judge from there:
stores that show buyers outside the US other prices or stock (naddod, staples), stores whose bot
wall turns servers away (avoira, serverbasket, directdial, insidesystems), and pages that draw
their price with JavaScript (hddprime, uptimumtech, stor3d). Here each product page is opened in Chrome from the US, like
their scrapers, and read as a buyer sees it.

The same rules as the dashboard's QA (src/lib/qa.ts there):
  - 25 random products of the site's newest saved full run
  - the price: the page's JSON-LD offer (the one with the product's SKU, if it lists several),
    else its price meta tags, else its first visible price; a product saved without a price
    whose page asks for a quote matches
  - prices to the cent; stores priced in another currency within 0.2% (conversion rounding)
  - stock only when both sides know it (the page's JSON-LD or meta availability)
  - passes when at most 1 in 10 of the products it could check differ
  - one INSERT per site into qa_checks with the qa_writer role, so its rows share checked_at

Env vars: DATABASE_URL, QA_DB_PASSWORD; QA_SITES (comma-separated, default SITES); QA_DRY_RUN=1
prints the results without saving them. The "QA in a browser" workflow runs it daily after the
dashboard's QA, and by hand.
"""
import os
import re
import sys
import time
from urllib.parse import unquote, urlparse

import psycopg
from playwright.sync_api import sync_playwright

from common import db

SITES = ["naddod", "staples", "avoira", "serverbasket", "hddprime", "uptimumtech", "stor3d", "directdial", "insidesystems"]
SAMPLE = int(os.getenv("QA_SAMPLE") or 25)  # fewer only for trying it out
MAX_MISMATCH = 0.1
MIN_CHECKED = 10
MISMATCHES = ("price", "stock", "price+stock")
# Stores priced in another currency (the dashboard's src/lib/currency.ts): their pages and data can
# round a converted price a unit apart.
OTHER_CURRENCY = {"avoira", "uptimumtech"}
CURRENCY_ROUNDING = 0.002
QUOTE_ONLY = re.compile(r"request (a )?quote|call for pric|contact (us )?for pric|price on request|get a quote|"
                        r"ask for (a )?price|quote only", re.I)
BLOCKED = re.compile(r"just a moment|access denied|attention required|verify you are human|forbidden", re.I)

# What the page shows: its JSON-LD offers, price meta tags, and visible prices (not crossed out,
# not in menus or other products' lists), top of the page first.
READ_PAGE = """() => {
  const ld = [];
  for (const sc of document.querySelectorAll('script[type="application/ld+json"]')) {
    try {
      const walk = (o, sku) => {
        if (!o || typeof o !== "object") return;
        if (Array.isArray(o)) return o.forEach((x) => walk(x, sku));
        const own = typeof o.sku === "string" ? o.sku : typeof o.mpn === "string" ? o.mpn : sku;
        if (o["@type"] === "Offer" || o["@type"] === "AggregateOffer")
          ld.push({ price: o.price ?? o.lowPrice ?? null, availability: String(o.availability || ""), sku: own || "" });
        Object.values(o).forEach((v) => walk(v, own));
      };
      walk(JSON.parse(sc.textContent), "");
    } catch {}
  }
  const meta = (n) => document.querySelector(`meta[property="${n}"],meta[name="${n}"],meta[itemprop="${n}"]`)?.content ?? null;
  const shown = (el) => { const r = el.getBoundingClientRect(); const st = getComputedStyle(el); return r.width > 0 && r.height > 0 && st.visibility !== "hidden" && st.display !== "none"; };
  const prices = [];
  for (const el of document.querySelectorAll('[class*="price" i], [id*="price" i], [itemprop="price"]')) {
    if (!shown(el) || el.closest("del, s, nav, header, footer, [class*='old' i], [class*='was' i], [class*='compare' i], [class*='regular' i], [class*='related' i], [class*='upsell' i], [class*='recent' i], [class*='cart' i]")) continue;
    const text = (el.innerText || "").trim().replace(/\\s+/g, " ");
    if (text && /\\d/.test(text) && text.length < 40) prices.push({ text, top: el.getBoundingClientRect().top + window.scrollY });
  }
  prices.sort((a, b) => a.top - b.top);
  return {
    title: document.title,
    ld,
    metaPrice: meta("product:price:amount") ?? meta("og:price:amount"),
    metaAvailability: meta("product:availability") ?? meta("og:availability"),
    prices: prices.slice(0, 5).map((p) => p.text),
    text: document.body.innerText.slice(0, 30000),
  };
}"""


class Unclear(Exception):
    """The product can't be checked (page gone, or no price shown): not a mismatch."""


def amount(text):
    """The first amount in a price text ("£1,234.56", "1.234,56 €", "₹25,499"), or None."""
    if text is None:
        return None
    if isinstance(text, (int, float)):
        return float(text)
    m = re.search(r"\d[\d.,\s  ]*", str(text))
    if not m:
        return None
    raw = re.sub(r"[\s  ]", "", m.group(0)).rstrip(".,")
    if "," in raw and "." in raw:
        raw = raw.replace(",", "") if raw.rfind(".") > raw.rfind(",") else raw.replace(".", "").replace(",", ".")
    elif "," in raw:
        raw = raw.replace(",", "") if re.search(r",\d{3}$", raw) else raw.replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return None


def in_stock(value):
    """In stock, out of stock, or None. Back-ordered and pre-order products can be ordered but
    aren't on the shelf: stores and scrapers call them either way, so they aren't compared."""
    s = re.sub(r"\s+", "", str(value or "")).lower()
    if not s:
        return None
    if re.search(r"instock|in_stock|limitedavailability|onlineonly", s):
        return True
    if re.search(r"outofstock|out_of_stock|soldout|discontinued", s):
        return False
    return None


def key(v):
    return (v or "").strip().upper()


def live_product(page, row):
    """The product as its page shows it now, to a buyer in the US."""
    if not (row["url"] or "").startswith("http"):
        raise Unclear("no product page")
    response = page.goto(row["url"], wait_until="domcontentloaded", timeout=60_000)
    if response and response.status in (404, 410):
        raise Unclear(f"page gone ({response.status})")
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except Exception:  # noqa: BLE001 - pages that keep polling: read them as they are
        pass
    seen = page.evaluate(READ_PAGE)
    if BLOCKED.search(seen["title"] or "") or (response and response.status in (403, 429)):
        raise RuntimeError(f"blocked ({response.status if response else '?'}: {seen['title'][:60]})")
    if response and response.status >= 500:
        raise RuntimeError(f"HTTP {response.status}")

    offers = [o for o in seen["ld"] if amount(o["price"]) is not None]
    mine = [o for o in offers if key(o["sku"]) and key(o["sku"]) in (key(row["sku"]), key(row["part_number"]))]
    offer = mine[0] if mine else offers[0] if offers and len({amount(o["price"]) for o in offers}) == 1 else None
    availability = (in_stock(offer["availability"]) if offer else None)
    if availability is None:
        availability = in_stock(seen["metaAvailability"])
    if availability is None:
        availability = next((in_stock(o["availability"]) for o in seen["ld"] if in_stock(o["availability"]) is not None), None)
    # A product saved without a price whose page asks for a quote: the price and stock in the
    # page's data are ones the store hides ("Contact for pricing / availability").
    if not row["price"] and QUOTE_ONLY.search(seen["text"]):
        return {"price": None, "available": None}
    price = amount(offer["price"]) if offer else amount(seen["metaPrice"])
    if price is None and len(offers) > 1:
        raise Unclear("the page lists several prices")
    if price is None:
        price = next((amount(p) for p in seen["prices"] if amount(p)), None)
    if price is None and (row["price"] or availability is None):
        raise Unclear("no price on the page")
    return {"price": price, "available": availability}


def compare(row, live, tolerance):
    saved = float(row["price"]) if row["price"] else None
    now = live["price"] or None
    price_ok = saved == now if saved is None or now is None else abs(saved - now) <= max(0.01, saved * tolerance)
    stock_ok = row["available"] is None or live["available"] is None or row["available"] == live["available"]
    if price_ok and stock_ok:
        return "match"
    if not price_ok and not stock_ok:
        return "price+stock"
    return "stock" if price_ok else "price"


def qa_connect():
    """The qa_writer role on the same server as DATABASE_URL."""
    password = os.getenv("QA_DB_PASSWORD")
    if not password:
        sys.exit("QA_DB_PASSWORD is not set (add it under the repo's Settings > Secrets > Actions)")
    url = urlparse(os.environ["DATABASE_URL"])
    return psycopg.connect(host=url.hostname, port=url.port or 5432, dbname=unquote(url.path.lstrip("/")),
                           user="qa_writer", password=password, connect_timeout=30)


def status_of(results):
    checked = sum(1 for r in results if r == "match" or r in MISMATCHES)
    mismatched = sum(1 for r in results if r in MISMATCHES)
    if checked < MIN_CHECKED:
        return "not_checked"
    return "passed" if mismatched / checked <= MAX_MISMATCH else "failed"


def sample_of(conn, site):
    """The site's newest saved full run and 25 random products of it, or (None, [])."""
    last = conn.execute("SELECT run_id FROM scrape_runs WHERE site = %s AND full_run AND status = 'saved' "
                        "ORDER BY logged_at DESC LIMIT 1", (site,)).fetchone()
    if not last:
        return None, []
    cur = conn.execute(f'SELECT product_id, sku, part_number, url, price, available FROM "{site}" '
                       "WHERE run_id = %s AND full_run ORDER BY random() LIMIT %s", (last[0], SAMPLE))
    names = [c.name for c in cur.description]
    return last[0], [dict(zip(names, r)) for r in cur]


def check_site(browser, site, run_id, sample):
    tolerance = CURRENCY_ROUNDING if site in OTHER_CURRENCY else 0
    context = browser.new_context(locale="en-US", viewport={"width": 1366, "height": 1000})
    page = context.new_page()
    checks = []
    for row in sample:
        live, note = None, None
        try:
            live = live_product(page, row)
            result = compare(row, live, tolerance)
        except Unclear as e:
            result, note = "unclear", str(e)
        except Exception as e:  # noqa: BLE001 - a store error is a result, not a crash
            result, note = "error", str(e).splitlines()[0][:300]
        checks.append((row, live, result, note))
        now = "-" if live is None else f"{live['price']} {live['available']}"
        print(f"  {row['product_id']}: saved {row['price']} {row['available']} -> {now}: {result}"
              + (f" ({note})" if note else ""), flush=True)
        time.sleep(1)
    context.close()

    if os.getenv("QA_DRY_RUN"):
        print(f"{site} run {run_id}: {status_of([c[2] for c in checks])} (dry run, not saved)", flush=True)
        return
    values = [(site, run_id, row["product_id"], row["url"], "page" if live else None, row["price"],
               live["price"] if live else None, row["available"], live["available"] if live else None,
               result, note) for row, live, result, note in checks]
    placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"] * len(values))
    with qa_connect() as conn:
        conn.execute("INSERT INTO qa_checks (website, run_id, product_id, url, source, saved_price, live_price,"
                     f" saved_available, live_available, result, note) VALUES {placeholders}",
                     [v for row in values for v in row])
    tally = {}
    for _, _, result, _ in checks:
        tally[result] = tally.get(result, 0) + 1
    print(f"{site} run {run_id}: {status_of([c[2] for c in checks])} {tally}", flush=True)


def main():
    sites = [s.strip() for s in (os.getenv("QA_SITES") or ",".join(SITES)).split(",") if s.strip()]
    with db.connect() as conn:
        samples = {site: sample_of(conn, site) for site in sites}
    with sync_playwright() as p:
        browser = p.chromium.launch(channel=os.getenv("QA_BROWSER_CHANNEL", "chrome"), headless=True)
        for site in sites:
            run_id, sample = samples[site]
            if not sample:
                print(f"{site}: no saved full run, skipped", flush=True)
                continue
            print(f"{site} (run {run_id}):", flush=True)
            try:
                check_site(browser, site, run_id, sample)
            except Exception as e:  # noqa: BLE001 - one site failing doesn't stop the others
                print(f"{site}: QA failed: {e}", flush=True)
        browser.close()


if __name__ == "__main__":
    main()
