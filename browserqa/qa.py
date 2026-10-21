"""QA in a real browser, from GitHub's US servers, for sites the dashboard's daily QA can't judge.

The dashboard (server-scrapers-ui, Vercel Cron, Frankfurt) re-fetches 25 random products of each
site's newest run and compares them with what was saved. Some sites it can't judge from there:
stores that show buyers outside the US other prices or stock (naddod, staples, directhardwaresupply),
stores whose bot wall turns servers away (avoira, serverbasket, directdial, insidesystems,
goharddrive, bulkdevices, 4tekgear, pbtech), and pages that draw their price with JavaScript
(hddprime, uptimumtech, stor3d). Here each product page is opened in Chrome from the US, like their
scrapers, and read as a buyer sees it.

The same rules as the dashboard's QA (src/lib/qa.ts there):
  - 25 random products of the site's newest saved full run
  - the price: the page's JSON-LD offer (the one with the product's SKU, if it lists several),
    else its price meta tags, else its first visible price; a product saved without a price
    whose page asks for a quote matches (insidesystems: its Store API, see STORE_API)
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

# The dashboard's daily QA leaves these out (NO_LIVE_CHECK in its src/app/api/qa/run/route.ts): keep the two lists alike.
SITES = ["naddod", "staples", "avoira", "serverbasket", "hddprime", "uptimumtech", "stor3d", "directdial", "insidesystems",
         "directhardwaresupply", "goharddrive", "bulkdevices", "4tekgear", "pbtech"]
SAMPLE = int(os.getenv("QA_SAMPLE") or 25)  # fewer only for trying it out
MAX_MISMATCH = 0.1
MIN_CHECKED = 10
BLOCKED_STOP = 5
MISMATCHES = ("price", "stock", "price+stock")
# Stores priced in another currency (the dashboard's src/lib/currency.ts): their pages and data can
# round a converted price a unit apart.
OTHER_CURRENCY = {"avoira", "uptimumtech", "pbtech"}
CURRENCY_ROUNDING = 0.002
# Stores whose dollar prices are converted from their own currency at each day's rate and rounded
# to whole dollars (insidesystems, from Danish krone): a day's rate move shifts them a dollar or so.
DAILY_RATE = {"insidesystems"}
DAILY_RATE_MOVE = 0.005
QUOTE_ONLY = re.compile(r"request (a )?quote|call for pric|contact (us )?for pric|price on request|get a quote|"
                        r"ask for (a )?price|quote only", re.I)
# Stores whose product pages are cached in whatever currency their first visitor was shown
# (insidesystems shows each country its own): their price and stock come from their WooCommerce
# Store API instead, which answers in the visitor's currency - US dollars here, as their scraper reads it.
STORE_API = {"insidesystems": "https://insidesystems.com"}
# Stores that show euros to everyone until the visitor picks US dollars in the header, as their
# scrapers do (Magento + Hyva: its "USD - US Dollar" button posts this form). Their visible price
# box is what the scraper saves: the page's data keeps a price the box hides ("Get in touch").
USD_SWITCH = {"directhardwaresupply": "https://directhardwaresupply.com/"}
SWITCH_FORM = re.compile(r'hyva\.postForm\((\{"action":"[^"]*currency\\/switch\\/","data":\{"currency":"USD"[^}]*\}\})\)')
PAGE_CURRENCY = re.compile(r"style:\s*'currency',\s*currency:\s*'([A-Z]{3})'")
BLOCKED = re.compile(r"just a moment|access denied|attention required|verify you are human|forbidden", re.I)
# A bot check that lets a real browser through by itself after a few seconds (Cloudflare's "Just a moment...")
CHECKING = "() => !/just a moment|verify you are human|attention required/i.test(document.title)"

# What the page shows: its JSON-LD offers, price meta tags, and its visible prices after the
# product's title (<h1>), in page order, without crossed-out prices and leaving out menus and
# other products' lists (related, recently viewed, carousels, the cart).
READ_PAGE = """() => {
  const ld = [];
  // An offer without a price of its own may list it in priceSpecification: the list price
  // (crossed out on the page) and the one you pay.
  const specPrice = (o) => {
    const specs = [].concat(o.priceSpecification || []).filter((p) => p && p.price != null);
    const paid = specs.filter((p) => !/ListPrice|StrikethroughPrice|MSRP/i.test(String(p.priceType || "")));
    return (paid[0] ?? specs[0])?.price ?? null;
  };
  for (const sc of document.querySelectorAll('script[type="application/ld+json"]')) {
    try {
      const walk = (o, sku) => {
        if (!o || typeof o !== "object") return;
        if (Array.isArray(o)) return o.forEach((x) => walk(x, sku));
        const own = typeof o.sku === "string" ? o.sku : typeof o.mpn === "string" ? o.mpn : sku;
        if (o["@type"] === "Offer" || o["@type"] === "AggregateOffer")
          ld.push({ price: o.price ?? o.lowPrice ?? specPrice(o), availability: String(o.availability || ""), sku: own || "",
                   currency: String(o.priceCurrency || "") });
        Object.values(o).forEach((v) => walk(v, own));
      };
      walk(JSON.parse(sc.textContent), "");
    } catch {}
  }
  const meta = (n) => document.querySelector(`meta[property="${n}"],meta[name="${n}"],meta[itemprop="${n}"]`)?.content ?? null;
  const shown = (el) => { const r = el.getBoundingClientRect(); const st = getComputedStyle(el); return r.width > 0 && r.height > 0 && st.visibility !== "hidden" && st.display !== "none"; };
  const title = document.querySelector("h1");
  const afterTitle = (el) => !title || Boolean(title.compareDocumentPosition(el) & Node.DOCUMENT_POSITION_FOLLOWING);
  const crossedOut = "del, s, [class*='old' i], [class*='was' i], [class*='compare' i], [class*='regular' i]";
  const elsewhere = "nav, header, footer, [class*='related' i], [class*='upsell' i], [class*='cross-sell' i], [class*='recent' i], " +
    "[class*='carousel' i], [class*='slider' i], [class*='swiper' i], [class*='owl-' i], [class*='cart' i], [class*='widget' i], " +
    "[class*='product-item' i], [class*='product-grid' i]";
  const prices = [];
  for (const el of document.querySelectorAll('[class*="price" i], [id*="price" i], [itemprop="price"]')) {
    if (!shown(el) || !afterTitle(el) || el.closest(crossedOut) || el.closest(elsewhere)) continue;
    const copy = el.cloneNode(true);
    copy.querySelectorAll(crossedOut).forEach((x) => x.remove());
    const text = (copy.textContent || "").trim().replace(/\\s+/g, " ");
    if (text && /\\d/.test(text) && text.length < 40) prices.push(text);
  }
  return {
    title: document.title,
    ld,
    metaPrice: meta("product:price:amount") ?? meta("og:price:amount"),
    // schema.org microdata: <span itemprop="price" content="199.95">
    microPrice: (() => { const el = document.querySelector('[itemprop="price"]'); return el ? el.getAttribute("content") || el.textContent : null; })(),
    metaAvailability: meta("product:availability") ?? meta("og:availability"),
    prices: prices.slice(0, 5),
    // Magento's price box (USD_SWITCH stores)
    boxPrice: document.querySelector("[id^='product-price-'] .price")?.textContent?.trim() ?? null,
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
    # Thousands may be split by a no-break space ("1 234,56 €"), not by a plain one: in
    # "₹19,999.00 33% off" the 33 is another number.
    m = re.search(r"\d[\d.,  ]*", str(text))
    if not m:
        return None
    raw = re.sub(r"[  ]", "", m.group(0)).rstrip(".,")
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


STOCK_WORDING = re.compile(r".{0,50}(out of stock|in stock|sold out|unavailable|back ?order|pre-?order|ships? (in|within|today)|"
                           r"deliver|pick ?up).{0,50}", re.I | re.S)


def key(v):
    return (v or "").strip().upper()


def switch_to_usd(page, home):
    """Pick US dollars in the store's header, as its scraper does: the choice stays in the session."""
    # The page as the store sent it: the browser's copy of the DOM escapes the button's quotes
    html = page.goto(home, wait_until="domcontentloaded", timeout=60_000).text()
    shown_in = PAGE_CURRENCY.search(html)
    if shown_in and shown_in.group(1) == "USD":
        return
    form = SWITCH_FORM.search(html)
    if not form:
        raise RuntimeError(f"{home}: no 'USD - US Dollar' switch ({shown_in.group(1) if shown_in else '?'} shown)")
    with page.expect_navigation(timeout=60_000):
        page.evaluate(f"hyva.postForm({form.group(1)})")
    shown_in = PAGE_CURRENCY.search(page.goto(home, wait_until="domcontentloaded", timeout=60_000).text())
    if not shown_in or shown_in.group(1) != "USD":
        raise RuntimeError(f"switching to US dollars failed: the store shows {shown_in.group(1) if shown_in else '?'}")


def live_product(page, row, site):
    """The product as its page shows it now, to a buyer in the US."""
    if not (row["url"] or "").startswith("http"):
        raise Unclear("no product page")
    response = page.goto(row["url"], wait_until="domcontentloaded", timeout=60_000)
    if response and response.status in (404, 410):
        raise Unclear(f"page gone ({response.status})")
    status = response.status if response else None
    checked = False  # a bot check let us through
    try:
        if not page.evaluate(CHECKING):
            page.wait_for_function(CHECKING, timeout=30_000)
            checked = True
    except Exception:  # noqa: BLE001 - still checking (blocked, below), or the page moved on meanwhile
        pass
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except Exception:  # noqa: BLE001 - pages that keep polling: read them as they are
        pass
    seen = page.evaluate(READ_PAGE)
    if status == 403 and checked and not BLOCKED.search(seen["title"] or ""):
        status = 200  # the bot check loaded the page
    if BLOCKED.search(seen["title"] or "") or status in (403, 429):
        raise RuntimeError(f"blocked ({status or '?'}: {seen['title'][:60]})")
    if status and status >= 500:
        raise RuntimeError(f"HTTP {status}")

    offers = [o for o in seen["ld"] if amount(o["price"]) is not None]
    mine = [o for o in offers if key(o["sku"]) and key(o["sku"]) in (key(row["sku"]), key(row["part_number"]))]
    offer = mine[0] if mine else offers[0] if offers and len({amount(o["price"]) for o in offers}) == 1 else None
    availability = (in_stock(offer["availability"]) if offer else None)
    if availability is None:
        availability = in_stock(seen["metaAvailability"])
    if availability is None:
        availability = next((in_stock(o["availability"]) for o in seen["ld"] if in_stock(o["availability"]) is not None), None)
    if site in USD_SWITCH:
        shown_in = PAGE_CURRENCY.search(page.content())
        if not shown_in or shown_in.group(1) != "USD":
            raise RuntimeError(f"the page shows {shown_in.group(1) if shown_in else '?'}, not US dollars")
        return {"price": amount(seen["boxPrice"]), "available": availability, "from": f"price box {seen['boxPrice']!r}"}
    # A product saved without a price whose page asks for a quote: the price and stock in the
    # page's data are ones the store hides ("Contact for pricing / availability").
    if not row["price"] and QUOTE_ONLY.search(seen["text"]):
        return {"price": None, "available": None, "from": "asks for a quote"}
    # Where the price was read, for the log: a mismatch can then be told from a misread page
    wording = STOCK_WORDING.search(seen["text"])
    stock_note = f"; stock from {'data' if availability is not None else '-'}, page says {' '.join(wording.group(0).split())!r}" if wording else ""
    if offer:
        if offer["currency"] not in ("", "USD") and site not in OTHER_CURRENCY:
            raise RuntimeError(f"the page's data is in {offer['currency']}, not US dollars")
        price, source = amount(offer["price"]), f"JSON-LD {offer['price']}"
    elif seen["metaPrice"] is not None:
        price, source = amount(seen["metaPrice"]), f"meta {seen['metaPrice']}"
    else:
        price, source = amount(seen["microPrice"]), f"microdata {seen['microPrice']}"
    if price is None and len(offers) > 1:
        raise Unclear("the page lists several prices")
    if price is None:
        shown = next((p for p in seen["prices"] if amount(p)), None)
        price, source = amount(shown), f"shown {shown!r}"
    if price is None and (row["price"] or availability is None):
        near = re.search(r".{0,40}price.{0,40}", seen["text"], re.I | re.S)
        raise Unclear(f"no price on the page ({status}: {seen['title'][:50]}; "
                      f"{' '.join(near.group(0).split()) if near else 'no price wording'})")
    return {"price": price, "available": availability, "from": source + stock_note}


def store_api_product(page, base, row):
    """The product as the store's WooCommerce Store API gives it now, to a buyer in the US."""
    response = page.request.get(f"{base}/wp-json/wc/store/v1/products/{row['product_id']}", timeout=60_000)
    if response.status == 404:
        raise Unclear("product gone (404)")
    if response.status != 200:
        raise RuntimeError(f"HTTP {response.status}")
    product = response.json()
    prices = product.get("prices") or {}
    if prices.get("currency_code") != "USD":
        raise Unclear(f"the store answered in {prices.get('currency_code')}")
    raw = prices.get("price")
    price = int(raw) / 10 ** int(prices.get("currency_minor_unit") or 0) if raw and raw != "0" else None
    return {"price": price, "available": product.get("is_in_stock"), "from": "Store API"}


def compare(row, live, tolerance, unit=0.01):
    saved = float(row["price"]) if row["price"] else None
    now = live["price"] or None
    price_ok = saved == now if saved is None or now is None else abs(saved - now) <= max(unit, saved * tolerance)
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
    tolerance = CURRENCY_ROUNDING if site in OTHER_CURRENCY else DAILY_RATE_MOVE if site in DAILY_RATE else 0
    unit = 1.0 if site in DAILY_RATE else 0.01
    context = browser.new_context(locale="en-US", viewport={"width": 1366, "height": 1000})
    page = context.new_page()
    if site in USD_SWITCH:
        switch_to_usd(page, USD_SWITCH[site])
    checks = []
    for row in sample:
        live, note = None, None
        try:
            live = store_api_product(page, STORE_API[site], row) if site in STORE_API else live_product(page, row, site)
            result = compare(row, live, tolerance, unit)
        except Unclear as e:
            result, note = "unclear", str(e)
        except Exception as e:  # noqa: BLE001 - a store error is a result, not a crash
            result, note = "error", str(e).splitlines()[0][:300]
        checks.append((row, live, result, note))
        now = "-" if live is None else f"{live['price']} {live['available']}"
        if result != "match" and live:
            note = f"from {live['from']}"[:300]
        print(f"  {row['product_id']}: saved {row['price']} {row['available']} -> {now}: {result}"
              + (f" ({note})" if note else ""), flush=True)
        # A store that turns away the first products turns away the rest: not worth the wait
        if len(checks) == BLOCKED_STOP and all(c[2] == "error" and "blocked" in (c[3] or "") for c in checks):
            print(f"  the first {BLOCKED_STOP} were blocked: the rest are left out", flush=True)
            break
        time.sleep(1)
    context.close()

    if os.getenv("QA_DRY_RUN"):
        print(f"{site} run {run_id}: {status_of([c[2] for c in checks])} (dry run, not saved)", flush=True)
        return
    source = "api" if site in STORE_API else "page"
    values = [(site, run_id, row["product_id"], row["url"], source if live else None, row["price"],
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
    # QA_HEADFUL=1: a visible Chrome window (on GitHub inside xvfb), which some bot walls let
    # through where a headless one isn't; off-screen on a desktop.
    headful = bool(os.getenv("QA_HEADFUL"))
    # Without the "controlled by automated software" flags, as a buyer's Chrome
    args = ["--disable-blink-features=AutomationControlled"] + (["--window-position=-2400,-2400"] if headful else [])
    with sync_playwright() as p:
        browser = p.chromium.launch(channel=os.getenv("QA_BROWSER_CHANNEL", "chrome"), headless=not headful,
                                    args=args, ignore_default_args=["--enable-automation"])
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
