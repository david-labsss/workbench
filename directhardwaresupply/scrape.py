"""Scrape directhardwaresupply.com (Direct Hardware Supply, Denmark; Magento 2 + Hyva behind
Cloudflare; ~26.5k refurbished server parts): every run's full snapshot appended to Postgres.

BLOCKED IN PAKISTAN: the store answers 403 (nginx, origin block) to Pakistan; US / Canadian / EU
networks get the pages (check-host.net and Common Crawl's US crawler, 2026-09/10). So the store
pages are only read from a US runner. Built from Common Crawl captures of the pages (2026-09), it
checks the live format hard and stops with a clear message when anything is off.

Two sources:
1. The store's own on-site search / recommendations service, Hello Retail (a public API on
   another domain, not blocked; the store's pages call it). One POST to
   core.helloretail.com/serve/recoms ("TOP" source, count 100000) lists every product: Magento
   entity id (extraData.itemNumber), SKU (productNumber, "00AR320 REF"), title
   ("<BRAND> - <MPN> - <name>"), URL, [brand, category], condition, stock status, the
   priceHidden flag, image URL. The live websiteUuid is read every run from
   /serve/init?url=https://directhardwaresupply.com/ (the uuid in the page source is an old
   staging feed). A second, separate call fetches the store's product descriptions (url +
   description only, ~70 MB unzipped); if it fails the run goes on without descriptions.
   Hello Retail's PRICES ARE NOT USED: they are whole numbers (site EUR 35.18, feed 35) and it
   still carries prices the site hides.
2. The product pages themselves (/<url-key>, no query string: robots.txt disallows every URL with
   "?", so the category pages past page 1, the search and the REST API are not used). The price
   is the visible price box (span#product-price-<entity id> span.price, the server-rendered text
   a visitor sees; it is cross-checked against the page's initialFinalPrice). The page hides the
   price (and shows "Get in touch" instead of "Add to cart") when the final price is 0.
   Stock = the "Availability" line: "268 in stock" (count), "Off-site warehouse (2-3 days)",
   "Backorder", ... available = the page shows a price and an "Add to cart" button.
   CURRENCY: the .com store shows EUR by default (also to US visitors). Its header offers
   "USD - US Dollar", which posts currency=USD to /directory/currency/switch/ (with the store's
   form key cookie, as the page's own hyva.postForm does). Each worker does that at the start,
   then every page must say USD (hyva.formatPrice currency) and show a $ price.
   PAGE LIMIT: after ~6,700 product pages from one machine the store serves its default EUR pages
   to that machine, even to a fresh USD session (2026-10-20: two runs, at 1.9 and 4.0 pages/s,
   both turned EUR at the same page, #6,678 of 11,155; a fresh runner then read later products in
   USD). So pages are read slowly: 3 at a time with a 1.5 s pause each, ~1.45 pages/s (~5.2k an
   hour, all ~11.2k in ~2 h 10 min). A worker that gets a EUR page switches to USD again (new
   session) and re-reads it; while pages stay EUR it pauses LIMIT_PAUSE and tries again, up to
   LIMIT_TRIES times, then the run stops (nothing saved). The limit is not worked around with
   more machines / addresses.

Which pages are read: every product Hello Retail does NOT list as price-hidden + "Backorder"
(~11.1k on 2026-10-20: 10,927 priced and in stock, 218 hidden but in stock). The other ~15.4k
(priceHidden = true, stockStatus = Backorder) show no price and "Backorder" on their pages (seen in
the captures), so they are saved from the feed as price empty, not available, stock empty. Every
run reads VERIFY_HIDDEN of them anyway: if more than MAX_HIDDEN_MISMATCH show a price or an "Add to
cart" button, the run stops (the feed's flags can no longer be trusted; DHS_FETCH_ALL=1 then reads
every product page, ~2.4x longer). Pages that read differently from the feed are saved as the page
says. A page that is gone (404 / redirected away) drops its product; more than 5% gone stops the run.

listed = the number of products Hello Retail lists, minus pages found gone. The store's sitemap
(google_sitemap_en_index.xml, from robots.txt) is compared with the feed for information only.

Fields: product_id = Magento entity id; sku = productNumber; part_number = the page's JSON-LD mpn
(or the MPN in the title); title = the page's h1 (or the feed title); brand / category = the
feed's hierarchy; condition = the feed's condition; price; available; stock_qty; image_url = the
feed's image (or og:image); description = the store's description (plain text, 2,000 chars).

Run steps, changes and env vars: common/runner.py. Columns: common/schema.py.
Extra env var: DHS_FETCH_ALL=1 reads every product page (ignores the feed's hidden flags).
"""
import html as htmllib
import json
import os
import random
import re
import string
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup

from common import http, runner, schema

SITE = "directhardwaresupply"
BASE = "https://directhardwaresupply.com"
HR_INIT = "https://core.helloretail.com/serve/init"
HR_RECOMS = "https://core.helloretail.com/serve/recoms"
HR_UUID = "69aea332-5faa-477e-b67a-16f353f31507"     # live feed on 2026-10-20 (read every run)
HR_FIELDS = ["title", "productNumber", "url", "hierarchies", "extraData", "inStock", "imgUrl"]
HR_MAX = 100000
HR_MIN = 10000                  # fewer products than this from the feed = something is wrong
SITEMAP = f"{BASE}/google_sitemap_en_index.xml"
WORKERS = 3                     # requests in flight
DELAY = 1.5                     # per worker, after each request (~1.45 pages/s, see PAGE LIMIT)
LIMIT_PAUSE = 900               # seconds to wait when the store serves EUR pages anyway
LIMIT_TRIES = 3
TRIES = 3
RETRY_PAUSE = 30
VERIFY_HIDDEN = 40              # hidden / backorder products whose pages are read as a check
MAX_HIDDEN_MISMATCH = 2
MAX_GONE_SHARE = 0.05
FETCH_ALL = os.getenv("DHS_FETCH_ALL") == "1"
CURRENCY_RE = re.compile(r"style:\s*'currency',\s*currency:\s*'([A-Z]{3})'")
ITEM_RE = re.compile(r'"item_id":"(\d+)","item_sku":"([^"]*)"')
STOCK_RE = re.compile(r'title="Availability"\s*>\s*([^<]*?)\s*</p>')
COUNT_RE = re.compile(r"^(\d[\d,]*)\s+in stock$", re.I)
LD_RE = re.compile(r'<script type="application/ld\+json">\s*(.*?)\s*</script>', re.S)
H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.S)
OG_IMAGE_RE = re.compile(r'<meta property="og:image" content="([^"]+)"')
SWITCH_RE = re.compile(r'hyva\.postForm\((\{"action":"[^"]*currency\\/switch\\/",'
                       r'"data":\{"currency":"USD","uenc":"[^"]*"\}\})\)')
AMOUNT_RE = re.compile(r"(\d[\d,]*(?:\.\d+)?)")

_local = threading.local()


class Gone(Exception):
    """A product page that no longer exists."""


class Blocked(RuntimeError):
    """The store refused this network (403)."""


def clean(text):
    return re.sub(r"\s+", " ", htmllib.unescape(text or "").replace("\xa0", " ")).strip()


def snippet(page, n=1200):
    body = re.split(r"<body[^>]*>", page, maxsplit=1, flags=re.I)[-1]
    return clean(re.sub(r"<script.*?</script>|<style.*?</style>|<[^>]+>", " ", body,
                        flags=re.S | re.I))[:n]


def request(session, method, url, allow_gone=False, **kw):
    """http's 429 / 5xx retries plus a few tries for stalled downloads. 403 raises Blocked."""
    for attempt in range(1, TRIES + 1):
        try:
            r = session.request(method, url, timeout=(30, 300), **kw)
            if r.status_code == 403:
                raise Blocked(f"{url}: HTTP 403 - the store blocks this network/country; run this "
                              f"scraper on a US runner. Page says: {snippet(r.text, 300)}")
            if allow_gone and r.status_code in (404, 410):
                raise Gone(url)
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            if attempt == TRIES:
                raise
            print(f"WARN {url[:120]}: {e.__class__.__name__}, retrying in {RETRY_PAUSE}s",
                  flush=True)
            time.sleep(RETRY_PAUSE)


# --- Hello Retail -------------------------------------------------------------------------

def hr_uuid():
    r = request(http.session(), "GET", HR_INIT, params={
        "websiteUuid": "", "url": f"{BASE}/", "trackingUserId": "", "v": "4"})
    uuid = ((r.json().get("website") or {}).get("uuid") or "").strip()
    if not uuid:
        raise RuntimeError(f"Hello Retail init gave no websiteUuid: {r.text[:300]}")
    if uuid != HR_UUID:
        print(f"WARN Hello Retail websiteUuid is now {uuid} (was {HR_UUID})", flush=True)
    return uuid


def hr_call(uuid, fields):
    body = {"websiteUuid": uuid, "requests": [{
        "trackingKey": "price-monitor", "count": HR_MAX, "hideAdditionalVariants": False,
        "fields": fields, "sources": [{"type": "TOP"}]}]}
    r = request(http.session(), "POST", HR_RECOMS, json=body)
    resp = (r.json().get("responses") or [{}])[0]
    products = resp.get("products") or []
    if not resp.get("success") or not products:
        raise RuntimeError(f"Hello Retail recoms failed: {r.text[:500]}")
    if len(products) >= HR_MAX:
        raise RuntimeError(f"Hello Retail returned {len(products)} products = the count asked for; "
                           "raise HR_MAX")
    return products, resp.get("countAfterSource")


def page_url(p):
    return (p.get("url") or "").split("#", 1)[0].strip()


def feed():
    """[feed product] with keys id, url, ... (every product the store's feed lists)."""
    uuid = hr_uuid()
    products, count = hr_call(uuid, HR_FIELDS)
    print(f"Hello Retail ({uuid}): {len(products)} products ({count})", flush=True)
    if len(products) < HR_MIN:
        raise RuntimeError(f"Hello Retail lists only {len(products)} products (expected ~26.5k)")
    out, seen = [], set()
    for p in products:
        extra = p.get("extraData") or {}
        pid = str(extra.get("itemNumber") or "").strip()
        url = page_url(p)
        if not pid.isdigit() or not url.startswith(BASE + "/"):
            raise RuntimeError(f"Hello Retail product without an entity id / store URL: {p}")
        if pid in seen:
            continue
        seen.add(pid)
        hier = (p.get("hierarchies") or [[]])[0] or []
        out.append({
            "id": pid,
            "url": url,
            "sku": clean(p.get("productNumber")),
            "title": clean(p.get("title")),
            "brand": clean(hier[0]) if hier else clean(extra.get("extraattributes_manufacturer")),
            "category": clean(hier[1]) if len(hier) > 1 else "",
            "condition": clean(extra.get("condition")),
            "stock_status": clean(extra.get("stockStatus")),
            "price_hidden": str(extra.get("priceHidden")).lower() == "true",
            "image": clean(p.get("imgUrl")),
        })
    return out, uuid


def descriptions(uuid):
    """{store URL: description} from a separate feed call; {} (with a warning) if it fails."""
    try:
        products, _ = hr_call(uuid, ["url", "description"])
    except Exception as e:                          # descriptions are not essential
        print(f"WARN no descriptions this run: {e.__class__.__name__}: {str(e)[:200]}", flush=True)
        return {}
    return {page_url(p): p.get("description") for p in products if p.get("description")}


def hidden_backorder(p):
    return p["price_hidden"] and p["stock_status"].lower() == "backorder"


# --- store pages (USD) ---------------------------------------------------------------------

def form_key(session):
    key = session.cookies.get("form_key")
    if not key:
        key = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(16))
        session.cookies.set("form_key", key, domain="directhardwaresupply.com", path="/")
    return key


def currency_of(page):
    m = CURRENCY_RE.search(page)
    return m.group(1) if m else None


def usd_session(again=False):
    """This worker's session, switched to USD (the header's "USD - US Dollar" button). again =
    the USD choice ran out: drop the session's cookies and switch again."""
    s = http.session()
    if again:
        s.cookies.clear()
        _local.usd = False
    if getattr(_local, "usd", False):
        return s
    home = request(s, "GET", f"{BASE}/").text
    cur = currency_of(home)
    if cur is None:
        raise RuntimeError(f"{BASE}/: no hyva.formatPrice currency on the home page; page format "
                           f"changed? {snippet(home)}")
    if cur != "USD":
        m = SWITCH_RE.search(home)
        if not m:
            raise RuntimeError(f"{BASE}/: shows {cur} and has no 'USD - US Dollar' switch")
        form = json.loads(m.group(1))
        data = dict(form["data"], form_key=form_key(s))
        # Don't follow the switch's redirect: it goes back to the "uenc" page baked into the
        # cached home page, which can be a dead URL (404 seen 2026-10-20). The cookie it sets is
        # what counts, and the home page is re-read below to confirm USD.
        request(s, "POST", form["action"], data=data, allow_redirects=False,
                headers={"Origin": BASE, "Referer": f"{BASE}/"})
        home = request(s, "GET", f"{BASE}/").text
        cur = currency_of(home)
        if cur != "USD":
            raise RuntimeError(f"switching the store to USD failed: the home page still shows {cur}")
        print(f"worker {threading.current_thread().name}: store switched to USD", flush=True)
    _local.usd = True
    return s


def json_ld_product(page):
    for raw in LD_RE.findall(page):
        try:
            data = json.loads(raw)
        except ValueError:
            continue
        if isinstance(data, dict) and data.get("@type") == "Product":
            return data
    return {}


def parse_product(page, pid):
    """What a product page shows. Raises when the page is not a product page we understand."""
    m = ITEM_RE.search(page)
    if not m and "product_addtocart_form" not in page:
        raise Gone("no product on the page")
    page_id = m.group(1) if m else None
    start = page.find('<main id="maincontent"')
    end = page.find("</main>", start)
    main = page[start:end] if start >= 0 and end > start else page
    h1 = H1_RE.search(main)
    stock = STOCK_RE.search(main)
    ld = json_ld_product(page)
    price_text, initial = None, None
    box = main.find(f'x-data="initPrice{pid}()"')
    if box >= 0:
        frag = BeautifulSoup(main[box - 30: box + 8000], "html.parser")
        span = frag.select_one(f"span#product-price-{pid} span.price")
        price_text = clean(span.get_text(" ")) if span else ""
        fn = main.rfind(f"function initPrice{pid}()", 0, box)
        im = re.search(r"initialFinalPrice:\s*([\d.]+)", main[fn:box]) if fn >= 0 else None
        initial = float(im.group(1)) if im else None
    image = OG_IMAGE_RE.search(page)
    return {
        "page_id": page_id,
        "currency": currency_of(page),
        "title": clean(re.sub(r"<[^>]+>", " ", h1.group(1))) if h1 else "",
        "stock_text": clean(stock.group(1)) if stock else "",
        "add_to_cart": 'id="product-addtocart-button"' in main,
        "price_box": box >= 0,
        "price_text": price_text,
        "initial": initial,
        "mpn": clean(str(ld.get("mpn") or "")),
        "image": image.group(1) if image else "",
    }


def page_price(info, url):
    """The visible price (USD) or None when the page hides it. Raises on a non-USD page."""
    if not info["price_box"] or info["initial"] == 0:
        return None                                 # isPriceHidden(): final price 0
    text = info["price_text"] or ""
    if info["currency"] != "USD" or "$" not in text:
        raise RuntimeError(f"{url}: price {text!r} in {info['currency']} - the page is not in USD "
                           "(the currency switch did not stick)")
    m = AMOUNT_RE.search(text.replace(" ", ""))
    if not m:
        raise RuntimeError(f"{url}: price box without an amount: {text!r}")
    value = float(m.group(1).replace(",", ""))
    return value or None


def stock(info, price):
    """(available, stock_qty) from the Availability line and the Add to cart button."""
    m = COUNT_RE.match(info["stock_text"])
    qty = int(m.group(1).replace(",", "")) if m else None
    available = bool(info["add_to_cart"] and price is not None)
    if qty is not None and available != (qty > 0):
        qty = None                                  # a count that contradicts the page
    return available, qty


def fetch_info(s, p):
    """(response, page info) of a product page; a page that is gone raises Gone."""
    r = request(s, "GET", p["url"], allow_gone=True)
    if r.url.split("#")[0].rstrip("/") != p["url"].rstrip("/") and r.history:
        raise Gone(f"redirected to {r.url}")
    info = parse_product(r.text, p["id"])
    if info["page_id"] and info["page_id"] != p["id"]:
        raise Gone(f"page is product {info['page_id']}, feed says {p['id']}")
    return r, info


def read_page(p):
    """(feed product, page info + price) or (feed product, None) when the page is gone."""
    s = usd_session()
    try:
        r, info = fetch_info(s, p)
        if info["currency"] != "USD":          # the USD choice ran out: switch again, re-read
            print(f"WARN worker {threading.current_thread().name}: {p['url']} shows "
                  f"{info['currency']}, switching to USD again", flush=True)
            s = usd_session(again=True)
            r, info = fetch_info(s, p)
        for attempt in range(1, LIMIT_TRIES + 1):
            if info["currency"] == "USD":
                break
            print(f"WARN worker {threading.current_thread().name}: still {info['currency']} after "
                  f"switching (the store's page limit?): pausing {LIMIT_PAUSE}s, try {attempt} of "
                  f"{LIMIT_TRIES}", flush=True)
            time.sleep(LIMIT_PAUSE)
            s = usd_session(again=True)
            r, info = fetch_info(s, p)
        if info["currency"] != "USD":
            raise RuntimeError(f"{p['url']}: page currency {info['currency']} (not USD: the switch "
                               f"did not stick, or the page format changed) {snippet(r.text, 300)}")
        info["price"] = page_price(info, p["url"])
        if info["price"] is not None and info["initial"] is not None \
                and abs(info["price"] - info["initial"]) > 0.005:
            info["mismatch"] = True
        if not info["stock_text"]:
            raise RuntimeError(f"{p['url']}: no 'Availability' line; page format changed? "
                               f"{snippet(r.text)}")
        return p, info
    except Gone as e:
        print(f"WARN gone: {p['url']} ({e})", flush=True)
        return p, None
    finally:
        time.sleep(DELAY)


def sitemap_check(products):
    """Compare the store's sitemap with the feed (information only)."""
    try:
        s = http.session()
        index = request(s, "GET", SITEMAP).text
        locs = [clean(u) for u in re.findall(r"<loc>([^<]+)</loc>", index)]
        urls = set()
        if "<sitemapindex" in index:
            for sm in locs[:50]:
                urls.update(clean(u) for u in re.findall(r"<loc>([^<]+)</loc>",
                                                         request(s, "GET", sm).text))
                time.sleep(DELAY)
        else:
            urls = set(locs)
        feed_urls = {p["url"] for p in products}
        missing = sorted(feed_urls - urls)
        extra = sorted(urls - feed_urls)
        print(f"sitemap: {len(urls)} URLs; {len(feed_urls) - len(missing)} of the feed's "
              f"{len(feed_urls)} products are in it; {len(extra)} sitemap URLs are not in the feed "
              f"(categories, CMS pages, or products the feed lacks), e.g. {extra[:8]}", flush=True)
    except Exception as e:
        print(f"WARN sitemap check failed: {e.__class__.__name__}: {str(e)[:200]}", flush=True)


def mpn_of(title):
    parts = title.split(" - ")
    return parts[1].strip() if len(parts) >= 3 else ""


def build_row(p, info, desc, scraped_at):
    if info is None:                                # not read: hidden price + backorder feed item
        price, available, qty = None, False, None
        stock_text = p["stock_status"] or "Backorder"
        title, mpn, image = p["title"], mpn_of(p["title"]), p["image"]
        note = f"Availability: {stock_text} (from the store's feed; price hidden)"
    else:
        price = info["price"]
        available, qty = stock(info, price)
        title = info["title"] or p["title"]
        mpn = info["mpn"] or mpn_of(p["title"])
        image = p["image"] or info["image"]
        note = f"Availability: {info['stock_text']}"
    return schema.product_row({
        "scraped_at": scraped_at,
        "website": SITE,
        "product_id": p["id"],
        "sku": p["sku"],
        "part_number": mpn,
        "title": title,
        "brand": p["brand"],
        "category": p["category"],
        "condition": p["condition"],
        "other_specs": note,
        "price": price,
        "available": available,
        "stock_qty": qty,
        "image_url": image if image.startswith("http") else None,
        "description": schema.plain_text(desc),
        "url": p["url"],
    })


def scrape(limit, scraped_at):
    usd_session()             # fail fast: the store answers this network and the USD switch works
    products, uuid = feed()
    if any("?" in p["url"] for p in products):
        raise RuntimeError("feed URLs with a query string (robots.txt disallows them)")
    hidden = [p for p in products if hidden_backorder(p)]
    to_read = products if FETCH_ALL else [p for p in products if not hidden_backorder(p)]
    rng = random.Random()
    verify = [] if FETCH_ALL else rng.sample(hidden, min(VERIFY_HIDDEN, len(hidden)))
    if limit:                 # test run: products spread over the whole list, first to last
        n = min(len(to_read), max(1, limit - limit // 3))
        to_read = [to_read[round(i * (len(to_read) - 1) / max(1, n - 1))] for i in range(n)]
        verify = verify[:limit // 3]
    print(f"{len(products)} products: {len(to_read)} pages to read, {len(hidden)} hidden-price "
          f"backorder items saved from the feed ({len(verify)} of them read as a check)",
          flush=True)
    sitemap_check(products)
    descs = descriptions(uuid)
    print(f"{len(descs)} descriptions", flush=True)

    infos, done = {}, 0
    started = time.time()
    with ThreadPoolExecutor(WORKERS) as pool:
        for p, info in pool.map(read_page, to_read + verify):
            infos[p["id"]] = info
            done += 1
            if done % 250 == 0:
                rate = done / (time.time() - started)
                print(f"{done}/{len(to_read) + len(verify)} pages ({rate:.1f}/s)", flush=True)

    read = [p for p in to_read + verify]
    gone = [p for p in read if infos.get(p["id"]) is None]
    if len(gone) > max(5, MAX_GONE_SHARE * len(read)):
        raise RuntimeError(f"{len(gone)} of {len(read)} product pages gone / not product pages; "
                           "the page format changed?")
    wrong = [p["url"] for p in verify if infos.get(p["id"])
             and (infos[p["id"]]["price"] is not None or infos[p["id"]]["add_to_cart"])]
    print(f"hidden-price check: {len(verify) - len(wrong)} of {len(verify)} read pages show no "
          f"price / no Add to cart as the feed says; differ: {wrong}", flush=True)
    if len(wrong) > MAX_HIDDEN_MISMATCH:
        raise RuntimeError(f"{len(wrong)} of {len(verify)} 'hidden price / Backorder' products show "
                           "a price or Add to cart on their page; the feed's flags are not "
                           "reliable, run with DHS_FETCH_ALL=1")
    mism = [p["url"] for p in read if infos.get(p["id"]) and infos[p["id"]].get("mismatch")]
    if mism:
        print(f"WARN {len(mism)} pages: shown price differs from initialFinalPrice, e.g. "
              f"{mism[:5]}", flush=True)
    if len(mism) > max(5, len(read) // 20):
        raise RuntimeError(f"{len(mism)} pages show a price different from their initialFinalPrice")
    texts = {}
    for info in infos.values():
        if info:
            key = re.sub(r"\d[\d,]*", "N", info["stock_text"])
            texts[key] = texts.get(key, 0) + 1
    print(f"availability texts on read pages: {texts}", flush=True)
    for p in to_read[:3]:
        print(f"sample: {p['url']} -> {infos.get(p['id'])}", flush=True)

    if limit:
        items = [p for p in to_read + verify if infos.get(p["id"]) is not None][:limit]
        return [build_row(p, infos[p["id"]], descs.get(p["url"]), scraped_at) for p in items], None
    rows = []
    for p in products:
        if p["id"] in infos:
            if infos[p["id"]] is None:
                continue                            # gone
            rows.append(build_row(p, infos[p["id"]], descs.get(p["url"]), scraped_at))
        else:
            rows.append(build_row(p, None, descs.get(p["url"]), scraped_at))
    return rows, len(products) - len(gone)          # gone pages are no longer on the site


if __name__ == "__main__":
    runner.run(SITE, scrape)
