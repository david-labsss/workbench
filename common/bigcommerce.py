"""Shared scraper for BigCommerce stores (unixsurplus, velocitytechsolutions, ...).

The storefront GraphQL API (/graphql) is open to the public token every BigCommerce theme puts
in its HTML (a JWT valid for a few days and only for the store's own Origin, so it is read
fresh from the home page each run). Products are paged by cursor, 50 at a time, each with all
its variants: SKU, MPN, brand, categories, custom fields (specs), price, in stock, and the stock
count when the store tracks it.

One row per variant: product_id = variant id (unique), parent_id = product id. Custom field
names are mapped to the standard columns (common/schema.py); the rest go to other_specs.
"""
import html
import re
import time

from common import http, schema

PAGE = 50
PAGE_DELAY = 0.5
TOKEN_RE = re.compile(r"eyJ0eXAiOiJKV1Qi[A-Za-z0-9._-]+")
CAPACITY_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(TB|GB)\b", re.I)

# Custom field name (lowercase, without a trailing ":") -> column
FIELD_COLUMNS = {
    "part number": "part_number", "part #": "part_number", "mpn": "part_number",
    "model": "model_number", "model number": "model_number",
    "condition": "condition",
    "series": "series", "product line": "series",
    "capacity": "capacity",
    "interface": "interface",
    "form factor": "form_factor",
    "rotation speed": "spindle_speed", "spindle speed": "spindle_speed", "rpm": "spindle_speed",
    "cache": "cache",
    "endurance": "endurance",
    "category": "category", "product type": "category",
}
SKIP_FIELDS = {"brand"}
GENERIC_CATEGORIES = {"shop", "products", "all products"}

QUERY = """
query($first: Int!, $after: String) {
  site {
    products(first: $first, after: $after) {
      pageInfo { hasNextPage endCursor }
      edges { node {
        entityId sku mpn name path condition
        brand { name }
        categories(first: 10) { edges { node { name path } } }
        prices { price { value } salePrice { value } retailPrice { value } basePrice { value } }
        availabilityV2 { status }
        customFields(first: 50) { edges { node { name value } } }
        variants(first: 250) {
          pageInfo { hasNextPage }
          edges { node {
            entityId sku mpn
            prices { price { value } retailPrice { value } basePrice { value } }
            inventory { isInStock aggregated { availableToSell } }
            options { edges { node { displayName values { edges { node { label } } } } } }
          } }
        }
      } }
    }
  }
}"""


def token(base):
    r = http.session().get(base + "/", timeout=30)
    r.raise_for_status()
    m = TOKEN_RE.search(r.text)
    if not m:
        raise RuntimeError(f"no storefront API token in {base}'s HTML")
    return m.group(0)


def fetch_products(base, limit=0):
    """Every product node. A failed page raises: it must never look like the end of the list."""
    headers = {"Authorization": f"Bearer {token(base)}", "Origin": base,
               "Content-Type": "application/json"}
    products, after = [], None
    while True:
        r = http.session().post(base + "/graphql", headers=headers, timeout=60,
                                json={"query": QUERY, "variables": {"first": PAGE, "after": after}})
        r.raise_for_status()
        data = r.json()
        if data.get("errors"):
            raise RuntimeError(f"GraphQL error: {data['errors'][0].get('message')}")
        page = data["data"]["site"]["products"]
        nodes = [e["node"] for e in page["edges"]]
        for n in nodes:
            if n["variants"]["pageInfo"]["hasNextPage"]:
                print(f"WARN product {n['entityId']} has more than 250 variants; "
                      "only the first 250 are saved", flush=True)
        products.extend(nodes)
        if len(products) % 500 < PAGE:
            print(f"{len(products)} products", flush=True)
        if not page["pageInfo"]["hasNextPage"] or (limit and len(products) >= limit):
            break
        after = page["pageInfo"]["endCursor"]
        time.sleep(PAGE_DELAY)
    print(f"fetched {len(products)} products", flush=True)
    return products[:limit] if limit else products


def value(money):
    return (money or {}).get("value")


def edges(conn):
    return [e["node"] for e in (conn or {}).get("edges", [])]


def custom_fields(node):
    specs, other = {}, []
    for f in edges(node.get("customFields")):
        name = (f.get("name") or "").strip().rstrip(":").strip()
        val = html.unescape(re.sub(r"<[^>]+>", " ", f.get("value") or ""))  # some hold HTML
        val = re.sub(r"\s+", " ", val).strip()
        if not name or not val:
            continue
        column = FIELD_COLUMNS.get(name.lower())
        if column:
            specs.setdefault(column, val)
        elif name.lower() not in SKIP_FIELDS:
            other.append(f"{name}: {val}")
    return specs, "; ".join(other)


def category_of(node):
    """The most specific (deepest path) store category, skipping catch-alls like "Shop"."""
    cats = [c for c in edges(node.get("categories"))
            if (c.get("name") or "").strip().lower() not in GENERIC_CATEGORIES]
    if not cats:
        return ""
    return max(cats, key=lambda c: (c.get("path") or "").strip("/").count("/"))["name"]


def capacity_tb(text):
    m = CAPACITY_RE.search(text or "")
    if not m:
        return None
    size = float(m.group(1))
    return size if m.group(2).upper() == "TB" else size / 1000


def build_rows(site, base, node, scraped_at):
    specs, other = custom_fields(node)
    variants = edges(node.get("variants"))
    condition = (node.get("condition") or "").title() or specs.get("condition", "")
    status = (node.get("availabilityV2") or {}).get("status")
    tb = capacity_tb(specs.get("capacity") or node.get("name"))
    rows = []
    for v in variants:
        options = [", ".join(x["label"] for x in edges(o.get("values")))
                   for o in edges(v.get("options"))]
        title = node.get("name", "") + (f" - {' / '.join(options)}" if len(variants) > 1 and options else "")
        vp, pp = v.get("prices") or {}, node.get("prices") or {}
        price = value(vp.get("price")) or value(pp.get("price"))
        compare = max((x for x in (value(vp.get("retailPrice")), value(vp.get("basePrice")),
                                   value(pp.get("retailPrice"))) if x), default=None)
        inv = v.get("inventory") or {}
        stock = (inv.get("aggregated") or {}).get("availableToSell")
        rows.append(schema.product_row({
            "scraped_at": scraped_at,
            "website": site,
            "product_id": str(v["entityId"]),       # unique: one row per variant
            "parent_id": str(node["entityId"]),
            "variant_id": str(v["entityId"]),
            "sku": v.get("sku") or node.get("sku") or "",
            "part_number": v.get("mpn") or node.get("mpn") or specs.get("part_number", ""),
            "model_number": specs.get("model_number", ""),
            "title": title,
            "brand": (node.get("brand") or {}).get("name") or "",
            "category": specs.get("category") or category_of(node),
            "condition": condition,
            "series": specs.get("series", ""),
            "capacity": specs.get("capacity", ""),
            "interface": specs.get("interface", ""),
            "form_factor": specs.get("form_factor", ""),
            "spindle_speed": specs.get("spindle_speed", ""),
            "cache": specs.get("cache", ""),
            "endurance": specs.get("endurance", ""),
            "other_specs": other,
            "price": price,
            "compare_at_price": compare if compare and price and compare > price else None,
            "price_per_tb": round(price / tb, 2) if price and tb else None,
            "available": bool(inv.get("isInStock")) and status != "Unavailable",
            "stock_qty": stock,
            "url": base + (node.get("path") or "/"),
        }))
    return rows


def scraper(site, base):
    """A scrape(limit, scraped_at) function for common/runner.py."""
    def scrape(limit, scraped_at):
        nodes = fetch_products(base, limit)
        rows = [row for n in nodes for row in build_rows(site, base, n, scraped_at)]
        # Cursor paging has no total; a failed page raises instead of ending the list early
        return rows, None
    return scrape
