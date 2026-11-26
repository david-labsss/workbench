"""One requests session per thread, retrying 429 / 5xx (honours Retry-After).

get() adds what urllib3's Retry cannot do: it retries a page whose body broke off half way
("Response ended prematurely"), a read timeout, a dropped connection, and (only when the scraper
asks) a 403 that some stores send for a few minutes and then stop (servertechsupply 11-26).
"""
import threading
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/130.0 Safari/537.36")

# 520-524 are Cloudflare's "the store's server did not answer" pages (aztekc 520, 11-24).
RETRY_STATUS = [429, 500, 502, 503, 504, 520, 521, 522, 523, 524]
DROPPED = (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError,
           requests.exceptions.Timeout)

_local = threading.local()


def session():
    s = getattr(_local, "session", None)
    if s is None:
        s = requests.Session()
        s.headers["User-Agent"] = USER_AGENT
        retry = Retry(total=6, backoff_factor=2, status_forcelist=RETRY_STATUS,
                      allowed_methods=["GET", "POST"], respect_retry_after_header=True)
        s.mount("https://", HTTPAdapter(max_retries=retry))
        _local.session = s
    return s


def get(url, tries=5, wait=15, retry_403=False, **kw):
    """session().get(url) that raises only after `tries` attempts, waiting wait, 2*wait, ...
    between them. Retries a broken-off body, a timeout or a dropped connection; with
    retry_403 also a 403. Any other error status raises at once (raise_for_status)."""
    kw.setdefault("timeout", 60)
    for attempt in range(1, tries + 1):
        try:
            r = session().get(url, **kw)
            if retry_403 and r.status_code == 403 and attempt < tries:
                print(f"403 on {url} (try {attempt} of {tries}); waiting {wait * attempt}s",
                      flush=True)
                time.sleep(wait * attempt)
                continue
            r.raise_for_status()
            r.content  # read the whole body here, so a body that breaks off is retried too
            return r
        except DROPPED as e:
            if attempt == tries:
                raise
            print(f"{type(e).__name__} on {url} (try {attempt} of {tries}); "
                  f"waiting {wait * attempt}s", flush=True)
            time.sleep(wait * attempt)
