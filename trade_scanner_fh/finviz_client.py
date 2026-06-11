"""Finviz earnings client — HTTP scrape of the per-ticker earnings tab.

Endpoint
--------
``https://finviz.com/quote.ashx?t={SYM}&ty=ea`` (equivalent to the
``/stock?t=SYM&ta=1&p=d&ty=ea`` URL the site links to). The page embeds
a JSON array under the ``"earningsData"`` key — one object per fiscal
quarter (past quarters carry actuals; future quarters carry only forward
analyst estimates):

    {"ticker":"AAOI","fiscalPeriod":"2026Q1",
     "earningsDate":"2026-05-07T16:30:00","fiscalEndDate":"2026-03-31",
     "epsActual":-0.07,"epsEstimate":-0.049,            # adjusted / non-GAAP
     "epsReportedActual":-0.19,"epsReportedEstimate":-0.095,  # GAAP
     "salesActual":151.144,"salesEstimate":156.9765, ...}

We take the **adjusted** fields (``epsActual`` / ``epsEstimate`` /
``salesActual`` / ``salesEstimate``) — validated to match Zacks ~98% to
the penny — and ignore the GAAP ``*Reported*`` fields. The fill module
maps these into the canonical earnings_history schema.

No API key. Finviz bot-filters generic clients, so we fetch through
``curl_cffi`` with Chrome TLS impersonation — the same technique the
Zacks scraper uses. A process-wide rate limiter paces requests well
under finviz's tolerance (see ``config.FINVIZ_MIN_INTERVAL_SEC``).

Failure kinds
-------------
Each call sets ``_LAST_FAILURE_KIND`` so the fill loop can distinguish a
coverage miss (``empty`` — no ``earningsData``, e.g. an ETF) from a real
block (``rate_limited`` / ``blocked`` / ``server_error`` / ``network``)
that should drive the auto-pause / back-off retry.
"""
from __future__ import annotations

import json
import logging
import random
import threading
import time
from typing import Optional

from curl_cffi import requests as creq

from . import config

log = logging.getLogger("scanner.finviz")

_EARNINGS_URL = "https://finviz.com/quote.ashx?t={sym}&ty=ea"

# Failure-kind sentinels. Read via last_failure_kind() after each call.
FAIL_EMPTY = "empty"              # 200 OK but no earningsData — ticker not covered (ETF/fund/new)
FAIL_RATE_LIMITED = "rate_limited"  # 429 — finviz throttle
FAIL_BLOCKED = "blocked"          # 200 but bot-block / challenge page (no quote content)
FAIL_FORBIDDEN = "forbidden"      # 403 — access denied
FAIL_SERVER = "server_error"      # 5xx — transient upstream failure
FAIL_NETWORK = "network"          # connection / timeout / DNS / non-200
FAIL_PARSE = "parse_error"        # earningsData found but un-parseable JSON
FAIL_TOO_LARGE = "too_large"      # response exceeded FINVIZ_MAX_RESPONSE_BYTES

_LAST_FAILURE_KIND: Optional[str] = None


def last_failure_kind() -> Optional[str]:
    """Return the FAIL_* sentinel for the most recent fetch, or None on
    success. Volatile — read it before the next call."""
    return _LAST_FAILURE_KIND


def _set_failure(kind: Optional[str]) -> None:
    global _LAST_FAILURE_KIND
    _LAST_FAILURE_KIND = kind


def is_configured() -> bool:
    """True — finviz needs no API key (HTTP scrape). Present for parity
    with finnhub_client.is_configured so callers can gate uniformly."""
    return True


# ──────────────────────────────────────────────────────────────────────
# Rate limiter (process-wide singleton, with jitter)
# ──────────────────────────────────────────────────────────────────────

class _RateLimiter:
    """Fixed minimum interval plus a small random jitter so the request
    cadence isn't a perfectly regular metronome."""

    def __init__(self, min_interval_sec: float, jitter_sec: float = 0.0):
        self._min_interval = float(min_interval_sec)
        self._jitter = max(0.0, float(jitter_sec))
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._next_allowed - now
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
            interval = self._min_interval
            if self._jitter:
                interval += random.uniform(-self._jitter, self._jitter)
                interval = max(0.5, interval)
            self._next_allowed = now + interval


_limiter = _RateLimiter(
    config.FINVIZ_MIN_INTERVAL_SEC, config.FINVIZ_JITTER_SEC,
)

# A browser-shaped header set on top of curl_cffi's Chrome TLS fingerprint.
_HEADERS = {
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,*/*;q=0.8"),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://finviz.com/",
    "Upgrade-Insecure-Requests": "1",
}


# ──────────────────────────────────────────────────────────────────────
# earningsData extraction
# ──────────────────────────────────────────────────────────────────────

_KEY = '"earningsData":'


def _extract_earnings_data(html: str) -> Optional[list]:
    """Pull the ``earningsData`` JSON array out of the page HTML.
    Returns the parsed list (possibly empty) on success, or None when
    the key isn't present at all.

    Uses ``json.JSONDecoder().raw_decode`` starting at the array's
    opening bracket — a real JSON scanner that correctly handles
    brackets / escapes inside string values (naive bracket-counting
    would miscount a ``]`` embedded in a string). Raises ``ValueError``
    (caught by the caller → FAIL_PARSE) when the array can't be parsed.
    """
    i = html.find(_KEY)
    if i == -1:
        return None
    start = html.find("[", i)
    if start == -1:
        raise ValueError("earningsData key without an opening bracket")
    try:
        parsed, _end = json.JSONDecoder().raw_decode(html, start)
    except json.JSONDecodeError as exc:
        raise ValueError(f"earningsData not parseable: {exc}")
    if not isinstance(parsed, list):
        raise ValueError("earningsData is not a list")
    return parsed


def fetch_earnings(symbol: str, *, timeout: float = 25.0) -> Optional[list[dict]]:
    """Fetch + parse the finviz earnings tab for one ticker.

    Returns a list of ``earningsData`` entry dicts (verbatim finviz
    fields) on success — possibly ``[]`` when finviz covers the ticker
    but has no earnings rows. Returns ``None`` on any hard failure, with
    ``last_failure_kind()`` set to a FAIL_* sentinel.

    A 200 page with no ``earningsData`` key at all is classified as
    ``FAIL_EMPTY`` (uncovered — ETF/fund/brand-new) when the page still
    looks like a real finviz quote page, else ``FAIL_BLOCKED`` (a
    challenge / throttle page that happens to return 200).
    """
    sym = (symbol or "").upper().strip()
    if not sym:
        _set_failure(FAIL_NETWORK)
        return None

    url = _EARNINGS_URL.format(sym=sym)
    _limiter.acquire()
    try:
        r = creq.get(url, impersonate="chrome", headers=_HEADERS,
                     timeout=timeout)
    except Exception as exc:
        log.debug("finviz network error for %s: %s", sym, exc)
        _set_failure(FAIL_NETWORK)
        return None

    sc = r.status_code
    if sc == 429:
        log.warning("finviz rate-limited (429) on %s", sym)
        _set_failure(FAIL_RATE_LIMITED)
        return None
    if sc == 403:
        _set_failure(FAIL_FORBIDDEN)
        return None
    if 500 <= sc < 600:
        _set_failure(FAIL_SERVER)
        return None
    if sc == 404:
        # Ticker not on finviz (delisted / junk symbol). The redirect
        # ``/quote.ashx?t=SYM`` → ``/quote?t=SYM`` lands on a 404 "not
        # found" stub. Classify as a coverage miss (EMPTY), NOT a network
        # failure: this lets the fill loop blacklist the symbol and, more
        # importantly, keeps a cluster of dead tickers from counting toward
        # the consecutive-block streak and tripping the auto-backoff.
        log.debug("finviz 404 (not covered) on %s", sym)
        _set_failure(FAIL_EMPTY)
        return None
    if sc != 200:
        log.debug("finviz returned %d on %s", sc, sym)
        _set_failure(FAIL_NETWORK)
        return None

    body = r.text or ""
    if len(body.encode("utf-8", "ignore")) > config.FINVIZ_MAX_RESPONSE_BYTES:
        _set_failure(FAIL_TOO_LARGE)
        return None

    try:
        data = _extract_earnings_data(body)
    except ValueError as exc:
        log.debug("finviz parse error for %s: %s", sym, exc)
        _set_failure(FAIL_PARSE)
        return None

    if data is None:
        # No earningsData key. Distinguish "covered but no earnings"
        # (ETF/fund → empty) from a bot-challenge page (blocked). A real
        # finviz quote page carries the ticker's snapshot table; a
        # challenge page does not and is typically tiny.
        looks_like_quote = ("snapshot-td" in body) or (f"t={sym}" in body)
        if looks_like_quote and len(body) > 50_000:
            _set_failure(FAIL_EMPTY)
        else:
            log.warning("finviz: no earningsData + non-quote page for %s "
                        "(len=%d) — treating as blocked", sym, len(body))
            _set_failure(FAIL_BLOCKED)
        return None

    _set_failure(None)
    return data
