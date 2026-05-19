"""Finnhub REST API client with keyring-backed credentials.

Endpoints used
--------------
* `/stock/earnings` — full quarterly history per ticker. EPS actual /
  estimate / surprise + revenue. Empty array `[]` = ETF / fund /
  uncovered. Returned in newest-first order.
* `/calendar/earnings?from=…&to=…` — events by date range. With a
  symbol filter: that ticker's events in the window. Without: every
  ticker reporting in the window. Used to recover real announcement
  dates for /stock/earnings rows (which only carry `period`, the
  fiscal-quarter end).
* `/stock/profile2` — sector / industry. Used by sector_map.

All free-tier (60 req/min). Pacing is centralized through the
process-wide `_limiter` so concurrent callers can't blow the cap.

Failure kinds
-------------
Each call sets `_LAST_FAILURE_KIND` on the module so a fill loop can
distinguish coverage misses (`empty`) — which mark a ticker as ETF / not
covered — from real blocks (`rate_limited`, `server_error`, `network`)
that should drive the auto-pause / step-back retry.

Credential storage
------------------
API key in OS keyring under service `trade_scanner_fh`,
username `finnhub_api_key`. Visible in Windows Credential Manager
under "Generic Credentials".
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import date, timedelta
from typing import Optional

import keyring
import pandas as pd
import requests

from . import config

log = logging.getLogger("scanner.finnhub")

_SERVICE_NAME = "trade_scanner_fh"
_USERNAME_KEY = "finnhub_api_key"
_BASE_URL = "https://finnhub.io/api/v1"

# Free-tier limit is 60 requests/minute. Pace at config.FINNHUB_MIN_INTERVAL_SEC
# (default 1.15s) which yields ~52/min with retry headroom.
_MIN_INTERVAL_SEC = config.FINNHUB_MIN_INTERVAL_SEC

# Failure-kind sentinels. Read from `last_failure_kind` after each fetch_*
# call. None on success.
FAIL_EMPTY = "empty"           # 200 OK with [] — ticker not covered (ETF/fund/IPO)
FAIL_RATE_LIMITED = "rate_limited"  # 429 — Finnhub minute cap hit
FAIL_AUTH = "auth"             # 401 — bad / revoked API key (fatal)
FAIL_FORBIDDEN = "forbidden"   # 403 — endpoint not in plan
FAIL_SERVER = "server_error"   # 5xx — transient upstream failure
FAIL_NETWORK = "network"       # connection / timeout / DNS
FAIL_PARSE = "parse_error"     # JSON or shape error on a 200

# Module-level failure kind so callers don't need to plumb a session
# object. Fill modules read this immediately after each call.
_LAST_FAILURE_KIND: Optional[str] = None


def last_failure_kind() -> Optional[str]:
    """Return the FAIL_* sentinel for the most recent fetch_* call, or
    None if it succeeded. Volatile — read it before the next call."""
    return _LAST_FAILURE_KIND


def _set_failure(kind: Optional[str]) -> None:
    global _LAST_FAILURE_KIND
    _LAST_FAILURE_KIND = kind


# ──────────────────────────────────────────────────────────────────────
# Keyring helpers
# ──────────────────────────────────────────────────────────────────────

def get_api_key() -> Optional[str]:
    """Fetch the Finnhub API key from the OS credential store, or None."""
    try:
        key = keyring.get_password(_SERVICE_NAME, _USERNAME_KEY)
        return key.strip() if key else None
    except Exception as exc:
        log.warning("keyring read failed: %s", exc)
        return None


def set_api_key(key: str) -> bool:
    """Persist the API key to the OS credential store. Returns True on success."""
    if not key or not key.strip():
        return False
    try:
        keyring.set_password(_SERVICE_NAME, _USERNAME_KEY, key.strip())
        return True
    except Exception as exc:
        log.warning("keyring write failed: %s", exc)
        return False


def clear_api_key() -> bool:
    """Remove the API key from the credential store. Returns True on success."""
    try:
        keyring.delete_password(_SERVICE_NAME, _USERNAME_KEY)
        return True
    except keyring.errors.PasswordDeleteError:
        return False
    except Exception as exc:
        log.warning("keyring delete failed: %s", exc)
        return False


def is_configured() -> bool:
    """True if a Finnhub API key is currently stored."""
    return get_api_key() is not None


# ──────────────────────────────────────────────────────────────────────
# Rate limiter (process-wide singleton)
# ──────────────────────────────────────────────────────────────────────

class _RateLimiter:
    def __init__(self, min_interval_sec: float):
        self._min_interval = float(min_interval_sec)
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._next_allowed - now
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
            self._next_allowed = now + self._min_interval


_limiter = _RateLimiter(_MIN_INTERVAL_SEC)


# ──────────────────────────────────────────────────────────────────────
# Low-level REST helper
# ──────────────────────────────────────────────────────────────────────

def _request(endpoint: str, params: dict, *, timeout: float = 15.0):
    """GET endpoint with the stored API key. Returns parsed JSON.
    Sets ``last_failure_kind`` to a FAIL_* sentinel on any failure
    (None on success). Empty list response is success — caller checks
    truthiness.

    For empty arrays the JSON itself is returned ([]) and
    last_failure_kind is set to FAIL_EMPTY so per-ticker callers can
    distinguish ETF / no-coverage from real failures.
    """
    key = get_api_key()
    if not key:
        _set_failure(FAIL_AUTH)
        return None
    _limiter.acquire()
    try:
        r = requests.get(
            f"{_BASE_URL}{endpoint}",
            params={**params, "token": key},
            timeout=timeout,
        )
    except requests.exceptions.RequestException as exc:
        log.debug("Finnhub %s network error: %s", endpoint, exc)
        _set_failure(FAIL_NETWORK)
        return None

    sc = r.status_code
    if sc == 429:
        log.warning("Finnhub rate-limited (429) on %s", endpoint)
        _set_failure(FAIL_RATE_LIMITED)
        return None
    if sc == 401:
        log.warning("Finnhub returned 401 on %s — API key invalid", endpoint)
        _set_failure(FAIL_AUTH)
        return None
    if sc == 403:
        log.warning("Finnhub returned 403 on %s — endpoint not in plan", endpoint)
        _set_failure(FAIL_FORBIDDEN)
        return None
    if 500 <= sc < 600:
        log.debug("Finnhub %s returned %d", endpoint, sc)
        _set_failure(FAIL_SERVER)
        return None
    if sc != 200:
        log.debug("Finnhub %s returned %d", endpoint, sc)
        _set_failure(FAIL_NETWORK)
        return None

    try:
        data = r.json()
    except ValueError as exc:
        log.debug("Finnhub %s JSON parse failed: %s", endpoint, exc)
        _set_failure(FAIL_PARSE)
        return None

    # Empty list is a valid 200 — ticker not covered. Tag as empty so
    # the bulk fill can route it to the Finnhub blacklist.
    if data == [] or data is None:
        _set_failure(FAIL_EMPTY)
        return data

    _set_failure(None)
    return data


# ──────────────────────────────────────────────────────────────────────
# High-level fetchers
# ──────────────────────────────────────────────────────────────────────

def verify_api_key() -> bool:
    """Cheap sanity probe (`/quote?symbol=AAPL`) used by the Finnhub
    auto-pause flow to distinguish a real block from a bad key. Returns
    True if the key works, False otherwise. Sets ``last_failure_kind``
    on failure so the caller can decide what to do (e.g., FAIL_AUTH →
    halt, FAIL_NETWORK → retry pause)."""
    data = _request("/quote", {"symbol": "AAPL"})
    return bool(data and isinstance(data, dict) and "c" in data)


def fetch_earnings_history(symbol: str, *, limit: int = 0) -> list[dict] | None:
    """GET /stock/earnings — full quarterly history for one ticker.
    Returns a list of records (newest-first per the endpoint contract)
    or None on hard failure. ``[]`` is a valid result (ETF / not
    covered) and is returned as-is; ``last_failure_kind`` will be
    FAIL_EMPTY so the caller can route the ticker to the Finnhub
    blacklist.

    Records carry the verbatim Finnhub field names: symbol, period
    (YYYY-MM-DD period-end), year, quarter, actual, estimate, surprise,
    surprisePercent, revenueActual, revenueEstimate.

    ``limit=0`` (default) means full history; positive integer caps
    the response.
    """
    params: dict = {"symbol": symbol.upper()}
    if limit and limit > 0:
        params["limit"] = int(limit)
    data = _request("/stock/earnings", params)
    if data is None:
        return None
    if not isinstance(data, list):
        log.debug("Finnhub /stock/earnings non-list shape for %s: %r",
                  symbol, type(data).__name__)
        _set_failure(FAIL_PARSE)
        return None
    return data


def fetch_calendar_earnings_window(
    *,
    start: date,
    end: date,
    symbol: Optional[str] = None,
) -> list[dict] | None:
    """GET /calendar/earnings?from=…&to=… — events in date range.

    With ``symbol``: just that ticker's events. Without: every ticker
    reporting in the window. Used by the Finnhub bulk fill to recover
    real announcement dates for /stock/earnings rows (which only carry
    `period` = fiscal-quarter end, NOT the announcement date).

    Returns a list of event dicts. Each event has at minimum:
    `date` (YYYY-MM-DD announcement), `symbol`, `quarter`, `year`,
    plus EPS / revenue actual+estimate when known.
    Returns ``[]`` for an empty window and None on hard failure.
    """
    params: dict = {
        "from": start.isoformat(),
        "to":   end.isoformat(),
    }
    if symbol:
        params["symbol"] = symbol.upper()

    data = _request("/calendar/earnings", params)
    if data is None:
        return None
    if isinstance(data, list) and data == []:
        # Spec defines /calendar/earnings as returning a dict — a bare
        # empty list would be unusual but treat as empty window.
        return []
    if not isinstance(data, dict):
        _set_failure(FAIL_PARSE)
        return None
    events = data.get("earningsCalendar")
    if events is None:
        return []
    if not isinstance(events, list):
        _set_failure(FAIL_PARSE)
        return None
    return events


def fetch_earnings_dates(
    symbol: str, *, days_back: int = 365, days_forward: int = 365,
) -> tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """Return (last_earnings, next_earnings) for a ticker via Finnhub
    /calendar/earnings — dates only; EPS values are not currently used by
    the scanner. Either or both may be None if the data is unavailable."""
    today = date.today()
    data = _request(
        "/calendar/earnings",
        {
            "symbol": symbol.upper(),
            "from": (today - timedelta(days=days_back)).isoformat(),
            "to":   (today + timedelta(days=days_forward)).isoformat(),
        },
    )
    if not data or not isinstance(data, dict):
        return None, None
    events = data.get("earningsCalendar") or []
    if not events:
        return None, None

    today_ts = pd.Timestamp(today)
    past_dates = []
    future_dates = []
    for evt in events:
        d_str = evt.get("date")
        if not d_str:
            continue
        try:
            ts = pd.Timestamp(d_str)
        except Exception:
            continue
        if ts <= today_ts:
            past_dates.append(ts)
        else:
            future_dates.append(ts)

    last_e = max(past_dates) if past_dates else None
    next_e = min(future_dates) if future_dates else None
    return last_e, next_e


def fetch_company_profile(symbol: str) -> Optional[dict]:
    """Return Finnhub /stock/profile2 dict for a ticker, or None.

    Useful keys for the sector map:
        finnhubIndustry  (e.g. "Semiconductors")
        gind             (GICS industry, when available)
        ggroup           (GICS group)
        gsector          (GICS sector — what we actually want)
    Finnhub uses its own coarse industry taxonomy on free tier; gsector is
    only present on paid plans. We read finnhubIndustry as the default
    sector source and let the caller map it through SECTOR_ETF_MAP.
    """
    return _request("/stock/profile2", {"symbol": symbol.upper()})


def fetch_sector(symbol: str) -> Optional[str]:
    """Return a single sector string for a ticker via Finnhub, or None.
    Falls back through the most-to-least-specific Finnhub fields."""
    profile = fetch_company_profile(symbol)
    if not profile:
        return None
    for key in ("gsector", "finnhubIndustry"):
        val = profile.get(key)
        if val and isinstance(val, str) and val.strip():
            return val.strip()
    return None
