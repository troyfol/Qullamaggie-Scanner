"""Tests for finnhub_client.py — keyring helpers, REST request layer,
and high-level fetchers."""
import threading
import time
from unittest.mock import patch

import pandas as pd
import pytest

from trade_scanner_fh import finnhub_client


# ──────────────────────────────────────────────────────────────────────
# In-memory keyring substitute used as a fixture
# ──────────────────────────────────────────────────────────────────────

@pytest.fixture
def fake_keyring(monkeypatch):
    """Replace keyring.get/set/delete_password with an in-memory dict so
    tests don't touch the actual OS credential manager."""
    store: dict[tuple[str, str], str] = {}

    def _get(service, username):
        return store.get((service, username))

    def _set(service, username, password):
        store[(service, username)] = password

    def _delete(service, username):
        if (service, username) not in store:
            import keyring.errors
            raise keyring.errors.PasswordDeleteError("not found")
        del store[(service, username)]

    monkeypatch.setattr(finnhub_client.keyring, "get_password", _get)
    monkeypatch.setattr(finnhub_client.keyring, "set_password", _set)
    monkeypatch.setattr(finnhub_client.keyring, "delete_password", _delete)
    return store


# ──────────────────────────────────────────────────────────────────────
# Keyring helpers
# ──────────────────────────────────────────────────────────────────────

def test_get_api_key_empty_returns_none(fake_keyring):
    assert finnhub_client.get_api_key() is None
    assert finnhub_client.is_configured() is False


def test_set_get_round_trip(fake_keyring):
    assert finnhub_client.set_api_key("abc123") is True
    assert finnhub_client.get_api_key() == "abc123"
    assert finnhub_client.is_configured() is True


def test_set_strips_whitespace(fake_keyring):
    finnhub_client.set_api_key("  spaced-key  ")
    assert finnhub_client.get_api_key() == "spaced-key"


def test_set_empty_key_is_rejected(fake_keyring):
    assert finnhub_client.set_api_key("") is False
    assert finnhub_client.set_api_key("   ") is False
    assert finnhub_client.get_api_key() is None


def test_clear_api_key(fake_keyring):
    finnhub_client.set_api_key("abc")
    assert finnhub_client.is_configured()
    assert finnhub_client.clear_api_key() is True
    assert finnhub_client.get_api_key() is None
    # Idempotent — second clear returns False (key wasn't there)
    assert finnhub_client.clear_api_key() is False


# ──────────────────────────────────────────────────────────────────────
# _request — auth + error semantics
# ──────────────────────────────────────────────────────────────────────

def test_request_returns_none_when_no_key(fake_keyring):
    """Without a stored key, _request should NEVER hit the network."""
    with patch.object(finnhub_client.requests, "get") as mock_get:
        result = finnhub_client._request("/anything", {"x": 1})
        assert result is None
        mock_get.assert_not_called()


def test_request_appends_token_to_params(fake_keyring):
    finnhub_client.set_api_key("test-token")

    class _Resp:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return {"ok": True}

    with patch.object(finnhub_client.requests, "get", return_value=_Resp()) as mg:
        finnhub_client._request("/x", {"symbol": "AAPL"})
        called_params = mg.call_args.kwargs["params"]
        assert called_params["token"] == "test-token"
        assert called_params["symbol"] == "AAPL"


@pytest.mark.parametrize("status_code", [401, 429])
def test_request_returns_none_on_auth_or_rate_limit(fake_keyring, status_code, monkeypatch):
    finnhub_client.set_api_key("k")
    monkeypatch.setattr(finnhub_client.time, "sleep", lambda *_: None)

    class _Resp:
        def __init__(self, code): self.status_code = code
        def raise_for_status(self): pass
        def json(self): return {}

    with patch.object(finnhub_client.requests, "get", return_value=_Resp(status_code)):
        assert finnhub_client._request("/x", {}) is None


# ──────────────────────────────────────────────────────────────────────
# fetch_earnings_dates — parse logic
# ──────────────────────────────────────────────────────────────────────

def test_fetch_earnings_dates_no_key_returns_none_pair(fake_keyring):
    last, nxt = finnhub_client.fetch_earnings_dates("AAPL")
    assert last is None and nxt is None


def test_fetch_earnings_dates_parses_calendar_response(fake_keyring, monkeypatch):
    """Mock /calendar/earnings to return a known mix of past + future
    events; verify the function picks the LATEST past and EARLIEST future."""
    finnhub_client.set_api_key("k")
    today = pd.Timestamp.today().normalize()
    payload = {
        "earningsCalendar": [
            {"symbol": "AAPL", "date": (today - pd.Timedelta(days=200)).date().isoformat()},
            {"symbol": "AAPL", "date": (today - pd.Timedelta(days=10)).date().isoformat()},
            {"symbol": "AAPL", "date": (today + pd.Timedelta(days=30)).date().isoformat()},
            {"symbol": "AAPL", "date": (today + pd.Timedelta(days=120)).date().isoformat()},
        ]
    }

    def fake_request(endpoint, params, **kwargs):
        assert endpoint == "/calendar/earnings"
        assert params["symbol"] == "AAPL"
        return payload

    monkeypatch.setattr(finnhub_client, "_request", fake_request)
    last, nxt = finnhub_client.fetch_earnings_dates("AAPL")
    assert last == today - pd.Timedelta(days=10)   # most recent past
    assert nxt == today + pd.Timedelta(days=30)    # earliest future


def test_fetch_earnings_dates_empty_calendar(fake_keyring, monkeypatch):
    finnhub_client.set_api_key("k")
    monkeypatch.setattr(finnhub_client, "_request",
                        lambda *a, **k: {"earningsCalendar": []})
    assert finnhub_client.fetch_earnings_dates("AAPL") == (None, None)


def test_fetch_earnings_dates_all_future(fake_keyring, monkeypatch):
    """No past events → last_e is None, next_e is the soonest future."""
    finnhub_client.set_api_key("k")
    today = pd.Timestamp.today().normalize()
    payload = {"earningsCalendar": [
        {"symbol": "X", "date": (today + pd.Timedelta(days=5)).date().isoformat()},
        {"symbol": "X", "date": (today + pd.Timedelta(days=60)).date().isoformat()},
    ]}
    monkeypatch.setattr(finnhub_client, "_request", lambda *a, **k: payload)
    last, nxt = finnhub_client.fetch_earnings_dates("X")
    assert last is None
    assert nxt == today + pd.Timedelta(days=5)


# ──────────────────────────────────────────────────────────────────────
# fetch_sector — fall-through gsector → finnhubIndustry
# ──────────────────────────────────────────────────────────────────────

def test_fetch_sector_prefers_gsector(fake_keyring, monkeypatch):
    finnhub_client.set_api_key("k")
    monkeypatch.setattr(finnhub_client, "_request",
                        lambda *a, **k: {"gsector": "Information Technology",
                                         "finnhubIndustry": "Software"})
    assert finnhub_client.fetch_sector("AAPL") == "Information Technology"


def test_fetch_sector_falls_back_to_finnhub_industry(fake_keyring, monkeypatch):
    finnhub_client.set_api_key("k")
    monkeypatch.setattr(finnhub_client, "_request",
                        lambda *a, **k: {"finnhubIndustry": "Semiconductors"})
    assert finnhub_client.fetch_sector("NVDA") == "Semiconductors"


def test_fetch_sector_returns_none_when_no_useful_fields(fake_keyring, monkeypatch):
    finnhub_client.set_api_key("k")
    monkeypatch.setattr(finnhub_client, "_request",
                        lambda *a, **k: {"ticker": "X", "name": "X Corp"})
    assert finnhub_client.fetch_sector("X") is None


# ──────────────────────────────────────────────────────────────────────
# Rate limiter (singleton)
# ──────────────────────────────────────────────────────────────────────

def test_rate_limiter_enforces_minimum_interval():
    limiter = finnhub_client._RateLimiter(0.05)
    t0 = time.monotonic()
    for _ in range(3):
        limiter.acquire()
    # 3 acquires at 0.05s spacing → at least 0.10s elapsed (first is free)
    assert time.monotonic() - t0 >= 0.09


def test_rate_limiter_thread_safe():
    limiter = finnhub_client._RateLimiter(0.02)
    t0 = time.monotonic()

    def worker():
        for _ in range(5):
            limiter.acquire()

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # 20 acquires × 0.02s = 0.4s minimum (19 inter-call gaps × 0.02s)
    assert time.monotonic() - t0 >= 0.36


# ──────────────────────────────────────────────────────────────────────
# Phase 2 — failure-kind classification on _request
# ──────────────────────────────────────────────────────────────────────

class _MockResp:
    def __init__(self, status: int, payload=None):
        self.status_code = status
        self._payload = payload

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


@pytest.fixture
def stubbed_key(fake_keyring, monkeypatch):
    finnhub_client.set_api_key("phase2-key")
    # Skip the rate limiter so tests don't sleep.
    monkeypatch.setattr(finnhub_client._limiter, "acquire", lambda: None)
    finnhub_client._set_failure(None)
    return None


def test_request_200_success_clears_failure(stubbed_key, monkeypatch):
    monkeypatch.setattr(finnhub_client.requests, "get",
                        lambda *a, **k: _MockResp(200, {"ok": True}))
    out = finnhub_client._request("/x", {})
    assert out == {"ok": True}
    assert finnhub_client.last_failure_kind() is None


def test_request_200_empty_list_sets_FAIL_EMPTY(stubbed_key, monkeypatch):
    monkeypatch.setattr(finnhub_client.requests, "get",
                        lambda *a, **k: _MockResp(200, []))
    out = finnhub_client._request("/x", {})
    assert out == []
    assert finnhub_client.last_failure_kind() == finnhub_client.FAIL_EMPTY


def test_request_429_sets_FAIL_RATE_LIMITED(stubbed_key, monkeypatch):
    monkeypatch.setattr(finnhub_client.requests, "get",
                        lambda *a, **k: _MockResp(429))
    assert finnhub_client._request("/x", {}) is None
    assert finnhub_client.last_failure_kind() == finnhub_client.FAIL_RATE_LIMITED


def test_request_401_sets_FAIL_AUTH(stubbed_key, monkeypatch):
    monkeypatch.setattr(finnhub_client.requests, "get",
                        lambda *a, **k: _MockResp(401))
    assert finnhub_client._request("/x", {}) is None
    assert finnhub_client.last_failure_kind() == finnhub_client.FAIL_AUTH


def test_request_403_sets_FAIL_FORBIDDEN(stubbed_key, monkeypatch):
    monkeypatch.setattr(finnhub_client.requests, "get",
                        lambda *a, **k: _MockResp(403))
    assert finnhub_client._request("/x", {}) is None
    assert finnhub_client.last_failure_kind() == finnhub_client.FAIL_FORBIDDEN


def test_request_503_sets_FAIL_SERVER(stubbed_key, monkeypatch):
    monkeypatch.setattr(finnhub_client.requests, "get",
                        lambda *a, **k: _MockResp(503))
    assert finnhub_client._request("/x", {}) is None
    assert finnhub_client.last_failure_kind() == finnhub_client.FAIL_SERVER


def test_request_network_error_sets_FAIL_NETWORK(stubbed_key, monkeypatch):
    def boom(*a, **k):
        raise finnhub_client.requests.exceptions.ConnectionError("kaboom")
    monkeypatch.setattr(finnhub_client.requests, "get", boom)
    assert finnhub_client._request("/x", {}) is None
    assert finnhub_client.last_failure_kind() == finnhub_client.FAIL_NETWORK


def test_request_no_key_sets_FAIL_AUTH(monkeypatch):
    monkeypatch.setattr(finnhub_client, "get_api_key", lambda: None)
    assert finnhub_client._request("/x", {}) is None
    assert finnhub_client.last_failure_kind() == finnhub_client.FAIL_AUTH


def test_request_json_parse_error_sets_FAIL_PARSE(stubbed_key, monkeypatch):
    monkeypatch.setattr(finnhub_client.requests, "get",
                        lambda *a, **k: _MockResp(200, ValueError("bad json")))
    assert finnhub_client._request("/x", {}) is None
    assert finnhub_client.last_failure_kind() == finnhub_client.FAIL_PARSE


# ──────────────────────────────────────────────────────────────────────
# Phase 2 — fetch_earnings_history
# ──────────────────────────────────────────────────────────────────────

def test_fetch_earnings_history_returns_verbatim_records(stubbed_key, monkeypatch):
    payload = [
        {"symbol": "AAPL", "period": "2025-09-30", "year": 2025, "quarter": 4,
         "actual": 1.64, "estimate": 1.60, "surprise": 0.04,
         "surprisePercent": 2.5,
         "revenueActual": 94930000000, "revenueEstimate": 94360000000},
        {"symbol": "AAPL", "period": "2025-06-30", "year": 2025, "quarter": 3,
         "actual": 1.40, "estimate": 1.35},
    ]
    monkeypatch.setattr(finnhub_client.requests, "get",
                        lambda *a, **k: _MockResp(200, payload))
    rows = finnhub_client.fetch_earnings_history("AAPL")
    assert rows == payload
    assert finnhub_client.last_failure_kind() is None


def test_fetch_earnings_history_passes_limit_param(stubbed_key, monkeypatch):
    captured = {}

    def fake_get(url, params=None, timeout=None, **kwargs):
        captured.update(params or {})
        return _MockResp(200, [])

    monkeypatch.setattr(finnhub_client.requests, "get", fake_get)
    finnhub_client.fetch_earnings_history("AAPL", limit=10)
    assert captured.get("limit") == 10
    assert captured.get("symbol") == "AAPL"


def test_fetch_earnings_history_no_limit_omits_param(stubbed_key, monkeypatch):
    captured = {}
    monkeypatch.setattr(
        finnhub_client.requests, "get",
        lambda url, params=None, timeout=None, **kw: (
            captured.update(params or {}), _MockResp(200, []))[1],
    )
    finnhub_client.fetch_earnings_history("AAPL")
    assert "limit" not in captured


def test_fetch_earnings_history_empty_response_sets_FAIL_EMPTY(stubbed_key, monkeypatch):
    monkeypatch.setattr(finnhub_client.requests, "get",
                        lambda *a, **k: _MockResp(200, []))
    rows = finnhub_client.fetch_earnings_history("VTI")
    assert rows == []
    assert finnhub_client.last_failure_kind() == finnhub_client.FAIL_EMPTY


def test_fetch_earnings_history_non_list_shape_sets_FAIL_PARSE(stubbed_key, monkeypatch):
    monkeypatch.setattr(finnhub_client.requests, "get",
                        lambda *a, **k: _MockResp(200, {"oops": True}))
    rows = finnhub_client.fetch_earnings_history("AAPL")
    assert rows is None
    assert finnhub_client.last_failure_kind() == finnhub_client.FAIL_PARSE


# ──────────────────────────────────────────────────────────────────────
# Phase 2 — fetch_calendar_earnings_window
# ──────────────────────────────────────────────────────────────────────

def test_fetch_calendar_earnings_window_returns_events(stubbed_key, monkeypatch):
    from datetime import date
    payload = {"earningsCalendar": [
        {"date": "2025-01-29", "symbol": "AAPL", "year": 2024, "quarter": 4},
        {"date": "2024-10-30", "symbol": "AAPL", "year": 2024, "quarter": 3},
    ]}
    monkeypatch.setattr(finnhub_client.requests, "get",
                        lambda *a, **k: _MockResp(200, payload))
    events = finnhub_client.fetch_calendar_earnings_window(
        start=date(2024, 1, 1), end=date(2025, 12, 31), symbol="AAPL",
    )
    assert events is not None
    assert len(events) == 2


def test_fetch_calendar_earnings_window_empty_returns_empty_list(stubbed_key, monkeypatch):
    from datetime import date
    monkeypatch.setattr(finnhub_client.requests, "get",
                        lambda *a, **k: _MockResp(200, {"earningsCalendar": []}))
    events = finnhub_client.fetch_calendar_earnings_window(
        start=date(2024, 1, 1), end=date(2024, 1, 31),
    )
    assert events == []


def test_fetch_calendar_earnings_window_non_dict_shape_sets_FAIL_PARSE(
    stubbed_key, monkeypatch,
):
    from datetime import date
    monkeypatch.setattr(finnhub_client.requests, "get",
                        lambda *a, **k: _MockResp(200, "not-json-dict"))
    events = finnhub_client.fetch_calendar_earnings_window(
        start=date(2024, 1, 1), end=date(2024, 1, 31),
    )
    assert events is None
    assert finnhub_client.last_failure_kind() == finnhub_client.FAIL_PARSE


# ──────────────────────────────────────────────────────────────────────
# Phase 2 — verify_api_key
# ──────────────────────────────────────────────────────────────────────

def test_verify_api_key_valid_quote(stubbed_key, monkeypatch):
    monkeypatch.setattr(finnhub_client.requests, "get",
                        lambda *a, **k: _MockResp(200, {"c": 175.5, "d": 1.2}))
    assert finnhub_client.verify_api_key() is True


def test_verify_api_key_401_returns_false(stubbed_key, monkeypatch):
    monkeypatch.setattr(finnhub_client.requests, "get",
                        lambda *a, **k: _MockResp(401))
    assert finnhub_client.verify_api_key() is False
    assert finnhub_client.last_failure_kind() == finnhub_client.FAIL_AUTH
