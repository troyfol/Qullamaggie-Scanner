"""Tests for finviz_client.py — earningsData extraction + failure kinds.

The HTTP layer is mocked (no live finviz hits) and the rate limiter is
neutralized so tests don't sleep on the slow finviz pace.
"""
from __future__ import annotations

import pytest

from trade_scanner_fh import finviz_client


@pytest.fixture(autouse=True)
def _no_rate_limit(monkeypatch):
    """Neutralize the (deliberately slow) finviz rate limiter in tests."""
    monkeypatch.setattr(finviz_client._limiter, "acquire", lambda: None)


class _Resp:
    def __init__(self, status=200, text=""):
        self.status_code = status
        self.text = text
        self.headers = {}


def _page_with_earnings(arr_json: str, *, pad: int = 0) -> str:
    body = (
        '<html><body><div class="snapshot-td-content">x</div>'
        '<script>window.__d={"earningsData":' + arr_json + '};</script>'
        + ("x" * pad) +
        "</body></html>"
    )
    return body


_PAST = (
    '{"ticker":"AAOI","fiscalPeriod":"2026Q1",'
    '"earningsDate":"2026-05-07T16:30:00","fiscalEndDate":"2026-03-31",'
    '"epsActual":-0.07,"epsEstimate":-0.049,"epsReportedActual":-0.19,'
    '"salesActual":151.144,"salesEstimate":156.9765}'
)
_FUTURE = (
    '{"ticker":"AAOI","fiscalPeriod":"2026Q2","earningsDate":null,'
    '"fiscalEndDate":"2026-06-30","epsActual":null,"epsEstimate":null,'
    '"epsReportedEstimate":0.0153}'
)


def test_is_configured_always_true():
    assert finviz_client.is_configured() is True


def test_fetch_earnings_parses_earningsdata(monkeypatch):
    html = _page_with_earnings(f"[{_PAST},{_FUTURE}]")
    monkeypatch.setattr(finviz_client.creq, "get",
                        lambda *a, **k: _Resp(200, html))
    data = finviz_client.fetch_earnings("AAOI")
    assert finviz_client.last_failure_kind() is None
    assert isinstance(data, list) and len(data) == 2
    assert data[0]["epsActual"] == -0.07
    assert data[0]["fiscalEndDate"] == "2026-03-31"


def test_fetch_earnings_empty_array_is_returned(monkeypatch):
    html = _page_with_earnings("[]")
    monkeypatch.setattr(finviz_client.creq, "get",
                        lambda *a, **k: _Resp(200, html))
    data = finviz_client.fetch_earnings("AAOI")
    assert data == []
    assert finviz_client.last_failure_kind() is None


def test_fetch_earnings_no_key_on_quote_page_is_empty(monkeypatch):
    # A real finviz quote page (snapshot-td marker + ticker echo, big)
    # but no earningsData → uncovered ticker (ETF / fund) → FAIL_EMPTY.
    body = ('<div class="snapshot-td-label">x</div>'
            '<a href="quote.ashx?t=SPY&p=d">SPY</a>' + ("y" * 60_000))
    monkeypatch.setattr(finviz_client.creq, "get",
                        lambda *a, **k: _Resp(200, body))
    assert finviz_client.fetch_earnings("SPY") is None
    assert finviz_client.last_failure_kind() == finviz_client.FAIL_EMPTY


def test_fetch_earnings_stripped_snapshot_marker_is_parse_not_empty(monkeypatch):
    # B2: ticker echo present but the snapshot-td marker stripped (finviz
    # redesign) — must degrade to a LOUD parse_error, never a silent
    # FAIL_EMPTY that would blacklist the ticker.
    body = '<a href="quote.ashx?t=AAOI&p=d">AAOI</a>' + ("y" * 60_000)
    monkeypatch.setattr(finviz_client.creq, "get",
                        lambda *a, **k: _Resp(200, body))
    assert finviz_client.fetch_earnings("AAOI") is None
    assert finviz_client.last_failure_kind() == finviz_client.FAIL_PARSE


def test_fetch_earnings_stripped_ticker_echo_is_parse_not_empty(monkeypatch):
    # B2: the inverse disagreement — snapshot-td present but no ticker
    # echo anywhere on a big page → also a redesign suspect → parse_error.
    body = '<div class="snapshot-td-label">x</div>' + ("y" * 60_000)
    monkeypatch.setattr(finviz_client.creq, "get",
                        lambda *a, **k: _Resp(200, body))
    assert finviz_client.fetch_earnings("SPY") is None
    assert finviz_client.last_failure_kind() == finviz_client.FAIL_PARSE


def test_fetch_earnings_tiny_nonquote_page_is_blocked(monkeypatch):
    # Small page with no quote markers + no earningsData → bot challenge.
    monkeypatch.setattr(finviz_client.creq, "get",
                        lambda *a, **k: _Resp(200, "<html>nope</html>"))
    assert finviz_client.fetch_earnings("AAOI") is None
    assert finviz_client.last_failure_kind() == finviz_client.FAIL_BLOCKED


@pytest.mark.parametrize("code,kind", [
    (429, finviz_client.FAIL_RATE_LIMITED),
    (403, finviz_client.FAIL_FORBIDDEN),
    (503, finviz_client.FAIL_SERVER),
    (500, finviz_client.FAIL_SERVER),
])
def test_fetch_earnings_http_status_failures(monkeypatch, code, kind):
    monkeypatch.setattr(finviz_client.creq, "get",
                        lambda *a, **k: _Resp(code, ""))
    assert finviz_client.fetch_earnings("AAOI") is None
    assert finviz_client.last_failure_kind() == kind


def test_fetch_earnings_404_is_empty_not_network(monkeypatch):
    # Delisted / junk tickers (e.g. ADAMG/ADAMH) 404 on finviz. This must
    # be a coverage miss (EMPTY → blacklist), NOT a network failure —
    # otherwise a cluster of dead tickers spuriously trips the fill loop's
    # consecutive-block backoff. Regression guard for that bug.
    monkeypatch.setattr(finviz_client.creq, "get",
                        lambda *a, **k: _Resp(404, "Not Found"))
    assert finviz_client.fetch_earnings("ADAMG") is None
    assert finviz_client.last_failure_kind() == finviz_client.FAIL_EMPTY


def test_fetch_earnings_unmapped_non200_is_network(monkeypatch):
    # A status that isn't 200/404/429/403/5xx (e.g. 418) → network.
    monkeypatch.setattr(finviz_client.creq, "get",
                        lambda *a, **k: _Resp(418, ""))
    assert finviz_client.fetch_earnings("AAOI") is None
    assert finviz_client.last_failure_kind() == finviz_client.FAIL_NETWORK


def test_fetch_earnings_network_exception(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("connection reset")
    monkeypatch.setattr(finviz_client.creq, "get", boom)
    assert finviz_client.fetch_earnings("AAOI") is None
    assert finviz_client.last_failure_kind() == finviz_client.FAIL_NETWORK


def test_fetch_earnings_malformed_json_is_parse_error(monkeypatch):
    html = '<div class="snapshot-td">x</div><script>"earningsData":[{bad json]</script>'
    monkeypatch.setattr(finviz_client.creq, "get",
                        lambda *a, **k: _Resp(200, html))
    assert finviz_client.fetch_earnings("AAOI") is None
    assert finviz_client.last_failure_kind() == finviz_client.FAIL_PARSE


def test_extract_earnings_data_bracket_matching():
    # Nested brackets inside the array must not end it early.
    html = '"earningsData":[{"a":[1,2,3],"b":"]"}] trailing'
    out = finviz_client._extract_earnings_data(html)
    assert isinstance(out, list) and len(out) == 1
    assert out[0]["a"] == [1, 2, 3]


def test_extract_earnings_data_absent_returns_none():
    assert finviz_client._extract_earnings_data("<html>no key</html>") is None
