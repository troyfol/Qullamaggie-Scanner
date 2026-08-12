"""Ticker allowlist — audit 2026-08-12 (SEC-3).

Symbols arrive from NASDAQ FTP (plaintext, anonymous — the realistic MITM
vector), a third-party GitHub mirror and SEC EDGAR, and two builders
interpolate them straight into a URL path/query where requests' safe-parameter
encoding does not apply. The only prior gate was a DENYLIST (`[+=%#@!]`), which
blocks `AAPL?x=1` and `AAPL#frag` but NOT `AAPL/../../admin` — the audit
verified that one reaching the finviz URL intact.

The allowlist is deliberately wider than `PLAUSIBLE_TICKER_RE`: reusing that
stricter pattern would drop 405 legitimate live symbols (preferred shares and
rights like ABR$D, AIIA^, AXIA$C), turning a security fix into a data bug.
"""
from __future__ import annotations

import pandas as pd
import pytest

from trade_scanner_fh import config


# ── the allowlist itself ───────────────────────────────────────────────

@pytest.mark.parametrize("sym", [
    "AAPL", "BRK.B", "BF-B", "A", "ZZZZ",
    # The 405-symbol class the audit warned about — preferred shares, rights,
    # and warrants that are genuinely in the live universe.
    "ABR$D", "AIIA^", "AXIA$C", "FITB-PA", "KCAC-WT", "BANC-PF",
    "1COV", "3M",
])
def test_legitimate_symbols_are_accepted(sym):
    assert config.url_safe_ticker(sym) == sym


@pytest.mark.parametrize("sym", [
    "AAPL/../../admin",     # path traversal — survived the old denylist
    "AA PL",                # space — also survived it
    "AAPL?x=1",             # query injection
    "AAPL#frag",            # fragment injection
    "AAPL%2f..",            # encoded traversal
    "../../etc/passwd",
    "AAPL\\..\\..",         # windows separator
    "AAPL\ttab",            # embedded whitespace
    "AAPL;rm -rf",
    "AAPL\nBBBB",           # embedded newline — strip() only trims the ends
    "", "   ",
    "TOOLONGSYMBOL12345",   # beyond the 12-char bound
])
def test_hostile_symbols_are_rejected(sym):
    assert config.url_safe_ticker(sym) is None


def test_symbol_is_normalised_not_merely_checked():
    assert config.url_safe_ticker("  aapl  ") == "AAPL"
    # Surrounding whitespace, including a trailing newline, is stripped rather
    # than rejected — the same normalisation the rest of the app applies.
    assert config.url_safe_ticker("AAPL\n") == "AAPL"


def test_ingest_filter_rejects_a_stringified_nan():
    """`nan` reaches the CSV when a blank cell is read as a float and cast to
    str. The ingest filter matches WITHOUT upper-casing, so the lowercase form
    is rejected there — which is where it matters. (`url_safe_ticker("nan")`
    normalises to "NAN", a validly-shaped symbol, and is not the guard for
    this case.)"""
    assert not config.URL_SAFE_TICKER_RE.match("nan")


def test_non_strings_are_rejected():
    for junk in (None, 123, b"AAPL", ["AAPL"], float("nan")):
        assert config.url_safe_ticker(junk) is None


def test_allowlist_accepts_the_entire_live_symbol_shape():
    """Guardrail against a future tightening that silently loses tickers:
    every character class present in the live universe must stay legal."""
    for sym in ("ABR$D", "AIIA^", "BRK.B", "BF-B", "AAPL", "0001"):
        assert config.URL_SAFE_TICKER_RE.match(sym), sym


# ── applied at ingest ──────────────────────────────────────────────────

def test_universe_filter_drops_traversal_shaped_symbols():
    from trade_scanner_fh import ticker_universe
    df = pd.DataFrame({
        "symbol": ["AAPL", "AAPL/../../admin", "AA PL", "ABR$D", "AIIA^", ""],
        "etf": [False] * 6,
        "adr": [False] * 6,
    })
    out = ticker_universe._filter_symbols(df)
    kept = set(out["symbol"])
    assert "AAPL" in kept
    assert "ABR$D" in kept and "AIIA^" in kept   # the 405-symbol class survives
    assert "AAPL/../../admin" not in kept
    assert "AA PL" not in kept
    assert "" not in kept


# ── applied at the URL builders ────────────────────────────────────────

def test_finviz_refuses_a_hostile_symbol_before_any_request(monkeypatch):
    from trade_scanner_fh import finviz_client
    called = []
    monkeypatch.setattr(finviz_client.creq, "get",
                        lambda *a, **k: called.append(a))
    assert finviz_client.fetch_earnings("AAPL/../../admin") is None
    assert not called, "a request was issued for a traversal-shaped symbol"


def test_finviz_url_encodes_permitted_specials(monkeypatch):
    """`$` and `^` are legal in the allowlist but must not go out raw."""
    from trade_scanner_fh import finviz_client
    seen = {}

    class _Resp:
        status_code = 200
        text = ""
        content = b""
        headers = {}

    def fake_get(url, *a, **k):
        seen["url"] = url
        return _Resp()

    monkeypatch.setattr(finviz_client.creq, "get", fake_get)
    monkeypatch.setattr(finviz_client._limiter, "acquire", lambda: None)
    finviz_client.fetch_earnings("ABR$D")
    assert "ABR%24D" in seen["url"]
    assert "$" not in seen["url"]


def test_zacks_refuses_a_hostile_symbol_before_any_request(monkeypatch):
    from trade_scanner_fh import zacks_scraper
    sess = zacks_scraper.ZacksSession.__new__(zacks_scraper.ZacksSession)
    called = []
    sess._session = type("S", (), {
        "get": lambda self, *a, **k: called.append(a)})()
    sess._timeout = 5
    sess.last_failure_kind = None
    assert sess.fetch("AAPL/../../admin") is None
    assert not called, "a request was issued for a traversal-shaped symbol"
