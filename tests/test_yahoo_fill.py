"""Tests for yahoo_fill.py — Phase 3 split-out yfinance-only fill module.

Phase 3 dropped the Finnhub-first chain that lived inside the old
`earnings_cache._fetch_earnings_for_ticker`. Yahoo paths now go ONLY to
yfinance; Finnhub is a separate top-level fill (finnhub_fill.py).
"""
from __future__ import annotations

import pandas as pd
import pytest

from trade_scanner_fh import config, earnings_cache, earnings_raw, yahoo_fill


@pytest.fixture
def tmp_world(tmp_path, monkeypatch):
    """Redirect parquet path + raw dir into a tmp tree."""
    monkeypatch.setattr(earnings_cache.config, "EARNINGS_PARQUET",
                        tmp_path / "earnings.parquet")
    raw_root = tmp_path / "earnings_raw"
    monkeypatch.setattr(config, "RAW_EARNINGS_DIR", raw_root)
    raw_root.mkdir()
    for src in config.RAW_SOURCES:
        (raw_root / src).mkdir()
    monkeypatch.setattr(yahoo_fill.time, "sleep", lambda *_: None)
    return tmp_path


def _mock_yfinance(monkeypatch, dates: list[pd.Timestamp]):
    """Patch yfinance.Ticker to return a DataFrame with `dates` as index."""
    ed_df = pd.DataFrame(
        {"EPS Estimate": [1.0] * len(dates)},
        index=pd.DatetimeIndex(dates, name="Earnings Date"),
    ) if dates else None

    class _FakeTicker:
        def __init__(self, sym): pass
        @property
        def earnings_dates(self): return ed_df

    import yfinance as yf
    monkeypatch.setattr(yf, "Ticker", _FakeTicker)


# ──────────────────────────────────────────────────────────────────────
# _fetch_one — yfinance only
# ──────────────────────────────────────────────────────────────────────

def test_fetch_one_returns_consumer_and_raw_rows(monkeypatch):
    today_ts = pd.Timestamp("2026-04-26")
    _mock_yfinance(monkeypatch, [
        pd.Timestamp("2026-02-15"),  # past
        pd.Timestamp("2026-05-10"),  # future
    ])
    row, raw = yahoo_fill._fetch_one("AAPL", today_ts, today_ts)
    assert row is not None
    assert row["last_earnings"] == pd.Timestamp("2026-02-15")
    assert row["next_earnings"] == pd.Timestamp("2026-05-10")
    assert row["source"] == "yahoo"
    assert raw is not None
    assert raw["ticker"] == "AAPL"
    assert "2026-02-15" in raw["all_dates_returned"]
    assert "2026-05-10" in raw["all_dates_returned"]


def test_fetch_one_handles_yfinance_exception(monkeypatch):
    """A raise from yfinance.Ticker(sym).earnings_dates must NOT
    propagate out — return (None, None) so the fill loop continues."""
    import yfinance as yf

    def boom(*a, **k):
        raise RuntimeError("rate limited")
    monkeypatch.setattr(yf, "Ticker", boom)

    today_ts = pd.Timestamp("2026-04-26")
    row, raw = yahoo_fill._fetch_one("AAPL", today_ts, today_ts)
    assert row is None and raw is None


def test_fetch_one_empty_or_none_returns_none(monkeypatch):
    today_ts = pd.Timestamp("2026-04-26")

    class _FakeTickerEmpty:
        def __init__(self, sym): pass
        @property
        def earnings_dates(self): return None

    import yfinance as yf
    monkeypatch.setattr(yf, "Ticker", _FakeTickerEmpty)
    row, raw = yahoo_fill._fetch_one("X", today_ts, today_ts)
    assert row is None
    assert raw is None


def test_fetch_one_drops_no_finnhub_chain(monkeypatch):
    """Phase 3 contract: yahoo_fill MUST NOT call into finnhub_client.
    If yfinance gives nothing, return (None, None) — there is no
    fallback."""
    from trade_scanner_fh import finnhub_client

    finnhub_called = {"flag": False}

    def must_not_call(*a, **k):
        finnhub_called["flag"] = True
        return (None, None)
    monkeypatch.setattr(finnhub_client, "fetch_earnings_dates", must_not_call)
    monkeypatch.setattr(finnhub_client, "is_configured", lambda: True)

    import yfinance as yf

    class _FakeTickerEmpty:
        def __init__(self, sym): pass
        @property
        def earnings_dates(self): return None

    monkeypatch.setattr(yf, "Ticker", _FakeTickerEmpty)
    row, raw = yahoo_fill._fetch_one(
        "AAPL", pd.Timestamp("2026-04-26"), pd.Timestamp("2026-04-26"),
    )
    assert row is None and raw is None
    assert finnhub_called["flag"] is False, (
        "yahoo_fill must never call Finnhub (Phase 3 separation)"
    )


# ──────────────────────────────────────────────────────────────────────
# targeted_fill_yahoo — flush behavior + raw layer
# ──────────────────────────────────────────────────────────────────────

def test_targeted_fill_flushes_incrementally(tmp_world, monkeypatch):
    """Regression: a kill mid-fill must leave the already-flushed rows
    on disk. Mirror of the old earnings_cache test."""
    def fake_fetch(sym, today_ts, now):
        return (
            {"ticker": sym, "last_earnings": pd.Timestamp("2026-02-01"),
             "next_earnings": pd.Timestamp("2026-05-01"),
             "updated_at": now, "source": "yahoo"},
            None,
        )
    monkeypatch.setattr(yahoo_fill, "_fetch_one", fake_fetch)

    counter = {"n": 0}
    stop = [False]

    def fake_progress(d, t):
        counter["n"] = d
        if d >= 25:
            stop[0] = True

    syms = [f"T{i:04d}" for i in range(1000)]
    filled, errors = yahoo_fill.targeted_fill_yahoo(
        syms, blacklist=set(),
        progress_cb=fake_progress,
        stop_flag=stop,
        flush_every=10,
    )
    saved = pd.read_parquet(earnings_cache.config.EARNINGS_PARQUET)
    assert len(saved) >= 10
    assert all(t.startswith("T") for t in saved["ticker"])


def test_targeted_fill_writes_yahoo_raw_layer(tmp_world, monkeypatch):
    """End-to-end: a Yahoo fill emits rows into
    earnings_raw/yahoo/<run_id>.parquet."""
    def fake_fetch(sym, today_ts, now):
        return (
            {"ticker": sym, "last_earnings": pd.Timestamp("2026-02-01"),
             "next_earnings": pd.Timestamp("2026-05-01"),
             "updated_at": now, "source": "yahoo"},
            {"ticker": sym, "all_dates_returned": "2026-02-01;2026-05-01"},
        )
    monkeypatch.setattr(yahoo_fill, "_fetch_one", fake_fetch)

    yahoo_fill.targeted_fill_yahoo(
        ["AAPL", "MSFT"], blacklist=set(), flush_every=1,
    )
    df = earnings_raw.read_raw(config.RAW_SOURCE_YAHOO)
    assert len(df) == 2
    assert set(df["ticker"]) == {"AAPL", "MSFT"}


def test_targeted_fill_skips_blacklisted_tickers(tmp_world, monkeypatch):
    fetched: list[str] = []

    def fake_fetch(sym, today_ts, now):
        fetched.append(sym)
        return (
            {"ticker": sym, "last_earnings": pd.Timestamp("2026-02-01"),
             "next_earnings": pd.Timestamp("2026-05-01"),
             "updated_at": now, "source": "yahoo"},
            None,
        )
    monkeypatch.setattr(yahoo_fill, "_fetch_one", fake_fetch)

    yahoo_fill.targeted_fill_yahoo(
        ["AAPL", "BAD", "MSFT"], blacklist={"BAD"}, flush_every=1,
    )
    assert fetched == ["AAPL", "MSFT"]


# ──────────────────────────────────────────────────────────────────────
# spot_fill_yahoo
# ──────────────────────────────────────────────────────────────────────

def test_spot_fill_writes_one_row(tmp_world, monkeypatch):
    _mock_yfinance(monkeypatch, [
        pd.Timestamp("2026-02-15"),
        pd.Timestamp("2026-05-10"),
    ])
    count, status = yahoo_fill.spot_fill_yahoo("AAPL", blacklist=set())
    assert (count, status) == (1, "ok")
    df = earnings_cache.load_earnings_cache()
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    assert aapl["source"] == "yahoo"
    assert aapl["last_earnings"] == pd.Timestamp("2026-02-15")
    assert aapl["next_earnings"] == pd.Timestamp("2026-05-10")


def test_spot_fill_blacklisted_returns_status(tmp_world):
    count, status = yahoo_fill.spot_fill_yahoo("X", blacklist={"X"})
    assert (count, status) == (0, "blacklisted")


def test_spot_fill_no_data_status(tmp_world, monkeypatch):
    _mock_yfinance(monkeypatch, [])
    count, status = yahoo_fill.spot_fill_yahoo("X", blacklist=set())
    assert (count, status) == (0, "no_data")


def test_spot_fill_writes_to_raw_layer(tmp_world, monkeypatch):
    _mock_yfinance(monkeypatch, [pd.Timestamp("2026-02-15")])
    yahoo_fill.spot_fill_yahoo("AAPL", blacklist=set())
    df = earnings_raw.read_raw(config.RAW_SOURCE_YAHOO)
    assert len(df) == 1
    assert df.iloc[0]["ticker"] == "AAPL"


# ──────────────────────────────────────────────────────────────────────
# Audit fix: yahoo fills trigger reconcile so subsequent fills can't
# clobber freshly-written rows before they're consolidated.
# ──────────────────────────────────────────────────────────────────────

def test_targeted_fill_triggers_reconcile_after_write(tmp_world, monkeypatch):
    from trade_scanner_fh import earnings_reconcile
    reconcile_calls: list = []

    def spy_reconcile(*args, **kwargs):
        reconcile_calls.append(kwargs)
        return (0, 0, 0)

    monkeypatch.setattr(
        earnings_reconcile, "reconcile_earnings_dates", spy_reconcile,
    )

    def fake_fetch(sym, today_ts, now):
        return (
            {"ticker": sym, "last_earnings": pd.Timestamp("2026-02-01"),
             "next_earnings": pd.Timestamp("2026-12-01"),
             "updated_at": now, "source": "yahoo"},
            None,
        )
    monkeypatch.setattr(yahoo_fill, "_fetch_one", fake_fetch)

    yahoo_fill.targeted_fill_yahoo(
        ["AAPL", "MSFT"], blacklist=set(), flush_every=1,
    )
    assert len(reconcile_calls) == 1
    affected = reconcile_calls[0].get("affected_tickers", [])
    assert set(affected) == {"AAPL", "MSFT"}


def test_spot_fill_triggers_reconcile_after_write(tmp_world, monkeypatch):
    from trade_scanner_fh import earnings_reconcile
    reconcile_calls: list = []

    def spy_reconcile(*args, **kwargs):
        reconcile_calls.append(kwargs)
        return (0, 0, 0)

    monkeypatch.setattr(
        earnings_reconcile, "reconcile_earnings_dates", spy_reconcile,
    )
    _mock_yfinance(monkeypatch, [pd.Timestamp("2026-12-15")])
    yahoo_fill.spot_fill_yahoo("AAPL", blacklist=set())
    assert len(reconcile_calls) == 1
    assert reconcile_calls[0].get("affected_tickers") == ["AAPL"]
