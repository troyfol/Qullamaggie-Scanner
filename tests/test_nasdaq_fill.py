"""Tests for nasdaq_fill.py — Phase 3 split-out Nasdaq calendar bulk."""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from trade_scanner_fh import config, earnings_cache, earnings_raw, nasdaq_fill


@pytest.fixture
def tmp_world(tmp_path, monkeypatch):
    monkeypatch.setattr(earnings_cache.config, "EARNINGS_PARQUET",
                        tmp_path / "earnings.parquet")
    raw_root = tmp_path / "earnings_raw"
    monkeypatch.setattr(config, "RAW_EARNINGS_DIR", raw_root)
    raw_root.mkdir()
    for src in config.RAW_SOURCES:
        (raw_root / src).mkdir()
    monkeypatch.setattr(nasdaq_fill.time, "sleep", lambda *_: None)
    return tmp_path


def _next_weekday(d: date, *, after: bool) -> date:
    """Walk forward (after=True) / backward (after=False) until we hit
    Mon-Fri. Used so tests don't depend on whether `date.today()` is a
    weekend (the fill skips weekends, so events scheduled on Sat/Sun
    would be invisible)."""
    step = 1 if after else -1
    while d.weekday() >= 5:
        d += timedelta(days=step)
    return d


def test_bulk_fill_nasdaq_writes_consumer_and_raw(tmp_world, monkeypatch):
    """End-to-end: simulate finance-calendars returning some events
    across a few weekdays; verify earnings_dates.parquet + the nasdaq
    raw layer both pick up the data, with source='nasdaq'."""
    universe = ["AAPL", "MSFT", "NVDA"]
    today = date.today()

    # Past + future weekdays inside the default ±90 window. Walk
    # off any weekend boundary so the bulk loop's weekday filter
    # doesn't skip our chosen anchor days.
    past_anchor = _next_weekday(today - timedelta(days=14), after=False)
    future_anchor = _next_weekday(today + timedelta(days=14), after=True)

    events_by_day = {
        past_anchor: ["AAPL", "MSFT"],
        future_anchor: ["NVDA"],
    }

    def fake_get_earnings_by_date(d):
        syms = events_by_day.get(d, [])
        if not syms:
            return pd.DataFrame()
        # Single placeholder column so .empty is False (pandas treats
        # zero-column frames as empty regardless of row count).
        return pd.DataFrame(
            {"_placeholder": [None] * len(syms)},
            index=pd.Index(syms, name="symbol"),
        )

    import finance_calendars.finance_calendars as fc
    monkeypatch.setattr(fc, "get_earnings_by_date", fake_get_earnings_by_date)

    filled, errors = nasdaq_fill.bulk_fill_nasdaq(
        universe, blacklist=set(), days_back=30, days_forward=30, delay=0,
    )
    assert filled == 3  # AAPL + MSFT + NVDA
    assert errors == 0

    df = earnings_cache.load_earnings_cache()
    assert df is not None
    assert (df["source"] == "nasdaq").all()

    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    assert aapl["last_earnings"] == pd.Timestamp(past_anchor)

    nvda = df.loc[df["ticker"] == "NVDA"].iloc[0]
    assert nvda["next_earnings"] == pd.Timestamp(future_anchor)

    raw = earnings_raw.read_raw(config.RAW_SOURCE_NASDAQ)
    assert len(raw) == 3


def test_bulk_fill_nasdaq_skips_blacklist(tmp_world, monkeypatch):
    """Each weekday in the window returns AAPL+BAD; verify BAD is
    filtered out by the blacklist."""
    def fake_get_earnings_by_date(d):
        return pd.DataFrame(
            {"_placeholder": [None, None]},
            index=pd.Index(["AAPL", "BAD"], name="symbol"),
        )

    import finance_calendars.finance_calendars as fc
    monkeypatch.setattr(fc, "get_earnings_by_date", fake_get_earnings_by_date)

    filled, _ = nasdaq_fill.bulk_fill_nasdaq(
        ["AAPL", "BAD"], blacklist={"BAD"},
        days_back=10, days_forward=10, delay=0,
    )
    df = earnings_cache.load_earnings_cache()
    assert df is not None
    assert "BAD" not in set(df["ticker"])
    assert "AAPL" in set(df["ticker"])


def test_bulk_fill_nasdaq_filters_to_universe(tmp_world, monkeypatch):
    """finance-calendars returns ALL tickers reporting on a day; the
    fill must filter to only universe-listed tickers (avoids polluting
    earnings_dates.parquet with random small-caps)."""
    def fake_get_earnings_by_date(d):
        return pd.DataFrame(
            {"_placeholder": [None, None]},
            index=pd.Index(["AAPL", "OBSCURE_PENNY"], name="symbol"),
        )

    import finance_calendars.finance_calendars as fc
    monkeypatch.setattr(fc, "get_earnings_by_date", fake_get_earnings_by_date)

    nasdaq_fill.bulk_fill_nasdaq(
        ["AAPL"], blacklist=set(),  # OBSCURE_PENNY not in universe
        days_back=10, days_forward=10, delay=0,
    )
    df = earnings_cache.load_earnings_cache()
    assert df is not None
    assert set(df["ticker"]) == {"AAPL"}


def test_bulk_fill_nasdaq_calendar_exception_counts_as_day_error(tmp_world, monkeypatch):
    def fake_get_earnings_by_date(d):
        raise RuntimeError("upstream is down")

    import finance_calendars.finance_calendars as fc
    monkeypatch.setattr(fc, "get_earnings_by_date", fake_get_earnings_by_date)

    filled, errors = nasdaq_fill.bulk_fill_nasdaq(
        ["AAPL"], blacklist=set(), days_back=3, days_forward=2, delay=0,
    )
    assert filled == 0
    assert errors >= 1


def test_bulk_fill_nasdaq_stop_flag_halts_loop(tmp_world, monkeypatch):
    seen_days: list[date] = []

    def fake_get_earnings_by_date(d):
        seen_days.append(d)
        return pd.DataFrame()

    import finance_calendars.finance_calendars as fc
    monkeypatch.setattr(fc, "get_earnings_by_date", fake_get_earnings_by_date)

    stop = [False]
    call_count = {"n": 0}

    def fake_progress(d, t):
        call_count["n"] = d
        if d >= 2:
            stop[0] = True

    nasdaq_fill.bulk_fill_nasdaq(
        ["AAPL"], blacklist=set(),
        days_back=30, days_forward=30, delay=0,
        progress_cb=fake_progress, stop_flag=stop,
    )
    # Stop was requested at day 2, so we shouldn't see all ~44 weekdays.
    assert len(seen_days) < 30


def test_bulk_fill_nasdaq_triggers_reconcile_after_write(tmp_world, monkeypatch):
    """Audit fix: a Nasdaq fill must reconcile so the freshly-written
    nasdaq-source rows can be consolidated against existing Zacks /
    Finnhub history. Without this, a subsequent Yahoo fill would
    overwrite the nasdaq rows by-ticker before reconcile ran."""
    today = date.today()

    def fake_get_earnings_by_date(d):
        return pd.DataFrame(
            {"_placeholder": [None]},
            index=pd.Index(["AAPL"], name="symbol"),
        )

    import finance_calendars.finance_calendars as fc
    monkeypatch.setattr(fc, "get_earnings_by_date", fake_get_earnings_by_date)

    from trade_scanner_fh import earnings_reconcile
    reconcile_calls: list = []

    def spy_reconcile(*args, **kwargs):
        reconcile_calls.append((args, kwargs))
        return (0, 0, 0)

    monkeypatch.setattr(
        earnings_reconcile, "reconcile_earnings_dates", spy_reconcile,
    )

    nasdaq_fill.bulk_fill_nasdaq(
        ["AAPL"], blacklist=set(), days_back=10, days_forward=10, delay=0,
    )
    assert len(reconcile_calls) == 1
    _, kwargs = reconcile_calls[0]
    assert "AAPL" in kwargs.get("affected_tickers", [])
