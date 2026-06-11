"""Fix A: the quick-range buttons + launch default anchor the scan's End
to the latest available trading day (so '1D' = the last trading day, not a
stale End field). Uses the MainWindow.__new__ pattern (no QApplication).
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pandas as pd

from trade_scanner_fh import config
from trade_scanner_fh.gui import main_window as mw
from trade_scanner_fh.gui.main_window import MainWindow


def _bare():
    win = MainWindow.__new__(MainWindow)
    win._latest_data_date_cache = None
    win._symbols = []
    return win


def test_latest_data_date_uses_max_ohlcv_bar(monkeypatch):
    """Max bar date across the sampled cached tickers."""
    win = _bare()
    win._symbols = ["AAA", "BBB", "CCC"]
    dates = {"AAA": "2026-06-04", "BBB": "2026-06-05", "CCC": "2026-06-03"}

    def fake_load(sym):
        d = dates[sym]
        return pd.DataFrame({"Close": [1.0, 2.0]}, index=pd.to_datetime([d, d]))

    monkeypatch.setattr(mw, "load_ohlcv", fake_load)
    assert win._latest_data_date() == date(2026, 6, 5)


def test_latest_data_date_cached(monkeypatch):
    """Second call uses the cache (load_ohlcv not re-invoked)."""
    win = _bare()
    win._symbols = ["AAA"]
    calls = []

    def fake_load(sym):
        calls.append(sym)
        return pd.DataFrame({"Close": [1.0]}, index=pd.to_datetime(["2026-06-05"]))

    monkeypatch.setattr(mw, "load_ohlcv", fake_load)
    assert win._latest_data_date() == date(2026, 6, 5)
    assert win._latest_data_date() == date(2026, 6, 5)
    assert len(calls) == 1


def test_latest_data_date_fallback_no_symbols(monkeypatch):
    """No cached OHLCV → fall back to the market calendar."""
    win = _bare()
    win._symbols = []
    monkeypatch.setattr(config, "last_market_close",
                        lambda: pd.Timestamp("2026-06-05 16:00"))
    assert win._latest_data_date() == date(2026, 6, 5)


def test_set_quick_range_anchors_end_to_latest():
    """'1D' sets End = latest data date and Start = End - 1 (not 'keep End
    as-is' — that was the stale-End trap)."""
    win = _bare()
    win._latest_data_date = lambda: date(2026, 6, 5)
    win.date_end = MagicMock()
    win.date_start = MagicMock()
    win._set_quick_range(1)
    end_qd = win.date_end.setDate.call_args[0][0]
    start_qd = win.date_start.setDate.call_args[0][0]
    assert (end_qd.year(), end_qd.month(), end_qd.day()) == (2026, 6, 5)
    assert (start_qd.year(), start_qd.month(), start_qd.day()) == (2026, 6, 4)


def test_set_quick_range_week():
    win = _bare()
    win._latest_data_date = lambda: date(2026, 6, 5)
    win.date_end = MagicMock()
    win.date_start = MagicMock()
    win._set_quick_range(7)
    start_qd = win.date_start.setDate.call_args[0][0]
    assert (start_qd.year(), start_qd.month(), start_qd.day()) == (2026, 5, 29)


# ── Fix B: the 1D (and any short) lookback counts back a *trading* session,
# not a raw calendar day. A calendar-day start lands on a weekend/holiday for
# a Monday/post-holiday End (e.g. End=Mon 2026-06-08 → Sun 06-07), leaving a
# single bar in the [start, end] slice — which scanner's `len(window) < 2`
# guard rejects for EVERY ticker, producing an empty 1D scan ("No tickers
# produced computable results"). _set_quick_range now floors the start to the
# most recent trading day via config.most_recent_trading_day(). ──────────────

def test_set_quick_range_1d_monday_uses_prior_trading_day():
    """End=Monday 2026-06-08 → 1D Start = prior Friday 2026-06-05, NOT the
    Sunday calendar day. This is the exact bug that emptied the 1D scan."""
    win = _bare()
    win._latest_data_date = lambda: date(2026, 6, 8)  # Monday
    win.date_end = MagicMock()
    win.date_start = MagicMock()
    win._set_quick_range(1)
    end_qd = win.date_end.setDate.call_args[0][0]
    start_qd = win.date_start.setDate.call_args[0][0]
    assert (end_qd.year(), end_qd.month(), end_qd.day()) == (2026, 6, 8)
    # Friday 2026-06-05, so [start, end] spans 2 trading bars (Fri + Mon)
    assert (start_qd.year(), start_qd.month(), start_qd.day()) == (2026, 6, 5)


def test_set_quick_range_1d_post_holiday_uses_prior_trading_day():
    """End=Tue 2026-05-26 (day after Memorial Day Mon 05-25) → 1D Start =
    Fri 2026-05-22, walking back past both the holiday and the weekend."""
    win = _bare()
    win._latest_data_date = lambda: date(2026, 5, 26)  # Tue after Memorial Day
    win.date_end = MagicMock()
    win.date_start = MagicMock()
    win._set_quick_range(1)
    start_qd = win.date_start.setDate.call_args[0][0]
    assert (start_qd.year(), start_qd.month(), start_qd.day()) == (2026, 5, 22)


def test_set_quick_range_1d_ordinary_weekday_unchanged():
    """End=Tue 2026-06-09 → 1D Start = Mon 2026-06-08 (the floor is a no-op
    when the prior calendar day is already a trading day)."""
    win = _bare()
    win._latest_data_date = lambda: date(2026, 6, 9)  # Tuesday
    win.date_end = MagicMock()
    win.date_start = MagicMock()
    win._set_quick_range(1)
    start_qd = win.date_start.setDate.call_args[0][0]
    assert (start_qd.year(), start_qd.month(), start_qd.day()) == (2026, 6, 8)


# ── _window_start is the shared start-date computation used by BOTH the
# standalone quick-range buttons (_set_quick_range) AND the multi-timeframe
# scan builder in _run_scan — the path that actually emptied the 1D scan. ────

def test_window_start_1d_monday_spans_two_trading_bars():
    """The exact regression: End=Mon 2026-06-08, 1D → Fri 2026-06-05, so the
    [start, end] slice holds 2 trading bars (Fri + Mon), not 1."""
    win = _bare()
    assert win._window_start(date(2026, 6, 8), 1) == date(2026, 6, 5)


def test_window_start_1d_post_holiday():
    """End=Tue 2026-05-26 (after Memorial Day Mon 05-25), 1D → Fri 05-22."""
    win = _bare()
    assert win._window_start(date(2026, 5, 26), 1) == date(2026, 5, 22)


def test_window_start_week_unchanged_on_monday_end():
    """1W from a Monday End already lands on a trading day (Mon 06-01) — the
    floor is a no-op, so longer timeframes keep their prior behavior."""
    win = _bare()
    assert win._window_start(date(2026, 6, 8), 7) == date(2026, 6, 1)


def test_window_start_month_floors_weekend_start_to_friday():
    """1M (30 calendar days) from Mon 06-08 lands on Sat 05-09 → floored back
    to Fri 05-08 (the start anchors to a real trading session)."""
    win = _bare()
    assert win._window_start(date(2026, 6, 8), 30) == date(2026, 5, 8)
