"""Incremental refetch overlap — 2026-08-13.

`download_one` used to start its incremental window at `last_date + 1 day`, so
the most recently cached bar was never requested again. The provider's newest
daily bar is still provisional for hours after the close, which made whatever a
post-close refill captured PERMANENT.

Measured on the live store (60 random tickers, cached 2026-08-12 bar vs a fresh
pull of the same date): Volume differed on 60/60 — 52 understated, median 1.2%,
p90 22.6%, worst 99% (IP 2,894,764 cached vs 4,992,100 actual). High differed on
30/60, Low on 31/60. Store-wide, 854 tickers were left with Open outside
[Low, High]. RVOL, ADR% and ATR all read the corrupted session.

The window now reaches back `config.OHLCV_REFETCH_OVERLAP_DAYS`. These tests
cover the widened window, the replacement it exists to perform, the INT-11
guard that still has to hold inside it, and the re-anchor filter that keeps the
overlap from re-triggering a full history pull on every run.
"""
from __future__ import annotations

import pandas as pd
import pytest

from trade_scanner_fh import config, data_engine


def _frame(dates, *, splits=0.0, dividends=0.0, volume=1_000, close=10.5,
           high=11.0):
    n = len(dates)
    return pd.DataFrame(
        {"Open": [10.0] * n, "High": [high] * n, "Low": [9.0] * n,
         "Close": [close] * n, "Volume": [volume] * n,
         "Stock Splits": [splits] * n, "Dividends": [dividends] * n},
        index=pd.DatetimeIndex(pd.to_datetime(dates), name="Date"),
    )


@pytest.fixture
def cached(tmp_path, monkeypatch):
    """A ticker already on disk, so download_one takes the incremental path.
    Last cached bar is 2026-08-05 and is deliberately PROVISIONAL: understated
    volume and high, the shape the live store was found in."""
    monkeypatch.setattr(config, "PARQUET_DIR", tmp_path)
    data_engine.clear_ohlcv_cache()
    hist = _frame(["2026-08-03", "2026-08-04"])
    provisional = _frame(["2026-08-05"], volume=6_284_876, high=74.25,
                         close=73.54)
    existing = pd.concat([hist, provisional])
    existing[data_engine._STORED_COLUMNS].to_parquet(tmp_path / "T.parquet")
    return tmp_path


def _capture(monkeypatch, frames):
    """Stub `_download_raw`, recording each `start` and returning the next
    frame SLICED to the window actually requested.

    The slicing matters: without it the stub hands back bars the caller never
    asked for, and every assertion here would pass just as happily against the
    old `last_date + 1 day` window the fix replaced.
    """
    calls = []

    def fake(symbol, start, end):
        calls.append(start)
        frame = frames[min(len(calls) - 1, len(frames) - 1)]
        return frame.loc[frame.index >= pd.Timestamp(start)]

    monkeypatch.setattr(data_engine, "_download_raw", fake)
    return calls


# ----------------------------------------------------------------------
# the widened window
# ----------------------------------------------------------------------

def test_incremental_start_reaches_back_before_the_last_cached_bar(
    cached, monkeypatch,
):
    """The fix itself: the window must re-request the last cached date rather
    than beginning the day after it."""
    monkeypatch.setattr(config, "OHLCV_REFETCH_OVERLAP_DAYS", 5)
    calls = _capture(monkeypatch, [_frame(["2026-08-06"])])

    data_engine.download_one("T")

    # last cached bar is 2026-08-05; 5 calendar days back is 2026-07-31
    assert calls[0] == "2026-07-31"
    assert calls[0] < "2026-08-05", "last cached bar must be re-requested"


def test_overlap_days_is_configurable(cached, monkeypatch):
    monkeypatch.setattr(config, "OHLCV_REFETCH_OVERLAP_DAYS", 1)
    calls = _capture(monkeypatch, [_frame(["2026-08-06"])])

    data_engine.download_one("T")

    assert calls[0] == "2026-08-04"


# ----------------------------------------------------------------------
# what the overlap is FOR
# ----------------------------------------------------------------------

def test_provisional_bar_is_replaced_by_the_finalized_one(cached, monkeypatch):
    """The bug this fix exists to kill: a re-sent bar carrying the finalized
    volume/high must overwrite the provisional copy already on disk."""
    monkeypatch.setattr(config, "OHLCV_REFETCH_OVERLAP_DAYS", 5)
    # provider now returns the settled 08-05 bar plus a new 08-06
    finalized = pd.concat([
        _frame(["2026-08-05"], volume=7_170_100, high=74.39, close=73.54),
        _frame(["2026-08-06"]),
    ])
    _capture(monkeypatch, [finalized])

    data_engine.download_one("T")

    saved = pd.read_parquet(cached / "T.parquet")
    bar = saved.loc[pd.Timestamp("2026-08-05")]
    assert bar["Volume"] == 7_170_100, "provisional volume was not corrected"
    assert bar["High"] == pytest.approx(74.39)


def test_overlap_does_not_truncate_earlier_history(cached, monkeypatch):
    """Re-fetching a tail must merge with the cache, never replace it."""
    monkeypatch.setattr(config, "OHLCV_REFETCH_OVERLAP_DAYS", 5)
    _capture(monkeypatch, [_frame(["2026-08-05", "2026-08-06"])])

    data_engine.download_one("T")

    saved = pd.read_parquet(cached / "T.parquet")
    assert len(saved) == 4
    assert str(saved.index.min().date()) == "2026-08-03"
    assert not saved.index.has_duplicates


def test_conflicting_resend_inside_the_overlap_is_still_rejected(
    cached, monkeypatch,
):
    """INT-11 still has to hold: the wider window must not become a wider hole
    for a bad response to overwrite good cached bars through."""
    monkeypatch.setattr(config, "OHLCV_REFETCH_OVERLAP_DAYS", 5)
    garbage = _frame(["2026-08-05"], close=0.01, volume=1)
    _capture(monkeypatch, [garbage])

    data_engine.download_one("T")

    saved = pd.read_parquet(cached / "T.parquet")
    assert saved.loc[pd.Timestamp("2026-08-05"), "Close"] == pytest.approx(73.54)


# ----------------------------------------------------------------------
# the re-anchor filter the overlap made necessary
# ----------------------------------------------------------------------

def test_dividend_inside_the_overlap_does_not_retrigger_the_reanchor(
    cached, monkeypatch,
):
    """A dividend on an ALREADY-CACHED date has been accounted for by a
    previous run. Without the `_bars_after` filter the overlap would re-detect
    it and pull five years of history again on every run until it aged out."""
    monkeypatch.setattr(config, "OHLCV_REFETCH_OVERLAP_DAYS", 5)
    # ex-date 08-04 sits inside the overlap and at/behind the last cached bar
    overlapping = _frame(["2026-08-04", "2026-08-05"], dividends=0.25)
    calls = _capture(monkeypatch, [overlapping])

    data_engine.download_one("T")

    assert len(calls) == 1, f"re-anchor fired on a stale dividend: {calls}"


def test_dividend_after_the_last_cached_bar_still_triggers_the_reanchor(
    cached, monkeypatch,
):
    """The INT-3 behaviour must survive the filter: a genuinely new ex-date
    still re-pulls the full history."""
    monkeypatch.setattr(config, "OHLCV_REFETCH_OVERLAP_DAYS", 5)
    incremental = _frame(["2026-08-06"], dividends=0.25)
    full = _frame(pd.date_range("2021-08-06", "2026-08-06", freq="B"))
    calls = _capture(monkeypatch, [incremental, full])

    data_engine.download_one("T")

    assert len(calls) == 2, "a new dividend must still re-anchor"


def test_split_after_the_last_cached_bar_still_triggers_the_reanchor(
    cached, monkeypatch,
):
    monkeypatch.setattr(config, "OHLCV_REFETCH_OVERLAP_DAYS", 5)
    incremental = _frame(["2026-08-06"], splits=2.0)
    full = _frame(pd.date_range("2021-08-06", "2026-08-06", freq="B"))
    calls = _capture(monkeypatch, [incremental, full])

    data_engine.download_one("T")

    assert len(calls) == 2


def test_split_inside_the_overlap_does_not_retrigger_the_reanchor(
    cached, monkeypatch,
):
    monkeypatch.setattr(config, "OHLCV_REFETCH_OVERLAP_DAYS", 5)
    overlapping = _frame(["2026-08-04", "2026-08-05"], splits=2.0)
    calls = _capture(monkeypatch, [overlapping])

    data_engine.download_one("T")

    assert len(calls) == 1


# ----------------------------------------------------------------------
# _bars_after — the tz normalisation the filter depends on
# ----------------------------------------------------------------------

def test_bars_after_handles_a_naive_cutoff_against_an_aware_index():
    """`_last_cached_date` returns a NAIVE timestamp from its parquet-statistics
    fast path but an AWARE one from the full-read fallback, while a downloaded
    index is always aware. Comparing them raw raises TypeError."""
    idx = pd.DatetimeIndex(
        pd.to_datetime(["2026-08-04", "2026-08-05", "2026-08-06"])
    ).tz_localize("America/New_York")

    mask = data_engine._bars_after(idx, pd.Timestamp("2026-08-05"))

    assert list(mask) == [False, False, True]


def test_bars_after_handles_an_aware_cutoff_against_an_aware_index():
    idx = pd.DatetimeIndex(
        pd.to_datetime(["2026-08-04", "2026-08-05", "2026-08-06"])
    ).tz_localize("America/New_York")
    cutoff = pd.Timestamp("2026-08-05", tz="America/New_York")

    assert list(data_engine._bars_after(idx, cutoff)) == [False, False, True]


def test_bars_after_handles_naive_on_both_sides():
    idx = pd.DatetimeIndex(pd.to_datetime(["2026-08-04", "2026-08-06"]))

    assert list(
        data_engine._bars_after(idx, pd.Timestamp("2026-08-05"))
    ) == [False, True]
