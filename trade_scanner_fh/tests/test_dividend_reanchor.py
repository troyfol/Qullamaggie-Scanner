"""Dividend re-anchor — audit 2026-08-12 (INT-3).

`_download_raw` uses `auto_adjust=True`, which back-adjusts prior bars for
splits **and dividends**. Only a split triggered a full re-download, and the
`Dividends` column was dropped before anything could inspect it — so every
ex-dividend date left a PERMANENT price discontinuity *inside* a single
ticker's cached series: bars written before that update kept the old
adjustment basis while newly-appended bars used the new one.

Measured on the live cache at audit time and re-measured after the code fix
(the existing files still carry it until each payer's next dividend):
CSCO +0.374%, JNJ +0.575%, WMT +0.191%, BAC +0.520%, KO +0.646%, VZ +1.705%
on the oldest bar. Any SMA / ADR% / ATR / RS window spanning a seam mixes two
price bases, and the error accumulates with dividend yield — a systematic bias
against high-yield names, not noise.

The correction is self-healing: the next dividend per payer triggers one full
re-anchor. That makes these tests the only thing standing between the fix and
a silent no-op, so they cover the trigger, the non-trigger, and the on-disk
schema.
"""
from __future__ import annotations

import pandas as pd
import pytest

from trade_scanner_fh import config, data_engine


def _frame(dates, *, splits=0.0, dividends=0.0):
    n = len(dates)
    return pd.DataFrame(
        {"Open": [10.0] * n, "High": [11.0] * n, "Low": [9.0] * n,
         "Close": [10.5] * n, "Volume": [1_000] * n,
         "Stock Splits": [splits] * n, "Dividends": [dividends] * n},
        index=pd.DatetimeIndex(pd.to_datetime(dates), name="Date"),
    )


@pytest.fixture
def cached(tmp_path, monkeypatch):
    """A ticker already in the cache, so download_one takes the INCREMENTAL
    path (last_date is not None) — the path where the seam was created."""
    monkeypatch.setattr(config, "PARQUET_DIR", tmp_path)
    data_engine.clear_ohlcv_cache()
    existing = _frame(["2026-08-03", "2026-08-04", "2026-08-05"])
    existing[data_engine._STORED_COLUMNS].to_parquet(tmp_path / "T.parquet")
    return tmp_path


def _capture_calls(monkeypatch, frames):
    """Stub _download_raw, returning `frames` in order and recording starts."""
    calls = []

    def fake(symbol, start, end):
        calls.append(start)
        return frames[min(len(calls) - 1, len(frames) - 1)]

    monkeypatch.setattr(data_engine, "_download_raw", fake)
    return calls


def test_a_dividend_triggers_a_full_reanchor(cached, monkeypatch):
    """The regression itself: a dividend in the incremental window must
    re-download the whole history so every bar shares one adjustment basis."""
    incremental = _frame(["2026-08-06"], dividends=0.25)
    full = _frame(pd.date_range("2021-08-06", "2026-08-06", freq="B"))
    calls = _capture_calls(monkeypatch, [incremental, full])

    result = data_engine.download_one("T")

    assert len(calls) == 2, "no re-anchor: the dividend was not detected"
    assert calls[1] < calls[0], "the second pull is not a full-history pull"
    assert result.was_incremental is False


def test_a_split_still_triggers_a_reanchor(cached, monkeypatch):
    """Pre-existing behaviour that must not regress."""
    calls = _capture_calls(monkeypatch, [
        _frame(["2026-08-06"], splits=2.0),
        _frame(pd.date_range("2021-08-06", "2026-08-06", freq="B")),
    ])
    data_engine.download_one("T")
    assert len(calls) == 2


def test_an_ordinary_update_does_not_reanchor(cached, monkeypatch):
    """No corporate action → one incremental pull, as before. Re-anchoring on
    every update would be a large unnecessary bandwidth increase."""
    calls = _capture_calls(monkeypatch, [_frame(["2026-08-06"])])
    result = data_engine.download_one("T")
    assert len(calls) == 1
    assert result.was_incremental is True


def test_a_zero_dividend_column_is_not_a_dividend(cached, monkeypatch):
    """yfinance returns a Dividends column of zeros on most days; only a
    NON-ZERO value is an ex-dividend date."""
    calls = _capture_calls(monkeypatch, [
        _frame(["2026-08-06", "2026-08-07"], dividends=0.0)])
    data_engine.download_one("T")
    assert len(calls) == 1


def test_dividends_are_never_persisted(cached, monkeypatch):
    """The on-disk schema must stay byte-compatible with the 14,892 files
    already in the cache — Dividends is fetched for detection only."""
    _capture_calls(monkeypatch, [
        _frame(["2026-08-06"], dividends=0.25),
        _frame(pd.date_range("2026-01-01", "2026-08-06", freq="B")),
    ])
    data_engine.download_one("T")

    written = pd.read_parquet(config.PARQUET_DIR / "T.parquet")
    assert "Dividends" not in written.columns
    assert list(written.columns) == data_engine._STORED_COLUMNS


def test_reanchor_replaces_rather_than_appends(cached, monkeypatch):
    """After a re-anchor the file must hold the FULL re-adjusted history, not
    the old bars with new ones stapled on — that would preserve the seam."""
    full = _frame(pd.date_range("2026-06-01", "2026-08-06", freq="B"))
    full["Close"] = 99.0                       # a distinctly new basis
    _capture_calls(monkeypatch, [
        _frame(["2026-08-06"], dividends=0.25), full])

    data_engine.download_one("T")

    written = pd.read_parquet(config.PARQUET_DIR / "T.parquet")
    # Every bar carries the NEW basis — this is the whole point. A merge would
    # leave the pre-dividend bars at 10.5 and reproduce the seam.
    assert (written["Close"] == 99.0).all(), \
        "stale pre-dividend bars survived the re-anchor — the seam remains"
    # The file IS the re-pulled frame, not the old one with new rows appended.
    assert len(written) == len(full)
    assert written.index.equals(full.index)
