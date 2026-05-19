"""Tests for data_engine.py — Phase 3 I1 (download_many, _RateLimiter) and
I11 (LRU cache on load_ohlcv)."""
import threading
import time
from unittest.mock import patch

import pandas as pd

from trade_scanner_fh import data_engine
from trade_scanner_fh.data_engine import (
    ScrapeResult,
    _RateLimiter,
    clear_ohlcv_cache,
    download_many,
    load_ohlcv,
)


# ----------------------------------------------------------------------
# I11 — LRU cache on load_ohlcv
# ----------------------------------------------------------------------

def test_load_ohlcv_caches_across_repeated_calls(tmp_path, monkeypatch):
    """Second load_ohlcv for the same symbol hits the LRU cache — verified
    by object identity (same DataFrame object returned)."""
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    clear_ohlcv_cache()

    df = pd.DataFrame(
        {"Close": [100.0]},
        index=pd.to_datetime(["2026-04-01"]),
    )
    df.to_parquet(tmp_path / "AAPL.parquet")

    r1 = load_ohlcv("AAPL")
    r2 = load_ohlcv("AAPL")
    assert r1 is not None and r2 is not None
    # lru_cache returns the same object for the same key
    assert r2 is r1


def test_load_ohlcv_mtime_bump_invalidates_cache(tmp_path, monkeypatch):
    """When a parquet is rewritten with a newer mtime, the cache must
    return fresh data — not the stale cached DataFrame."""
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    clear_ohlcv_cache()

    df1 = pd.DataFrame(
        {"Close": [100.0]},
        index=pd.to_datetime(["2026-04-01"]),
    )
    df1.to_parquet(tmp_path / "AAPL.parquet")
    r1 = load_ohlcv("AAPL")
    assert r1["Close"].iloc[0] == 100.0

    # Wait long enough that mtime_ns bumps on Windows (filesystem resolution)
    time.sleep(0.05)

    df2 = pd.DataFrame(
        {"Close": [200.0]},
        index=pd.to_datetime(["2026-04-01"]),
    )
    df2.to_parquet(tmp_path / "AAPL.parquet")

    r2 = load_ohlcv("AAPL")
    # Fresh data via cache miss on new mtime key
    assert r2["Close"].iloc[0] == 200.0
    assert r2 is not r1


def test_load_ohlcv_missing_file_returns_none(tmp_path, monkeypatch):
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    clear_ohlcv_cache()
    assert load_ohlcv("NOTHERE") is None


# ----------------------------------------------------------------------
# I1 — _RateLimiter
# ----------------------------------------------------------------------

def test_rate_limiter_enforces_minimum_interval():
    """3 sequential acquire() calls at 0.1s min interval must take ≥ 0.2s
    (first is free, second and third each wait ~0.1s)."""
    limiter = _RateLimiter(min_interval_sec=0.1)
    t0 = time.monotonic()
    limiter.acquire()
    limiter.acquire()
    limiter.acquire()
    elapsed = time.monotonic() - t0
    assert elapsed >= 0.18, f"expected >= 0.18s, got {elapsed:.3f}s"


def test_rate_limiter_is_thread_safe():
    """With 4 parallel threads each acquiring 5 times at 0.05s interval,
    total elapsed across all 20 acquires must be ≥ ~0.95s (19 gaps)."""
    limiter = _RateLimiter(min_interval_sec=0.05)
    threads = []
    t0 = time.monotonic()

    def worker():
        for _ in range(5):
            limiter.acquire()

    for _ in range(4):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    elapsed = time.monotonic() - t0
    # 20 acquires, 19 gaps × 0.05s = 0.95s minimum
    assert elapsed >= 0.9, f"expected >= 0.9s, got {elapsed:.3f}s"


# ----------------------------------------------------------------------
# I1 — download_many parallel primitive
# ----------------------------------------------------------------------

def test_download_many_calls_download_one_for_each_symbol():
    """download_many invokes download_one once per input symbol and
    returns one ScrapeResult per symbol."""
    def fake_download(sym):
        return ScrapeResult(symbol=sym, status="ok", rows_received=1)

    with patch("trade_scanner_fh.data_engine.download_one", side_effect=fake_download):
        syms = [f"T{i}" for i in range(10)]
        results = download_many(syms, max_workers=3, min_interval_sec=0.01)

    assert len(results) == 10
    assert {r.symbol for r in results} == set(syms)
    assert all(r.status == "ok" for r in results)


def test_download_many_parallel_faster_than_serial():
    """With worker parallelism, 8 tickers each taking 0.15s net time plus a
    0.02s rate-limit interval should finish faster than 8×0.17 = 1.36s serial.
    (Parallel = roughly max(N * interval, longest-chain).)"""
    def slow_download(sym):
        time.sleep(0.15)
        return ScrapeResult(symbol=sym, status="ok")

    with patch("trade_scanner_fh.data_engine.download_one", side_effect=slow_download):
        syms = [f"T{i}" for i in range(8)]
        t0 = time.monotonic()
        results = download_many(syms, max_workers=4, min_interval_sec=0.02)
        elapsed = time.monotonic() - t0

    assert len(results) == 8
    # Serial would be 8 × 0.17 ≈ 1.36s. With 4 workers we should comfortably
    # finish under 1.0s.
    assert elapsed < 1.0, f"expected < 1.0s, got {elapsed:.3f}s"


def test_download_many_progress_callback_invoked_per_result():
    def fake_download(sym):
        return ScrapeResult(symbol=sym, status="ok")

    received = []
    with patch("trade_scanner_fh.data_engine.download_one", side_effect=fake_download):
        download_many(
            ["A", "B", "C"],
            max_workers=2,
            min_interval_sec=0.01,
            progress_cb=received.append,
        )
    assert len(received) == 3
    assert {r.symbol for r in received} == {"A", "B", "C"}


# ----------------------------------------------------------------------
# R18 — Parquet schema version stamp
# ----------------------------------------------------------------------

def test_schema_stamp_round_trip(tmp_path, monkeypatch):
    """stamp_schema_version writes the current version; read_schema_version
    returns it; check_schema_version stamps if missing without raising."""
    parquet_dir = tmp_path / "ohlcv"
    parquet_dir.mkdir()
    schema_file = parquet_dir / "_schema_version.txt"
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", parquet_dir)
    monkeypatch.setattr(data_engine.config, "PARQUET_SCHEMA_FILE", schema_file)

    # No stamp yet
    assert data_engine.read_schema_version() is None

    # Stamp → round trip
    data_engine.stamp_schema_version()
    assert schema_file.exists()
    assert data_engine.read_schema_version() == data_engine.config.PARQUET_SCHEMA_VERSION


def test_check_schema_version_stamps_if_missing(tmp_path, monkeypatch):
    """check_schema_version must leave a stamp file after first call."""
    parquet_dir = tmp_path / "ohlcv"
    parquet_dir.mkdir()
    schema_file = parquet_dir / "_schema_version.txt"
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", parquet_dir)
    monkeypatch.setattr(data_engine.config, "PARQUET_SCHEMA_FILE", schema_file)

    assert not schema_file.exists()
    data_engine.check_schema_version()
    assert schema_file.exists()
    assert data_engine.read_schema_version() == data_engine.config.PARQUET_SCHEMA_VERSION


# ----------------------------------------------------------------------
# R9 — rebuild_ticker deletes + re-downloads
# ----------------------------------------------------------------------

def test_rebuild_ticker_deletes_existing_parquet(tmp_path, monkeypatch):
    """rebuild_ticker unlinks the cached parquet before invoking
    download_one. We verify the delete path via patching download_one to
    record its side effects."""
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    clear_ohlcv_cache()

    # Seed an existing parquet
    pq = tmp_path / "OLD.parquet"
    pd.DataFrame({"Close": [1.0]}, index=pd.to_datetime(["2026-04-01"])).to_parquet(pq)
    assert pq.exists()

    called = {"count": 0, "sym": None}

    def fake_download(sym):
        called["count"] += 1
        called["sym"] = sym
        return ScrapeResult(symbol=sym, status="ok", rows_received=42)

    with patch("trade_scanner_fh.data_engine.download_one", side_effect=fake_download):
        res = data_engine.rebuild_ticker("OLD")

    assert not pq.exists(), "old parquet should have been deleted"
    assert called["count"] == 1
    assert called["sym"] == "OLD"
    assert res.status == "ok"
    assert res.rows_received == 42


# ----------------------------------------------------------------------
# download_one — incremental append / split-detection paths
# ----------------------------------------------------------------------

def _seed_existing_parquet(tmp_path, symbol="AAPL", n=30):
    """Write a 30-bar parquet to simulate an existing cache."""
    idx = pd.date_range("2024-01-01", periods=n, freq="B")
    df = pd.DataFrame({
        "Open":   [100.0] * n, "High": [101.0] * n, "Low": [99.0] * n,
        "Close":  [100.0] * n, "Volume": [1_000_000] * n,
        "Stock Splits": [0.0] * n,
    }, index=idx)
    df.index.name = "Date"
    df.to_parquet(tmp_path / f"{symbol}.parquet")
    return df


def test_download_one_incremental_append_no_split(tmp_path, monkeypatch):
    """Without a split in the incremental delta, download_one appends the
    new rows to the existing cache and does NOT trigger a full re-download."""
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    clear_ohlcv_cache()
    _seed_existing_parquet(tmp_path, "AAPL", n=30)

    call_log = []

    def fake_download(symbol, start, end):
        call_log.append({"start": start, "end": end})
        idx = pd.date_range("2024-02-15", periods=3, freq="B")
        return pd.DataFrame({
            "Open":   [100.0] * 3, "High": [101.0] * 3, "Low": [99.0] * 3,
            "Close":  [100.0] * 3, "Volume": [1_000_000] * 3,
            "Stock Splits": [0.0] * 3,  # no splits
        }, index=idx)

    monkeypatch.setattr(data_engine, "_download_raw", fake_download)
    res = data_engine.download_one("AAPL")

    assert len(call_log) == 1, "one call only when no split"
    assert res.was_incremental is True
    assert res.status == "ok"
    final = pd.read_parquet(tmp_path / "AAPL.parquet")
    assert len(final) == 33  # 30 existing + 3 new


def test_download_one_split_triggers_full_redownload(tmp_path, monkeypatch):
    """When a stock split appears in the incremental delta, download_one
    must re-download the full history (so yfinance re-adjusts old prices)."""
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    clear_ohlcv_cache()
    _seed_existing_parquet(tmp_path, "AAPL", n=30)

    call_log = []

    def fake_download(symbol, start, end):
        call_log.append({"start": start, "end": end})
        if len(call_log) == 1:
            # Incremental delta — contains a split on bar 1
            idx = pd.date_range("2024-02-15", periods=3, freq="B")
            return pd.DataFrame({
                "Open":   [100, 50, 50], "High": [101, 51, 51],
                "Low":    [99, 49, 49],  "Close": [100, 50, 50],
                "Volume": [1_000_000] * 3,
                "Stock Splits": [0.0, 2.0, 0.0],  # 2:1 split mid-window
            }, index=idx)
        # Second call = full re-download with post-adjusted prices
        idx = pd.date_range("2023-01-01", periods=280, freq="B")
        return pd.DataFrame({
            "Open":   [50.0] * 280, "High": [51.0] * 280, "Low": [49.0] * 280,
            "Close":  [50.0] * 280, "Volume": [1_000_000] * 280,
            "Stock Splits": [0.0] * 280,
        }, index=idx)

    monkeypatch.setattr(data_engine, "_download_raw", fake_download)
    res = data_engine.download_one("AAPL")

    assert len(call_log) == 2, "split must trigger a second full-history call"
    assert res.status == "ok"
    # was_incremental is reset to False after the split re-download
    assert res.was_incremental is False
    final = pd.read_parquet(tmp_path / "AAPL.parquet")
    assert len(final) == 280, "final cache should reflect the full re-download"


def test_download_one_first_time_no_data_returns_no_data(tmp_path, monkeypatch):
    """First-time download that yields an empty DataFrame should produce
    status='no_data', not 'error'."""
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    clear_ohlcv_cache()

    def empty_download(symbol, start, end):
        return pd.DataFrame()

    monkeypatch.setattr(data_engine, "_download_raw", empty_download)
    res = data_engine.download_one("NEVERTRADED")
    assert res.status == "no_data"
    assert res.was_incremental is False
    assert not (tmp_path / "NEVERTRADED.parquet").exists()


def test_download_one_exception_becomes_error_status(tmp_path, monkeypatch):
    """_download_raw raising propagates to ScrapeResult.status='error' with
    the exception message captured in error_msg — never re-raised."""
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    clear_ohlcv_cache()

    def raising_download(symbol, start, end):
        raise RuntimeError("simulated network failure")

    monkeypatch.setattr(data_engine, "_download_raw", raising_download)
    res = data_engine.download_one("AAPL")
    assert res.status == "error"
    assert "simulated network failure" in res.error_msg


def test_download_many_respects_stop_flag():
    """When stop_flag() becomes True mid-run, remaining tickers must
    short-circuit with status='stopped' (not 'ok' or 'error')."""
    stop_event = threading.Event()

    def slow_download(sym):
        time.sleep(0.05)
        return ScrapeResult(symbol=sym, status="ok")

    # Set stop after a short delay
    threading.Timer(0.08, stop_event.set).start()

    with patch("trade_scanner_fh.data_engine.download_one", side_effect=slow_download):
        results = download_many(
            [f"T{i}" for i in range(30)],
            max_workers=2,
            min_interval_sec=0.01,
            stop_flag=stop_event.is_set,
        )

    statuses = [r.status for r in results]
    # Some were stopped and/or cancellation pruned the pool — either way
    # we must not have completed all 30 as "ok".
    assert statuses.count("ok") < 30
