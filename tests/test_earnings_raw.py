"""Tests for the lightweight raw earnings audit layer (Phase 1)."""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta

import pandas as pd
import pytest

from trade_scanner_fh import config, earnings_raw


@pytest.fixture
def tmp_raw(tmp_path, monkeypatch):
    """Redirect RAW_EARNINGS_DIR to a tmp dir and pre-create per-source folders."""
    raw_root = tmp_path / "earnings_raw"
    monkeypatch.setattr(config, "RAW_EARNINGS_DIR", raw_root)
    raw_root.mkdir(parents=True)
    for src in config.RAW_SOURCES:
        (raw_root / src).mkdir()
    return raw_root


# ----------------------------------------------------------------------
# Run id
# ----------------------------------------------------------------------

def test_new_run_id_format():
    rid = earnings_raw.new_run_id()
    # Format: YYYYMMDDHHMMSS_<8 hex chars>
    assert len(rid) == 14 + 1 + 8
    assert rid[14] == "_"
    int(rid[:14])  # numeric prefix parses
    int(rid[15:], 16)  # suffix is hex


def test_new_run_id_unique_across_calls():
    ids = {earnings_raw.new_run_id() for _ in range(50)}
    assert len(ids) == 50  # uuid suffix guarantees uniqueness even within a second


# ----------------------------------------------------------------------
# Append — Zacks
# ----------------------------------------------------------------------

def test_append_zacks_creates_file_and_writes_rows(tmp_raw):
    rid = "20260504000000_aaaaaaaa"
    rows = [
        {"ticker": "AAPL",
         "period_ending": pd.Timestamp("2025-12-01"),
         "report_date": pd.Timestamp("2026-01-29"),
         "report_time": "Close",
         "estimated_eps": 2.0, "reported_eps": 2.1,
         "surprise_eps": 0.1, "surprise_eps_pct": 5.0,
         "estimated_rev": 100.0, "reported_rev": 105.0,
         "surprise_rev": 5.0, "surprise_rev_pct": 5.0},
    ]
    n = earnings_raw.append_zacks_rows(rows, rid)
    assert n == 1
    df = earnings_raw.read_raw(config.RAW_SOURCE_ZACKS, run_id=rid)
    assert len(df) == 1
    assert df.iloc[0]["ticker"] == "AAPL"
    assert df.iloc[0]["run_id"] == rid
    assert pd.notna(df.iloc[0]["fetched_at"])


def test_append_zacks_appends_to_existing_run_file(tmp_raw):
    rid = earnings_raw.new_run_id()
    earnings_raw.append_zacks_rows([{"ticker": "AAPL", "period_ending": pd.Timestamp("2025-12-01")}], rid)
    earnings_raw.append_zacks_rows([{"ticker": "MSFT", "period_ending": pd.Timestamp("2025-12-01")}], rid)
    df = earnings_raw.read_raw(config.RAW_SOURCE_ZACKS, run_id=rid)
    assert len(df) == 2
    assert set(df["ticker"]) == {"AAPL", "MSFT"}


def test_append_empty_rows_is_noop(tmp_raw):
    rid = earnings_raw.new_run_id()
    n = earnings_raw.append_zacks_rows([], rid)
    assert n == 0
    # No file should be created.
    df = earnings_raw.read_raw(config.RAW_SOURCE_ZACKS, run_id=rid)
    assert df.empty


def test_explicit_fetched_at_is_honored(tmp_raw):
    rid = earnings_raw.new_run_id()
    when = datetime(2024, 1, 15, 10, 30, 0)
    earnings_raw.append_zacks_rows(
        [{"ticker": "AAPL", "period_ending": pd.Timestamp("2025-12-01")}],
        rid, fetched_at=when,
    )
    df = earnings_raw.read_raw(config.RAW_SOURCE_ZACKS, run_id=rid)
    assert pd.Timestamp(df.iloc[0]["fetched_at"]) == pd.Timestamp(when)


def test_caller_supplied_fetched_at_and_run_id_in_row_dict_preserved(tmp_raw):
    """If a caller pre-stamps a row with its own fetched_at / run_id values,
    `_append_rows` must not overwrite them — it only fills gaps. The file
    on disk is named by the function-arg run_id (groups one fill); the
    row's run_id field is independent metadata about where the row
    originated and survives the round-trip."""
    rid = earnings_raw.new_run_id()
    custom_when = datetime(2020, 6, 1)
    custom_run = "custom_run_xyz"
    rows = [{
        "ticker": "AAPL",
        "period_ending": pd.Timestamp("2025-12-01"),
        "fetched_at": custom_when,
        "run_id": custom_run,
    }]
    earnings_raw.append_zacks_rows(rows, rid)
    # File is named after the function-arg rid, not the row's run_id.
    df = earnings_raw.read_raw(config.RAW_SOURCE_ZACKS, run_id=rid)
    assert len(df) == 1
    # Row metadata preserved — caller's own values survived the write.
    assert pd.Timestamp(df.iloc[0]["fetched_at"]) == pd.Timestamp(custom_when)
    assert df.iloc[0]["run_id"] == custom_run


# ----------------------------------------------------------------------
# Append — schema per source
# ----------------------------------------------------------------------

def test_finnhub_schema_columns(tmp_raw):
    rid = earnings_raw.new_run_id()
    earnings_raw.append_finnhub_rows(
        [{"symbol": "AAPL", "period": "2025-12-01", "year": 2025, "quarter": 4,
          "actual": 2.1, "estimate": 2.0, "surprise": 0.1, "surprise_percent": 5.0,
          "revenue_actual": 100000000000, "revenue_estimate": 95000000000}],
        rid,
    )
    df = earnings_raw.read_raw(config.RAW_SOURCE_FINNHUB, run_id=rid)
    expected = {"symbol", "period", "year", "quarter", "actual", "estimate",
                "surprise", "surprise_percent", "revenue_actual", "revenue_estimate",
                "fetched_at", "run_id"}
    assert expected.issubset(set(df.columns))


def test_nasdaq_schema_columns(tmp_raw):
    rid = earnings_raw.new_run_id()
    earnings_raw.append_nasdaq_rows(
        [{"ticker": "AAPL", "calendar_date": pd.Timestamp("2026-01-29")}],
        rid,
    )
    df = earnings_raw.read_raw(config.RAW_SOURCE_NASDAQ, run_id=rid)
    assert {"ticker", "calendar_date", "fetched_at", "run_id"}.issubset(df.columns)


def test_yahoo_schema_columns(tmp_raw):
    rid = earnings_raw.new_run_id()
    earnings_raw.append_yahoo_rows(
        [{"ticker": "AAPL", "all_dates_returned": "2024-01-29;2024-04-30"}],
        rid,
    )
    df = earnings_raw.read_raw(config.RAW_SOURCE_YAHOO, run_id=rid)
    assert {"ticker", "all_dates_returned", "fetched_at", "run_id"}.issubset(df.columns)
    assert df.iloc[0]["all_dates_returned"] == "2024-01-29;2024-04-30"


def test_unknown_source_raises_value_error(tmp_raw):
    """The path resolver rejects unknown sources rather than silently
    creating typo'd directories."""
    with pytest.raises(ValueError):
        earnings_raw._source_dir("not_a_real_source")


# ----------------------------------------------------------------------
# Read helpers
# ----------------------------------------------------------------------

def test_read_raw_returns_concat_across_runs(tmp_raw):
    rid_a = earnings_raw.new_run_id()
    rid_b = earnings_raw.new_run_id()
    earnings_raw.append_zacks_rows(
        [{"ticker": "AAPL", "period_ending": pd.Timestamp("2025-12-01")}], rid_a)
    earnings_raw.append_zacks_rows(
        [{"ticker": "MSFT", "period_ending": pd.Timestamp("2025-12-01")}], rid_b)
    df = earnings_raw.read_raw(config.RAW_SOURCE_ZACKS)
    assert len(df) == 2
    assert set(df["ticker"]) == {"AAPL", "MSFT"}


def test_read_raw_filter_by_run_id(tmp_raw):
    rid_a = earnings_raw.new_run_id()
    rid_b = earnings_raw.new_run_id()
    earnings_raw.append_zacks_rows([{"ticker": "AAPL"}], rid_a)
    earnings_raw.append_zacks_rows([{"ticker": "MSFT"}], rid_b)
    df = earnings_raw.read_raw(config.RAW_SOURCE_ZACKS, run_id=rid_a)
    assert len(df) == 1
    assert df.iloc[0]["ticker"] == "AAPL"


def test_read_raw_filter_by_since(tmp_raw):
    rid_old = earnings_raw.new_run_id()
    earnings_raw.append_zacks_rows([{"ticker": "OLD"}], rid_old)

    # Backdate the file by 5 days
    old_path = tmp_raw / "zacks" / f"{rid_old}.parquet"
    old_ts = (datetime.now() - timedelta(days=5)).timestamp()
    os.utime(old_path, (old_ts, old_ts))

    rid_new = earnings_raw.new_run_id()
    earnings_raw.append_zacks_rows([{"ticker": "NEW"}], rid_new)

    df = earnings_raw.read_raw(
        config.RAW_SOURCE_ZACKS, since=datetime.now() - timedelta(days=1),
    )
    assert set(df["ticker"]) == {"NEW"}


def test_list_run_ids_orders_newest_first(tmp_raw):
    rid_a = earnings_raw.new_run_id()
    earnings_raw.append_zacks_rows([{"ticker": "A"}], rid_a)
    time.sleep(0.05)
    rid_b = earnings_raw.new_run_id()
    earnings_raw.append_zacks_rows([{"ticker": "B"}], rid_b)
    ids = earnings_raw.list_run_ids(config.RAW_SOURCE_ZACKS)
    assert ids[0] == rid_b
    assert ids[1] == rid_a


def test_read_raw_empty_dir_returns_empty_df(tmp_raw):
    df = earnings_raw.read_raw(config.RAW_SOURCE_ZACKS)
    assert df.empty


# ----------------------------------------------------------------------
# Pruning
# ----------------------------------------------------------------------

def test_prune_drops_old_files(tmp_raw):
    rid_old = earnings_raw.new_run_id()
    earnings_raw.append_zacks_rows([{"ticker": "OLD"}], rid_old)
    rid_new = earnings_raw.new_run_id()
    earnings_raw.append_zacks_rows([{"ticker": "NEW"}], rid_new)

    # Backdate the OLD file 60 days; default retention is 30.
    old_path = tmp_raw / "zacks" / f"{rid_old}.parquet"
    old_ts = (datetime.now() - timedelta(days=60)).timestamp()
    os.utime(old_path, (old_ts, old_ts))

    deleted = earnings_raw.prune_old_raw()
    assert deleted == 1
    assert not old_path.exists()
    assert (tmp_raw / "zacks" / f"{rid_new}.parquet").exists()


def test_prune_returns_zero_when_nothing_to_drop(tmp_raw):
    rid = earnings_raw.new_run_id()
    earnings_raw.append_zacks_rows([{"ticker": "AAPL"}], rid)
    deleted = earnings_raw.prune_old_raw()
    assert deleted == 0


def test_prune_walks_every_source(tmp_raw):
    """A 60-day-old file in EVERY source dir should all get pruned in one call."""
    cutoff_ts = (datetime.now() - timedelta(days=60)).timestamp()
    for src in config.RAW_SOURCES:
        rid = earnings_raw.new_run_id()
        if src == config.RAW_SOURCE_FINNHUB:
            earnings_raw.append_finnhub_rows([{"symbol": "X"}], rid)
        elif src == config.RAW_SOURCE_NASDAQ:
            earnings_raw.append_nasdaq_rows(
                [{"ticker": "X", "calendar_date": pd.Timestamp("2020-01-01")}], rid)
        elif src == config.RAW_SOURCE_YAHOO:
            earnings_raw.append_yahoo_rows(
                [{"ticker": "X", "all_dates_returned": ""}], rid)
        else:
            earnings_raw.append_zacks_rows([{"ticker": "X"}], rid)
        path = tmp_raw / src / f"{rid}.parquet"
        os.utime(path, (cutoff_ts, cutoff_ts))

    deleted = earnings_raw.prune_old_raw()
    assert deleted == len(config.RAW_SOURCES)


def test_prune_custom_retention(tmp_raw):
    rid = earnings_raw.new_run_id()
    earnings_raw.append_zacks_rows([{"ticker": "AAPL"}], rid)
    path = tmp_raw / "zacks" / f"{rid}.parquet"
    # Backdate 5 days and prune with retention=3 → should drop.
    os.utime(path, ((datetime.now() - timedelta(days=5)).timestamp(),) * 2)
    assert earnings_raw.prune_old_raw(retention_days=3) == 1
    assert not path.exists()


# ----------------------------------------------------------------------
# Resilience
# ----------------------------------------------------------------------

def test_corrupted_run_file_overwrites_gracefully(tmp_raw):
    """A truncated / non-parquet file under the source dir must not crash
    a subsequent append — instead the bad file is overwritten with the
    fresh batch (loss-of-audit acceptable; loss-of-fill-progress is not).
    """
    rid = earnings_raw.new_run_id()
    bad = tmp_raw / "zacks" / f"{rid}.parquet"
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_bytes(b"not-a-parquet-file")
    earnings_raw.append_zacks_rows([{"ticker": "AAPL"}], rid)
    df = earnings_raw.read_raw(config.RAW_SOURCE_ZACKS, run_id=rid)
    assert len(df) == 1
    assert df.iloc[0]["ticker"] == "AAPL"
