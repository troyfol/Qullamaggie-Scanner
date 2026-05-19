"""Tests for earnings_cache.py — Phase 5 O2."""
import pandas as pd
import pytest

from trade_scanner_fh import earnings_cache


# ----------------------------------------------------------------------
# load / save round trip
# ----------------------------------------------------------------------

def test_save_load_round_trip(tmp_path, monkeypatch):
    path = tmp_path / "earnings.parquet"
    monkeypatch.setattr(earnings_cache.config, "EARNINGS_PARQUET", path)

    df = pd.DataFrame({
        "ticker":         ["AAPL", "MSFT"],
        "last_earnings":  pd.to_datetime(["2026-02-01", "2026-01-25"]),
        "next_earnings":  pd.to_datetime(["2026-05-01", "2026-04-25"]),
        "updated_at":     pd.to_datetime(["2026-04-01"] * 2),
    })
    earnings_cache.save_earnings_cache(df)
    loaded = earnings_cache.load_earnings_cache()

    assert loaded is not None
    assert set(loaded["ticker"]) == {"AAPL", "MSFT"}


def test_load_missing_returns_none(tmp_path, monkeypatch):
    monkeypatch.setattr(
        earnings_cache.config, "EARNINGS_PARQUET", tmp_path / "missing.parquet"
    )
    assert earnings_cache.load_earnings_cache() is None


# ----------------------------------------------------------------------
# get_earnings_dates lookup + NaT normalization
# ----------------------------------------------------------------------

def _cache() -> pd.DataFrame:
    return pd.DataFrame({
        "ticker":        ["AAPL", "MSFT", "NODATES"],
        "last_earnings": [pd.Timestamp("2026-02-01"), pd.Timestamp("2026-01-25"), pd.NaT],
        "next_earnings": [pd.Timestamp("2026-05-01"), pd.NaT, pd.NaT],
        "updated_at":    [pd.Timestamp("2026-04-01")] * 3,
    })


def test_get_earnings_dates_normal_ticker():
    last, nxt = earnings_cache.get_earnings_dates("AAPL", _cache())
    assert last == pd.Timestamp("2026-02-01")
    assert nxt == pd.Timestamp("2026-05-01")


def test_get_earnings_dates_nat_next_normalizes_to_none():
    last, nxt = earnings_cache.get_earnings_dates("MSFT", _cache())
    assert last == pd.Timestamp("2026-01-25")
    assert nxt is None


def test_get_earnings_dates_all_nat_returns_none_pair():
    last, nxt = earnings_cache.get_earnings_dates("NODATES", _cache())
    assert last is None
    assert nxt is None


def test_get_earnings_dates_missing_ticker_returns_none_pair():
    last, nxt = earnings_cache.get_earnings_dates("UNKNOWN", _cache())
    assert last is None
    assert nxt is None


def test_get_earnings_dates_empty_df_returns_none_pair():
    last, nxt = earnings_cache.get_earnings_dates("AAPL", pd.DataFrame())
    assert last is None and nxt is None

    last2, nxt2 = earnings_cache.get_earnings_dates("AAPL", None)
    assert last2 is None and nxt2 is None


# ----------------------------------------------------------------------
# _merge_and_save — new rows merged with existing, drop_duplicates semantics
# ----------------------------------------------------------------------

def test_merge_and_save_adds_new_and_updates_existing(tmp_path, monkeypatch):
    path = tmp_path / "earnings.parquet"
    monkeypatch.setattr(earnings_cache.config, "EARNINGS_PARQUET", path)

    # Seed an existing cache
    existing = pd.DataFrame({
        "ticker":        ["AAPL"],
        "last_earnings": [pd.Timestamp("2026-02-01")],
        "next_earnings": [pd.Timestamp("2026-05-01")],
        "updated_at":    [pd.Timestamp("2026-04-01")],
    })
    earnings_cache.save_earnings_cache(existing)

    # Merge a new row (MSFT) + an update to AAPL
    new_rows = [
        {
            "ticker": "AAPL",
            "last_earnings": pd.Timestamp("2026-04-25"),  # updated
            "next_earnings": pd.Timestamp("2026-07-25"),
            "updated_at": pd.Timestamp("2026-04-25"),
        },
        {
            "ticker": "MSFT",
            "last_earnings": pd.Timestamp("2026-04-20"),
            "next_earnings": pd.Timestamp("2026-07-20"),
            "updated_at": pd.Timestamp("2026-04-25"),
        },
    ]
    earnings_cache._merge_and_save(new_rows, existing)
    merged = earnings_cache.load_earnings_cache()

    assert set(merged["ticker"]) == {"AAPL", "MSFT"}
    aapl = merged.loc[merged["ticker"] == "AAPL"].iloc[0]
    # keep="last" means the UPDATE row wins
    assert aapl["last_earnings"] == pd.Timestamp("2026-04-25")
    assert aapl["next_earnings"] == pd.Timestamp("2026-07-25")


def test_merge_and_save_no_new_rows_is_noop(tmp_path, monkeypatch):
    path = tmp_path / "earnings.parquet"
    monkeypatch.setattr(earnings_cache.config, "EARNINGS_PARQUET", path)
    earnings_cache._merge_and_save([], None)
    # No file written
    assert not path.exists()


# ----------------------------------------------------------------------
# Bug fixes — module logger name + incremental save
# ----------------------------------------------------------------------

def test_module_logger_uses_scanner_namespace():
    """Regression: earnings_cache previously used getLogger(__name__) which
    routes records to `trade_scanner_fh.earnings_cache` — a logger with no
    handlers attached, so all targeted-fill log messages were silently
    dropped. Must be on the `scanner.*` namespace so QtLogHandler picks
    them up."""
    assert earnings_cache.log.name.startswith("scanner.")


# NOTE: tests for the per-ticker fetch and targeted-fill flush moved to
# test_yahoo_fill.py in Phase 3 along with the production code. The
# legacy round-trip test below is the one earnings_cache test that
# stayed — it covers the schema/IO surface that this module retained.


def test_load_earnings_cache_legacy_rows_get_legacy_source(tmp_path, monkeypatch):
    """A parquet written before the source column existed must load with
    `source = "legacy"` so the reconciler doesn't blow up on NaN."""
    path = tmp_path / "earnings.parquet"
    monkeypatch.setattr(earnings_cache.config, "EARNINGS_PARQUET", path)

    legacy = pd.DataFrame({
        "ticker": ["X"],
        "last_earnings": pd.to_datetime(["2026-01-01"]),
        "next_earnings": pd.to_datetime(["2026-04-01"]),
        "updated_at": pd.to_datetime(["2026-04-01"]),
    })
    legacy.to_parquet(path, index=False)
    df = earnings_cache.load_earnings_cache()
    assert "source" in df.columns
    assert df.iloc[0]["source"] == "legacy"
