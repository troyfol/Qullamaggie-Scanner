"""Tests for sector_map.py — Phase 5 O2."""
import pandas as pd
import pytest

from trade_scanner_fh import sector_map


# ----------------------------------------------------------------------
# load / save round trip
# ----------------------------------------------------------------------

def test_save_load_round_trip(tmp_path, monkeypatch):
    path = tmp_path / "sector_map.parquet"
    monkeypatch.setattr(sector_map.config, "SECTOR_MAP_PARQUET", path)

    df = pd.DataFrame({
        "ticker":     ["AAPL", "MSFT", "JPM"],
        "sector":     ["Technology", "Technology", "Financials"],
        "sector_etf": ["XLK", "XLK", "XLF"],
        "updated_at": pd.to_datetime(["2026-04-01"] * 3),
    })
    sector_map.save_sector_map(df)
    loaded = sector_map.load_sector_map()

    assert loaded is not None
    assert set(loaded["ticker"]) == {"AAPL", "MSFT", "JPM"}
    assert set(loaded["sector_etf"]) == {"XLK", "XLF"}


def test_load_missing_file_returns_none(tmp_path, monkeypatch):
    monkeypatch.setattr(
        sector_map.config, "SECTOR_MAP_PARQUET", tmp_path / "missing.parquet"
    )
    assert sector_map.load_sector_map() is None


# ----------------------------------------------------------------------
# get_sector_etf lookup semantics
# ----------------------------------------------------------------------

def _small_map() -> pd.DataFrame:
    return pd.DataFrame({
        "ticker":     ["AAPL", "JPM", "ZERO", "EMPTY"],
        "sector":     ["Technology", "Financials", "Unknown", ""],
        "sector_etf": ["XLK", "XLF", None, ""],
    })


@pytest.mark.parametrize("ticker, expected", [
    ("AAPL", "XLK"),
    ("JPM", "XLF"),
    ("UNKNOWN", None),   # not in map
    ("ZERO", None),      # NaN etf
    ("EMPTY", None),     # empty string etf
])
def test_get_sector_etf_lookup(ticker, expected):
    assert sector_map.get_sector_etf(ticker, _small_map()) == expected


def test_get_sector_etf_empty_df_returns_none():
    assert sector_map.get_sector_etf("AAPL", pd.DataFrame()) is None
    assert sector_map.get_sector_etf("AAPL", None) is None


# ----------------------------------------------------------------------
# atomic-write guarantee (Phase 1 R12 regression)
# ----------------------------------------------------------------------

def test_save_leaves_no_tmp_file(tmp_path, monkeypatch):
    """R12 atomic write must clean up the .tmp file on success."""
    path = tmp_path / "sector_map.parquet"
    monkeypatch.setattr(sector_map.config, "SECTOR_MAP_PARQUET", path)

    df = pd.DataFrame({
        "ticker": ["AAPL"], "sector": ["Technology"], "sector_etf": ["XLK"],
        "updated_at": pd.to_datetime(["2026-04-01"]),
    })
    sector_map.save_sector_map(df)
    assert path.exists()
    assert not (tmp_path / "sector_map.parquet.tmp").exists()


# ----------------------------------------------------------------------
# Bug fix — module logger name on scanner.* namespace
# ----------------------------------------------------------------------

def test_module_logger_uses_scanner_namespace():
    """Regression: sector_map previously used getLogger(__name__) which
    routes records to `trade_scanner_fh.sector_map` — a logger with no
    handlers attached. Must be on the `scanner.*` namespace so QtLogHandler
    picks them up."""
    assert sector_map.log.name.startswith("scanner.")
