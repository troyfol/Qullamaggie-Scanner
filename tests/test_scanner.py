"""Tests for scanner.py — Phase 2 (I3, I4) and Phase 3 (I2)."""
from datetime import date

import pandas as pd

from trade_scanner_fh import data_engine, scanner
from trade_scanner_fh.scanner import ScanParams


# ----------------------------------------------------------------------
# I4 — sector lookup dict build (mirror of run_scan's pre-load pattern)
# ----------------------------------------------------------------------

def test_sector_lookup_built_from_dataframe():
    """Reproduces the run_scan pre-load pattern and verifies O(1) lookup
    semantics for the sector map."""
    sector_map_df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT", "JPM", "BADSECTOR"],
        "sector": ["Technology", "Technology", "Financials", ""],
        "sector_etf": ["XLK", "XLK", "XLF", None],
    })
    # Same logic as run_scan
    sector_lookup = {
        str(t): str(e)
        for t, e in zip(
            sector_map_df["ticker"],
            sector_map_df["sector_etf"].fillna(""),
        )
        if e
    }
    assert sector_lookup == {"AAPL": "XLK", "MSFT": "XLK", "JPM": "XLF"}
    assert "BADSECTOR" not in sector_lookup  # empty etf filtered out
    # O(1) membership test
    assert sector_lookup.get("AAPL") == "XLK"
    assert sector_lookup.get("UNKNOWN") is None


# ----------------------------------------------------------------------
# I4 — earnings lookup dict build
# ----------------------------------------------------------------------

def test_earnings_lookup_built_from_dataframe():
    """Reproduces the run_scan earnings-lookup pattern including NaT/None
    normalization."""
    earnings_df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT", "NODATES"],
        "last_earnings": [pd.Timestamp("2026-02-01"), pd.Timestamp("2026-01-25"), pd.NaT],
        "next_earnings": [pd.Timestamp("2026-05-01"), pd.NaT, pd.NaT],
        "updated_at": [pd.Timestamp("2026-04-01")] * 3,
    })
    earnings_lookup = {}
    for t, last_e, next_e in zip(
        earnings_df["ticker"],
        earnings_df["last_earnings"],
        earnings_df["next_earnings"],
    ):
        le = None if pd.isna(last_e) else pd.Timestamp(last_e)
        ne = None if pd.isna(next_e) else pd.Timestamp(next_e)
        earnings_lookup[str(t)] = (le, ne)

    assert earnings_lookup["AAPL"] == (pd.Timestamp("2026-02-01"), pd.Timestamp("2026-05-01"))
    assert earnings_lookup["MSFT"] == (pd.Timestamp("2026-01-25"), None)
    assert earnings_lookup["NODATES"] == (None, None)

    # Lookup behavior: missing ticker returns the default tuple
    last, nxt = earnings_lookup.get("UNKNOWN", (None, None))
    assert last is None and nxt is None


# ----------------------------------------------------------------------
# I3 + I4 — _compute_ticker accepts the new dict-based signature
# ----------------------------------------------------------------------

def test_compute_ticker_signature_accepts_dicts():
    """_compute_ticker now takes sector_lookup / earnings_lookup dicts
    rather than DataFrames. The signature change is part of Phase 2 I4."""
    import inspect
    sig = inspect.signature(scanner._compute_ticker)
    params = list(sig.parameters.keys())
    assert "sector_lookup" in params
    assert "earnings_lookup" in params
    assert "sector_map_df" not in params
    assert "earnings_df" not in params


# ----------------------------------------------------------------------
# Phase 3 I2 — Conditional indicator computation
# ----------------------------------------------------------------------

def _make_fake_parquet(tmp_path, monkeypatch, symbol="TEST", days=80):
    """Write a minimal business-day OHLCV parquet and point PARQUET_DIR at it."""
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    data_engine.clear_ohlcv_cache()

    idx = pd.date_range("2026-01-01", periods=days, freq="B")
    df = pd.DataFrame({
        "Open":   [100.0 + i * 0.1 for i in range(days)],
        "High":   [101.0 + i * 0.1 for i in range(days)],
        "Low":    [99.0 + i * 0.1 for i in range(days)],
        "Close":  [100.5 + i * 0.1 for i in range(days)],
        "Volume": [1_000_000] * days,
    }, index=idx)
    df.index.name = "Date"
    df.to_parquet(tmp_path / f"{symbol}.parquet")
    return df


def _all_disabled_params() -> ScanParams:
    """ScanParams with every _enabled flag set to False (most still-on-by-default
    indicators explicitly disabled)."""
    return ScanParams(
        start_date=date(2026, 1, 1), end_date=date(2026, 3, 1),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False, top_pct_enabled=False,
        consec_gaps_enabled=False, consec_gaps_down_enabled=False,
        current_gap_enabled=False, max_gap_enabled=False, max_neg_gap_enabled=False,
        surge_enabled=False, adr_enabled=False, atr_enabled=False,
        bbw_enabled=False, atr_ratio_enabled=False, vol_dryup_enabled=False,
        min_price_enabled=False, avg_vol_enabled=False, dollar_vol_enabled=False,
        rs_market_enabled=False, rs_nasdaq_enabled=False, rs_sector_enabled=False,
        days_since_earnings_enabled=False,
        days_until_earnings_enabled=False, days_until_max_enabled=False,
    )


def test_compute_ticker_all_disabled_produces_always_on_keys_only(tmp_path, monkeypatch):
    """When every indicator is disabled, _compute_ticker should emit only
    the always-on keys required by the pipeline."""
    _make_fake_parquet(tmp_path, monkeypatch, "TEST", days=80)
    params = _all_disabled_params()

    row = scanner._compute_ticker("TEST", params)
    assert row is not None
    # Exact set — neither more nor less. gain_start_date is part of the
    # always-on bundle (paired with pct_gain) for results display.
    assert set(row.keys()) == {
        "symbol", "close", "price", "pct_gain", "gain_start_date",
    }


def test_compute_ticker_enabled_indicators_included(tmp_path, monkeypatch):
    """Enabled indicators must appear in the row dict; disabled ones must not."""
    _make_fake_parquet(tmp_path, monkeypatch, "TEST", days=80)
    params = _all_disabled_params()
    # Enable a few
    params.adr_enabled = True
    params.atr_enabled = True
    params.bbw_enabled = True

    row = scanner._compute_ticker("TEST", params)
    assert row is not None
    # Enabled → present
    assert "adr_pct" in row
    assert "atr" in row
    assert "bbw" in row
    # Disabled → absent
    assert "sti" not in row
    assert "vol_dryup" not in row
    assert "consec_gaps" not in row
    assert "max_gap_pct" not in row
    # New date columns paired with disabled indicators must also be absent
    assert "max_gap_date" not in row
    assert "min_gap_date" not in row
    assert "up_gap_start_date" not in row
    assert "down_gap_start_date" not in row
    assert "surge_start_date" not in row


def test_compute_ticker_emits_paired_date_columns(tmp_path, monkeypatch):
    """Enabling a gap/streak/surge indicator also produces its sibling date
    column (Max Gap Date, Min Gap Date, Up Gap Start, Down Gap Start,
    Surge Start)."""
    _make_fake_parquet(tmp_path, monkeypatch, "TEST", days=80)
    params = _all_disabled_params()
    params.consec_gaps_enabled = True
    params.consec_gaps_down_enabled = True
    params.max_gap_enabled = True
    params.max_neg_gap_enabled = True
    params.surge_enabled = True

    row = scanner._compute_ticker("TEST", params)
    assert row is not None
    # Each paired date key exists alongside its value key (may be None
    # when the synthetic monotonic data has no gaps in a given direction)
    assert "consec_gaps" in row and "up_gap_start_date" in row
    assert "consec_gaps_down" in row and "down_gap_start_date" in row
    assert "max_gap_pct" in row and "max_gap_date" in row
    assert "max_neg_gap_pct" in row and "min_gap_date" in row
    assert "surge_pct" in row and "surge_start_date" in row


def test_compute_ticker_sma_enabled_includes_parametric_column(tmp_path, monkeypatch):
    """Enabling an SMA indicator should add a `sma{period}` key."""
    _make_fake_parquet(tmp_path, monkeypatch, "TEST", days=80)
    params = _all_disabled_params()
    params.sma1_enabled = True
    params.sma1_period = 20  # custom period

    row = scanner._compute_ticker("TEST", params)
    assert row is not None
    assert "sma20" in row
    # Second SMA disabled → no sma50 column
    assert "sma50" not in row


# ----------------------------------------------------------------------
# Phase 4 R2 — cancel_token interrupts run_scan's per-ticker loop
# ----------------------------------------------------------------------

def test_run_scan_cancel_token_stops_mid_loop(tmp_path, monkeypatch):
    """When cancel_token() returns True, run_scan must stop iterating new
    tickers — previously Stop only worked between timeframes."""
    # Seed 5 fake tickers
    for s in ["A", "B", "C", "D", "E"]:
        _make_fake_parquet(tmp_path, monkeypatch, s, days=80)

    calls = {"n": 0}

    def cancel_after_two():
        calls["n"] += 1
        # Returns True once we've been polled at least twice (after 2 tickers)
        return calls["n"] > 2

    params = _all_disabled_params()
    result = scanner.run_scan(
        ["A", "B", "C", "D", "E"],
        params,
        cancel_token=cancel_after_two,
    )

    # At most the first 2 tickers should have been processed
    assert len(result.results_df) <= 2


def test_run_scan_no_cancel_token_processes_all(tmp_path, monkeypatch):
    """Without cancel_token (or always-False), every ticker is processed."""
    for s in ["A", "B", "C"]:
        _make_fake_parquet(tmp_path, monkeypatch, s, days=80)

    params = _all_disabled_params()
    result = scanner.run_scan(["A", "B", "C"], params, cancel_token=None)
    assert len(result.results_df) == 3
