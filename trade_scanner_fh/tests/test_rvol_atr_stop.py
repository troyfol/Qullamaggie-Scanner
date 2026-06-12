"""Tests for FEATURE F1 — Relative Volume (RVOL) indicator + ATR Stop column.

Covers:
  * indicator math (known arrays, zero-volume base, short history,
    last-bar exclusion from the base mean)
  * ScanParams defaults (OFF / neutral so old presets load unchanged)
  * funnel filter stage inclusion / exclusion + NaN-fails convention
  * display-only semantics (value computed, stage skipped, red-on-fail)
  * RESULT_COLUMNS presence + formatting ("RVOL" 2-dec, "ATR Stop" price)
  * ATR Stop arithmetic (close − 2×ATR), NaN safety, no-filter/no-panel-row
  * legacy preset (no rvol keys) loads unchanged
"""
import json
from datetime import date

import numpy as np
import pandas as pd
import pytest

from trade_scanner_fh import data_engine, indicators, scanner
from trade_scanner_fh.scanner import ScanParams


# ======================================================================
# Helpers
# ======================================================================

def _ohlcv(closes, opens=None, highs=None, lows=None, volumes=None):
    """Business-day-indexed OHLCV frame (same shape as test_indicators)."""
    n = len(closes)
    idx = pd.date_range("2026-01-01", periods=n, freq="B")
    return pd.DataFrame({
        "Open":   opens   if opens   is not None else list(closes),
        "High":   highs   if highs   is not None else [c + 1.0 for c in closes],
        "Low":    lows    if lows    is not None else [c - 1.0 for c in closes],
        "Close":  list(closes),
        "Volume": volumes if volumes is not None else [1_000_000] * n,
    }, index=idx)


def _all_disabled_params() -> ScanParams:
    """ScanParams with every on-by-default filter explicitly disabled."""
    return ScanParams(
        start_date=date(2026, 1, 1), end_date=date(2026, 4, 30),
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


def _write_parquet(tmp_path, monkeypatch, df, symbol="TEST"):
    """Point PARQUET_DIR at tmp_path and write `df` as the symbol's OHLCV."""
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    data_engine.clear_ohlcv_cache()
    out = df.copy()
    out.index.name = "Date"
    out.to_parquet(tmp_path / f"{symbol}.parquet")
    return out


# ======================================================================
# Indicator math — indicators.relative_volume
# ======================================================================

def test_rvol_known_array():
    # 20 prior bars at 1M, last bar 2.5M → 2.5
    vols = [1_000_000] * 20 + [2_500_000]
    df = _ohlcv([100.0] * 21, volumes=vols)
    assert indicators.relative_volume(df, lookback=20) == pytest.approx(2.5)


def test_rvol_excludes_last_bar_from_base():
    # lookback=2: base = mean(100, 200) = 150; last = 600 → 4.0.
    # If the last bar leaked into the base (mean(100,200,600)=300) the
    # ratio would be 2.0 instead — guards the exclusion contract.
    df = _ohlcv([100.0] * 3, volumes=[100, 200, 600])
    assert indicators.relative_volume(df, lookback=2) == pytest.approx(4.0)


def test_rvol_exact_minimum_length_ok():
    # Exactly lookback+1 bars is sufficient.
    df = _ohlcv([100.0] * 6, volumes=[1000] * 5 + [3000])
    assert indicators.relative_volume(df, lookback=5) == pytest.approx(3.0)


def test_rvol_short_history_nan():
    # lookback+1 bars required — one short → NaN.
    df = _ohlcv([100.0] * 5, volumes=[1000] * 5)
    assert np.isnan(indicators.relative_volume(df, lookback=5))


def test_rvol_zero_volume_base_nan():
    # All-zero prior volume → base 0 → NaN (no divide-by-zero).
    df = _ohlcv([100.0] * 21, volumes=[0] * 20 + [5_000_000])
    assert np.isnan(indicators.relative_volume(df, lookback=20))


def test_rvol_nan_last_bar_volume_nan():
    vols = [1_000_000.0] * 20 + [np.nan]
    df = _ohlcv([100.0] * 21, volumes=vols)
    assert np.isnan(indicators.relative_volume(df, lookback=20))


def test_rvol_missing_volume_column_nan():
    df = _ohlcv([100.0] * 30).drop(columns=["Volume"])
    assert np.isnan(indicators.relative_volume(df, lookback=20))


def test_rvol_quiet_day_below_one():
    # Last bar at half the prior average → 0.5.
    vols = [2_000_000] * 20 + [1_000_000]
    df = _ohlcv([100.0] * 21, volumes=vols)
    assert indicators.relative_volume(df, lookback=20) == pytest.approx(0.5)


# ======================================================================
# ScanParams defaults — old presets must load unchanged
# ======================================================================

def test_scanparams_rvol_defaults_off_and_neutral():
    p = ScanParams()
    assert p.rvol_enabled is False
    assert p.rvol_display_only is False
    assert p.rvol_lookback == 20
    assert p.rvol_min == 1.5


def test_scanparams_constructs_from_legacy_kwargs_without_rvol():
    # An old preset's ScanParams construction (no rvol keys) still works
    # and lands on the OFF defaults.
    p = ScanParams(start_date=date(2026, 1, 1), end_date=date(2026, 3, 1),
                   adr_enabled=True, atr_enabled=True)
    assert p.rvol_enabled is False
    assert p.rvol_display_only is False


# ======================================================================
# Funnel filter stage — _build_filter_stages
# ======================================================================

def test_rvol_stage_absent_when_disabled():
    stages = scanner._build_filter_stages(_all_disabled_params())
    assert not any("RVOL" in name for name, _ in stages)


def test_rvol_stage_included_when_enabled_and_mask_math():
    p = _all_disabled_params()
    p.rvol_enabled = True
    p.rvol_min = 1.5

    stages = scanner._build_filter_stages(p)
    assert len(stages) == 1
    name, fn = stages[0]
    assert "RVOL" in name

    df = pd.DataFrame({
        "symbol": ["A", "B", "C", "D"],
        "rvol":   [0.5, 1.5, 3.0, np.nan],
    })
    mask = fn(df).fillna(False)  # scanner applies .fillna(False) downstream
    # 0.5 fails, 1.5 passes (>=), 3.0 passes, NaN fails (convention)
    assert list(mask) == [False, True, True, False]


def test_rvol_stage_skipped_in_display_only():
    p = _all_disabled_params()
    p.rvol_enabled = True
    p.rvol_display_only = True

    stages = scanner._build_filter_stages(p)
    assert not any("RVOL" in name for name, _ in stages)


# ======================================================================
# Display-only semantics — red-on-fail + value computation
# ======================================================================

def test_display_only_fails_flags_rvol_below_min():
    p = _all_disabled_params()
    p.rvol_display_only = True
    p.rvol_min = 1.5

    assert scanner._compute_display_only_fails(p, {"rvol": 0.8}) == {"rvol": True}
    assert "rvol" not in scanner._compute_display_only_fails(p, {"rvol": 1.5})
    assert "rvol" not in scanner._compute_display_only_fails(p, {"rvol": 4.2})
    # NaN / missing → no red flag (data-absent cells stay default color)
    assert "rvol" not in scanner._compute_display_only_fails(p, {"rvol": np.nan})
    assert "rvol" not in scanner._compute_display_only_fails(p, {})


def test_display_only_fails_not_flagged_when_display_only_off():
    p = _all_disabled_params()
    p.rvol_enabled = True   # plain filter mode — no red-on-fail marking
    assert "rvol" not in scanner._compute_display_only_fails(p, {"rvol": 0.1})


# ======================================================================
# _compute_ticker integration
# ======================================================================

def test_compute_ticker_rvol_present_when_enabled(tmp_path, monkeypatch):
    df = _ohlcv([100.0] * 80)  # constant 1M volume → rvol = 1.0
    _write_parquet(tmp_path, monkeypatch, df)
    p = _all_disabled_params()
    p.rvol_enabled = True

    row = scanner._compute_ticker("TEST", p)
    assert row is not None
    assert row["rvol"] == pytest.approx(1.0)


def test_compute_ticker_rvol_absent_when_off(tmp_path, monkeypatch):
    df = _ohlcv([100.0] * 80)
    _write_parquet(tmp_path, monkeypatch, df)

    row = scanner._compute_ticker("TEST", _all_disabled_params())
    assert row is not None
    assert "rvol" not in row


def test_compute_ticker_rvol_display_only_computes_value(tmp_path, monkeypatch):
    vols = [1_000_000] * 79 + [4_000_000]
    df = _ohlcv([100.0] * 80, volumes=vols)
    _write_parquet(tmp_path, monkeypatch, df)
    p = _all_disabled_params()
    p.rvol_display_only = True
    p.rvol_min = 99.0  # absurd threshold → would fail the filter

    row = scanner._compute_ticker("TEST", p)
    assert row is not None
    assert row["rvol"] == pytest.approx(4.0)
    # Display-only red-on-fail marking is stashed for the widget.
    assert row.get("_display_only_fails", {}).get("rvol") is True


def test_compute_ticker_rvol_anchored_to_end_date_bar(tmp_path, monkeypatch):
    """RVOL's 'last bar' must be the scan End-date bar, not the newest
    bar in the parquet — bars after end_date are excluded entirely."""
    n = 80
    idx = pd.date_range("2026-01-01", periods=n, freq="B")
    vols = [1_000_000] * n
    end_i = 50
    vols[end_i] = 3_000_000        # the End-date bar — 3× prior average
    for j in range(end_i + 1, n):  # newer bars: huge volume, must be ignored
        vols[j] = 50_000_000
    df = pd.DataFrame({
        "Open":   [100.0] * n,
        "High":   [101.0] * n,
        "Low":    [99.0] * n,
        "Close":  [100.0] * n,
        "Volume": vols,
    }, index=idx)
    _write_parquet(tmp_path, monkeypatch, df)

    p = _all_disabled_params()
    p.rvol_enabled = True
    p.start_date = idx[10].date()
    p.end_date = idx[end_i].date()

    row = scanner._compute_ticker("TEST", p)
    assert row is not None
    assert row["rvol"] == pytest.approx(3.0)


# ======================================================================
# ATR Stop — arithmetic, NaN safety, display-only nature
# ======================================================================

def test_atr_stop_arithmetic(tmp_path, monkeypatch):
    # Constant H-L=2, |H-prevC|=1, |L-prevC|=1 → TR=2 → ATR(14)=2.0.
    # Close=100 → ATR Stop = 100 − 2.0×2.0 = 96.0.
    df = _ohlcv([100.0] * 80,
                highs=[101.0] * 80, lows=[99.0] * 80)
    _write_parquet(tmp_path, monkeypatch, df)
    p = _all_disabled_params()
    p.atr_enabled = True
    p.atr_period = 14

    row = scanner._compute_ticker("TEST", p)
    assert row is not None
    assert row["atr"] == pytest.approx(2.0)
    assert row["atr_stop"] == pytest.approx(96.0)
    assert row["atr_stop"] == pytest.approx(
        row["close"] - scanner.ATR_STOP_MULTIPLIER * row["atr"]
    )


def test_atr_stop_present_in_atr_display_only_mode(tmp_path, monkeypatch):
    df = _ohlcv([100.0] * 80, highs=[101.0] * 80, lows=[99.0] * 80)
    _write_parquet(tmp_path, monkeypatch, df)
    p = _all_disabled_params()
    p.atr_display_only = True

    row = scanner._compute_ticker("TEST", p)
    assert row is not None
    assert row["atr_stop"] == pytest.approx(96.0)


def test_atr_stop_nan_when_atr_nan(tmp_path, monkeypatch):
    # 10 bars < period+1 → ATR NaN → ATR Stop NaN (not a crash).
    df = _ohlcv([100.0] * 10)
    _write_parquet(tmp_path, monkeypatch, df)
    p = _all_disabled_params()
    p.atr_enabled = True
    p.atr_period = 14
    p.start_date = date(2026, 1, 1)
    p.end_date = date(2026, 1, 31)

    row = scanner._compute_ticker("TEST", p)
    assert row is not None
    assert np.isnan(row["atr"])
    assert np.isnan(row["atr_stop"])


def test_atr_stop_absent_when_atr_off(tmp_path, monkeypatch):
    df = _ohlcv([100.0] * 80)
    _write_parquet(tmp_path, monkeypatch, df)

    row = scanner._compute_ticker("TEST", _all_disabled_params())
    assert row is not None
    assert "atr_stop" not in row


def test_atr_stop_has_no_filter_stage():
    """ATR Stop is informational — even with ATR fully enabled there is
    no 'ATR Stop' funnel stage (only the existing ATR range filter)."""
    p = _all_disabled_params()
    p.atr_enabled = True
    stages = scanner._build_filter_stages(p)
    names = [n for n, _ in stages]
    assert any(n.startswith("ATR") for n in names)
    assert not any("Stop" in n for n in names)


def test_atr_stop_has_no_scanparams_fields():
    p = ScanParams()
    assert not hasattr(p, "atr_stop_enabled")
    assert not hasattr(p, "atr_stop_display_only")


# ======================================================================
# Result columns — RESULT_COLUMNS + dynamic column build
# ======================================================================

def _columns_by_key():
    from trade_scanner_fh.gui.widgets import RESULT_COLUMNS
    return {k: (h, f) for h, k, f in RESULT_COLUMNS}


def test_result_columns_contain_rvol_with_two_decimals():
    cols = _columns_by_key()
    assert "rvol" in cols
    header, fmt = cols["rvol"]
    assert header == "RVOL"
    assert fmt(1.234) == "1.23"
    assert fmt(12.0) == "12.00"


def test_result_columns_contain_atr_stop_with_price_format():
    cols = _columns_by_key()
    assert "atr_stop" in cols
    header, fmt = cols["atr_stop"]
    assert header == "ATR Stop"
    assert fmt(96.0) == "$96.00"
    assert fmt(12.5) == "$12.50"


def test_dynamic_columns_include_new_keys_when_populated():
    from trade_scanner_fh.gui.widgets import _build_dynamic_columns
    df = pd.DataFrame({
        "symbol": ["A"], "close": [100.0], "pct_gain": [5.0],
        "gain_start_date": [None],
        "rvol": [2.0], "atr": [2.0], "atr_stop": [96.0],
    })
    cols, _, _ = _build_dynamic_columns(df)
    keys = [k for _, k, _ in cols]
    assert "rvol" in keys
    assert "atr_stop" in keys


def test_dynamic_columns_exclude_new_keys_when_absent():
    from trade_scanner_fh.gui.widgets import _build_dynamic_columns
    df = pd.DataFrame({
        "symbol": ["A"], "close": [100.0], "pct_gain": [5.0],
        "gain_start_date": [None],
    })
    cols, _, _ = _build_dynamic_columns(df)
    keys = [k for _, k, _ in cols]
    assert "rvol" not in keys
    assert "atr_stop" not in keys


# ======================================================================
# GUI panel — RVOL row, ATR Stop has no row, preset back-compat
# ======================================================================

@pytest.fixture
def panel(_qapp):
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    p = IndicatorPanel()
    yield p
    p.deleteLater()


def test_panel_rvol_row_three_state_and_defaults(panel):
    row = panel.rows["rvol"]
    # Standard 3-state: Filter toggle + Display Only checkbox, default OFF.
    assert row.display_only is not None
    assert row.is_enabled() is False
    assert row.is_display_only() is False
    assert row.value("lookback") == 20
    assert row.value("min_ratio") == pytest.approx(1.5)
    # Threshold spinbox range 0.1–50.0
    sb = row.spinboxes["min_ratio"]
    assert sb.minimum() == pytest.approx(0.1)
    assert sb.maximum() == pytest.approx(50.0)


def test_panel_has_no_atr_stop_row(panel):
    assert "atr_stop" not in panel.rows


def test_build_scan_params_threads_rvol(panel):
    panel.rows["rvol"].set_enabled(True)
    panel.rows["rvol"].set_value("lookback", 10)
    panel.rows["rvol"].set_value("min_ratio", 2.5)

    p = panel.build_scan_params(date(2026, 1, 1), date(2026, 3, 1))
    assert p.rvol_enabled is True
    assert p.rvol_display_only is False
    assert p.rvol_lookback == 10
    assert p.rvol_min == pytest.approx(2.5)


def test_build_scan_params_rvol_display_only(panel):
    panel.rows["rvol"].display_only.setChecked(True)
    p = panel.build_scan_params(date(2026, 1, 1), date(2026, 3, 1))
    assert p.rvol_enabled is False
    assert p.rvol_display_only is True


def test_old_preset_without_rvol_keys_loads_unchanged(panel):
    """A legacy preset's `indicators` dict has no 'rvol' entry. from_dict
    must load it without error and leave RVOL at its OFF defaults."""
    legacy = panel.to_dict()
    del legacy["rvol"]
    assert "rvol" not in legacy
    # Round-trip through JSON like a real preset file.
    legacy = json.loads(json.dumps(legacy))

    panel.from_dict(legacy)  # must not raise

    row = panel.rows["rvol"]
    assert row.is_enabled() is False
    assert row.is_display_only() is False
    assert row.value("lookback") == 20
    assert row.value("min_ratio") == pytest.approx(1.5)

    p = panel.build_scan_params(date(2026, 1, 1), date(2026, 3, 1))
    assert p.rvol_enabled is False
    assert p.rvol_display_only is False


def test_new_preset_round_trips_rvol(panel):
    panel.rows["rvol"].set_enabled(True)
    panel.rows["rvol"].set_value("lookback", 15)
    panel.rows["rvol"].set_value("min_ratio", 3.0)
    snap = json.loads(json.dumps(panel.to_dict()))

    panel.rows["rvol"].set_enabled(False)
    panel.rows["rvol"].set_value("lookback", 20)
    panel.rows["rvol"].set_value("min_ratio", 1.5)

    panel.from_dict(snap)
    row = panel.rows["rvol"]
    assert row.is_enabled() is True
    assert row.value("lookback") == 15
    assert row.value("min_ratio") == pytest.approx(3.0)
