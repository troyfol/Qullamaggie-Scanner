"""Tests for FEATURE — ADR% formula change + $ADR filter + ADR Stop
column + configurable stop multipliers (2026-06).

Covers:
  * NEW ADR% math: mean(100 × (High/Low − 1)) over the trailing
    `lookback` bars (default 20), zero/negative-Low bars masked out.
    Hand-computed values pin the new formula where the OLD form
    (mean((H−L)/Close) × 100, lookback 14) gave a different number —
    a regression to the old formula, a sign flip, or a window
    off-by-one fails these.
  * $ADR = adr_pct/100 × last Close — derived from the SAME masked
    ratio mean so ADR% and $ADR always agree (identity asserted both
    at the indicator level and through _compute_ticker).
  * $ADR funnel stage in/out + NaN-fails, display-only semantics
    (value computed, stage skipped, red-on-fail).
  * "ADR Stop" = Close − adr_stop_multiplier × $ADR, gated like
    ATR Stop (present when $ADR computed: enabled OR display-only),
    NaN-safe, no filter stage of its own.
  * Configurable multipliers: ScanParams.atr_stop_multiplier
    (default 2.0 = previously hard-coded behavior) and
    .adr_stop_multiplier (default 1.0) honored by both stop columns.
  * Preset back-compat: an OLD preset (no new keys) loads on a fresh
    panel with multipliers 2.0/1.0 (→ ATR Stop identical to today)
    while a stored adr lookback (e.g. 14) is KEPT; a new preset
    round-trips lookback 20 + both multipliers.
  * RESULT_COLUMNS entries ("$ADR", "ADR Stop", price format) +
    dynamic column build, IndicatorPanel 3-state row wiring.
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
    """ScanParams with every on-by-default filter explicitly disabled.
    (adr_dollar / rvol default OFF already — nothing to disable.)"""
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


# 80 bars, H=103 / L=100 / C=100 → every bar's ratio range is exactly
# 100×(103/100 − 1) = 3.0% and the last close is 100.0. Used by most
# of the _compute_ticker tests below: ADR% = 3.0, $ADR = 3.00.
def _const_3pct_df(n=80):
    return _ohlcv([100.0] * n, highs=[103.0] * n, lows=[100.0] * n)


# ======================================================================
# NEW ADR% formula — mean(100 × (High/Low − 1)), trailing window
# ======================================================================

def test_adr_pct_new_formula_known_value():
    # 100×(103/100 − 1) = 3.0 for every bar → exactly 3.0.
    df = _const_3pct_df(30)
    assert indicators.adr_pct(df, lookback=20) == pytest.approx(3.0)


def test_adr_pct_new_formula_differs_from_old_on_trending_series():
    """Trending 2-bar series where High/Low ratios are equal but the
    old (H−L)/Close form is not. New: bar1 = 100×(110/100−1) = 10,
    bar2 = 100×(121/110−1) = 10 → mean exactly 10.0.
    Old: (10/105 + 11/120)/2 × 100 = 9.345238… — a regression to the
    old formula lands ~0.65 below and fails the exact pin."""
    df = _ohlcv([105.0, 120.0], highs=[110.0, 121.0], lows=[100.0, 110.0])
    new_val = indicators.adr_pct(df, lookback=2)
    assert new_val == pytest.approx(10.0)

    old_val = (((110.0 - 100.0) / 105.0) + ((121.0 - 110.0) / 120.0)) / 2 * 100.0
    assert old_val == pytest.approx(9.3452380952, abs=1e-9)
    assert abs(new_val - old_val) > 0.5  # the two formulas measurably differ


def test_adr_pct_window_is_trailing_bars_ending_at_last():
    """25 bars: oldest 5 have a 50% ratio range, trailing 20 exactly 2%.
    lookback=20 must cover ONLY the trailing 20 → exactly 2.0. An
    off-by-one window (e.g. iloc[-21:-1] → (50 + 19×2)/20 = 4.4) or a
    sign flip (100×(L/H−1) ≈ −1.96) both fail."""
    highs = [150.0] * 5 + [102.0] * 20
    lows = [100.0] * 25
    df = _ohlcv([101.0] * 25, highs=highs, lows=lows)
    assert indicators.adr_pct(df, lookback=20) == pytest.approx(2.0)


def test_adr_pct_default_lookback_is_20_not_14():
    """34 bars: oldest 14 at 2%, then 6 bars at 10%, trailing 14 at 2%.
    Default lookback 20 → (6×10 + 14×2)/20 = 4.4 exactly.
    A 14-bar default would give 2.0; 21 bars would give 90/21 ≈ 4.286."""
    highs = [102.0] * 14 + [110.0] * 6 + [102.0] * 14
    lows = [100.0] * 34
    df = _ohlcv([101.0] * 34, highs=highs, lows=lows)
    assert indicators.adr_pct(df) == pytest.approx(4.4)
    assert indicators.adr_pct(df, lookback=14) == pytest.approx(2.0)


def test_adr_pct_masks_zero_and_negative_low_bars():
    """Bad bars (Low <= 0) are masked out of the mean, mirroring the
    old formula's zero-Close guard. 2 good bars at 2% + one Low=0 bar
    + one Low=−5 bar → mean of the good bars only = 2.0."""
    highs = [102.0, 102.0, 102.0, 102.0]
    lows = [100.0, 0.0, 100.0, -5.0]
    df = _ohlcv([100.0] * 4, highs=highs, lows=lows)
    assert indicators.adr_pct(df, lookback=4) == pytest.approx(2.0)


def test_adr_pct_all_bad_low_bars_nan():
    df = _ohlcv([100.0] * 3, highs=[102.0] * 3, lows=[0.0] * 3)
    assert np.isnan(indicators.adr_pct(df, lookback=3))


def test_adr_pct_shorter_history_uses_available_bars():
    # Existing adr_pct contract (unchanged): fewer bars than lookback →
    # average over what's there (NaN only when empty).
    df = _ohlcv([100.0] * 5, highs=[104.0] * 5, lows=[100.0] * 5)
    assert indicators.adr_pct(df, lookback=20) == pytest.approx(4.0)


# ======================================================================
# $ADR indicator — indicators.adr_dollar
# ======================================================================

def test_adr_dollar_known_value():
    # ADR% = 3.0, last Close = 100 → $ADR = 3.00.
    df = _const_3pct_df(30)
    assert indicators.adr_dollar(df, lookback=20) == pytest.approx(3.0)


def test_adr_dollar_scales_with_last_close():
    # Same 3% range, but the last close is 50 → $ADR = 1.50.
    closes = [100.0] * 29 + [50.0]
    df = _ohlcv(closes, highs=[103.0] * 30, lows=[100.0] * 30)
    assert indicators.adr_dollar(df, lookback=20) == pytest.approx(1.5)


def test_adr_dollar_identity_with_adr_pct():
    """$ADR must equal adr_pct/100 × last Close on an arbitrary
    (trending, uneven) series — same lookback, same masking."""
    closes = [100.0, 104.0, 99.0, 107.5, 123.45]
    highs = [105.0, 108.0, 104.5, 110.0, 125.0]
    lows = [98.0, 101.0, 97.0, 103.0, 119.0]
    df = _ohlcv(closes, highs=highs, lows=lows)
    pct = indicators.adr_pct(df, lookback=5)
    assert indicators.adr_dollar(df, lookback=5) == pytest.approx(
        pct / 100.0 * 123.45
    )


def test_adr_dollar_partial_window_matches_adr_pct():
    # Shorter-than-lookback history mirrors adr_pct's partial-window
    # mean (the two must agree on availability too).
    df = _ohlcv([100.0] * 5, highs=[104.0] * 5, lows=[100.0] * 5)
    assert indicators.adr_dollar(df, lookback=20) == pytest.approx(4.0)


def test_adr_dollar_empty_df_nan():
    df = _ohlcv([])
    assert np.isnan(indicators.adr_dollar(df, lookback=20))
    assert np.isnan(indicators.adr_dollar(None, lookback=20))


def test_adr_dollar_missing_columns_nan():
    df = _const_3pct_df(30)
    assert np.isnan(indicators.adr_dollar(df.drop(columns=["Low"]), lookback=20))
    assert np.isnan(indicators.adr_dollar(df.drop(columns=["Close"]), lookback=20))
    assert np.isnan(indicators.adr_dollar(df.drop(columns=["High"]), lookback=20))


def test_adr_dollar_nonpositive_last_close_nan():
    closes = [100.0] * 29 + [0.0]
    df = _ohlcv(closes, highs=[103.0] * 30, lows=[100.0] * 30)
    assert np.isnan(indicators.adr_dollar(df, lookback=20))
    closes[-1] = -4.0
    df = _ohlcv(closes, highs=[103.0] * 30, lows=[100.0] * 30)
    assert np.isnan(indicators.adr_dollar(df, lookback=20))


def test_adr_dollar_nan_last_close_nan():
    closes = [100.0] * 29 + [np.nan]
    df = _ohlcv(closes, highs=[103.0] * 30, lows=[100.0] * 30)
    assert np.isnan(indicators.adr_dollar(df, lookback=20))


def test_adr_dollar_all_bad_bars_nan():
    # ADR% itself NaN (every Low masked) → $ADR NaN.
    df = _ohlcv([100.0] * 25, highs=[102.0] * 25, lows=[0.0] * 25)
    assert np.isnan(indicators.adr_dollar(df, lookback=20))


# ======================================================================
# ScanParams defaults — old presets must load unchanged
# ======================================================================

def test_scanparams_adr_dollar_defaults_off_and_neutral():
    p = ScanParams()
    assert p.adr_dollar_enabled is False
    assert p.adr_dollar_display_only is False
    assert p.adr_dollar_min == pytest.approx(0.50)


def test_scanparams_adr_lookback_default_now_20():
    assert ScanParams().adr_lookback == 20


def test_scanparams_stop_multiplier_defaults_match_shipped_behavior():
    p = ScanParams()
    assert p.atr_stop_multiplier == pytest.approx(2.0)
    assert p.adr_stop_multiplier == pytest.approx(1.0)
    assert scanner.ATR_STOP_MULTIPLIER == pytest.approx(2.0)
    assert scanner.ADR_STOP_MULTIPLIER == pytest.approx(1.0)


def test_scanparams_constructs_from_legacy_kwargs_without_new_fields():
    # An old preset's ScanParams construction (no adr_dollar / multiplier
    # keys) still works and lands on the no-behavior-change defaults.
    p = ScanParams(start_date=date(2026, 1, 1), end_date=date(2026, 3, 1),
                   adr_enabled=True, adr_lookback=14, atr_enabled=True)
    assert p.adr_dollar_enabled is False
    assert p.adr_dollar_display_only is False
    assert p.atr_stop_multiplier == pytest.approx(2.0)
    assert p.adr_stop_multiplier == pytest.approx(1.0)
    # Explicitly stored lookback is honored (only the DEFAULT moved to 20).
    assert p.adr_lookback == 14


def test_scanparams_has_no_separate_adr_dollar_lookback():
    # One lookback drives both ADR% and $ADR by design.
    assert not hasattr(ScanParams(), "adr_dollar_lookback")


def test_adr_stop_has_no_scanparams_fields():
    p = ScanParams()
    assert not hasattr(p, "adr_stop_enabled")
    assert not hasattr(p, "adr_stop_display_only")


# ======================================================================
# Funnel filter stage — _build_filter_stages
# ======================================================================

def test_adr_dollar_stage_absent_when_disabled():
    stages = scanner._build_filter_stages(_all_disabled_params())
    assert not any("$ADR" in name for name, _ in stages)


def test_adr_dollar_stage_included_when_enabled_and_mask_math():
    p = _all_disabled_params()
    p.adr_dollar_enabled = True
    p.adr_dollar_min = 0.50

    stages = scanner._build_filter_stages(p)
    assert len(stages) == 1
    name, fn = stages[0]
    assert name == "$ADR >= 0.50"

    df = pd.DataFrame({
        "symbol": ["A", "B", "C", "D"],
        "adr_dollar": [0.30, 0.50, 2.00, np.nan],
    })
    mask = fn(df).fillna(False)  # scanner applies .fillna(False) downstream
    # 0.30 fails, 0.50 passes (>=), 2.00 passes, NaN fails (convention)
    assert list(mask) == [False, True, True, False]


def test_adr_dollar_stage_skipped_in_display_only():
    p = _all_disabled_params()
    p.adr_dollar_enabled = True
    p.adr_dollar_display_only = True
    stages = scanner._build_filter_stages(p)
    assert not any("$ADR" in name for name, _ in stages)


def test_adr_stop_has_no_filter_stage():
    """ADR Stop is informational — even with $ADR fully enabled there is
    no 'Stop' funnel stage (only the $ADR minimum filter)."""
    p = _all_disabled_params()
    p.adr_dollar_enabled = True
    names = [n for n, _ in scanner._build_filter_stages(p)]
    assert any(n.startswith("$ADR") for n in names)
    assert not any("Stop" in n for n in names)


# ======================================================================
# Display-only semantics — red-on-fail
# ======================================================================

def test_display_only_fails_flags_adr_dollar_below_min():
    p = _all_disabled_params()
    p.adr_dollar_display_only = True
    p.adr_dollar_min = 1.0

    assert scanner._compute_display_only_fails(
        p, {"adr_dollar": 0.40}) == {"adr_dollar": True}
    assert "adr_dollar" not in scanner._compute_display_only_fails(
        p, {"adr_dollar": 1.0})
    assert "adr_dollar" not in scanner._compute_display_only_fails(
        p, {"adr_dollar": 3.7})
    # NaN / missing → no red flag (data-absent cells stay default color)
    assert "adr_dollar" not in scanner._compute_display_only_fails(
        p, {"adr_dollar": np.nan})
    assert "adr_dollar" not in scanner._compute_display_only_fails(p, {})


def test_display_only_fails_not_flagged_when_display_only_off():
    p = _all_disabled_params()
    p.adr_dollar_enabled = True  # plain filter mode — no red-on-fail marking
    assert "adr_dollar" not in scanner._compute_display_only_fails(
        p, {"adr_dollar": 0.01})


# ======================================================================
# _compute_ticker integration — values, identity, gating, anchoring
# ======================================================================

def test_compute_ticker_adr_dollar_and_stop_when_enabled(tmp_path, monkeypatch):
    _write_parquet(tmp_path, monkeypatch, _const_3pct_df())
    p = _all_disabled_params()
    p.adr_dollar_enabled = True

    row = scanner._compute_ticker("TEST", p)
    assert row is not None
    assert row["adr_dollar"] == pytest.approx(3.0)
    # Default adr_stop_multiplier = 1.0 → stop = 100 − 1.0×3.0 = 97.0.
    assert row["adr_stop"] == pytest.approx(97.0)
    assert row["adr_stop"] == pytest.approx(
        row["close"] - scanner.ADR_STOP_MULTIPLIER * row["adr_dollar"]
    )


def test_compute_ticker_adr_dollar_identity_with_adr_pct(tmp_path, monkeypatch):
    """$ADR == adr_pct/100 × close through the full pipeline (both
    indicators enabled, sharing one lookback)."""
    closes = [100.0] * 79 + [123.45]
    highs = [103.0] * 80
    lows = [100.0] * 80
    _write_parquet(tmp_path, monkeypatch, _ohlcv(closes, highs=highs, lows=lows))
    p = _all_disabled_params()
    p.adr_enabled = True
    p.adr_dollar_enabled = True

    row = scanner._compute_ticker("TEST", p)
    assert row is not None
    assert np.isfinite(row["adr_pct"])
    assert row["adr_dollar"] == pytest.approx(
        row["adr_pct"] / 100.0 * row["close"]
    )
    assert row["close"] == pytest.approx(123.45)


def test_compute_ticker_adr_dollar_absent_when_off(tmp_path, monkeypatch):
    _write_parquet(tmp_path, monkeypatch, _const_3pct_df())
    row = scanner._compute_ticker("TEST", _all_disabled_params())
    assert row is not None
    assert "adr_dollar" not in row
    assert "adr_stop" not in row


def test_compute_ticker_adr_dollar_display_only_computes_value(tmp_path, monkeypatch):
    _write_parquet(tmp_path, monkeypatch, _const_3pct_df())
    p = _all_disabled_params()
    p.adr_dollar_display_only = True
    p.adr_dollar_min = 99.0  # absurd threshold → would fail the filter

    row = scanner._compute_ticker("TEST", p)
    assert row is not None
    assert row["adr_dollar"] == pytest.approx(3.0)
    # ADR Stop present in display-only mode too (parent-computed gate).
    assert row["adr_stop"] == pytest.approx(97.0)
    # Display-only red-on-fail marking is stashed for the widget.
    assert row.get("_display_only_fails", {}).get("adr_dollar") is True


def test_compute_ticker_one_lookback_drives_both(tmp_path, monkeypatch):
    """params.adr_lookback feeds BOTH ADR% and $ADR. 30 bars: first 28
    at 2% range, last 2 at 10% → lookback=2 gives 10.0% / $10.00
    (a separate $ADR lookback stuck at 20 would give 2.8)."""
    highs = [102.0] * 28 + [110.0] * 2
    lows = [100.0] * 30
    _write_parquet(tmp_path, monkeypatch,
                   _ohlcv([100.0] * 30, highs=highs, lows=lows))
    p = _all_disabled_params()
    p.adr_enabled = True
    p.adr_dollar_enabled = True
    p.adr_lookback = 2

    row = scanner._compute_ticker("TEST", p)
    assert row is not None
    assert row["adr_pct"] == pytest.approx(10.0)
    assert row["adr_dollar"] == pytest.approx(10.0)


def test_compute_ticker_adr_dollar_anchored_to_end_date_bar(tmp_path, monkeypatch):
    """$ADR's window must end at the scan End-date bar — bars after
    end_date (huge ranges here) are excluded entirely."""
    n = 80
    idx = pd.date_range("2026-01-01", periods=n, freq="B")
    highs = [102.0] * n
    end_i = 50
    for j in range(end_i + 1, n):  # newer bars: 100% range, must be ignored
        highs[j] = 200.0
    df = pd.DataFrame({
        "Open":   [100.0] * n,
        "High":   highs,
        "Low":    [100.0] * n,
        "Close":  [100.0] * n,
        "Volume": [1_000_000] * n,
    }, index=idx)
    _write_parquet(tmp_path, monkeypatch, df)

    p = _all_disabled_params()
    p.adr_dollar_enabled = True
    p.start_date = idx[10].date()
    p.end_date = idx[end_i].date()

    row = scanner._compute_ticker("TEST", p)
    assert row is not None
    assert row["adr_dollar"] == pytest.approx(2.0)
    assert row["adr_stop"] == pytest.approx(98.0)


def test_compute_ticker_adr_stop_nan_when_adr_dollar_nan(tmp_path, monkeypatch):
    # All Lows non-positive → ADR% NaN → $ADR NaN → ADR Stop NaN (no crash).
    df = _ohlcv([100.0] * 10, highs=[101.0] * 10, lows=[0.0] * 10)
    _write_parquet(tmp_path, monkeypatch, df)
    p = _all_disabled_params()
    p.adr_dollar_enabled = True

    row = scanner._compute_ticker("TEST", p)
    assert row is not None
    assert np.isnan(row["adr_dollar"])
    assert np.isnan(row["adr_stop"])


# ======================================================================
# Configurable stop multipliers — both columns honor per-scan settings
# ======================================================================

def test_adr_stop_non_default_multiplier(tmp_path, monkeypatch):
    _write_parquet(tmp_path, monkeypatch, _const_3pct_df())
    p = _all_disabled_params()
    p.adr_dollar_enabled = True
    p.adr_stop_multiplier = 2.5

    row = scanner._compute_ticker("TEST", p)
    assert row is not None
    # 100 − 2.5 × 3.0 = 92.5
    assert row["adr_stop"] == pytest.approx(92.5)


def test_atr_stop_default_multiplier_identical_to_shipped_2x(tmp_path, monkeypatch):
    """An old preset (no multiplier keys) → ScanParams default 2.0 →
    ATR Stop identical to the previously hard-coded behavior."""
    # H−L=2, |H−prevC|=1, |L−prevC|=1 → TR=2 → ATR(14)=2.0; Close=100.
    df = _ohlcv([100.0] * 80, highs=[101.0] * 80, lows=[99.0] * 80)
    _write_parquet(tmp_path, monkeypatch, df)
    p = _all_disabled_params()  # legacy construction — no multiplier kwargs
    p.atr_enabled = True
    p.atr_period = 14

    row = scanner._compute_ticker("TEST", p)
    assert row is not None
    assert row["atr"] == pytest.approx(2.0)
    assert row["atr_stop"] == pytest.approx(96.0)  # 100 − 2.0×2.0, as shipped
    assert row["atr_stop"] == pytest.approx(
        row["close"] - scanner.ATR_STOP_MULTIPLIER * row["atr"]
    )


def test_atr_stop_multiplier_override_changes_column(tmp_path, monkeypatch):
    df = _ohlcv([100.0] * 80, highs=[101.0] * 80, lows=[99.0] * 80)
    _write_parquet(tmp_path, monkeypatch, df)
    p = _all_disabled_params()
    p.atr_enabled = True
    p.atr_period = 14
    p.atr_stop_multiplier = 3.0

    row = scanner._compute_ticker("TEST", p)
    assert row is not None
    assert row["atr"] == pytest.approx(2.0)
    assert row["atr_stop"] == pytest.approx(94.0)  # 100 − 3.0×2.0


# ======================================================================
# Result columns — RESULT_COLUMNS + dynamic column build
# ======================================================================

def _columns_by_key():
    from trade_scanner_fh.gui.widgets import RESULT_COLUMNS
    return {k: (h, f) for h, k, f in RESULT_COLUMNS}


def test_result_columns_contain_adr_dollar_with_price_format():
    cols = _columns_by_key()
    assert "adr_dollar" in cols
    header, fmt = cols["adr_dollar"]
    assert header == "$ADR"
    assert fmt(3.0) == "$3.00"
    assert fmt(1.234) == "$1.23"


def test_result_columns_contain_adr_stop_with_price_format():
    cols = _columns_by_key()
    assert "adr_stop" in cols
    header, fmt = cols["adr_stop"]
    assert header == "ADR Stop"
    assert fmt(97.0) == "$97.00"
    assert fmt(12.5) == "$12.50"


def test_dynamic_columns_include_new_keys_when_populated():
    from trade_scanner_fh.gui.widgets import _build_dynamic_columns
    df = pd.DataFrame({
        "symbol": ["A"], "close": [100.0], "pct_gain": [5.0],
        "gain_start_date": [None],
        "adr_dollar": [3.0], "adr_stop": [97.0],
    })
    cols, _, _ = _build_dynamic_columns(df)
    keys = [k for _, k, _ in cols]
    assert "adr_dollar" in keys
    assert "adr_stop" in keys


def test_dynamic_columns_exclude_new_keys_when_absent():
    from trade_scanner_fh.gui.widgets import _build_dynamic_columns
    df = pd.DataFrame({
        "symbol": ["A"], "close": [100.0], "pct_gain": [5.0],
        "gain_start_date": [None],
    })
    cols, _, _ = _build_dynamic_columns(df)
    keys = [k for _, k, _ in cols]
    assert "adr_dollar" not in keys
    assert "adr_stop" not in keys


# ======================================================================
# GUI panel — $ADR row, multiplier spinboxes, preset back-compat
# ======================================================================

@pytest.fixture
def panel(_qapp):
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    p = IndicatorPanel()
    yield p
    p.deleteLater()


def test_panel_adr_dollar_row_three_state_and_defaults(panel):
    row = panel.rows["adr_dollar"]
    # Standard 3-state: Filter toggle + Display Only checkbox, default OFF.
    assert row.display_only is not None
    assert row.is_enabled() is False
    assert row.is_display_only() is False
    assert row.value("min_val") == pytest.approx(0.50)
    sb = row.spinboxes["min_val"]
    assert sb.minimum() == pytest.approx(0.05)
    assert sb.maximum() == pytest.approx(100.0)
    assert sb.singleStep() == pytest.approx(0.05)


def test_panel_adr_row_default_lookback_20(panel):
    assert panel.rows["adr"].value("lookback") == 20


def test_panel_stop_multiplier_spinboxes(panel):
    for key, default in (("adr_dollar", 1.0), ("atr", 2.0)):
        sb = panel.rows[key].spinboxes["stop_mult"]
        assert sb.value() == pytest.approx(default)
        assert sb.minimum() == pytest.approx(0.1)
        assert sb.maximum() == pytest.approx(10.0)
        assert sb.singleStep() == pytest.approx(0.1)
        assert sb.decimals() == 1


def test_panel_has_no_adr_stop_row(panel):
    assert "adr_stop" not in panel.rows


def test_build_scan_params_threads_adr_dollar_and_multipliers(panel):
    panel.rows["adr_dollar"].set_enabled(True)
    panel.rows["adr_dollar"].set_value("min_val", 1.25)
    panel.rows["adr_dollar"].set_value("stop_mult", 1.5)
    panel.rows["atr"].set_value("stop_mult", 3.5)
    panel.rows["adr"].set_value("lookback", 10)

    p = panel.build_scan_params(date(2026, 1, 1), date(2026, 3, 1))
    assert p.adr_dollar_enabled is True
    assert p.adr_dollar_display_only is False
    assert p.adr_dollar_min == pytest.approx(1.25)
    assert p.adr_stop_multiplier == pytest.approx(1.5)
    assert p.atr_stop_multiplier == pytest.approx(3.5)
    # $ADR shares the ADR% row's lookback.
    assert p.adr_lookback == 10


def test_build_scan_params_adr_dollar_display_only(panel):
    panel.rows["adr_dollar"].display_only.setChecked(True)
    p = panel.build_scan_params(date(2026, 1, 1), date(2026, 3, 1))
    assert p.adr_dollar_enabled is False
    assert p.adr_dollar_display_only is True


def test_old_preset_without_new_keys_loads_unchanged(panel):
    """A legacy preset has no 'adr_dollar' entry, no 'stop_mult' in the
    'atr' entry, and an old explicit adr lookback of 14. On a fresh
    panel: the stored lookback is KEPT (not migrated to 20), the
    multipliers stay at the no-behavior-change defaults (2.0 / 1.0 →
    ATR Stop identical to today), and the $ADR row stays OFF."""
    legacy = panel.to_dict()
    del legacy["adr_dollar"]
    del legacy["atr"]["stop_mult"]
    legacy["adr"] = {"enabled": True, "lookback": 14, "min_pct": 3.0}
    # Round-trip through JSON like a real preset file.
    legacy = json.loads(json.dumps(legacy))

    panel.from_dict(legacy)  # must not raise

    assert panel.rows["adr"].value("lookback") == 14  # stored value kept
    assert panel.rows["atr"].value("stop_mult") == pytest.approx(2.0)
    row = panel.rows["adr_dollar"]
    assert row.is_enabled() is False
    assert row.is_display_only() is False
    assert row.value("min_val") == pytest.approx(0.50)
    assert row.value("stop_mult") == pytest.approx(1.0)

    p = panel.build_scan_params(date(2026, 1, 1), date(2026, 3, 1))
    assert p.adr_lookback == 14
    assert p.adr_dollar_enabled is False
    assert p.adr_dollar_display_only is False
    assert p.atr_stop_multiplier == pytest.approx(2.0)
    assert p.adr_stop_multiplier == pytest.approx(1.0)


def test_new_preset_round_trips_lookback_and_multipliers(panel):
    panel.rows["adr_dollar"].set_enabled(True)
    panel.rows["adr_dollar"].set_value("min_val", 2.0)
    panel.rows["adr_dollar"].set_value("stop_mult", 1.8)
    panel.rows["atr"].set_value("stop_mult", 4.0)
    # adr lookback left at the new default 20 — must persist as 20.
    snap = json.loads(json.dumps(panel.to_dict()))
    assert snap["adr"]["lookback"] == 20
    assert snap["adr_dollar"]["stop_mult"] == pytest.approx(1.8)
    assert snap["atr"]["stop_mult"] == pytest.approx(4.0)

    # Perturb everything, then restore from the snapshot.
    panel.rows["adr_dollar"].set_enabled(False)
    panel.rows["adr_dollar"].set_value("min_val", 0.50)
    panel.rows["adr_dollar"].set_value("stop_mult", 1.0)
    panel.rows["atr"].set_value("stop_mult", 2.0)
    panel.rows["adr"].set_value("lookback", 7)

    panel.from_dict(snap)
    assert panel.rows["adr_dollar"].is_enabled() is True
    assert panel.rows["adr_dollar"].value("min_val") == pytest.approx(2.0)
    assert panel.rows["adr_dollar"].value("stop_mult") == pytest.approx(1.8)
    assert panel.rows["atr"].value("stop_mult") == pytest.approx(4.0)
    assert panel.rows["adr"].value("lookback") == 20
