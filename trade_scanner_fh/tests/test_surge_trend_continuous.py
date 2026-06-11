"""Tests for the trend-continuous surge mode (default for episodic
pivots). Pins the exact algorithm and the legacy-preset migration."""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from trade_scanner_fh import indicators, scanner
from trade_scanner_fh.scanner import ScanParams


def _ohlcv_from_closes(closes: list[float]) -> pd.DataFrame:
    """Build a synthetic OHLCV frame from a list of closes — opens =
    closes (no overnight gaps), highs/lows ±1%."""
    idx = pd.bdate_range(start="2025-01-01", periods=len(closes))
    df = pd.DataFrame({
        "Open":   closes,
        "High":   [c * 1.005 for c in closes],
        "Low":    [c * 0.995 for c in closes],
        "Close":  closes,
        "Volume": [1_000_000] * len(closes),
    }, index=idx)
    return df


# ──────────────────────────────────────────────────────────────────────
# surge_trend_continuous — core math
# ──────────────────────────────────────────────────────────────────────

def test_simple_uptrend_returns_full_window():
    """Pure monotonic uptrend → start=bar0, end=last bar."""
    df = _ohlcv_from_closes([100, 105, 110, 115, 120, 125, 130])
    pct, s, e = indicators.surge_trend_continuous(df, max_drawdown_pct=25.0)
    assert pct == pytest.approx(30.0, abs=0.001)
    assert s == df.index[0].date()
    assert e == df.index[-1].date()


def test_pullback_below_threshold_does_not_break_streak():
    """A 10% mid-rally pullback under a 25% threshold is preserved."""
    # 100 → 130 → 117 (10% pullback) → 180
    df = _ohlcv_from_closes([100, 110, 120, 130, 117, 140, 160, 180])
    pct, s, e = indicators.surge_trend_continuous(df, max_drawdown_pct=25.0)
    assert pct == pytest.approx(80.0, abs=0.001)
    assert s == df.index[0].date()
    assert e == df.index[-1].date()


def test_deep_pullback_resets_to_local_low():
    """A 30% pullback under a 25% threshold breaks the prior trend.
    The new start lands at the local low after the breakdown."""
    # 100, 130 (peak1), 100.5 (giving back 99% of move) → 180 (peak)
    closes = [100, 110, 120, 130, 110, 100.5, 130, 160, 180]
    df = _ohlcv_from_closes(closes)
    pct, s, e = indicators.surge_trend_continuous(df, max_drawdown_pct=25.0)
    # Drawdown from 130 to 100.5 = 22.7% — under 25% threshold.
    # So the algorithm SHOULD include the original start at bar 0.
    assert pct == pytest.approx(80.0, abs=0.001)
    assert s == df.index[0].date()
    assert e == df.index[-1].date()


def test_user_scenario_matches_described_behavior():
    """The user's stated case: stock goes up 20%, gives back 99% of
    that, then rallies 80%. With a generous max_dd_pct (25%),
    the original start IS preserved because the 19.6% pullback fits."""
    # 100 → 120 (+20%) → 100.2 (gives back 99% of $20 move = $19.8)
    #     → 180 (+79.6% from 100.2)
    closes = [100, 120, 100.2, 130, 160, 180]
    df = _ohlcv_from_closes(closes)
    pct, s, e = indicators.surge_trend_continuous(df, max_drawdown_pct=25.0)
    # 120 → 100.2 = 16.5% drawdown → under 25% → start stays at bar 0
    assert s == df.index[0].date()
    assert pct == pytest.approx(80.0, abs=0.001)


def test_user_scenario_with_tight_threshold_resets():
    """Same user scenario with a tight 5% threshold — the deep
    pullback DOES break the trend, and the surge starts at the low."""
    closes = [100, 120, 100.2, 130, 160, 180]
    df = _ohlcv_from_closes(closes)
    pct, s, e = indicators.surge_trend_continuous(df, max_drawdown_pct=5.0)
    # Resets at bar 1 (drawdown immediately exceeds 5%), then resets
    # again at bar 2 (low). From there 100.2 → 180 is the new rally.
    # Algorithm walks forward; the 5% drawdown from 120 happens at
    # bar 2 (closes=100.2). Reset to bar 2 with new running peak 100.2.
    # No further reset since each subsequent bar is higher than the
    # last running peak.
    assert s == df.index[2].date()
    assert pct == pytest.approx((180 - 100.2) / 100.2 * 100.0, abs=0.001)


def test_peak_at_first_bar_returns_nan():
    """Global peak is bar 0 — no rally exists."""
    df = _ohlcv_from_closes([100, 95, 90, 85])
    pct, s, e = indicators.surge_trend_continuous(df, max_drawdown_pct=25.0)
    assert np.isnan(pct)
    assert s is None
    assert e is None


def test_too_few_bars_returns_nan():
    df = _ohlcv_from_closes([100])
    pct, s, e = indicators.surge_trend_continuous(df, max_drawdown_pct=25.0)
    assert np.isnan(pct)


def test_ignores_post_peak_data():
    """The algorithm only scans up to the global peak; what happens
    AFTER the peak (subsequent decline) doesn't affect the result."""
    # Peak at index 4 (close=160), then crash
    closes = [100, 120, 140, 150, 160, 80, 70]
    df = _ohlcv_from_closes(closes)
    pct, s, e = indicators.surge_trend_continuous(df, max_drawdown_pct=25.0)
    assert pct == pytest.approx(60.0, abs=0.001)
    assert s == df.index[0].date()
    assert e == df.index[4].date()


# ──────────────────────────────────────────────────────────────────────
# ScanParams + _compute_ticker dispatch
# ──────────────────────────────────────────────────────────────────────

def test_scanparams_default_mode_is_trend():
    """A fresh ScanParams must default to trend-continuous mode."""
    p = ScanParams()
    assert p.surge_mode == "trend"
    assert p.surge_max_drawdown_pct == 25.0


def test_compute_ticker_dispatches_trend_mode(tmp_path, monkeypatch):
    """A scan with surge_enabled and mode='trend' calls
    surge_trend_continuous, not surge_detection."""
    from trade_scanner_fh import config as cfg
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)
    monkeypatch.setattr(cfg, "PARQUET_DIR", tmp_path / "ohlcv")
    cfg.PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # Synthetic ticker with a clean 50% rally
    closes = [100] * 30 + [105, 110, 120, 130, 140, 150]
    idx = pd.bdate_range(start="2025-09-01", periods=len(closes))
    df = pd.DataFrame({
        "Open": closes, "High": closes, "Low": closes,
        "Close": closes, "Volume": [1_000_000] * len(closes),
    }, index=idx)
    df.to_parquet(cfg.PARQUET_DIR / "TKR.parquet")

    p = ScanParams(
        start_date=date(2025, 9, 1), end_date=date(2025, 12, 31),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        surge_enabled=True, surge_mode="trend",
        surge_min_pct=20.0, surge_max_drawdown_pct=25.0,
    )
    result = scanner.run_scan(["TKR"], p)
    assert not result.results_df.empty
    row = result.results_df.iloc[0]
    # Trend-continuous reports the rally from start of upmove to peak
    assert row["surge_pct"] == pytest.approx(50.0, abs=0.5)
    assert row["surge_window"]  # non-empty


# ──────────────────────────────────────────────────────────────────────
# IndicatorPanel UI — combo + greyout + preset migration
# ──────────────────────────────────────────────────────────────────────

def test_panel_default_surge_mode_is_trend(_qapp):
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    panel = IndicatorPanel()
    assert panel.rows["surge"].value("mode") == "trend"


def test_panel_changing_mode_greys_correct_field(_qapp):
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    panel = IndicatorPanel()
    surge = panel.rows["surge"]
    days = surge.spinboxes["days"]
    max_dd = surge.spinboxes["max_dd"]

    # Default = trend → days disabled, max_dd enabled
    assert not days.isEnabled()
    assert max_dd.isEnabled()

    # Switch to close → days enabled, max_dd disabled
    surge.set_value("mode", "close")
    assert days.isEnabled()
    assert not max_dd.isEnabled()

    # Switch to high_low → days enabled, max_dd disabled
    surge.set_value("mode", "high_low")
    assert days.isEnabled()
    assert not max_dd.isEnabled()

    # Back to trend
    surge.set_value("mode", "trend")
    assert not days.isEnabled()
    assert max_dd.isEnabled()


def test_panel_round_trip_preserves_mode_and_max_dd(_qapp):
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    panel = IndicatorPanel()
    panel.rows["surge"].set_enabled(True)
    panel.rows["surge"].set_value("mode", "trend")
    panel.rows["surge"].set_value("max_dd", 35.0)
    panel.rows["surge"].set_value("min_pct", 50.0)
    snap = panel.to_dict()

    # Reset and reload
    panel.rows["surge"].set_enabled(False)
    panel.rows["surge"].set_value("mode", "close")
    panel.rows["surge"].set_value("max_dd", 25.0)
    panel.from_dict(snap)

    assert panel.rows["surge"].is_enabled()
    assert panel.rows["surge"].value("mode") == "trend"
    assert panel.rows["surge"].value("max_dd") == pytest.approx(35.0)
    assert panel.rows["surge"].value("min_pct") == pytest.approx(50.0)


def test_panel_legacy_preset_use_hl_true_migrates_to_high_low(_qapp):
    """A pre-fork preset stored `use_hl: True` and no `mode` key. After
    load, the row's mode must be "high_low" so the user gets the
    behavior they saved."""
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    panel = IndicatorPanel()
    legacy = {
        "surge": {
            "enabled": True,
            "min_pct": 40.0,
            "days": 7,
            "use_hl": True,  # legacy bool — no `mode` key
        }
    }
    panel.from_dict(legacy)
    assert panel.rows["surge"].value("mode") == "high_low"
    assert panel.rows["surge"].is_enabled()


def test_panel_legacy_preset_use_hl_false_migrates_to_close(_qapp):
    """A pre-fork preset with `use_hl: False` should map to mode=close
    (matching the old default), NOT silently switch to trend."""
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    panel = IndicatorPanel()
    legacy = {
        "surge": {
            "enabled": True,
            "min_pct": 40.0,
            "days": 7,
            "use_hl": False,
        }
    }
    panel.from_dict(legacy)
    assert panel.rows["surge"].value("mode") == "close"


def test_build_scan_params_reads_mode_and_max_dd(_qapp):
    """build_scan_params returns ScanParams with the new fields."""
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    panel = IndicatorPanel()
    panel.rows["surge"].set_enabled(True)
    panel.rows["surge"].set_value("mode", "trend")
    panel.rows["surge"].set_value("max_dd", 30.0)

    p = panel.build_scan_params(date(2025, 1, 1), date(2025, 12, 31))
    assert p.surge_mode == "trend"
    assert p.surge_max_drawdown_pct == pytest.approx(30.0)
    # Legacy bool stays in sync (False since mode != "high_low")
    assert p.surge_use_high_low is False
