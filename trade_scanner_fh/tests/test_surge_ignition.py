"""Tests for the ignition surge mode — pins the catalyst-bar
re-anchoring on top of the trend-continuous rally detector.

Ignition builds on top of `surge_trend_continuous`:
  1. Find the rally bounds via the same drawdown-gated detector.
  2. Walk forward inside the rally; the START is re-anchored to the
     first bar whose day-over-day close gain >= `min_pct` AND whose
     volume >= `vol_mult` × median of the prior 20 days' volumes.
  3. If no qualifying ignition bar exists, fall back to the trend
     start (non-strict — same rallies pass; only the start moves).

The peak_pct is recomputed from ignition close to peak so the
reported % reflects the post-ignition gain.
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from trade_scanner_fh import indicators, scanner
from trade_scanner_fh.scanner import ScanParams


def _ohlcv(closes: list[float], volumes: list[float]) -> pd.DataFrame:
    """OHLCV frame with closes + volumes, opens = closes (no gaps),
    highs/lows ±1%."""
    assert len(closes) == len(volumes)
    idx = pd.bdate_range(start="2025-01-01", periods=len(closes))
    return pd.DataFrame({
        "Open":   closes,
        "High":   [c * 1.005 for c in closes],
        "Low":    [c * 0.995 for c in closes],
        "Close":  closes,
        "Volume": volumes,
    }, index=idx)


# ──────────────────────────────────────────────────────────────────────
# Core: ignition re-anchoring
# ──────────────────────────────────────────────────────────────────────

def test_ignition_reanchors_to_high_volume_up_day():
    """Bars 0..4 drift sideways at $100. Bar 5 is the catalyst:
    price closes 8% up on 5x median volume. Bars 6..10 trend higher.
    Trend mode would call bar 0 the start; ignition must call bar 5
    the start."""
    closes  = [100, 100, 100, 100, 100, 108, 115, 125, 140, 155, 170]
    #          0    1    2    3    4    5(IG) 6    7    8    9    10(peak)
    volumes = [1_000_000] * 5 + [5_000_000] + [1_500_000] * 5

    df = _ohlcv(closes, volumes)
    pct, s, e = indicators.surge_ignition(
        df, max_drawdown_pct=25.0, vol_mult=2.0, min_pct=5.0,
    )
    assert s == df.index[5].date()
    assert e == df.index[-1].date()
    # peak_pct measured from ignition close (108) to peak (170)
    assert pct == pytest.approx((170 - 108) / 108 * 100, abs=0.001)


def test_ignition_volume_gate_blocks_low_volume_up_day():
    """Same shape but the "catalyst" bar's volume is BELOW threshold
    (1.2x median, threshold 2.0x). Bar 5 is rejected as ignition;
    no later bar qualifies (all 1.5x volume), so the function falls
    back to the trend-continuous start (bar 0)."""
    closes  = [100, 100, 100, 100, 100, 108, 115, 125, 140, 155, 170]
    volumes = [1_000_000] * 5 + [1_200_000] + [1_500_000] * 5

    df = _ohlcv(closes, volumes)
    pct, s, e = indicators.surge_ignition(
        df, max_drawdown_pct=25.0, vol_mult=2.0, min_pct=5.0,
    )
    # Fallback: same as trend-continuous result.
    base = indicators.surge_trend_continuous(df, max_drawdown_pct=25.0)
    assert s == base[1]
    assert e == base[2]
    assert pct == pytest.approx(base[0], abs=0.001)


def test_ignition_pct_gate_blocks_small_up_day():
    """Bar 5's volume IS 5x but its close is only +2% — below the 5%
    pct gate. No later bar qualifies on % either. Falls back to
    trend start."""
    closes  = [100, 100, 100, 100, 100, 102, 104, 106, 108, 110, 112]
    volumes = [1_000_000] * 5 + [5_000_000] + [1_500_000] * 5

    df = _ohlcv(closes, volumes)
    pct, s, e = indicators.surge_ignition(
        df, max_drawdown_pct=25.0, vol_mult=2.0, min_pct=5.0,
    )
    base = indicators.surge_trend_continuous(df, max_drawdown_pct=25.0)
    assert s == base[1]
    assert pct == pytest.approx(base[0], abs=0.001)


def test_ignition_picks_first_qualifying_bar_not_largest():
    """Two qualifying bars (5 and 7). Ignition must pick bar 5 (the
    earliest), not bar 7 (the largest %)."""
    closes  = [100, 100, 100, 100, 100, 108, 110, 130, 145, 160, 175]
    volumes = [1_000_000] * 5 + [5_000_000, 1_500_000, 6_000_000] + [1_500_000] * 3

    df = _ohlcv(closes, volumes)
    pct, s, _ = indicators.surge_ignition(
        df, max_drawdown_pct=25.0, vol_mult=2.0, min_pct=5.0,
    )
    assert s == df.index[5].date()
    assert pct == pytest.approx((175 - 108) / 108 * 100, abs=0.001)


def test_ignition_no_rally_returns_nan():
    """When trend-continuous finds no rally (peak at bar 0), ignition
    returns the same NaN/None tuple — no fallback to invent a start."""
    closes  = [200, 180, 160, 140, 120, 100]
    volumes = [10_000_000] * 6
    df = _ohlcv(closes, volumes)
    pct, s, e = indicators.surge_ignition(
        df, max_drawdown_pct=25.0, vol_mult=2.0, min_pct=5.0,
    )
    assert np.isnan(pct)
    assert s is None and e is None


def test_ignition_falls_back_when_no_volume_column():
    """A frame missing the Volume column can't run ignition logic.
    Fall back to the trend result so the rally still surfaces."""
    closes = [100, 105, 110, 115, 120]
    idx = pd.bdate_range(start="2025-01-01", periods=len(closes))
    df = pd.DataFrame({
        "Open": closes, "High": closes, "Low": closes, "Close": closes,
    }, index=idx)
    pct, s, e = indicators.surge_ignition(df, max_drawdown_pct=25.0)
    base = indicators.surge_trend_continuous(df, max_drawdown_pct=25.0)
    assert s == base[1]
    assert pct == pytest.approx(base[0], abs=0.001)


def test_ignition_threshold_tightening_pushes_start_later():
    """Two candidate up-days both meet the default thresholds. With
    vol_mult tightened past the first candidate's volume but still
    under the second's, the start advances to the second."""
    # Bar 5 = 3x volume + 6%. Bar 7 = 6x volume + 8%.
    closes  = [100, 100, 100, 100, 100, 106, 108, 117, 125, 140, 160]
    volumes = [1_000_000] * 5 + [3_000_000, 1_200_000, 6_000_000] + [1_200_000] * 3

    df = _ohlcv(closes, volumes)

    # Loose gate (2x vol, 5%) → bar 5 qualifies first
    _, s_loose, _ = indicators.surge_ignition(
        df, max_drawdown_pct=25.0, vol_mult=2.0, min_pct=5.0,
    )
    assert s_loose == df.index[5].date()

    # Tight gate (4x vol, 5%) → bar 5 is now BELOW threshold; bar 7 wins
    _, s_tight, _ = indicators.surge_ignition(
        df, max_drawdown_pct=25.0, vol_mult=4.0, min_pct=5.0,
    )
    assert s_tight == df.index[7].date()


# ──────────────────────────────────────────────────────────────────────
# ScanParams + scanner dispatch
# ──────────────────────────────────────────────────────────────────────

def test_scanparams_has_ignition_defaults():
    p = ScanParams()
    assert p.surge_ignition_vol_mult == 2.0
    assert p.surge_ignition_min_pct == 5.0


def test_scanner_dispatches_ignition_mode(tmp_path, monkeypatch):
    """A scan with surge_mode='ignition' must call surge_ignition,
    and the resulting Surge Start row reflects the catalyst bar (not
    the leading sideways drift)."""
    from trade_scanner_fh import config as cfg
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)
    monkeypatch.setattr(cfg, "PARQUET_DIR", tmp_path / "ohlcv")
    cfg.PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # 30 sideways bars then a clear catalyst at bar 30 (+10% on 5x vol)
    n_pre = 30
    pre_closes = [10.0] * n_pre
    pre_vols   = [500_000] * n_pre
    rally_closes = [11.0, 12.0, 13.0, 14.0, 15.0, 16.0]
    rally_vols   = [3_000_000, 800_000, 800_000, 800_000, 800_000, 800_000]
    closes = pre_closes + rally_closes
    vols   = pre_vols + rally_vols
    idx = pd.bdate_range(start="2025-09-01", periods=len(closes))
    df = pd.DataFrame({
        "Open": closes, "High": closes, "Low": closes,
        "Close": closes, "Volume": vols,
    }, index=idx)
    df.to_parquet(cfg.PARQUET_DIR / "TKR.parquet")

    p = ScanParams(
        start_date=date(2025, 9, 1), end_date=date(2025, 12, 31),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        surge_enabled=True, surge_mode="ignition",
        surge_min_pct=20.0, surge_max_drawdown_pct=25.0,
        surge_ignition_vol_mult=2.0, surge_ignition_min_pct=5.0,
    )
    result = scanner.run_scan(["TKR"], p)
    assert not result.results_df.empty
    row = result.results_df.iloc[0]
    # Ignition % is from catalyst (close=11) to peak (16) = ~45.45%
    assert row["surge_pct"] == pytest.approx(
        (16.0 - 11.0) / 11.0 * 100.0, abs=0.5,
    )
    # Surge start = the catalyst bar (idx[30]), NOT bar 0
    expected_start = idx[n_pre].date()
    assert pd.Timestamp(row["surge_start_date"]).date() == expected_start


# ──────────────────────────────────────────────────────────────────────
# UI greyout — ignition adds two new mode-dependent fields
# ──────────────────────────────────────────────────────────────────────

def test_panel_ignition_mode_enables_vol_mult_and_ig_min_pct(_qapp):
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    panel = IndicatorPanel()
    surge = panel.rows["surge"]

    days = surge.spinboxes["days"]
    max_dd = surge.spinboxes["max_dd"]
    vol_mult = surge.spinboxes["vol_mult"]
    ig_min_pct = surge.spinboxes["ig_min_pct"]

    # trend (default): max_dd on, others off
    assert max_dd.isEnabled()
    assert not days.isEnabled()
    assert not vol_mult.isEnabled()
    assert not ig_min_pct.isEnabled()

    # ignition: max_dd + vol_mult + ig_min_pct on, days off
    surge.set_value("mode", "ignition")
    assert max_dd.isEnabled()
    assert vol_mult.isEnabled()
    assert ig_min_pct.isEnabled()
    assert not days.isEnabled()

    # close: only days on
    surge.set_value("mode", "close")
    assert days.isEnabled()
    assert not max_dd.isEnabled()
    assert not vol_mult.isEnabled()
    assert not ig_min_pct.isEnabled()

    # high_low: only days on
    surge.set_value("mode", "high_low")
    assert days.isEnabled()
    assert not vol_mult.isEnabled()

    # back to trend
    surge.set_value("mode", "trend")
    assert max_dd.isEnabled()
    assert not vol_mult.isEnabled()


def test_panel_round_trip_preserves_ignition_settings(_qapp):
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    panel = IndicatorPanel()
    panel.rows["surge"].set_enabled(True)
    panel.rows["surge"].set_value("mode", "ignition")
    panel.rows["surge"].set_value("vol_mult", 3.5)
    panel.rows["surge"].set_value("ig_min_pct", 8.0)
    snap = panel.to_dict()

    panel.rows["surge"].set_enabled(False)
    panel.rows["surge"].set_value("mode", "trend")
    panel.rows["surge"].set_value("vol_mult", 2.0)
    panel.rows["surge"].set_value("ig_min_pct", 5.0)
    panel.from_dict(snap)

    assert panel.rows["surge"].is_enabled()
    assert panel.rows["surge"].value("mode") == "ignition"
    assert panel.rows["surge"].value("vol_mult") == pytest.approx(3.5)
    assert panel.rows["surge"].value("ig_min_pct") == pytest.approx(8.0)
    # And greyout reflects the loaded mode (vol_mult should be enabled)
    assert panel.rows["surge"].spinboxes["vol_mult"].isEnabled()
    assert panel.rows["surge"].spinboxes["ig_min_pct"].isEnabled()


def test_panel_loading_legacy_preset_still_works_with_new_fields(_qapp):
    """A v3-era preset that has no `vol_mult` or `ig_min_pct` keys
    must load without crashing — the ignition fields fall back to
    their defaults."""
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    panel = IndicatorPanel()
    legacy = {
        "surge": {
            "enabled": True,
            "min_pct": 40.0,
            "days": 7,
            "mode": "trend",
            "max_dd": 25.0,
            # Note: no vol_mult / ig_min_pct
        }
    }
    panel.from_dict(legacy)
    assert panel.rows["surge"].is_enabled()
    assert panel.rows["surge"].value("mode") == "trend"
    # New fields exist with default values
    assert panel.rows["surge"].value("vol_mult") == pytest.approx(2.0)
    assert panel.rows["surge"].value("ig_min_pct") == pytest.approx(5.0)


def test_build_scan_params_reads_ignition_fields(_qapp):
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    panel = IndicatorPanel()
    panel.rows["surge"].set_enabled(True)
    panel.rows["surge"].set_value("mode", "ignition")
    panel.rows["surge"].set_value("vol_mult", 4.0)
    panel.rows["surge"].set_value("ig_min_pct", 7.5)

    p = panel.build_scan_params(date(2025, 1, 1), date(2025, 12, 31))
    assert p.surge_mode == "ignition"
    assert p.surge_ignition_vol_mult == pytest.approx(4.0)
    assert p.surge_ignition_min_pct == pytest.approx(7.5)


# ──────────────────────────────────────────────────────────────────────
# Filter-window width cap removed
# ──────────────────────────────────────────────────────────────────────

def test_indicator_panel_has_no_max_width_cap(_qapp):
    """Regression: the 720px ceiling truncated long surge mode labels
    on wide monitors. After removal, the panel should only constrain
    its MIN width — max stays at Qt's default sentinel (16777215).

    The Qt-default 16777215 == QWIDGETSIZE_MAX. Anything that big
    means 'no cap'. We just assert it's not at the old 720 value.
    """
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    panel = IndicatorPanel()
    assert panel.maximumWidth() > 1000, (
        "IndicatorPanel re-introduced a max-width cap — drop it so "
        "the splitter can resize the filter pane freely."
    )
