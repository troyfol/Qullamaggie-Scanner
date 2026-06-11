"""Tests for Phase 7 — per-quarter earnings filters and consecutive-beats
mutual-exclusion in scanner._build_filter_stages."""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from trade_scanner_fh import scanner
from trade_scanner_fh.scanner import ScanParams


def _df(**cols) -> pd.DataFrame:
    """Five-ticker test frame with the per-quarter earnings columns
    pre-populated. Any not passed defaults to NaN."""
    base = {
        "symbol":              ["A", "B", "C", "D", "E"],
        "close":               [50.0] * 5,
        "price":               [50.0] * 5,
        "pct_gain":            [10.0] * 5,
        "reported_eps":        [np.nan] * 5,
        "surprise_eps_dollar": [np.nan] * 5,
        "surprise_eps_pct":    [np.nan] * 5,
        "reported_rev":        [np.nan] * 5,
        "surprise_rev_dollar": [np.nan] * 5,
        "surprise_rev_pct":    [np.nan] * 5,
        "consec_eps_beats":    [0] * 5,
        "consec_rev_beats":    [0] * 5,
    }
    base.update(cols)
    return pd.DataFrame(base)


def _params() -> ScanParams:
    """ScanParams with everything OFF so each test can enable one filter."""
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


def _apply(stages, df):
    """Run a list of (name, fn) stages over df, fill NaN→False, return df."""
    cur = df.copy()
    for _, fn in stages:
        mask = fn(cur).fillna(False)
        cur = cur[mask].copy()
    return cur


# ──────────────────────────────────────────────────────────────────────
# Single-quarter filters
# ──────────────────────────────────────────────────────────────────────

def test_reported_eps_min_filter():
    p = _params()
    p.reported_eps_enabled = True
    p.reported_eps_min = 1.5
    df = _df(reported_eps=[0.5, 1.0, 1.5, 2.0, 3.0])
    out = _apply(scanner._build_filter_stages(p), df)
    assert list(out["symbol"]) == ["C", "D", "E"]


def test_surprise_eps_pct_filter():
    p = _params()
    p.surprise_eps_pct_enabled = True
    p.surprise_eps_pct_min = 5.0
    df = _df(surprise_eps_pct=[-1.0, 4.99, 5.0, 5.01, 100.0])
    out = _apply(scanner._build_filter_stages(p), df)
    # Strict >= 5.0
    assert list(out["symbol"]) == ["C", "D", "E"]


def test_earnings_data_only_drops_nan_rows_else_passes_them():
    """earnings_data_only=False (default) → NaN rows pass earnings-data
    filters; earnings_data_only=True → NaN rows fail. Replaces the
    per-filter `*_include_no_data` mechanism with one global toggle.
    NaN-safe at both branches — never crashes on missing data."""
    p = _params()
    p.reported_eps_enabled = True
    p.reported_eps_min = 1.0
    df = _df(reported_eps=[np.nan, np.nan, 0.5, 1.0, 2.0])

    # Default: earnings_data_only=False → NaN rows pass + values >= 1.0
    p.earnings_data_only = False
    out = _apply(scanner._build_filter_stages(p), df)
    assert set(out["symbol"]) == {"A", "B", "D", "E"}

    # earnings_data_only=True → NaN rows fail (only valid >= 1.0 pass)
    p.earnings_data_only = True
    out = _apply(scanner._build_filter_stages(p), df)
    assert set(out["symbol"]) == {"D", "E"}


def test_earnings_dates_filter_respects_data_implies_date_invariant():
    """A row with NaN days_since_er but real earnings DATA values
    must NOT be dropped by the dates filter (data implies date).
    Verifies the dates ⊇ data invariant on the funnel side."""
    import pandas as pd

    p = _params()
    p.days_since_earnings_enabled = True
    p.days_since_min = 0
    p.days_since_max = 90
    p.earnings_dates_only = True  # NaN days fails by default

    # B has NaN date but real EPS data → must pass via the
    # data-implies-date escape hatch.
    df = pd.DataFrame({
        "symbol": ["A", "B", "C", "D"],
        "days_since_er": [30, np.nan, np.nan, 200],
        "reported_eps": [np.nan, 1.5, np.nan, 1.0],
    })

    out = _apply(scanner._build_filter_stages(p), df)
    # A passes (in range), B passes (NaN date but has data),
    # C dropped (NaN date AND no data), D dropped (out of range).
    assert set(out["symbol"]) == {"A", "B"}


def test_earnings_data_only_does_not_affect_dates_filter():
    """earnings_data_only=True should only gate the data filters, not
    the days_since/until ones (those are gated by earnings_dates_only)."""
    import pandas as pd

    p = _params()
    p.days_since_earnings_enabled = True
    p.days_since_min = 0
    p.days_since_max = 90
    p.earnings_data_only = True   # data-only on
    p.earnings_dates_only = False  # dates-only off → NaN dates pass

    df = pd.DataFrame({
        "symbol": ["A", "B"],
        "days_since_er": [30, np.nan],
    })
    out = _apply(scanner._build_filter_stages(p), df)
    assert set(out["symbol"]) == {"A", "B"}


def test_revenue_filters_use_dollar_M_units():
    p = _params()
    p.reported_rev_enabled = True
    p.reported_rev_min = 100.0  # $M
    df = _df(reported_rev=[50.0, 99.0, 100.0, 200.0, 1000.0])
    out = _apply(scanner._build_filter_stages(p), df)
    assert list(out["symbol"]) == ["C", "D", "E"]


# ──────────────────────────────────────────────────────────────────────
# §7.2 mutual exclusion — beats filter bypasses individual EPS / Rev
# ──────────────────────────────────────────────────────────────────────

def test_eps_beats_bypasses_individual_eps_filters():
    """When Consecutive EPS Beats is ON, the three individual EPS
    filters MUST NOT appear in the stage list — they're advisory only."""
    p = _params()
    p.reported_eps_enabled = True
    p.surprise_eps_dollar_enabled = True
    p.surprise_eps_pct_enabled = True
    p.consec_eps_beats_enabled = True
    p.consec_eps_beats_min = 2

    stage_names = [n for n, _ in scanner._build_filter_stages(p)]
    assert not any("Reported EPS" in n for n in stage_names)
    assert not any("Surprise EPS $" in n for n in stage_names)
    assert not any("Surprise EPS %" in n for n in stage_names)
    assert any("Consec EPS Beats" in n for n in stage_names)


def test_rev_beats_bypasses_individual_rev_filters():
    p = _params()
    p.reported_rev_enabled = True
    p.surprise_rev_dollar_enabled = True
    p.surprise_rev_pct_enabled = True
    p.consec_rev_beats_enabled = True
    p.consec_rev_beats_min = 2

    stage_names = [n for n, _ in scanner._build_filter_stages(p)]
    assert not any("Reported Rev" in n for n in stage_names)
    assert not any("Surprise Rev $" in n for n in stage_names)
    assert not any("Surprise Rev %" in n for n in stage_names)
    assert any("Consec Rev Beats" in n for n in stage_names)


def test_eps_beats_does_not_bypass_rev_filters():
    """Crossover guard: turning on EPS beats must NOT bypass the
    individual revenue filters — the §7.2 rules are scoped per-metric."""
    p = _params()
    p.consec_eps_beats_enabled = True
    p.consec_eps_beats_min = 2
    p.reported_rev_enabled = True
    p.reported_rev_min = 100.0

    stage_names = [n for n, _ in scanner._build_filter_stages(p)]
    assert any("Reported Rev" in n for n in stage_names)
    assert any("Consec EPS Beats" in n for n in stage_names)


# ──────────────────────────────────────────────────────────────────────
# Consecutive Beats filter logic
# ──────────────────────────────────────────────────────────────────────

def test_consec_eps_beats_min_filter():
    p = _params()
    p.consec_eps_beats_enabled = True
    p.consec_eps_beats_min = 3
    df = _df(consec_eps_beats=[0, 1, 2, 3, 5])
    out = _apply(scanner._build_filter_stages(p), df)
    assert list(out["symbol"]) == ["D", "E"]


def test_consec_rev_beats_min_filter():
    p = _params()
    p.consec_rev_beats_enabled = True
    p.consec_rev_beats_min = 4
    df = _df(consec_rev_beats=[0, 1, 3, 4, 10])
    out = _apply(scanner._build_filter_stages(p), df)
    assert list(out["symbol"]) == ["D", "E"]


def test_both_beats_active_uses_both_stages():
    """Both EPS + Rev beats filters checked → both stages emit."""
    p = _params()
    p.consec_eps_beats_enabled = True
    p.consec_eps_beats_min = 2
    p.consec_rev_beats_enabled = True
    p.consec_rev_beats_min = 3
    df = _df(
        consec_eps_beats=[0, 2, 2, 5, 5],
        consec_rev_beats=[5, 0, 3, 2, 10],
    )
    out = _apply(scanner._build_filter_stages(p), df)
    # Need EPS >= 2 AND Rev >= 3:
    #   A: eps=0   → fail
    #   B: rev=0   → fail
    #   C: ✓
    #   D: rev=2   → fail
    #   E: ✓
    assert set(out["symbol"]) == {"C", "E"}


def test_consec_beats_missing_column_fails_all():
    """When consec_eps_beats column is absent (no Zacks history loaded),
    the filter must fail every row — the user explicitly asked for a
    streak so 'no data' = miss."""
    p = _params()
    p.consec_eps_beats_enabled = True
    p.consec_eps_beats_min = 1
    df = pd.DataFrame({"symbol": ["A", "B"], "close": [10.0, 20.0],
                       "price": [10.0, 20.0], "pct_gain": [5.0, 5.0]})
    out = _apply(scanner._build_filter_stages(p), df)
    assert out.empty


# ──────────────────────────────────────────────────────────────────────
# IndicatorPanel round-trip (§11 §7 verification step)
# ──────────────────────────────────────────────────────────────────────

@pytest.fixture
def panel(_qapp):
    """Fresh IndicatorPanel for each test."""
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    p = IndicatorPanel()
    yield p
    p.deleteLater()


def test_indicator_panel_round_trip_preserves_phase7_fields(panel):
    """Set values, snapshot via to_dict(), restore via from_dict() —
    every Phase 7 field must come back identical. Note: per-row
    `include_no_data` was removed in v3 in favor of the global
    earnings toolbar toggles (`earnings_dates_only` /
    `earnings_data_only` in v4) — managed by MainWindow, not
    IndicatorPanel."""
    panel.rows["reported_eps"].set_enabled(True)
    panel.rows["reported_eps"].set_value("min_val", 1.25)
    panel.rows["surprise_eps_pct"].set_enabled(True)
    panel.rows["surprise_eps_pct"].set_value("min_val", 7.5)
    panel.rows["consec_rev_beats"].set_enabled(True)
    panel.rows["consec_rev_beats"].set_value("min_count", 4)
    panel.rows["consec_rev_beats"].set_value("threshold_pct", 2.5)

    snap = panel.to_dict()

    # Reset to defaults, then restore
    panel.rows["reported_eps"].set_enabled(False)
    panel.rows["reported_eps"].set_value("min_val", 0.0)
    panel.rows["surprise_eps_pct"].set_enabled(False)
    panel.rows["consec_rev_beats"].set_enabled(False)
    panel.rows["consec_rev_beats"].set_value("min_count", 3)

    panel.from_dict(snap)

    assert panel.rows["reported_eps"].is_enabled()
    assert panel.rows["reported_eps"].value("min_val") == pytest.approx(1.25)
    assert panel.rows["surprise_eps_pct"].is_enabled()
    assert panel.rows["surprise_eps_pct"].value("min_val") == pytest.approx(7.5)
    assert panel.rows["consec_rev_beats"].is_enabled()
    assert panel.rows["consec_rev_beats"].value("min_count") == 4
    assert panel.rows["consec_rev_beats"].value("threshold_pct") == pytest.approx(2.5)


def test_eps_beats_toggle_locks_individual_eps_filter_inputs(panel):
    """Checking Consecutive EPS Beats locks the FILTER inputs of the
    three individual EPS rows (toggle + threshold spinboxes), but
    Display-Only stays clickable so the user can still surface those
    values alongside the active beats filter (Phase 8 §8.5).
    Unchecking re-enables the filter inputs."""
    # Start: filter inputs interactable
    for k in ("reported_eps", "surprise_eps_dollar", "surprise_eps_pct"):
        row = panel.rows[k]
        assert row.toggle.isEnabled()
        for sb in row.spinboxes.values():
            assert sb.isEnabled()
        # Display-only checkbox always interactable when supported
        assert row.display_only is not None
        assert row.display_only.isEnabled()

    panel.rows["consec_eps_beats"].set_enabled(True)
    for k in ("reported_eps", "surprise_eps_dollar", "surprise_eps_pct"):
        row = panel.rows[k]
        assert not row.toggle.isEnabled()
        for sb in row.spinboxes.values():
            assert not sb.isEnabled()
        # CRITICAL: Display-Only stays clickable while beats is active
        assert row.display_only.isEnabled()
    # Rev rows should still have filter inputs interactable
    assert panel.rows["reported_rev"].toggle.isEnabled()

    panel.rows["consec_eps_beats"].set_enabled(False)
    for k in ("reported_eps", "surprise_eps_dollar", "surprise_eps_pct"):
        row = panel.rows[k]
        assert row.toggle.isEnabled()
        for sb in row.spinboxes.values():
            assert sb.isEnabled()


def test_rev_beats_toggle_locks_individual_rev_filter_inputs(panel):
    panel.rows["consec_rev_beats"].set_enabled(True)
    for k in ("reported_rev", "surprise_rev_dollar", "surprise_rev_pct"):
        row = panel.rows[k]
        assert not row.toggle.isEnabled()
        for sb in row.spinboxes.values():
            assert not sb.isEnabled()
        assert row.display_only.isEnabled()
    # EPS rows still interactable
    assert panel.rows["reported_eps"].toggle.isEnabled()

    panel.rows["consec_rev_beats"].set_enabled(False)
    for k in ("reported_rev", "surprise_rev_dollar", "surprise_rev_pct"):
        row = panel.rows[k]
        assert row.toggle.isEnabled()
        for sb in row.spinboxes.values():
            assert sb.isEnabled()


def test_beats_filter_toggled_signal_fires(panel):
    """beats_filter_toggled fires with True when EITHER beats checkbox
    becomes active, False when both go off."""
    # Track emissions
    states: list[bool] = []
    panel.beats_filter_toggled.connect(states.append)

    panel.rows["consec_eps_beats"].set_enabled(True)
    assert states[-1] is True

    panel.rows["consec_rev_beats"].set_enabled(True)
    # Still True — both active
    assert states[-1] is True

    panel.rows["consec_eps_beats"].set_enabled(False)
    # Rev still active
    assert states[-1] is True

    panel.rows["consec_rev_beats"].set_enabled(False)
    # Both off
    assert states[-1] is False


def test_from_dict_reapplies_grey_out(panel):
    """After loading a preset where consec_eps_beats was on, the
    individual EPS rows' filter inputs must be locked again — even if
    the rows' own state matched what they would have been organically.
    Phase 8 §8.5: Display-Only stays clickable in the locked state."""
    # Capture a snapshot with EPS beats on
    panel.rows["consec_eps_beats"].set_enabled(True)
    snap = panel.to_dict()

    # Reset everything to off
    panel.rows["consec_eps_beats"].set_enabled(False)
    assert panel.rows["reported_eps"].toggle.isEnabled()  # back unlocked

    # Reload — EPS rows must be filter-locked again
    panel.from_dict(snap)
    for k in ("reported_eps", "surprise_eps_dollar", "surprise_eps_pct"):
        row = panel.rows[k]
        assert not row.toggle.isEnabled()
        # Display-Only remains interactable
        assert row.display_only is not None
        assert row.display_only.isEnabled()
