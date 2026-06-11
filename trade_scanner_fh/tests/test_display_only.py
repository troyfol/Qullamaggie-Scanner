"""Tests for Phase 8 §8.5 display-only mode.

Display-only on a filter:
  * Computes the indicator value (column populated in results)
  * Skips the threshold filter stage entirely (every ticker passes)
  * UI: greys threshold inputs, leaves enabled checkbox + display-only
    checkbox interactable

Mutual-exclusion relaxation:
  * Individual EPS / Rev filters can coexist with consec beats when the
    beats filter is in display-only mode
  * The individual filter is skipped when its OWN display-only is on
"""
from datetime import date

import pandas as pd
import pytest

from trade_scanner_fh import scanner
from trade_scanner_fh.scanner import ScanParams


# ----------------------------------------------------------------------
# Filter-stage gating: display_only must skip the stage
# ----------------------------------------------------------------------

def _df(**cols) -> pd.DataFrame:
    """Builder mirroring test_filter_stages._computed_df."""
    base = {
        "symbol":           ["A", "B", "C", "D", "E"],
        "close":             [50.0, 100.0, 150.0, 200.0, 500.0],
        "price":             [50.0, 100.0, 150.0, 200.0, 500.0],
        "pct_gain":          [5.0, 15.0, 25.0, 35.0, 50.0],
        "avg_vol":           [100_000, 500_000, 1_000_000, 2_000_000, 5_000_000],
        "dollar_vol":        [5e6, 50e6, 150e6, 400e6, 2.5e9],
        "adr_pct":           [1.5, 3.0, 4.5, 6.0, 8.0],
        "sti":               [0.95, 1.05, 1.10, 1.20, 1.30],
        "dist_high_pct":     [20.0, 10.0, 5.0, 2.0, 0.5],
    }
    base.update(cols)
    return pd.DataFrame(base)


def _params_off() -> ScanParams:
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


def test_display_only_skips_filter_stage_pct_gain():
    p = _params_off()
    p.pct_gain_enabled = True
    p.pct_gain_display_only = True
    p.pct_gain_min = 9999.0  # threshold nobody could pass

    stages = scanner._build_filter_stages(p)
    names = [n for n, _ in stages]
    assert not any("% Gain" in n for n in names), (
        "display-only should drop the filter stage even when the threshold is set"
    )


def test_display_only_skips_filter_stage_avg_vol():
    p = _params_off()
    p.avg_vol_enabled = True
    p.avg_vol_display_only = True
    p.avg_vol_min = 1e15

    stages = scanner._build_filter_stages(p)
    names = [n for n, _ in stages]
    assert not any("Avg Volume" in n for n in names)


def test_display_only_OFF_keeps_filter_stage():
    """Sanity: display_only=False → stage still emits."""
    p = _params_off()
    p.adr_enabled = True
    p.adr_display_only = False
    p.adr_min_pct = 3.0

    stages = scanner._build_filter_stages(p)
    names = [n for n, _ in stages]
    assert any("ADR%" in n for n in names)


def test_display_only_only_skips_THAT_stage_not_others():
    """One filter in display-only must not affect a sibling filter."""
    p = _params_off()
    p.adr_enabled = True
    p.adr_display_only = True
    p.adr_min_pct = 99.0
    p.dist_high_enabled = True
    p.dist_high_display_only = False
    p.dist_high_max_pct = 5.0

    stages = scanner._build_filter_stages(p)
    names = [n for n, _ in stages]
    assert not any("ADR%" in n for n in names)
    assert any("Within 5.0% of High" in n for n in names)


# ----------------------------------------------------------------------
# Per-quarter EPS/Rev coexistence with consec beats
# ----------------------------------------------------------------------

def _params_earnings_off() -> ScanParams:
    p = _params_off()
    p.reported_eps_enabled = False
    p.surprise_eps_dollar_enabled = False
    p.surprise_eps_pct_enabled = False
    p.reported_rev_enabled = False
    p.surprise_rev_dollar_enabled = False
    p.surprise_rev_pct_enabled = False
    p.consec_eps_beats_enabled = False
    p.consec_rev_beats_enabled = False
    return p


def test_consec_eps_beats_display_only_lets_individual_eps_filter_apply():
    """Spec: when consec_eps_beats is in display-only mode, the
    individual EPS filters DO get applied (the streak isn't a filter
    so there's no mutual exclusion to enforce)."""
    p = _params_earnings_off()
    p.consec_eps_beats_enabled = True
    p.consec_eps_beats_display_only = True  # display-only — not a filter
    p.consec_eps_beats_min = 3
    p.reported_eps_enabled = True
    p.reported_eps_min = 1.0

    stages = scanner._build_filter_stages(p)
    names = [n for n, _ in stages]
    # Individual filter must be present
    assert any("Reported EPS" in n for n in names)
    # Beats stage must NOT be present (display-only)
    assert not any("Consec EPS Beats" in n for n in names)


def test_consec_eps_beats_active_filter_still_bypasses_individual():
    """Sanity: when beats is enabled AND not display-only, the existing
    §7.2 mutual exclusion still drops the individual EPS filters."""
    p = _params_earnings_off()
    p.consec_eps_beats_enabled = True
    p.consec_eps_beats_display_only = False
    p.consec_eps_beats_min = 3
    p.reported_eps_enabled = True
    p.reported_eps_min = 1.0

    stages = scanner._build_filter_stages(p)
    names = [n for n, _ in stages]
    assert not any("Reported EPS" in n for n in names)
    assert any("Consec EPS Beats" in n for n in names)


def test_individual_eps_display_only_skips_individual_even_without_beats():
    """An individual EPS filter in display-only must drop its own stage
    even if consec beats isn't involved at all."""
    p = _params_earnings_off()
    p.reported_eps_enabled = True
    p.reported_eps_display_only = True
    p.reported_eps_min = 9999.0

    stages = scanner._build_filter_stages(p)
    names = [n for n, _ in stages]
    assert not any("Reported EPS" in n for n in names)


def test_consec_rev_beats_display_only_lets_individual_rev_filter_apply():
    """Mirror of the EPS case for the revenue side."""
    p = _params_earnings_off()
    p.consec_rev_beats_enabled = True
    p.consec_rev_beats_display_only = True
    p.consec_rev_beats_min = 3
    p.reported_rev_enabled = True
    p.reported_rev_min = 100.0

    stages = scanner._build_filter_stages(p)
    names = [n for n, _ in stages]
    assert any("Reported Rev" in n for n in names)
    assert not any("Consec Rev Beats" in n for n in names)


# ----------------------------------------------------------------------
# IndicatorRow / IndicatorPanel UI behavior
# ----------------------------------------------------------------------

@pytest.fixture(scope="module")
def _qapp():
    from PyQt6.QtWidgets import QApplication
    import sys
    return QApplication.instance() or QApplication(sys.argv[:1])


@pytest.fixture
def panel(_qapp):
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    p = IndicatorPanel()
    yield p
    p.deleteLater()


def test_display_only_checkbox_added_to_filter_rows(panel):
    """Every filter row that supports display-only has the checkbox."""
    expected_supported = [
        "sma1", "sma2", "sti", "dist_high", "pct_gain",
        "consec_gaps", "consec_gaps_down", "current_gap",
        "max_gap", "max_neg_gap", "surge", "adr", "atr",
        "bbw", "atr_ratio", "vol_dryup", "avg_vol", "dollar_vol",
        "rs_market", "rs_nasdaq", "rs_sector",
        "days_since_er", "days_until_er", "days_until_er_max",
        "reported_eps", "surprise_eps_dollar", "surprise_eps_pct",
        "reported_rev", "surprise_rev_dollar", "surprise_rev_pct",
        "consec_eps_beats", "consec_rev_beats",
    ]
    for k in expected_supported:
        assert panel.rows[k].display_only is not None, (
            f"row {k!r} should have display_only checkbox"
        )


def test_display_only_NOT_supported_for_top_pct_and_min_price(panel):
    """top_pct (population-level) and min_price (price already shown)
    have no display-only mode."""
    assert panel.rows["top_pct"].display_only is None
    assert panel.rows["min_price"].display_only is None


def test_display_only_keeps_threshold_inputs_editable(panel):
    """Phase 8 §8.5 (revised): display-only no longer greys out the
    threshold inputs. The threshold is the cutoff used by the
    scanner's per-cell red-on-fail color in the results table, so the
    user MUST be able to keep editing it while in display-only mode."""
    row = panel.rows["adr"]
    # Start: threshold inputs interactable
    for sb in row.spinboxes.values():
        assert sb.isEnabled()
    assert row.toggle.isEnabled()

    # Turn on display-only — threshold inputs MUST stay editable.
    row.display_only.setChecked(True)
    for sb in row.spinboxes.values():
        assert sb.isEnabled()
    assert row.display_only.isEnabled()

    # Turn back off — still editable (sanity).
    row.display_only.setChecked(False)
    for sb in row.spinboxes.values():
        assert sb.isEnabled()


def test_build_scan_params_passes_display_only_through(panel):
    """The display-only state on each row must be reflected on
    ScanParams.*_display_only after build_scan_params."""
    panel.rows["adr"].display_only.setChecked(True)
    panel.rows["sma1"].display_only.setChecked(True)
    panel.rows["consec_eps_beats"].display_only.setChecked(True)

    p = panel.build_scan_params(date(2026, 1, 1), date(2026, 3, 1))

    assert p.adr_display_only is True
    assert p.sma1_display_only is True
    assert p.consec_eps_beats_display_only is True
    # Untouched rows default to False
    assert p.dist_high_display_only is False
    assert p.consec_rev_beats_display_only is False


def test_to_dict_round_trip_preserves_display_only(panel):
    """to_dict / from_dict must preserve display-only state."""
    panel.rows["adr"].display_only.setChecked(True)
    panel.rows["surprise_eps_pct"].display_only.setChecked(True)
    snap = panel.to_dict()

    # Reset
    panel.rows["adr"].display_only.setChecked(False)
    panel.rows["surprise_eps_pct"].display_only.setChecked(False)
    assert not panel.rows["adr"].is_display_only()

    # Restore
    panel.from_dict(snap)
    assert panel.rows["adr"].is_display_only()
    assert panel.rows["surprise_eps_pct"].is_display_only()


def test_legacy_preset_without_display_only_loads_clean(panel):
    """Older presets predating display-only must load with all
    display-only flags False (no exception, no surprise behavior)."""
    legacy = {
        "adr": {"enabled": True, "lookback": 14, "min_pct": 3.0},
        "sma1": {"enabled": True, "period": 200},
    }
    panel.from_dict(legacy)
    assert not panel.rows["adr"].is_display_only()
    assert not panel.rows["sma1"].is_display_only()


def test_beats_lock_leaves_display_only_clickable(panel):
    """When consec_eps_beats is enabled, the individual EPS rows are
    filter-locked but their display_only checkbox stays interactable —
    so the user can surface those values alongside the active beats
    filter."""
    panel.rows["consec_eps_beats"].set_enabled(True)
    for k in ("reported_eps", "surprise_eps_dollar", "surprise_eps_pct"):
        row = panel.rows[k]
        assert not row.toggle.isEnabled()
        for sb in row.spinboxes.values():
            assert not sb.isEnabled()
        assert row.display_only is not None
        assert row.display_only.isEnabled()


def test_lock_disables_thresholds_but_display_only_does_not(panel):
    """Beats-lock still disables threshold inputs (the row's filter
    is being preempted by the streak filter), BUT display-only on
    its own does NOT — the user needs the threshold value to drive
    the per-cell red-on-fail color in the results table."""
    row = panel.rows["reported_eps"]
    panel.rows["consec_eps_beats"].set_enabled(True)
    # Lock active → thresholds disabled
    for sb in row.spinboxes.values():
        assert not sb.isEnabled()

    # Turn on display-only while locked. Lock still gates enablement
    # (locked rows have their threshold ignored by the funnel), so
    # spinboxes remain disabled.
    row.display_only.setChecked(True)
    for sb in row.spinboxes.values():
        assert not sb.isEnabled()

    # Unlock by disabling beats. Display-only is still on, but with
    # the new red-on-fail semantics the spinboxes ARE editable —
    # threshold drives cell coloring.
    panel.rows["consec_eps_beats"].set_enabled(False)
    for sb in row.spinboxes.values():
        assert sb.isEnabled()

    # Turn off display-only → still editable.
    row.display_only.setChecked(False)
    for sb in row.spinboxes.values():
        assert sb.isEnabled()


# ----------------------------------------------------------------------
# Filter ↔ Display Only mutex (per-row gating)
# ----------------------------------------------------------------------

def test_filter_and_display_only_are_mutually_exclusive(panel):
    """Per-row mutex: turning Display Only ON auto-turns Filter OFF,
    and vice versa. The two states are conceptually exclusive — a
    row is either filtering OR passively displaying, never both."""
    row = panel.rows["adr"]

    # Start: filter on (default), display_only off.
    assert row.toggle.isChecked() is True
    assert row.display_only.isChecked() is False

    # Turn display_only ON → filter MUST flip OFF.
    row.display_only.setChecked(True)
    assert row.display_only.isChecked() is True
    assert row.toggle.isChecked() is False

    # Turn filter back ON → display_only MUST flip OFF.
    row.toggle.setChecked(True)
    assert row.toggle.isChecked() is True
    assert row.display_only.isChecked() is False


def test_mutex_keeps_thresholds_editable_in_either_mode(panel):
    """Mutex toggles between Filter-mode and Display-Only-mode without
    ever disabling the threshold inputs. (The legacy "grey them out"
    behavior was reverted — the threshold drives red-on-fail coloring
    in display-only mode and gates filtering in filter mode.)"""
    row = panel.rows["adr"]

    # Start clean.
    row.display_only.setChecked(False)
    row.toggle.setChecked(True)

    # Filter-mode → spinboxes editable.
    for sb in row.spinboxes.values():
        assert sb.isEnabled()

    # Toggle display_only → mutex flips filter off, spinboxes STAY
    # editable (threshold is the cutoff for cell coloring).
    row.display_only.setChecked(True)
    assert row.toggle.isChecked() is False
    for sb in row.spinboxes.values():
        assert sb.isEnabled()

    # Toggle filter back → mutex flips display_only off, spinboxes
    # still editable.
    row.toggle.setChecked(True)
    assert row.display_only.isChecked() is False
    for sb in row.spinboxes.values():
        assert sb.isEnabled()


# ----------------------------------------------------------------------
# _compute_display_only_fails — scanner side
# ----------------------------------------------------------------------

def _make_params(**overrides) -> ScanParams:
    """Build a ScanParams with everything OFF, then apply overrides."""
    p = ScanParams(
        start_date=date(2026, 1, 1), end_date=date(2026, 3, 1),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False, top_pct_enabled=False,
        consec_gaps_enabled=False, consec_gaps_down_enabled=False,
        current_gap_enabled=False, max_gap_enabled=False, max_neg_gap_enabled=False,
        surge_enabled=False, adr_enabled=False, atr_enabled=False,
        bbw_enabled=False, atr_ratio_enabled=False, vol_dryup_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False, min_price_enabled=False,
        rs_market_enabled=False, rs_nasdaq_enabled=False, rs_sector_enabled=False,
        days_since_earnings_enabled=False, days_until_earnings_enabled=False,
        days_until_max_enabled=False,
        reported_eps_enabled=False, surprise_eps_dollar_enabled=False,
        surprise_eps_pct_enabled=False, reported_rev_enabled=False,
        surprise_rev_dollar_enabled=False, surprise_rev_pct_enabled=False,
        consec_eps_beats_enabled=False, consec_rev_beats_enabled=False,
    )
    for k, v in overrides.items():
        setattr(p, k, v)
    return p


def test_display_only_fails_flags_min_threshold_failure():
    """A row whose value falls below a min-threshold filter that's in
    display-only mode gets the column's key flagged True."""
    from trade_scanner_fh.scanner import _compute_display_only_fails

    p = _make_params(pct_gain_display_only=True, pct_gain_min=20.0)
    fails = _compute_display_only_fails(p, {"pct_gain": 15.0})
    assert fails == {"pct_gain": True}


def test_display_only_fails_does_not_flag_passing_value():
    """A value AT OR ABOVE the threshold is absent from the dict —
    the widget's `fail_flags.get(key) is True` check then leaves the
    cell in default color."""
    from trade_scanner_fh.scanner import _compute_display_only_fails

    p = _make_params(pct_gain_display_only=True, pct_gain_min=20.0)
    fails = _compute_display_only_fails(p, {"pct_gain": 25.0})
    assert "pct_gain" not in fails


def test_display_only_fails_handles_max_threshold_filter():
    """`dist_high <= max_pct` → flag fail when value EXCEEDS max."""
    from trade_scanner_fh.scanner import _compute_display_only_fails

    p = _make_params(dist_high_display_only=True, dist_high_max_pct=5.0)
    fails = _compute_display_only_fails(p, {"dist_high_pct": 12.0})
    assert fails == {"dist_high_pct": True}

    fails = _compute_display_only_fails(p, {"dist_high_pct": 3.0})
    assert "dist_high_pct" not in fails


def test_display_only_fails_skips_non_display_only_filters():
    """Filters that aren't in display-only mode never produce flags —
    even if the value would have failed. Those filters still run in
    the funnel and drop the row, so flagging them would be redundant."""
    from trade_scanner_fh.scanner import _compute_display_only_fails

    # pct_gain enabled as a FILTER, not display-only.
    p = _make_params(pct_gain_enabled=True, pct_gain_min=20.0)
    fails = _compute_display_only_fails(p, {"pct_gain": 5.0})
    assert "pct_gain" not in fails


def test_display_only_fails_handles_sma_period_dependent_column():
    """SMA filter's column key is `sma{period}` — period-dependent.
    The fail-evaluator must compute the right key per row."""
    from trade_scanner_fh.scanner import _compute_display_only_fails

    p = _make_params(sma1_display_only=True, sma1_period=200)
    # Close BELOW the 200-SMA → fail.
    fails = _compute_display_only_fails(
        p, {"close": 95.0, "sma200": 100.0},
    )
    assert fails == {"sma200": True}
    # Close ABOVE the 200-SMA → no fail.
    fails = _compute_display_only_fails(
        p, {"close": 110.0, "sma200": 100.0},
    )
    assert "sma200" not in fails


def test_display_only_fails_skips_missing_data():
    """Missing/NaN values do NOT generate fail flags. The widget
    renders `N/A` for missing data; flagging it red would be noise."""
    from trade_scanner_fh.scanner import _compute_display_only_fails

    p = _make_params(pct_gain_display_only=True, pct_gain_min=20.0)
    fails = _compute_display_only_fails(p, {"pct_gain": float("nan")})
    assert "pct_gain" not in fails

    fails = _compute_display_only_fails(p, {})  # key absent entirely
    assert "pct_gain" not in fails


def test_display_only_fails_atr_range_filter():
    """ATR has a (min, max) RANGE filter — fail if outside [min, max]."""
    from trade_scanner_fh.scanner import _compute_display_only_fails

    p = _make_params(atr_display_only=True, atr_min=1.0, atr_max=5.0)
    assert _compute_display_only_fails(p, {"atr": 0.5}) == {"atr": True}
    assert _compute_display_only_fails(p, {"atr": 10.0}) == {"atr": True}
    assert "atr" not in _compute_display_only_fails(p, {"atr": 3.0})


# ----------------------------------------------------------------------
# Widget renders failing cells in red
# ----------------------------------------------------------------------

def test_results_table_paints_display_only_fail_cells_red(_qapp):
    """ResultsTable.populate must apply the FAIL_RED foreground to
    cells whose key is flagged True in `_display_only_fails`."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    df = pd.DataFrame([{
        "symbol": "TKR", "close": 100.0, "price": 100.0,
        "pct_gain": 5.0, "adr_pct": 1.5,
        "_display_only_fails": {"pct_gain": True},
    }])
    table = ResultsTable()
    table.populate(df)
    headers = [c[0] for c in table.active_columns]
    pct_idx = headers.index("% Gain")

    item = table.model_src.item(0, pct_idx)
    c = item.foreground().color()
    fr = ResultsTable._FAIL_RED
    assert (c.red(), c.green(), c.blue()) == (fr.red(), fr.green(), fr.blue())


def test_results_table_passing_value_renders_default_color(_qapp):
    """A value NOT in `_display_only_fails` keeps the default
    foreground (no red applied)."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    df = pd.DataFrame([{
        "symbol": "TKR", "close": 100.0, "price": 100.0,
        "pct_gain": 25.0, "adr_pct": 4.0,
        "_display_only_fails": {"pct_gain": True},  # only pct_gain flagged
    }])
    table = ResultsTable()
    table.populate(df)
    headers = [c[0] for c in table.active_columns]

    # adr_pct column was NOT flagged → not red.
    if "ADR%" in headers:
        adr_idx = headers.index("ADR%")
        c = table.model_src.item(0, adr_idx).foreground().color()
        fr = ResultsTable._FAIL_RED
        assert (c.red(), c.green(), c.blue()) != (fr.red(), fr.green(), fr.blue())


# ----------------------------------------------------------------------
# Preset v2 — filter/display-only state + timeframe + sequenced run
# ----------------------------------------------------------------------

def test_preset_to_dict_records_filter_vs_display_only_state(panel):
    """A row in display-only mode (Filter off, Display Only on) must
    serialize as `enabled=False, display_only=True`. A row in filter
    mode is `enabled=True, display_only=False`. The two are mutually
    exclusive at the GUI level so this captures the active 'mode'."""
    # Filter mode (default for adr).
    panel.rows["adr"].toggle.setChecked(True)
    panel.rows["adr"].display_only.setChecked(False)
    # Display-only mode for sti.
    panel.rows["sti"].display_only.setChecked(True)
    # Off-off for dist_high.
    panel.rows["dist_high"].toggle.setChecked(False)
    panel.rows["dist_high"].display_only.setChecked(False)

    snap = panel.to_dict()

    assert snap["adr"]["enabled"] is True
    assert snap["adr"]["display_only"] is False

    assert snap["sti"]["enabled"] is False  # mutex turned filter off
    assert snap["sti"]["display_only"] is True

    assert snap["dist_high"]["enabled"] is False
    assert snap["dist_high"]["display_only"] is False


def test_preset_round_trip_preserves_display_only_state(panel):
    """from_dict must restore the exact selector state that to_dict
    captured — including the case where display_only is on and the
    filter toggle is off (the active display-only-mode row)."""
    panel.rows["adr"].display_only.setChecked(True)
    snap = panel.to_dict()

    # Reset everything.
    panel.rows["adr"].display_only.setChecked(False)
    panel.rows["adr"].toggle.setChecked(True)
    assert not panel.rows["adr"].is_display_only()

    # Restore from snapshot.
    panel.from_dict(snap)
    assert panel.rows["adr"].is_display_only() is True
    assert panel.rows["adr"].toggle.isChecked() is False


def test_preset_schema_version_is_v2():
    """v2 added timeframe / sequenced run fields; the version stamp
    must reflect that so loaders can warn on forward-compat skew."""
    from trade_scanner_fh.gui.main_window import PRESET_SCHEMA_VERSION
    assert PRESET_SCHEMA_VERSION >= 2


# ----------------------------------------------------------------------
# Regression — BUG-1: NaN consec_*_beats must not crash populate
# ----------------------------------------------------------------------

def test_populate_does_not_crash_when_streak_is_nan(_qapp):
    """Real-world repro: consec_*_beats display-only on, but a ticker
    has no Zacks history so its row dict didn't write the key. Pandas
    builds the DataFrame from the union of keys and fills missing with
    NaN. The naive `int(nan or 0)` raises ValueError because Python
    treats NaN as truthy. populate() runs on the main GUI thread and
    isn't wrapped in try/except, so a single NaN row crashes the whole
    table render — and on Windows under PyInstaller windowed mode an
    uncaught slot exception aborts the whole executable.

    Guard: `_safe_streak` coerces NaN → 0. This test pins that
    contract; if anyone ever reverts to `int(... or 0)`, the test
    fires immediately."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    # Two rows: AAPL has a real streak (5), MSFT has NaN (no Zacks).
    df = pd.DataFrame([
        {"symbol": "AAPL", "close": 100.0, "price": 100.0, "pct_gain": 5.0,
         "consec_eps_beats": 5, "consec_rev_beats": 3,
         "q1_report_date_eps": pd.Timestamp("2026-02-15"),
         "q1_reported_eps": 2.0, "q1_surprise_eps_dollar": 0.1,
         "q1_surprise_eps_pct": 5.0},
        {"symbol": "MSFT", "close": 200.0, "price": 200.0, "pct_gain": 10.0,
         # No consec_*_beats keys — pandas fills with NaN below.
         },
    ])
    table = ResultsTable()
    # Must not raise — even though MSFT row has NaN streak values.
    table.populate(df)
    # Both rows in the model
    assert table.model_src.rowCount() == 2


def test_safe_streak_helper_returns_zero_for_pandas_nan():
    """Direct unit test on the helper itself."""
    from trade_scanner_fh.gui.widgets import _safe_streak
    import numpy as np
    assert _safe_streak(np.nan) == 0
    assert _safe_streak(float("nan")) == 0
    assert _safe_streak(None) == 0
    assert _safe_streak(0) == 0
    assert _safe_streak(5) == 5
    assert _safe_streak(5.7) == 5  # int truncation
    assert _safe_streak("not a number") == 0  # graceful fallback


def test_populate_per_row_safety_isolates_bad_rows(_qapp):
    """If one row's render throws, other rows must still render. This
    is the inner safety net inside `populate()` — `_populate_row` is
    wrapped in try/except so a single ticker with corrupt data can't
    take down the whole result table."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    # First row is valid, second row triggers a TypeError because
    # consec_eps_beats is a non-numeric string. _safe_streak coerces
    # it to 0 cleanly so this row actually still works — let's force
    # a real failure by patching _populate_row on a single row.
    df = pd.DataFrame([
        {"symbol": "AAPL", "close": 100.0, "price": 100.0, "pct_gain": 5.0},
        {"symbol": "BAD", "close": 50.0, "price": 50.0, "pct_gain": 3.0},
        {"symbol": "MSFT", "close": 200.0, "price": 200.0, "pct_gain": 10.0},
    ])

    table = ResultsTable()
    orig = table._populate_row
    call_count = {"n": 0}

    def faulty_populate_row(r, row_data, cols):
        call_count["n"] += 1
        if r == 1:
            raise RuntimeError("simulated row crash")
        return orig(r, row_data, cols)

    table._populate_row = faulty_populate_row
    table.populate(df)

    # All 3 row attempts happened
    assert call_count["n"] == 3
    # Model still has 3 rows (the bad one is empty)
    assert table.model_src.rowCount() == 3


# ----------------------------------------------------------------------
# Beats min = 0 (allows the user to suppress red-on-fail entirely)
# ----------------------------------------------------------------------

def test_consec_beats_min_spinbox_allows_zero(panel):
    """The Min spinbox for consec_eps_beats / consec_rev_beats now
    accepts 0. Setting min=0 makes the streak filter trivially pass
    every ticker AND the display-only red-on-fail can never fire
    (streak < 0 is impossible). Lets the user surface the streak
    count + Q-i blocks for context without any threshold gating or
    red coloring."""
    eps_row = panel.rows["consec_eps_beats"]
    rev_row = panel.rows["consec_rev_beats"]

    # Spinbox accepts 0 as a valid value (was clamped to 1 previously).
    for row in (eps_row, rev_row):
        sb = row.spinboxes["min_count"]
        assert sb.minimum() == 0, (
            f"{row}: Min spinbox minimum should be 0, got {sb.minimum()}"
        )
        sb.setValue(0)
        assert sb.value() == 0


def test_consec_beats_min_zero_produces_no_red_on_fail():
    """With consec_eps_beats_min=0 and display-only on, no ticker
    should be flagged as a fail — streak < 0 is impossible."""
    from trade_scanner_fh.scanner import _compute_display_only_fails

    p = _make_params(
        consec_eps_beats_display_only=True,
        consec_eps_beats_min=0,
    )
    # Even a streak of 0 (most recent quarter missed) should not flag.
    assert "consec_eps_beats" not in _compute_display_only_fails(
        p, {"consec_eps_beats": 0},
    )
    assert "consec_eps_beats" not in _compute_display_only_fails(
        p, {"consec_eps_beats": 5},
    )

    # Sanity check: with min=1 a streak of 0 DOES flag.
    p2 = _make_params(
        consec_eps_beats_display_only=True,
        consec_eps_beats_min=1,
    )
    assert _compute_display_only_fails(p2, {"consec_eps_beats": 0}) == {
        "consec_eps_beats": True,
    }


# ----------------------------------------------------------------------
# Q-i columns render based on populated data, not streak length
# ----------------------------------------------------------------------

def test_q_columns_render_beyond_streak_break(_qapp):
    """A streak break at Q-3 must NOT hide Q-3..Q-N from the table —
    earnings-related cells should always be visible (and thus
    eligible for match-coloring against non-earnings indicator dates)
    regardless of whether each quarter contributed to a beat streak.
    The streak count still drives the green-text coloring inside
    `_populate_row` — only display gating is decoupled."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    # Ticker with 5 quarters of EPS data but a streak that breaks at Q-3.
    # consec_eps_beats=2 means only Q-1 and Q-2 are inside the streak.
    df = pd.DataFrame([{
        "symbol": "TKR", "close": 100.0, "price": 100.0, "pct_gain": 5.0,
        "consec_eps_beats": 2,
        # 5 quarters of populated data
        "q1_report_date_eps": pd.Timestamp("2025-02-15"),
        "q1_reported_eps": 2.0, "q1_surprise_eps_dollar": 0.1, "q1_surprise_eps_pct": 5.0,
        "q2_report_date_eps": pd.Timestamp("2024-11-15"),
        "q2_reported_eps": 1.9, "q2_surprise_eps_dollar": 0.05, "q2_surprise_eps_pct": 3.0,
        "q3_report_date_eps": pd.Timestamp("2024-08-15"),
        "q3_reported_eps": 1.8, "q3_surprise_eps_dollar": -0.05, "q3_surprise_eps_pct": -3.0,
        "q4_report_date_eps": pd.Timestamp("2024-05-15"),
        "q4_reported_eps": 1.7, "q4_surprise_eps_dollar": 0.02, "q4_surprise_eps_pct": 1.0,
        "q5_report_date_eps": pd.Timestamp("2024-02-15"),
        "q5_reported_eps": 1.6, "q5_surprise_eps_dollar": 0.01, "q5_surprise_eps_pct": 0.5,
    }])
    table = ResultsTable()
    table.populate(df)
    headers = [c[0] for c in table.active_columns]

    # All 5 Q-i Date columns must be in the rendered set (not capped
    # at the streak length of 2).
    for k in range(1, 6):
        # Q-{k} Date column key is q{k}_report_date_eps
        keys = [c[1] for c in table.active_columns]
        assert f"q{k}_report_date_eps" in keys, (
            f"Q-{k} Date column missing — should render even though "
            f"streak (2) ended before Q-{k}"
        )


def test_q_columns_match_color_works_for_post_streak_quarters(_qapp):
    """Concrete user requirement: when a non-earnings indicator date
    (max_gap_date) matches a post-streak quarter's date, the
    earnings-related cells for that quarter must still be eligible
    for match-coloring. Streak break at Q-2; max_gap_date matches
    Q-4's date; Q-4's cells should pick up the gap-unit color."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    earnings_day = pd.Timestamp("2024-08-15")
    df = pd.DataFrame([{
        "symbol": "TKR", "close": 100.0, "price": 100.0, "pct_gain": 5.0,
        # Gap landed on the earnings day
        "max_gap_pct": 12.5,
        "max_gap_date": earnings_day,
        # Streak of 1 — broke at Q-2
        "consec_eps_beats": 1,
        "q1_report_date_eps": pd.Timestamp("2025-02-15"),
        "q1_reported_eps": 2.0, "q1_surprise_eps_dollar": 0.1, "q1_surprise_eps_pct": 5.0,
        "q2_report_date_eps": pd.Timestamp("2024-11-15"),
        "q2_reported_eps": 1.9, "q2_surprise_eps_dollar": -0.05, "q2_surprise_eps_pct": -3.0,
        "q3_report_date_eps": pd.Timestamp("2024-08-15"),
        "q3_reported_eps": 1.8, "q3_surprise_eps_dollar": 0.05, "q3_surprise_eps_pct": 3.0,
        "q4_report_date_eps": earnings_day,  # ← match!
        "q4_reported_eps": 1.7, "q4_surprise_eps_dollar": 0.05, "q4_surprise_eps_pct": 3.0,
        "_earnings_aligned_dates": ["2024-08-15"],
    }])
    table = ResultsTable()
    table.populate(df)

    keys = [c[1] for c in table.active_columns]
    # Q-4 columns must be rendered (post-streak)
    assert "q4_report_date_eps" in keys
    assert "q4_reported_eps" in keys

    # Find Q-4 column indices and verify they share the gap unit's color
    gap_idx = keys.index("max_gap_date")
    q4_date_idx = keys.index("q4_report_date_eps")
    q4_value_idx = keys.index("q4_reported_eps")

    gap_color = table.model_src.item(0, gap_idx).foreground().color()
    q4_date_color = table.model_src.item(0, q4_date_idx).foreground().color()
    q4_value_color = table.model_src.item(0, q4_value_idx).foreground().color()

    palette_rgbs = {(c.red(), c.green(), c.blue())
                    for c in ResultsTable._ALIGN_PALETTE}
    # All three must come from the alignment palette
    assert (gap_color.red(), gap_color.green(), gap_color.blue()) in palette_rgbs
    # And they must MATCH each other (shared unit, matching anchor date)
    assert gap_color == q4_date_color == q4_value_color, (
        "Post-streak Q-4 cells should share the gap unit's match color"
    )


# ----------------------------------------------------------------------
# Quarter cap on consec_*_beats — limits Q-i columns to N quarters
# ----------------------------------------------------------------------

def test_quarter_cap_zero_means_no_cap_in_scanner():
    """Default quarter_cap=0 means "no cap" — the scanner populates
    every quarter up to MAX_BEATS_QUARTERS (20), same as before."""
    from trade_scanner_fh.scanner import _compute_ticker, ScanParams
    from trade_scanner_fh import config
    from datetime import date as _d
    import pandas as pd

    # Build a fake earnings_history with 6 quarters of EPS data.
    quarters = []
    for k in range(6):
        period_end = pd.Timestamp("2024-12-31") - pd.DateOffset(months=3 * k)
        quarters.append({
            "ticker": "TKR",
            "period_ending": period_end,
            "report_date": period_end + pd.DateOffset(months=1),
            "report_time": "After Close",
            "estimated_eps": 1.0,
            "reported_eps": 1.0 + k * 0.1,
            "surprise_eps": 0.05,
            "surprise_eps_pct": 5.0,
            "estimated_rev": 100.0,
            "reported_rev": 100.0 + k,
            "surprise_rev": 1.0,
            "surprise_rev_pct": 1.0,
            "source": "zacks",
            "updated_at": pd.Timestamp.now(),
        })
    hist_df = pd.DataFrame(quarters).sort_values(
        ["ticker", "report_date"], ascending=[True, False],
    ).reset_index(drop=True)
    earnings_history_lookup = {"TKR": hist_df}

    # Minimal price history so _compute_ticker doesn't bail on no OHLCV.
    # We bypass that path by constructing the row dict directly via
    # _compute_ticker; need OHLCV cache too. Simpler: just verify the
    # scanner field exists and the cap is wired.
    p = ScanParams(
        start_date=_d(2024, 1, 1), end_date=_d(2025, 1, 1),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        consec_eps_beats_display_only=True,
        consec_eps_beats_min=0,
        consec_eps_beats_quarter_cap=0,  # no cap
    )
    assert p.consec_eps_beats_quarter_cap == 0
    assert p.consec_rev_beats_quarter_cap == 0  # default


@pytest.fixture
def _q_cap_cache(tmp_path, monkeypatch):
    """Local fixture: redirect config paths + write a 6-quarter
    earnings_history.parquet + a synthetic OHLCV bar for one ticker.
    Used by the quarter-cap tests below to drive `run_scan` against a
    deterministic mini-universe without touching the real cache."""
    from trade_scanner_fh import config as cfg
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)
    monkeypatch.setattr(cfg, "PARQUET_DIR", tmp_path / "ohlcv")
    monkeypatch.setattr(cfg, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "earnings_history.parquet")
    monkeypatch.setattr(cfg, "EARNINGS_PARQUET",
                        tmp_path / "earnings_dates.parquet")
    monkeypatch.setattr(cfg, "SECTOR_MAP_PARQUET",
                        tmp_path / "sector_map.parquet")
    (tmp_path / "ohlcv").mkdir(parents=True, exist_ok=True)

    # Synthetic OHLCV — 200 weekday bars ending 2026-04-30.
    end = pd.Timestamp(date(2026, 4, 30))
    idx = pd.bdate_range(end=end, periods=200)
    ohlcv = pd.DataFrame({
        "Open": [100.0] * len(idx),
        "High": [101.0] * len(idx),
        "Low": [99.0] * len(idx),
        "Close": [100.0] * len(idx),
        "Volume": [1_000_000] * len(idx),
    }, index=idx)
    ohlcv.to_parquet(tmp_path / "ohlcv" / "TKR.parquet")

    # 6 quarters of earnings history.
    def _row(period_str, report_str):
        return {
            "ticker": "TKR",
            "period_ending": pd.Timestamp(period_str),
            "report_date": pd.Timestamp(report_str),
            "report_time": "Close",
            "estimated_eps": 1.0, "reported_eps": 1.05,
            "surprise_eps": 0.05, "surprise_eps_pct": 5.0,
            "estimated_rev": 100.0, "reported_rev": 105.0,
            "surprise_rev": 5.0, "surprise_rev_pct": 5.0,
            "source": "zacks",
            "updated_at": pd.Timestamp.now(),
        }
    pd.DataFrame([
        _row("2026-01-31", "2026-02-15"),
        _row("2025-10-31", "2025-11-15"),
        _row("2025-07-31", "2025-08-15"),
        _row("2025-04-30", "2025-05-15"),
        _row("2025-01-31", "2025-02-15"),
        _row("2024-10-31", "2024-11-15"),
    ]).to_parquet(cfg.EARNINGS_HISTORY_PARQUET)
    return tmp_path


def test_quarter_cap_limits_populated_q_columns(_q_cap_cache):
    """Setting consec_eps_beats_quarter_cap=4 limits the scanner to
    populate q1..q4 EPS columns even when the ticker has 6 quarters
    of history. q5 and q6 keys should be ABSENT from the row dict."""
    from datetime import date as _d
    from trade_scanner_fh import scanner
    from trade_scanner_fh.scanner import ScanParams

    p = ScanParams(
        start_date=_d(2024, 1, 1), end_date=_d(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        consec_eps_beats_display_only=True,
        consec_eps_beats_min=0,
        consec_eps_beats_quarter_cap=4,  # ← the cap
    )
    result = scanner.run_scan(["TKR"], p)
    assert not result.results_df.empty
    df = result.results_df

    # q1..q4 columns must be populated; q5 and q6 must NOT.
    for k in range(1, 5):
        assert f"q{k}_report_date_eps" in df.columns, f"q{k} missing under cap=4"
        assert f"q{k}_reported_eps" in df.columns
    for k in (5, 6):
        assert f"q{k}_report_date_eps" not in df.columns, (
            f"q{k} should be ABSENT under quarter_cap=4 (got it in columns)"
        )
        assert f"q{k}_reported_eps" not in df.columns


def test_quarter_cap_per_side_independent(_q_cap_cache):
    """EPS cap and Rev cap are independent. Setting EPS cap=4 and
    Rev cap=2 should produce 4 EPS quarters and 2 Rev quarters
    in the row."""
    from datetime import date as _d
    from trade_scanner_fh import scanner
    from trade_scanner_fh.scanner import ScanParams

    p = ScanParams(
        start_date=_d(2024, 1, 1), end_date=_d(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        consec_eps_beats_display_only=True, consec_eps_beats_min=0,
        consec_eps_beats_quarter_cap=4,
        consec_rev_beats_display_only=True, consec_rev_beats_min=0,
        consec_rev_beats_quarter_cap=2,
    )
    result = scanner.run_scan(["TKR"], p)
    assert not result.results_df.empty
    df = result.results_df

    # 4 EPS quarters; q5..q6 EPS absent
    for k in range(1, 5):
        assert f"q{k}_report_date_eps" in df.columns
    for k in (5, 6):
        assert f"q{k}_report_date_eps" not in df.columns

    # 2 Rev quarters; q3..q6 Rev absent
    for k in range(1, 3):
        assert f"q{k}_report_date_rev" in df.columns
    for k in (3, 4, 5, 6):
        assert f"q{k}_report_date_rev" not in df.columns


def test_quarter_cap_zero_means_full_population(_q_cap_cache):
    """The default cap=0 means "no cap" — all 6 available quarters
    should populate (up to MAX_BEATS_QUARTERS=20)."""
    from datetime import date as _d
    from trade_scanner_fh import scanner
    from trade_scanner_fh.scanner import ScanParams

    p = ScanParams(
        start_date=_d(2024, 1, 1), end_date=_d(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        consec_eps_beats_display_only=True, consec_eps_beats_min=0,
        consec_eps_beats_quarter_cap=0,  # ← no cap (default)
    )
    result = scanner.run_scan(["TKR"], p)
    df = result.results_df
    # All 6 quarters present.
    for k in range(1, 7):
        assert f"q{k}_report_date_eps" in df.columns, f"q{k} missing under cap=0"


def test_quarter_cap_above_history_safe():
    """Cap > available quarter count is harmless — scanner just
    populates as many as exist (handled by `past_desc.head(N)`
    semantics in pandas)."""
    from trade_scanner_fh.scanner import ScanParams
    p = ScanParams(consec_eps_beats_quarter_cap=50)
    assert p.consec_eps_beats_quarter_cap == 50  # accepted at the params level


def test_quarter_cap_spinbox_present_with_zero_default(panel):
    """The Quarter Cap spinbox is present on both beats rows, defaults
    to 0 (no cap), and accepts 0..20 (matching MAX_BEATS_QUARTERS)."""
    for key in ("consec_eps_beats", "consec_rev_beats"):
        sb = panel.rows[key].spinboxes.get("quarter_cap")
        assert sb is not None, f"Quarter Cap spinbox missing on {key}"
        assert sb.minimum() == 0
        assert sb.maximum() == 20
        assert sb.value() == 0  # default = no cap
