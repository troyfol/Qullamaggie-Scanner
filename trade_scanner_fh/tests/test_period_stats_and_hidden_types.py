"""Batch 1: Period Avg / Period Max columns + hide-by-column-type.

Two features, tested together because they meet in `_build_dynamic_columns`:
the Period columns are new output, and the hide control decides what of that
output reaches the table.

The regression this module exists to prevent is `test_hiding_reported_eps_*`
below. Hiding a per-quarter type by dropping columns from the DataFrame would
collapse `n_eps` (derived from which `q*_reported_eps` columns exist) to zero
and take the whole EPS block and the interleave layout with it. Hiding must
therefore happen on the emitted column list, never on the frame.
"""

import os

import numpy as np
import pandas as pd
import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt6.QtWidgets import QApplication  # noqa: E402

from trade_scanner_fh import earnings_series as es  # noqa: E402
from trade_scanner_fh import scanner as sc  # noqa: E402
from trade_scanner_fh.gui import widgets as W  # noqa: E402
from trade_scanner_fh.gui.dialogs import ExcelExportDialog  # noqa: E402


SERIES_PREFIXES = (
    "consec_eps_beats", "consec_rev_beats",
    "consec_eps_growth", "consec_rev_growth",
    "accel_eps_surp", "accel_rev_surp", "accel_eps_yoy", "accel_rev_yoy",
)


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance() or QApplication([])
    yield app


def _points(values, start="2023-03-01"):
    periods = pd.date_range(start, periods=len(values), freq="QS")
    return [
        es.QuarterPoint(value=v, period=p, report_date=p, missing_before=0)
        for v, p in zip(values, periods)
    ]


# ----------------------------------------------------------------------
# Engine — the resolved window
# ----------------------------------------------------------------------

def test_run_series_carries_only_the_qualifying_quarters():
    """The window is the run, not the pool: the two leading misses are
    excluded from both the length and the values."""
    res = es.run_series(
        _points([-5.0, -1.0, 2.0, 4.0, 9.0, 20.0]),
        threshold=0.0, min_count=3, inclusive=False,
    )
    assert res.length == 4
    assert res.values == (2.0, 4.0, 9.0, 20.0)
    assert es.series_avg(res.values) == pytest.approx(8.75)
    assert es.series_max(res.values) == 20.0


def test_zero_length_run_reports_na_not_zero():
    """A pool with no run at all must give NaN, which the table renders as
    N/A. 0.0 would read as a genuine average of zero percent."""
    res = es.run_series(
        _points([-5.0, -3.0, -1.0]),
        threshold=0.0, min_count=3, inclusive=False,
    )
    assert res.length == 0
    assert res.values == ()
    assert np.isnan(es.series_avg(res.values))
    assert np.isnan(es.series_max(res.values))


def test_empty_pool_helpers_are_nan():
    assert np.isnan(es.series_avg(()))
    assert np.isnan(es.series_max(()))


def test_sub_threshold_run_still_reports_a_window():
    """`qualifies=False` still carries its values, so display-only mode can
    show how far the ticker actually got."""
    res = es.run_series(
        _points([-1.0, 5.0, 7.0]),
        threshold=0.0, min_count=10, inclusive=False,
    )
    assert res.qualifies is False
    assert res.length == 2
    assert es.series_avg(res.values) == pytest.approx(6.0)


def test_accelerating_window_is_the_metric_not_the_step():
    """Period Avg on an accel filter averages the metric at each quarter,
    matching the `_vals` column — not the quarter-over-quarter step."""
    res = es.accelerating_series(
        _points([1.0, 6.0, 12.0, 21.0]),
        min_start_pct=0.0, min_step_pct=5.0, min_count=3,
    )
    assert res.values == (1.0, 6.0, 12.0, 21.0)
    assert es.series_avg(res.values) == pytest.approx(10.0)
    # The step mean would be 6.67 and the step max 9.0 — neither appears.
    assert es.series_max(res.values) == 21.0


def test_accelerating_window_starts_at_the_threshold_crossing():
    """Pass 2 of series construction starts the series at the first quarter
    clearing the start threshold; earlier chain members are excluded from
    the average as well as from the length."""
    res = es.accelerating_series(
        _points([2.0, 8.0, 14.0, 20.0]),
        min_start_pct=8.0, min_step_pct=5.0, min_count=3,
    )
    assert res.values == (8.0, 14.0, 20.0)
    assert es.series_avg(res.values) == pytest.approx(14.0)


def test_series_max_of_a_contraction_is_signed():
    """Least-bad quarter, not largest magnitude."""
    assert es.series_max((-30.0, -20.0, -12.0)) == -12.0


def test_window_values_filters_non_finite():
    pts = _points([1.0, float("nan"), 3.0])
    assert es.window_values(pts, 0, 2) == (1.0, 3.0)


# ----------------------------------------------------------------------
# Scanner — columns are populated whenever the filter ran
# ----------------------------------------------------------------------

def test_write_period_stats_always_writes_both_keys():
    row = {}
    sc._write_period_stats(row, "consec_eps_beats", None)
    assert "consec_eps_beats_period_avg" in row
    assert "consec_eps_beats_period_max" in row
    assert np.isnan(row["consec_eps_beats_period_avg"])
    assert np.isnan(row["consec_eps_beats_period_max"])


def test_scanparams_period_thresholds_default_to_off():
    """Both toggles default off, so every pre-existing preset selects
    exactly as it did before these fields existed."""
    p = sc.ScanParams()
    for prefix in SERIES_PREFIXES:
        assert getattr(p, f"{prefix}_period_avg_enabled") is False
        assert getattr(p, f"{prefix}_period_max_enabled") is False
        assert getattr(p, f"{prefix}_period_avg_min") == 0.0
        assert getattr(p, f"{prefix}_period_max_min") == 0.0


def _stage_labels(params):
    stages = sc._build_filter_stages(params)
    return [label for label, _fn in stages]


def test_untoggled_threshold_appends_no_stage():
    """A value sitting in the spinbox does nothing until its box is ticked."""
    p = sc.ScanParams(
        consec_eps_beats_enabled=True, consec_eps_beats_min=3,
        consec_eps_beats_period_avg_min=25.0,
    )
    labels = _stage_labels(p)
    assert not any("Period Avg" in s or "Period Max" in s for s in labels)


def test_zero_threshold_is_expressible_when_toggled_on():
    """The whole reason the toggle replaced the 0-means-off sentinel:
    'the run averaged positive' is a real filter."""
    p = sc.ScanParams(
        consec_eps_beats_enabled=True, consec_eps_beats_min=3,
        consec_eps_beats_period_avg_enabled=True,
        consec_eps_beats_period_avg_min=0.0,
    )
    assert any("Consec EPS Beats Period Avg >= 0%" == s
               for s in _stage_labels(p))


def test_nonzero_threshold_appends_a_stage():
    p = sc.ScanParams(
        consec_eps_beats_enabled=True, consec_eps_beats_min=3,
        consec_eps_beats_period_avg_enabled=True,
        consec_eps_beats_period_avg_min=7.5,
    )
    labels = _stage_labels(p)
    assert any("Consec EPS Beats Period Avg >= 7.5%" == s for s in labels)


def test_threshold_on_a_disabled_filter_is_inert():
    """The Period gate narrows a running filter; it can never start one."""
    p = sc.ScanParams(
        consec_eps_beats_enabled=False,
        consec_eps_beats_period_avg_enabled=True,
        consec_eps_beats_period_avg_min=50.0,
    )
    labels = _stage_labels(p)
    assert not any("Period Avg" in s for s in labels)


def test_threshold_on_a_display_only_filter_is_inert():
    p = sc.ScanParams(
        consec_eps_beats_enabled=True, consec_eps_beats_display_only=True,
        consec_eps_beats_period_avg_enabled=True,
        consec_eps_beats_period_avg_min=50.0,
    )
    labels = _stage_labels(p)
    assert not any("Period Avg" in s for s in labels)


def test_negative_threshold_is_applied():
    """Negatives bound a contraction series."""
    p = sc.ScanParams(
        consec_eps_growth_enabled=True,
        consec_eps_growth_period_max_enabled=True,
        consec_eps_growth_period_max_min=-5.0,
    )
    assert any("Period Max >= -5%" in s for s in _stage_labels(p))


# ----------------------------------------------------------------------
# Column build — the Period columns
# ----------------------------------------------------------------------

def _beats_frame(n_q=3, with_period=True):
    row = {"symbol": "AAA", "consec_eps_beats": 3, "consec_rev_beats": 2}
    if with_period:
        row.update({
            "consec_eps_beats_period_avg": 8.75,
            "consec_eps_beats_period_max": 20.0,
            "consec_rev_beats_period_avg": 3.1,
            "consec_rev_beats_period_max": 4.2,
        })
    for k in range(1, n_q + 1):
        row[f"q{k}_report_date_eps"] = pd.Timestamp("2025-01-15")
        row[f"q{k}_reported_eps"] = 1.0
        row[f"q{k}_surprise_eps_dollar"] = 0.1
        row[f"q{k}_surprise_eps_pct"] = 5.0
        row[f"q{k}_yoy_eps_pct"] = 12.0
        row[f"q{k}_report_date_rev"] = pd.Timestamp("2025-01-15")
        row[f"q{k}_reported_rev"] = 100.0
        row[f"q{k}_surprise_rev_dollar"] = 1.0
        row[f"q{k}_surprise_rev_pct"] = 2.0
        row[f"q{k}_yoy_rev_pct"] = 8.0
    return pd.DataFrame([row])


def _keys(cols):
    return [k for _h, k, _f in cols]


def test_beats_period_columns_sit_next_to_their_counter():
    cols, _, _ = W._build_dynamic_columns(_beats_frame())
    keys = _keys(cols)
    i = keys.index("consec_eps_beats")
    assert keys[i + 1] == "consec_eps_beats_period_avg"
    assert keys[i + 2] == "consec_eps_beats_period_max"


def test_beats_period_columns_absent_when_frame_lacks_them():
    """A scan predating the feature renders the counter alone rather than
    two permanently-N/A columns."""
    cols, _, _ = W._build_dynamic_columns(_beats_frame(with_period=False))
    keys = _keys(cols)
    assert "consec_eps_beats" in keys
    assert "consec_eps_beats_period_avg" not in keys


def test_period_pct_formatter_renders_na_and_sign():
    assert W._fmt_period_pct(float("nan")) == "N/A"
    assert W._fmt_period_pct(None) == "N/A"
    assert W._fmt_period_pct(8.75) == "+8.75%"
    assert W._fmt_period_pct(-3.5) == "-3.50%"


# ----------------------------------------------------------------------
# Hide-by-type — the taxonomy
# ----------------------------------------------------------------------

def test_type_of_quarter_columns_collapses_the_quarter_index():
    assert W.earnings_column_type_of("q1_reported_eps") == "q_reported_eps"
    assert W.earnings_column_type_of("q17_reported_eps") == "q_reported_eps"


def test_eps_and_rev_date_are_distinct_types():
    """Both render a header literally called 'Q-X Date', so one toggle
    covering both would be a silent surprise."""
    assert W.earnings_column_type_of("q1_report_date_eps") == "q_report_date_eps"
    assert W.earnings_column_type_of("q1_report_date_rev") == "q_report_date_rev"


def test_non_earnings_columns_have_no_type():
    for key in ("symbol", "close", "rvol", "atr"):
        assert W.earnings_column_type_of(key) is None


def test_series_columns_group_per_filter():
    """One tick hides a filter's whole output, not one role across every
    filter — the user usually runs a few series filters together."""
    for prefix in SERIES_PREFIXES:
        expected = f"series_{prefix}"
        assert W.earnings_column_type_of(f"{prefix}_period_avg") == expected
        assert W.earnings_column_type_of(f"{prefix}_period_max") == expected
    # The count column uses a different name per family.
    assert W.earnings_column_type_of(
        "consec_eps_beats") == "series_consec_eps_beats"
    assert W.earnings_column_type_of(
        "consec_eps_growth") == "series_consec_eps_growth"
    assert W.earnings_column_type_of(
        "accel_eps_surp_len") == "series_accel_eps_surp"
    # Accel-only span / values ride with their own filter.
    assert W.earnings_column_type_of(
        "accel_rev_yoy_span") == "series_accel_rev_yoy"
    assert W.earnings_column_type_of(
        "accel_rev_yoy_vals") == "series_accel_rev_yoy"


def test_present_types_only_lists_what_the_scan_produced():
    cols, _, _ = W._build_dynamic_columns(_beats_frame())
    present = {tid for tid, _label, _n in
               W.present_earnings_column_types(cols)}
    assert "q_reported_eps" in present
    assert "series_consec_eps_beats" in present
    assert "series_consec_rev_beats" in present
    # No accel filter ran, so its group must not be offered at all.
    assert "series_accel_eps_surp" not in present
    assert "series_accel_rev_yoy" not in present


def test_present_types_counts_every_quarter():
    cols, _, _ = W._build_dynamic_columns(_beats_frame(n_q=4))
    counts = {tid: n for tid, _l, n in W.present_earnings_column_types(cols)}
    assert counts["q_reported_eps"] == 4


# ----------------------------------------------------------------------
# Hide-by-type — THE REGRESSION GUARD
# ----------------------------------------------------------------------

def test_hiding_reported_eps_does_not_collapse_the_eps_block():
    """The bug this feature could easily have shipped.

    `n_eps` is derived from the `q*_reported_eps` columns. If hiding worked
    by dropping them from the frame, every other EPS column would vanish
    too. Hiding one type must remove exactly that type.
    """
    df = _beats_frame(n_q=3)
    cols, n_eps, n_rev = W._build_dynamic_columns(
        df, hidden_types={"q_reported_eps"},
    )
    keys = _keys(cols)
    # The hidden type is gone...
    assert not any(k.endswith("_reported_eps") for k in keys)
    # ...and nothing else in the EPS block went with it.
    for k in range(1, 4):
        assert f"q{k}_report_date_eps" in keys
        assert f"q{k}_surprise_eps_dollar" in keys
        assert f"q{k}_surprise_eps_pct" in keys
        assert f"q{k}_yoy_eps_pct" in keys
    assert "consec_eps_beats" in keys
    # The quarter counts are still computed off the full frame.
    assert n_eps == 3 and n_rev == 3


def test_hiding_reported_eps_does_not_disable_interleave():
    """`use_interleave` requires n_eps > 0 AND n_rev > 0. A collapsed n_eps
    would silently switch the layout back to sequential blocks."""
    df = _beats_frame(n_q=2)
    plain, _, _ = W._build_dynamic_columns(df, interleave_quarters=True)
    hidden, _, _ = W._build_dynamic_columns(
        df, interleave_quarters=True, hidden_types={"q_reported_eps"},
    )
    # Interleaved layout puts both counters up front, then alternates sides.
    assert _keys(hidden)[:2] == _keys(plain)[:2]
    order = [k for k in _keys(hidden) if k.startswith("q")]
    assert order[0].endswith("_eps") and any(k.endswith("_rev") for k in order)
    # Q-1 Rev columns precede Q-2 EPS columns — i.e. still interleaved.
    assert order.index("q1_yoy_rev_pct") < order.index("q2_report_date_eps")


def test_hiding_is_identical_under_both_view_modes():
    """The hidden SET must not depend on the layout flag — only the order."""
    df = _beats_frame(n_q=2)
    for hidden in ({"q_surprise_eps_pct"}, {"series_consec_eps_beats"},
                   {"q_report_date_rev", "q_yoy_eps_pct"}):
        seq, _, _ = W._build_dynamic_columns(
            df, interleave_quarters=False, hidden_types=hidden)
        inter, _, _ = W._build_dynamic_columns(
            df, interleave_quarters=True, hidden_types=hidden)
        assert set(_keys(seq)) == set(_keys(inter)), hidden


def test_hiding_never_removes_an_always_visible_column():
    df = _beats_frame()
    cols, _, _ = W._build_dynamic_columns(
        df, hidden_types=set(W.EARNINGS_COLUMN_TYPE_LABELS),
    )
    keys = set(_keys(cols))
    for k in W._ALWAYS_VISIBLE_KEYS:
        assert k in keys, k


def test_hiding_everything_leaves_a_usable_table():
    df = _beats_frame()
    cols, _, _ = W._build_dynamic_columns(
        df, hidden_types=set(W.EARNINGS_COLUMN_TYPE_LABELS),
    )
    assert "symbol" in _keys(cols)
    assert not any(k.startswith("q1_") for k in _keys(cols))


def test_empty_hidden_set_is_byte_identical_to_no_argument():
    """The default path must be untouched by the feature existing."""
    df = _beats_frame(n_q=3)
    for interleave in (False, True):
        a, na, nr = W._build_dynamic_columns(
            df, interleave_quarters=interleave)
        b, nb, nrb = W._build_dynamic_columns(
            df, interleave_quarters=interleave, hidden_types=frozenset())
        assert _keys(a) == _keys(b)
        assert (na, nr) == (nb, nrb)


def test_unknown_type_id_is_ignored():
    """A preset written by a newer build must not blank the table."""
    df = _beats_frame()
    cols, _, _ = W._build_dynamic_columns(
        df, hidden_types={"q_some_future_column"},
    )
    assert _keys(cols) == _keys(W._build_dynamic_columns(df)[0])


# ----------------------------------------------------------------------
# ResultsTable plumbing
# ----------------------------------------------------------------------

def test_results_table_defaults_to_nothing_hidden(qapp):
    table = W.ResultsTable()
    assert table.hidden_column_types == frozenset()


def test_set_hidden_column_types_invalidates_the_width_cache(qapp):
    table = W.ResultsTable()
    table._cached_column_widths = {"symbol": 80}
    table.set_hidden_column_types({"q_reported_eps"})
    assert table._cached_column_widths == {}
    assert table.hidden_column_types == frozenset({"q_reported_eps"})


def test_set_hidden_column_types_is_a_noop_when_unchanged(qapp):
    table = W.ResultsTable()
    table.set_hidden_column_types({"q_reported_eps"})
    table._cached_column_widths = {"symbol": 80}
    table.set_hidden_column_types({"q_reported_eps"})
    assert table._cached_column_widths == {"symbol": 80}


def test_unfiltered_columns_for_ignores_the_hidden_set(qapp):
    """The menu source must show a hidden type so it can be un-hidden."""
    table = W.ResultsTable()
    table.set_hidden_column_types({"q_reported_eps"})
    cols = table.unfiltered_columns_for(_beats_frame())
    assert any(k.endswith("_reported_eps") for _h, k, _f in cols)


# ----------------------------------------------------------------------
# Export dialog
# ----------------------------------------------------------------------

def _cols(*keys):
    return [(k.replace("_", " ").title(), k, str) for k in keys]


def test_export_dialog_checks_everything_without_prechecked(qapp):
    """Historical contract: no `prechecked` means all ticked."""
    dlg = ExcelExportDialog(_cols("symbol", "close", "rvol"))
    assert set(dlg.selected_keys()) == {"symbol", "close", "rvol"}


def test_export_dialog_honours_prechecked(qapp):
    dlg = ExcelExportDialog(
        _cols("symbol", "close", "rvol"), prechecked={"symbol", "close"},
    )
    assert set(dlg.selected_keys()) == {"symbol", "close"}


def test_export_dialog_bundles_hidden_quarter_columns_separately(qapp):
    """Hidden per-quarter columns get their own unticked bundle instead of
    joining the all-or-nothing visible bundle."""
    cols = _cols(
        "symbol",
        "q1_reported_eps", "q2_reported_eps",
        "q1_surprise_eps_pct", "q2_surprise_eps_pct",
    )
    prechecked = {"symbol", "q1_reported_eps", "q2_reported_eps"}
    dlg = ExcelExportDialog(cols, prechecked=prechecked)
    assert dlg._group_eps_hidden_checkbox is not None
    assert set(dlg._group_eps_hidden_keys) == {
        "q1_surprise_eps_pct", "q2_surprise_eps_pct"}
    # Defaults to the on-screen set.
    assert set(dlg.selected_keys()) == prechecked
    # Ticking the hidden bundle brings them back.
    dlg._group_eps_hidden_checkbox.setChecked(True)
    assert set(dlg.selected_keys()) == set(prechecked) | {
        "q1_surprise_eps_pct", "q2_surprise_eps_pct"}


def test_export_dialog_select_all_covers_hidden_bundles(qapp):
    cols = _cols("symbol", "q1_reported_eps", "q1_surprise_eps_pct")
    dlg = ExcelExportDialog(
        cols, prechecked={"symbol", "q1_reported_eps"},
    )
    dlg._select_all()
    assert set(dlg.selected_keys()) == {
        "symbol", "q1_reported_eps", "q1_surprise_eps_pct"}
    dlg._select_none()
    assert dlg.selected_keys() == []


def test_export_dialog_no_hidden_bundle_when_nothing_hidden(qapp):
    """A scan with nothing hidden shows exactly the two bundles it always
    did."""
    cols = _cols("symbol", "q1_reported_eps", "q1_surprise_eps_pct")
    dlg = ExcelExportDialog(cols)
    assert dlg._group_eps_hidden_checkbox is None
    assert dlg._group_rev_hidden_checkbox is None


# ----------------------------------------------------------------------
# Period threshold toggles (checkbox + greyed spinbox)
# ----------------------------------------------------------------------

@pytest.fixture
def panel(qapp):
    return W.IndicatorPanel()


def test_every_series_row_has_both_toggle_pairs(panel):
    for key in W._SERIES_ROW_KEYS:
        boxes = panel.rows[key].spinboxes
        for stat in ("period_avg", "period_max"):
            assert f"{stat}_on" in boxes, (key, stat)
            assert f"{stat}_min" in boxes, (key, stat)


def test_toggles_default_off_and_spinbox_starts_greyed(panel):
    for key in W._SERIES_ROW_KEYS:
        boxes = panel.rows[key].spinboxes
        for stat in ("period_avg", "period_max"):
            assert boxes[f"{stat}_on"].isChecked() is False
            assert boxes[f"{stat}_min"].isEnabled() is False


def test_ticking_the_box_enables_its_spinbox(panel):
    boxes = panel.rows["consec_eps_beats"].spinboxes
    boxes["period_avg_on"].setChecked(True)
    assert boxes["period_avg_min"].isEnabled() is True
    assert boxes["period_avg_min"].styleSheet() == ""
    # The other stat on the same row is untouched.
    assert boxes["period_max_min"].isEnabled() is False


def test_unticking_re_greys_the_spinbox(panel):
    boxes = panel.rows["accel_eps_yoy"].spinboxes
    boxes["period_max_on"].setChecked(True)
    boxes["period_max_on"].setChecked(False)
    assert boxes["period_max_min"].isEnabled() is False
    assert boxes["period_max_min"].styleSheet() != ""


def test_sync_reapplies_greyout_after_a_silent_preset_load(panel):
    """`setChecked` emits nothing when the value is unchanged, so a preset
    re-asserting the current value would strand the spinbox enabled."""
    boxes = panel.rows["consec_rev_growth"].spinboxes
    # Force a desynced state the way a silent preset load would.
    boxes["period_avg_min"].setEnabled(True)
    boxes["period_avg_min"].setStyleSheet("")
    assert boxes["period_avg_on"].isChecked() is False
    panel._sync_period_stat_fields()
    assert boxes["period_avg_min"].isEnabled() is False
    assert boxes["period_avg_min"].styleSheet() != ""


def test_toggle_state_reaches_scan_params(panel):
    import datetime as _dt
    panel.rows["consec_eps_beats"].set_value("period_avg_on", True)
    panel.rows["consec_eps_beats"].set_value("period_avg_min", 0.0)
    p = panel.build_scan_params(_dt.date(2025, 1, 1), _dt.date(2025, 6, 1))
    assert p.consec_eps_beats_period_avg_enabled is True
    assert p.consec_eps_beats_period_avg_min == 0.0
    assert p.consec_eps_beats_period_max_enabled is False


def test_toggle_state_survives_a_preset_round_trip(panel, qapp):
    import datetime as _dt
    panel.rows["accel_rev_surp"].set_value("period_max_on", True)
    panel.rows["accel_rev_surp"].set_value("period_max_min", -7.25)
    other = W.IndicatorPanel()
    other.from_dict(panel.to_dict())
    p = other.build_scan_params(_dt.date(2025, 1, 1), _dt.date(2025, 6, 1))
    assert p.accel_rev_surp_period_max_enabled is True
    assert p.accel_rev_surp_period_max_min == -7.25
    # And the greyout followed the restored value.
    assert other.rows["accel_rev_surp"].spinboxes[
        "period_max_min"].isEnabled() is True
