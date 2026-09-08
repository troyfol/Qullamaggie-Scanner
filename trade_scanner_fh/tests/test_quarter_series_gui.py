"""GUI-level tests for the quarter-series filters.

Panel wiring, preset round-trip, the Backward-Only greyout, and the
match-colour anchoring of an accelerating series' condensed span cell.
"""

import datetime as dt
import os

import pandas as pd
import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt6.QtWidgets import QApplication  # noqa: E402

from trade_scanner_fh.gui import widgets as W  # noqa: E402


ACCEL_KEYS = ("accel_eps_surp", "accel_rev_surp",
              "accel_eps_yoy", "accel_rev_yoy")
GROWTH_KEYS = ("consec_eps_growth", "consec_rev_growth")


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance() or QApplication([])
    yield app


@pytest.fixture
def panel(qapp):
    return W.IndicatorPanel()


# ----------------------------------------------------------------------
# Panel wiring
# ----------------------------------------------------------------------

def test_all_six_rows_exist_and_default_off(panel):
    for key in ACCEL_KEYS + GROWTH_KEYS:
        assert key in panel.rows, key
        assert not panel.rows[key].is_enabled(), key
        assert not panel.rows[key].is_display_only(), key


def test_min_count_floors_at_two(panel):
    """Spec Part 5 item 2 — a one-quarter 'series' has no acceleration
    step, so the spinbox cannot be driven below 2."""
    for key in ACCEL_KEYS:
        sb = panel.rows[key].spinboxes["min_count"]
        assert sb.minimum() == 2
        sb.setValue(1)
        assert sb.value() == 2


def test_step_threshold_cannot_be_negative(panel):
    """Minimum % Growth Threshold is >= 0 per the spec's parameter
    table: a negative step would make a decelerating chain 'accelerate'."""
    for key in ACCEL_KEYS:
        assert panel.rows[key].spinboxes["min_step_pct"].minimum() == 0.0


def test_start_threshold_may_be_negative(panel):
    for key in ACCEL_KEYS:
        assert panel.rows[key].spinboxes["min_start_pct"].minimum() < 0.0


def test_build_scan_params_carries_every_series_field(panel):
    panel.rows["accel_eps_yoy"].set_enabled(True)
    panel.rows["accel_eps_yoy"].set_value("min_start_pct", 12.5)
    panel.rows["accel_eps_yoy"].set_value("min_step_pct", 3.0)
    panel.rows["accel_eps_yoy"].set_value("min_count", 4)
    panel.rows["accel_eps_yoy"].set_value("quarter_cap", 12)
    panel.rows["accel_eps_yoy"].set_value("selection", "most_recent")
    panel.rows["accel_eps_yoy"].set_value("backward_only", True)
    panel.rows["consec_rev_growth"].set_enabled(True)
    panel.rows["consec_rev_growth"].set_value("threshold_pct", 25.0)
    panel.rows["consec_rev_growth"].set_value("min_count", 5)

    p = panel.build_scan_params(dt.date(2024, 1, 1), dt.date(2025, 1, 1))
    assert p.accel_eps_yoy_enabled
    assert p.accel_eps_yoy_min_start_pct == 12.5
    assert p.accel_eps_yoy_min_step_pct == 3.0
    assert p.accel_eps_yoy_min_count == 4
    assert p.accel_eps_yoy_quarter_cap == 12
    assert p.accel_eps_yoy_selection == "most_recent"
    assert p.accel_eps_yoy_backward_only is True
    assert p.consec_rev_growth_enabled
    assert p.consec_rev_growth_threshold_pct == 25.0
    assert p.consec_rev_growth_min == 5


def test_display_only_is_supported_on_every_new_row(panel):
    for key in ACCEL_KEYS + GROWTH_KEYS:
        panel.rows[key].display_only.setChecked(True)
        assert panel.rows[key].is_display_only(), key
        assert not panel.rows[key].is_enabled(), key


# ----------------------------------------------------------------------
# Backward Only greys out Series Selection
# ----------------------------------------------------------------------

def test_backward_only_greys_out_selection(panel):
    for key in ACCEL_KEYS:
        row = panel.rows[key]
        selection = row.spinboxes["selection"]
        assert selection.isEnabled(), key
        row.spinboxes["backward_only"].setChecked(True)
        assert not selection.isEnabled(), key
        assert selection.styleSheet(), key
        row.spinboxes["backward_only"].setChecked(False)
        assert selection.isEnabled(), key
        assert not selection.styleSheet(), key


def test_greyout_resyncs_after_preset_load(panel):
    """`setChecked` emits nothing when the value is unchanged, so a
    preset that re-asserts Backward Only needs the explicit resync."""
    state = panel.to_dict()
    state["accel_eps_yoy"]["backward_only"] = True
    panel.from_dict(state)
    assert not panel.rows["accel_eps_yoy"].spinboxes["selection"].isEnabled()

    state["accel_eps_yoy"]["backward_only"] = False
    panel.from_dict(state)
    assert panel.rows["accel_eps_yoy"].spinboxes["selection"].isEnabled()


def test_preset_round_trip_preserves_series_settings(panel):
    panel.rows["accel_rev_surp"].set_enabled(True)
    panel.rows["accel_rev_surp"].set_value("selection", "most_recent")
    panel.rows["accel_rev_surp"].set_value("min_count", 6)
    panel.rows["consec_eps_growth"].display_only.setChecked(True)

    saved = panel.to_dict()
    fresh = W.IndicatorPanel()
    fresh.from_dict(saved)

    assert fresh.rows["accel_rev_surp"].is_enabled()
    assert fresh.rows["accel_rev_surp"].value("selection") == "most_recent"
    assert fresh.rows["accel_rev_surp"].value("min_count") == 6
    assert fresh.rows["consec_eps_growth"].is_display_only()


def test_legacy_preset_without_series_keys_loads_cleanly(panel):
    """A preset saved before these filters existed must still load, with
    the new rows left at their defaults (off)."""
    panel.from_dict({"consec_eps_beats": {"enabled": False, "min_count": 3}})
    for key in ACCEL_KEYS + GROWTH_KEYS:
        assert not panel.rows[key].is_enabled(), key


def test_series_filters_do_not_lock_sequenced_run(panel):
    """Only the beats filters lock Sequenced Run — the series filters add
    scalar columns, not a wide multi-quarter block, so they must not."""
    for key in ACCEL_KEYS + GROWTH_KEYS:
        panel.rows[key].set_enabled(True)
    assert not panel.is_beats_filter_active()


def test_series_filters_do_not_lock_the_individual_earnings_rows(panel):
    panel.rows["consec_eps_growth"].set_enabled(True)
    panel.rows["accel_eps_yoy"].set_enabled(True)
    assert panel.rows["yoy_eps_pct"].toggle.isEnabled()
    assert panel.rows["reported_eps"].toggle.isEnabled()


# ----------------------------------------------------------------------
# Result columns
# ----------------------------------------------------------------------

def test_result_columns_include_three_cells_per_accel_filter():
    keys = {k for _h, k, _f in W.RESULT_COLUMNS}
    for prefix in ACCEL_KEYS:
        for suffix in ("len", "span", "vals"):
            assert f"{prefix}_{suffix}" in keys
    for key in GROWTH_KEYS:
        assert key in keys


def test_series_columns_render_only_when_populated():
    """`_build_dynamic_columns` drops any column the scan didn't
    populate, so a scan with these filters off is visually unchanged."""
    off = pd.DataFrame({"symbol": ["A"], "close": [10.0]})
    cols_off = {k for _h, k, _f in W._build_dynamic_columns(off)[0]}
    assert not any(k.startswith("accel_") for k in cols_off)

    on = pd.DataFrame({
        "symbol": ["A"], "close": [10.0],
        "accel_eps_yoy_len": [3], "accel_eps_yoy_span": ["x -> y"],
        "accel_eps_yoy_vals": ["a -> b"],
    })
    cols_on = {k for _h, k, _f in W._build_dynamic_columns(on)[0]}
    assert {"accel_eps_yoy_len", "accel_eps_yoy_span",
            "accel_eps_yoy_vals"} <= cols_on


def test_internal_anchor_keys_are_not_visible_columns():
    keys = {k for _h, k, _f in W.RESULT_COLUMNS}
    assert not any(k.startswith("_accel_") for k in keys)


# ----------------------------------------------------------------------
# Match-colour anchoring on the condensed span cell
# ----------------------------------------------------------------------

def test_accel_cells_anchor_on_the_series_report_dates():
    row = {
        "_accel_eps_yoy_start_date": pd.Timestamp("2024-02-01"),
        "_accel_eps_yoy_end_date": pd.Timestamp("2024-11-01"),
    }
    for key in ("accel_eps_yoy_len", "accel_eps_yoy_span",
                "accel_eps_yoy_vals"):
        cands = W._anchor_date_candidates(key, row)
        assert cands == [pd.Timestamp("2024-11-01"),
                         pd.Timestamp("2024-02-01")], key
        # The single-anchor accessor returns the primary (newest) one.
        assert W._anchor_date_value(key, row) == pd.Timestamp("2024-11-01")


def test_accel_keys_count_as_earnings_anchors():
    """The span IS a pair of report dates, so these cells satisfy the
    earnings-anchor gate that stops two non-earnings indicator dates
    sharing a colour with no visible earnings cell."""
    for prefix in ACCEL_KEYS:
        assert W._is_earnings_anchor_key(f"{prefix}_span")
    # The streak counts stay unanchored, as `consec_*_beats` already do.
    assert not W._is_earnings_anchor_key("consec_eps_growth")
    assert not W._is_earnings_anchor_key("consec_eps_beats")


def test_accel_anchor_candidates_empty_without_a_series():
    assert W._anchor_date_candidates("accel_eps_yoy_span", {}) == []
    assert W._anchor_date_value("accel_eps_yoy_span", {}) is None


def test_single_anchor_columns_still_yield_exactly_one_candidate():
    """The candidates helper must not change behaviour for the columns
    that always had one anchor."""
    row = {
        "max_gap_date": pd.Timestamp("2024-06-03"),
        "max_gap_pct": 12.0,
        "last_report_date": pd.Timestamp("2024-05-01"),
        "reported_eps": 1.5,
    }
    assert W._anchor_date_candidates("max_gap_pct", row) == \
        [pd.Timestamp("2024-06-03")]
    assert W._anchor_date_candidates("reported_eps", row) == \
        [pd.Timestamp("2024-05-01")]
    assert W._anchor_date_candidates("symbol", row) == []


def _colour_of(table, row_data, cols, key):
    table.model_src.clear()
    table.model_src.setRowCount(1)
    table.model_src.setColumnCount(len(cols))
    table._populate_row(0, row_data, cols)
    col = [k for _h, k, _f in cols].index(key)
    return table.model_src.item(0, col).foreground().color().name()


def test_span_cell_colours_when_an_indicator_lands_on_either_end(qapp):
    """An indicator date that matched the series' START report date must
    still colour the span cell — the whole point of carrying both
    candidates."""
    table = W.ResultsTable()
    cols = [
        ("Sym", "symbol", str),
        ("Max Gap Date", "max_gap_date", W._fmt_date),
        ("Accel Span", "accel_eps_yoy_span", str),
    ]
    start = pd.Timestamp("2024-02-01")
    end = pd.Timestamp("2024-11-01")
    row = {
        "symbol": "TEST",
        "max_gap_date": start,
        "accel_eps_yoy_span": "2024-02-01 -> 2024-11-01",
        "_accel_eps_yoy_start_date": start,
        "_accel_eps_yoy_end_date": end,
        "_earnings_aligned_dates": [start.date().isoformat()],
    }
    gap_colour = _colour_of(table, row, cols, "max_gap_date")
    span_colour = _colour_of(table, row, cols, "accel_eps_yoy_span")
    assert span_colour == gap_colour

    # And on the end date, via the primary candidate.
    row_end = dict(row, max_gap_date=end,
                   _earnings_aligned_dates=[end.date().isoformat()])
    assert _colour_of(table, row_end, cols, "accel_eps_yoy_span") == \
        _colour_of(table, row_end, cols, "max_gap_date")


def test_span_cell_uncoloured_when_nothing_aligns(qapp):
    table = W.ResultsTable()
    cols = [("Sym", "symbol", str),
            ("Accel Span", "accel_eps_yoy_span", str)]
    row = {
        "symbol": "TEST",
        "accel_eps_yoy_span": "2024-02-01 -> 2024-11-01",
        "_accel_eps_yoy_start_date": pd.Timestamp("2024-02-01"),
        "_accel_eps_yoy_end_date": pd.Timestamp("2024-11-01"),
    }
    plain = _colour_of(table, row, cols, "symbol")
    assert _colour_of(table, row, cols, "accel_eps_yoy_span") == plain


def test_display_only_fail_paints_the_len_cell_red(qapp):
    table = W.ResultsTable()
    cols = [("Sym", "symbol", str),
            ("Accel Q", "accel_eps_yoy_len", lambda x: str(int(x)))]
    row = {"symbol": "TEST", "accel_eps_yoy_len": 2,
           "_display_only_fails": {"accel_eps_yoy_len": True}}
    assert _colour_of(table, row, cols, "accel_eps_yoy_len") == \
        table._FAIL_RED.name()
