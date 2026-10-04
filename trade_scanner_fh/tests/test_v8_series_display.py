"""v8.0.0 round 2 (2026-09-26 user feedback) — what the quarter-series
filters count, show and colour, plus the computed Beta's frequency.

Built from the three real-store cases that drove it:

  * ATRO  — growth run Q-2..Q-4 stepped over an N/A YoY at Q-1 (year-ago EPS
            $0.04, under the floor). Legitimate: one skipped quarter is the
            bridging allowance. But Q-4 was not on screen, and the default
            shading painted the N/A cell green.
  * ELOX  — newest report has no YoY and the history jumps back 2.75 years;
            Backward Only still passed on a run from 2023.
  * a late filing — report order and fiscal order disagree, so "Q-1..Q-n"
            is not the set of quarters a streak counted.
"""
from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd
import pytest
from PyQt6.QtCore import Qt

from trade_scanner_fh import earnings_series as es
from trade_scanner_fh import indicators
from trade_scanner_fh import scanner as S
from trade_scanner_fh.gui import coloring as C
from trade_scanner_fh.gui import color_rules_dialog as D
from trade_scanner_fh.gui import widgets as W


@pytest.fixture(scope="module", autouse=True)
def qapp():
    from PyQt6.QtWidgets import QApplication
    return QApplication.instance() or QApplication([])


T = pd.Timestamp


def _hist(rows):
    """A past_pref-shaped frame: report_date DESC, one row per quarter."""
    df = pd.DataFrame(rows, columns=[
        "period_ending", "report_date", "reported_eps", "surprise_eps",
        "surprise_eps_pct", "yoy_eps_pct"])
    df["period_ending"] = pd.to_datetime(df["period_ending"])
    df["report_date"] = pd.to_datetime(df["report_date"])
    return df.sort_values("report_date", ascending=False).reset_index(
        drop=True)


# ATRO as the live store held it on 2026-09-18 (EPS side).
ATRO = _hist([
    ("2026-06-01", "2026-08-11", 0.70, 0.11, 18.66, np.nan),
    ("2026-03-01", "2026-05-12", 0.47, 0.02, 5.37, 81.076923),
    ("2025-12-01", "2026-02-24", 0.75, 0.15, 25.00, 1037.5),
    ("2025-09-01", "2025-11-04", 0.49, 0.07, 16.67, 244.117647),
    ("2025-06-01", "2025-08-06", 0.04, -0.29, -87.88, np.nan),
    ("2025-03-01", "2025-05-06", 0.26, np.nan, np.nan, 388.888889),
    ("2024-12-01", "2025-03-04", -0.08, -0.29, -138.10, -140.0),
])

# ELOX: newest report valueless, then a 2.75-year hole.
ELOX = _hist([
    ("2026-06-01", "2026-08-12", -0.66, np.nan, np.nan, np.nan),
    ("2023-09-01", "2023-11-13", -12.81, np.nan, np.nan, 67.65),
    ("2023-06-01", "2023-08-14", -21.56, np.nan, np.nan, 59.17),
    ("2023-03-01", "2023-05-15", -31.68, np.nan, np.nan, 44.63),
])


def _growth_params(**kw):
    base = dict(consec_eps_growth_enabled=True, consec_eps_growth_min=3,
                consec_eps_growth_threshold_pct=0.0,
                consec_eps_growth_quarter_cap=4,
                consec_eps_growth_selection="longest",
                consec_eps_growth_backward_only=True)
    base.update(kw)
    return S.ScanParams(**base)


# ======================================================================
# Series engine: the trailing-gap limit under Backward Only
# ======================================================================

def test_trailing_missing_counts_valueless_and_absent_newest_quarters():
    pts = es.build_quarter_points(ATRO, "yoy_eps_pct", quarter_cap=4)
    assert es.trailing_missing_quarters(ATRO, pts, quarter_cap=4) == 1
    pts = es.build_quarter_points(ELOX, "yoy_eps_pct")
    assert es.trailing_missing_quarters(ELOX, pts) == 11
    full = ATRO.iloc[1:]           # newest quarter has a value
    pts = es.build_quarter_points(full, "yoy_eps_pct")
    assert es.trailing_missing_quarters(full, pts) == 0
    assert es.trailing_missing_quarters(ATRO, []) == 0


def test_backward_only_allows_exactly_the_bridging_allowance_at_the_tail():
    pts = es.build_quarter_points(ATRO, "yoy_eps_pct", quarter_cap=4)
    kw = dict(threshold=0.0, min_count=3, inclusive=True, max_bridged=1,
              backward_only=True)
    ok = es.run_series(pts, trailing_missing=1, **kw)
    assert ok.length == 3 and ok.qualifies
    assert [p.strftime("%Y-%m") for p in ok.periods] == \
        ["2025-09", "2025-12", "2026-03"]
    assert np.isclose(np.mean(ok.values), 454.231523)
    over = es.run_series(pts, trailing_missing=2, **kw)
    assert over.length == 0 and not over.qualifies


def test_elox_no_longer_passes_backward_only_on_a_2023_run():
    pts = es.build_quarter_points(ELOX, "yoy_eps_pct")
    tm = es.trailing_missing_quarters(ELOX, pts)
    res = es.run_series(pts, threshold=0.0, min_count=3, inclusive=True,
                        max_bridged=1, backward_only=True, trailing_missing=tm)
    assert res.length == 0
    # Without Backward Only the old run is still findable — by design.
    anywhere = es.run_series(pts, threshold=0.0, min_count=3, inclusive=True,
                             max_bridged=1, backward_only=False,
                             trailing_missing=tm)
    assert anywhere.length == 3


def test_accelerating_backward_only_obeys_the_same_tail_limit():
    pts = es.build_quarter_points(ELOX, "yoy_eps_pct")
    kw = dict(min_start_pct=0.0, min_step_pct=0.0, min_count=2,
              backward_only=True, max_bridged=1)
    assert es.accelerating_series(pts, trailing_missing=0, **kw) is not None
    assert es.accelerating_series(pts, trailing_missing=11, **kw) is None


def test_periods_exclude_a_bridged_quarter():
    hist = _hist([
        ("2026-06-01", "2026-08-01", 1, 0, 1, 30.0),
        ("2026-03-01", "2026-05-01", 1, 0, 1, np.nan),   # bridged
        ("2025-12-01", "2026-02-01", 1, 0, 1, 20.0),
        ("2025-09-01", "2025-11-01", 1, 0, 1, 10.0),
    ])
    pts = es.build_quarter_points(hist, "yoy_eps_pct")
    res = es.run_series(pts, threshold=0.0, min_count=1, inclusive=True,
                        max_bridged=1, backward_only=True)
    assert res.length == 3
    assert T("2026-03-01") not in res.periods


# ======================================================================
# Scanner: growth draws its own quarters, Span / V, run membership
# ======================================================================

def test_atro_growth_row_shows_every_quarter_the_count_used():
    row: dict = {}
    S._populate_quarter_series(row, _growth_params(), ATRO)
    assert row["consec_eps_growth"] == 3
    assert row["_consec_eps_growth_qs"] == [2, 3, 4]
    # The whole capped pool is on screen: Q-4 (+244.12%) included.
    assert np.isclose(row["q4_yoy_eps_pct"], 244.117647)
    assert "q5_yoy_eps_pct" not in row
    assert np.isnan(row["q1_yoy_eps_pct"])
    assert row["consec_eps_growth_span"] == S._fmt_series_span(
        T("2025-09-01"), T("2026-03-01"))
    assert row["_consec_eps_growth_end_date"] == T("2026-05-12")
    assert np.isclose(row["consec_eps_growth_period_avg"], 454.231523)


def test_uncapped_growth_counts_the_whole_pool_and_shows_every_quarter():
    """Uncapped, ATRO's run also steps over its N/A Q-5 (one bridged quarter
    is the allowance) to Q-6 (+388.89%): four quarters counted, Q-5 not among
    them. v8.0.2: an uncapped row shows every quarter on file up to the
    20-block ceiling (it used to stop at the run's oldest counted quarter,
    Q-6, while an uncapped beats row drew 20 — the user's rule made them
    agree)."""
    row: dict = {}
    S._populate_quarter_series(
        row, _growth_params(consec_eps_growth_quarter_cap=0), ATRO)
    assert row["consec_eps_growth"] == 4
    assert row["_consec_eps_growth_qs"] == [2, 3, 4, 6]
    n = min(len(ATRO), S.MAX_BEATS_QUARTERS)
    assert n > 6, "fixture must reach past the run's oldest quarter"
    assert f"q{n}_yoy_eps_pct" in row and f"q{n + 1}_yoy_eps_pct" not in row


def test_a_failed_backward_only_growth_run_shows_no_span_and_no_quarters():
    row: dict = {}
    S._populate_quarter_series(
        row, _growth_params(consec_eps_growth_quarter_cap=0), ELOX)
    assert row["consec_eps_growth"] == 0
    assert row["consec_eps_growth_span"] is None
    assert row["_consec_eps_growth_qs"] == []
    assert "q1_report_date_eps" in row      # Q-1 still shown for context


def test_accel_filters_record_their_counted_quarters():
    row: dict = {}
    params = S.ScanParams(accel_eps_yoy_enabled=True,
                          accel_eps_yoy_min_start_pct=0.0,
                          accel_eps_yoy_min_step_pct=-1e9,
                          accel_eps_yoy_min_count=1,
                          accel_eps_yoy_backward_only=True)
    S._populate_quarter_series(row, params, ATRO.iloc[1:4])
    assert row["_accel_eps_yoy_qs"] and all(
        1 <= k <= 3 for k in row["_accel_eps_yoy_qs"])


def test_beats_membership_follows_fiscal_order_not_report_order():
    """A quarter filed late sits at Q-1 by report date but is the OLDEST
    fiscal quarter. It missed; the three fiscally newer quarters beat. The
    streak is 3 and it is Q-2..Q-4 — not Q-1..Q-3, which is what the old
    `k <= streak` colouring painted."""
    hist = _hist([
        ("2025-06-01", "2026-05-25", 1, -0.1, -5.0, 1.0),   # late, missed
        ("2026-03-01", "2026-05-10", 1, 0.1, 5.0, 1.0),
        ("2025-12-01", "2026-02-10", 1, 0.1, 5.0, 1.0),
        ("2025-09-01", "2025-11-10", 1, 0.1, 5.0, 1.0),
    ])
    params = S.ScanParams(consec_eps_beats_display_only=True)
    series = S._beats_series(hist, "surprise_eps_pct", params,
                             "consec_eps_beats")
    row: dict = {}
    S._write_run_quarters(row, "consec_eps_beats", hist, series)
    assert series.length == 3
    assert row["_consec_eps_beats_qs"] == [2, 3, 4]


def test_last_report_date_is_kept_beside_growth_blocks(tmp_path, monkeypatch):
    """v8.0.2 (the user's rule): every earnings filter produces a date
    column, so Last Report Date is no longer dropped beside a Q-1 Date — it
    is hidden by default through Hide Q Columns instead (pinned in
    test_v802_dates_and_tooltips.py). It used to be suppressed whenever beats
    or growth drew the blocks."""
    from trade_scanner_fh import data_engine
    days = pd.bdate_range("2026-01-02", "2026-09-18")
    ohlcv = pd.DataFrame({"Open": 10.0, "High": 10.5, "Low": 9.5,
                          "Close": 10.0, "Volume": 1e6}, index=days)
    ohlcv.index.name = "Date"
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    data_engine.clear_ohlcv_cache()
    ohlcv.to_parquet(tmp_path / "ATRO.parquet")
    lookup = {"ATRO": ATRO}

    def run(**kw):
        p = S.ScanParams(start_date=dt.date(2026, 9, 1),
                         end_date=dt.date(2026, 9, 18),
                         yoy_eps_pct_display_only=True, **kw)
        for f in S.ScanParams.__dataclass_fields__:
            if (f.endswith("_enabled") and getattr(p, f)
                    and f != "consec_eps_growth_enabled"):
                setattr(p, f, False)
        return S._compute_ticker("ATRO", p,
                                 earnings_history_lookup=lookup)

    alone = run()
    assert alone["last_report_date"] == T("2026-08-11")
    with_growth = run(consec_eps_growth_enabled=True,
                      consec_eps_growth_quarter_cap=4,
                      consec_eps_growth_backward_only=True)
    assert with_growth["last_report_date"] == T("2026-08-11")
    assert with_growth["q1_report_date_eps"] == T("2026-08-11")
    assert with_growth["_consec_eps_growth_qs"] == [2, 3, 4]


# ======================================================================
# Colouring: "is counted in the run of", N/A skipping, defaults
# ======================================================================

def _atro_frame():
    row = {"symbol": "ATRO", "close": 1.0, "pct_gain": 1.0}
    S._populate_quarter_series(row, _growth_params(), ATRO)
    return pd.DataFrame([row])


def _layout(df):
    return [k for _h, k, _f in W._build_dynamic_columns(df)[0]]


def test_default_growth_rule_shades_exactly_the_counted_yoy_cells():
    df = _atro_frame()
    rs = C.evaluate(df, C.default_rules(), _layout(df))[0]
    shaded = [k for k in (1, 2, 3, 4)
              if rs.resolve(f"q{k}_yoy_eps_pct")[1] == C.GROWTH_BG]
    assert shaded == [2, 3, 4]
    assert rs.resolve("q2_reported_eps")[1] is None, "YoY cell only"


def test_in_run_reports_a_filter_that_did_not_run():
    df = pd.DataFrame([{"symbol": "A", "q1_yoy_eps_pct": 5.0}])
    rule = C.Rule(id="x", scope="quarter", target="columns",
                  target_columns=["q_yoy_eps_pct"],
                  conditions=[C.Condition(kind="quarter", op="in_run",
                                          other="consec_rev_growth")],
                  style=C.Style(text=C.ColorSpec("fixed", "#ff0000")))
    report = {}
    styles = C.evaluate(df, [rule], ["symbol", "q1_yoy_eps_pct"],
                        report=report)
    assert styles == [None]
    assert report["x"]["missing"] == ["consec_rev_growth"]


def test_skip_blank_leaves_na_cells_unpainted_for_every_target_kind():
    df = pd.DataFrame([{"symbol": "A", "rvol": 3.0, "note": None,
                        "adr_pct": np.nan}])
    layout = ["symbol", "rvol", "note", "adr_pct"]
    cond = C.Condition("value", "rvol", ">=", 1.0)
    for target, cols in (("row", []), ("columns", ["note", "rvol"])):
        rule = C.Rule(id="s", conditions=[cond], target=target,
                      target_columns=cols, skip_blank=True,
                      style=C.Style(background=C.ColorSpec("fixed",
                                                           "#123456")))
        rs = C.evaluate(df, [rule], layout)[0]
        assert rs.resolve("rvol")[1] == "#123456"
        assert rs.resolve("note")[1] is None
        assert rs.resolve("adr_pct")[1] is None
    off = C.Rule(id="s", conditions=[cond], target="row",
                 style=C.Style(background=C.ColorSpec("fixed", "#123456")))
    assert C.evaluate(df, [off], layout)[0].resolve("note")[1] == "#123456"


def test_beats_green_skips_the_na_yoy_cell_but_keeps_the_rest_of_the_block():
    df = pd.DataFrame([{
        "symbol": "AEHR", "consec_eps_beats": 1, "_consec_eps_beats_qs": [1],
        "q1_report_date_eps": T("2026-07-01"), "q1_reported_eps": 0.11,
        "q1_surprise_eps_dollar": 0.1, "q1_surprise_eps_pct": 15.0,
        "q1_yoy_eps_pct": np.nan}])
    rs = C.evaluate(df, C.default_rules(), _layout(df))[0]
    assert rs.resolve("q1_reported_eps")[0] == C.STREAK_GREEN
    assert rs.resolve("q1_yoy_eps_pct")[0] is None


def test_skip_blank_and_in_run_round_trip_through_json():
    rules = C.default_rules()
    back = C.rules_from_json(C.rules_to_json(rules))
    assert [r.to_dict() for r in back] == [r.to_dict() for r in rules]
    assert C.default_rules()[2].conditions[0].columns() == \
        ["_consec_eps_beats_qs"]


# ----------------------------------------------------------------------
# Rules version 1 -> 2
# ----------------------------------------------------------------------

def _v1_payload(rules):
    return {"version": 1, "rules": [r.to_dict() for r in rules]}


def _v1_defaults():
    v1 = [C.default_rules()[0], C.default_rules()[1],
          C._v1_streak_rule("eps"), C._v1_streak_rule("rev")]
    for r in v1:
        r.to_dict()          # all serialisable
    return v1


def test_v1_untouched_defaults_upgrade_and_growth_rules_are_added():
    got = C.rules_from_json(_v1_payload(_v1_defaults()))
    assert [r.id for r in got] == [r.id for r in C.default_rules()]
    assert [r.to_dict() for r in got] == \
        [r.to_dict() for r in C.default_rules()]


def test_v1_upgrade_keeps_the_switch_and_leaves_edited_defaults_alone():
    """The shape of the user's own 'eps test' preset: a custom rule on top,
    the EPS streak rule EDITED (its right-hand side blanked) and switched off,
    the Rev streak rule untouched but switched off."""
    custom = C.Rule(id="cb71693bfa69", name="EPS yoy growth", scope="quarter",
                    conditions=[C.Condition(kind="filter",
                                            column="consec_eps_growth",
                                            op="passes")],
                    style=C.Style(text=C.ColorSpec("fixed", "#00ff26")))
    eps = C._v1_streak_rule("eps")
    eps.conditions[0].other = ""
    eps.enabled = False
    rev = C._v1_streak_rule("rev")
    rev.enabled = False
    fail = C.default_rules()[1]
    fail.enabled = False
    got = C.rules_from_json(_v1_payload(
        [custom, C.default_rules()[0], fail, eps, rev]))
    ids = [r.id for r in got]
    assert ids == ["cb71693bfa69", "default_earnings_match",
                   "default_display_only_fail", "default_eps_streak",
                   "default_rev_streak", "default_eps_growth_run",
                   "default_rev_growth_run"]
    by = {r.id: r for r in got}
    assert by["default_eps_streak"].to_dict() == eps.to_dict(), \
        "an edited default is the user's and stays exactly as saved"
    assert by["default_rev_streak"].conditions[0].op == "in_run"
    assert by["default_rev_streak"].skip_blank is True
    assert by["default_rev_streak"].enabled is False, "switch kept"
    assert by["default_display_only_fail"].enabled is False


def test_v2_payload_is_not_upgraded():
    """A user who deleted the growth rules in version 2 keeps them deleted."""
    rules = [r for r in C.default_rules() if "growth" not in r.id]
    got = C.rules_from_json(C.rules_to_json(rules))
    assert [r.id for r in got] == [r.id for r in rules]


def test_bare_list_payload_is_treated_as_version_1():
    got = C.rules_from_json([r.to_dict() for r in _v1_defaults()])
    assert "default_eps_growth_run" in [r.id for r in got]


# ======================================================================
# Dialog
# ======================================================================

COLS = [("Consec EPS Beats", "consec_eps_beats", "num"),
        ("RVOL", "rvol", "num"),
        ("Q-X YoY EPS %", "q{k}_yoy_eps_pct", "num")]


def test_in_run_condition_round_trips_and_lists_series_filters():
    cond = C.Condition(kind="quarter", op="in_run", other="accel_eps_yoy")
    ed = D.ConditionEditor(COLS)
    ed.set_scope("quarter")
    ed.load(cond)
    assert ed.condition().to_dict() == cond.to_dict()
    listed = [ed.other.itemData(i) for i in range(ed.other.count())]
    assert listed == [p for p, _l in C.RUN_SOURCES]


def test_switching_the_operator_swaps_the_right_hand_list():
    ed = D.ConditionEditor(COLS)
    ed.set_scope("quarter")
    ed.load(C.Condition(kind="quarter", op="<=", other="consec_eps_beats"))
    D._set(ed.op, "in_run")
    # consec_eps_beats is both a column and a series filter: kept.
    assert ed.condition().to_dict()["other"] == "consec_eps_beats"
    D._set(ed.op, "<=")
    assert ed.condition().other == "consec_eps_beats"


def test_every_default_rule_round_trips_through_the_editor():
    dlg = D.ColorRulesDialog(C.default_rules(), COLS,
                             [("Q-X YoY EPS %", "q_yoy_eps_pct")])
    for row in range(len(dlg._rules)):
        dlg.list.setCurrentRow(row)
    dlg.list.setCurrentRow(0)
    assert [r.to_dict() for r in dlg.rules()] == \
        [r.to_dict() for r in C.default_rules()]


def test_skip_blank_checkbox_round_trips():
    ed = D.RuleEditor(COLS, [])
    rule = C.default_rules()[4]
    ed.load(rule)
    assert ed.skip_blank.isChecked()
    ed.skip_blank.setChecked(False)
    assert ed.rule().skip_blank is False


# ======================================================================
# Table layout
# ======================================================================

def test_growth_only_layout_has_no_phantom_beats_column():
    df = _atro_frame()
    cols = W._build_dynamic_columns(df)[0]
    headers = [h for h, _k, _f in cols]
    assert "Consec EPS Beats" not in headers
    keys = [k for _h, k, _f in cols]
    i = keys.index("consec_eps_growth")
    assert keys[i + 1:i + 3] == ["consec_eps_growth_span",
                                 "consec_eps_growth_vals"]
    assert "q4_yoy_eps_pct" in keys


def test_growth_span_cells_join_their_hide_group_and_anchor_on_reports():
    assert W.earnings_column_type_of("consec_eps_growth_span") == \
        "series_consec_eps_growth"
    assert W.earnings_column_type_of("consec_rev_growth_vals") == \
        "series_consec_rev_growth"
    row = {"_consec_eps_growth_end_date": T("2026-05-12"),
           "_consec_eps_growth_start_date": T("2025-11-04")}
    assert W._anchor_date_candidates("consec_eps_growth_span", row) == \
        [T("2026-05-12"), T("2025-11-04")]
    assert W._anchor_date_candidates("consec_eps_growth", row) == [], \
        "the count cell still does not take part in date matching"


def test_beta_header_names_its_basis():
    df = pd.DataFrame([{"symbol": "A", "beta_calc": 1.1,
                        "_beta_calc_basis": "60M"}])
    headers = {k: h for h, k, _f in W._build_dynamic_columns(df)[0]}
    assert headers["beta_calc"] == "Beta calc 60M"
    plain = pd.DataFrame([{"symbol": "A", "beta_calc": 1.1}])
    headers = {k: h for h, k, _f in W._build_dynamic_columns(plain)[0]}
    assert headers["beta_calc"] == "Beta (calc)"


# ======================================================================
# Panel: presets restore every setting, Beta frequency control
# ======================================================================

def test_a_setting_the_preset_predates_falls_back_to_its_default():
    """The EPS YoY preset predates Period Avg / Max. A Period Avg filter
    switched on in the session must not ride into that preset's scan."""
    panel = W.IndicatorPanel()
    row = panel.rows["consec_eps_beats"]
    row.set_value("period_avg_on", True)
    row.display_only.setChecked(True)
    panel.rows["beta_calc"].set_value("frequency", "monthly")
    old = {"consec_eps_beats": {"enabled": False, "min_count": 0,
                                "threshold_pct": 0.0, "quarter_cap": 3},
           "beta_calc": {"enabled": False, "lookback": 252,
                         "min_beta": -5.0, "max_beta": 10.0}}
    panel.from_dict(old)
    assert row.value("period_avg_on") is False
    assert row.is_display_only() is False
    assert panel.rows["beta_calc"].value("frequency") == "daily"
    assert row.value("quarter_cap") == 3, "the preset's own values win"


def test_beta_frequency_is_saved_before_lookback_and_both_restore():
    panel = W.IndicatorPanel()
    keys = list(panel.to_dict()["beta_calc"])
    assert keys.index("frequency") < keys.index("lookback")
    panel.from_dict({"beta_calc": {"enabled": True, "frequency": "monthly",
                                   "lookback": 48}})
    b = panel.rows["beta_calc"]
    assert (b.value("frequency"), b.value("lookback")) == ("monthly", 48)
    params = panel.build_scan_params(dt.date(2026, 9, 1), dt.date(2026, 9, 2))
    assert params.beta_calc_frequency == "monthly"
    assert params.beta_calc_lookback == 48


def test_choosing_a_frequency_by_hand_resets_the_lookback():
    panel = W.IndicatorPanel()
    combo = panel.rows["beta_calc"].spinboxes["frequency"]
    for freq, span in (("monthly", 60), ("weekly", 104), ("daily", 252)):
        idx = next(i for i in range(combo.count())
                   if combo.itemData(i) == freq)
        combo.setCurrentIndex(idx)
        combo.activated.emit(idx)
        assert panel.rows["beta_calc"].value("lookback") == span


# ======================================================================
# Beta frequency maths
# ======================================================================

def _monthly_pair(r_bench, slope):
    """Daily frames whose MONTH-END closes carry exact returns: the stock's
    simple monthly return is `slope` x the benchmark's."""
    months = pd.date_range("2020-01-31", periods=len(r_bench) + 1, freq="ME")
    b = np.concatenate([[100.0], 100.0 * np.cumprod(1 + np.asarray(r_bench))])
    s = np.concatenate([[50.0], 50.0 * np.cumprod(
        1 + slope * np.asarray(r_bench))])
    days = pd.bdate_range(months[0] - pd.offsets.MonthBegin(1), months[-1])
    idx = np.searchsorted(months, days)          # value of the month ahead
    idx = np.clip(idx, 0, len(months) - 1)
    return (pd.DataFrame({"Close": s[idx]}, index=days),
            pd.DataFrame({"Close": b[idx]}, index=days))


def test_monthly_beta_recovers_an_exact_slope():
    rng = np.random.default_rng(7)
    r = rng.normal(0.01, 0.05, 72)
    stock, bench = _monthly_pair(r, 1.7)
    got = indicators.beta_vs_benchmark(stock, bench, lookback=60,
                                       frequency="monthly")
    assert got == pytest.approx(1.7, abs=1e-9)


def test_monthly_lookback_reads_only_the_last_n_months():
    rng = np.random.default_rng(11)
    r = rng.normal(0.01, 0.05, 48)
    early, _ = _monthly_pair(r[:36], 0.5)
    stock, bench = _monthly_pair(r, 1.0)
    # Rebuild the stock so its LAST 12 months move at 3x, earlier at 0.5x.
    slope = np.r_[np.full(36, 0.5), np.full(12, 3.0)]
    s = np.concatenate([[50.0], 50.0 * np.cumprod(1 + slope * r)])
    months = pd.date_range("2020-01-31", periods=49, freq="ME")
    idx = np.clip(np.searchsorted(months, stock.index), 0, 48)
    stock = pd.DataFrame({"Close": s[idx]}, index=stock.index)
    got = indicators.beta_vs_benchmark(stock, bench, lookback=12,
                                       frequency="monthly")
    assert got == pytest.approx(3.0, abs=1e-9)


def test_weekly_beta_and_bad_index():
    days = pd.bdate_range("2023-01-02", periods=600)
    rng = np.random.default_rng(3)
    b = 100 * np.cumprod(1 + rng.normal(0, 0.01, len(days)))
    bench = pd.DataFrame({"Close": b}, index=days)
    fri = bench.resample("W-FRI").last()
    s_week = 40 * np.cumprod(np.r_[1.0, 1 + 0.8 * (
        fri["Close"].to_numpy()[1:] / fri["Close"].to_numpy()[:-1] - 1)])
    week_of = np.searchsorted(fri.index, days)
    stock = pd.DataFrame({"Close": s_week[week_of]}, index=days)
    got = indicators.beta_vs_benchmark(stock, bench, lookback=104,
                                       frequency="weekly")
    assert got == pytest.approx(0.8, abs=1e-9)
    no_dates = stock.reset_index(drop=True)
    assert np.isnan(indicators.beta_vs_benchmark(
        no_dates, bench.reset_index(drop=True), frequency="monthly"))


@pytest.mark.parametrize("freq,rule", [("weekly", "W-FRI"),
                                       ("monthly", "ME")])
@pytest.mark.parametrize("start", ["1969-11-03", "2019-06-03", "2024-02-26"])
def test_period_ends_are_exactly_what_resample_keeps(freq, rule, start):
    """The fast period-end picker must choose the same sessions pandas'
    resample(...).last() does — across gaps (halts, holidays, a missing
    month) and across the 1970 epoch the week arithmetic is anchored on."""
    rng = np.random.default_rng(len(start) + len(freq))
    days = pd.bdate_range(start, periods=900)
    keep = rng.random(len(days)) > 0.15
    keep[300:340] = False                         # a six-week hole
    days = days[keep]
    frame = pd.DataFrame({"s": rng.random(len(days)) + 1,
                          "b": rng.random(len(days)) + 1}, index=days)
    fast = frame.iloc[indicators._period_end_positions(frame.index, freq)]
    slow = frame.resample(rule).last().dropna()
    np.testing.assert_array_equal(fast["s"].to_numpy(), slow["s"].to_numpy())
    np.testing.assert_array_equal(fast["b"].to_numpy(), slow["b"].to_numpy())


def test_scan_computes_beta_at_the_chosen_frequency(tmp_path, monkeypatch):
    from trade_scanner_fh import data_engine
    rng = np.random.default_rng(5)
    stock, bench = _monthly_pair(rng.normal(0.01, 0.05, 72), 1.7)
    ohlcv = pd.DataFrame({"Open": stock["Close"], "High": stock["Close"],
                          "Low": stock["Close"], "Close": stock["Close"],
                          "Volume": 1e6})
    ohlcv.index.name = "Date"
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    data_engine.clear_ohlcv_cache()
    ohlcv.to_parquet(tmp_path / "BETA.parquet")
    end = ohlcv.index[-1].date()
    p = S.ScanParams(start_date=end - dt.timedelta(days=10), end_date=end,
                     beta_calc_display_only=True, beta_calc_lookback=60,
                     beta_calc_frequency="monthly")
    for f in S.ScanParams.__dataclass_fields__:
        if f.endswith("_enabled") and getattr(p, f):
            setattr(p, f, False)
    row = S._compute_ticker("BETA", p, benchmark_data={"SPY": bench})
    assert row["beta_calc"] == pytest.approx(1.7, abs=1e-6)
    assert row["_beta_calc_basis"] == "60M"


def test_beta_basis_label_and_scan_reach():
    assert indicators.beta_basis("daily", 252) == "252D"
    assert indicators.beta_basis("weekly", 104) == "104W"
    assert indicators.beta_basis("monthly", 60) == "60M"
    p = S.ScanParams(beta_calc_display_only=True, beta_calc_lookback=60,
                     beta_calc_frequency="monthly")
    assert p.max_trailing_bars() >= 61 * 21
