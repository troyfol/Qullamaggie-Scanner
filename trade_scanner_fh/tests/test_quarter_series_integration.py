"""Integration tests for the six quarter-series filters.

Covers the wiring the pure-algorithm suite in `test_earnings_series.py`
cannot: ScanParams -> `_compute_ticker` columns -> filter stages ->
display-only red-on-fail -> results-table columns and match-colouring,
plus the beats Q Cap change that now bounds the streak.
"""

import datetime as dt

import pandas as pd
import pytest

from trade_scanner_fh import scanner


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def history(values, *, metric="yoy_eps_pct", ticker="TEST",
            start="2022-03-01"):
    """A `report_date` DESC earnings slice, oldest quarter first in
    `values`. `None` marks an absent quarter."""
    base = pd.Timestamp(start)
    rows = []
    for i, v in enumerate(values):
        if v is None:
            continue
        period = base + pd.DateOffset(months=3 * i)
        rows.append({
            "ticker": ticker,
            "period_ending": period,
            "report_date": period + pd.Timedelta(days=40),
            metric: float(v),
        })
    return (pd.DataFrame(rows)
            .sort_values("report_date", ascending=False)
            .reset_index(drop=True))


def base_params(**kw):
    return scanner.ScanParams(
        start_date=dt.date(2022, 1, 1), end_date=dt.date(2030, 1, 1), **kw,
    )


def compute_series(hist, params):
    """Run just the quarter-series block, the way `_compute_ticker`
    calls it: on the point-in-time-preferred slice, report_date DESC."""
    row: dict = {}
    scanner._populate_quarter_series(row, params, hist)
    return row


# ----------------------------------------------------------------------
# Column population
# ----------------------------------------------------------------------

def test_accel_columns_absent_when_filter_is_off():
    row = compute_series(history([10, 15, 20, 25]), base_params())
    assert row == {}


def test_accel_populates_three_condensed_columns():
    params = base_params(
        accel_eps_yoy_enabled=True, accel_eps_yoy_min_start_pct=0.0,
        accel_eps_yoy_min_step_pct=5.0, accel_eps_yoy_min_count=3,
    )
    row = compute_series(history([10, 15, 20, 25]), params)
    assert row["accel_eps_yoy_len"] == 4
    assert row["accel_eps_yoy_span"] == "2022-03 -> 2022-12"
    assert row["accel_eps_yoy_vals"] == "+10.00% -> +25.00%"
    # Raw anchors for match-colouring travel alongside, underscore-
    # prefixed so they never surface as visible columns.
    assert row["_accel_eps_yoy_start_date"] == pd.Timestamp("2022-04-10")
    assert row["_accel_eps_yoy_end_date"] == pd.Timestamp("2023-01-10")


def test_accel_display_only_also_populates():
    params = base_params(
        accel_eps_yoy_display_only=True, accel_eps_yoy_min_step_pct=5.0,
        accel_eps_yoy_min_count=3,
    )
    row = compute_series(history([10, 15, 20]), params)
    assert row["accel_eps_yoy_len"] == 3


def test_accel_len_is_nan_when_metric_absent_everywhere():
    """No quarter carries the metric -> NaN, not 0. A zero-length series
    was never measured, and the `>= min_count` test rejects NaN anyway."""
    params = base_params(
        accel_rev_surp_enabled=True, accel_rev_surp_min_count=3,
    )
    row = compute_series(history([10, 15, 20]), params)
    assert pd.isna(row["accel_rev_surp_len"])
    assert "accel_rev_surp_span" not in row


def test_sub_threshold_series_is_still_reported():
    """When nothing reaches Min Count the best short candidate is kept,
    so display-only shows how far the ticker got and red-on-fail has a
    value to mark."""
    params = base_params(
        accel_eps_yoy_display_only=True, accel_eps_yoy_min_step_pct=5.0,
        accel_eps_yoy_min_count=4,
    )
    row = compute_series(history([10, 15, 20]), params)
    assert row["accel_eps_yoy_len"] == 3
    fails = scanner._compute_display_only_fails(params, row)
    assert fails.get("accel_eps_yoy_len") is True


def test_display_only_no_fail_flag_when_series_qualifies():
    params = base_params(
        accel_eps_yoy_display_only=True, accel_eps_yoy_min_step_pct=5.0,
        accel_eps_yoy_min_count=3,
    )
    row = compute_series(history([10, 15, 20]), params)
    assert scanner._compute_display_only_fails(params, row) == {}


@pytest.mark.parametrize("prefix, metric", [
    ("accel_eps_surp", "surprise_eps_pct"),
    ("accel_rev_surp", "surprise_rev_pct"),
    ("accel_eps_yoy", "yoy_eps_pct"),
    ("accel_rev_yoy", "yoy_rev_pct"),
])
def test_each_accel_filter_reads_its_own_metric(prefix, metric):
    params = base_params(**{
        f"{prefix}_enabled": True, f"{prefix}_min_step_pct": 5.0,
        f"{prefix}_min_count": 3,
    })
    row = compute_series(history([10, 15, 20], metric=metric), params)
    assert row[f"{prefix}_len"] == 3


def test_growth_filters_populate_a_single_int_column():
    params = base_params(
        consec_eps_growth_enabled=True,
        consec_eps_growth_threshold_pct=10.0,
        consec_rev_growth_enabled=True,
        consec_rev_growth_threshold_pct=10.0,
    )
    hist = history([20, 25, 30, 1, 40])
    hist["yoy_rev_pct"] = hist["yoy_eps_pct"]
    row = compute_series(hist, params)
    assert row["consec_eps_growth"] == 3
    assert row["consec_rev_growth"] == 3


def test_growth_quarter_cap_bounds_the_run():
    params = base_params(
        consec_eps_growth_enabled=True,
        consec_eps_growth_threshold_pct=10.0,
        consec_eps_growth_quarter_cap=2,
    )
    row = compute_series(history([20, 25, 30, 35]), params)
    assert row["consec_eps_growth"] == 2


def test_backward_only_is_honoured_through_scan_params():
    """Spec T6 through the real parameter path: a valid 4-quarter series
    exists, but the anchor quarter terminates nothing."""
    values = [10, 16, 22, 28, 25]
    strict = base_params(
        accel_eps_yoy_enabled=True, accel_eps_yoy_min_start_pct=5.0,
        accel_eps_yoy_min_step_pct=5.0, accel_eps_yoy_min_count=3,
        accel_eps_yoy_backward_only=True,
    )
    assert compute_series(history(values), strict)["accel_eps_yoy_len"] == 1
    free = base_params(
        accel_eps_yoy_enabled=True, accel_eps_yoy_min_start_pct=5.0,
        accel_eps_yoy_min_step_pct=5.0, accel_eps_yoy_min_count=3,
    )
    assert compute_series(history(values), free)["accel_eps_yoy_len"] == 4


def test_selection_mode_is_honoured_through_scan_params():
    values = [8, 12, 18, 25, 33, 30, 36, 42]
    common = dict(
        accel_eps_yoy_enabled=True, accel_eps_yoy_min_start_pct=10.0,
        accel_eps_yoy_min_step_pct=4.0, accel_eps_yoy_min_count=3,
    )
    longest = compute_series(
        history(values), base_params(accel_eps_yoy_selection="longest", **common),
    )
    recent = compute_series(
        history(values),
        base_params(accel_eps_yoy_selection="most_recent", **common),
    )
    assert longest["accel_eps_yoy_len"] == 4
    assert longest["accel_eps_yoy_vals"] == "+12.00% -> +33.00%"
    assert recent["accel_eps_yoy_len"] == 3
    assert recent["accel_eps_yoy_vals"] == "+30.00% -> +42.00%"


def test_span_uses_the_fiscal_period_not_the_report_date():
    """The span must stay monotonic. `report_date` is not: a late filing
    can announce a Q4 after the following Q1 (real cases in the live
    store: JOB, BYSI, LHX), and one ticker carries a plainly corrupt
    report_date. `period_ending` is monotonic by construction."""
    hist = history([10, 15, 20])
    # Give the newest quarter a report_date far in the past, the shape
    # CXAI has in the live store.
    newest = hist["period_ending"].max()
    hist.loc[hist["period_ending"] == newest, "report_date"] =         pd.Timestamp("2012-08-14")
    params = base_params(
        accel_eps_yoy_enabled=True, accel_eps_yoy_min_step_pct=5.0,
        accel_eps_yoy_min_count=3,
    )
    row = compute_series(hist, params)
    start_span, end_span = row["accel_eps_yoy_span"].split(" -> ")
    assert start_span < end_span, row["accel_eps_yoy_span"]
    # The corrupt report_date still travels as the colour anchor - the
    # span simply is not what displays it.
    assert row["_accel_eps_yoy_end_date"] == pd.Timestamp("2012-08-14")


def test_span_renders_a_missing_period_as_a_question_mark():
    assert scanner._fmt_series_span(None, pd.Timestamp("2024-03-01")) ==         "? -> 2024-03"


# ----------------------------------------------------------------------
# Filter stages
# ----------------------------------------------------------------------

def stage_names(params):
    return [name for name, _fn in scanner._build_filter_stages(params)]


def test_no_series_stages_when_all_six_are_off():
    names = stage_names(base_params())
    assert not any("Accel" in n or "Growth" in n for n in names)


def test_enabled_series_filters_append_one_stage_each():
    params = base_params(
        consec_eps_growth_enabled=True, consec_rev_growth_enabled=True,
        accel_eps_surp_enabled=True, accel_rev_surp_enabled=True,
        accel_eps_yoy_enabled=True, accel_rev_yoy_enabled=True,
    )
    names = stage_names(params)
    assert sum(1 for n in names if n.startswith("Consec YoY")) == 2
    assert sum(1 for n in names if n.startswith("Accel ")) == 4


def test_display_only_series_filter_appends_no_stage():
    params = base_params(accel_eps_yoy_display_only=True)
    assert not any(n.startswith("Accel") for n in stage_names(params))


def test_stage_label_reports_the_active_selection_mode():
    longest = stage_names(base_params(accel_eps_yoy_enabled=True))
    backward = stage_names(base_params(
        accel_eps_yoy_enabled=True, accel_eps_yoy_backward_only=True,
    ))
    assert any("longest)" in n for n in longest)
    assert any("backward)" in n for n in backward)


def _apply(params, frame):
    """Run every stage in sequence, as the funnel does."""
    for _name, fn in scanner._build_filter_stages(params):
        frame = frame.loc[fn(frame)]
    return frame


def test_series_stage_drops_nan_and_short_rows():
    params = base_params(accel_eps_yoy_enabled=True, accel_eps_yoy_min_count=3,
                         min_price_enabled=False, avg_vol_enabled=False,
                         dollar_vol_enabled=False, sma1_enabled=False,
                         sma2_enabled=False, sti_enabled=False,
                         dist_high_enabled=False, pct_gain_enabled=False,
                         adr_enabled=False)
    df = pd.DataFrame({
        "symbol": ["PASS", "SHORT", "NONE"],
        "accel_eps_yoy_len": [4, 2, float("nan")],
    })
    assert list(_apply(params, df)["symbol"]) == ["PASS"]


def test_series_stage_fails_everything_when_column_is_absent():
    params = base_params(accel_eps_yoy_enabled=True, accel_eps_yoy_min_count=3,
                         min_price_enabled=False, avg_vol_enabled=False,
                         dollar_vol_enabled=False, sma1_enabled=False,
                         sma2_enabled=False, sti_enabled=False,
                         dist_high_enabled=False, pct_gain_enabled=False,
                         adr_enabled=False)
    df = pd.DataFrame({"symbol": ["A", "B"]})
    assert _apply(params, df).empty


# ----------------------------------------------------------------------
# Beats Q Cap now bounds the streak, not just the columns
# ----------------------------------------------------------------------

def beats_history(n_beats, *, ticker="TEST"):
    """`n_beats` consecutive positive EPS surprises, newest last."""
    base = pd.Timestamp("2020-03-01")
    rows = []
    for i in range(n_beats):
        period = base + pd.DateOffset(months=3 * i)
        rows.append({
            "ticker": ticker,
            "period_ending": period,
            "report_date": period + pd.Timedelta(days=40),
            "surprise_eps_pct": 10.0,
            "surprise_rev_pct": 10.0,
        })
    return (pd.DataFrame(rows)
            .sort_values("report_date", ascending=False)
            .reset_index(drop=True))


def test_beats_quarter_cap_now_bounds_the_streak():
    from trade_scanner_fh.earnings_history import compute_consecutive_beats
    hist = beats_history(8)
    assert compute_consecutive_beats(hist, "eps", 0.0) == 8
    # The scanner hands the capped pool through, so cap=4 can no longer
    # report a streak of 8.
    assert compute_consecutive_beats(hist.head(4), "eps", 0.0) == 4


def test_beats_uncapped_streak_is_not_clipped_at_twenty():
    """cap<=0 must stay genuinely uncapped: the MAX_BEATS_QUARTERS=20
    ceiling is a display limit and must not truncate a longer streak."""
    from trade_scanner_fh.earnings_history import compute_consecutive_beats
    hist = beats_history(26)
    assert compute_consecutive_beats(hist, "eps", 0.0) == 26
