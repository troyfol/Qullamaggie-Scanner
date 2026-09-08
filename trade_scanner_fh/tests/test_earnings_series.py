"""Acceptance tests for the quarter-series filters (spec Part 4).

T1-T8 are transcribed verbatim from `earnings-filters-spec.md`; the
remaining tests cover the shared rules (Part 3) and the pool/cap
construction that the acceptance tests take as given.
"""

import pandas as pd
import pytest

from trade_scanner_fh import earnings_series as es


# ----------------------------------------------------------------------
# Fixtures / helpers
# ----------------------------------------------------------------------

def make_points(values, *, start="2020-03-01"):
    """Build a contiguous quarter series from `values`, oldest first.

    `None` marks a missing period: it consumes a quarter slot but yields
    no point, exactly like an absent row or a NaN metric value.
    """
    base = pd.Timestamp(start)
    points = []
    prev_period = None
    for i, v in enumerate(values):
        period = base + pd.DateOffset(months=3 * i)
        if v is None:
            continue
        if prev_period is None:
            missing = 0
        else:
            missing = es._period_steps(prev_period, period) - 1
        points.append(es.QuarterPoint(
            value=float(v), period=period,
            report_date=period + pd.Timedelta(days=40),
            missing_before=missing,
        ))
        prev_period = period
    return points


def make_history(values, *, metric="yoy_eps_pct", start="2020-03-01"):
    """Build a `report_date` DESC history frame, the shape the scanner
    hands the series functions. `None` values become absent rows."""
    base = pd.Timestamp(start)
    rows = []
    for i, v in enumerate(values):
        if v is None:
            continue
        period = base + pd.DateOffset(months=3 * i)
        rows.append({
            "ticker": "TEST",
            "period_ending": period,
            "report_date": period + pd.Timedelta(days=40),
            metric: float(v),
        })
    df = pd.DataFrame(rows)
    return df.sort_values("report_date", ascending=False).reset_index(drop=True)


def accel(values, *, start_pct, step_pct, count, selection=es.SELECT_LONGEST,
          backward_only=False):
    return es.accelerating_series(
        make_points(values), min_start_pct=start_pct, min_step_pct=step_pct,
        min_count=count, selection=selection, backward_only=backward_only,
    )


# ----------------------------------------------------------------------
# Part 4 - acceptance tests, transcribed from the spec
# ----------------------------------------------------------------------

# T1 - Longest vs. Most Recent diverge.
# Q1=8, Q2=12, Q3=18, Q4=25, Q5=33, Q6=30, Q7=36, Q8=42
# Start >= 10, Growth >= 4, Count >= 3.
_T1 = [8, 12, 18, 25, 33, 30, 36, 42]
_T1_KW = dict(start_pct=10.0, step_pct=4.0, count=3)


def test_t1_longest_returns_series_a():
    """Longest -> series A, Q2-Q5 (12 -> 33), 4 quarters."""
    res = accel(_T1, selection=es.SELECT_LONGEST, **_T1_KW)
    assert res is not None and res.qualifies
    assert res.length == 4
    assert res.start_value == 12.0
    assert res.end_value == 33.0


def test_t1_most_recent_returns_series_b():
    """Most Recent -> series B, Q6-Q8 (30 -> 42), 3 quarters.
    Q5 -> Q6 is -3, which breaks the chain."""
    res = accel(_T1, selection=es.SELECT_MOST_RECENT, **_T1_KW)
    assert res is not None and res.qualifies
    assert res.length == 3
    assert res.start_value == 30.0
    assert res.end_value == 42.0


def test_t1_backward_only_anchors_on_q8():
    """Backward Only -> anchor Q8, walks back to Q6, stops at Q6 -> Q5."""
    res = accel(_T1, backward_only=True, **_T1_KW)
    assert res is not None and res.qualifies
    assert res.length == 3
    assert res.start_value == 30.0
    assert res.end_value == 42.0


def test_t2_negative_progression_qualifies():
    """T2 - `-45, -40, -35` with Start >= -50, Growth >= 5, Count >= 3
    passes as a 3-quarter series. Negative-to-less-negative is normal
    acceleration."""
    res = accel([-45, -40, -35], start_pct=-50.0, step_pct=5.0, count=3)
    assert res is not None and res.qualifies
    assert res.length == 3
    assert res.start_value == -45.0
    assert res.end_value == -35.0


def test_t3_sign_flip_breaks():
    """T3 - `-8, -3, +4` with Start >= -10, Growth >= 4, Count >= 3.
    `-8 -> -3` is +5 and qualifies; `-3 -> +4` is +7 but flips sign, so
    the longest valid series is 2 quarters -> fail."""
    res = accel([-8, -3, 4], start_pct=-10.0, step_pct=4.0, count=3)
    assert res is not None
    assert not res.qualifies
    assert res.length == 2


def test_t4_single_gap_bridges():
    """T4 - Q1=10, Q2=missing, Q3=20, Q4=26 with Start >= 5, Growth >= 5,
    Count >= 3. Bridges Q2; the series is the 3 quarters that have
    data."""
    res = accel([10, None, 20, 26], start_pct=5.0, step_pct=5.0, count=3)
    assert res is not None and res.qualifies
    assert res.length == 3
    assert res.start_value == 10.0
    assert res.end_value == 26.0


def test_t5_double_gap_breaks():
    """T5 - Q1=10, Q2=missing, Q3=missing, Q4=20. Two consecutive missing
    periods break the chain -> no series of 3."""
    res = accel([10, None, None, 20], start_pct=5.0, step_pct=5.0, count=3)
    assert res is not None
    assert not res.qualifies
    assert res.length == 1


def test_t6_backward_only_strict_anchor():
    """T6 - `10, 16, 22, 28, 25` with Start >= 5, Growth >= 5, Count >= 3.
    The anchor is Q5; Q4 -> Q5 is -3, so nothing terminates at the
    anchor. Fails despite a valid 4-quarter series at Q1-Q4 - confirms
    there is no anchor fallback."""
    values = [10, 16, 22, 28, 25]
    res = accel(values, start_pct=5.0, step_pct=5.0, count=3,
                backward_only=True)
    assert res is not None
    assert not res.qualifies
    assert res.length == 1
    assert res.end_value == 25.0

    # Control: without Backward Only the same data passes with 4 quarters.
    free = accel(values, start_pct=5.0, step_pct=5.0, count=3)
    assert free is not None and free.qualifies
    assert free.length == 4


def test_t7_accelerates_up_through_starting_threshold():
    """T7 - `2, 9, 16, 23, 30` with Start >= 15, Growth >= 5, Count >= 3.
    The chain is unbroken Q1-Q5; the series begins at the first quarter
    clearing 15, which is Q3. An implementation that rejects the chain
    because its oldest quarter is below the threshold fails this."""
    res = accel([2, 9, 16, 23, 30], start_pct=15.0, step_pct=5.0, count=3)
    assert res is not None and res.qualifies
    assert res.length == 3
    assert res.start_value == 16.0
    assert res.end_value == 30.0


def test_t7b_below_threshold_quarters_do_not_count():
    """T7b - `2, 9, 16` with the same params. The series begins at Q3 and
    is 1 quarter long -> fail. Quarters below the starting threshold do
    not count toward Minimum Series Count."""
    res = accel([2, 9, 16], start_pct=15.0, step_pct=5.0, count=3)
    assert res is not None
    assert not res.qualifies
    assert res.length == 1
    assert res.start_value == 16.0


def test_t8_percentage_points_not_relative():
    """T8 - `20 -> 25` with Growth >= 5 qualifies (+5 points). The
    relative-change reading would demand 21 and reject this."""
    res = accel([20, 25], start_pct=0.0, step_pct=5.0, count=2)
    assert res is not None and res.qualifies
    assert res.length == 2

    # And the relative reading is definitively not implemented: a +5.0
    # point step off a base of 20 is exactly at the threshold.
    just_under = accel([20, 24.99], start_pct=0.0, step_pct=5.0, count=2)
    assert just_under is not None and not just_under.qualifies


# ----------------------------------------------------------------------
# Part 3 - shared rules
# ----------------------------------------------------------------------

def test_thresholds_are_inclusive():
    """3.3 - a value exactly equal to the threshold passes, on both the
    starting threshold and the step threshold."""
    res = accel([10, 15, 20], start_pct=10.0, step_pct=5.0, count=3)
    assert res is not None and res.qualifies
    assert res.length == 3


def test_zero_is_neither_positive_nor_negative():
    """3.2 assumption - only a strict `V(n-1) < 0` paired with a strict
    `V(n) > 0` breaks the chain, so `-5 -> 0` and `0 -> +5` both hold."""
    res = accel([-5, 0, 5], start_pct=-10.0, step_pct=5.0, count=3)
    assert res is not None and res.qualifies
    assert res.length == 3


def test_sign_flip_breaks_regardless_of_step_size():
    """A huge negative-to-positive move still breaks the chain."""
    res = accel([-1, 500], start_pct=-10.0, step_pct=5.0, count=2)
    assert res is not None
    assert not res.qualifies
    assert res.length == 1


def test_step_below_growth_threshold_breaks():
    res = accel([10, 14, 20], start_pct=0.0, step_pct=5.0, count=3)
    assert res is not None
    assert not res.qualifies
    assert res.length == 2
    # The surviving 2-quarter series is the newest pair (14 -> 20).
    assert res.start_value == 14.0
    assert res.end_value == 20.0


def test_no_quarter_clears_starting_threshold():
    """A chain whose every quarter sits below the starting threshold
    yields no series at all."""
    res = accel([1, 6, 11], start_pct=50.0, step_pct=5.0, count=3)
    assert res is None


def test_empty_pool_returns_none():
    assert es.accelerating_series(
        [], min_start_pct=0.0, min_step_pct=5.0, min_count=3,
    ) is None


def test_longest_tie_breaks_to_newest_terminal():
    """Two 3-quarter series of equal length -> the newer-ending one
    wins."""
    # 10,15,20 then a break (-5), then 30,35,40.
    res = accel([10, 15, 20, -5, 30, 35, 40],
                start_pct=0.0, step_pct=5.0, count=3)
    assert res is not None and res.qualifies
    assert res.length == 3
    assert res.start_value == 30.0
    assert res.end_value == 40.0


def test_most_recent_need_not_end_on_the_newest_quarter():
    """`Most Recent` ranks by recency; it does not require the series to
    terminate on the newest quarter in the pool. Here the newest quarter
    (Q5) terminates only a 1-quarter series, so the newest *qualifying*
    series (Q1-Q4) is returned - the distinction from Backward Only."""
    values = [10, 16, 22, 28, 25]
    res = accel(values, start_pct=5.0, step_pct=5.0, count=3,
                selection=es.SELECT_MOST_RECENT)
    assert res is not None and res.qualifies
    assert res.length == 4
    assert res.end_value == 28.0


# ----------------------------------------------------------------------
# Part 1 - consecutive YoY growth
# ----------------------------------------------------------------------

def test_growth_run_counts_longest_run_anywhere():
    """Spec Part 1: "the stock passes if there exists a run of hits" -
    the longest run anywhere in the window, not only the trailing one."""
    # hits at 20,25,30 (run of 3), then a miss, then 40 (run of 1).
    points = make_points([20, 25, 30, 1, 40])
    assert es.consecutive_growth_run(points, 10.0) == 3


def test_growth_run_threshold_is_inclusive():
    points = make_points([10, 10, 10])
    assert es.consecutive_growth_run(points, 10.0) == 3
    assert es.consecutive_growth_run(points, 10.01) == 0


def test_growth_run_bridges_one_missing_period():
    points = make_points([20, None, 25, 30])
    assert es.consecutive_growth_run(points, 10.0) == 3


def test_growth_run_breaks_on_two_missing_periods():
    points = make_points([20, None, None, 25, 30])
    assert es.consecutive_growth_run(points, 10.0) == 2


def test_growth_run_breaks_on_sign_flip():
    """A negative-to-positive step breaks the run even when both
    quarters clear the threshold."""
    points = make_points([-5, 20, 25])
    assert es.consecutive_growth_run(points, -10.0) == 2


def test_growth_run_negative_threshold_allows_contraction_streak():
    points = make_points([-45, -40, -35])
    assert es.consecutive_growth_run(points, -50.0) == 3


def test_growth_run_empty_pool():
    assert es.consecutive_growth_run([], 0.0) == 0


# ----------------------------------------------------------------------
# Pool construction and the quarter cap (3.4)
# ----------------------------------------------------------------------

def test_build_quarter_points_orders_oldest_to_newest():
    hist = make_history([1, 2, 3, 4])
    points = es.build_quarter_points(hist, "yoy_eps_pct")
    assert [p.value for p in points] == [1.0, 2.0, 3.0, 4.0]
    assert all(p.missing_before == 0 for p in points)


def test_quarter_cap_keeps_the_most_recently_reported_quarters():
    """The cap defines the pool: the N most recently reported quarters,
    sliced on the same report_date DESC ordering the beats Q Cap uses."""
    hist = make_history([1, 2, 3, 4, 5, 6])
    capped = es.build_quarter_points(hist, "yoy_eps_pct", quarter_cap=3)
    assert [p.value for p in capped] == [4.0, 5.0, 6.0]


def test_quarter_cap_zero_means_no_cap():
    hist = make_history([1, 2, 3, 4, 5, 6])
    points = es.build_quarter_points(hist, "yoy_eps_pct", quarter_cap=0)
    assert len(points) == 6


def test_quarter_cap_bounds_the_reachable_series():
    """3.4 - nothing outside the pool is reachable, so a cap shorter than
    a qualifying series truncates it."""
    hist = make_history([10, 15, 20, 25])
    full = es.accelerating_series(
        es.build_quarter_points(hist, "yoy_eps_pct"),
        min_start_pct=0.0, min_step_pct=5.0, min_count=3,
    )
    assert full is not None and full.length == 4
    capped = es.accelerating_series(
        es.build_quarter_points(hist, "yoy_eps_pct", quarter_cap=2),
        min_start_pct=0.0, min_step_pct=5.0, min_count=3,
    )
    assert capped is not None and not capped.qualifies
    assert capped.length == 2


def test_nan_metric_value_is_a_missing_slot_not_a_point():
    """A row that exists but has no value for the metric is treated
    exactly like an absent row (3.1)."""
    hist = make_history([10, 99, 20, 26])
    hist.loc[hist["yoy_eps_pct"] == 99.0, "yoy_eps_pct"] = float("nan")
    points = es.build_quarter_points(hist, "yoy_eps_pct")
    assert [p.value for p in points] == [10.0, 20.0, 26.0]
    assert [p.missing_before for p in points] == [0, 1, 0]
    res = es.accelerating_series(
        points, min_start_pct=5.0, min_step_pct=5.0, min_count=3,
    )
    assert res is not None and res.qualifies and res.length == 3


def test_absent_row_and_nan_row_are_indistinguishable():
    nan_hist = make_history([10, 99, 20, 26])
    nan_hist.loc[nan_hist["yoy_eps_pct"] == 99.0, "yoy_eps_pct"] = float("nan")
    absent_hist = make_history([10, None, 20, 26])
    a = es.build_quarter_points(nan_hist, "yoy_eps_pct")
    b = es.build_quarter_points(absent_hist, "yoy_eps_pct")
    assert [(p.value, p.missing_before) for p in a] == \
           [(p.value, p.missing_before) for p in b]


def test_missing_metric_column_yields_empty_pool():
    hist = make_history([1, 2, 3], metric="yoy_eps_pct")
    assert es.build_quarter_points(hist, "surprise_eps_pct") == []


def test_missing_period_ending_column_yields_empty_pool():
    hist = make_history([1, 2, 3]).drop(columns=["period_ending"])
    assert es.build_quarter_points(hist, "yoy_eps_pct") == []


def test_none_and_empty_history_yield_empty_pool():
    assert es.build_quarter_points(None, "yoy_eps_pct") == []
    assert es.build_quarter_points(pd.DataFrame(), "yoy_eps_pct") == []


def test_duplicate_period_is_counted_once():
    """Defensive: a duplicated fiscal slot must not read as two
    quarters."""
    hist = make_history([10, 15, 20])
    dup = pd.concat([hist, hist.iloc[[0]]], ignore_index=True)
    dup = dup.sort_values("report_date", ascending=False).reset_index(drop=True)
    points = es.build_quarter_points(dup, "yoy_eps_pct")
    assert len(points) == 3


def test_nat_period_row_is_dropped_without_opening_a_hole():
    hist = make_history([10, 15, 20])
    orphan = pd.DataFrame([{
        "ticker": "TEST", "period_ending": pd.NaT,
        "report_date": pd.Timestamp("2021-01-01"), "yoy_eps_pct": 12.0,
    }]).astype(hist.dtypes.to_dict())
    hist = pd.concat([hist, orphan], ignore_index=True)
    points = es.build_quarter_points(hist, "yoy_eps_pct")
    assert [p.value for p in points] == [10.0, 15.0, 20.0]
    assert all(p.missing_before == 0 for p in points)


@pytest.mark.parametrize("months, expected_steps", [
    (3, 1), (4, 1), (2, 1), (6, 2), (7, 2), (9, 3), (12, 4),
])
def test_period_steps_month_arithmetic(months, expected_steps):
    """A 52/53-week filer's period_ending drifts by up to a month; the
    step count must round to the nearest whole quarter."""
    a = pd.Timestamp("2024-03-01")
    b = a + pd.DateOffset(months=months)
    assert es._period_steps(a, b) == expected_steps


def test_period_steps_non_positive_delta():
    a = pd.Timestamp("2024-03-01")
    assert es._period_steps(a, a) == 0
