"""Series selectors across all three filter types (v6.3.0).

Beats, Consecutive YoY Growth and Accelerating Quarters now share one
primitive (`earnings_series.run_series` / `accelerating_series`) and answer
the same three questions: which run is chosen (Series), must it still be live
(Backward Only), and how many missing quarters may it bridge
(`config.SERIES_MAX_BRIDGED_*`, Settings -> Advanced).

The defaults are chosen so an upgrade changes nothing:

    beats  = backward_only=True,  bridge 0, strict  >
    growth = selection='longest', bridge 1, inclusive >=
    accel  = selection='longest', bridge 1
"""
from __future__ import annotations

import pandas as pd
import pytest

from trade_scanner_fh import config
from trade_scanner_fh import earnings_history as eh
from trade_scanner_fh import earnings_series as es
from trade_scanner_fh import scanner as sc
from trade_scanner_fh.scanner import ScanParams


@pytest.fixture(autouse=True)
def _restore_bridging():
    """`load_user_config` writes straight onto the config module, so a test
    that exercises the persisted knobs would otherwise leak its values into
    every test that runs after it in the session."""
    saved = {k: getattr(config, k) for k in config.SERIES_BRIDGE_KEYS}
    yield
    for k, v in saved.items():
        setattr(config, k, v)


def _pts(values, *, start="2020-03-01", skip=()):
    """Quarter points from a value list, oldest first. `skip` names 0-based
    positions to omit entirely, creating real holes on the fiscal grid."""
    out = []
    base = pd.Timestamp(start)
    prev = None
    for i, v in enumerate(values):
        if i in skip:
            continue
        period = base + pd.DateOffset(months=3 * i)
        missing = 0 if prev is None else max(
            0, es._period_steps(prev, period) - 1)
        out.append(es.QuarterPoint(
            value=float("nan") if v is None else float(v),
            period=period,
            report_date=period + pd.Timedelta(days=30),
            missing_before=missing,
        ))
        prev = period
    return out


# ── selection modes ────────────────────────────────────────────────────

def test_longest_picks_the_longest_run_wherever_it_sits():
    # oldest -> newest: a 4-run, a break, then a 2-run at the end.
    pts = _pts([10, 10, 10, 10, -5, 10, 10])
    r = es.run_series(pts, threshold=0, min_count=3, max_bridged=0)
    assert r.length == 4
    assert r.end_period == pts[3].period


def test_most_recent_picks_the_newest_run_even_when_shorter():
    pts = _pts([10, 10, 10, 10, -5, 10, 10])
    r = es.run_series(pts, threshold=0, min_count=2, max_bridged=0,
                      selection=es.SELECT_MOST_RECENT)
    assert r.length == 2
    assert r.end_period == pts[-1].period


def test_longest_breaks_ties_toward_the_newer_run():
    pts = _pts([10, 10, -5, 10, 10])
    r = es.run_series(pts, threshold=0, min_count=2, max_bridged=0)
    assert r.length == 2
    assert r.end_period == pts[-1].period


def test_backward_only_requires_the_run_to_reach_the_newest_quarter():
    """The whole point of the selector: a run that ended a year ago is not a
    live streak, however long it was."""
    pts = _pts([10, 10, 10, 10, 10, 10, -5])
    assert es.run_series(pts, threshold=0, min_count=3,
                         max_bridged=0).length == 6
    r = es.run_series(pts, threshold=0, min_count=3, max_bridged=0,
                      backward_only=True)
    assert r.length == 0 and not r.qualifies


def test_backward_only_counts_a_live_run():
    pts = _pts([-5, 10, 10, 10])
    r = es.run_series(pts, threshold=0, min_count=3, max_bridged=0,
                      backward_only=True)
    assert r.length == 3 and r.qualifies


def test_backward_only_ignores_the_selection_mode():
    pts = _pts([10, 10, 10, -5, 10, 10])
    a = es.run_series(pts, threshold=0, min_count=2, max_bridged=0,
                      backward_only=True, selection=es.SELECT_LONGEST)
    b = es.run_series(pts, threshold=0, min_count=2, max_bridged=0,
                      backward_only=True, selection=es.SELECT_MOST_RECENT)
    assert a.length == b.length == 2


# ── bridging ───────────────────────────────────────────────────────────

def test_bridge_zero_breaks_on_any_hole():
    pts = _pts([10, 10, 10, 10], skip=(1,))     # one quarter absent
    assert es.run_series(pts, threshold=0, min_count=1,
                         max_bridged=0).length == 2


def test_bridge_one_spans_a_single_hole_without_counting_it():
    pts = _pts([10, 10, 10, 10], skip=(1,))
    r = es.run_series(pts, threshold=0, min_count=1, max_bridged=1)
    assert r.length == 3, "the bridged quarter must not count toward length"


def test_bridge_one_still_breaks_on_two_holes():
    pts = _pts([10, 10, 10, 10, 10], skip=(1, 2))
    assert es.run_series(pts, threshold=0, min_count=1,
                         max_bridged=1).length == 2


def test_bridge_two_spans_two_holes():
    pts = _pts([10, 10, 10, 10, 10], skip=(1, 2))
    assert es.run_series(pts, threshold=0, min_count=1,
                         max_bridged=2).length == 3


# ── threshold and sign rules ───────────────────────────────────────────

def test_inclusive_and_strict_thresholds_differ_exactly_at_the_bar():
    pts = _pts([5, 5, 5])
    assert es.run_series(pts, threshold=5, min_count=1,
                         inclusive=True, max_bridged=0).length == 3
    assert es.run_series(pts, threshold=5, min_count=1,
                         inclusive=False, max_bridged=0).length == 0


def test_negative_to_positive_step_breaks_the_run():
    pts = _pts([-10, 10])
    assert es.run_series(pts, threshold=-99, min_count=1,
                         max_bridged=0).length == 1


def test_nan_is_never_a_hit():
    pts = _pts([10, None, 10])
    r = es.run_series(pts, threshold=0, min_count=1, max_bridged=0)
    assert r.length == 1


def test_empty_pool_returns_none():
    assert es.run_series([], threshold=0, min_count=1) is None


def test_no_qualifying_run_still_reports_a_length_for_display_only():
    pts = _pts([10, 10])
    r = es.run_series(pts, threshold=0, min_count=5, max_bridged=0)
    assert r.length == 2 and not r.qualifies


# ── keep_valueless: a NaN quarter is a miss, not a hole ────────────────

def test_keep_valueless_makes_a_null_quarter_present_and_fatal():
    hist = pd.DataFrame({
        "ticker": ["T"] * 3,
        "period_ending": pd.to_datetime(
            ["2026-06-01", "2026-03-01", "2025-12-01"]),
        "report_date": pd.to_datetime(
            ["2026-07-01", "2026-04-01", "2026-01-01"]),
        "surprise_eps_pct": [None, 10.0, 10.0],
    })
    dropped = es.build_quarter_points(hist, "surprise_eps_pct")
    kept = es.build_quarter_points(hist, "surprise_eps_pct",
                                   keep_valueless=True)
    assert len(dropped) == 2 and len(kept) == 3
    # Dropping it lets the streak look live; keeping it correctly kills it.
    assert es.run_series(dropped, threshold=0, min_count=1, inclusive=False,
                         max_bridged=0, backward_only=True).length == 2
    assert es.run_series(kept, threshold=0, min_count=1, inclusive=False,
                         max_bridged=0, backward_only=True).length == 0


# ── defaults preserve the historical behaviour ─────────────────────────

def test_scanparams_defaults_match_each_types_prior_behaviour():
    p = ScanParams()
    assert p.consec_eps_beats_backward_only is True
    assert p.consec_rev_beats_backward_only is True
    assert p.consec_eps_growth_backward_only is False
    assert p.consec_rev_growth_backward_only is False
    assert p.consec_eps_growth_selection == "longest"
    assert config.SERIES_MAX_BRIDGED_BEATS == 0
    assert config.SERIES_MAX_BRIDGED_GROWTH == 1
    assert config.SERIES_MAX_BRIDGED_ACCEL == 1


def _hist(values, *, col="surprise_eps_pct", start="2021-03-01"):
    """report_date-DESC frame in the shape `_compute_ticker` hands the beats
    helper: newest first."""
    n = len(values)
    periods = [pd.Timestamp(start) + pd.DateOffset(months=3 * i)
               for i in range(n)]
    rows = [{
        "ticker": "T", "period_ending": p,
        "report_date": p + pd.Timedelta(days=30),
        col: v, "report_date_proxy": False,
    } for p, v in zip(periods, values)]
    return pd.DataFrame(rows).sort_values("report_date", ascending=False)


def test_beats_default_matches_compute_consecutive_beats():
    p = ScanParams()
    for values in (
        [5, 5, 5, 5],            # all beats
        [5, -1, 5, 5],           # a miss in the middle
        [5, 5, 5, -1],           # newest is a miss -> 0
        [5, 5, None, 5],         # a null surprise -> a miss
        [0, 5, 5, 5],            # strict >, so 0 is not a beat
    ):
        h = _hist(values)
        assert sc._beats_run(h, "surprise_eps_pct", p, "consec_eps_beats") \
            == eh.compute_consecutive_beats(h, "eps", 0.0), values


def test_beats_unticking_backward_only_finds_an_older_streak():
    """The new capability: a streak that has since broken still reports."""
    p = ScanParams(consec_eps_beats_backward_only=False)
    h = _hist([5, 5, 5, 5, -1])
    assert sc._beats_run(h, "surprise_eps_pct", p, "consec_eps_beats") == 4
    p_live = ScanParams()
    assert sc._beats_run(h, "surprise_eps_pct", p_live,
                         "consec_eps_beats") == 0


def test_growth_default_is_still_longest_run_anywhere():
    pts = _pts([40, 35, 50, 45, 60, 55, -10, -20, -5, -30])
    assert es.consecutive_growth_run(pts, 20.0) == 6


def test_growth_backward_only_rejects_a_dead_run():
    pts = _pts([40, 35, 50, 45, 60, 55, -10, -20, -5, -30])
    assert es.consecutive_growth_run(pts, 20.0, backward_only=True) == 0


# ── the report_date-ordering phantom gap the new path fixes ────────────

def test_out_of_fiscal_order_rows_no_longer_fake_a_missing_quarter():
    """`compute_consecutive_beats` measures the period_ending gap across a
    frame ordered by REPORT_DATE. When a late filing puts two quarters out of
    fiscal sequence, that diff spans two quarters and reads as a hole that
    does not exist — truncating a real streak.

    Live example (2026-09-18): ALRM's 2019 quarters arrive 2019-09, 2019-03,
    2019-06 under report_date DESC; the diff measured 184 days and broke a
    35-quarter streak at 28. Five tickers across both metrics were affected.
    The series path sorts into fiscal order before measuring, so the hole
    never appears.
    """
    rows = [
        {"period_ending": "2019-03-01", "report_date": "2019-05-10"},
        {"period_ending": "2019-06-01", "report_date": "2019-11-20"},  # late
        {"period_ending": "2019-09-01", "report_date": "2019-10-15"},
        {"period_ending": "2019-12-01", "report_date": "2020-02-10"},
    ]
    h = pd.DataFrame([{
        "ticker": "T",
        "period_ending": pd.Timestamp(r["period_ending"]),
        "report_date": pd.Timestamp(r["report_date"]),
        "surprise_eps_pct": 10.0,
        "report_date_proxy": False,
    } for r in rows]).sort_values("report_date", ascending=False)

    # The frame really is out of fiscal order under report_date DESC.
    pe = list(pd.to_datetime(h["period_ending"]).dt.strftime("%Y-%m"))
    assert pe != sorted(pe, reverse=True), "fixture must be out of order"

    assert eh.compute_consecutive_beats(h, "eps", 0.0) < 4, (
        "the phantom gap should truncate the legacy implementation"
    )
    assert sc._beats_run(
        h, "surprise_eps_pct", ScanParams(), "consec_eps_beats") == 4


# ── configurable bridging ──────────────────────────────────────────────

def test_bridging_knobs_are_persisted_and_clamped(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    assert config.save_user_config({
        "SERIES_MAX_BRIDGED_BEATS": 2,
        "SERIES_MAX_BRIDGED_GROWTH": 0,
        "SERIES_MAX_BRIDGED_ACCEL": 99,      # out of range -> clamped
    })
    config.load_user_config()
    assert config.SERIES_MAX_BRIDGED_BEATS == 2
    assert config.SERIES_MAX_BRIDGED_GROWTH == 0
    assert config.SERIES_MAX_BRIDGED_ACCEL == 4


def test_garbage_bridging_value_falls_back_to_the_default(tmp_path, monkeypatch):
    import json
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    (tmp_path / "user_config.json").write_text(
        json.dumps({"SERIES_MAX_BRIDGED_BEATS": "not-a-number"}),
        encoding="utf-8")
    config.load_user_config()
    assert config.SERIES_MAX_BRIDGED_BEATS == 0


def test_accelerating_series_honours_the_bridging_argument():
    pts = _pts([0, 10, 20, 30], skip=(1,))     # one hole
    tight = es.accelerating_series(
        pts, min_start_pct=-999, min_step_pct=5, min_count=2, max_bridged=0)
    loose = es.accelerating_series(
        pts, min_start_pct=-999, min_step_pct=5, min_count=2, max_bridged=1)
    assert tight.length < loose.length


# ── GUI surface ────────────────────────────────────────────────────────

def test_every_series_row_exposes_both_controls(_qapp):
    from trade_scanner_fh.gui.widgets import IndicatorPanel, _SERIES_ROW_KEYS
    panel = IndicatorPanel()
    assert len(_SERIES_ROW_KEYS) == 8
    for key in _SERIES_ROW_KEYS:
        row = panel.rows[key]
        assert "selection" in row.spinboxes, key
        assert "backward_only" in row.spinboxes, key


def test_row_defaults_match_scanparams_defaults(_qapp):
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    panel = IndicatorPanel()
    for key in ("consec_eps_beats", "consec_rev_beats"):
        assert panel.rows[key].value("backward_only") is True, key
    for key in ("consec_eps_growth", "consec_rev_growth",
                "accel_eps_yoy", "accel_rev_surp"):
        assert panel.rows[key].value("backward_only") is False, key


def test_backward_only_greys_the_series_combo_on_every_row(_qapp):
    from trade_scanner_fh.gui.widgets import IndicatorPanel, _SERIES_ROW_KEYS
    panel = IndicatorPanel()
    for key in _SERIES_ROW_KEYS:
        row = panel.rows[key]
        row.spinboxes["backward_only"].setChecked(False)
        assert row.spinboxes["selection"].isEnabled(), key
        row.spinboxes["backward_only"].setChecked(True)
        assert not row.spinboxes["selection"].isEnabled(), key


def test_preset_round_trip_restores_both_controls_and_the_greyout(_qapp):
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    a = IndicatorPanel()
    a.rows["consec_eps_growth"].spinboxes["backward_only"].setChecked(True)
    a.rows["consec_rev_beats"].spinboxes["backward_only"].setChecked(False)
    a.rows["consec_rev_beats"].spinboxes["selection"].setCurrentIndex(1)

    b = IndicatorPanel()
    b.from_dict(a.to_dict())
    assert b.rows["consec_eps_growth"].value("backward_only") is True
    assert b.rows["consec_rev_beats"].value("backward_only") is False
    assert b.rows["consec_rev_beats"].value("selection") == "most_recent"
    # `set_value` drives setChecked, which only emits on a real change — the
    # post-load resync is what keeps the greyout honest.
    assert not b.rows["consec_eps_growth"].spinboxes["selection"].isEnabled()
    assert b.rows["consec_rev_beats"].spinboxes["selection"].isEnabled()


def test_old_preset_without_the_new_keys_still_loads(_qapp):
    """A v6.2.0 preset has no selection/backward_only entries; the rows must
    keep their defaults rather than raising."""
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    a = IndicatorPanel()
    d = a.to_dict()
    for key in ("consec_eps_beats", "consec_eps_growth"):
        d[key].pop("selection", None)
        d[key].pop("backward_only", None)
    b = IndicatorPanel()
    b.from_dict(d)
    assert b.rows["consec_eps_beats"].value("backward_only") is True
    assert b.rows["consec_eps_growth"].value("backward_only") is False


def test_build_scan_params_carries_the_new_fields(_qapp):
    from datetime import date
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    panel = IndicatorPanel()
    panel.rows["consec_eps_growth"].spinboxes["backward_only"].setChecked(True)
    p = panel.build_scan_params(date(2026, 1, 1), date(2026, 9, 18))
    assert p.consec_eps_growth_backward_only is True
    assert p.consec_eps_beats_backward_only is True
    assert p.consec_rev_growth_backward_only is False


# ── a missing quarter never counts toward a length ─────────────────────
#
# Raised 2026-09-19: with bridging in play, could a hole at the START of the
# period plus one real hit be reported as a 2-quarter streak? It cannot, for
# two independent reasons, and both are worth pinning because a future change
# to either would break the guarantee silently.

def test_leading_hole_plus_one_hit_is_a_run_of_one():
    """Reason 1: length counts POINTS, and a missing quarter has no point.
    Bridging only relaxes the continuity test between two real quarters — it
    never contributes a unit of its own."""
    pts = _pts([10, 10], skip=(0,))          # oldest quarter absent
    assert len(pts) == 1
    for bridge in (0, 1, 2, 4):
        assert es.run_series(pts, threshold=0, min_count=1,
                             max_bridged=bridge).length == 1, bridge


def test_leading_hole_never_inflates_a_longer_run_either():
    pts = _pts([10, 10, 10], skip=(0,))
    for bridge in (0, 1, 2, 4):
        assert es.run_series(pts, threshold=0, min_count=1,
                             max_bridged=bridge).length == 2, bridge


def test_missing_before_is_zero_on_the_first_point():
    """Reason 2, specific to a leading hole: there is no previous point to
    measure the gap against, so `missing_before` is 0 and the hole is
    invisible. That is correct — the pool boundary is arbitrary (Q Cap, the
    history window), so "a quarter is missing before the oldest one I can
    see" is not a statement the data supports."""
    pts = _pts([10, 10, 10], skip=(0, 1))    # two leading quarters absent
    assert len(pts) == 1
    assert pts[0].missing_before == 0


def test_a_bridged_interior_hole_is_not_counted_either():
    """The same guarantee mid-run: three real quarters spanning four calendar
    ones report 3, not 4."""
    pts = _pts([10, 10, 10, 10], skip=(1,))
    r = es.run_series(pts, threshold=0, min_count=1, max_bridged=1)
    assert r.length == 3
    # ...and the span it reports really is the wider calendar range.
    assert (r.end_period.year - r.start_period.year) * 12 + (
        r.end_period.month - r.start_period.month) == 9


def test_leading_hole_guarantee_holds_for_beats_and_accel_too():
    """All three types count points, so none of them can inflate."""
    h = _hist([None, 5.0])                   # Q1 present-but-null, Q2 a beat
    assert sc._beats_run(h, "surprise_eps_pct", ScanParams(),
                         "consec_eps_beats") == 1

    pts = _pts([0, 10], skip=(0,))
    acc = es.accelerating_series(pts, min_start_pct=-999, min_step_pct=1,
                                 min_count=1, max_bridged=4)
    assert acc.length == 1
