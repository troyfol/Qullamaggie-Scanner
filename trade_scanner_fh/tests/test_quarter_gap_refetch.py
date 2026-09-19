"""Quarter-gap re-fetch ledger (2026-09-18).

`missing_quarter` reported 525 tickers and offered no way to act on them:
no selective re-fetch, and no memory of what had already been tried, so the
same wall of names came back every run whether or not anything was done. These
cover the detector, the attempt ledger, and the resting window that makes the
finding shrink as you work through it.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from trade_scanner_fh import config
from trade_scanner_fh import earnings_history as eh


def _q(ticker: str, period: str) -> dict:
    """One minimal history row — the detector only reads ticker/period."""
    return {
        "ticker": ticker,
        "period_ending": pd.Timestamp(period),
        "report_date": pd.Timestamp(period) + pd.Timedelta(days=30),
        "report_time": "Close",
        "estimated_eps": None, "reported_eps": 1.0,
        "surprise_eps": None, "surprise_eps_pct": None,
        "estimated_rev": None, "reported_rev": 100.0,
        "surprise_rev": None, "surprise_rev_pct": None,
        "source": "finviz",
        "updated_at": pd.Timestamp("2026-06-01"),
        "report_date_proxy": False,
    }


def _contiguous(ticker: str, n: int = 8) -> list[dict]:
    """`n` quarters at a clean ~91-day cadence, ending this month."""
    end = pd.Timestamp.today().normalize()
    return [_q(ticker, end - pd.Timedelta(days=91 * i)) for i in range(n)]


def _with_hole(ticker: str) -> list[dict]:
    """Quarters bracketing a missing one — a >135-day hole."""
    end = pd.Timestamp.today().normalize()
    return [
        _q(ticker, end),
        _q(ticker, end - pd.Timedelta(days=91)),
        # skip the next quarter entirely -> ~273-day hole
        _q(ticker, end - pd.Timedelta(days=364)),
        _q(ticker, end - pd.Timedelta(days=455)),
    ]


# ── detector ───────────────────────────────────────────────────────────

def test_find_quarter_gap_tickers_reports_widest_hole_per_ticker():
    df = pd.DataFrame(_contiguous("CLEAN") + _with_hole("HOLED"))
    gaps = eh.find_quarter_gap_tickers(df)
    assert "CLEAN" not in gaps
    assert gaps["HOLED"] > eh._MAX_QUARTER_GAP_DAYS


def test_find_quarter_gap_tickers_empty_inputs():
    assert eh.find_quarter_gap_tickers(None) == {}
    assert eh.find_quarter_gap_tickers(pd.DataFrame()) == {}
    assert eh.find_quarter_gap_tickers(pd.DataFrame({"x": [1]})) == {}


def test_detector_and_finding_agree_on_the_ticker_set(tmp_parquets):
    """The GUI queues what `find_quarter_gap_tickers` returns while the text
    the user reads comes from the finding. They must not drift."""
    df = pd.DataFrame(_contiguous("CLEAN") + _with_hole("A") + _with_hole("B"))
    finding = eh._quarter_gap_finding(df)
    assert finding is not None
    assert finding.affected_rows == len(eh.find_quarter_gap_tickers(df)) == 2


# ── ledger ─────────────────────────────────────────────────────────────

def test_ledger_round_trips(tmp_parquets):
    assert eh.load_earnings_gap_attempts() == {}
    eh.record_earnings_gap_attempts(["AAA", "BBB"])
    data = eh.load_earnings_gap_attempts()
    assert set(data) == {"AAA", "BBB"}
    assert data["AAA"] == date.today().isoformat()


def test_unreadable_ledger_degrades_to_empty(tmp_parquets):
    (tmp_parquets / config.EARNINGS_GAP_ATTEMPTS_FILE).write_text(
        "{not json", encoding="utf-8")
    assert eh.load_earnings_gap_attempts() == {}


def test_ledger_of_wrong_shape_degrades_to_empty(tmp_parquets):
    (tmp_parquets / config.EARNINGS_GAP_ATTEMPTS_FILE).write_text(
        '["a", "list"]', encoding="utf-8")
    assert eh.load_earnings_gap_attempts() == {}


def test_clear_all_and_clear_subset(tmp_parquets):
    eh.record_earnings_gap_attempts(["AAA", "BBB", "CCC"])
    assert eh.clear_earnings_gap_attempts(["BBB"]) == 1
    assert set(eh.load_earnings_gap_attempts()) == {"AAA", "CCC"}
    assert eh.clear_earnings_gap_attempts() == 2
    assert eh.load_earnings_gap_attempts() == {}


def test_recording_nothing_is_a_no_op(tmp_parquets):
    eh.record_earnings_gap_attempts([])
    assert eh.load_earnings_gap_attempts() == {}


# ── resting window ─────────────────────────────────────────────────────

def test_recent_attempt_rests_the_ticker(tmp_parquets):
    gaps = {"AAA": 300, "BBB": 200}
    eh.record_earnings_gap_attempts(["AAA"])
    assert eh.select_quarter_gap_refetches(gaps) == ["BBB"]


def test_attempt_older_than_the_window_becomes_eligible_again(tmp_parquets):
    gaps = {"AAA": 300}
    stale = date.today() - timedelta(days=config.EARNINGS_GAP_RECHECK_DAYS + 1)
    eh.record_earnings_gap_attempts(["AAA"], today=stale)
    assert eh.select_quarter_gap_refetches(gaps) == ["AAA"]


def test_attempt_exactly_at_the_window_edge_is_eligible(tmp_parquets):
    """`> cutoff` rests it; an attempt exactly `recheck_days` old is due."""
    gaps = {"AAA": 300}
    edge = date.today() - timedelta(days=config.EARNINGS_GAP_RECHECK_DAYS)
    eh.record_earnings_gap_attempts(["AAA"], today=edge)
    assert eh.select_quarter_gap_refetches(gaps) == ["AAA"]


def test_corrupt_stamp_re_offers_rather_than_hiding_forever(tmp_parquets):
    import json
    (tmp_parquets / config.EARNINGS_GAP_ATTEMPTS_FILE).write_text(
        json.dumps({"AAA": "not-a-date"}), encoding="utf-8")
    assert eh.select_quarter_gap_refetches({"AAA": 300}) == ["AAA"]


def test_selection_is_widest_hole_first(tmp_parquets):
    gaps = {"NARROW": 140, "WIDEST": 900, "MID": 400}
    assert eh.select_quarter_gap_refetches(gaps) == ["WIDEST", "MID", "NARROW"]


def test_selection_respects_an_explicit_limit(tmp_parquets):
    gaps = {"A": 900, "B": 400, "C": 200}
    assert eh.select_quarter_gap_refetches(gaps, limit=2) == ["A", "B"]


def test_selection_is_uncapped_by_default(tmp_parquets):
    gaps = {f"T{i}": 200 + i for i in range(300)}
    assert len(eh.select_quarter_gap_refetches(gaps)) == 300


# ── the finding honours the ledger ─────────────────────────────────────

def test_finding_hides_rested_tickers_and_says_how_many(tmp_parquets):
    df = pd.DataFrame(_with_hole("AAA") + _with_hole("BBB"))
    assert eh._quarter_gap_finding(df).affected_rows == 2

    eh.record_earnings_gap_attempts(["AAA"])
    finding = eh._quarter_gap_finding(df)
    assert finding.affected_rows == 1
    assert "1 ticker(s) are resting" in finding.description


def test_finding_disappears_once_every_ticker_is_rested(tmp_parquets):
    df = pd.DataFrame(_with_hole("AAA"))
    eh.record_earnings_gap_attempts(["AAA"])
    assert eh._quarter_gap_finding(df) is None


def test_unfiltered_view_still_available(tmp_parquets):
    """The ledger is a work-queue convenience, not a way to lose the truth."""
    df = pd.DataFrame(_with_hole("AAA"))
    eh.record_earnings_gap_attempts(["AAA"])
    unfiltered = eh._quarter_gap_finding(df, apply_attempt_ledger=False)
    assert unfiltered is not None and unfiltered.affected_rows == 1


def test_verify_integrity_surfaces_the_filtered_finding(tmp_parquets):
    df = pd.DataFrame(_contiguous("CLEAN") + _with_hole("AAA"))
    checks = {f.check for f in eh.verify_integrity(history_df=df)}
    assert "missing_quarter" in checks
    eh.record_earnings_gap_attempts(["AAA"])
    checks = {f.check for f in eh.verify_integrity(history_df=df)}
    assert "missing_quarter" not in checks
