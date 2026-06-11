"""Tests for the daily smart-refresh candidate selector
(`earnings_history.find_smart_refresh_candidates`).

Simplified, source-neutral rules (drives finviz / zacks / finnhub):
  A. No earnings history at all → candidate.
  B. Calendar `last_earnings` (most-recent PAST report) is newer than the
     most-recent report we've captured → candidate.
  C. No `last_earnings` in the calendar AND latest captured report is
     > EARNINGS_REFRESH_NOCAL_STALE_DAYS (90) days old → candidate.
Re-poll guard (B and C only): a ticker fetched within
EARNINGS_REFRESH_RECHECK_GUARD_DAYS (5) days is not re-queued.
"""
from __future__ import annotations

import pandas as pd

from trade_scanner_fh import (
    earnings_cache as ec,
    earnings_history as eh,
)


# ──────────────────────────────────────────────────────────────────────
# Fixtures + helpers
# ──────────────────────────────────────────────────────────────────────

TODAY = pd.Timestamp("2026-04-30")
# An "old" fetch time well outside the re-poll guard window — the default
# so staleness tests aren't accidentally suppressed by the guard.
OLD_FETCH = pd.Timestamp("2026-01-01")


def _hist(ticker: str, report_date: str, *, updated: pd.Timestamp = OLD_FETCH) -> dict:
    """One history row. `updated` is the fetch time the re-poll guard
    keys on — defaults to OLD_FETCH so it never trips the guard."""
    return {
        "ticker": ticker,
        "period_ending": pd.Timestamp(report_date) - pd.Timedelta(days=30),
        "report_date": pd.Timestamp(report_date),
        "report_time": "Close",
        "estimated_eps": 2.0, "reported_eps": 2.1,
        "surprise_eps": 0.1, "surprise_eps_pct": 5.0,
        "estimated_rev": 100.0, "reported_rev": 105.0,
        "surprise_rev": 5.0, "surprise_rev_pct": 5.0,
        "source": "zacks",
        "updated_at": updated,
    }


def _dates(ticker: str, last: str | None) -> dict:
    return {
        "ticker": ticker,
        "last_earnings": pd.Timestamp(last) if last else pd.NaT,
        "next_earnings": pd.NaT,  # selector no longer reads this
        "updated_at": pd.Timestamp("2026-01-01"),
    }


# ──────────────────────────────────────────────────────────────────────
# Rule A — gap fill
# ──────────────────────────────────────────────────────────────────────

def test_ticker_with_no_history_is_a_candidate(tmp_parquets):
    cand = eh.find_smart_refresh_candidates(["AAPL"], blacklist=set(), today=TODAY)
    assert cand == ["AAPL"]


def test_blacklist_excludes_gap_ticker(tmp_parquets):
    cand = eh.find_smart_refresh_candidates(["AAPL"], blacklist={"AAPL"}, today=TODAY)
    assert cand == []


def test_multiple_gap_tickers_all_returned(tmp_parquets):
    cand = eh.find_smart_refresh_candidates(
        ["AAPL", "MSFT", "GOOG"], blacklist=set(), today=TODAY,
    )
    assert cand == ["AAPL", "GOOG", "MSFT"]  # sorted


# ──────────────────────────────────────────────────────────────────────
# Rule B — calendar reports a quarter newer than our latest capture
# ──────────────────────────────────────────────────────────────────────

def test_last_earnings_newer_than_capture_is_candidate(tmp_parquets):
    eh.save_earnings_history(pd.DataFrame([_hist("AAPL", "2026-01-15")]))
    ec.save_earnings_cache(pd.DataFrame([_dates("AAPL", "2026-04-20")]))
    cand = eh.find_smart_refresh_candidates(["AAPL"], blacklist=set(), today=TODAY)
    assert cand == ["AAPL"]


def test_last_earnings_equal_to_capture_is_not_candidate(tmp_parquets):
    """We already hold the most-recent reported quarter → up to date."""
    eh.save_earnings_history(pd.DataFrame([_hist("AAPL", "2026-04-20")]))
    ec.save_earnings_cache(pd.DataFrame([_dates("AAPL", "2026-04-20")]))
    cand = eh.find_smart_refresh_candidates(["AAPL"], blacklist=set(), today=TODAY)
    assert cand == []


def test_last_earnings_older_than_capture_is_not_candidate(tmp_parquets):
    """We're ahead of the calendar (it lags) → nothing to fetch."""
    eh.save_earnings_history(pd.DataFrame([_hist("AAPL", "2026-04-20")]))
    ec.save_earnings_cache(pd.DataFrame([_dates("AAPL", "2026-01-15")]))
    cand = eh.find_smart_refresh_candidates(["AAPL"], blacklist=set(), today=TODAY)
    assert cand == []


def test_last_earnings_with_no_real_capture_is_candidate(tmp_parquets):
    """Only a FUTURE-dated placeholder row → no real capture → behind."""
    eh.save_earnings_history(pd.DataFrame([_hist("AAPL", "2026-09-01")]))  # future
    ec.save_earnings_cache(pd.DataFrame([_dates("AAPL", "2026-04-20")]))
    cand = eh.find_smart_refresh_candidates(["AAPL"], blacklist=set(), today=TODAY)
    assert cand == ["AAPL"]


def test_future_placeholder_does_not_mask_staleness(tmp_parquets):
    """A real capture (2026-01) plus a future placeholder (2026-09): the
    future row must NOT count as 'captured', so last_earnings 2026-04 still
    reads as newer → candidate."""
    eh.save_earnings_history(pd.DataFrame([
        _hist("AAPL", "2026-01-15"),
        _hist("AAPL", "2026-09-01"),  # future placeholder
    ]))
    ec.save_earnings_cache(pd.DataFrame([_dates("AAPL", "2026-04-20")]))
    cand = eh.find_smart_refresh_candidates(["AAPL"], blacklist=set(), today=TODAY)
    assert cand == ["AAPL"]


# ──────────────────────────────────────────────────────────────────────
# Re-poll guard
# ──────────────────────────────────────────────────────────────────────

def test_recent_fetch_suppresses_otherwise_due_ticker(tmp_parquets):
    """Behind per Rule B, but fetched 2 days ago → guard suppresses it.
    The uncaptured report (2026-02-10) is OLDER than the fresh window
    (21d), so the uncaptured-fresh bypass does NOT apply and the guard
    still wins."""
    eh.save_earnings_history(pd.DataFrame([
        _hist("AAPL", "2026-01-15", updated=TODAY - pd.Timedelta(days=2)),
    ]))
    ec.save_earnings_cache(pd.DataFrame([_dates("AAPL", "2026-02-10")]))
    cand = eh.find_smart_refresh_candidates(["AAPL"], blacklist=set(), today=TODAY)
    assert cand == []


def test_fetch_just_outside_guard_window_is_candidate(tmp_parquets):
    """Fetched 6 days ago (guard is 5) → re-queued."""
    eh.save_earnings_history(pd.DataFrame([
        _hist("AAPL", "2026-01-15", updated=TODAY - pd.Timedelta(days=6)),
    ]))
    ec.save_earnings_cache(pd.DataFrame([_dates("AAPL", "2026-04-20")]))
    cand = eh.find_smart_refresh_candidates(["AAPL"], blacklist=set(), today=TODAY)
    assert cand == ["AAPL"]


def test_guard_does_not_apply_to_gap_tickers(tmp_parquets):
    """Rule A (no history) has no fetch time, so the guard can't suppress
    a brand-new ticker."""
    cand = eh.find_smart_refresh_candidates(["NEW"], blacklist=set(), today=TODAY)
    assert cand == ["NEW"]


# ──────────────────────────────────────────────────────────────────────
# Uncaptured-fresh bypass — a recently-reported quarter we haven't
# captured keeps retrying every launch despite the re-poll guard
# ──────────────────────────────────────────────────────────────────────

def test_uncaptured_fresh_report_bypasses_guard(tmp_parquets):
    """The BBCP case: captured quarter is old (2026-01-15), the calendar
    shows a recent report (2026-04-20, 10d ago — within the 21d fresh
    window) we haven't captured, and we fetched only 2 days ago (inside the
    guard). The bypass keeps it a candidate so we retry until the source
    publishes the actual."""
    eh.save_earnings_history(pd.DataFrame([
        _hist("BBCP", "2026-01-15", updated=TODAY - pd.Timedelta(days=2)),
    ]))
    ec.save_earnings_cache(pd.DataFrame([_dates("BBCP", "2026-04-20")]))
    cand = eh.find_smart_refresh_candidates(["BBCP"], blacklist=set(), today=TODAY)
    assert cand == ["BBCP"]


def test_uncaptured_fresh_with_no_prior_capture_bypasses_guard(tmp_parquets):
    """No real capture (only a future placeholder row) + a recent calendar
    report + a recent fetch → still a candidate via the bypass."""
    eh.save_earnings_history(pd.DataFrame([
        _hist("BBCP", "2026-09-01", updated=TODAY - pd.Timedelta(days=1)),  # future
    ]))
    ec.save_earnings_cache(pd.DataFrame([_dates("BBCP", "2026-04-20")]))
    cand = eh.find_smart_refresh_candidates(["BBCP"], blacklist=set(), today=TODAY)
    assert cand == ["BBCP"]


def test_uncaptured_report_outside_fresh_window_respects_guard(tmp_parquets):
    """Boundary: the uncaptured report is 22 days old (just past the 21d
    fresh window) and we fetched 2 days ago → bypass does NOT apply, the
    guard suppresses it (falls back to the guarded cadence so a
    permanently-uncoverable name can't churn forever)."""
    le = TODAY - pd.Timedelta(days=22)
    eh.save_earnings_history(pd.DataFrame([
        _hist("BBCP", "2026-01-15", updated=TODAY - pd.Timedelta(days=2)),
    ]))
    ec.save_earnings_cache(pd.DataFrame([_dates("BBCP", str(le.date()))]))
    cand = eh.find_smart_refresh_candidates(["BBCP"], blacklist=set(), today=TODAY)
    assert cand == []


# ──────────────────────────────────────────────────────────────────────
# Rule C — no calendar event, fixed quarterly cadence
# ──────────────────────────────────────────────────────────────────────

def test_no_last_earnings_and_stale_capture_is_candidate(tmp_parquets):
    """No calendar row + last capture > 90d ago → candidate."""
    eh.save_earnings_history(pd.DataFrame([_hist("AAPL", "2026-01-10")]))  # ~110d
    cand = eh.find_smart_refresh_candidates(["AAPL"], blacklist=set(), today=TODAY)
    assert cand == ["AAPL"]


def test_no_last_earnings_nat_and_stale_capture_is_candidate(tmp_parquets):
    """Calendar row present but last_earnings is NaT → treated as 'no
    calendar event' → Rule C."""
    eh.save_earnings_history(pd.DataFrame([_hist("AAPL", "2026-01-10")]))
    ec.save_earnings_cache(pd.DataFrame([_dates("AAPL", None)]))
    cand = eh.find_smart_refresh_candidates(["AAPL"], blacklist=set(), today=TODAY)
    assert cand == ["AAPL"]


def test_no_last_earnings_and_fresh_capture_is_not_candidate(tmp_parquets):
    """No calendar row + last capture only 30d ago (< 90) → skip."""
    eh.save_earnings_history(pd.DataFrame([
        _hist("AAPL", str((TODAY - pd.Timedelta(days=30)).date())),
    ]))
    cand = eh.find_smart_refresh_candidates(["AAPL"], blacklist=set(), today=TODAY)
    assert cand == []


def test_rule_c_respects_repoll_guard(tmp_parquets):
    """Rule-C stale, but fetched 1 day ago → guard suppresses."""
    eh.save_earnings_history(pd.DataFrame([
        _hist("AAPL", "2026-01-10", updated=TODAY - pd.Timedelta(days=1)),
    ]))
    cand = eh.find_smart_refresh_candidates(["AAPL"], blacklist=set(), today=TODAY)
    assert cand == []


# ──────────────────────────────────────────────────────────────────────
# Mixed cases & invariants
# ──────────────────────────────────────────────────────────────────────

def test_mixed_universe_returns_only_due(tmp_parquets):
    """AAPL gap, GOOG behind (Rule B), MSFT up to date → only AAPL+GOOG."""
    eh.save_earnings_history(pd.DataFrame([
        _hist("MSFT", "2026-02-25"),   # captured == last_earnings below
        _hist("GOOG", "2026-01-10"),   # behind last_earnings below
    ]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates("MSFT", "2026-02-25"),  # equal → not due
        _dates("GOOG", "2026-04-22"),  # newer → due
    ]))
    cand = eh.find_smart_refresh_candidates(
        ["AAPL", "MSFT", "GOOG"], blacklist=set(), today=TODAY,
    )
    assert cand == ["AAPL", "GOOG"]


def test_empty_universe_returns_empty(tmp_parquets):
    assert eh.find_smart_refresh_candidates([], blacklist=set(), today=TODAY) == []


def test_blacklist_excludes_due_ticker(tmp_parquets):
    eh.save_earnings_history(pd.DataFrame([_hist("AAPL", "2026-01-15")]))
    ec.save_earnings_cache(pd.DataFrame([_dates("AAPL", "2026-04-20")]))
    cand = eh.find_smart_refresh_candidates(["AAPL"], blacklist={"AAPL"}, today=TODAY)
    assert cand == []


def test_today_default_is_today(tmp_parquets):
    """today=None falls back to pd.Timestamp.today(); a gap ticker always
    comes back regardless of the resolved 'today'."""
    cand = eh.find_smart_refresh_candidates(["NEW"], blacklist=set())
    assert cand == ["NEW"]


def test_in_memory_overrides_skip_disk(tmp_parquets):
    """history_df + dates_df overrides bypass disk reads. Stale capture +
    no calendar → Rule C."""
    history = pd.DataFrame([_hist("AAPL", "2026-01-10")])
    cand = eh.find_smart_refresh_candidates(
        ["AAPL"], blacklist=set(), today=TODAY,
        history_df=history, dates_df=None,
    )
    assert cand == ["AAPL"]  # rule C
