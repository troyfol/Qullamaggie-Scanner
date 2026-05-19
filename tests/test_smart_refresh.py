"""Tests for the Phase 5 daily smart-refresh candidate selector
(`earnings_history.find_smart_refresh_candidates`)."""
from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from trade_scanner_fh import (
    earnings_cache as ec,
    earnings_history as eh,
)


# ──────────────────────────────────────────────────────────────────────
# Fixtures + helpers
# ──────────────────────────────────────────────────────────────────────

TODAY = pd.Timestamp("2026-04-30")


@pytest.fixture
def tmp_parquets(tmp_path, monkeypatch):
    """Redirect parquet paths to tmp_path so the selector reads only the
    rows each test seeds."""
    monkeypatch.setattr(eh.config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(eh.config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "earnings_history.parquet")
    monkeypatch.setattr(eh.config, "EARNINGS_PARQUET",
                        tmp_path / "earnings_dates.parquet")
    return tmp_path


def _hist(ticker: str, report_date: str) -> dict:
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
        "updated_at": pd.Timestamp(datetime.now()),
    }


def _dates(ticker: str, last: str | None, nxt: str | None) -> dict:
    return {
        "ticker": ticker,
        "last_earnings": pd.Timestamp(last) if last else pd.NaT,
        "next_earnings": pd.Timestamp(nxt) if nxt else pd.NaT,
        "updated_at": pd.Timestamp(datetime(2026, 1, 1)),
    }


# ──────────────────────────────────────────────────────────────────────
# Rule A — gap fill
# ──────────────────────────────────────────────────────────────────────

def test_ticker_with_no_history_is_a_candidate(tmp_parquets):
    cand = eh.find_smart_refresh_candidates(
        ["AAPL"], blacklist=set(), today=TODAY,
    )
    assert cand == ["AAPL"]


def test_blacklist_excludes_gap_ticker(tmp_parquets):
    cand = eh.find_smart_refresh_candidates(
        ["AAPL"], blacklist={"AAPL"}, today=TODAY,
    )
    assert cand == []


def test_multiple_gap_tickers_all_returned(tmp_parquets):
    cand = eh.find_smart_refresh_candidates(
        ["AAPL", "MSFT", "GOOG"], blacklist=set(), today=TODAY,
    )
    assert cand == ["AAPL", "GOOG", "MSFT"]  # sorted


# ──────────────────────────────────────────────────────────────────────
# Rule B — recently-reported (>95d since last AND next_e <= today + 7d)
# ──────────────────────────────────────────────────────────────────────

def test_stale_with_imminent_next_is_candidate(tmp_parquets):
    """report_date > 95d ago AND next_earnings 5 days from now → due."""
    eh.save_earnings_history(pd.DataFrame([_hist("AAPL", "2026-01-15")]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates("AAPL", "2026-01-15", "2026-05-05"),  # 5 days from TODAY
    ]))
    cand = eh.find_smart_refresh_candidates(
        ["AAPL"], blacklist=set(), today=TODAY,
    )
    assert cand == ["AAPL"]


def test_stale_with_past_next_earnings_is_candidate(tmp_parquets):
    """report_date > 95d ago AND next_earnings already passed → due."""
    eh.save_earnings_history(pd.DataFrame([_hist("AAPL", "2026-01-15")]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates("AAPL", "2026-01-15", "2026-04-25"),  # 5 days before TODAY
    ]))
    cand = eh.find_smart_refresh_candidates(
        ["AAPL"], blacklist=set(), today=TODAY,
    )
    assert cand == ["AAPL"]


def test_stale_with_far_future_next_is_not_candidate(tmp_parquets):
    """report_date > 95d ago BUT next_earnings is far enough out → skip
    (no new quarter to capture yet)."""
    eh.save_earnings_history(pd.DataFrame([_hist("AAPL", "2026-01-15")]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates("AAPL", "2026-01-15", "2026-06-15"),  # ~46 days out
    ]))
    cand = eh.find_smart_refresh_candidates(
        ["AAPL"], blacklist=set(), today=TODAY,
    )
    assert cand == []


def test_recent_report_with_future_next_is_not_candidate(tmp_parquets):
    """Just-reported (<95d ago) → not due even with imminent next_e."""
    eh.save_earnings_history(pd.DataFrame([_hist("AAPL", "2026-02-15")]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates("AAPL", "2026-02-15", "2026-05-05"),
    ]))
    cand = eh.find_smart_refresh_candidates(
        ["AAPL"], blacklist=set(), today=TODAY,
    )
    assert cand == []


# ──────────────────────────────────────────────────────────────────────
# Rule C — long-stale, no forward calendar
# ──────────────────────────────────────────────────────────────────────

def test_long_stale_no_next_is_candidate(tmp_parquets):
    """report_date > 100d ago AND next_earnings missing → due."""
    eh.save_earnings_history(pd.DataFrame([_hist("AAPL", "2026-01-10")]))
    # Note: no row in earnings_dates for AAPL
    cand = eh.find_smart_refresh_candidates(
        ["AAPL"], blacklist=set(), today=TODAY,
    )
    assert cand == ["AAPL"]


def test_long_stale_no_next_with_nat_is_candidate(tmp_parquets):
    """next_earnings present but NaT counts as 'no next'."""
    eh.save_earnings_history(pd.DataFrame([_hist("AAPL", "2026-01-10")]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates("AAPL", "2026-01-10", None),
    ]))
    cand = eh.find_smart_refresh_candidates(
        ["AAPL"], blacklist=set(), today=TODAY,
    )
    assert cand == ["AAPL"]


def test_borderline_stale_no_next_is_not_candidate(tmp_parquets):
    """Stale (>95d) but not long-stale (<=100d), no next → skip per rule C."""
    # 96 days ago: stale per rule B threshold but no next_e to satisfy rule B,
    # and not yet past the 100-day rule-C cutoff.
    eh.save_earnings_history(pd.DataFrame([
        _hist("AAPL", str((TODAY - pd.Timedelta(days=96)).date())),
    ]))
    cand = eh.find_smart_refresh_candidates(
        ["AAPL"], blacklist=set(), today=TODAY,
    )
    assert cand == []


# ──────────────────────────────────────────────────────────────────────
# Mixed cases & invariants
# ──────────────────────────────────────────────────────────────────────

def test_mixed_universe_returns_only_due(tmp_parquets):
    """Three tickers: one gap, one due, one fresh → only first two returned."""
    eh.save_earnings_history(pd.DataFrame([
        # MSFT: just reported (fresh), with future next
        _hist("MSFT", "2026-02-25"),
        # GOOG: stale, with imminent next → due (rule B)
        _hist("GOOG", "2026-01-10"),
    ]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates("MSFT", "2026-02-25", "2026-05-25"),  # ~25 days out
        _dates("GOOG", "2026-01-10", "2026-05-02"),  # 2 days out
    ]))
    cand = eh.find_smart_refresh_candidates(
        ["AAPL", "MSFT", "GOOG"], blacklist=set(), today=TODAY,
    )
    assert cand == ["AAPL", "GOOG"]  # sorted; MSFT excluded


def test_empty_universe_returns_empty(tmp_parquets):
    cand = eh.find_smart_refresh_candidates(
        [], blacklist=set(), today=TODAY,
    )
    assert cand == []


def test_blacklist_excludes_due_ticker(tmp_parquets):
    """A ticker that would otherwise be due is dropped if blacklisted."""
    eh.save_earnings_history(pd.DataFrame([_hist("AAPL", "2026-01-15")]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates("AAPL", "2026-01-15", "2026-05-05"),
    ]))
    cand = eh.find_smart_refresh_candidates(
        ["AAPL"], blacklist={"AAPL"}, today=TODAY,
    )
    assert cand == []


def test_today_default_is_today(tmp_parquets):
    """Passing today=None falls back to pd.Timestamp.today() — a gap
    ticker should always come back regardless of the resolved 'today'."""
    cand = eh.find_smart_refresh_candidates(["NEW"], blacklist=set())
    assert cand == ["NEW"]


def test_in_memory_overrides_skip_disk(tmp_parquets):
    """history_df + dates_df overrides bypass disk reads — useful for
    callers that haven't flushed yet."""
    history = pd.DataFrame([_hist("AAPL", "2026-01-10")])
    # No file on disk; without overrides this would fall through to gap-fill
    # rule A. With overrides showing recent stale + missing next, rule C fires.
    cand = eh.find_smart_refresh_candidates(
        ["AAPL"], blacklist=set(), today=TODAY,
        history_df=history, dates_df=None,
    )
    assert cand == ["AAPL"]  # rule C
