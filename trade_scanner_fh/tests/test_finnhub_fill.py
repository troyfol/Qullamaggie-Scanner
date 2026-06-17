"""Tests for finnhub_fill.py — Phase 2 deep-history fill orchestration:
schema mapping, calendar join, 5-year cap, step-back-on-block,
resumable checkpoint, and ETF identification."""
from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

from trade_scanner_fh import config, earnings_history, earnings_raw, finnhub_fill
from trade_scanner_fh import finnhub_client


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_world(tmp_path, monkeypatch):
    """Redirect every parquet path + raw dir + checkpoint into a tmp tree
    so tests are hermetic."""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "earnings_history.parquet")
    monkeypatch.setattr(config, "EARNINGS_PARQUET",
                        tmp_path / "earnings_dates.parquet")
    monkeypatch.setattr(config, "FINNHUB_BULK_CHECKPOINT",
                        tmp_path / ".finnhub_bulk_checkpoint.json")
    raw_root = tmp_path / "earnings_raw"
    monkeypatch.setattr(config, "RAW_EARNINGS_DIR", raw_root)
    raw_root.mkdir()
    for src in config.RAW_SOURCES:
        (raw_root / src).mkdir()

    # Bypass the rate limiter so per-ticker tests don't sleep 1s.
    monkeypatch.setattr(finnhub_client._limiter, "acquire", lambda: None)

    # Make sure FAIL kind starts clean.
    finnhub_client._set_failure(None)
    yield tmp_path


@pytest.fixture
def fake_clients(monkeypatch):
    """Replace the per-call HTTP fetchers with simple lambdas. Tests
    inject behaviors via the returned MagicMocks."""
    history_mock = MagicMock()
    calendar_mock = MagicMock()
    monkeypatch.setattr(finnhub_client, "fetch_earnings_history", history_mock)
    monkeypatch.setattr(finnhub_client, "fetch_calendar_earnings_window",
                        calendar_mock)
    monkeypatch.setattr(finnhub_client, "is_configured", lambda: True)
    monkeypatch.setattr(finnhub_client, "verify_api_key", lambda: True)
    # Make sure the failure kind stays None unless a test sets it.
    monkeypatch.setattr(finnhub_client, "last_failure_kind", lambda: None)
    return history_mock, calendar_mock


def _hist_record(period: str, *, year: int, quarter: int,
                 actual=2.0, estimate=1.9, surprise=0.1, surprise_pct=5.26,
                 revenue_actual=10000, revenue_estimate=9500,
                 symbol="AAPL"):
    return {
        "symbol": symbol, "period": period, "year": year, "quarter": quarter,
        "actual": actual, "estimate": estimate,
        "surprise": surprise, "surprisePercent": surprise_pct,
        "revenueActual": revenue_actual,
        "revenueEstimate": revenue_estimate,
    }


# ──────────────────────────────────────────────────────────────────────
# Schema mapping — _record_to_history_dict
# ──────────────────────────────────────────────────────────────────────

def test_record_to_history_dict_maps_finnhub_fields_to_schema():
    today = pd.Timestamp("2026-05-04")
    cutoff = today - pd.DateOffset(years=5)
    cal = {(2025, 4): pd.Timestamp("2026-01-29")}
    rec = _hist_record("2025-12-31", year=2025, quarter=4)
    row = finnhub_fill._record_to_history_dict(
        rec, queried_symbol="AAPL",
        calendar_lookup=cal, cutoff=cutoff, now=datetime.now(),
    )
    assert row is not None
    assert row["ticker"] == "AAPL"
    # period_ending normalized to day-1 of month (Zacks convention) so
    # cross-source dedup keys match. Finnhub supplies 2025-12-31 →
    # stored as 2025-12-01.
    assert row["period_ending"] == pd.Timestamp("2025-12-01")
    assert row["report_date"] == pd.Timestamp("2026-01-29")
    assert row["report_date_proxy"] is False
    assert row["estimated_eps"] == 1.9
    assert row["reported_eps"] == 2.0
    assert row["surprise_eps"] == 0.1
    assert row["surprise_eps_pct"] == 5.26
    assert row["estimated_rev"] == 9500
    assert row["reported_rev"] == 10000
    assert row["surprise_rev"] == 500
    assert abs(row["surprise_rev_pct"] - (500 / 9500 * 100)) < 1e-6
    assert row["source"] == "finnhub"
    assert row["report_time"] == "Unknown"


def test_record_falls_back_to_period_ending_with_proxy_true_when_no_calendar_match():
    today = pd.Timestamp("2026-05-04")
    cutoff = today - pd.DateOffset(years=5)
    cal: dict = {}  # no announcements known
    rec = _hist_record("2024-06-30", year=2024, quarter=2)
    row = finnhub_fill._record_to_history_dict(
        rec, queried_symbol="AAPL",
        calendar_lookup=cal, cutoff=cutoff, now=datetime.now(),
    )
    assert row is not None
    # report_date stays the EXACT period-end Finnhub returned (2024-06-30),
    # not the day-1-normalized period_ending — these are independent.
    assert row["report_date"] == pd.Timestamp("2024-06-30")
    assert row["period_ending"] == pd.Timestamp("2024-06-01")
    assert row["report_date_proxy"] is True


def test_period_ending_normalized_to_day_1_independent_of_report_date():
    """Belt-and-suspenders: confirm that for every supplied period (any
    day of month), period_ending is stamped to day-1 while report_date
    is preserved exactly as the source supplied it."""
    today = pd.Timestamp("2026-05-04")
    cutoff = today - pd.DateOffset(years=5)
    for period_str, expected_p1 in [
        ("2026-03-31", "2026-03-01"),  # Mar quarter-end
        ("2025-06-30", "2025-06-01"),  # Jun quarter-end
        ("2025-09-30", "2025-09-01"),  # Sep quarter-end
        ("2025-12-31", "2025-12-01"),  # Dec quarter-end
        ("2025-09-15", "2025-09-01"),  # mid-month, still goes to day-1
    ]:
        rec = _hist_record(period_str, year=2025, quarter=4)
        cal = {(2025, 4): pd.Timestamp("2026-01-29")}
        row = finnhub_fill._record_to_history_dict(
            rec, queried_symbol="X",
            calendar_lookup=cal, cutoff=cutoff, now=datetime.now(),
        )
        assert row is not None, f"period={period_str} returned None"
        assert row["period_ending"] == pd.Timestamp(expected_p1), (
            f"period={period_str}: expected {expected_p1}, "
            f"got {row['period_ending']}"
        )
        # report_date is independent — comes from the calendar lookup
        # in this test, untouched by period_ending normalization.
        assert row["report_date"] == pd.Timestamp("2026-01-29")


def test_record_drops_pre_cutoff_period():
    today = pd.Timestamp("2026-05-04")
    cutoff = today - pd.DateOffset(years=5)  # 2021-05-04
    rec = _hist_record("2020-12-31", year=2020, quarter=4)
    row = finnhub_fill._record_to_history_dict(
        rec, queried_symbol="AAPL",
        calendar_lookup={}, cutoff=cutoff, now=datetime.now(),
    )
    assert row is None  # filtered out by 5-year cap


def test_record_handles_null_revenue_gracefully():
    today = pd.Timestamp("2026-05-04")
    cutoff = today - pd.DateOffset(years=5)
    rec = _hist_record("2024-06-30", year=2024, quarter=2,
                       revenue_actual=None, revenue_estimate=None)
    row = finnhub_fill._record_to_history_dict(
        rec, queried_symbol="AAPL",
        calendar_lookup={}, cutoff=cutoff, now=datetime.now(),
    )
    assert row is not None
    assert row["estimated_rev"] is None
    assert row["reported_rev"] is None
    assert row["surprise_rev"] is None
    assert row["surprise_rev_pct"] is None


def test_record_zero_rev_estimate_does_not_blow_up_pct():
    today = pd.Timestamp("2026-05-04")
    cutoff = today - pd.DateOffset(years=5)
    rec = _hist_record("2024-06-30", year=2024, quarter=2,
                       revenue_actual=100, revenue_estimate=0)
    row = finnhub_fill._record_to_history_dict(
        rec, queried_symbol="AAPL",
        calendar_lookup={}, cutoff=cutoff, now=datetime.now(),
    )
    assert row is not None
    # surprise_rev computed (100 - 0 = 100), pct skipped to avoid /0
    assert row["surprise_rev"] == 100
    assert row["surprise_rev_pct"] is None


def test_record_missing_symbol_or_period_returns_none():
    today = pd.Timestamp("2026-05-04")
    cutoff = today - pd.DateOffset(years=5)
    # Phase 6.5: queried_symbol is the source of truth now. Missing
    # period still returns None, but the rec's "symbol" field is no
    # longer the gating factor — what matters is the queried form.
    bad = {"period": None, "year": 2024, "quarter": 2}
    assert finnhub_fill._record_to_history_dict(
        bad, queried_symbol="AAPL",
        calendar_lookup={}, cutoff=cutoff, now=datetime.now(),
    ) is None
    # Empty queried_symbol still returns None
    rec = _hist_record("2024-06-30", year=2024, quarter=2)
    assert finnhub_fill._record_to_history_dict(
        rec, queried_symbol="",
        calendar_lookup={}, cutoff=cutoff, now=datetime.now(),
    ) is None


# ──────────────────────────────────────────────────────────────────────
# Calendar lookup
# ──────────────────────────────────────────────────────────────────────

def test_calendar_events_to_lookup_keyed_by_year_quarter():
    events = [
        {"date": "2026-01-29", "year": 2025, "quarter": 4, "symbol": "AAPL"},
        {"date": "2025-10-30", "year": 2025, "quarter": 3, "symbol": "AAPL"},
    ]
    out = finnhub_fill._calendar_events_to_lookup(events)
    assert out[(2025, 4)] == pd.Timestamp("2026-01-29")
    assert out[(2025, 3)] == pd.Timestamp("2025-10-30")


def test_calendar_events_to_lookup_takes_latest_on_duplicate():
    events = [
        {"date": "2026-01-15", "year": 2025, "quarter": 4, "symbol": "AAPL"},
        {"date": "2026-01-29", "year": 2025, "quarter": 4, "symbol": "AAPL"},
    ]
    out = finnhub_fill._calendar_events_to_lookup(events)
    assert out[(2025, 4)] == pd.Timestamp("2026-01-29")


def test_calendar_events_to_lookup_skips_malformed():
    events = [
        {"date": "2026-01-29", "symbol": "X"},  # missing year/quarter
        {"date": "not-a-date", "year": 2025, "quarter": 4, "symbol": "Y"},
        {"year": 2025, "quarter": 4, "symbol": "Z"},  # missing date
    ]
    assert finnhub_fill._calendar_events_to_lookup(events) == {}


# ──────────────────────────────────────────────────────────────────────
# _fetch_one_ticker — joins history with calendar, classifies failures
# ──────────────────────────────────────────────────────────────────────

def test_fetch_one_ticker_empty_history_marks_is_empty(tmp_world, fake_clients):
    history_mock, calendar_mock = fake_clients
    history_mock.return_value = []
    # Don't even call the calendar — empty history short-circuits.
    today = pd.Timestamp("2026-05-04")
    result = finnhub_fill._fetch_one_ticker(
        "VTI",
        cutoff=today - pd.DateOffset(years=5),
        cal_start=date(2021, 5, 4), cal_end=date(2026, 8, 2),
        now=datetime.now(),
    )
    assert result.is_empty is True
    assert result.failure == finnhub_client.FAIL_EMPTY
    assert result.rows == []
    calendar_mock.assert_not_called()


def test_fetch_one_ticker_history_failure_propagates(tmp_world, fake_clients, monkeypatch):
    history_mock, calendar_mock = fake_clients
    history_mock.return_value = None
    monkeypatch.setattr(finnhub_client, "last_failure_kind",
                        lambda: finnhub_client.FAIL_RATE_LIMITED)
    today = pd.Timestamp("2026-05-04")
    result = finnhub_fill._fetch_one_ticker(
        "AAPL",
        cutoff=today - pd.DateOffset(years=5),
        cal_start=date(2021, 5, 4), cal_end=date(2026, 8, 2),
        now=datetime.now(),
    )
    assert result.failure == finnhub_client.FAIL_RATE_LIMITED
    assert result.rows == []


def test_fetch_one_ticker_calendar_failure_falls_back_to_proxy(tmp_world, fake_clients):
    """When /stock/earnings succeeds but /calendar/earnings fails, we
    still emit history rows — they just have report_date_proxy=True."""
    history_mock, calendar_mock = fake_clients
    history_mock.return_value = [_hist_record("2025-12-31", year=2025, quarter=4)]
    calendar_mock.return_value = None  # calendar failed
    today = pd.Timestamp("2026-05-04")
    result = finnhub_fill._fetch_one_ticker(
        "AAPL",
        cutoff=today - pd.DateOffset(years=5),
        cal_start=date(2021, 5, 4), cal_end=date(2026, 8, 2),
        now=datetime.now(),
    )
    assert result.failure is None
    assert len(result.rows) == 1
    assert result.rows[0]["report_date_proxy"] is True
    assert result.rows[0]["report_date"] == pd.Timestamp("2025-12-31")


def test_fetch_one_ticker_joins_history_with_calendar(tmp_world, fake_clients):
    history_mock, calendar_mock = fake_clients
    history_mock.return_value = [
        _hist_record("2025-12-31", year=2025, quarter=4),
        _hist_record("2025-09-30", year=2025, quarter=3),
    ]
    calendar_mock.return_value = [
        {"date": "2026-01-29", "year": 2025, "quarter": 4, "symbol": "AAPL"},
        {"date": "2025-10-30", "year": 2025, "quarter": 3, "symbol": "AAPL"},
    ]
    today = pd.Timestamp("2026-05-04")
    result = finnhub_fill._fetch_one_ticker(
        "AAPL",
        cutoff=today - pd.DateOffset(years=5),
        cal_start=date(2021, 5, 4), cal_end=date(2026, 8, 2),
        now=datetime.now(),
    )
    assert len(result.rows) == 2
    by_period = {r["period_ending"]: r for r in result.rows}
    # period_ending normalized to day-1 (was 2025-12-31 / 2025-09-30)
    q4 = by_period[pd.Timestamp("2025-12-01")]
    q3 = by_period[pd.Timestamp("2025-09-01")]
    assert q4["report_date"] == pd.Timestamp("2026-01-29")
    assert q4["report_date_proxy"] is False
    assert q3["report_date"] == pd.Timestamp("2025-10-30")
    assert q3["report_date_proxy"] is False


def test_fetch_one_ticker_drops_pre_cutoff_records(tmp_world, fake_clients):
    history_mock, calendar_mock = fake_clients
    history_mock.return_value = [
        _hist_record("2025-12-31", year=2025, quarter=4),
        _hist_record("2018-12-31", year=2018, quarter=4),  # > 5y old
    ]
    calendar_mock.return_value = []
    today = pd.Timestamp("2026-05-04")
    result = finnhub_fill._fetch_one_ticker(
        "AAPL",
        cutoff=today - pd.DateOffset(years=5),
        cal_start=date(2021, 5, 4), cal_end=date(2026, 8, 2),
        now=datetime.now(),
    )
    # Only the 2025-12 row survives the 5-year cap; period_ending
    # normalized to day-1 of month.
    assert len(result.rows) == 1
    assert result.rows[0]["period_ending"] == pd.Timestamp("2025-12-01")


# ──────────────────────────────────────────────────────────────────────
# Bulk fill — end-to-end
# ──────────────────────────────────────────────────────────────────────

def test_bulk_fill_writes_history_and_raw(tmp_world, fake_clients):
    history_mock, calendar_mock = fake_clients
    history_mock.side_effect = lambda sym: [
        _hist_record("2025-12-31", year=2025, quarter=4, symbol=sym),
    ]
    calendar_mock.return_value = [
        {"date": "2026-01-29", "year": 2025, "quarter": 4, "symbol": "AAPL"},
    ]
    filled, errors = finnhub_fill.bulk_fill_finnhub(
        ["AAPL", "MSFT"], blacklist=set(),
        flush_every=1, resume_from_checkpoint=False,
    )
    assert filled == 2
    assert errors == 0

    # Consumer parquet has both tickers, source=finnhub
    df = earnings_history.load_earnings_history()
    assert df is not None
    assert set(df["ticker"]) == {"AAPL", "MSFT"}
    assert (df["source"] == "finnhub").all()

    # Raw parquet has both
    raw = earnings_raw.read_raw(config.RAW_SOURCE_FINNHUB)
    assert set(raw["symbol"]) == {"AAPL", "MSFT"}


def test_bulk_fill_etf_callback_fired_on_empty_response(tmp_world, fake_clients):
    history_mock, calendar_mock = fake_clients

    def fake_history(sym):
        if sym == "VTI":
            # Empty response: must set FAIL_EMPTY on the client too,
            # since _fetch_one_ticker reads last_failure_kind() when
            # history is None. For [] we never read it but be safe.
            return []
        return [_hist_record("2025-12-31", year=2025, quarter=4, symbol=sym)]

    history_mock.side_effect = fake_history
    calendar_mock.return_value = []

    seen_etfs: list[str] = []
    filled, errors = finnhub_fill.bulk_fill_finnhub(
        ["AAPL", "VTI", "MSFT"], blacklist=set(),
        flush_every=1, resume_from_checkpoint=False,
        on_etf_identified=seen_etfs.append,
    )
    assert filled == 2  # AAPL + MSFT
    assert errors == 1  # VTI counts as an "error" (empty)
    assert seen_etfs == ["VTI"]


def test_bulk_fill_forbidden_skips_listed_and_no_block(
    tmp_world, fake_clients, monkeypatch,
):
    """A 403 (FAIL_FORBIDDEN — symbol not in the account's plan) must be
    treated as a permanent per-ticker coverage gap: routed to the skip-list
    callback and NOT counted toward the consecutive-block streak. With more
    consecutive 403s than FINNHUB_CONSEC_BLOCK_LIMIT (3), the loop must NOT
    pause/rewind — each ticker is fetched exactly once."""
    history_mock, calendar_mock = fake_clients

    seq: list[str] = []

    def history_side_effect(sym):
        seq.append(sym)
        return None  # hard failure; kind supplied by last_failure_kind below

    history_mock.side_effect = history_side_effect
    calendar_mock.return_value = []
    # Every history failure is a 403 (out of plan).
    monkeypatch.setattr(finnhub_client, "last_failure_kind",
                        lambda: finnhub_client.FAIL_FORBIDDEN)
    # A backoff pause would call time.sleep — assert it never happens.
    sleep_calls: list[float] = []
    monkeypatch.setattr(finnhub_fill.time, "sleep",
                        lambda *a, **k: sleep_calls.append(a[0] if a else 0))

    seen_skips: list[str] = []
    syms = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]  # 6 > CONSEC_BLOCK_LIMIT
    filled, errors = finnhub_fill.bulk_fill_finnhub(
        syms, blacklist=set(),
        flush_every=10, resume_from_checkpoint=False,
        on_etf_identified=seen_skips.append,
    )

    assert filled == 0
    assert errors == len(syms)
    # Routed to the skip list, in order, once each.
    assert seen_skips == syms
    # No block streak → no rewind → each ticker fetched exactly once.
    assert seq == syms
    # No backoff pause was taken.
    assert sleep_calls == []


def test_bulk_fill_resumes_from_checkpoint(tmp_world, fake_clients):
    """First run gets killed mid-way after writing checkpoint with one
    ticker complete. Second run resumes — only the remaining tickers
    are fetched."""
    history_mock, calendar_mock = fake_clients
    history_mock.side_effect = lambda sym: [
        _hist_record("2025-12-31", year=2025, quarter=4, symbol=sym),
    ]
    calendar_mock.return_value = []

    # First run: process AAPL, then stop_flag flips so MSFT/NVDA never run.
    stop = [False]
    call_count = {"n": 0}
    real_fetch = finnhub_fill._fetch_one_ticker

    def gating_fetch(sym, **kw):
        call_count["n"] += 1
        result = real_fetch(sym, **kw)
        if call_count["n"] == 1:
            stop[0] = True
        return result

    import trade_scanner_fh.finnhub_fill as ff_mod
    orig = ff_mod._fetch_one_ticker
    ff_mod._fetch_one_ticker = gating_fetch
    try:
        finnhub_fill.bulk_fill_finnhub(
            ["AAPL", "MSFT", "NVDA"], blacklist=set(),
            flush_every=1, stop_flag=stop, resume_from_checkpoint=False,
        )
    finally:
        ff_mod._fetch_one_ticker = orig

    # Checkpoint should record AAPL as complete.
    cp = finnhub_fill._load_checkpoint()
    assert cp is not None
    assert "AAPL" in cp.completed
    assert "MSFT" not in cp.completed

    # Second run with resume_from_checkpoint=True. Reset stop flag.
    fetch_calls: list[str] = []

    def tracking_fetch(sym, **kw):
        fetch_calls.append(sym)
        return real_fetch(sym, **kw)

    ff_mod._fetch_one_ticker = tracking_fetch
    try:
        filled, _ = finnhub_fill.bulk_fill_finnhub(
            ["AAPL", "MSFT", "NVDA"], blacklist=set(),
            flush_every=1, resume_from_checkpoint=True,
        )
    finally:
        ff_mod._fetch_one_ticker = orig
    assert "AAPL" not in fetch_calls  # already complete
    assert "MSFT" in fetch_calls
    assert "NVDA" in fetch_calls

    # Clean finish must clear the checkpoint.
    assert finnhub_fill._load_checkpoint() is None


def test_bulk_fill_step_back_on_consecutive_failures(tmp_world, fake_clients, monkeypatch):
    """After FINNHUB_CONSEC_BLOCK_LIMIT consecutive failures, the loop
    should pause + verify key + rewind. Verify rewind happens by
    counting fetch calls."""
    history_mock, calendar_mock = fake_clients

    # Track call sequence so we can prove the step-back retried tickers.
    seq: list[str] = []
    fail_then_pass = {"AAPL": True, "MSFT": True, "NVDA": True}

    def history_side_effect(sym):
        seq.append(sym)
        # First call to each ticker fails, subsequent calls succeed.
        if fail_then_pass.get(sym):
            fail_then_pass[sym] = False
            return None
        return [_hist_record("2025-12-31", year=2025, quarter=4, symbol=sym)]

    history_mock.side_effect = history_side_effect
    calendar_mock.return_value = []
    # Simulate every history failure as rate-limited (a real block).
    monkeypatch.setattr(finnhub_client, "last_failure_kind",
                        lambda: finnhub_client.FAIL_RATE_LIMITED)
    monkeypatch.setattr(finnhub_fill.time, "sleep", lambda *_: None)
    # Force key probe to succeed so we don't halt on auth.
    monkeypatch.setattr(finnhub_client, "verify_api_key", lambda: True)

    finnhub_fill.bulk_fill_finnhub(
        ["AAPL", "MSFT", "NVDA"], blacklist=set(),
        flush_every=10, resume_from_checkpoint=False,
    )
    # AAPL/MSFT/NVDA each got fetched twice (first fail, then retry pass)
    counts = {sym: seq.count(sym) for sym in ("AAPL", "MSFT", "NVDA")}
    assert all(c >= 2 for c in counts.values()), (
        f"Each ticker should retry after the block. Got: {counts}"
    )


def test_bulk_fill_auth_failure_halts_immediately(tmp_world, fake_clients, monkeypatch):
    history_mock, calendar_mock = fake_clients
    seq: list[str] = []

    def fake_history(sym):
        seq.append(sym)
        return None

    history_mock.side_effect = fake_history
    calendar_mock.return_value = []
    monkeypatch.setattr(finnhub_client, "last_failure_kind",
                        lambda: finnhub_client.FAIL_AUTH)
    monkeypatch.setattr(finnhub_fill.time, "sleep", lambda *_: None)

    filled, errors = finnhub_fill.bulk_fill_finnhub(
        ["AAPL", "MSFT", "NVDA"], blacklist=set(),
        flush_every=10, resume_from_checkpoint=False,
    )
    # Should halt on first ticker, never reach MSFT/NVDA.
    assert seq == ["AAPL"]
    assert filled == 0


# ──────────────────────────────────────────────────────────────────────
# Gap fill
# ──────────────────────────────────────────────────────────────────────

def test_find_finnhub_gap_tickers(tmp_world):
    # Pre-populate earnings_history.parquet with one Zacks row +
    # one Finnhub row.
    rows = [
        {
            "ticker": "AAPL", "period_ending": pd.Timestamp("2025-12-31"),
            "report_date": pd.Timestamp("2026-01-29"), "report_time": "Close",
            "estimated_eps": 1.9, "reported_eps": 2.0, "surprise_eps": 0.1,
            "surprise_eps_pct": 5.26, "estimated_rev": 9500, "reported_rev": 10000,
            "surprise_rev": 500, "surprise_rev_pct": 5.26,
            "source": "zacks", "updated_at": pd.Timestamp(datetime.now()),
            "report_date_proxy": False,
        },
        {
            "ticker": "MSFT", "period_ending": pd.Timestamp("2025-12-31"),
            "report_date": pd.Timestamp("2026-01-29"), "report_time": "Unknown",
            "estimated_eps": 2.5, "reported_eps": 2.6, "surprise_eps": 0.1,
            "surprise_eps_pct": 4.0, "estimated_rev": 50000, "reported_rev": 51000,
            "surprise_rev": 1000, "surprise_rev_pct": 2.0,
            "source": "finnhub", "updated_at": pd.Timestamp(datetime.now()),
            "report_date_proxy": False,
        },
    ]
    earnings_history.save_earnings_history(pd.DataFrame(rows))

    # AAPL has Zacks but no Finnhub → it's a gap. MSFT has Finnhub → not.
    universe = ["AAPL", "MSFT", "NVDA", "GOOG"]
    gaps = finnhub_fill.find_finnhub_gap_tickers(universe, blacklist=set())
    assert "AAPL" in gaps
    assert "NVDA" in gaps
    assert "GOOG" in gaps
    assert "MSFT" not in gaps  # already has finnhub


# ──────────────────────────────────────────────────────────────────────
# Spot fill
# ──────────────────────────────────────────────────────────────────────

def test_spot_fill_writes_one_ticker(tmp_world, fake_clients):
    history_mock, calendar_mock = fake_clients
    history_mock.return_value = [
        _hist_record("2025-12-31", year=2025, quarter=4, symbol="AAPL"),
    ]
    calendar_mock.return_value = [
        {"date": "2026-01-29", "year": 2025, "quarter": 4, "symbol": "AAPL"},
    ]
    count, status = finnhub_fill.spot_fill_finnhub("AAPL", blacklist=set())
    assert status == "ok"
    assert count == 1
    df = earnings_history.load_earnings_history()
    assert df is not None
    aapl = df.loc[df["ticker"] == "AAPL"]
    assert len(aapl) == 1
    assert aapl.iloc[0]["source"] == "finnhub"


def test_spot_fill_empty_returns_empty_status_and_calls_etf_cb(tmp_world, fake_clients):
    history_mock, calendar_mock = fake_clients
    history_mock.return_value = []
    seen: list[str] = []
    count, status = finnhub_fill.spot_fill_finnhub(
        "VTI", blacklist=set(), on_etf_identified=seen.append,
    )
    assert status == "empty"
    assert count == 0
    assert seen == ["VTI"]


def test_spot_fill_blacklisted_returns_blacklisted_status(tmp_world, fake_clients):
    count, status = finnhub_fill.spot_fill_finnhub("VTI", blacklist={"VTI"})
    assert status == "blacklisted"
    assert count == 0


# ──────────────────────────────────────────────────────────────────────
# 5-year cap config tie-in
# ──────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────
# Phase 6.5 regression: canonicalization mismatch + defensive mask_replace.
# Live data hit this when querying e.g. "ENB" returned records with
# symbol="ENB.TO" (Finnhub's canonical form for the Toronto listing).
# The pre-fix code used record["symbol"] for the row's ticker AND keyed
# mask_replace on pending.keys() (queried form), which mismatched and
# caused dups to compound across runs. Two fixes:
#   1. _record_to_history_dict uses the queried symbol as ticker.
#   2. _flush_pending_to_disk keys mask_replace on new_df["ticker"]
#      (defensive — works even if a future caller breaks invariant 1).
# ──────────────────────────────────────────────────────────────────────

def test_record_uses_queried_symbol_not_response(tmp_world, fake_clients):
    """Even when Finnhub canonicalizes (query='ENB' → response symbol
    ='ENB.TO'), the row's ticker must be the QUERIED form so the
    mask_replace and downstream lookups work consistently."""
    history_mock, calendar_mock = fake_clients
    history_mock.return_value = [{
        "symbol": "ENB.TO",  # Finnhub's canonical
        "period": "2025-12-31", "year": 2025, "quarter": 4,
        "actual": 1.0, "estimate": 0.9, "surprise": 0.1,
        "surprisePercent": 11.0,
        "revenueActual": None, "revenueEstimate": None,
    }]
    calendar_mock.return_value = []
    count, status = finnhub_fill.spot_fill_finnhub("ENB", blacklist=set())
    assert status == "ok"
    df = earnings_history.load_earnings_history()
    # Ticker stored under queried form, not Finnhub's canonical
    assert set(df["ticker"]) == {"ENB"}, (
        f"Expected ticker='ENB' (queried), got {set(df['ticker'])}"
    )


def test_canonicalized_ticker_does_not_dup_across_runs(tmp_world, fake_clients):
    """The actual production failure pattern: queried 'ENB', Finnhub
    returns symbol='ENB.TO'. Multiple back-to-back runs must NOT
    accumulate duplicate rows — the mask_replace must catch existing
    rows by the row's own ticker, not by pending.keys()."""
    history_mock, calendar_mock = fake_clients
    history_mock.return_value = [{
        "symbol": "ENB.TO",  # canonicalized
        "period": "2025-12-31", "year": 2025, "quarter": 4,
        "actual": 1.0, "estimate": 0.9, "surprise": 0.1,
        "surprisePercent": 11.0,
        "revenueActual": None, "revenueEstimate": None,
    }]
    calendar_mock.return_value = []

    # Simulate the user clicking Bulk Fill 3 times in a row (each clean run
    # clears the checkpoint, so each starts fresh).
    for run_n in range(3):
        finnhub_fill.bulk_fill_finnhub(
            ["ENB"], blacklist=set(), flush_every=1,
            resume_from_checkpoint=False,
        )

    df = earnings_history.load_earnings_history()
    # 1 ticker × 1 quarter = 1 row, regardless of how many runs we did
    assert len(df) == 1, (
        f"After 3 clean runs we should still have 1 ENB row, got {len(df)}"
    )
    dups = df.duplicated(subset=["ticker", "period_ending", "source"]).sum()
    assert dups == 0, f"Found {dups} duplicate-PK rows after 3 runs"


def test_fiscal_year_multi_record_per_period_is_deduped(tmp_world, fake_clients):
    """Phase 6.5 fix #2: Finnhub returns 2 records for the same period
    when a ticker has a non-calendar fiscal year (one for fiscal-year
    classification, one for calendar-year). Without dedup, both rows
    would land in the consumer parquet under the same (ticker,
    period_ending, source) PK. Keep the higher (year, quarter) — the
    fiscal-year-aligned record matches what the company actually
    announced and what filters expect."""
    history_mock, calendar_mock = fake_clients
    # Simulated AENT response: same period 2025-09-30 returned as
    # both fiscal-Q1-2026 (year=2026, quarter=1) AND calendar-Q3-2025
    # (year=2025, quarter=3) with different EPS values.
    history_mock.return_value = [
        {"symbol": "AENT", "period": "2025-09-30",
         "year": 2026, "quarter": 1,  # fiscal-year view (winner)
         "actual": 0.10, "estimate": 0.0816,
         "surprise": 0.0184, "surprisePercent": 22.5,
         "revenueActual": None, "revenueEstimate": None},
        {"symbol": "AENT", "period": "2025-09-30",
         "year": 2025, "quarter": 3,  # calendar-year view (loser)
         "actual": 0.04, "estimate": None,
         "surprise": None, "surprisePercent": None,
         "revenueActual": None, "revenueEstimate": None},
        # Plus a normal (non-dup) period for control
        {"symbol": "AENT", "period": "2025-12-31",
         "year": 2026, "quarter": 2,
         "actual": 0.18, "estimate": 0.3162,
         "surprise": -0.1362, "surprisePercent": -43.1,
         "revenueActual": None, "revenueEstimate": None},
    ]
    calendar_mock.return_value = []

    count, status = finnhub_fill.spot_fill_finnhub("AENT", blacklist=set())
    assert status == "ok"
    df = earnings_history.load_earnings_history()
    aent = df.loc[df["ticker"] == "AENT"].sort_values("period_ending")
    # 2 unique periods, NOT 3 rows
    assert len(aent) == 2, f"Expected 2 rows after dedup, got {len(aent)}"
    # The 2025-09 row should be the fiscal-year view (actual=0.10).
    # period_ending is normalized to day-1 of month.
    sep = aent.loc[aent["period_ending"] == pd.Timestamp("2025-09-01")]
    assert len(sep) == 1
    assert sep.iloc[0]["reported_eps"] == 0.10, (
        f"Expected reported_eps=0.10 (fiscal-year-2026 view, higher year/quarter), "
        f"got {sep.iloc[0]['reported_eps']}"
    )
    # No dups
    dups = df.duplicated(subset=["ticker", "period_ending", "source"]).sum()
    assert dups == 0


def test_flush_dedup_belt_and_suspenders(tmp_world):
    """Defensive: even if a future caller violates the (ticker, period)
    invariant by stuffing a duplicate row into pending, the flush layer
    catches it before the parquet sees the dup."""
    pending = {"X": [
        {"ticker": "X", "period_ending": pd.Timestamp("2025-12-31"),
         "report_date": pd.Timestamp("2025-12-31"), "report_time": "Unknown",
         "estimated_eps": 1.0, "reported_eps": 1.1, "surprise_eps": 0.1,
         "surprise_eps_pct": 10.0,
         "estimated_rev": None, "reported_rev": None,
         "surprise_rev": None, "surprise_rev_pct": None,
         "source": "finnhub",
         "updated_at": pd.Timestamp(datetime.now()),
         "report_date_proxy": True},
        {"ticker": "X", "period_ending": pd.Timestamp("2025-12-31"),
         "report_date": pd.Timestamp("2025-12-31"), "report_time": "Unknown",
         "estimated_eps": 1.5, "reported_eps": 1.6, "surprise_eps": 0.1,
         "surprise_eps_pct": 6.7,
         "estimated_rev": None, "reported_rev": None,
         "surprise_rev": None, "surprise_rev_pct": None,
         "source": "finnhub",
         "updated_at": pd.Timestamp(datetime.now()),
         "report_date_proxy": True},
    ]}
    finnhub_fill._flush_pending_to_disk(pending)
    df = earnings_history.load_earnings_history()
    assert len(df) == 1, f"Flush should have deduped to 1 row, got {len(df)}"


def test_flush_keys_replacement_on_row_ticker_not_pending_key(tmp_world):
    """Direct unit test on _flush_pending_to_disk: even if pending key
    differs from the row's ticker, the mask must still drop the
    existing row matching the row's ticker. Belt-and-suspenders for
    the canonicalization bug."""
    # Seed with a pre-existing row for AAPL
    earnings_history.save_earnings_history(pd.DataFrame([{
        "ticker": "AAPL",
        "period_ending": pd.Timestamp("2025-12-31"),
        "report_date": pd.Timestamp("2026-01-29"),
        "report_time": "Unknown",
        "estimated_eps": 1.9, "reported_eps": 2.0,
        "surprise_eps": 0.1, "surprise_eps_pct": 5.26,
        "estimated_rev": None, "reported_rev": None,
        "surprise_rev": None, "surprise_rev_pct": None,
        "source": "finnhub",
        "updated_at": pd.Timestamp(datetime.now()),
        "report_date_proxy": True,
    }]))

    # Now flush a pending where the KEY is "WRONG_KEY" but the row's
    # ticker is the real "AAPL" — pre-fix this would not match the
    # existing row, leading to a dup.
    pending = {"WRONG_KEY": [{
        "ticker": "AAPL",
        "period_ending": pd.Timestamp("2025-12-31"),
        "report_date": pd.Timestamp("2026-01-29"),
        "report_time": "Unknown",
        "estimated_eps": 1.95, "reported_eps": 2.05,  # different values
        "surprise_eps": 0.10, "surprise_eps_pct": 5.13,
        "estimated_rev": None, "reported_rev": None,
        "surprise_rev": None, "surprise_rev_pct": None,
        "source": "finnhub",
        "updated_at": pd.Timestamp(datetime.now()),
        "report_date_proxy": True,
    }]}
    finnhub_fill._flush_pending_to_disk(pending)

    df = earnings_history.load_earnings_history()
    assert len(df) == 1, (
        f"Expected 1 row (replacement), got {len(df)} — mask_replace "
        f"didn't catch the existing row by its ticker"
    )
    # Verify the NEW values won (not the seeded ones)
    assert df.iloc[0]["estimated_eps"] == 1.95


def test_5_year_cap_uses_config_constant(tmp_world, fake_clients):
    """Verify _fetch_one_ticker actually respects EARNINGS_HISTORY_YEARS
    by setting it to 1 and confirming pre-cutoff rows drop."""
    import trade_scanner_fh.config as cfg
    cfg_orig = cfg.EARNINGS_HISTORY_YEARS
    try:
        cfg.EARNINGS_HISTORY_YEARS = 1
        # Today is 2026-05-04 in the project; 1y cutoff = 2025-05-04.
        history_mock, calendar_mock = fake_clients
        history_mock.return_value = [
            _hist_record("2025-12-31", year=2025, quarter=4),  # post-cutoff, kept
            _hist_record("2024-06-30", year=2024, quarter=2),  # pre-cutoff, dropped
        ]
        calendar_mock.return_value = []
        count, status = finnhub_fill.spot_fill_finnhub("AAPL", blacklist=set())
        assert status == "ok"
        df = earnings_history.load_earnings_history()
        assert len(df.loc[df["ticker"] == "AAPL"]) == 1
    finally:
        cfg.EARNINGS_HISTORY_YEARS = cfg_orig
