"""Tests for finviz_fill.py — schema mapping, forward-row skip, 5-year
cap, report_time bucketing, per-ticker fetch classification, and the
spot-fill write path."""
from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from trade_scanner_fh import config, earnings_history, finviz_client, finviz_fill


@pytest.fixture
def tmp_world(tmp_path, monkeypatch):
    """Hermetic parquet/raw/checkpoint tree + neutralized rate limiter."""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "earnings_history.parquet")
    monkeypatch.setattr(config, "EARNINGS_PARQUET",
                        tmp_path / "earnings_dates.parquet")
    monkeypatch.setattr(config, "FINVIZ_BULK_CHECKPOINT",
                        tmp_path / ".finviz_bulk_checkpoint.json")
    raw_root = tmp_path / "earnings_raw"
    monkeypatch.setattr(config, "RAW_EARNINGS_DIR", raw_root)
    raw_root.mkdir()
    for src in config.RAW_SOURCES:
        (raw_root / src).mkdir()
    monkeypatch.setattr(finviz_client._limiter, "acquire", lambda: None)
    finviz_client._set_failure(None)
    yield tmp_path


# A fiscal-quarter end ~2 months ago (safely inside the 5-yr window) and
# one ~6 years ago (outside) — computed relative to today so the tests
# don't rot as the calendar advances.
_RECENT_END = (pd.Timestamp.today().normalize()
               - pd.DateOffset(months=2)).strftime("%Y-%m-%d")
_OLD_END = (pd.Timestamp.today().normalize()
            - pd.DateOffset(years=6)).strftime("%Y-%m-%d")


def _entry(fiscal_end=_RECENT_END, earnings_date=None, *,
           eps_actual=-0.07, eps_est=-0.05,
           sales_actual=151.1, sales_est=156.9, ticker="AAOI", **extra):
    if earnings_date is None:
        earnings_date = fiscal_end + "T16:30:00"
    d = {
        "ticker": ticker, "fiscalPeriod": "X",
        "earningsDate": earnings_date, "fiscalEndDate": fiscal_end,
        "epsActual": eps_actual, "epsEstimate": eps_est,
        "epsReportedActual": -0.19, "epsReportedEstimate": -0.10,
        "salesActual": sales_actual, "salesEstimate": sales_est,
    }
    d.update(extra)
    return d


_CUTOFF = pd.Timestamp.today().normalize() - pd.DateOffset(years=5)


# ──────────────────────────────────────────────────────────────────────
# _record_to_history_dict
# ──────────────────────────────────────────────────────────────────────

def test_record_mapping_uses_adjusted_fields():
    row = finviz_fill._record_to_history_dict(
        _entry(), queried_symbol="AAOI", cutoff=_CUTOFF, now=datetime.now())
    assert row is not None
    assert row["source"] == "finviz"
    assert row["report_date_proxy"] is False
    # Adjusted EPS (epsActual), NOT the GAAP epsReportedActual (-0.19).
    assert row["reported_eps"] == -0.07
    assert row["estimated_eps"] == -0.05
    # Surprise derived from actual − estimate.
    assert abs(row["surprise_eps"] - (-0.07 - -0.05)) < 1e-9
    assert abs(row["surprise_eps_pct"] - ((-0.07 - -0.05) / 0.05 * 100)) < 1e-6
    assert row["reported_rev"] == 151.1
    # period_ending normalized to day-1.
    assert row["period_ending"].day == 1
    assert row["report_time"] == "Close"


def test_record_skips_forward_estimate_rows():
    # No epsActual → a forward analyst-estimate row → dropped.
    assert finviz_fill._record_to_history_dict(
        _entry(eps_actual=None), queried_symbol="AAOI",
        cutoff=_CUTOFF, now=datetime.now()) is None


def test_record_skips_when_no_earnings_date():
    assert finviz_fill._record_to_history_dict(
        _entry(earnings_date=""), queried_symbol="AAOI",
        cutoff=_CUTOFF, now=datetime.now()) is None


def test_record_respects_5y_cap():
    assert finviz_fill._record_to_history_dict(
        _entry(fiscal_end=_OLD_END), queried_symbol="AAOI",
        cutoff=_CUTOFF, now=datetime.now()) is None


@pytest.mark.parametrize("hhmm,expected", [
    ("16:30", "Close"), ("17:00", "Close"),
    ("08:30", "Open"), ("09:00", "Open"),
    ("12:00", "Unknown"), ("00:00", "Unknown"),
])
def test_report_time_buckets(hhmm, expected):
    row = finviz_fill._record_to_history_dict(
        _entry(earnings_date=f"{_RECENT_END}T{hhmm}:00"),
        queried_symbol="AAOI", cutoff=_CUTOFF, now=datetime.now())
    assert row["report_time"] == expected


# ──────────────────────────────────────────────────────────────────────
# _fetch_one_ticker classification
# ──────────────────────────────────────────────────────────────────────

def test_fetch_one_ticker_builds_rows(tmp_world, monkeypatch):
    monkeypatch.setattr(finviz_client, "fetch_earnings",
                        lambda s: [_entry(), _entry(eps_actual=None)])
    res = finviz_fill._fetch_one_ticker("AAOI", cutoff=_CUTOFF, now=datetime.now())
    assert res.failure is None
    assert len(res.rows) == 1            # the forward row was skipped
    assert len(res.raw_records) == 2     # raw layer keeps everything
    assert res.rows[0]["source"] == "finviz"


def test_fetch_one_ticker_empty_array(tmp_world, monkeypatch):
    monkeypatch.setattr(finviz_client, "fetch_earnings", lambda s: [])
    res = finviz_fill._fetch_one_ticker("SPY", cutoff=_CUTOFF, now=datetime.now())
    assert res.is_empty is True
    assert res.failure == finviz_client.FAIL_EMPTY


def test_fetch_one_ticker_empty_via_none(tmp_world, monkeypatch):
    monkeypatch.setattr(finviz_client, "fetch_earnings", lambda s: None)
    monkeypatch.setattr(finviz_client, "last_failure_kind",
                        lambda: finviz_client.FAIL_EMPTY)
    res = finviz_fill._fetch_one_ticker("SPY", cutoff=_CUTOFF, now=datetime.now())
    assert res.is_empty is True


def test_fetch_one_ticker_real_failure(tmp_world, monkeypatch):
    monkeypatch.setattr(finviz_client, "fetch_earnings", lambda s: None)
    monkeypatch.setattr(finviz_client, "last_failure_kind",
                        lambda: finviz_client.FAIL_RATE_LIMITED)
    res = finviz_fill._fetch_one_ticker("AAOI", cutoff=_CUTOFF, now=datetime.now())
    assert res.is_empty is False
    assert res.failure == finviz_client.FAIL_RATE_LIMITED


# ──────────────────────────────────────────────────────────────────────
# next-earnings date capture (finviz forward rows → earnings_dates)
# ──────────────────────────────────────────────────────────────────────

def test_next_date_from_entries_picks_nearest_future():
    today = pd.Timestamp.today().normalize()
    fut_near = (today + pd.Timedelta(days=10)).strftime("%Y-%m-%dT08:30:00")
    fut_far = (today + pd.Timedelta(days=40)).strftime("%Y-%m-%dT08:30:00")
    entries = [
        _entry(),                                          # past, has actual
        _entry(eps_actual=None, earnings_date=fut_far),    # forward (farther)
        _entry(eps_actual=None, earnings_date=fut_near),   # forward (nearer)
    ]
    assert finviz_fill._next_date_from_entries(entries, today) == (
        today + pd.Timedelta(days=10))


def test_next_date_none_when_all_past():
    today = pd.Timestamp.today().normalize()
    assert finviz_fill._next_date_from_entries([_entry()], today) is None


def test_fetch_one_ticker_captures_next_date(tmp_world, monkeypatch):
    today = pd.Timestamp.today().normalize()
    fut = (today + pd.Timedelta(days=14)).strftime("%Y-%m-%dT16:30:00")
    monkeypatch.setattr(finviz_client, "fetch_earnings",
                        lambda s: [_entry(), _entry(eps_actual=None, earnings_date=fut)])
    res = finviz_fill._fetch_one_ticker("AAOI", cutoff=_CUTOFF, now=datetime.now())
    assert len(res.rows) == 1                       # forward row stays out of history
    assert res.next_date == today + pd.Timedelta(days=14)


def test_spot_fill_routes_next_date_to_dates_cache(tmp_world, monkeypatch):
    """A forward (future) finviz row's date lands in earnings_dates as the
    ticker's next_earnings — without any future row in the history."""
    from trade_scanner_fh import earnings_cache as ec
    today = pd.Timestamp.today().normalize()
    fut = (today + pd.Timedelta(days=14)).strftime("%Y-%m-%dT16:30:00")
    monkeypatch.setattr(finviz_client, "fetch_earnings",
                        lambda s: [_entry(), _entry(eps_actual=None, earnings_date=fut)])
    count, status = finviz_fill.spot_fill_finviz("AAOI", blacklist=set())
    assert status == "ok"
    dates = ec.load_earnings_cache()
    row = dates[dates["ticker"] == "AAOI"]
    assert not row.empty
    assert pd.Timestamp(row.iloc[0]["next_earnings"]) == today + pd.Timedelta(days=14)
    # And no future-dated row leaked into the per-quarter history.
    hist = earnings_history.load_earnings_history()
    assert (hist["report_date"] <= pd.Timestamp(today)).all()


def test_gap_fill_persists_next_date_via_incremental_flush(tmp_world, monkeypatch):
    """The bulk/gap loop (_fill_via_finviz) captures finviz forward dates,
    flushes them durably each _persist_progress (flush_every=1 here), and
    the end reconcile folds them into earnings_dates."""
    from trade_scanner_fh import earnings_cache as ec
    today = pd.Timestamp.today().normalize()
    fut = (today + pd.Timedelta(days=20)).strftime("%Y-%m-%dT16:30:00")
    monkeypatch.setattr(finviz_client, "fetch_earnings",
                        lambda s: [_entry(), _entry(eps_actual=None, earnings_date=fut)])
    finviz_fill.gap_fill_finviz(["AAOI"], set(), flush_every=1)
    row = ec.load_earnings_cache()
    row = row[row["ticker"] == "AAOI"]
    assert not row.empty
    assert pd.Timestamp(row.iloc[0]["next_earnings"]) == today + pd.Timedelta(days=20)


def test_flush_next_dates_returns_true_empty():
    assert finviz_fill._flush_next_dates_to_cache({}, datetime.now()) is True


def test_flush_next_dates_returns_true_on_write(tmp_world):
    from trade_scanner_fh import earnings_cache as ec
    today = pd.Timestamp.today().normalize()
    ok = finviz_fill._flush_next_dates_to_cache(
        {"AAOI": today + pd.Timedelta(days=7)}, datetime.now())
    assert ok is True
    dates = ec.load_earnings_cache()
    assert (dates["ticker"] == "AAOI").any()
    assert (dates.loc[dates["ticker"] == "AAOI", "source"] == "finviz").all()


def test_flush_next_dates_returns_false_on_write_failure(tmp_world, monkeypatch):
    """A failed cache write returns False so the caller keeps the buffer to
    retry on the next flush rather than silently dropping the dates."""
    from trade_scanner_fh import earnings_cache as ec
    monkeypatch.setattr(ec, "_merge_and_save",
                        lambda *a, **k: (_ for _ in ()).throw(OSError("disk full")))
    today = pd.Timestamp.today().normalize()
    ok = finviz_fill._flush_next_dates_to_cache(
        {"AAOI": today + pd.Timedelta(days=7)}, datetime.now())
    assert ok is False


# ──────────────────────────────────────────────────────────────────────
# spot fill + gap helper (write path)
# ──────────────────────────────────────────────────────────────────────

def test_spot_fill_writes_finviz_rows(tmp_world, monkeypatch):
    monkeypatch.setattr(finviz_client, "fetch_earnings",
                        lambda s: [_entry(), _entry(
                            fiscal_end=(pd.Timestamp.today().normalize()
                                        - pd.DateOffset(months=5)).strftime("%Y-%m-%d"))])
    count, status = finviz_fill.spot_fill_finviz("AAOI", blacklist=set())
    assert status == "ok"
    assert count == 2
    df = earnings_history.load_earnings_history()
    aaoi = df[df["ticker"] == "AAOI"]
    assert len(aaoi) == 2
    assert set(aaoi["source"]) == {"finviz"}


def test_spot_fill_blacklisted(tmp_world):
    assert finviz_fill.spot_fill_finviz("AAOI", {"AAOI"}) == (0, "blacklisted")


def test_spot_fill_empty(tmp_world, monkeypatch):
    seen = []
    monkeypatch.setattr(finviz_client, "fetch_earnings", lambda s: [])
    count, status = finviz_fill.spot_fill_finviz(
        "SPY", set(), on_empty_identified=lambda s: seen.append(s))
    assert (count, status) == (0, "empty")
    assert seen == ["SPY"]


def test_find_finviz_gap_tickers(tmp_world):
    # Ticker A has a finviz row; B has only zacks; C is absent.
    earnings_history.save_earnings_history(pd.DataFrame([
        {"ticker": "A", "period_ending": pd.Timestamp("2025-12-01"),
         "report_date": pd.Timestamp("2026-02-01"), "report_time": "Close",
         "estimated_eps": 1.0, "reported_eps": 1.1, "surprise_eps": 0.1,
         "surprise_eps_pct": 10.0, "estimated_rev": None, "reported_rev": None,
         "surprise_rev": None, "surprise_rev_pct": None, "source": "finviz",
         "updated_at": pd.Timestamp.now(), "report_date_proxy": False},
        {"ticker": "B", "period_ending": pd.Timestamp("2025-12-01"),
         "report_date": pd.Timestamp("2026-02-01"), "report_time": "Close",
         "estimated_eps": 1.0, "reported_eps": 1.1, "surprise_eps": 0.1,
         "surprise_eps_pct": 10.0, "estimated_rev": None, "reported_rev": None,
         "surprise_rev": None, "surprise_rev_pct": None, "source": "zacks",
         "updated_at": pd.Timestamp.now(), "report_date_proxy": False},
    ]))
    gaps = finviz_fill.find_finviz_gap_tickers(["A", "B", "C"], set())
    # A already has finviz; B (zacks-only) + C (absent) are gaps.
    assert set(gaps) == {"B", "C"}
