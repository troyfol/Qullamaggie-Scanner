"""Regression tests for the audit-fix patches (H1–L10).

Each test names the audit ID it locks in. Grouped by source file."""
from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from trade_scanner_fh import config, earnings_history as eh, scanner
from trade_scanner_fh.scanner import ScanParams
from trade_scanner_fh.zacks_scraper import (
    FAIL_BLOCKED, FAIL_NOT_FOUND, _normalize_time, _strip_html,
    set_zacks_cookies,
)


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_parquets(tmp_path, monkeypatch):
    monkeypatch.setattr(eh.config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(eh.config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "earnings_history.parquet")
    monkeypatch.setattr(eh.config, "EARNINGS_PARQUET",
                        tmp_path / "earnings_dates.parquet")
    return tmp_path


@pytest.fixture
def fake_scan_cache(tmp_path, monkeypatch):
    """Wire every cache directory + parquet path into tmp_path."""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "PARQUET_DIR", tmp_path / "ohlcv")
    monkeypatch.setattr(
        config, "EARNINGS_HISTORY_PARQUET",
        tmp_path / "earnings_history.parquet",
    )
    monkeypatch.setattr(
        config, "EARNINGS_PARQUET",
        tmp_path / "earnings_dates.parquet",
    )
    monkeypatch.setattr(
        config, "SECTOR_MAP_PARQUET",
        tmp_path / "sector_map.parquet",
    )
    (tmp_path / "ohlcv").mkdir(parents=True, exist_ok=True)
    return tmp_path


def _hist_row(ticker, period_str, report_str, *, eps_pct=5.0, rev_pct=5.0):
    return {
        "ticker": ticker,
        "period_ending": pd.Timestamp(period_str),
        "report_date": pd.Timestamp(report_str),
        "report_time": "Close",
        "estimated_eps": 2.0, "reported_eps": 2.1,
        "surprise_eps": 0.1, "surprise_eps_pct": eps_pct,
        "estimated_rev": 100.0, "reported_rev": 105.0,
        "surprise_rev": 5.0, "surprise_rev_pct": rev_pct,
        "source": "zacks",
        "updated_at": pd.Timestamp(datetime.now()),
    }


def _write_ohlcv(symbol, end, days=200, close=100.0):
    idx = pd.bdate_range(end=end, periods=days)
    df = pd.DataFrame({
        "Open":   [close] * days,
        "High":   [close * 1.01] * days,
        "Low":    [close * 0.99] * days,
        "Close":  [close] * days,
        "Volume": [1_000_000] * days,
    }, index=idx)
    df.to_parquet(config.PARQUET_DIR / f"{symbol}.parquet")


# ──────────────────────────────────────────────────────────────────────
# H2 / M8 — Per-quarter columns surface without any Phase 7 filter
# ──────────────────────────────────────────────────────────────────────

def test_per_column_gating_no_earnings_columns_when_filters_off(fake_scan_cache):
    """Option B (replaces audit H2's "always-on context" behavior):
    when NO individual earnings filter and NO beats filter is on, none
    of the 6 individual columns nor `last_report_date` appears in the
    output — even if earnings_history.parquet has data. The user's Off
    state must hide the column entirely, like every other indicator."""
    end = pd.Timestamp(date(2026, 4, 30))
    _write_ohlcv("AAPL", end)
    pd.DataFrame([
        _hist_row("AAPL", "2026-01-31", "2026-02-15", eps_pct=10.0, rev_pct=5.0),
    ]).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    p = ScanParams(
        start_date=date(2026, 1, 1), end_date=date(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        # NOTE: no earnings filters enabled.
    )
    result = scanner.run_scan(["AAPL"], p)
    df = result.results_df

    # Per-column gating: with no individual earnings filters AND no
    # beats filters active, NONE of the 6 individual columns nor
    # last_report_date should be in the output.
    for col in ("reported_eps", "surprise_eps_dollar", "surprise_eps_pct",
                "reported_rev", "surprise_rev_dollar", "surprise_rev_pct",
                "last_report_date"):
        assert col not in df.columns, (
            f"Column {col!r} should be absent when no earnings filters active"
        )


def test_per_column_gating_only_active_columns_appear(fake_scan_cache):
    """Per-column gating: enabling reported_eps as display-only puts
    just `reported_eps` in the output (plus `last_report_date` since
    no beat is active). The other 5 individual columns stay absent."""
    end = pd.Timestamp(date(2026, 4, 30))
    _write_ohlcv("AAPL", end)
    pd.DataFrame([
        _hist_row("AAPL", "2026-01-31", "2026-02-15", eps_pct=10.0, rev_pct=5.0),
    ]).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    p = ScanParams(
        start_date=date(2026, 1, 1), end_date=date(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        reported_eps_display_only=True,  # ← only this one
    )
    result = scanner.run_scan(["AAPL"], p)
    df = result.results_df.set_index("symbol")

    assert df.loc["AAPL", "reported_eps"] == pytest.approx(2.1)
    assert "last_report_date" in df.columns
    # The other individual columns are absent (per-column gating).
    for col in ("surprise_eps_dollar", "surprise_eps_pct",
                "reported_rev", "surprise_rev_dollar", "surprise_rev_pct"):
        assert col not in df.columns, (
            f"Column {col!r} should be absent — only reported_eps was active"
        )


def test_last_report_date_suppressed_when_beats_active(fake_scan_cache):
    """When a beat filter (or display-only) is active, the Q-1 Date
    column shows the same date as `last_report_date` would, so
    `last_report_date` is suppressed to avoid redundancy."""
    end = pd.Timestamp(date(2026, 4, 30))
    _write_ohlcv("AAPL", end)
    pd.DataFrame([
        _hist_row("AAPL", "2026-01-31", "2026-02-15", eps_pct=10.0, rev_pct=5.0),
    ]).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    p = ScanParams(
        start_date=date(2026, 1, 1), end_date=date(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        # Both an individual + a beat active. last_report_date is
        # suppressed because the q1_report_date_eps column shows it.
        reported_eps_display_only=True,
        consec_eps_beats_display_only=True,
    )
    result = scanner.run_scan(["AAPL"], p)
    df = result.results_df.set_index("symbol")

    assert "reported_eps" in df.columns
    assert "consec_eps_beats" in df.columns
    assert "q1_report_date_eps" in df.columns
    # last_report_date suppressed when beat is active.
    assert "last_report_date" not in df.columns, (
        "last_report_date should be suppressed when a beat filter is active "
        "(redundant with the Q-1 Date column)."
    )


def test_h2_columns_missing_when_history_parquet_absent(fake_scan_cache):
    """No earnings_history.parquet → graceful NaN, no warning storm."""
    end = pd.Timestamp(date(2026, 4, 30))
    _write_ohlcv("AAPL", end)
    p = ScanParams(
        start_date=date(2026, 1, 1), end_date=date(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
    )
    result = scanner.run_scan(["AAPL"], p)
    df = result.results_df
    assert not df.empty
    # Column either missing or NaN — both acceptable
    if "reported_eps" in df.columns:
        assert pd.isna(df.iloc[0]["reported_eps"])


# ──────────────────────────────────────────────────────────────────────
# H3 / L8 — Bulk fill drops per-flush reconcile, single end-of-fill instead
# ──────────────────────────────────────────────────────────────────────

class _FakeSession:
    def __init__(self, canned):
        self._canned = canned
        self.last_failure_kind = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass

    def fetch(self, symbol, years=5):
        result = self._canned.get(symbol)
        self.last_failure_kind = None if result else FAIL_NOT_FOUND
        return result


def _zacks_quarter(period_str, report_str, surp=5.0):
    return {
        "period_ending": pd.Timestamp(period_str),
        "report_date": pd.Timestamp(report_str),
        "report_time": "Close",
        "estimated_eps": 2.0, "reported_eps": 2.1,
        "surprise_eps": 0.1, "surprise_eps_pct": surp,
        "estimated_rev": 100.0, "reported_rev": 105.0,
        "surprise_rev": 5.0, "surprise_rev_pct": surp,
    }


def test_h3_single_reconcile_runs_after_fill_only(tmp_parquets):
    """Per-flush reconcile is gone (audit H3). Verify reconcile is
    invoked once at end of fill, not per flush."""
    canned = {f"T{i:02d}": [_zacks_quarter("2026-01-31", "2026-02-15")]
              for i in range(60)}
    fake = _FakeSession(canned)
    reconcile_calls = []

    @contextmanager
    def fake_ctx(*a, **kw):
        yield fake

    with patch.object(eh, "ZacksSession", fake_ctx), \
         patch.object(eh.time, "sleep"), \
         patch("trade_scanner_fh.earnings_reconcile.reconcile_earnings_dates",
               side_effect=lambda **kw: reconcile_calls.append(kw)):
        eh._fill_via_zacks(
            list(canned.keys()), blacklist=set(),
            flush_every=10, delay_sec=0,
        )

    # Pre-fix: every flush + final = 7 calls.
    # Post-fix: exactly one call from _finalize_fill.
    assert len(reconcile_calls) == 1


def test_h3_finalize_sorts_parquet_canonical(tmp_parquets):
    """Per-flush save uses sort=False; _finalize_fill re-saves with sort=True."""
    canned = {
        "BBB": [_zacks_quarter("2026-01-31", "2026-02-15")],
        "AAA": [_zacks_quarter("2026-01-31", "2026-02-15")],
    }
    fake = _FakeSession(canned)

    @contextmanager
    def fake_ctx(*a, **kw):
        yield fake

    with patch.object(eh, "ZacksSession", fake_ctx), \
         patch.object(eh.time, "sleep"):
        eh._fill_via_zacks(
            list(canned.keys()), blacklist=set(),
            flush_every=10, delay_sec=0,
        )

    final = pd.read_parquet(config.EARNINGS_HISTORY_PARQUET)
    # Sort by ticker ASC: AAA before BBB
    assert list(final["ticker"]) == ["AAA", "BBB"]


# ──────────────────────────────────────────────────────────────────────
# M1 — Imperva interstitial vs "ticker not found" classification
# ──────────────────────────────────────────────────────────────────────

def test_m1_interstitial_text_marks_failure_blocked():
    """A 200 with the Pardon-Our-Interruption page = FAIL_BLOCKED."""
    from trade_scanner_fh import zacks_scraper as zs

    interstitial_html = """<html><head><title>Pardon Our Interruption</title>
    </head><body><p>Pardon Our Interruption — Request unsuccessful.
    Incapsula incident ID: 12345</p></body></html>"""

    with patch("trade_scanner_fh.zacks_scraper.requests.Session") as mock_cls:
        sess = mock_cls.return_value
        sess.headers = {}
        resp = MagicMock()
        resp.status_code = 200
        resp.text = interstitial_html
        sess.get.return_value = resp
        sess.close.return_value = None

        with zs.ZacksSession() as s:
            rows = s.fetch("AAPL")
            assert rows is None
            assert s.last_failure_kind == FAIL_BLOCKED


def test_m1_no_obj_data_without_interstitial_marks_not_found():
    """A 200 with neither obj_data nor Imperva markers = FAIL_NOT_FOUND."""
    from trade_scanner_fh import zacks_scraper as zs

    plain_html = "<html><body><p>Stock not covered</p></body></html>"

    with patch("trade_scanner_fh.zacks_scraper.requests.Session") as mock_cls:
        sess = mock_cls.return_value
        sess.headers = {}
        resp = MagicMock()
        resp.status_code = 200
        resp.text = plain_html
        sess.get.return_value = resp
        sess.close.return_value = None

        with zs.ZacksSession() as s:
            rows = s.fetch("XYZ")
            assert rows is None
            assert s.last_failure_kind == FAIL_NOT_FOUND


def test_m1_not_found_failures_do_not_advance_auto_pause(tmp_parquets):
    """5 consecutive FAIL_NOT_FOUND tickers must NOT trip the
    auto-pause callback — only FAIL_BLOCKED runs do."""
    fake_seq = [None] * 5
    callback = MagicMock(return_value="stop")

    session = MagicMock()
    seq_iter = iter(fake_seq)

    def _fetch(*a, **kw):
        result = next(seq_iter)
        session.last_failure_kind = FAIL_NOT_FOUND
        return result

    session.fetch.side_effect = _fetch

    @contextmanager
    def fake_ctx(*a, **kw):
        yield session

    with patch.object(eh, "ZacksSession", fake_ctx), \
         patch.object(eh.time, "sleep"):
        eh._fill_via_zacks(
            ["A", "B", "C", "D", "E"], blacklist=set(),
            consec_error_limit=5,
            on_block_callback=callback,
            delay_sec=0,
        )

    callback.assert_not_called()


# ──────────────────────────────────────────────────────────────────────
# M2 — updated_at is set per-fetch, not run-start
# ──────────────────────────────────────────────────────────────────────

def test_m2_updated_at_differs_per_fetch(tmp_parquets):
    """Each ticker's `updated_at` should reflect when it was actually
    fetched. With multiple tickers fetched seconds apart, the rows'
    timestamps must differ, not all match the run-start time."""
    canned = {
        "A": [_zacks_quarter("2026-01-31", "2026-02-15")],
        "B": [_zacks_quarter("2026-01-31", "2026-02-15")],
    }
    fake = _FakeSession(canned)

    @contextmanager
    def fake_ctx(*a, **kw):
        yield fake

    # Use a real-but-fast clock by mocking sleep, and patch datetime.now
    # to step forward by 1 second per call.
    fake_now_calls = []
    base = datetime(2026, 4, 30, 12, 0, 0)

    def fake_now():
        idx = len(fake_now_calls)
        fake_now_calls.append(idx)
        return base.replace(second=idx)

    with patch.object(eh, "ZacksSession", fake_ctx), \
         patch.object(eh.time, "sleep"), \
         patch.object(eh, "datetime", MagicMock(now=fake_now)):
        eh._fill_via_zacks(
            ["A", "B"], blacklist=set(),
            flush_every=10, delay_sec=0,
        )

    df = eh.load_earnings_history()
    a_ts = df.loc[df["ticker"] == "A", "updated_at"].iloc[0]
    b_ts = df.loc[df["ticker"] == "B", "updated_at"].iloc[0]
    # A's stamp must be earlier than B's (different per-fetch values)
    assert a_ts != b_ts
    assert a_ts < b_ts


# ──────────────────────────────────────────────────────────────────────
# M3 — rewind retries pace before the retry, not after
# ──────────────────────────────────────────────────────────────────────

def test_m3_rewind_path_calls_sleep_before_retry(tmp_parquets):
    """The 'continue' branch after the block callback must call
    `time.sleep(delay_sec)` before re-entering the loop, otherwise
    the rewind tickers hit Zacks back-to-back with no pacing."""
    fake_seq = [None] * 5 + [
        [_zacks_quarter("2026-01-31", "2026-02-15")]
    ] * 5
    session = MagicMock()
    seq_iter = iter(fake_seq)

    def _fetch(*a, **kw):
        result = next(seq_iter)
        session.last_failure_kind = (
            FAIL_BLOCKED if result is None else None
        )
        return result

    session.fetch.side_effect = _fetch

    @contextmanager
    def fake_ctx(*a, **kw):
        yield session

    sleep_calls = []
    with patch.object(eh, "ZacksSession", fake_ctx), \
         patch.object(eh.time, "sleep",
                      side_effect=lambda s: sleep_calls.append(s)):
        eh._fill_via_zacks(
            ["A", "B", "C", "D", "E"], blacklist=set(),
            consec_error_limit=5,
            on_block_callback=lambda c, s: "continue",
            delay_sec=1.5,
        )

    # 5 sleeps for the initial run + 1 sleep before rewind retry +
    # 5 sleeps for the retried 5 tickers (last is post-loop) = at least 10.
    assert len(sleep_calls) >= 10
    assert all(s == 1.5 for s in sleep_calls if s)


# ──────────────────────────────────────────────────────────────────────
# M4 — compute_consecutive_beats orders by report_date DESC
# ──────────────────────────────────────────────────────────────────────

def test_m4_streak_uses_report_date_ordering():
    """Two rows with reverse period_ending vs report_date order: the
    streak compute should agree with the table's Q-i column ordering
    (audit M4 = report_date DESC everywhere)."""
    rows = [
        # Period-ending order: A (2025-09), B (2025-06)
        # Report-date order:   B (2025-12), A (2025-08)  — A reported earlier
        {"ticker": "T", "period_ending": pd.Timestamp("2025-09-30"),
         "report_date": pd.Timestamp("2025-08-15"),
         "surprise_eps_pct": 5.0},
        {"ticker": "T", "period_ending": pd.Timestamp("2025-06-30"),
         "report_date": pd.Timestamp("2025-12-01"),
         "surprise_eps_pct": -5.0},
    ]
    df = pd.DataFrame(rows)
    # By report_date DESC the most-recent row is the MISS (-5.0%).
    # Streak should be 0.
    assert eh.compute_consecutive_beats(df, "eps", 0.0) == 0


# ──────────────────────────────────────────────────────────────────────
# M5 — consec_error_limit clamped to >= 1
# ──────────────────────────────────────────────────────────────────────

def test_m5_consec_error_limit_clamped(tmp_parquets):
    """A non-positive consec_error_limit must not fire the callback
    every iteration — it gets clamped to 1."""
    fake_seq = [None] * 3
    session = MagicMock()
    seq_iter = iter(fake_seq)

    def _fetch(*a, **kw):
        result = next(seq_iter)
        session.last_failure_kind = FAIL_BLOCKED
        return result

    session.fetch.side_effect = _fetch
    callback = MagicMock(return_value="stop")

    @contextmanager
    def fake_ctx(*a, **kw):
        yield session

    with patch.object(eh, "ZacksSession", fake_ctx), \
         patch.object(eh.time, "sleep"):
        eh._fill_via_zacks(
            ["A", "B", "C"], blacklist=set(),
            consec_error_limit=0,  # non-positive
            on_block_callback=callback,
            delay_sec=0,
        )

    # Clamped to 1 → fires after the first failure, returns "stop".
    callback.assert_called_once()


# ──────────────────────────────────────────────────────────────────────
# L3 — set_zacks_cookies("") on missing file is a clean success
# ──────────────────────────────────────────────────────────────────────

def test_l3_clear_cookies_when_file_absent_succeeds(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    # File does not exist
    assert set_zacks_cookies("") is True


def test_zacks_cookies_roundtrip_and_plaintext_backcompat(tmp_path, monkeypatch):
    """Audit (sec-zacks-cookies-plaintext): cookies round-trip through
    set/get (DPAPI-encrypted on Windows, plaintext fallback elsewhere), and a
    pre-existing legacy plaintext file (no DPAPI marker) still reads back
    verbatim so the at-rest-encryption upgrade can't lock anyone out."""
    from trade_scanner_fh import zacks_scraper as zs
    monkeypatch.setattr(zs.config, "DATA_DIR", tmp_path)

    blob = "reese84=abc123; visid_incap_9=def456"
    assert zs.set_zacks_cookies(blob) is True
    assert zs.get_zacks_cookies() == blob  # encrypt→decrypt (or plaintext) round-trip

    # Legacy plaintext file written before encryption existed.
    zs._cookies_path().write_text("k=v; a=b", encoding="utf-8")
    assert zs.get_zacks_cookies() == "k=v; a=b"


# ──────────────────────────────────────────────────────────────────────
# L4 — _strip_html unescapes arbitrary HTML entities
# ──────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("&amp;", "&"),
    ("Pre &lt;tag&gt; post", "Pre <tag> post"),
    ("AT&amp;T", "AT&T"),
    ("Mr. Smith&#39;s shares", "Mr. Smith's shares"),
    ("&nbsp;hello&nbsp;", "hello"),
])
def test_l4_strip_html_decodes_entities(raw, expected):
    assert _strip_html(raw) == expected


# ──────────────────────────────────────────────────────────────────────
# L5 — _normalize_time tolerates whitespace runs
# ──────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("Before Open", "Open"),
    ("Before  Open", "Open"),     # double space
    ("Before\tOpen", "Open"),     # tab
    ("Before Open", "Open"), # nbsp
    ("After  Close", "Close"),
    ("During    Market    Hours", "Market"),
])
def test_l5_normalize_time_whitespace_tolerant(raw, expected):
    assert _normalize_time(raw) == expected
