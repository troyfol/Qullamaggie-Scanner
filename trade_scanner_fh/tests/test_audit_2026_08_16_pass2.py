"""Audit 2026-08-16 — Pass 2 (detection) regression tests.

Covers F2 (OHLCV interior gaps), F6 (missing quarters), F11 (schema-version
verdict) and F12 (anomaly persistence).

The through-line: before this pass the codebase could not SEE any of these.
F2's holes were found by ad-hoc probing because F12's output was discarded;
F6 is F1's detector, and its absence is why a truncation could have run for
months unnoticed.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from trade_scanner_fh import config, data_engine
from trade_scanner_fh import earnings_history as eh


# ── helpers ───────────────────────────────────────────────────────────

def _write_ohlcv(symbol: str, dates, *, tz="America/New_York"):
    """Write a per-ticker parquet whose index is exactly `dates`."""
    idx = pd.DatetimeIndex(pd.to_datetime(list(dates)))
    if tz:
        idx = idx.tz_localize(tz)
    idx.name = "Date"
    df = pd.DataFrame(
        {
            "Open": [10.0] * len(idx), "High": [11.0] * len(idx),
            "Low": [9.0] * len(idx), "Close": [10.5] * len(idx),
            "Volume": [1_000] * len(idx), "Stock Splits": [0.0] * len(idx),
        },
        index=idx,
    )
    df.to_parquet(config.PARQUET_DIR / f"{symbol}.parquet")
    return idx


def _sessions(n=40, start="2026-01-05"):
    return pd.bdate_range(start=start, periods=n)


# ══════════════════════════════════════════════════════════════════════
# F2 — interior gap detection
# ══════════════════════════════════════════════════════════════════════

def test_interior_hole_is_detected(fake_scan_cache):
    """The measured failure: a ticker whose LAST bar is current but which is
    missing sessions from the middle. Staleness keys on the last bar only, so
    nothing ever re-fetched these — 261 of 581 sampled live tickers."""
    sess = _sessions()
    _write_ohlcv("SPY", sess)
    holed = sess.delete([10, 11, 25])          # three interior sessions gone
    _write_ohlcv("AAPL", holed)

    gaps = data_engine.find_interior_gaps(["AAPL"])

    assert gaps == {"AAPL": 3}


def test_complete_ticker_is_not_flagged(fake_scan_cache):
    sess = _sessions()
    _write_ohlcv("SPY", sess)
    _write_ohlcv("AAPL", sess)
    assert data_engine.find_interior_gaps(["AAPL"]) == {}


def test_short_history_is_judged_only_on_its_own_span(fake_scan_cache):
    """A recent IPO must not be flagged for the years it never had — the
    comparison window is the ticker's OWN first→last range."""
    sess = _sessions()
    _write_ohlcv("SPY", sess)
    _write_ohlcv("IPO", sess[-10:])            # complete, just short
    assert data_engine.find_interior_gaps(["IPO"]) == {}


def test_delisted_ticker_is_judged_only_on_its_own_span(fake_scan_cache):
    sess = _sessions()
    _write_ohlcv("SPY", sess)
    _write_ohlcv("DEAD", sess[:12])            # stops early, but complete
    assert data_engine.find_interior_gaps(["DEAD"]) == {}


def test_gap_sweep_is_a_noop_without_a_reference(fake_scan_cache):
    """Expected on a fresh install — must degrade quietly, not raise."""
    _write_ohlcv("AAPL", _sessions())
    assert data_engine.reference_sessions() is None
    assert data_engine.find_interior_gaps(["AAPL"]) == {}


def test_missing_and_unreadable_files_are_skipped(fake_scan_cache):
    sess = _sessions()
    _write_ohlcv("SPY", sess)
    (config.PARQUET_DIR / "JUNK.parquet").write_bytes(b"not a parquet")
    gaps = data_engine.find_interior_gaps(["NOPE", "JUNK"])
    assert gaps == {}


def test_precomputed_spans_are_reused(fake_scan_cache):
    """The launch path reads every footer once for staleness; re-reading them
    for the gap sweep would add ~2 minutes (10.6 ms/file × 11,854)."""
    sess = _sessions()
    _write_ohlcv("SPY", sess)
    _write_ohlcv("AAPL", sess.delete([5, 6]))

    spans = data_engine.cached_spans(["SPY", "AAPL", "GONE"])
    assert spans["GONE"] is None
    assert spans["AAPL"][2] == len(sess) - 2      # row count from the footer

    # Deleting the file must NOT change the answer — proof the spans were used
    # rather than the parquet being re-read.
    (config.PARQUET_DIR / "AAPL.parquet").unlink()
    gaps = data_engine.find_interior_gaps(
        ["AAPL"], sessions=sess, spans=spans)
    assert gaps == {"AAPL": 2}


def test_footer_span_survives_a_tz_aware_index(fake_scan_cache):
    """Every cached file has a `datetimetz America/New_York` index and pyarrow
    reports its statistics as tz-AWARE UTC. Comparing that against the naive
    reference index raises — the same hazard `_bars_after` exists to handle."""
    sess = _sessions()
    _write_ohlcv("SPY", sess, tz="America/New_York")
    _write_ohlcv("AAPL", sess.delete([4]), tz="America/New_York")

    lo, hi, _ = data_engine._footer_span(config.PARQUET_DIR / "SPY.parquet")
    assert lo.tzinfo is None and hi.tzinfo is None
    assert lo == pd.Timestamp(sess[0])           # date preserved, not shifted
    assert data_engine.find_interior_gaps(["AAPL"]) == {"AAPL": 1}


def test_footer_span_reads_without_touching_data_pages(fake_scan_cache):
    """The sweep runs across ~12k files, so it must stay metadata-only."""
    sess = _sessions()
    _write_ohlcv("SPY", sess)
    lo, hi, rows = data_engine._footer_span(
        config.PARQUET_DIR / "SPY.parquet")
    assert rows == len(sess)
    assert lo == pd.Timestamp(sess[0])
    assert hi == pd.Timestamp(sess[-1])


# ── F2 — bounded, ledger-guarded selection ────────────────────────────

def test_selection_is_worst_first_and_bounded(fake_scan_cache):
    """Unbounded, the first run after this ships would queue ~5,300 full
    re-pulls at yfinance pacing."""
    gaps = {"A": 1, "B": 9, "C": 5, "D": 3}
    assert data_engine.select_gap_rebuilds(gaps, limit=2) == ["B", "C"]


def test_selection_skips_recently_attempted(fake_scan_cache):
    """A hole that SURVIVES a rebuild is source-level — a real halt, or bars
    the provider lacks. Without this it would be rebuilt every launch."""
    today = date(2026, 8, 16)
    data_engine.record_gap_attempts(["B"], today=today)

    picked = data_engine.select_gap_rebuilds(
        {"A": 1, "B": 9}, recheck_days=90, today=today)

    assert picked == ["A"], "B was just attempted and must rest"


def test_recheck_window_expires(fake_scan_cache):
    data_engine.record_gap_attempts(["B"], today=date(2026, 1, 1))
    picked = data_engine.select_gap_rebuilds(
        {"B": 9}, recheck_days=90, today=date(2026, 8, 16))
    assert picked == ["B"]


def test_attempt_ledger_survives_a_corrupt_file(fake_scan_cache):
    """The ledger is an optimisation — losing it costs a repeated rebuild,
    never data — so a malformed file must not break the sweep."""
    data_engine._gap_attempts_path().write_text("{not json", encoding="utf-8")
    assert data_engine.load_gap_attempts() == {}
    assert data_engine.select_gap_rebuilds({"A": 1}) == ["A"]


def test_attempt_ledger_round_trips(fake_scan_cache):
    data_engine.record_gap_attempts(["A", "B"], today=date(2026, 8, 16))
    data_engine.record_gap_attempts(["C"], today=date(2026, 8, 17))
    saved = json.loads(
        data_engine._gap_attempts_path().read_text(encoding="utf-8"))
    assert saved == {"A": "2026-08-16", "B": "2026-08-16", "C": "2026-08-17"}


# ══════════════════════════════════════════════════════════════════════
# F12 — validate_ticker output is persisted, not discarded
# ══════════════════════════════════════════════════════════════════════

class _Res:
    def __init__(self, symbol, anomalies):
        self.symbol = symbol
        self.anomalies = anomalies


def test_anomalies_are_written_to_a_csv(fake_scan_cache):
    n = data_engine.write_anomaly_report([
        _Res("AAPL", ["3 zero-volume bar(s)", "1 duplicate date(s)"]),
        _Res("MSFT", []),
        _Res("TSLA", ["2 bar(s) with zero price(s)"]),
    ])

    assert n == 3
    df = pd.read_csv(data_engine.anomalies_csv_path())
    assert list(df.columns) == data_engine.ANOMALY_COLUMNS
    assert set(df["ticker"]) == {"AAPL", "TSLA"}
    assert (df["ticker"] == "AAPL").sum() == 2


def test_a_clean_run_leaves_an_existing_report_alone(fake_scan_cache):
    """The trap that destroyed the disagreement report before v5.5.1: a small
    run rewriting the file wipes a full sweep's findings."""
    data_engine.write_anomaly_report([_Res("AAPL", ["3 zero-volume bar(s)"])])
    before = data_engine.anomalies_csv_path().read_text(encoding="utf-8")

    assert data_engine.write_anomaly_report([_Res("MSFT", [])]) == 0

    assert data_engine.anomalies_csv_path().read_text(
        encoding="utf-8") == before


def test_anomaly_report_never_breaks_an_update(fake_scan_cache):
    class _Boom:
        symbol = "X"

        @property
        def anomalies(self):
            raise RuntimeError("boom")

    # Reading .anomalies raises; the helper must swallow it.
    with pytest.raises(RuntimeError):
        _ = _Boom().anomalies
    assert data_engine.write_anomaly_report([]) == 0


# ══════════════════════════════════════════════════════════════════════
# F6 — missing-quarter detection
# ══════════════════════════════════════════════════════════════════════

def _q(ticker, period, source="finviz", eps=1.0):
    ts = pd.Timestamp(period)
    return {
        "ticker": ticker, "period_ending": ts,
        "report_date": ts + pd.DateOffset(months=2), "report_time": "Close",
        "estimated_eps": None, "reported_eps": eps,
        "surprise_eps": None, "surprise_eps_pct": None,
        "estimated_rev": None, "reported_rev": None,
        "surprise_rev": None, "surprise_rev_pct": None,
        "source": source, "updated_at": pd.Timestamp("2026-08-16"),
        "report_date_proxy": False,
    }


def _recent_quarters(ticker, n=8, *, skip=()):
    """`n` contiguous quarters ending near today, optionally omitting some."""
    end = pd.Timestamp.today().normalize().replace(day=1)
    rows = []
    for i in range(n):
        if i in skip:
            continue
        rows.append(_q(ticker, end - pd.DateOffset(months=3 * i)))
    return rows


def test_missing_quarter_is_reported(tmp_parquets):
    """F1's detector. A truncation removes quarters one ticker at a time, and
    none of the other 14 checks asks whether quarters are contiguous."""
    df = pd.DataFrame(_recent_quarters("AAPL", 8, skip=(3,)))
    findings = {f.check: f for f in eh.verify_integrity(history_df=df)}

    assert "missing_quarter" in findings
    f = findings["missing_quarter"]
    assert f.affected_rows == 1
    assert f.auto_fixable is False, (
        "a missing quarter cannot be repaired by rewriting what is here"
    )
    assert f.sample and f.sample[0]["ticker"] == "AAPL"


def test_contiguous_history_is_clean(tmp_parquets):
    df = pd.DataFrame(_recent_quarters("AAPL", 8))
    checks = {f.check for f in eh.verify_integrity(history_df=df)}
    assert "missing_quarter" not in checks


def test_old_gaps_are_outside_the_window(tmp_parquets):
    """Legitimate deep-history gaps dominate — IPOs, dark periods, the
    retention cap — so the check is deliberately scoped to recent quarters."""
    df = pd.DataFrame([
        _q("AAPL", "2009-03-01"),
        _q("AAPL", "2018-06-01"),      # a 9-year hole, but long ago
    ])
    checks = {f.check for f in eh.verify_integrity(history_df=df)}
    assert "missing_quarter" not in checks


def test_gap_check_does_not_span_tickers(tmp_parquets):
    """Two tickers each with one quarter must not look like one gap."""
    df = pd.DataFrame(
        [_recent_quarters("AAA", 1)[0], _recent_quarters("BBB", 1)[0]])
    checks = {f.check for f in eh.verify_integrity(history_df=df)}
    assert "missing_quarter" not in checks


def test_gap_finding_counts_every_hole(tmp_parquets):
    df = pd.DataFrame(
        _recent_quarters("AAA", 8, skip=(2,))
        + _recent_quarters("BBB", 8, skip=(4, 5)))
    f = eh._quarter_gap_finding(df)
    assert f is not None
    # BBB's two consecutive omissions read as ONE hole, AAA's as another.
    assert f.affected_rows == 2


# ══════════════════════════════════════════════════════════════════════
# F11 — schema-version verdict
# ══════════════════════════════════════════════════════════════════════

def test_schema_check_reports_ok_when_matching(fake_scan_cache):
    data_engine.stamp_schema_version()
    status, stamped, expected = data_engine.check_schema_version()
    assert status == data_engine.SCHEMA_OK
    assert stamped == expected == config.PARQUET_SCHEMA_VERSION


def test_schema_check_stamps_an_unstamped_cache(fake_scan_cache):
    _write_ohlcv("AAPL", _sessions())
    status, stamped, _ = data_engine.check_schema_version()
    assert status == data_engine.SCHEMA_OK
    assert stamped is None                      # nothing was there before
    assert data_engine.read_schema_version() == config.PARQUET_SCHEMA_VERSION


def test_schema_check_distinguishes_older_from_newer(fake_scan_cache):
    """The directions are not equally dangerous: a NEWER cache was written by
    a build this one may not understand, and writing to it loses that data."""
    expected = config.PARQUET_SCHEMA_VERSION
    config.atomic_write_text(config.PARQUET_SCHEMA_FILE, str(expected - 1))
    assert data_engine.check_schema_version()[0] == data_engine.SCHEMA_OLDER

    config.atomic_write_text(config.PARQUET_SCHEMA_FILE, str(expected + 1))
    assert data_engine.check_schema_version()[0] == data_engine.SCHEMA_NEWER


def test_schema_check_treats_an_unreadable_stamp_as_unstamped(fake_scan_cache):
    config.atomic_write_text(config.PARQUET_SCHEMA_FILE, "not-a-number")
    status, stamped, _ = data_engine.check_schema_version()
    assert status == data_engine.SCHEMA_OK
    assert stamped is None
