"""Tests for the split-adjustment seam quarantine (item 0).

Covers the seam classifier, the generated skip list, the tolerant loader, and
the scan-time exclusion + funnel stage.
"""
import pandas as pd
import pytest

from trade_scanner_fh import config, data_engine, scanner
from trade_scanner_fh.data_engine import (
    find_seams_in_frame,
    load_split_seam_skip,
    rebuild_split_seam_skip,
    _seam_verdict,
)


def _frame(closes, splits, start="2024-01-02"):
    idx = pd.bdate_range(start, periods=len(closes))
    return pd.DataFrame({"Close": closes, "Stock Splits": splits}, index=idx)


# ----------------------------------------------------------------------
# Seam classifier
# ----------------------------------------------------------------------

def test_step_matching_ratio_is_unadjusted():
    """BESS's real shape: 9.80 -> 0.07 across a 1-for-140, step == ratio."""
    assert _seam_verdict(0.007143, 0.007143) == "UNADJUSTED"


def test_step_near_one_is_adjusted():
    """DWTX's real shape: a 1-for-25 whose series stays continuous."""
    assert _seam_verdict(0.7303, 0.04) == "adjusted"
    assert _seam_verdict(0.9857, 0.04) == "adjusted"


def test_step_near_neither_is_ambiguous():
    assert _seam_verdict(0.25, 0.001) == "ambiguous"


def test_overlapping_hypotheses_refuse_to_decide():
    """At ratio 2.0 the "stepped by the ratio" and "stepped by ~1.0" bands
    overlap, so a step inside the overlap must not be forced either way.
    BDPT's real shape: x1.43 across a 2-for-1."""
    assert _seam_verdict(1.428571, 2.0) == "ambiguous"
    assert _seam_verdict(0.70, 0.5) == "ambiguous"


def test_unambiguous_small_ratio_still_decides():
    """The overlap guard must not blunt a clean call at the same ratio."""
    assert _seam_verdict(2.0, 2.0) == "UNADJUSTED"
    assert _seam_verdict(0.5, 0.5) == "UNADJUSTED"
    assert _seam_verdict(0.98, 0.5) == "adjusted"


def test_bad_prices_reported_not_crashed():
    assert _seam_verdict(float("nan"), 0.05) == "bad_price"
    assert _seam_verdict(0.0, 0.05) == "bad_price"
    assert _seam_verdict(-1.0, 0.05) == "bad_price"


# ----------------------------------------------------------------------
# Frame-level detection
# ----------------------------------------------------------------------

def test_unadjusted_seam_detected():
    df = _frame([10.0, 10.0, 0.5, 0.5], [0, 0, 0.05, 0])
    got = find_seams_in_frame("TEST", df)
    assert len(got) == 1
    assert got[0].verdict == "UNADJUSTED"
    assert got[0].ratio == pytest.approx(0.05)
    assert got[0].step == pytest.approx(0.05)


def test_adjusted_series_produces_no_finding():
    df = _frame([10.0, 10.0, 9.8, 10.1], [0, 0, 0.05, 0])
    got = find_seams_in_frame("TEST", df)
    assert len(got) == 1
    assert got[0].verdict == "adjusted"


def test_spinoff_ratio_is_never_tested():
    """A distribution factor is not a share ratio, so is_share_split gates it
    out before the price step is even looked at — otherwise HON and GSK would
    be judged against a step they were never expected to make."""
    df = _frame([10.0, 10.0, 9.5, 9.6], [0, 0, 0.9535, 0])
    assert find_seams_in_frame("HON", df) == []


def test_split_on_first_bar_is_skipped():
    """No prior bar exists to compare against; must not IndexError."""
    df = _frame([10.0, 10.0, 10.0], [0.05, 0, 0])
    assert find_seams_in_frame("TEST", df) == []


def test_zero_split_column_produces_nothing():
    df = _frame([10.0, 10.1, 10.2], [0, 0, 0])
    assert find_seams_in_frame("TEST", df) == []


def test_missing_columns_tolerated():
    assert find_seams_in_frame("T", pd.DataFrame()) == []
    assert find_seams_in_frame("T", pd.DataFrame({"Close": [1.0]})) == []
    assert find_seams_in_frame("T", None) == []


def test_multiple_events_each_classified():
    df = _frame(
        [10.0, 10.0, 0.5, 0.5, 0.49, 0.50],
        [0, 0, 0.05, 0, 0.1, 0],
    )
    got = find_seams_in_frame("TEST", df)
    assert [f.verdict for f in got] == ["UNADJUSTED", "adjusted"]


# ----------------------------------------------------------------------
# Skip-list round trip
# ----------------------------------------------------------------------

def test_rebuild_writes_only_unadjusted_tickers(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "PARQUET_DIR", tmp_path)
    monkeypatch.setattr(config, "SPLIT_SEAM_SKIP_FILE",
                        tmp_path / "split_seam_skip.txt")

    _frame([10.0, 10.0, 0.5, 0.5], [0, 0, 0.05, 0]).to_parquet(
        tmp_path / "BAD.parquet")
    _frame([10.0, 10.0, 9.8, 10.1], [0, 0, 0.05, 0]).to_parquet(
        tmp_path / "GOOD.parquet")
    _frame([10.0, 10.1, 10.2, 10.3], [0, 0, 0, 0]).to_parquet(
        tmp_path / "PLAIN.parquet")

    tickers, events = rebuild_split_seam_skip(["BAD", "GOOD", "PLAIN"])
    assert (tickers, events) == (1, 1)
    assert load_split_seam_skip() == frozenset({"BAD"})


def test_generated_file_carries_a_do_not_rebuild_warning(tmp_path, monkeypatch):
    """The whole point of the file is that rebuilding cannot fix these, so the
    warning must survive in the artifact itself."""
    monkeypatch.setattr(config, "PARQUET_DIR", tmp_path)
    monkeypatch.setattr(config, "SPLIT_SEAM_SKIP_FILE",
                        tmp_path / "seam.txt")
    _frame([10.0, 10.0, 0.5], [0, 0, 0.05]).to_parquet(tmp_path / "BAD.parquet")
    rebuild_split_seam_skip(["BAD"])
    text = (tmp_path / "seam.txt").read_text(encoding="utf-8")
    assert "DO NOT REBUILD" in text
    assert "GENERATED" in text


def test_loader_returns_empty_when_file_absent(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "SPLIT_SEAM_SKIP_FILE", tmp_path / "nope.txt")
    assert load_split_seam_skip() == frozenset()


def test_loader_skips_comments_and_accepts_commas(tmp_path, monkeypatch):
    p = tmp_path / "seam.txt"
    p.write_text("# header\n# events=3\nAAA\nbbb, CCC\n\n", encoding="utf-8")
    monkeypatch.setattr(config, "SPLIT_SEAM_SKIP_FILE", p)
    assert load_split_seam_skip() == frozenset({"AAA", "BBB", "CCC"})


# ----------------------------------------------------------------------
# v6.1.2 — transient bad bars are not seams
# ----------------------------------------------------------------------

def test_single_bad_bar_at_ex_date_is_transient_not_unadjusted():
    """APRE's real shape: the series runs ~10, the ex-date bar alone prints at
    10 x 0.05 = 0.474, and the next session is back at 8.20. The adjacent step
    is a perfect match to the 1-for-20, but nothing about the SERIES changed
    basis, so this is a bad print and must not quarantine the ticker."""
    df = _frame(
        [10.4, 10.4, 10.2, 10.0, 10.4, 0.5, 8.2, 7.8, 7.1, 6.9],
        [0, 0, 0, 0, 0, 0.05, 0, 0, 0, 0],
    )
    got = find_seams_in_frame("APRE", df)
    assert [f.verdict for f in got] == ["transient"]
    assert got[0].level_ratio == pytest.approx(7.45 / 10.4, rel=1e-6)


def test_two_bar_dropout_is_still_transient():
    """SILO's real shape: TWO zero-volume bars at the split ratio, then the
    series resumes at its old level. A median over five bars survives both."""
    df = _frame(
        [136.5, 150.0, 141.75, 149.25, 138.75, 2.55, 2.55,
         108.75, 109.5, 105.0, 107.25, 107.45],
        [0, 0, 0, 0, 0, 0.02, 0, 0, 0, 0, 0, 0],
    )
    assert [f.verdict for f in find_seams_in_frame("SILO", df)] == ["transient"]


def test_genuine_seam_survives_the_level_test():
    """The level test must not blunt a real basis change: BESS steps by its
    1-for-140 and STAYS there, so before/after levels differ by the ratio."""
    df = _frame(
        [9.8, 9.9, 9.7, 9.8, 9.85, 0.07, 0.071, 0.069, 0.07, 0.072],
        [0, 0, 0, 0, 0, 0.007143, 0, 0, 0, 0],
    )
    got = find_seams_in_frame("BESS", df)
    assert [f.verdict for f in got] == ["UNADJUSTED"]
    assert got[0].level_ratio < 0.01


def test_transient_findings_go_to_the_anomaly_report(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    findings = [
        data_engine.SeamFinding("APRE", pd.Timestamp("2023-02-13"), 0.05,
                                0.05, "transient", 0.73, float("nan")),
        data_engine.SeamFinding("BESS", pd.Timestamp("2025-02-03"), 0.007143,
                                0.0071, "UNADJUSTED", 0.007, 16.9),
    ]
    assert data_engine.write_split_bad_bar_report(findings) == 1
    df = pd.read_csv(data_engine.anomalies_csv_path())
    assert list(df["ticker"]) == ["APRE"]
    assert df.loc[0, "source"] == data_engine.ANOMALY_SOURCE_SPLIT_SWEEP
    assert "2023-02-13" in df.loc[0, "anomaly"]


def test_split_sweep_rows_do_not_erase_the_download_validator_rows(
        tmp_path, monkeypatch):
    """Two producers write one CSV. Each may only replace its own rows — the
    trap that destroyed the disagreement report before v5.5.1."""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    class _Res:
        def __init__(self, symbol, anomalies):
            self.symbol, self.anomalies = symbol, anomalies

    data_engine.write_anomaly_report([_Res("ZZZ", ["3 zero-volume bar(s)"])])
    data_engine.write_split_bad_bar_report([
        data_engine.SeamFinding("APRE", pd.Timestamp("2023-02-13"), 0.05,
                                0.05, "transient", 0.73, float("nan")),
    ])
    df = pd.read_csv(data_engine.anomalies_csv_path())
    assert set(df["ticker"]) == {"ZZZ", "APRE"}

    # A second sweep with nothing transient clears only the sweep's own rows.
    data_engine.write_split_bad_bar_report([])
    df = pd.read_csv(data_engine.anomalies_csv_path())
    assert list(df["ticker"]) == ["ZZZ"]


def test_legacy_anomaly_csv_without_source_is_attributed_to_the_validator(
        tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    pd.DataFrame([{"ticker": "OLD", "anomaly": "1 zero-volume bar(s)",
                   "detected_at": "2026-01-01T00:00:00"}]).to_csv(
        data_engine.anomalies_csv_path(), index=False)
    data_engine.write_split_bad_bar_report([
        data_engine.SeamFinding("APRE", pd.Timestamp("2023-02-13"), 0.05,
                                0.05, "transient", 0.73, float("nan")),
    ])
    df = pd.read_csv(data_engine.anomalies_csv_path())
    assert set(df["ticker"]) == {"OLD", "APRE"}
    assert df.set_index("ticker").loc["OLD", "source"] == (
        data_engine.ANOMALY_SOURCE_DOWNLOAD)


# ----------------------------------------------------------------------
# v6.1.2 — a weak ratio needs the step to be an outlier
# ----------------------------------------------------------------------

def test_weak_ratio_inside_the_tickers_own_noise_is_ambiguous():
    """PPCB's real shape: a 1-for-2 on a stock that swings 0.010 <-> 0.015
    every day. A x0.36 step is smaller than its ordinary range, so it is not
    evidence — and refusing to decide leaves the ticker scannable."""
    closes = [0.010, 0.015, 0.010, 0.015, 0.010, 0.015, 0.0054,
              0.0036, 0.0054, 0.0036, 0.0054, 0.0036]
    splits = [0] * 6 + [0.5] + [0] * 5
    got = find_seams_in_frame("PPCB", _frame(closes, splits))
    assert [f.verdict for f in got] == ["ambiguous"]


def test_weak_ratio_that_IS_an_outlier_still_condemns():
    """SMCX's real shape: the same 1-for-2 magnitude, but on a series whose
    ordinary day is a couple of percent."""
    closes = [23.4, 22.8, 23.0, 22.6, 22.9, 22.7, 7.6,
              7.5, 7.7, 7.6, 7.8, 7.7]
    splits = [0] * 6 + [0.5] + [0] * 5
    got = find_seams_in_frame("SMCX", _frame(closes, splits))
    assert [f.verdict for f in got] == ["UNADJUSTED"]
    assert got[0].isolation > config.SPLIT_SEAM_MIN_ISOLATION


def test_strong_ratio_is_not_subject_to_the_isolation_test():
    """A step matching a 1-for-140 to four decimals is not something noise
    produces, however wild the ticker is — the gate must not reach it."""
    closes = [9.8, 4.0, 12.0, 5.0, 11.0, 0.077, 0.07, 0.15, 0.06, 0.08]
    splits = [0] * 5 + [0.007143] + [0] * 4
    got = find_seams_in_frame("WILD", _frame(closes, splits))
    assert [f.verdict for f in got] == ["UNADJUSTED"]


# ----------------------------------------------------------------------
# v6.1.2 — the skip list is dated and the scan is window-aware
# ----------------------------------------------------------------------

def test_skip_file_carries_the_newest_seam_date(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "PARQUET_DIR", tmp_path)
    monkeypatch.setattr(config, "SPLIT_SEAM_SKIP_FILE", tmp_path / "seam.txt")
    df = _frame(
        [10.0, 10.0, 10.0, 0.5, 0.5, 0.5, 0.5, 0.025, 0.025, 0.025, 0.025],
        [0, 0, 0, 0.05, 0, 0, 0, 0.05, 0, 0, 0],
    )
    df.to_parquet(tmp_path / "BAD.parquet")
    tickers, events = rebuild_split_seam_skip(["BAD"])
    assert (tickers, events) == (1, 2)
    text = (tmp_path / "seam.txt").read_text(encoding="utf-8")
    assert "BAD\t2024-01-11" in text          # the SECOND event, not the first
    got = data_engine.load_split_seam_quarantine()
    assert got == {"BAD": pd.Timestamp("2024-01-11")}


def test_undated_legacy_line_still_loads_and_is_unconditional(
        tmp_path, monkeypatch):
    p = tmp_path / "seam.txt"
    p.write_text("# header\nAAA\nBBB\t2025-06-01\n", encoding="utf-8")
    monkeypatch.setattr(config, "SPLIT_SEAM_SKIP_FILE", p)
    got = data_engine.load_split_seam_quarantine()
    assert got == {"AAA": None, "BBB": pd.Timestamp("2025-06-01")}
    assert load_split_seam_skip() == frozenset({"AAA", "BBB"})


def test_unparseable_date_degrades_to_unconditional(tmp_path, monkeypatch):
    p = tmp_path / "seam.txt"
    p.write_text("AAA\tnot-a-date\n", encoding="utf-8")
    monkeypatch.setattr(config, "SPLIT_SEAM_SKIP_FILE", p)
    assert data_engine.load_split_seam_quarantine() == {"AAA": None}


def test_seam_relevance_cutoff_precedes_start_by_the_longest_lookback(
        monkeypatch):
    monkeypatch.setattr(data_engine, "reference_sessions", lambda: None)
    params = scanner.ScanParams(start_date="2025-01-01",
                                end_date="2025-06-30", sma1_period=200)
    cutoff = scanner.seam_relevance_cutoff(params)
    assert cutoff < pd.Timestamp("2025-01-01")
    # Generous by design: at least the ~290 calendar days 200 sessions span.
    assert (pd.Timestamp("2025-01-01") - cutoff).days >= 290


def test_shorter_lookbacks_shrink_the_protected_span(monkeypatch):
    monkeypatch.setattr(data_engine, "reference_sessions", lambda: None)
    short = scanner.ScanParams(start_date="2025-01-01", sma1_period=20,
                               sma2_period=20, sti_long_lb=20)
    long = scanner.ScanParams(start_date="2025-01-01")
    assert scanner.seam_relevance_cutoff(short) > (
        scanner.seam_relevance_cutoff(long))


def test_seam_outside_the_scans_reach_is_not_dropped(monkeypatch):
    """GBCS's real position: a seam 1,238 bars back cannot reach any indicator
    the scan runs, so excluding the ticker protects nothing."""
    monkeypatch.setattr(data_engine, "reference_sessions", lambda: None)
    ctx = scanner.ScanContext(
        seam_quarantine={"OLD": pd.Timestamp("2019-01-02")})
    seen = []
    monkeypatch.setattr(scanner, "_compute_ticker",
                        lambda sym, *a, **k: seen.append(sym))
    params = scanner.ScanParams(start_date="2025-01-01", end_date="2025-06-30")
    res = scanner.run_scan(["AAA", "OLD"], params, context=ctx)
    assert seen == ["AAA", "OLD"]
    assert not [s for s in res.funnel if s.name == "Split-seam quarantine"]


def test_seam_inside_the_scans_reach_is_still_dropped(monkeypatch):
    monkeypatch.setattr(data_engine, "reference_sessions", lambda: None)
    ctx = scanner.ScanContext(
        seam_quarantine={"NEW": pd.Timestamp("2025-03-04")})
    seen = []
    monkeypatch.setattr(scanner, "_compute_ticker",
                        lambda sym, *a, **k: seen.append(sym))
    params = scanner.ScanParams(start_date="2025-01-01", end_date="2025-06-30")
    res = scanner.run_scan(["AAA", "NEW"], params, context=ctx)
    assert seen == ["AAA"]
    assert [s.name for s in res.funnel
            if s.name == "Split-seam quarantine"] == ["Split-seam quarantine"]


def test_seam_inside_the_trailing_lookback_is_dropped(monkeypatch):
    """A seam BEFORE start_date still corrupts SMA200, which reads 200 bars of
    history behind the window."""
    monkeypatch.setattr(data_engine, "reference_sessions", lambda: None)
    ctx = scanner.ScanContext(
        seam_quarantine={"PRE": pd.Timestamp("2024-11-01")})
    seen = []
    monkeypatch.setattr(scanner, "_compute_ticker",
                        lambda sym, *a, **k: seen.append(sym))
    params = scanner.ScanParams(start_date="2025-01-01", end_date="2025-06-30")
    scanner.run_scan(["AAA", "PRE"], params, context=ctx)
    assert seen == ["AAA"]


def test_undated_entry_is_dropped_regardless_of_window(monkeypatch):
    monkeypatch.setattr(data_engine, "reference_sessions", lambda: None)
    ctx = scanner.ScanContext(seam_quarantine={"BAD": None})
    seen = []
    monkeypatch.setattr(scanner, "_compute_ticker",
                        lambda sym, *a, **k: seen.append(sym))
    params = scanner.ScanParams(start_date="2025-01-01", end_date="2025-06-30")
    scanner.run_scan(["AAA", "BAD"], params, context=ctx)
    assert seen == ["AAA"]


def test_seam_skip_property_still_exposes_the_ticker_set():
    ctx = scanner.ScanContext(
        seam_quarantine={"A": None, "B": pd.Timestamp("2025-01-01")})
    assert ctx.seam_skip == frozenset({"A", "B"})


# ----------------------------------------------------------------------
# Scan-time exclusion
# ----------------------------------------------------------------------

def test_run_scan_excludes_quarantined_and_records_funnel(monkeypatch):
    ctx = scanner.ScanContext(seam_quarantine={"BAD": None})
    seen = []

    def fake_compute(sym, *a, **k):
        seen.append(sym)
        return None

    monkeypatch.setattr(scanner, "_compute_ticker", fake_compute)
    params = scanner.ScanParams(start_date="2024-01-02", end_date="2024-06-28")
    res = scanner.run_scan(["AAA", "BAD", "CCC"], params, context=ctx)

    assert "BAD" not in seen
    stages = [s for s in res.funnel if s.name == "Split-seam quarantine"]
    assert len(stages) == 1
    assert (stages[0].total_before, stages[0].passed) == (3, 2)


def test_run_scan_case_insensitive_exclusion(monkeypatch):
    ctx = scanner.ScanContext(seam_quarantine={"BAD": None})
    seen = []
    monkeypatch.setattr(scanner, "_compute_ticker",
                        lambda sym, *a, **k: seen.append(sym))
    params = scanner.ScanParams(start_date="2024-01-02", end_date="2024-06-28")
    scanner.run_scan(["aaa", "bad"], params, context=ctx)
    assert seen == ["aaa"]


def test_run_scan_without_quarantine_adds_no_stage(monkeypatch):
    """An empty skip list must leave the funnel exactly as it was, so existing
    funnel assertions elsewhere keep holding."""
    monkeypatch.setattr(scanner, "_compute_ticker", lambda sym, *a, **k: None)
    params = scanner.ScanParams(start_date="2024-01-02", end_date="2024-06-28")
    res = scanner.run_scan(["AAA"], params, context=scanner.ScanContext())
    assert not [s for s in res.funnel if s.name == "Split-seam quarantine"]
