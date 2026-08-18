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
# Scan-time exclusion
# ----------------------------------------------------------------------

def test_run_scan_excludes_quarantined_and_records_funnel(monkeypatch):
    ctx = scanner.ScanContext(seam_skip=frozenset({"BAD"}))
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
    ctx = scanner.ScanContext(seam_skip=frozenset({"BAD"}))
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
