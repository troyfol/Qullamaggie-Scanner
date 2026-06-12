"""Tests for F4 — cross-source EPS disagreement flagging (report-only).

Covers earnings_history.find_cross_source_disagreements (both tolerance
axes, strict-> boundaries, NaN handling, single-source slots) and the
report wiring in save_earnings_history's canonical-dedup path (atomic
CSV overwrite, loud log when > 0 / silent when 0, dedup outcome
unchanged).
"""
from __future__ import annotations

import logging

import pandas as pd
import pytest

from trade_scanner_fh import config
from trade_scanner_fh import earnings_history as eh


def _row(
    ticker: str, period: str, source: str,
    *,
    eps=None, surprise=None, rev=105.0, updated="2026-06-01",
) -> dict:
    """One schema-shaped history row. EPS / surprise default to None so
    each test states exactly which values participate."""
    return {
        "ticker": ticker,
        "period_ending": pd.Timestamp(period),
        "report_date": pd.Timestamp(period) + pd.Timedelta(days=30),
        "report_time": "Close",
        "estimated_eps": None,
        "reported_eps": eps,
        "surprise_eps": None,
        "surprise_eps_pct": surprise,
        "estimated_rev": 100.0,
        "reported_rev": rev,
        "surprise_rev": None,
        "surprise_rev_pct": None,
        "source": source,
        "updated_at": pd.Timestamp(updated),
        "report_date_proxy": False,
    }


# ----------------------------------------------------------------------
# find_cross_source_disagreements — detection
# ----------------------------------------------------------------------

def test_eps_axis_disagreement_detected():
    """Two sources, same slot, |Δeps| = 0.50 > 0.10 default → flagged.
    source_a is the dedup-priority winner (finviz) regardless of row order."""
    df = pd.DataFrame([
        _row("AAPL", "2026-03-01", "zacks", eps=1.50),
        _row("AAPL", "2026-03-01", "finviz", eps=1.00),
    ])
    rep = eh.find_cross_source_disagreements(df)
    assert list(rep.columns) == eh.DISAGREEMENT_COLUMNS
    assert len(rep) == 1
    r = rep.iloc[0]
    assert r["ticker"] == "AAPL"
    assert r["period_ending"] == pd.Timestamp("2026-03-01")
    assert r["source_a"] == "finviz" and r["source_b"] == "zacks"
    assert r["eps_a"] == pytest.approx(1.00)
    assert r["eps_b"] == pytest.approx(1.50)
    assert r["delta_eps"] == pytest.approx(0.50)
    # No surprise values on either side → surprise delta is NaN.
    assert pd.isna(r["surprise_a"]) and pd.isna(r["surprise_b"])
    assert pd.isna(r["delta_surprise_pp"])


def test_surprise_axis_disagreement_detected():
    """EPS agrees exactly but surprise % differs by 7pp > 2.0 default →
    flagged on the surprise axis alone."""
    df = pd.DataFrame([
        _row("MSFT", "2026-03-01", "finviz", eps=2.00, surprise=5.0),
        _row("MSFT", "2026-03-01", "finnhub", eps=2.00, surprise=12.0),
    ])
    rep = eh.find_cross_source_disagreements(df)
    assert len(rep) == 1
    r = rep.iloc[0]
    assert r["source_a"] == "finviz" and r["source_b"] == "finnhub"
    assert r["delta_eps"] == pytest.approx(0.0)
    assert r["surprise_a"] == pytest.approx(5.0)
    assert r["surprise_b"] == pytest.approx(12.0)
    assert r["delta_surprise_pp"] == pytest.approx(7.0)


def test_below_tolerance_is_silent():
    """Deltas under both tolerances (Δeps 0.05 ≤ 0.10, Δpp 1.0 ≤ 2.0)
    → empty report."""
    df = pd.DataFrame([
        _row("NVDA", "2026-03-01", "finviz", eps=1.00, surprise=5.0),
        _row("NVDA", "2026-03-01", "zacks", eps=1.05, surprise=6.0),
    ])
    rep = eh.find_cross_source_disagreements(df)
    assert rep.empty
    assert list(rep.columns) == eh.DISAGREEMENT_COLUMNS


def test_exactly_at_tolerance_not_flagged_strict_greater():
    """Strict > on both axes: deltas exactly equal to the tolerance pass.
    Binary-exact tolerances passed explicitly to dodge float fuzz."""
    df = pd.DataFrame([
        _row("T", "2026-03-01", "finviz", eps=1.00, surprise=4.0),
        _row("T", "2026-03-01", "zacks", eps=1.50, surprise=8.0),
    ])
    # Δeps = 0.5 == tol, Δpp = 4.0 == tol → both at-tolerance → silent.
    rep = eh.find_cross_source_disagreements(
        df, eps_abs_tol=0.5, surprise_pp_tol=4.0)
    assert rep.empty
    # Nudge either tolerance below the delta → flagged.
    assert len(eh.find_cross_source_disagreements(
        df, eps_abs_tol=0.25, surprise_pp_tol=4.0)) == 1
    assert len(eh.find_cross_source_disagreements(
        df, eps_abs_tol=0.5, surprise_pp_tol=3.0)) == 1


def test_documented_default_tolerances():
    """The config constants are the documented defaults."""
    assert config.EPS_DISAGREEMENT_ABS_TOL == 0.10
    assert config.SURPRISE_DISAGREEMENT_PP_TOL == 2.0


def test_single_source_slots_ignored():
    """Slots covered by only one source never flag — including a
    same-source duplicate pair with wildly different values, and
    different sources covering DIFFERENT slots (gap-fill)."""
    df = pd.DataFrame([
        # Same slot, same source, big delta → same-source: ignored.
        _row("A", "2026-03-01", "zacks", eps=1.00, updated="2026-05-01"),
        _row("A", "2026-03-01", "zacks", eps=9.00, updated="2026-06-01"),
        # Cross-source but different slots → no pair.
        _row("B", "2026-03-01", "finviz", eps=1.00),
        _row("B", "2025-12-01", "finnhub", eps=9.00),
    ])
    rep = eh.find_cross_source_disagreements(df)
    assert rep.empty


def test_same_source_dups_collapse_to_most_recent_before_pairing():
    """A stale same-source duplicate must not fabricate a disagreement:
    zacks' older 9.00 copy is superseded by its 1.00 rewrite, which
    agrees with finviz → silent."""
    df = pd.DataFrame([
        _row("C", "2026-03-01", "zacks", eps=9.00, updated="2026-05-01"),
        _row("C", "2026-03-01", "zacks", eps=1.00, updated="2026-06-01"),
        _row("C", "2026-03-01", "finviz", eps=1.00),
    ])
    assert eh.find_cross_source_disagreements(df).empty


def test_nan_handling():
    """A null on one side disables that axis (both-non-null required);
    the other axis can still flag the pair, and the disabled axis's
    delta is NaN in the output."""
    df = pd.DataFrame([
        # EPS null on one side, surprise differs by 10pp → flagged via
        # surprise; delta_eps NaN.
        _row("D", "2026-03-01", "finviz", eps=None, surprise=2.0),
        _row("D", "2026-03-01", "zacks", eps=1.00, surprise=12.0),
        # EPS null on one side AND surprise null on one side → silent.
        _row("E", "2026-03-01", "finviz", eps=None, surprise=5.0),
        _row("E", "2026-03-01", "zacks", eps=1.00, surprise=None),
        # Everything null on both sides → silent.
        _row("F", "2026-03-01", "finviz"),
        _row("F", "2026-03-01", "zacks"),
    ])
    rep = eh.find_cross_source_disagreements(df)
    assert list(rep["ticker"]) == ["D"]
    r = rep.iloc[0]
    assert pd.isna(r["delta_eps"])
    assert pd.isna(r["eps_a"]) and r["eps_b"] == pytest.approx(1.00)
    assert r["delta_surprise_pp"] == pytest.approx(10.0)


def test_three_source_slot_yields_all_cross_pairs():
    """finviz/zacks/finnhub all disagreeing in one slot → 3 pairwise
    rows, each ordered winner-first by dedup priority."""
    df = pd.DataFrame([
        _row("G", "2026-03-01", "finnhub", eps=3.00),
        _row("G", "2026-03-01", "zacks", eps=2.00),
        _row("G", "2026-03-01", "finviz", eps=1.00),
    ])
    rep = eh.find_cross_source_disagreements(df)
    assert len(rep) == 3
    pairs = set(zip(rep["source_a"], rep["source_b"]))
    assert pairs == {("finviz", "zacks"), ("finviz", "finnhub"),
                     ("zacks", "finnhub")}


def test_empty_and_none_inputs():
    for inp in (None, pd.DataFrame(), pd.DataFrame(columns=eh.COLUMNS)):
        rep = eh.find_cross_source_disagreements(inp)
        assert rep.empty
        assert list(rep.columns) == eh.DISAGREEMENT_COLUMNS


def test_input_frame_not_mutated():
    df = pd.DataFrame([
        _row("H", "2026-03-01", "finviz", eps=1.00),
        _row("H", "2026-03-01", "zacks", eps=2.00),
    ])
    snapshot = df.copy(deep=True)
    eh.find_cross_source_disagreements(df)
    pd.testing.assert_frame_equal(df, snapshot)


# ----------------------------------------------------------------------
# Wiring — save_earnings_history's canonical (dedup=True) path
# ----------------------------------------------------------------------

def _csv_path(tmp_parquets):
    return tmp_parquets / config.EARNINGS_DISAGREEMENTS_CSV_NAME


def test_csv_written_atomically_on_canonical_save(tmp_parquets):
    """A dedup=True save with a cross-source disagreement writes the CSV
    (via atomic_write_csv) and leaves no temp residue."""
    eh.save_earnings_history(pd.DataFrame([
        _row("AAPL", "2026-03-01", "finviz", eps=1.00),
        _row("AAPL", "2026-03-01", "zacks", eps=2.00),
    ]))
    csv = _csv_path(tmp_parquets)
    assert csv.exists()
    rep = pd.read_csv(csv)
    assert len(rep) == 1
    assert list(rep.columns) == eh.DISAGREEMENT_COLUMNS
    assert rep.iloc[0]["source_a"] == "finviz"
    assert rep.iloc[0]["delta_eps"] == pytest.approx(1.00)
    # No .tmp residue anywhere in the data dir (atomic write completed).
    assert not list(tmp_parquets.glob("*.tmp"))


def test_csv_overwritten_each_run_clean_scan_empties_it(tmp_parquets):
    """The CSV reflects the CURRENT scan: a later clean canonical save
    overwrites a previously-populated report with an empty one."""
    eh.save_earnings_history(pd.DataFrame([
        _row("AAPL", "2026-03-01", "finviz", eps=1.00),
        _row("AAPL", "2026-03-01", "zacks", eps=2.00),
    ]))
    assert len(pd.read_csv(_csv_path(tmp_parquets))) == 1
    eh.save_earnings_history(pd.DataFrame([
        _row("MSFT", "2026-03-01", "finviz", eps=3.00),
    ]))
    rep = pd.read_csv(_csv_path(tmp_parquets))
    assert rep.empty
    assert list(rep.columns) == eh.DISAGREEMENT_COLUMNS


def test_per_flush_save_skips_report(tmp_parquets):
    """dedup=False (per-flush) saves never touch the CSV."""
    eh.save_earnings_history(pd.DataFrame([
        _row("AAPL", "2026-03-01", "finviz", eps=1.00),
        _row("AAPL", "2026-03-01", "zacks", eps=2.00),
    ]), sort=False, dedup=False)
    assert not _csv_path(tmp_parquets).exists()


def test_loud_log_when_found_silent_when_clean(tmp_parquets, caplog):
    with caplog.at_level(logging.WARNING, logger="scanner.earnings_history"):
        eh.save_earnings_history(pd.DataFrame([
            _row("AAPL", "2026-03-01", "finviz", eps=1.00),
            _row("AAPL", "2026-03-01", "zacks", eps=2.00),
        ]))
    assert any(
        "cross-source EPS disagreements — see earnings_disagreements.csv"
        in r.getMessage() and r.getMessage().startswith("1 ")
        for r in caplog.records
    )
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="scanner.earnings_history"):
        eh.save_earnings_history(pd.DataFrame([
            _row("MSFT", "2026-03-01", "finviz", eps=3.00),
        ]))
    assert not any(
        "cross-source EPS disagreements" in r.getMessage()
        for r in caplog.records
    )


def test_dedup_outcome_unchanged_by_report(tmp_parquets):
    """Report-only invariant: the on-disk winner after a canonical save
    with a flagged disagreement is exactly what dedupe_history alone
    would have kept (priority winner, values untouched)."""
    rows = [
        _row("AAPL", "2026-03-01", "finviz", eps=1.00, surprise=5.0),
        _row("AAPL", "2026-03-01", "zacks", eps=2.00, surprise=15.0),
    ]
    expected = eh.dedupe_history(pd.DataFrame(rows))
    assert len(expected) == 1
    assert expected.iloc[0]["source"] == "finviz"

    eh.save_earnings_history(pd.DataFrame(rows))
    on_disk = eh.load_earnings_history()
    assert len(on_disk) == 1
    got = on_disk.iloc[0]
    assert got["source"] == "finviz"
    assert got["reported_eps"] == pytest.approx(1.00)
    assert got["surprise_eps_pct"] == pytest.approx(5.0)


def test_csv_write_failure_does_not_block_parquet_save(tmp_parquets, monkeypatch):
    """A locked/failing CSV target (e.g. open in Excel) must never abort
    the history save itself."""
    def _boom(df, path, **kwargs):
        raise OSError("locked")
    monkeypatch.setattr(config, "atomic_write_csv", _boom)
    eh.save_earnings_history(pd.DataFrame([
        _row("AAPL", "2026-03-01", "finviz", eps=1.00),
        _row("AAPL", "2026-03-01", "zacks", eps=2.00),
    ]))
    on_disk = eh.load_earnings_history()
    assert on_disk is not None and len(on_disk) == 1


def test_report_function_returns_frame(tmp_parquets):
    """report_cross_source_disagreements returns the scan result and
    writes the same rows to the CSV."""
    df = pd.DataFrame([
        _row("AAPL", "2026-03-01", "finviz", eps=1.00),
        _row("AAPL", "2026-03-01", "zacks", eps=2.00),
        _row("MSFT", "2026-03-01", "finviz", eps=3.00),
    ])
    rep = eh.report_cross_source_disagreements(df)
    assert len(rep) == 1
    csv_rep = pd.read_csv(_csv_path(tmp_parquets))
    assert len(csv_rep) == 1
    assert csv_rep.iloc[0]["ticker"] == "AAPL"
