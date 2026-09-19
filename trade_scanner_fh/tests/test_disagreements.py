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


def test_resolved_disagreement_clears_the_csv(tmp_parquets):
    """Self-clearing is preserved for the case it exists to cover: a later
    save that DOES have cross-source slots to compare, and finds them
    consistent, empties a previously-populated report."""
    eh.save_earnings_history(pd.DataFrame([
        _row("AAPL", "2026-03-01", "finviz", eps=1.00),
        _row("AAPL", "2026-03-01", "zacks", eps=2.00),
    ]))
    assert len(pd.read_csv(_csv_path(tmp_parquets))) == 1

    # Same slot, still two sources — but now they agree.
    eh.save_earnings_history(pd.DataFrame([
        _row("AAPL", "2026-03-01", "finviz", eps=1.00),
        _row("AAPL", "2026-03-01", "zacks", eps=1.00),
    ]))
    rep = pd.read_csv(_csv_path(tmp_parquets))
    assert rep.empty
    assert list(rep.columns) == eh.DISAGREEMENT_COLUMNS


def test_save_with_nothing_to_compare_leaves_the_csv_intact(tmp_parquets):
    """The 2026-08-13 regression, in miniature.

    `report_cross_source_disagreements` runs on every canonical save, right
    BEFORE `dedupe_history` collapses each slot to a single source. So the
    save that finds a disagreement also destroys the evidence, and the NEXT
    canonical save — reading an already-deduped frame — has zero comparable
    slots. Treating that as "clean" wiped the report: 701 real findings
    survived two minutes on the live store before the next fill finalized.

    A frame with nothing to compare yields no information, so it must not
    overwrite a report that does.
    """
    eh.save_earnings_history(pd.DataFrame([
        _row("AAPL", "2026-03-01", "finviz", eps=1.00),
        _row("AAPL", "2026-03-01", "zacks", eps=2.00),
    ]))
    assert len(pd.read_csv(_csv_path(tmp_parquets))) == 1

    # A single-source frame: nothing here can be compared to anything.
    eh.save_earnings_history(pd.DataFrame([
        _row("MSFT", "2026-03-01", "finviz", eps=3.00),
    ]))

    rep = pd.read_csv(_csv_path(tmp_parquets))
    assert len(rep) == 1, "a no-information save wiped the report"
    assert rep.iloc[0]["ticker"] == "AAPL"


def test_reload_and_resave_of_a_deduped_store_preserves_the_report(tmp_parquets):
    """End-to-end shape of the live failure: fill -> canonical save (finds
    the disagreement, dedups the store) -> a later save driven by whatever
    is now on disk. The second save must not erase the first's findings."""
    eh.save_earnings_history(pd.DataFrame([
        _row("AAPL", "2026-03-01", "finviz", eps=1.00),
        _row("AAPL", "2026-03-01", "zacks", eps=2.00),
    ]))
    assert len(pd.read_csv(_csv_path(tmp_parquets))) == 1

    on_disk = eh.load_earnings_history()
    assert len(on_disk) == 1, "dedup should have collapsed the slot"
    eh.save_earnings_history(on_disk)

    assert len(pd.read_csv(_csv_path(tmp_parquets))) == 1


def test_comparable_slot_count_distinguishes_no_data_from_clean():
    """The gate itself: multi-source slots are comparable, single-source
    slots (however many) are not."""
    assert eh._comparable_slot_count(None) == 0
    assert eh._comparable_slot_count(pd.DataFrame()) == 0
    assert eh._comparable_slot_count(pd.DataFrame([
        _row("AAPL", "2026-03-01", "finviz", eps=1.0),
        _row("MSFT", "2026-03-01", "finviz", eps=2.0),
        _row("AAPL", "2025-12-01", "zacks", eps=3.0),
    ])) == 0
    assert eh._comparable_slot_count(pd.DataFrame([
        _row("AAPL", "2026-03-01", "finviz", eps=1.0),
        _row("AAPL", "2026-03-01", "zacks", eps=2.0),
    ])) == 1
    # same source twice in a slot is NOT comparable
    assert eh._comparable_slot_count(pd.DataFrame([
        _row("AAPL", "2026-03-01", "finviz", eps=1.0, updated="2026-06-01"),
        _row("AAPL", "2026-03-01", "finviz", eps=2.0, updated="2026-06-02"),
    ])) == 0


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
        "cross-source EPS disagreements" in r.getMessage()
        and "earnings_disagreements.csv" in r.getMessage()
        and r.getMessage().startswith("1 ")
        # the comparable-slot count says how much was actually evaluated,
        # which is what distinguishes "clean" from "nothing to compare"
        and "1 comparable slot(s)" in r.getMessage()
        for r in caplog.records
    )
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="scanner.earnings_history"):
        eh.save_earnings_history(pd.DataFrame([
            _row("MSFT", "2026-03-01", "finviz", eps=3.00),
            _row("MSFT", "2026-03-01", "zacks", eps=3.00),
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


# ── 2026-09-18: concurrent finalize blanked the report ──────────────────

def test_a_later_narrow_save_cannot_blank_another_sources_findings(tmp_parquets):
    """The 2026-09-18 live failure, in miniature.

    The smart refresh runs finviz + zacks CONCURRENTLY, so each finalizes on
    its own. Zacks finalized first and wrote 221 findings. Finviz finalized
    three minutes later on the already-deduped store plus its own freshly
    fetched rows — which re-created a FEW comparable slots, none of them
    contested. That was enough to clear the "nothing comparable" guard, so the
    scan result (empty) was written over the whole file. All 221 were gone.

    The later save must speak only for the slots it actually compared.
    """
    # Zacks finalizes: AAPL is contested, and the save dedups the store.
    eh.save_earnings_history(pd.DataFrame([
        _row("AAPL", "2026-03-01", "finviz", eps=1.00),
        _row("AAPL", "2026-03-01", "zacks", eps=2.00),
    ]))
    assert len(pd.read_csv(_csv_path(tmp_parquets))) == 1

    # Finviz finalizes: the deduped store, plus its own new rows for a
    # DIFFERENT ticker that agree with each other. Comparable > 0, contested
    # == 0 — precisely the shape that used to blank the file.
    on_disk = eh.load_earnings_history()
    eh.save_earnings_history(pd.concat([on_disk, pd.DataFrame([
        _row("MSFT", "2026-03-01", "finviz", eps=3.00),
        _row("MSFT", "2026-03-01", "zacks", eps=3.00),
    ])], ignore_index=True))

    rep = pd.read_csv(_csv_path(tmp_parquets))
    assert len(rep) == 1, "a later narrow save blanked the standing report"
    assert rep.iloc[0]["ticker"] == "AAPL"


def test_a_save_that_re_examines_a_slot_may_clear_it(tmp_parquets):
    """The other half: scoping must not make findings immortal. A save that
    genuinely re-compared the contested slot and found it clean still clears
    that row — the self-clearing 'previously reported, now resolved' signal."""
    eh.save_earnings_history(pd.DataFrame([
        _row("AAPL", "2026-03-01", "finviz", eps=1.00),
        _row("AAPL", "2026-03-01", "zacks", eps=2.00),
    ]))
    assert len(pd.read_csv(_csv_path(tmp_parquets))) == 1

    # Same slot, both sources, now agreeing.
    eh.save_earnings_history(pd.DataFrame([
        _row("AAPL", "2026-03-01", "finviz", eps=2.00, updated="2026-07-01"),
        _row("AAPL", "2026-03-01", "zacks", eps=2.00, updated="2026-07-01"),
    ]))
    assert pd.read_csv(_csv_path(tmp_parquets)).empty, (
        "a re-examined slot that is now clean should clear"
    )


def test_findings_from_two_sources_accumulate(tmp_parquets):
    """Two independent finalizes, two different contested tickers — the
    report must end up holding BOTH, which the old last-writer-wins
    behaviour could never do."""
    eh.save_earnings_history(pd.DataFrame([
        _row("AAPL", "2026-03-01", "finviz", eps=1.00),
        _row("AAPL", "2026-03-01", "zacks", eps=2.00),
    ]))
    eh.save_earnings_history(pd.DataFrame([
        _row("TSLA", "2026-03-01", "finviz", eps=5.00),
        _row("TSLA", "2026-03-01", "zacks", eps=9.00),
    ]))
    rep = pd.read_csv(_csv_path(tmp_parquets))
    assert sorted(rep["ticker"]) == ["AAPL", "TSLA"]


def test_slot_key_matches_across_the_csv_round_trip():
    """The merge compares a freshly-scanned slot (Timestamp period_ending)
    against one read back off disk (string). If those keys don't match, every
    prior row looks un-evaluated and the report grows without bound."""
    fresh = eh._slot_key(pd.Series(["aapl"]), pd.Series([pd.Timestamp("2026-03-01")]))
    from_csv = eh._slot_key(pd.Series(["AAPL"]), pd.Series(["2026-03-01"]))
    assert fresh.iloc[0] == from_csv.iloc[0] == "AAPL|2026-03-01"


# ── 2026-09-19: the gate is absolute AND relative ──────────────────────

def test_large_absolute_but_small_relative_is_not_flagged():
    """The inversion the old gate had. A $0.22 gap on a $3.40 EPS is routine
    vendor variance; an absolute-only threshold called it a disagreement.
    Measured on the live store, 32% of findings were of exactly this shape
    (median |EPS| $3.37)."""
    df = pd.DataFrame([
        _row("BIG", "2026-03-01", "finviz", eps=3.40),
        _row("BIG", "2026-03-01", "zacks", eps=3.18),
    ])
    assert eh.find_cross_source_disagreements(df).empty
    # ...and the old absolute-only behaviour is still reachable.
    assert len(eh.find_cross_source_disagreements(df, eps_rel_tol=0.0)) == 1


def test_large_relative_but_small_absolute_is_still_not_flagged():
    """The absolute floor has to survive too, or a one-cent difference on a
    two-cent EPS becomes a 50% 'disagreement'."""
    df = pd.DataFrame([
        _row("TINY", "2026-03-01", "finviz", eps=0.02),
        _row("TINY", "2026-03-01", "zacks", eps=0.01),
    ])
    assert eh.find_cross_source_disagreements(df).empty


def test_both_gates_cleared_is_flagged():
    df = pd.DataFrame([
        _row("REAL", "2026-03-01", "finviz", eps=1.00),
        _row("REAL", "2026-03-01", "zacks", eps=0.10),
    ])
    rep = eh.find_cross_source_disagreements(df)
    assert len(rep) == 1
    assert rep.iloc[0]["rel_eps"] == pytest.approx(0.90)


def test_relative_gate_is_symmetric_in_a_and_b():
    """Measured against the LARGER magnitude, so swapping the sources cannot
    change the verdict and a near-zero denominator cannot inflate it."""
    hi = pd.DataFrame([_row("S", "2026-03-01", "finviz", eps=1.00),
                       _row("S", "2026-03-01", "zacks", eps=0.70)])
    lo = pd.DataFrame([_row("S", "2026-03-01", "finviz", eps=0.70),
                       _row("S", "2026-03-01", "zacks", eps=1.00)])
    a = eh.find_cross_source_disagreements(hi)
    b = eh.find_cross_source_disagreements(lo)
    assert len(a) == len(b) == 1
    assert a.iloc[0]["rel_eps"] == pytest.approx(b.iloc[0]["rel_eps"])
    assert a.iloc[0]["rel_eps"] == pytest.approx(0.30)


def test_surprise_axis_is_unaffected_by_the_eps_relative_gate():
    """The surprise figure is already expressed in percentage points, so it
    needs no relative companion — and must not be suppressed by one."""
    df = pd.DataFrame([
        _row("SURP", "2026-03-01", "finviz", eps=3.40, surprise=1.0),
        _row("SURP", "2026-03-01", "zacks", eps=3.40, surprise=40.0),
    ])
    assert len(eh.find_cross_source_disagreements(df)) == 1


# ── the structure score: basis vs noise ────────────────────────────────

def _series(ticker, pairs):
    """pairs = [(period, finviz_eps, zacks_eps)]"""
    rows = []
    for p, a, b in pairs:
        rows.append(_row(ticker, p, "finviz", eps=a))
        rows.append(_row(ticker, p, "zacks", eps=b))
    return pd.DataFrame(rows)


_PERIODS = [f"20{y}-{m}-01" for y in (20, 21, 22) for m in ("03", "06", "09", "12")]


def test_a_constant_multiplier_scores_as_structured():
    """A basis difference is the same multiplier every quarter, so the
    log-ratio is flat and its lag-1 autocorrelation is defined as 0 only when
    there is literally no variance — here we vary the level but hold the
    RATIO fixed, which is the real basis signature."""
    vals = [1.0, 1.4, 0.8, 2.2, 1.1, 1.9, 0.6, 2.5, 1.3, 1.7, 0.9, 2.0]
    df = _series("BASIS", [(p, v * 10.0, v) for p, v in zip(_PERIODS, vals)])
    rep = eh.find_cross_source_disagreements(df)
    assert not rep.empty
    # An exactly-constant ratio has no variance, so the score is decided
    # explicitly rather than read off floating-point noise: 1.0, maximally
    # structured. That is the textbook basis difference.
    assert rep["structure"].dropna().iloc[0] == pytest.approx(1.0)
    assert rep["rel_eps"].iloc[0] == pytest.approx(0.90)


def test_a_constant_ratio_of_one_is_agreement_not_structure():
    """The degenerate twin of the case above: identical values every quarter
    are also zero-variance, but that is the sources agreeing."""
    from trade_scanner_fh.earnings_history import _structure_scores
    sub = pd.DataFrame({
        "ticker": ["SAME"] * 8,
        "period_ending": pd.to_datetime(_PERIODS[:8]),
        "eps_a": [1.0, 1.4, 0.8, 2.2, 1.1, 1.9, 0.6, 2.5],
        "eps_b": [1.0, 1.4, 0.8, 2.2, 1.1, 1.9, 0.6, 2.5],
    })
    assert _structure_scores(sub)["SAME"] == pytest.approx(0.0)


def test_an_alternating_offset_scores_negative_and_noise_scores_low():
    """A source that alternates high/low quarter to quarter is the opposite of
    a basis difference, and the score says so with a negative value."""
    alt = [(p, 1.0 if i % 2 else 0.2, 0.2 if i % 2 else 1.0)
           for i, p in enumerate(_PERIODS)]
    rep = eh.find_cross_source_disagreements(_series("ALT", alt))
    score = rep["structure"].dropna().iloc[0]
    assert score < 0, f"alternating offset should score negative, got {score}"


def test_structure_is_nan_below_the_minimum_overlap():
    short = [(p, 1.0, 0.1) for p in _PERIODS[:3]]
    rep = eh.find_cross_source_disagreements(_series("SHORT", short))
    assert not rep.empty
    assert rep["structure"].isna().all()


def test_structure_is_scored_over_all_overlap_not_just_flagged_rows():
    """Scoring only the flagged subset would measure a selected sample — the
    exact error that made the first pass at this look like a basis story."""
    pairs = [(p, 1.00, 0.99) for p in _PERIODS[:10]]      # agree: never flagged
    pairs += [(p, 1.00, 0.10) for p in _PERIODS[10:]]     # flagged
    rep = eh.find_cross_source_disagreements(_series("MIX", pairs))
    assert len(rep) == 2, "only the two contested quarters should be flagged"
    assert rep["structure"].notna().all(), (
        "the score must still be computed, using all 12 overlapping quarters"
    )


def test_structure_column_is_in_the_csv_schema(tmp_parquets):
    eh.save_earnings_history(pd.DataFrame([
        _row("AAPL", "2026-03-01", "finviz", eps=1.00),
        _row("AAPL", "2026-03-01", "zacks", eps=0.10),
    ]))
    cols = list(pd.read_csv(_csv_path(tmp_parquets)).columns)
    assert cols == eh.DISAGREEMENT_COLUMNS
    assert "rel_eps" in cols and "structure" in cols
