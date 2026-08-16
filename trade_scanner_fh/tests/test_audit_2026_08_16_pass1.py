"""Audit 2026-08-16 — Pass 1 remediation regression tests.

Covers F1 (short-response truncation, CRITICAL), F5 (re-sent bar volume
regression), F8 (orphaned atomic-write temps), F9 (backup spacing) and the
Zacks merge-key change.

F1 is the one that matters: both flush paths dropped EVERY ``(ticker, source)``
row and wrote whatever the current response returned, so a 200-OK-but-SHORT
response permanently truncated that ticker's history — silently, per ticker, on
every run. The INT-1 guard covered a DIFFERENT failure (an unreadable store),
which is why ``test_store_truncation_guards.py`` passed throughout.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import pandas as pd
import pytest

from trade_scanner_fh import config, data_engine
from trade_scanner_fh import earnings_history as eh
from trade_scanner_fh import fill_framework as ff
from trade_scanner_fh import zacks_scraper as zs


# ── helpers ───────────────────────────────────────────────────────────

def _hrow(ticker, period, report, source="finviz", eps=1.0):  # noqa: D103
    return {
        "ticker": ticker,
        "period_ending": pd.Timestamp(period),
        "report_date": pd.Timestamp(report),
        "report_time": "Close",
        "estimated_eps": eps, "reported_eps": eps,
        "surprise_eps": 0.0, "surprise_eps_pct": 0.0,
        "estimated_rev": None, "reported_rev": None,
        "surprise_rev": None, "surprise_rev_pct": None,
        "source": source,
        "updated_at": pd.Timestamp("2026-08-16"),
        "report_date_proxy": False,
    }


def _quarters(ticker, periods, source="finviz"):
    """One row per period; report_date is ~2 months after the period end."""
    out = []
    for p in periods:
        ts = pd.Timestamp(p)
        out.append(_hrow(ticker, ts, ts + pd.DateOffset(months=2), source))
    return out


# ══════════════════════════════════════════════════════════════════════
# F1 — rows_superseded_by: the span rule
# ══════════════════════════════════════════════════════════════════════

def test_short_response_supersedes_only_the_span_it_covers():
    """The truncation. A response carrying just the newest quarter must not
    delete the older ones it never mentioned."""
    existing = pd.DataFrame(_quarters(
        "AAPL", ["2024-03-01", "2024-06-01", "2024-09-01", "2024-12-01"]))
    new_df = pd.DataFrame(_quarters("AAPL", ["2024-12-01"]))

    mask = eh.rows_superseded_by(existing, new_df, "finviz")

    kept = existing.loc[~mask, "period_ending"]
    assert set(kept) == {
        pd.Timestamp("2024-03-01"),
        pd.Timestamp("2024-06-01"),
        pd.Timestamp("2024-09-01"),
    }
    # The quarter the response DID cover is superseded (replaced, not doubled).
    assert mask.sum() == 1


def test_full_response_supersedes_everything_so_restatements_still_apply():
    """The other half of the rule, and the reason a naive merge-by-key would be
    wrong: a response that reaches at least as far back as the store IS
    authoritative, so a WITHDRAWN quarter is still removed."""
    existing = pd.DataFrame(_quarters(
        "AAPL", ["2024-03-01", "2024-06-01", "2024-09-01"]))
    # Same span, but 2024-06-01 has been withdrawn upstream.
    new_df = pd.DataFrame(_quarters("AAPL", ["2024-03-01", "2024-09-01"]))

    mask = eh.rows_superseded_by(existing, new_df, "finviz")

    assert mask.all(), "a full-span response must be able to drop a quarter"


def test_response_reaching_further_back_supersedes_everything():
    existing = pd.DataFrame(_quarters("AAPL", ["2024-09-01"]))
    new_df = pd.DataFrame(_quarters(
        "AAPL", ["2024-03-01", "2024-06-01", "2024-09-01"]))
    assert eh.rows_superseded_by(existing, new_df, "finviz").all()


def test_other_sources_and_other_tickers_are_never_touched():
    existing = pd.DataFrame(
        _quarters("AAPL", ["2024-03-01", "2024-12-01"], source="finviz")
        + _quarters("AAPL", ["2024-03-01"], source="zacks")
        + _quarters("MSFT", ["2024-03-01"], source="finviz")
    )
    new_df = pd.DataFrame(_quarters("AAPL", ["2024-12-01"]))

    mask = eh.rows_superseded_by(existing, new_df, "finviz")

    superseded = existing.loc[mask]
    assert len(superseded) == 1
    assert superseded.iloc[0]["ticker"] == "AAPL"
    assert superseded.iloc[0]["source"] == "finviz"
    assert superseded.iloc[0]["period_ending"] == pd.Timestamp("2024-12-01")


def test_per_ticker_spans_are_independent():
    """One ticker's short response must not change what another's does."""
    existing = pd.DataFrame(
        _quarters("AAA", ["2024-03-01", "2024-12-01"])
        + _quarters("BBB", ["2024-03-01", "2024-12-01"])
    )
    new_df = pd.DataFrame(
        _quarters("AAA", ["2024-12-01"])              # short
        + _quarters("BBB", ["2024-03-01", "2024-12-01"])  # full
    )

    mask = eh.rows_superseded_by(existing, new_df, "finviz")
    kept = existing.loc[~mask]

    assert list(kept["ticker"]) == ["AAA"]
    assert kept.iloc[0]["period_ending"] == pd.Timestamp("2024-03-01")


def test_unparseable_response_periods_supersede_only_exact_matches():
    """A response whose period_ending is entirely unusable must not be treated
    as covering an open-ended span — it can only replace rows it names."""
    existing = pd.DataFrame(_quarters("AAPL", ["2024-03-01", "2024-12-01"]))
    new_df = pd.DataFrame(_quarters("AAPL", ["2024-12-01"]))
    new_df["period_ending"] = pd.NaT

    mask = eh.rows_superseded_by(existing, new_df, "finviz")

    assert not mask.any(), "a period-less response must delete nothing"


def test_missing_period_column_falls_back_to_the_historical_behaviour():
    existing = pd.DataFrame(_quarters("AAPL", ["2024-03-01", "2024-12-01"]))
    new_df = pd.DataFrame(_quarters("AAPL", ["2024-12-01"])).drop(
        columns=["period_ending"])
    assert eh.rows_superseded_by(existing, new_df, "finviz").all()


def test_superseded_helper_is_inert_on_empty_inputs():
    existing = pd.DataFrame(_quarters("AAPL", ["2024-12-01"]))
    assert not eh.rows_superseded_by(existing, None, "finviz").any()
    assert not eh.rows_superseded_by(
        existing, pd.DataFrame(columns=eh.COLUMNS), "finviz").any()
    assert not eh.rows_superseded_by(None, existing, "finviz").any()


def test_retention_is_logged_so_a_shrinking_source_is_visible(caplog):
    """F1 firing is a signal, not a silent success — a source that keeps
    serving less than it used to needs to surface."""
    existing = pd.DataFrame(_quarters("AAPL", ["2024-03-01", "2024-12-01"]))
    new_df = pd.DataFrame(_quarters("AAPL", ["2024-12-01"]))

    import logging
    with caplog.at_level(logging.WARNING):
        eh.rows_superseded_by(existing, new_df, "finviz")

    assert any("covered less history" in r.getMessage() for r in caplog.records)


# ══════════════════════════════════════════════════════════════════════
# F1 — end to end through the shared flush path (finviz / finnhub)
# ══════════════════════════════════════════════════════════════════════

def test_framework_flush_preserves_uncovered_quarters(tmp_parquets):
    eh.save_earnings_history(pd.DataFrame(_quarters(
        "AAPL", ["2023-03-01", "2023-06-01", "2024-12-01"])), sort=True)

    wrote = ff.flush_pending_to_disk(
        {"AAPL": _quarters("AAPL", ["2024-12-01"])}, source="finviz")

    assert wrote is not False
    df = eh.load_earnings_history()
    assert set(df["period_ending"]) == {
        pd.Timestamp("2023-03-01"),
        pd.Timestamp("2023-06-01"),
        pd.Timestamp("2024-12-01"),
    }
    assert len(df) == 3, "the re-covered quarter must not have duplicated"


def test_framework_flush_still_replaces_a_refetched_quarters_values(
        tmp_parquets):
    """Narrowing the deletion scope must not break the update path."""
    eh.save_earnings_history(
        pd.DataFrame([_hrow("AAPL", "2024-12-01", "2025-02-01", eps=1.0)]),
        sort=True)

    ff.flush_pending_to_disk(
        {"AAPL": [_hrow("AAPL", "2024-12-01", "2025-02-01", eps=2.5)]},
        source="finviz")

    df = eh.load_earnings_history()
    assert len(df) == 1
    assert df.iloc[0]["reported_eps"] == 2.5


# ══════════════════════════════════════════════════════════════════════
# F13 — cross-source metric-group merge + per-group provenance
# ══════════════════════════════════════════════════════════════════════

def _with_rev(row, est, rep, surp, pct):
    row.update({"estimated_rev": est, "reported_rev": rep,
                "surprise_rev": surp, "surprise_rev_pct": pct})
    return row


def test_write_path_merges_revenue_before_deleting_the_donor(tmp_parquets):
    """The whole point of F13. The canonical save is the ONLY moment the loser
    row still exists — the read-side merge could never fire because this save
    had already deleted its inputs."""
    finviz = _with_rev(
        _hrow("AAPL", "2025-12-01", "2026-01-29", "finviz"),
        None, None, None, None)
    zacks = _with_rev(
        _hrow("AAPL", "2025-12-01", "2026-01-29", "zacks"),
        900.0, 1000.0, 100.0, 11.1)

    eh.save_earnings_history(pd.DataFrame([finviz, zacks]), sort=True)

    df = eh.load_earnings_history()
    assert len(df) == 1, "the slot must still collapse to one row"
    row = df.iloc[0]
    assert row["source"] == "finviz"
    assert row["eps_source"] == "finviz"
    # The reported actual crosses over from zacks...
    assert row["rev_source"] == "zacks"
    assert row["reported_rev"] == 1000.0
    # ...but zacks' estimate-derived figures do not (F14).
    assert pd.isna(row["estimated_rev"])
    assert pd.isna(row["surprise_rev"])
    assert pd.isna(row["surprise_rev_pct"])


def test_non_finviz_estimates_never_reach_the_parquet(tmp_parquets):
    """F14, at the choke point. A zacks-only ticker keeps its reported values
    and loses its estimate cluster — including on a NON-canonical per-flush
    save, so nothing can slip through between finalizes."""
    zacks = _with_rev(
        _hrow("AAPL", "2025-12-01", "2026-01-29", "zacks"),
        900.0, 1000.0, 100.0, 11.1)
    zacks.update({"estimated_eps": 1.9, "surprise_eps": 0.2,
                  "surprise_eps_pct": 10.5})

    eh.save_earnings_history(pd.DataFrame([zacks]), sort=False, dedup=False)

    row = eh.load_earnings_history().iloc[0]
    assert row["reported_eps"] == 1.0        # reported survives
    assert row["reported_rev"] == 1000.0
    assert row["eps_source"] == "zacks"
    assert row["rev_source"] == "zacks"
    for col in ("estimated_eps", "surprise_eps", "surprise_eps_pct",
                "estimated_rev", "surprise_rev", "surprise_rev_pct"):
        assert pd.isna(row[col]), f"{col} reached the parquet from zacks"


def test_finviz_estimates_survive_the_strip(tmp_parquets):
    finviz = _with_rev(
        _hrow("AAPL", "2025-12-01", "2026-01-29", "finviz"),
        900.0, 1000.0, 100.0, 11.1)
    finviz.update({"estimated_eps": 1.9, "surprise_eps": 0.2,
                   "surprise_eps_pct": 10.5})

    eh.save_earnings_history(pd.DataFrame([finviz]), sort=True)

    row = eh.load_earnings_history().iloc[0]
    assert row["estimated_eps"] == 1.9
    assert row["surprise_eps_pct"] == 10.5
    assert row["estimated_rev"] == 900.0
    assert row["surprise_rev_pct"] == 11.1


def test_reported_eps_and_rev_can_come_from_different_sources(tmp_parquets):
    """Free mixing of the reported actuals, tracked per column."""
    finviz = _with_rev(_hrow("AAPL", "2025-12-01", "2026-01-29", "finviz"),
                       None, None, None, None)
    finviz["reported_eps"] = None
    zacks = _with_rev(_hrow("AAPL", "2025-12-01", "2026-01-29", "zacks"),
                      None, None, None, None)
    zacks["reported_eps"] = 2.10
    finnhub = _with_rev(_hrow("AAPL", "2025-12-01", "2026-01-29", "finnhub"),
                        None, 1000.0, None, None)
    finnhub["reported_eps"] = None

    eh.save_earnings_history(
        pd.DataFrame([finviz, zacks, finnhub]), sort=True)

    row = eh.load_earnings_history().iloc[0]
    assert row["source"] == "finviz"
    assert row["reported_eps"] == 2.10 and row["eps_source"] == "zacks"
    assert row["reported_rev"] == 1000.0 and row["rev_source"] == "finnhub"


def test_reported_sources_round_trip_through_the_parquet(tmp_parquets):
    """They are written as `category` (like `source`), which must survive a
    read/modify/write cycle — the dtype that broke the first cut of this.

    The second save is the real test: `rev_source` says "zacks" while the
    row's own `source` says "finviz", so a strip or a re-merge keyed on the
    wrong column would corrupt it on the way back through."""
    finviz = _with_rev(
        _hrow("AAPL", "2025-12-01", "2026-01-29", "finviz"),
        None, None, None, None)
    zacks = _with_rev(
        _hrow("AAPL", "2025-12-01", "2026-01-29", "zacks"),
        900.0, 1000.0, 100.0, 11.1)
    eh.save_earnings_history(pd.DataFrame([finviz, zacks]), sort=True)

    # Re-save what we just read — the cycle a second fill performs.
    again = eh.load_earnings_history()
    eh.save_earnings_history(again, sort=True)
    row = eh.load_earnings_history().iloc[0]

    assert row["rev_source"] == "zacks"
    assert row["eps_source"] == "finviz"
    assert row["reported_rev"] == 1000.0


def test_legacy_rows_without_the_columns_default_to_their_own_source(
        tmp_parquets):
    """A pre-F13 row never took part in a merge, so its reported values came
    from its own source. A column with no value gets NA, not a source."""
    row_in = _hrow("AAPL", "2025-12-01", "2026-01-29", "zacks")   # EPS only
    df = pd.DataFrame([row_in])
    df = df.drop(columns=[c for c in ("eps_source", "rev_source")
                          if c in df.columns])
    eh.save_earnings_history(df, sort=True)

    row = eh.load_earnings_history().iloc[0]
    assert row["eps_source"] == "zacks"
    assert pd.isna(row["rev_source"]), "no revenue means no revenue source"


def test_a_fill_flush_populates_the_reported_sources(tmp_parquets):
    """Fill rows are built without the columns; the save path fills them in."""
    ff.flush_pending_to_disk(
        {"AAPL": [_with_rev(
            _hrow("AAPL", "2025-12-01", "2026-01-29", "finviz"),
            900.0, 1000.0, 100.0, 11.1)]},
        source="finviz", is_final=True)
    row = eh.load_earnings_history().iloc[0]
    assert row["eps_source"] == "finviz"
    assert row["rev_source"] == "finviz"


def test_merge_does_not_resurrect_a_deleted_quarter(tmp_parquets):
    """Cross-source filling must not reintroduce the loser as its own row."""
    rows = [
        _with_rev(_hrow("AAPL", "2025-12-01", "2026-01-29", "finviz"),
                  None, None, None, None),
        _with_rev(_hrow("AAPL", "2025-12-01", "2026-01-29", "zacks"),
                  900.0, 1000.0, 100.0, 11.1),
        _with_rev(_hrow("AAPL", "2025-09-01", "2025-10-29", "zacks"),
                  800.0, 850.0, 50.0, 6.3),
    ]
    eh.save_earnings_history(pd.DataFrame(rows), sort=True)
    df = eh.load_earnings_history()

    assert len(df) == 2
    assert set(df["period_ending"]) == {
        pd.Timestamp("2025-09-01"), pd.Timestamp("2025-12-01")}


# ══════════════════════════════════════════════════════════════════════
# F15 — a fill may only retract what it owns
# ══════════════════════════════════════════════════════════════════════

def test_merged_value_survives_the_next_fill_from_the_winning_source(
        tmp_parquets):
    """The hole F13 opened. `source` is both the row's winning source AND the
    key the flush supersedes on, but a merged row's values can belong to a
    DIFFERENT source. So a source=finviz row carrying zacks revenue was wiped
    by the next finviz fill — and the zacks donor row had already been deleted
    by the merge that created the value.

    Measured before F15: 1000.0 -> NaN on step 3.
    """
    def r(src, eps, rev):
        return _with_rev(
            _hrow("AAPL", "2025-12-01", "2026-01-29", src, eps=eps),
            None, rev, None, None)

    # 1. finviz covers the quarter but has no revenue.
    ff.flush_pending_to_disk({"AAPL": [r("finviz", 2.10, None)]},
                             source="finviz", is_final=True)
    # 2. zacks supplies the revenue; the merge folds it in and drops the donor.
    ff.flush_pending_to_disk({"AAPL": [r("zacks", 2.10, 1000.0)]},
                             source="zacks", is_final=True)
    mid = eh.load_earnings_history().iloc[0]
    assert mid["reported_rev"] == 1000.0 and mid["rev_source"] == "zacks"

    # 3. finviz refreshes the same quarter — still with no revenue of its own.
    ff.flush_pending_to_disk({"AAPL": [r("finviz", 2.11, None)]},
                             source="finviz", is_final=True)

    row = eh.load_earnings_history().iloc[0]
    assert row["reported_eps"] == 2.11, "finviz's own update must still land"
    assert row["reported_rev"] == 1000.0, (
        "finviz retracted a value it never owned"
    )
    assert row["rev_source"] == "zacks"


def test_a_source_may_still_retract_its_own_value(tmp_parquets):
    """The other side of the rule: when the superseded row's value belongs to
    the fill's OWN source, a response that drops it is a real correction and
    must be honoured — otherwise a withdrawn figure could never be removed."""
    def r(rev):
        return _with_rev(
            _hrow("AAPL", "2025-12-01", "2026-01-29", "finviz"),
            None, rev, None, None)

    ff.flush_pending_to_disk({"AAPL": [r(1000.0)]},
                             source="finviz", is_final=True)
    assert eh.load_earnings_history().iloc[0]["reported_rev"] == 1000.0

    ff.flush_pending_to_disk({"AAPL": [r(None)]},
                             source="finviz", is_final=True)

    row = eh.load_earnings_history().iloc[0]
    assert pd.isna(row["reported_rev"]), "finviz could not retract its own value"
    assert pd.isna(row["rev_source"])


def test_incoming_value_beats_a_carried_one(tmp_parquets):
    """Carry-forward fills a GAP; it never overrides what the response sent."""
    ff.flush_pending_to_disk(
        {"AAPL": [_with_rev(_hrow("AAPL", "2025-12-01", "2026-01-29", "finviz"),
                            None, None, None, None)]},
        source="finviz", is_final=True)
    ff.flush_pending_to_disk(
        {"AAPL": [_with_rev(_hrow("AAPL", "2025-12-01", "2026-01-29", "zacks"),
                            None, 1000.0, None, None)]},
        source="zacks", is_final=True)

    # finviz now HAS revenue of its own — it outranks zacks and must win.
    ff.flush_pending_to_disk(
        {"AAPL": [_with_rev(_hrow("AAPL", "2025-12-01", "2026-01-29", "finviz"),
                            None, 1234.0, None, None)]},
        source="finviz", is_final=True)

    row = eh.load_earnings_history().iloc[0]
    assert row["reported_rev"] == 1234.0
    assert row["rev_source"] == "finviz"


def test_carry_forward_leaves_untouched_slots_alone(tmp_parquets):
    """A quarter the incoming response doesn't cover is out of scope for both
    the supersede and the carry-forward."""
    def r(period, src, rev):
        return _with_rev(
            _hrow("AAPL", period, "2026-01-29", src), None, rev, None, None)

    ff.flush_pending_to_disk(
        {"AAPL": [r("2025-09-01", "finviz", None),
                  r("2025-12-01", "finviz", None)]},
        source="finviz", is_final=True)
    ff.flush_pending_to_disk(
        {"AAPL": [r("2025-09-01", "zacks", 800.0),
                  r("2025-12-01", "zacks", 1000.0)]},
        source="zacks", is_final=True)

    # A short finviz response covering only the newest quarter.
    ff.flush_pending_to_disk({"AAPL": [r("2025-12-01", "finviz", None)]},
                             source="finviz", is_final=True)

    df = eh.load_earnings_history().set_index("period_ending")
    assert df.loc[pd.Timestamp("2025-09-01"), "reported_rev"] == 800.0
    assert df.loc[pd.Timestamp("2025-12-01"), "reported_rev"] == 1000.0


def test_an_all_none_batch_does_not_drag_a_column_to_object_dtype(
        tmp_parquets):
    """A finnhub-only fill has no revenue at all, so `reported_rev` arrives as
    an OBJECT column of Nones. Concatenated onto the float64 column already on
    disk, pandas 2.2 warns and pandas 3 resolves the result to object — the
    exact drift `verify_integrity` check #6 was written to REPORT. Fixed at the
    origin instead."""
    eh.save_earnings_history(
        pd.DataFrame([_with_rev(
            _hrow("AAPL", "2025-09-01", "2025-10-29", "finviz"),
            900.0, 1000.0, 100.0, 11.1)]), sort=True)

    ff.flush_pending_to_disk(
        {"MSFT": [_with_rev(_hrow("MSFT", "2025-09-01", "2025-10-29",
                                  "finnhub"), None, None, None, None)]},
        source="finnhub", is_final=True)

    df = eh.load_earnings_history()
    for col in ("estimated_rev", "reported_rev", "surprise_rev",
                "surprise_rev_pct", "reported_eps"):
        assert df[col].dtype != object, f"{col} drifted to object dtype"
    # And the real value is untouched.
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    assert aapl["reported_rev"] == 1000.0


# ══════════════════════════════════════════════════════════════════════
# F5 — a re-sent bar that loses volume is the provisional one
# ══════════════════════════════════════════════════════════════════════

def _bars(dates, closes, volumes):
    return pd.DataFrame(
        {"Open": closes, "High": closes, "Low": closes,
         "Close": closes, "Volume": volumes},
        index=pd.to_datetime(dates),
    )


def test_resent_bar_losing_volume_is_rejected():
    """The overlap window exists so FINAL bars replace PROVISIONAL ones. The
    guard checked Close alone — while Volume was the field measured wrong on
    60/60 sampled tickers — so a bar with a matching Close and a fraction of
    the true volume sailed through and overwrote the good one."""
    old = _bars(["2026-08-04"], [100.0], [1_000_000])
    new = _bars(["2026-08-04", "2026-08-05"], [100.0, 101.0], [500_000, 900_000])

    out = data_engine._reject_conflicting_bars("TEST", old, new)

    assert pd.Timestamp("2026-08-04") not in out.index
    assert pd.Timestamp("2026-08-05") in out.index


def test_resent_bar_gaining_volume_is_adopted():
    """Upward revision is the correction the refetch overlap was added for and
    must always be adopted."""
    old = _bars(["2026-08-04"], [100.0], [1_000_000])
    new = _bars(["2026-08-04"], [100.0], [1_400_000])
    assert len(data_engine._reject_conflicting_bars("TEST", old, new)) == 1


def test_small_volume_drift_is_tolerated():
    """Below the threshold is ordinary provider noise, not a regression."""
    old = _bars(["2026-08-04"], [100.0], [1_000_000])
    new = _bars(["2026-08-04"], [100.0], [980_000])   # 2% under
    assert len(data_engine._reject_conflicting_bars("TEST", old, new)) == 1


def test_volume_guard_needs_a_usable_cached_volume():
    """A zero/missing cached volume must never reject a good incoming bar."""
    old = _bars(["2026-08-04"], [100.0], [0])
    new = _bars(["2026-08-04"], [100.0], [1_000_000])
    assert len(data_engine._reject_conflicting_bars("TEST", old, new)) == 1

    old_nan = _bars(["2026-08-04"], [100.0], [float("nan")])
    assert len(data_engine._reject_conflicting_bars(
        "TEST", old_nan, new)) == 1


def test_volume_guard_absent_column_does_not_break_the_update():
    old = _bars(["2026-08-04"], [100.0], [1_000_000]).drop(columns=["Volume"])
    new = _bars(["2026-08-04"], [100.0], [1])
    assert len(data_engine._reject_conflicting_bars("TEST", old, new)) == 1


# ══════════════════════════════════════════════════════════════════════
# F8 — orphaned atomic-write temps
# ══════════════════════════════════════════════════════════════════════

def _make_temp(dirpath, name="earnings_dates.parquet", pid=3696):
    p = dirpath / f".{name}.{pid}.{'d' * 32}.tmp"
    p.write_bytes(b"partial")
    return p


def test_stale_temp_files_are_reaped(tmp_path, monkeypatch):
    """Observed live: a 57,731-byte `.earnings_dates.parquet.3696.<uuid>.tmp`
    orphaned two days by a hard kill — larger than the file it was becoming.
    The unlink-on-failure path only runs when the writer RAISES."""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    stale = _make_temp(tmp_path)
    old = (datetime.now() - timedelta(hours=48)).timestamp()
    os.utime(stale, (old, old))

    assert config.prune_stale_temp_files() == 1
    assert not stale.exists()


def test_fresh_temp_files_are_left_alone(tmp_path, monkeypatch):
    """A concurrent writer's temp must never be pulled out from under it."""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    fresh = _make_temp(tmp_path)
    assert config.prune_stale_temp_files() == 0
    assert fresh.exists()


def test_reaper_only_matches_our_own_temp_naming(tmp_path, monkeypatch):
    """The pattern is exact so it can only ever match a temp WE created."""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    decoys = [
        tmp_path / "notes.tmp",
        tmp_path / ".hidden.tmp",
        tmp_path / ".earnings.parquet.notapid.abc.tmp",
        tmp_path / "earnings_history.parquet",
    ]
    for d in decoys:
        d.write_bytes(b"x")
    old = (datetime.now() - timedelta(days=30)).timestamp()
    for d in decoys:
        os.utime(d, (old, old))

    assert config.prune_stale_temp_files() == 0
    assert all(d.exists() for d in decoys)


def test_reaper_walks_subdirectories(tmp_path, monkeypatch):
    """Temps land beside their target, so ohlcv/ and earnings_raw/ have them."""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    sub = tmp_path / "ohlcv"
    sub.mkdir()
    stale = _make_temp(sub, name="AAPL.parquet")
    old = (datetime.now() - timedelta(hours=48)).timestamp()
    os.utime(stale, (old, old))

    assert config.prune_stale_temp_files() == 1
    assert not stale.exists()


# ══════════════════════════════════════════════════════════════════════
# F9 — backups spaced by time, not by write
# ══════════════════════════════════════════════════════════════════════

def test_backups_are_not_rotated_twice_within_the_interval(tmp_parquets):
    """Canonical saves cluster — three sources finalize per smart refresh, plus
    the launch migrations — so a per-write rotation burned all three slots in
    one session and left three snapshots of the same afternoon."""
    df = pd.DataFrame(_quarters("AAPL", ["2024-12-01"]))
    eh.save_earnings_history(df, sort=True)          # creates the file
    eh.save_earnings_history(df, sort=True)          # first snapshot
    eh.save_earnings_history(df, sort=True)          # must NOT rotate again
    eh.save_earnings_history(df, sort=True)

    assert len(eh.history_backup_paths()) == 1


def test_a_snapshot_older_than_the_interval_rotates(tmp_parquets):
    df = pd.DataFrame(_quarters("AAPL", ["2024-12-01"]))
    eh.save_earnings_history(df, sort=True)
    eh.save_earnings_history(df, sort=True)
    assert len(eh.history_backup_paths()) == 1

    aged = (datetime.now()
            - timedelta(hours=config.HISTORY_BACKUP_MIN_INTERVAL_HOURS + 1))
    for p in eh.history_backup_paths():
        os.utime(p, (aged.timestamp(), aged.timestamp()))

    eh.save_earnings_history(df, sort=True)
    assert len(eh.history_backup_paths()) == 2


def test_backup_depth_is_still_capped(tmp_parquets):
    df = pd.DataFrame(_quarters("AAPL", ["2024-12-01"]))
    eh.save_earnings_history(df, sort=True)
    aged = timedelta(hours=config.HISTORY_BACKUP_MIN_INTERVAL_HOURS + 1)
    for _ in range(config.HISTORY_BACKUP_COUNT + 3):
        eh.save_earnings_history(df, sort=True)
        for p in eh.history_backup_paths():
            ts = (datetime.now() - aged).timestamp()
            os.utime(p, (ts, ts))

    assert len(eh.history_backup_paths()) <= config.HISTORY_BACKUP_COUNT


# ══════════════════════════════════════════════════════════════════════
# Zacks merge key — period_ending, not report_date
# ══════════════════════════════════════════════════════════════════════

def _z(report, period, *, eps=True, **over):
    row = {
        "report_date": pd.Timestamp(report) if report else None,
        "period_ending": pd.Timestamp(period) if period else None,
        "report_time": "Close",
    }
    if eps:
        row.update({"estimated_eps": 1.0, "reported_eps": 1.1,
                    "surprise_eps": 0.1, "surprise_eps_pct": 10.0})
    else:
        row.update({"estimated_rev": 100.0, "reported_rev": 110.0,
                    "surprise_rev": 10.0, "surprise_rev_pct": 10.0})
    row.update(over)
    return row


def test_two_quarters_announced_the_same_day_both_survive():
    """Keying the merge on report_date silently LOST a quarter whenever two
    were announced together — a catch-up filing after a delinquency."""
    eps = [
        _z("2026-02-02", "2025-09-01"),
        _z("2026-02-02", "2025-12-01"),
    ]
    out = zs._merge_and_filter(eps, [], pd.Timestamp("2020-01-01"))

    assert len(out) == 2
    assert {r["period_ending"] for r in out} == {
        pd.Timestamp("2025-09-01"), pd.Timestamp("2025-12-01")}


def test_revenue_joins_on_period_ending():
    eps = [_z("2026-01-29", "2025-12-01")]
    rev = [_z("2026-01-29", "2025-12-01", eps=False)]
    out = zs._merge_and_filter(eps, rev, pd.Timestamp("2020-01-01"))

    assert len(out) == 1
    assert out[0]["reported_eps"] == 1.1
    assert out[0]["reported_rev"] == 110.0


def test_merge_reports_dropped_rows(caplog):
    """A row dropped here is invisible everywhere else — it never reaches
    failed_cb and never feeds the parse-spike alarm — so it would look exactly
    like the F1 truncation it is not."""
    stats: dict = {}
    eps = [
        _z("2026-01-29", "2025-12-01"),     # kept
        _z(None, "2025-09-01"),             # no report_date
        _z("2026-01-29", None),             # no period_ending
        _z("2018-01-29", "2017-12-01"),     # pre-cutoff
    ]
    out = zs._merge_and_filter(eps, [], pd.Timestamp("2021-01-01"), stats)

    assert len(out) == 1
    assert stats["no_report_date"] == 1
    assert stats["no_period"] == 1
    assert stats["pre_cutoff"] == 1


def test_merge_output_is_newest_period_first():
    eps = [_z(f"{y}-02-01", f"{y - 1}-12-01")
           for y in (2024, 2026, 2025)]
    out = zs._merge_and_filter(eps, [], pd.Timestamp("2020-01-01"))
    periods = [r["period_ending"] for r in out]
    assert periods == sorted(periods, reverse=True)
