"""Tests for earnings_history.py — parquet I/O, schema, lookup helpers,
and the bulk/targeted fill loop."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest

from trade_scanner_fh import earnings_history as eh


def _row(
    ticker: str, period_str: str, report_str: str,
    *,
    eps_est=2.0, eps_rep=2.1, eps_surp=0.1, eps_pct=5.0,
    rev_est=100.0, rev_rep=105.0, rev_surp=5.0, rev_pct=5.0,
    source="zacks", report_time="Close",
) -> dict:
    """Build a single earnings_history row dict in the §3.1 schema."""
    return {
        "ticker": ticker,
        "period_ending": pd.Timestamp(period_str),
        "report_date": pd.Timestamp(report_str),
        "report_time": report_time,
        "estimated_eps": eps_est,
        "reported_eps": eps_rep,
        "surprise_eps": eps_surp,
        "surprise_eps_pct": eps_pct,
        "estimated_rev": rev_est,
        "reported_rev": rev_rep,
        "surprise_rev": rev_surp,
        "surprise_rev_pct": rev_pct,
        "source": source,
        "updated_at": pd.Timestamp(datetime.now()),
    }


# ----------------------------------------------------------------------
# YoY columns — compute_yoy_columns
# ----------------------------------------------------------------------

def test_yoy_columns_computed_from_prior_year_same_period():
    """Standard case: same ticker, exactly 365 days between periods.
    yoy = (cur - prior) / |prior| * 100, rounded to internal float."""
    rows = [
        _row("AAPL", "2025-03-01", "2025-04-29",
             eps_rep=1.65, rev_rep=95359.0),
        _row("AAPL", "2026-03-01", "2026-04-28",
             eps_rep=2.01, rev_rep=111184.0),
    ]
    out = eh.compute_yoy_columns(pd.DataFrame(rows))
    cur = out.loc[out["period_ending"] == pd.Timestamp("2026-03-01")].iloc[0]
    prior = out.loc[out["period_ending"] == pd.Timestamp("2025-03-01")].iloc[0]
    # Current row gets YoY values; prior row stays NaN (no Q1 2024 fixture)
    assert abs(cur["yoy_eps_pct"] - 21.818) < 0.01
    assert abs(cur["yoy_rev_pct"] - 16.595) < 0.01
    assert pd.isna(prior["yoy_eps_pct"])
    assert pd.isna(prior["yoy_rev_pct"])


def test_yoy_columns_handles_negative_prior_eps_correctly():
    """Negative prior + positive current → positive YoY (improvement).
    (0.10 - (-0.50)) / 0.50 = +120%."""
    rows = [
        _row("X", "2025-03-01", "2025-05-01", eps_rep=-0.50),
        _row("X", "2026-03-01", "2026-05-01", eps_rep=0.10),
    ]
    out = eh.compute_yoy_columns(pd.DataFrame(rows))
    cur = out.loc[out["period_ending"] == pd.Timestamp("2026-03-01")].iloc[0]
    assert abs(cur["yoy_eps_pct"] - 120.0) < 0.01


def test_yoy_columns_nan_when_prior_missing():
    """Single-row ticker → no prior-year row → YoY stays NaN."""
    rows = [
        _row("LONELY", "2026-03-01", "2026-05-01", eps_rep=2.0),
    ]
    out = eh.compute_yoy_columns(pd.DataFrame(rows))
    assert pd.isna(out.iloc[0]["yoy_eps_pct"])
    assert pd.isna(out.iloc[0]["yoy_rev_pct"])


def test_yoy_columns_nan_when_prior_value_is_zero():
    """Prior=0 produces div-by-zero → skip (NaN). Tested for both EPS
    and Rev independently."""
    rows = [
        _row("Z", "2025-03-01", "2025-05-01", eps_rep=0.0, rev_rep=0.0),
        _row("Z", "2026-03-01", "2026-05-01", eps_rep=0.5, rev_rep=100.0),
    ]
    out = eh.compute_yoy_columns(pd.DataFrame(rows))
    cur = out.loc[out["period_ending"] == pd.Timestamp("2026-03-01")].iloc[0]
    assert pd.isna(cur["yoy_eps_pct"])
    assert pd.isna(cur["yoy_rev_pct"])


def test_yoy_columns_floor_nulls_near_zero_base():
    """A prior-year base below the floor (EPS<$0.05, rev<$1M) yields NaN
    rather than a meaningless blow-up; a base above the floor computes
    normally."""
    rows = [
        # TINY: prior EPS 0.001 (<0.05) and prior rev 0.5 (<1.0) → both NaN
        _row("TINY", "2024-03-01", "2024-05-01", eps_rep=0.001, rev_rep=0.5),
        _row("TINY", "2025-03-01", "2025-05-01", eps_rep=0.50, rev_rep=50.0),
        # BIG: prior above both floors → computed
        _row("BIG", "2024-03-01", "2024-05-01", eps_rep=1.00, rev_rep=100.0),
        _row("BIG", "2025-03-01", "2025-05-01", eps_rep=1.50, rev_rep=150.0),
    ]
    out = eh.compute_yoy_columns(pd.DataFrame(rows))
    tiny = out.loc[(out["ticker"] == "TINY")
                   & (out["period_ending"] == pd.Timestamp("2025-03-01"))].iloc[0]
    assert pd.isna(tiny["yoy_eps_pct"])   # prior 0.001 < 0.05 floor
    assert pd.isna(tiny["yoy_rev_pct"])   # prior 0.5 < 1.0 floor
    big = out.loc[(out["ticker"] == "BIG")
                  & (out["period_ending"] == pd.Timestamp("2025-03-01"))].iloc[0]
    assert big["yoy_eps_pct"] == pytest.approx(50.0)   # (1.5-1.0)/1.0*100
    assert big["yoy_rev_pct"] == pytest.approx(50.0)   # (150-100)/100*100


def test_yoy_columns_per_ticker_isolation():
    """Two tickers in the same frame: YoY for AAPL must NOT match
    against MSFT's prior-year row (different ticker)."""
    rows = [
        _row("AAPL", "2025-03-01", "2025-05-01", eps_rep=1.5),
        _row("MSFT", "2026-03-01", "2026-05-01", eps_rep=3.0),
    ]
    out = eh.compute_yoy_columns(pd.DataFrame(rows))
    # MSFT 2026 has no MSFT 2025 → NaN
    msft = out.loc[out["ticker"] == "MSFT"].iloc[0]
    assert pd.isna(msft["yoy_eps_pct"])


def test_yoy_columns_idempotent_on_repeat_call():
    """Calling compute_yoy_columns twice must produce identical
    yoy_*_pct values — the helper is the canonical refresh point."""
    rows = [
        _row("AAPL", "2025-03-01", "2025-05-01", eps_rep=1.50, rev_rep=80.0),
        _row("AAPL", "2026-03-01", "2026-05-01", eps_rep=2.00, rev_rep=100.0),
    ]
    out1 = eh.compute_yoy_columns(pd.DataFrame(rows))
    out2 = eh.compute_yoy_columns(out1)
    pd.testing.assert_series_equal(out1["yoy_eps_pct"], out2["yoy_eps_pct"])
    pd.testing.assert_series_equal(out1["yoy_rev_pct"], out2["yoy_rev_pct"])


def test_yoy_columns_dup_prior_period_last_wins():
    """Audit H1 parity: when the prior-year (ticker, period) appears twice,
    the LAST row's value is the prior base (matches the pre-vectorization
    loop's last-assignment-wins map)."""
    rows = [
        _row("A", "2024-12-01", "2025-01-15", eps_rep=1.0, rev_rep=100.0),
        _row("A", "2024-12-01", "2025-01-16", eps_rep=2.0, rev_rep=200.0),  # last
        _row("A", "2025-12-01", "2026-01-15", eps_rep=3.0, rev_rep=300.0),  # current
    ]
    out = eh.compute_yoy_columns(pd.DataFrame(rows))
    cur = out.iloc[2]
    # prior base = the LAST dup (eps 2.0, rev 200): (3-2)/2*100, (300-200)/200*100
    assert cur["yoy_eps_pct"] == pytest.approx(50.0)
    assert cur["yoy_rev_pct"] == pytest.approx(50.0)


def test_yoy_columns_empty_frame_returns_unchanged():
    """Empty / None input passes through without crashing."""
    assert eh.compute_yoy_columns(pd.DataFrame()).empty
    out = eh.compute_yoy_columns(None)
    assert out is None


def test_save_load_preserves_yoy_columns(tmp_parquets):
    """Schema: yoy_eps_pct + yoy_rev_pct round-trip through save/load."""
    rows = [
        _row("AAPL", "2025-03-01", "2025-05-01", eps_rep=1.65),
        _row("AAPL", "2026-03-01", "2026-05-01", eps_rep=2.01),
    ]
    df = eh.compute_yoy_columns(pd.DataFrame(rows))
    eh.save_earnings_history(df)
    loaded = eh.load_earnings_history()
    assert "yoy_eps_pct" in loaded.columns
    assert "yoy_rev_pct" in loaded.columns
    cur = loaded.loc[loaded["period_ending"] == pd.Timestamp("2026-03-01")].iloc[0]
    assert abs(cur["yoy_eps_pct"] - 21.818) < 0.01


# ----------------------------------------------------------------------
# Save / load round trip
# ----------------------------------------------------------------------

def test_save_load_round_trip(tmp_parquets):
    rows = [
        _row("AAPL", "2025-12-01", "2026-01-29"),
        _row("AAPL", "2025-09-01", "2025-10-30"),
        _row("MSFT", "2025-12-01", "2026-01-28"),
    ]
    eh.save_earnings_history(pd.DataFrame(rows))
    df = eh.load_earnings_history()
    assert df is not None
    assert len(df) == 3
    assert set(df["ticker"]) == {"AAPL", "MSFT"}


def test_save_sorts_ticker_asc_period_desc(tmp_parquets):
    rows = [
        _row("MSFT", "2025-09-01", "2025-10-28"),
        _row("AAPL", "2024-12-01", "2025-01-30"),
        _row("AAPL", "2025-12-01", "2026-01-29"),
        _row("MSFT", "2025-12-01", "2026-01-28"),
    ]
    eh.save_earnings_history(pd.DataFrame(rows))
    df = eh.load_earnings_history()
    # AAPL's two rows come first (alphabetical), with newest period first
    assert list(df["ticker"]) == ["AAPL", "AAPL", "MSFT", "MSFT"]
    aapl_periods = list(df.loc[df["ticker"] == "AAPL", "period_ending"])
    assert aapl_periods == sorted(aapl_periods, reverse=True)


def test_load_missing_file_returns_none(tmp_parquets):
    assert eh.load_earnings_history() is None


def test_save_drops_rows_without_ticker_or_period(tmp_parquets):
    df = pd.DataFrame([
        _row("AAPL", "2025-12-01", "2026-01-29"),
        # ticker None — should be dropped
        {**_row("X", "2025-09-01", "2025-10-29"), "ticker": None},
        # period_ending NaT — should be dropped
        {**_row("Y", "2025-09-01", "2025-10-29"), "period_ending": pd.NaT},
    ])
    eh.save_earnings_history(df)
    out = eh.load_earnings_history()
    assert len(out) == 1
    assert out.iloc[0]["ticker"] == "AAPL"


def test_save_atomic_write_no_tmp_residue(tmp_parquets):
    eh.save_earnings_history(pd.DataFrame([_row("AAPL", "2025-12-01", "2026-01-29")]))
    assert (tmp_parquets / "earnings_history.parquet").exists()
    assert not (tmp_parquets / "earnings_history.parquet.tmp").exists()


def test_save_empty_or_none_is_noop(tmp_parquets):
    eh.save_earnings_history(None)
    eh.save_earnings_history(pd.DataFrame())
    assert eh.load_earnings_history() is None


def test_save_reprunes_rows_past_cap(tmp_parquets):
    """save_earnings_history re-prunes the rolling history cap: a row whose
    period_ending is older than EARNINGS_HISTORY_YEARS is dropped on write
    (boundary rows can't linger as the daily cutoff advances)."""
    today = pd.Timestamp.today().normalize()
    recent = (today - pd.DateOffset(years=1)).replace(day=1)
    old = (today - pd.DateOffset(years=eh.config.EARNINGS_HISTORY_YEARS + 2)).replace(day=1)
    eh.save_earnings_history(pd.DataFrame([
        _row("AAA", str(recent.date()), str((recent + pd.Timedelta(days=40)).date())),
        _row("AAA", str(old.date()), str((old + pd.Timedelta(days=40)).date())),
    ]))
    periods = {pd.Timestamp(p) for p in eh.load_earnings_history()["period_ending"]}
    assert recent in periods
    assert old not in periods   # re-pruned past the cap on save


# ----------------------------------------------------------------------
# Phase 1 — `report_date_proxy` schema addition
# ----------------------------------------------------------------------

def test_row_to_history_dict_stamps_report_date_proxy_false_for_zacks():
    """Zacks always supplies real announcement dates so the proxy flag
    is False on every Zacks row."""
    raw = {
        "period_ending": pd.Timestamp("2025-12-01"),
        "report_date":   pd.Timestamp("2026-01-29"),
        "report_time":   "Close",
    }
    out = eh._row_to_history_dict(raw, "AAPL", "zacks", datetime.now())
    assert out["report_date_proxy"] is False


def test_load_earnings_history_legacy_rows_get_proxy_false(tmp_parquets):
    """A parquet written before the report_date_proxy column existed
    must round-trip with the column added and stamped False on read.
    """
    path = tmp_parquets / "earnings_history.parquet"
    legacy_row = {
        "ticker": "AAPL",
        "period_ending": pd.Timestamp("2025-12-01"),
        "report_date":   pd.Timestamp("2026-01-29"),
        "report_time":   "Close",
        "estimated_eps": 2.0, "reported_eps": 2.1,
        "surprise_eps": 0.1, "surprise_eps_pct": 5.0,
        "estimated_rev": 100.0, "reported_rev": 105.0,
        "surprise_rev": 5.0, "surprise_rev_pct": 5.0,
        "source": "zacks",
        "updated_at": pd.Timestamp(datetime.now()),
    }
    pd.DataFrame([legacy_row]).to_parquet(path, index=False)
    df = eh.load_earnings_history()
    assert "report_date_proxy" in df.columns
    assert df.iloc[0]["report_date_proxy"] is False or \
           df.iloc[0]["report_date_proxy"] == False  # noqa: E712


# ----------------------------------------------------------------------
# Phase 1 — Zacks fill writes raw layer
# ----------------------------------------------------------------------

def test_zacks_fill_writes_raw_layer(tmp_parquets, monkeypatch):
    """End-to-end: a successful _fill_via_zacks pass appends rows into
    earnings_raw/zacks/<run_id>.parquet on every flush."""
    from trade_scanner_fh import config, earnings_raw

    raw_root = tmp_parquets / "earnings_raw"
    monkeypatch.setattr(config, "RAW_EARNINGS_DIR", raw_root)
    # exist_ok: the tmp_parquets fixture itself now redirects
    # RAW_EARNINGS_DIR here and pre-creates the per-source folders
    # (conftest trap fix) — this inline setup stays as belt-and-braces.
    raw_root.mkdir(exist_ok=True)
    for src in config.RAW_SOURCES:
        (raw_root / src).mkdir(exist_ok=True)

    monkeypatch.setattr(eh.time, "sleep", lambda *_: None)

    # Stub ZacksSession to return a deterministic 1-quarter response.
    fake_rows = [{
        "period_ending": pd.Timestamp("2025-12-01"),
        "report_date":   pd.Timestamp("2026-01-29"),
        "report_time":   "Close",
        "estimated_eps": 2.0, "reported_eps": 2.1,
        "surprise_eps": 0.1, "surprise_eps_pct": 5.0,
        "estimated_rev": 100.0, "reported_rev": 105.0,
        "surprise_rev": 5.0, "surprise_rev_pct": 5.0,
    }]

    class FakeSession:
        last_failure_kind = None

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def fetch(self, sym, years=5):
            return fake_rows

    monkeypatch.setattr(eh, "ZacksSession", FakeSession)

    eh.bulk_fill_zacks(["AAPL", "MSFT"], blacklist=set(),
                       delay_sec=0, flush_every=1)

    df = earnings_raw.read_raw(config.RAW_SOURCE_ZACKS)
    assert len(df) == 2
    assert set(df["ticker"]) == {"AAPL", "MSFT"}
    assert all(df["run_id"].notna())


def test_zacks_fill_caps_consumer_rows_on_period_ending(tmp_parquets, monkeypatch):
    """Zacks consumer rows are capped on period_ending (aligned with
    finviz/finnhub) so the dedup window matches across sources. A quarter
    older than the cap is dropped from earnings_history.parquet but the raw
    layer keeps it (full depth for replay)."""
    from trade_scanner_fh import config, earnings_raw

    raw_root = tmp_parquets / "earnings_raw"
    monkeypatch.setattr(config, "RAW_EARNINGS_DIR", raw_root)
    # exist_ok: the tmp_parquets fixture itself now redirects
    # RAW_EARNINGS_DIR here and pre-creates the per-source folders
    # (conftest trap fix) — this inline setup stays as belt-and-braces.
    raw_root.mkdir(exist_ok=True)
    for src in config.RAW_SOURCES:
        (raw_root / src).mkdir(exist_ok=True)
    monkeypatch.setattr(eh.time, "sleep", lambda *_: None)

    today = pd.Timestamp.today().normalize()
    recent_pe = (today - pd.DateOffset(years=1)).replace(day=1)
    old_pe = (today - pd.DateOffset(years=config.EARNINGS_HISTORY_YEARS + 3)).replace(day=1)
    fake_rows = [
        {"period_ending": recent_pe, "report_date": recent_pe + pd.Timedelta(days=40),
         "report_time": "Close", "estimated_eps": 2.0, "reported_eps": 2.1,
         "surprise_eps": 0.1, "surprise_eps_pct": 5.0, "estimated_rev": 100.0,
         "reported_rev": 105.0, "surprise_rev": 5.0, "surprise_rev_pct": 5.0},
        {"period_ending": old_pe, "report_date": old_pe + pd.Timedelta(days=40),
         "report_time": "Close", "estimated_eps": 1.0, "reported_eps": 1.1,
         "surprise_eps": 0.1, "surprise_eps_pct": 9.0, "estimated_rev": 50.0,
         "reported_rev": 52.0, "surprise_rev": 2.0, "surprise_rev_pct": 4.0},
    ]

    class FakeSession:
        last_failure_kind = None
        def __enter__(self): return self
        def __exit__(self, *exc): return False
        def fetch(self, sym, years=5): return fake_rows

    monkeypatch.setattr(eh, "ZacksSession", FakeSession)

    eh.bulk_fill_zacks(["AAPL"], blacklist=set(), delay_sec=0, flush_every=1)

    hist = eh.load_earnings_history()
    periods = {pd.Timestamp(p) for p in hist.loc[hist["ticker"] == "AAPL", "period_ending"]}
    assert recent_pe in periods          # within cap → kept
    assert old_pe not in periods         # older than cap → dropped from consumer
    # Raw layer preserves BOTH quarters.
    raw = earnings_raw.read_raw(config.RAW_SOURCE_ZACKS)
    assert len(raw.loc[raw["ticker"] == "AAPL"]) == 2


# ----------------------------------------------------------------------
# Phase 2 — dedupe helper (Zacks > Finnhub) + (ticker, source) PK flush
# ----------------------------------------------------------------------

def test_dedupe_zacks_wins_same_slot_keeps_finnhub_gap_fill():
    """Gap-fill source policy: per (ticker, period_ending), the
    highest-priority source wins (zacks > finnhub). Rows from
    lower-priority sources on DIFFERENT periods are kept as gap-fill —
    Finnhub data on a quarter Zacks doesn't cover survives."""
    rows = [
        _row("AAPL", "2025-12-01", "2026-01-29", source="zacks",
             eps_est=1.95, eps_rep=2.10),
        # Same period as Zacks — Finnhub LOSES this slot to Zacks.
        _row("AAPL", "2025-12-01", "2026-01-29", source="finnhub",
             eps_est=1.90, eps_rep=2.05),
        # DIFFERENT period — Finnhub fills the gap (KEPT).
        _row("AAPL", "2025-06-01", "2025-07-30", source="finnhub",
             eps_est=1.50, eps_rep=1.55),
    ]
    df = pd.DataFrame(rows)
    deduped = eh.dedupe_history(df)
    assert len(deduped) == 2
    by_period = {
        pd.Timestamp(r.period_ending): r
        for r in deduped.itertuples(index=False)
    }
    same_slot = by_period[pd.Timestamp("2025-12-01")]
    assert same_slot.source == "zacks"
    assert same_slot.reported_eps == 2.10
    gap_slot = by_period[pd.Timestamp("2025-06-01")]
    assert gap_slot.source == "finnhub"
    assert gap_slot.reported_eps == 1.55


def test_dedupe_finviz_beats_zacks_and_finnhub_in_same_slot():
    """Finviz is top priority (finviz > zacks > finnhub). When all three
    cover the same slot, finviz wins."""
    rows = [
        _row("AAPL", "2025-12-01", "2026-01-29", source="zacks",
             eps_rep=2.10),
        _row("AAPL", "2025-12-01", "2026-01-29", source="finnhub",
             eps_rep=2.05),
        _row("AAPL", "2025-12-01", "2026-01-29", source="finviz",
             eps_rep=2.11),
    ]
    deduped = eh.dedupe_history(pd.DataFrame(rows))
    assert len(deduped) == 1
    assert deduped.iloc[0]["source"] == "finviz"
    assert deduped.iloc[0]["reported_eps"] == 2.11


def test_dedupe_zacks_still_beats_finnhub_without_finviz():
    """With no finviz row present, zacks still outranks finnhub."""
    rows = [
        _row("AAPL", "2025-12-01", "2026-01-29", source="finnhub",
             eps_rep=2.05),
        _row("AAPL", "2025-12-01", "2026-01-29", source="zacks",
             eps_rep=2.10),
    ]
    deduped = eh.dedupe_history(pd.DataFrame(rows))
    assert len(deduped) == 1
    assert deduped.iloc[0]["source"] == "zacks"


def test_reported_actuals_mix_freely_across_sources():
    """Audit 2026-08-16 (F13/F14): every source quotes the same adjusted basis,
    so the REPORTED actuals are taken per column from the highest-priority row
    that has one — EPS and revenue independently."""
    rows = [
        _row("AAPL", "2025-12-01", "2026-01-29", source="finviz",
             eps_est=1.95, eps_rep=None, eps_surp=None, eps_pct=None),
        _row("AAPL", "2025-12-01", "2026-01-29", source="zacks",
             eps_est=None, eps_rep=2.10, eps_surp=None, eps_pct=None),
        _row("AAPL", "2025-12-01", "2026-01-29", source="finnhub",
             eps_est=None, eps_rep=2.05, eps_surp=None, eps_pct=None),
    ]
    rows[0].update({"reported_rev": None})
    rows[1].update({"reported_rev": None})
    rows[2].update({"reported_rev": 1000.0})

    deduped = eh.dedupe_history(pd.DataFrame(rows), merge_sources=True)
    assert len(deduped) == 1
    win = deduped.iloc[0]

    assert win["source"] == "finviz"          # slot winner is unchanged
    # EPS from zacks (finviz had none, zacks outranks finnhub).
    assert win["reported_eps"] == 2.10
    assert win["eps_source"] == "zacks"
    # Revenue from finnhub, independently.
    assert win["reported_rev"] == 1000.0
    assert win["rev_source"] == "finnhub"


def test_estimates_and_surprises_are_finviz_only():
    """Audit 2026-08-16 (F14): the estimate and its surprise are one provider's
    opinion and are inextricably paired (surprise = actual − estimate), so the
    whole cluster is single-sourced. A zacks estimate never reaches the
    parquet, and a merge can never pull one onto the winner."""
    rows = [
        _row("AAPL", "2025-12-01", "2026-01-29", source="finviz",
             eps_est=None, eps_rep=None, eps_surp=None, eps_pct=None),
        _row("AAPL", "2025-12-01", "2026-01-29", source="zacks",
             eps_est=1.90, eps_rep=2.05, eps_surp=0.15, eps_pct=7.9),
    ]
    # The finviz row owns the slot but has NOTHING — the merge must pull the
    # actuals across and nothing else.
    rows[0].update({"estimated_rev": None, "reported_rev": None,
                    "surprise_rev": None, "surprise_rev_pct": None})
    rows[1].update({"estimated_rev": 900.0, "reported_rev": 1000.0,
                    "surprise_rev": 100.0, "surprise_rev_pct": 11.1})

    win = eh.dedupe_history(pd.DataFrame(rows), merge_sources=True).iloc[0]

    # The REPORTED actuals cross over...
    assert win["reported_eps"] == 2.05
    assert win["eps_source"] == "zacks"
    assert win["reported_rev"] == 1000.0
    assert win["rev_source"] == "zacks"
    # ...but nothing estimate-derived does.
    for col in ("estimated_eps", "surprise_eps", "surprise_eps_pct",
                "estimated_rev", "surprise_rev", "surprise_rev_pct"):
        assert pd.isna(win[col]), f"{col} leaked from a non-finviz source"


def test_finviz_keeps_its_own_estimates_and_surprises():
    """The other half: finviz's own estimate cluster is what the columns are
    for, so it must survive the same pass untouched."""
    rows = [
        _row("AAPL", "2025-12-01", "2026-01-29", source="finviz",
             eps_est=1.95, eps_rep=2.10, eps_surp=0.15, eps_pct=7.7),
        _row("AAPL", "2025-12-01", "2026-01-29", source="zacks",
             eps_est=1.90, eps_rep=2.05, eps_surp=0.15, eps_pct=7.9),
    ]
    win = eh.dedupe_history(pd.DataFrame(rows), merge_sources=True).iloc[0]

    assert win["source"] == "finviz"
    assert win["reported_eps"] == 2.10
    assert win["eps_source"] == "finviz"
    assert win["estimated_eps"] == 1.95
    assert win["surprise_eps"] == 0.15
    assert win["surprise_eps_pct"] == 7.7


def test_reported_source_is_na_when_there_is_no_reported_value():
    """No value means no source to attribute."""
    rows = [
        _row("AAPL", "2025-12-01", "2026-01-29", source="finviz",
             eps_est=None, eps_rep=None, eps_surp=None, eps_pct=None),
    ]
    rows[0].update({"reported_rev": None})
    win = eh.dedupe_history(pd.DataFrame(rows), merge_sources=True).iloc[0]
    assert pd.isna(win["eps_source"])
    assert pd.isna(win["rev_source"])


def test_dedupe_winner_keeps_its_own_reported_value():
    """A present value is never overwritten by a lower-priority one — the
    merge only fills genuine NaNs.

    (Audit 2026-08-16 F14 changed the estimate half of this: neither of these
    rows is finviz, so their estimates are stripped and no longer assertable.
    The reported-value precedence this really guards is unchanged.)"""
    rows = [
        _row("AAPL", "2025-12-01", "2026-01-29", source="zacks",
             eps_est=1.95, eps_rep=2.10, eps_surp=0.15, eps_pct=7.7),
        _row("AAPL", "2025-12-01", "2026-01-29", source="finnhub",
             eps_est=1.90, eps_rep=2.05, eps_surp=0.15, eps_pct=7.9),
    ]
    deduped = eh.dedupe_history(pd.DataFrame(rows), merge_sources=True)
    assert len(deduped) == 1
    win = deduped.iloc[0]
    assert win["source"] == "zacks"
    assert win["reported_eps"] == 2.10   # Zacks' own, not Finnhub's 2.05
    assert win["eps_source"] == "zacks"


def test_dedupe_merge_off_by_default_leaves_winner_nan():
    """Without the flag the winner stays pure — no cross-source fill at all.
    (The write path now passes it; audit 2026-08-16 F13. The default itself is
    unchanged, and this guards it — `get_ticker_history` and the integrity
    fixer both rely on the plain dedup not rewriting values.)"""
    rows = [
        _row("AAPL", "2025-12-01", "2026-01-29", source="zacks",
             eps_est=None, eps_rep=2.10, eps_surp=None, eps_pct=None),
        _row("AAPL", "2025-12-01", "2026-01-29", source="finnhub",
             eps_est=1.95, eps_rep=2.05,
             eps_surp=0.10, eps_pct=5.13),
    ]
    deduped = eh.dedupe_history(pd.DataFrame(rows))
    assert len(deduped) == 1
    win = deduped.iloc[0]
    assert win["source"] == "zacks"
    assert pd.isna(win["estimated_eps"])
    assert pd.isna(win["surprise_eps_pct"])


def test_dedupe_merge_does_not_invent_a_quarter():
    """The merge must collapse same-slot dups to ONE row, never leave
    the duplicate behind — this is the column-shift regression guard."""
    rows = [
        _row("AAOI", "2026-03-01", "2026-05-07", source="zacks",
             eps_est=-0.05, eps_rep=-0.07, eps_surp=-0.02, eps_pct=-40.0),
        _row("AAOI", "2026-03-01", "2026-05-08", source="finnhub",
             eps_est=-0.05, eps_rep=-0.07, eps_surp=-0.02, eps_pct=-41.0),
        _row("AAOI", "2025-12-01", "2026-02-26", source="zacks"),
        _row("AAOI", "2025-12-01", "2025-12-31", source="finnhub"),
    ]
    deduped = eh.dedupe_history(pd.DataFrame(rows), merge_sources=True)
    # Two distinct fiscal quarters → exactly two rows, no doubles.
    assert len(deduped) == 2
    assert sorted(p.date().isoformat()
                  for p in deduped["period_ending"]) == ["2025-12-01",
                                                         "2026-03-01"]


def test_dedupe_keeps_finnhub_when_ticker_has_no_zacks_coverage():
    """Finnhub rows survive ONLY when no Zacks row exists for the
    ticker. Different ticker with Zacks data must not affect them."""
    rows = [
        _row("AAPL", "2025-12-01", "2026-01-29", source="zacks"),
        _row("MSFT", "2025-12-01", "2026-01-29", source="finnhub"),
        _row("MSFT", "2025-09-01", "2025-10-30", source="finnhub"),
    ]
    df = pd.DataFrame(rows)
    deduped = eh.dedupe_history(df)
    assert len(deduped) == 3
    msft = deduped.loc[deduped["ticker"] == "MSFT"]
    assert len(msft) == 2
    assert set(msft["source"]) == {"finnhub"}


def test_dedupe_collapses_same_source_pk_duplicates():
    """If the same source ends up with two rows for (ticker, period)
    (shouldn't happen post-flush, but defensive), keep the most-recent
    by updated_at."""
    older = _row("AAPL", "2025-12-01", "2026-01-29", source="zacks",
                 eps_est=1.90)
    older["updated_at"] = pd.Timestamp("2026-01-01")
    newer = _row("AAPL", "2025-12-01", "2026-01-29", source="zacks",
                 eps_est=2.10)
    newer["updated_at"] = pd.Timestamp("2026-02-01")
    deduped = eh.dedupe_history(pd.DataFrame([older, newer]))
    assert len(deduped) == 1
    assert deduped.iloc[0]["estimated_eps"] == 2.10


def test_dedupe_single_source_emits_bare_source_label():
    """When only one source contributes, the source label stays bare
    ('zacks' or 'finnhub'). Merged labels no longer exist post-rewrite."""
    rows = [
        _row("AAPL", "2025-12-01", "2026-01-29", source="finnhub"),
    ]
    deduped = eh.dedupe_history(pd.DataFrame(rows))
    assert len(deduped) == 1
    assert deduped.iloc[0]["source"] == "finnhub"


def test_get_ticker_history_dedupes_internally():
    """Scanner-side consumers should see Zacks-only rows for tickers
    Zacks covers, even if Finnhub rows were written for the same ticker
    (e.g. before the binary policy applied)."""
    rows = [
        _row("AAPL", "2025-12-01", "2026-01-29", source="zacks", eps_est=1.95),
        _row("AAPL", "2025-12-01", "2026-01-29", source="finnhub", eps_est=1.90),
        _row("AAPL", "2025-09-01", "2025-10-30", source="zacks", eps_est=1.85),
    ]
    df = pd.DataFrame(rows)
    sub = eh.get_ticker_history("AAPL", df)
    assert len(sub) == 2  # two distinct quarters, Finnhub row dropped
    assert set(sub["source"]) == {"zacks"}
    q4 = sub.loc[sub["period_ending"] == pd.Timestamp("2025-12-01")].iloc[0]
    assert q4["estimated_eps"] == 1.95


def test_flush_pending_to_disk_per_source_pk(tmp_parquets):
    """Critical Phase 2 behavior: writing Finnhub rows for a ticker
    must NOT wipe its Zacks rows, and vice versa."""
    # Seed with Zacks rows for AAPL.
    eh.save_earnings_history(pd.DataFrame([
        _row("AAPL", "2025-12-31", "2026-01-29", source="zacks"),
        _row("AAPL", "2025-09-30", "2025-10-30", source="zacks"),
    ]))

    # Now write Finnhub rows for AAPL (same period as one Zacks row + a new one).
    finnhub_rows = [
        _row("AAPL", "2025-12-31", "2026-01-29", source="finnhub"),
        _row("AAPL", "2025-06-30", "2025-07-31", source="finnhub"),
    ]
    pending = {"AAPL": finnhub_rows}
    eh._flush_pending_to_disk(pending, [], source="finnhub")

    df = eh.load_earnings_history()
    assert df is not None
    aapl = df.loc[df["ticker"] == "AAPL"]
    sources = aapl["source"].value_counts().to_dict()
    # Both Zacks rows preserved + both new Finnhub rows.
    assert sources.get("zacks") == 2
    assert sources.get("finnhub") == 2


# ----------------------------------------------------------------------
# Phase 6.5 — coverage_report
# ----------------------------------------------------------------------

def test_coverage_report_partitions_correctly():
    """Universe split into zacks_only / finnhub_only / both / neither."""
    rows = [
        _row("AAPL", "2025-12-31", "2026-01-29", source="zacks"),  # Z only
        _row("MSFT", "2025-12-31", "2026-01-29", source="finnhub"),  # F only
        _row("NVDA", "2025-12-31", "2026-01-29", source="zacks"),    # both
        _row("NVDA", "2025-09-30", "2025-10-30", source="finnhub"),
        # GOOG → in universe but no rows anywhere → neither
    ]
    df = pd.DataFrame(rows)
    universe = ["AAPL", "MSFT", "NVDA", "GOOG"]
    rep = eh.coverage_report(universe, blacklist=set(), history_df=df)
    assert rep["total_universe"] == 4
    assert rep["in_scope"] == 4
    assert rep["zacks_only"]["tickers"] == ["AAPL"]
    assert rep["finnhub_only"]["tickers"] == ["MSFT"]
    assert rep["both"]["tickers"] == ["NVDA"]
    assert rep["neither"]["tickers"] == ["GOOG"]


def test_coverage_report_handles_legacy_merged_source_label():
    """Backward-compat: legacy merged-source rows from before the
    binary policy still parse correctly — substring match on the
    source label counts the row for BOTH buckets. New writes never
    produce merged labels, but pre-existing parquets may contain them."""
    df = pd.DataFrame([
        {**_row("X", "2025-12-01", "2026-01-29", source="zacks"),
         "source": "zacks+finnhub_merged"},
    ])
    rep = eh.coverage_report(["X"], blacklist=set(), history_df=df)
    assert rep["both"]["count"] == 1
    assert rep["zacks_only"]["count"] == 0
    assert rep["finnhub_only"]["count"] == 0


def test_coverage_report_respects_blacklist():
    rows = [_row("BAD", "2025-12-31", "2026-01-29", source="zacks")]
    rep = eh.coverage_report(
        ["BAD", "GOOD"], blacklist={"BAD"}, history_df=pd.DataFrame(rows),
    )
    assert rep["blacklisted"] == 1
    assert rep["in_scope"] == 1
    assert rep["zacks_only"]["count"] == 0  # BAD is blacklisted
    assert rep["neither"]["tickers"] == ["GOOD"]


def test_coverage_report_empty_history_returns_all_neither():
    rep = eh.coverage_report(["X", "Y"], blacklist=set(), history_df=None)
    assert rep["zacks_only"]["count"] == 0
    assert rep["finnhub_only"]["count"] == 0
    assert rep["both"]["count"] == 0
    assert rep["neither"]["count"] == 2


def test_coverage_report_tracks_most_recent_per_source():
    rows = [
        _row("A", "2025-12-31", "2026-01-29", source="zacks"),
        _row("A", "2024-09-30", "2024-10-30", source="zacks"),
        _row("B", "2026-03-31", "2026-04-29", source="finnhub"),
    ]
    rep = eh.coverage_report(
        ["A", "B"], blacklist=set(), history_df=pd.DataFrame(rows),
    )
    assert rep["most_recent_zacks_quarter"] == pd.Timestamp("2025-12-31")
    assert rep["most_recent_finnhub_quarter"] == pd.Timestamp("2026-03-31")


# ----------------------------------------------------------------------
# Phase 6.5 — verify_integrity + fix_integrity_issues
# ----------------------------------------------------------------------

def test_verify_integrity_clean_data_returns_no_findings():
    rows = [
        _row("AAPL", "2025-12-31", "2026-01-29", source="zacks"),
        _row("MSFT", "2025-12-31", "2026-01-29", source="finnhub"),
    ]
    findings = eh.verify_integrity(history_df=pd.DataFrame(rows))
    # Clean data should produce ZERO findings (or only proxy-related
    # warnings if dtype quirks crept in — none expected here).
    assert findings == []


def test_verify_integrity_detects_duplicate_pk():
    # Two identical (ticker, period, source) rows.
    r = _row("AAPL", "2025-12-31", "2026-01-29", source="zacks")
    findings = eh.verify_integrity(history_df=pd.DataFrame([r, r]))
    dup = [f for f in findings if f.check == "duplicate_pk"]
    assert len(dup) == 1
    assert dup[0].affected_rows == 2
    assert dup[0].auto_fixable


def test_verify_integrity_detects_orphan_ticker():
    rows = [
        _row("AAPL", "2025-12-31", "2026-01-29"),
        {**_row("X", "2025-12-31", "2026-01-29"), "ticker": ""},
    ]
    findings = eh.verify_integrity(history_df=pd.DataFrame(rows))
    orph = [f for f in findings if f.check == "orphan_ticker"]
    assert len(orph) == 1
    assert orph[0].affected_rows == 1


def test_verify_integrity_detects_null_source():
    rows = [
        _row("AAPL", "2025-12-31", "2026-01-29", source="zacks"),
        {**_row("MSFT", "2025-12-31", "2026-01-29"), "source": None},
    ]
    findings = eh.verify_integrity(history_df=pd.DataFrame(rows))
    null = [f for f in findings if f.check == "null_source"]
    assert len(null) == 1
    assert null[0].auto_fixable


def test_verify_integrity_detects_rev_dtype_drift():
    # All-None revenue columns -> object dtype after pd.DataFrame().
    rows = [
        {**_row("AAPL", "2025-12-31", "2026-01-29"),
         "estimated_rev": None, "reported_rev": None,
         "surprise_rev": None, "surprise_rev_pct": None},
    ] * 3
    df = pd.DataFrame(rows)
    findings = eh.verify_integrity(history_df=df)
    dr = [f for f in findings if f.check == "rev_column_dtype"]
    assert len(dr) == 1
    assert dr[0].auto_fixable
    assert dr[0].affected_rows == 4  # 4 rev columns affected


def test_fix_integrity_issues_drops_duplicates():
    r = _row("AAPL", "2025-12-31", "2026-01-29", source="zacks")
    df = pd.DataFrame([r, r, r])  # 3 duplicates
    findings = eh.verify_integrity(history_df=df)
    fixed, msgs = eh.fix_integrity_issues(df, findings)
    assert len(fixed) == 1
    assert any("duplicate_pk" in m for m in msgs)


def test_fix_integrity_issues_coerces_rev_dtype():
    rows = [{**_row("AAPL", "2025-12-31", "2026-01-29"),
             "estimated_rev": None, "reported_rev": None,
             "surprise_rev": None, "surprise_rev_pct": None}] * 2
    df = pd.DataFrame(rows)
    findings = eh.verify_integrity(history_df=df)
    fixed, _ = eh.fix_integrity_issues(df, findings)
    # All four rev columns should now be numeric (float-compatible).
    for c in ("estimated_rev", "reported_rev",
              "surprise_rev", "surprise_rev_pct"):
        assert pd.api.types.is_numeric_dtype(fixed[c]), (
            f"{c} should be numeric after fix, got {fixed[c].dtype}"
        )


def test_fix_integrity_issues_skips_non_fixable():
    # Force a non-fixable schema_missing_cols finding by handing in
    # a frame missing required columns.
    df = pd.DataFrame({"ticker": ["AAPL"], "period_ending": [pd.Timestamp("2025-12-31")]})
    findings = eh.verify_integrity(history_df=df)
    fixed, msgs = eh.fix_integrity_issues(df, findings)
    # Frame is preserved (no fixes possible)
    assert len(fixed) == 1
    assert any("schema_missing_cols" in m and "NOT auto-fixable" in m
               for m in msgs)


def test_verify_integrity_ignores_cross_source_gap_fill():
    """Under gap-fill, a ticker with rows from multiple sources covering
    DIFFERENT periods is normal — the integrity check must NOT flag it."""
    df = pd.DataFrame([
        # AAPL: Zacks Q4, Finnhub Q3 (gap-fill — DIFFERENT periods)
        _row("AAPL", "2025-12-01", "2026-01-29", source="zacks"),
        _row("AAPL", "2025-09-01", "2025-10-30", source="finnhub"),
        # MSFT: Finnhub only
        _row("MSFT", "2025-12-01", "2026-01-28", source="finnhub"),
    ])
    findings = eh.verify_integrity(history_df=df)
    slot = [f for f in findings if f.check == "cross_source_slot_overlap"]
    assert slot == [], (
        "Gap-fill across different periods must not raise an overlap "
        "warning — only same-slot multi-source rows do."
    )


def test_verify_integrity_detects_same_slot_cross_source_overlap():
    """Same (ticker, period_ending) carried by two sources is the only
    cross-source overlap that's still a violation — it means write-time
    dedup was bypassed. Auto-fix re-runs dedupe_history per-slot."""
    df = pd.DataFrame([
        # Zacks + Finnhub on the same slot — write-time dedup should
        # have collapsed this to one row, but didn't.
        _row("AAPL", "2025-12-01", "2026-01-29", source="zacks"),
        _row("AAPL", "2025-12-01", "2026-01-29", source="finnhub"),
        # Pure gap-fill — Finnhub on a quarter Zacks doesn't have.
        _row("AAPL", "2025-09-01", "2025-10-30", source="finnhub"),
    ])
    findings = eh.verify_integrity(history_df=df)
    slot = [f for f in findings if f.check == "cross_source_slot_overlap"]
    assert len(slot) == 1
    assert slot[0].affected_rows == 2  # both rows on the overlapping slot
    assert slot[0].auto_fixable is True


def test_fix_integrity_issues_resolves_same_slot_via_priority_dedup():
    """Auto-fix for the same-slot overlap: drop lower-priority sources
    on overlapping slots (zacks > finnhub). Different-period
    gap-fill rows are preserved."""
    df = pd.DataFrame([
        _row("AAPL", "2025-12-01", "2026-01-29", source="zacks"),
        _row("AAPL", "2025-12-01", "2026-01-29", source="finnhub"),  # same slot — drop
        _row("AAPL", "2025-09-01", "2025-10-30", source="finnhub"),  # gap — keep
        _row("MSFT", "2025-12-01", "2026-01-28", source="finnhub"),
    ])
    findings = eh.verify_integrity(history_df=df)
    fixed, msgs = eh.fix_integrity_issues(df, findings)
    # AAPL: 1 Zacks (Q4) + 1 Finnhub (Q3 gap-fill) = 2 rows after fix
    aapl = fixed.loc[fixed["ticker"] == "AAPL"]
    assert len(aapl) == 2
    aapl_q4 = aapl.loc[aapl["period_ending"] == pd.Timestamp("2025-12-01")]
    assert aapl_q4.iloc[0]["source"] == "zacks"
    aapl_q3 = aapl.loc[aapl["period_ending"] == pd.Timestamp("2025-09-01")]
    assert aapl_q3.iloc[0]["source"] == "finnhub"
    # MSFT untouched
    msft = fixed.loc[fixed["ticker"] == "MSFT"]
    assert len(msft) == 1
    assert any("cross_source_slot_overlap" in m for m in msgs)


# ----------------------------------------------------------------------
# Calendar-vs-fiscal phantom-duplicate collapse (finnhub proxy rows)
# ----------------------------------------------------------------------

def test_dedupe_drops_calendar_proxy_covered_by_fiscal_quarter():
    """A finnhub proxy row buckets a non-calendar fiscal quarter into its
    containing calendar quarter, so the same event lands at a different
    period_ending than the finviz fiscal-end row. Both fall in the same
    calendar quarter → the proxy is dropped, finviz (true fiscal end) wins.
    Mirrors the real BBCP case: fiscal-Apr quarter stored by finviz at
    2025-04-01 and by finnhub at 2025-06-01 (both Q2 2025)."""
    rows = [
        {**_row("BBCP", "2025-04-01", "2025-06-05", source="finviz",
                eps_rep=-0.01), "report_date_proxy": False},
        {**_row("BBCP", "2025-06-01", "2025-06-30", source="finnhub",
                eps_rep=-0.01), "report_date_proxy": True},
    ]
    deduped = eh.dedupe_history(pd.DataFrame(rows))
    assert len(deduped) == 1
    win = deduped.iloc[0]
    assert win["source"] == "finviz"
    assert pd.Timestamp(win["period_ending"]) == pd.Timestamp("2025-04-01")
    assert bool(win["report_date_proxy"]) is False


def test_dedupe_drops_NONproxy_finnhub_calendar_duplicate():
    """The key broadening: a finnhub row with proxy=False (a real
    announcement date came from /calendar/earnings) STILL has a
    calendar-normed period_ending, so it can duplicate a finviz fiscal row
    in the same calendar quarter. The collapse keys on source, not the
    proxy flag, so this is dropped too. Mirrors the AMAT/AVGO/CSCO residual
    cases."""
    rows = [
        {**_row("AMAT", "2026-04-01", "2026-05-14", source="finviz",
                eps_rep=2.86), "report_date_proxy": False},
        # finnhub: same event, calendar-normed period_ending, REAL date.
        {**_row("AMAT", "2026-06-01", "2026-05-14", source="finnhub",
                eps_rep=2.86), "report_date_proxy": False},
    ]
    deduped = eh.dedupe_history(pd.DataFrame(rows))
    assert len(deduped) == 1
    assert deduped.iloc[0]["source"] == "finviz"
    assert pd.Timestamp(deduped.iloc[0]["period_ending"]) == pd.Timestamp("2026-04-01")


def test_dedupe_never_drops_fiscal_accurate_stub_quarters():
    """False-collapse guard: two SAME-source (finviz) rows in one calendar
    quarter — a genuine fiscal-year-change stub case — are both kept. The
    collapse only ever drops non-fiscal-accurate (finnhub) rows, so
    finviz/zacks rows are never merged by calendar quarter."""
    rows = [
        {**_row("STUB", "2026-01-01", "2026-02-10", source="finviz",
                eps_rep=0.50), "report_date_proxy": False},   # cal Q1
        {**_row("STUB", "2026-02-01", "2026-03-15", source="finviz",
                eps_rep=0.20), "report_date_proxy": False},   # also cal Q1
    ]
    deduped = eh.dedupe_history(pd.DataFrame(rows))
    assert len(deduped) == 2


def test_dedupe_keeps_calendar_proxy_gap_fill_without_fiscal_cover():
    """A finnhub proxy in a calendar quarter NO higher-priority source
    covers is genuine gap-fill — it must survive."""
    rows = [
        {**_row("BBCP", "2025-04-01", "2025-06-05", source="finviz"),
         "report_date_proxy": False},
        # Q4 2024 — finviz has no row here, so the proxy is the only data.
        {**_row("BBCP", "2024-12-01", "2024-12-31", source="finnhub"),
         "report_date_proxy": True},
    ]
    deduped = eh.dedupe_history(pd.DataFrame(rows))
    assert len(deduped) == 2


def test_dedupe_proxy_not_collapsed_into_adjacent_calendar_quarter():
    """False-collapse guard: a proxy in a DIFFERENT calendar quarter than
    the finviz row is never merged — only same-calendar-quarter cover
    drops a proxy."""
    rows = [
        {**_row("BBCP", "2025-04-01", "2025-06-05", source="finviz"),
         "report_date_proxy": False},                    # Q2 2025
        {**_row("BBCP", "2025-09-01", "2025-09-30", source="finnhub"),
         "report_date_proxy": True},                     # Q3 2025 — distinct
    ]
    deduped = eh.dedupe_history(pd.DataFrame(rows))
    assert len(deduped) == 2


def test_dedupe_calendar_year_company_collapses_via_exact_key():
    """Calendar-year filers already agree on period_ending across sources,
    so the exact-slot dedup collapses them (finviz wins) before the
    calendar-quarter stage runs — behavior unchanged for them."""
    rows = [
        {**_row("AAPL", "2025-12-01", "2026-01-29", source="finviz",
                eps_rep=2.11), "report_date_proxy": False},
        {**_row("AAPL", "2025-12-01", "2025-12-31", source="finnhub",
                eps_rep=2.05), "report_date_proxy": True},
    ]
    deduped = eh.dedupe_history(pd.DataFrame(rows))
    assert len(deduped) == 1
    assert deduped.iloc[0]["source"] == "finviz"


def test_dedupe_bbcp_calendar_proxies_collapse_to_fiscal_quarters():
    """Realistic BBCP slice: 4 finviz fiscal quarters + 4 finnhub calendar
    proxies for the SAME events (shifted +2 months) collapse to 4 rows,
    all finviz, none proxy."""
    finviz = [
        ("2025-04-01", "2025-06-05", -0.01),
        ("2025-07-01", "2025-09-04", 0.07),
        ("2025-10-01", "2026-01-13", 0.09),
        ("2026-01-01", "2026-03-10", -0.06),
    ]
    finnhub = [
        ("2025-06-01", "2025-06-30", -0.01),
        ("2025-09-01", "2025-09-30", 0.07),
        ("2025-12-01", "2025-12-31", 0.09),
        ("2026-03-01", "2026-03-31", -0.06),
    ]
    rows = [
        {**_row("BBCP", p, r, source="finviz", eps_rep=e),
         "report_date_proxy": False}
        for p, r, e in finviz
    ] + [
        {**_row("BBCP", p, r, source="finnhub", eps_rep=e),
         "report_date_proxy": True}
        for p, r, e in finnhub
    ]
    deduped = eh.dedupe_history(pd.DataFrame(rows))
    assert len(deduped) == 4
    assert set(deduped["source"]) == {"finviz"}
    assert not deduped["report_date_proxy"].astype(bool).any()


def test_get_ticker_history_drops_calendar_proxy_duplicate():
    """Read-side path (get_ticker_history → dedupe_history) also collapses
    the phantom proxy so consumers never see the duplicate quarter."""
    df = pd.DataFrame([
        {**_row("BBCP", "2025-04-01", "2025-06-05", source="finviz"),
         "report_date_proxy": False},
        {**_row("BBCP", "2025-06-01", "2025-06-30", source="finnhub"),
         "report_date_proxy": True},
    ])
    sub = eh.get_ticker_history("BBCP", df)
    assert len(sub) == 1
    assert sub.iloc[0]["source"] == "finviz"


def test_verify_integrity_detects_calendar_quarter_overlap():
    """A proxy row covered by a non-proxy row in the same calendar quarter
    is flagged as calendar_quarter_overlap (auto-fixable)."""
    df = pd.DataFrame([
        {**_row("BBCP", "2025-04-01", "2025-06-05", source="finviz"),
         "report_date_proxy": False},
        {**_row("BBCP", "2025-06-01", "2025-06-30", source="finnhub"),
         "report_date_proxy": True},
    ])
    findings = eh.verify_integrity(history_df=df)
    pc = [f for f in findings if f.check == "calendar_quarter_overlap"]
    assert len(pc) == 1
    assert pc[0].affected_rows == 1
    assert pc[0].auto_fixable is True
    # Different period_endings, so the same-slot check must NOT fire here.
    assert not [f for f in findings if f.check == "cross_source_slot_overlap"]


def test_verify_integrity_ignores_proxy_gap_fill():
    """A proxy row with no non-proxy cover in its calendar quarter is
    legitimate gap-fill — not flagged."""
    df = pd.DataFrame([
        {**_row("BBCP", "2025-04-01", "2025-06-05", source="finviz"),
         "report_date_proxy": False},
        {**_row("BBCP", "2024-12-01", "2024-12-31", source="finnhub"),
         "report_date_proxy": True},
    ])
    findings = eh.verify_integrity(history_df=df)
    assert not [f for f in findings if f.check == "calendar_quarter_overlap"]


def test_fix_integrity_resolves_calendar_quarter_overlap():
    """Auto-fix drops the covered proxy and keeps the gap-fill proxy."""
    df = pd.DataFrame([
        {**_row("BBCP", "2025-04-01", "2025-06-05", source="finviz"),
         "report_date_proxy": False},
        {**_row("BBCP", "2025-06-01", "2025-06-30", source="finnhub"),
         "report_date_proxy": True},                     # covered — drop
        {**_row("BBCP", "2024-12-01", "2024-12-31", source="finnhub"),
         "report_date_proxy": True},                     # gap-fill — keep
    ])
    findings = eh.verify_integrity(history_df=df)
    fixed, msgs = eh.fix_integrity_issues(df, findings)
    bbcp = fixed.loc[fixed["ticker"] == "BBCP"]
    assert len(bbcp) == 2
    periods = {pd.Timestamp(p) for p in bbcp["period_ending"]}
    assert pd.Timestamp("2025-04-01") in periods
    assert pd.Timestamp("2025-06-01") not in periods
    assert pd.Timestamp("2024-12-01") in periods
    assert any("calendar_quarter_overlap" in m for m in msgs)


def test_migrate_calendar_dedup_cleans_disk_once(tmp_parquets):
    """One-time on-disk migration drops covered proxies, keeps gap-fill,
    creates the sentinel, and no-ops on the second call."""
    rows = [
        {**_row("BBCP", "2025-04-01", "2025-06-05", source="finviz",
                eps_rep=-0.01), "report_date_proxy": False},
        {**_row("BBCP", "2025-06-01", "2025-06-30", source="finnhub",
                eps_rep=-0.01), "report_date_proxy": True},   # covered — drop
        {**_row("BBCP", "2024-12-01", "2024-12-31", source="finnhub",
                eps_rep=0.05), "report_date_proxy": True},    # gap-fill — keep
    ]
    # Write the phantom-dup state to disk without dedup so the migration
    # has something to clean.
    eh.save_earnings_history(pd.DataFrame(rows), sort=False, dedup=False)

    before, after = eh.migrate_calendar_dedup()
    assert (before, after) == (3, 2)

    df = eh.load_earnings_history()
    bbcp = df.loc[df["ticker"] == "BBCP"]
    periods = {pd.Timestamp(p) for p in bbcp["period_ending"]}
    assert pd.Timestamp("2025-04-01") in periods
    assert pd.Timestamp("2025-06-01") not in periods
    assert pd.Timestamp("2024-12-01") in periods

    # Sentinel created → idempotent.
    assert eh._calendar_migration_flag_path().exists()
    assert eh.migrate_calendar_dedup() == (0, 0)


def test_migrate_backfill_finviz_recovers_old_quarters_from_raw(tmp_parquets, monkeypatch):
    """The finviz-from-raw backfill (after raising the cap to 10y) replays
    raw rows through the production converter: quarters within the cap that
    aren't yet in the consumer parquet are recovered, quarters older than
    the cap stay out, existing non-finviz rows are preserved, and the run
    is idempotent."""
    from trade_scanner_fh import config, earnings_raw

    raw_root = tmp_parquets / "earnings_raw"
    monkeypatch.setattr(config, "RAW_EARNINGS_DIR", raw_root)
    # exist_ok: the tmp_parquets fixture itself now redirects
    # RAW_EARNINGS_DIR here and pre-creates the per-source folders
    # (conftest trap fix) — this inline setup stays as belt-and-braces.
    raw_root.mkdir(exist_ok=True)
    for src in config.RAW_SOURCES:
        (raw_root / src).mkdir(exist_ok=True)

    today = pd.Timestamp.today().normalize()
    mid = (today - pd.DateOffset(years=7)).replace(day=1)            # within 10y
    older = (today - pd.DateOffset(years=config.EARNINGS_HISTORY_YEARS + 3)).replace(day=1)
    recent = (today - pd.DateOffset(years=1)).replace(day=1)
    upcoming = (today - pd.DateOffset(days=10)).replace(day=1)       # just-reported, NaN actual
    fe = lambda pe: (pe + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")
    rd = lambda pe: (pe + pd.Timedelta(days=40)).strftime("%Y-%m-%dT16:30:00")
    raw_rows = [
        {"symbol": "BBCP", "fiscal_period": "mid", "fiscal_end_date": fe(mid),
         "earnings_date": rd(mid), "eps_actual": 0.11, "eps_estimate": 0.10,
         "sales_actual": 90.0, "sales_estimate": 88.0},
        {"symbol": "BBCP", "fiscal_period": "old", "fiscal_end_date": fe(older),
         "earnings_date": rd(older), "eps_actual": 0.05, "eps_estimate": 0.04,
         "sales_actual": 50.0, "sales_estimate": 49.0},
        {"symbol": "BBCP", "fiscal_period": "recent", "fiscal_end_date": fe(recent),
         "earnings_date": rd(recent), "eps_actual": -0.06, "eps_estimate": -0.01,
         "sales_actual": 86.0, "sales_estimate": 91.0},
        # Forward-estimate row: a REAL earnings_date but NaN actual (the
        # just-reported/upcoming quarter). Stored as NaN in raw, NOT None —
        # must still be filtered (regression: NaN-actual leaked through the
        # `is None` guard and wrote a reported_eps=NaN row).
        {"symbol": "BBCP", "fiscal_period": "fwd", "fiscal_end_date": fe(upcoming),
         "earnings_date": rd(upcoming), "eps_actual": float("nan"),
         "eps_estimate": 0.05, "sales_actual": float("nan"), "sales_estimate": 95.0},
    ]
    earnings_raw.append_finviz_rows(raw_rows, earnings_raw.new_run_id())

    # Seed consumer parquet with the recent finviz quarter + an unrelated
    # zacks quarter that must survive the backfill.
    zk = (today - pd.DateOffset(years=6)).replace(day=1)
    eh.save_earnings_history(pd.DataFrame([
        {**_row("BBCP", str(recent.date()), str((recent + pd.Timedelta(days=40)).date()),
                source="finviz"), "report_date_proxy": False},
        {**_row("BBCP", str(zk.date()), str((zk + pd.Timedelta(days=40)).date()),
                source="zacks"), "report_date_proxy": False},
    ]), dedup=False, sort=False)

    before, after = eh.migrate_backfill_finviz_history_from_raw()
    assert after > before

    df = eh.load_earnings_history()
    periods = {pd.Timestamp(p) for p in df.loc[df["ticker"] == "BBCP", "period_ending"]}
    assert mid in periods               # recovered from raw (within cap)
    assert older not in periods         # older than cap → not recovered
    assert recent in periods            # already present → kept
    assert zk in periods                # zacks row preserved
    assert upcoming not in periods      # NaN-actual forward estimate → filtered
    # Idempotent (sentinel gates a second run).
    assert eh._finviz_backfill_flag_path().exists()
    assert eh.migrate_backfill_finviz_history_from_raw() == (0, 0)


# ----------------------------------------------------------------------
# EPS sanitization — reverse-split artifact filtering
# ----------------------------------------------------------------------

def _ohlcv(dirpath, ticker, close, *, start="2021-01-04", periods=520):
    """Write a cache-shaped OHLCV parquet: DatetimeIndex, flat close."""
    idx = pd.bdate_range(start, periods=periods)
    pd.DataFrame({"Close": [close] * len(idx)}, index=idx).to_parquet(
        dirpath / f"{ticker}.parquet")


def test_implausible_eps_mask_absolute_and_price_relative():
    df = pd.DataFrame([
        _row("ADTX", "2021-01-01", "2021-03-01", source="finviz", eps_rep=-4.3e11),
        _row("CETX", "2022-03-01", "2022-05-15", source="finviz", eps_rep=615.0),
        _row("NVR",  "2022-03-01", "2022-04-20", source="finviz", eps_rep=120.0),
        _row("AAPL", "2022-03-01", "2022-04-28", source="finviz", eps_rep=1.5),
    ])
    # Absolute cap only (no price): only the impossible-magnitude ADTX row.
    assert list(eh._implausible_eps_mask(df)) == [True, False, False, False]
    # Price-relative: CETX $615 on a $0.50 stock is implausible; NVR $120 on
    # a $5,000 stock is legit; AAPL normal.
    prices = {"ADTX": 1.0, "CETX": 0.5, "NVR": 5000.0, "AAPL": 180.0}
    assert list(eh._implausible_eps_mask(df, price_by_ticker=prices)) == \
        [True, True, False, False]


def test_save_flags_absurd_eps_but_keeps_the_value(tmp_parquets):
    """The canonical write path STAMPS eps_flag on an unverifiable magnitude.
    It must not empty the row: nulling destroyed correct data, which is the
    whole reason this changed."""
    eh.save_earnings_history(pd.DataFrame([
        _row("ADTX", "2021-01-01", "2021-03-01", source="finviz",
             eps_rep=-4.3e11, eps_est=-4.0e11, eps_surp=-3e10, eps_pct=12.0,
             rev_rep=5.0),
        _row("AAPL", "2022-03-01", "2022-04-28", source="finviz", eps_rep=1.5),
    ]))
    df = eh.load_earnings_history()
    adtx = df.loc[df["ticker"] == "ADTX"].iloc[0]
    assert adtx["eps_flag"] == eh.EPS_FLAG_ABS
    assert adtx["reported_eps"] == -4.3e11      # VALUE KEPT
    assert adtx["estimated_eps"] == -4.0e11
    assert adtx["reported_rev"] == 5.0
    assert pd.notna(adtx["report_date"])
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    assert aapl["reported_eps"] == 1.5
    assert aapl["eps_flag"] == eh.EPS_FLAG_NONE


def test_eps_judged_against_its_own_period_not_todays_price(
        tmp_parquets, monkeypatch):
    """THE BUG THIS REPLACES. A restated historical EPS carries the cumulative
    split factor; so does the price for that same quarter. Judging it against
    TODAY's post-split price is a basis mismatch that condemns correct data.

    ABTC's real shape: -7800 EPS for a 2019 quarter whose restated close was
    ~$96,000, against a present-day close of $7.12.
    """
    ohlcv = tmp_parquets / "ohlcv"
    ohlcv.mkdir()
    monkeypatch.setattr(eh.config, "PARQUET_DIR", ohlcv)

    idx = pd.bdate_range("2019-01-01", periods=1500)
    close = pd.Series(96_000.0, index=idx)
    close.iloc[-260:] = 7.12          # post-split era
    pd.DataFrame({"Close": close}).to_parquet(ohlcv / "ABTC.parquet")

    df = pd.DataFrame([
        _row("ABTC", "2019-09-30", "2019-11-15", source="finviz", eps_rep=-7800.0),
    ])
    out = eh.sanitize_eps_artifacts(df)
    assert out.iloc[0]["eps_flag"] == eh.EPS_FLAG_NONE   # 7800 / 96000 -> sane
    assert out.iloc[0]["reported_eps"] == -7800.0

    # Against today's close the same row scores 7800/7.12 = 1096x and would
    # have been condemned — the behaviour being removed.
    assert eh._implausible_eps_mask(df, price_by_ticker={"ABTC": 7.12}).iloc[0]


def test_high_priced_stock_with_huge_eps_is_not_flagged(
        tmp_parquets, monkeypatch):
    """BRK-A earns thousands per share on a ~$700k share. Large EPS is not
    evidence of anything on its own."""
    ohlcv = tmp_parquets / "ohlcv"
    ohlcv.mkdir()
    monkeypatch.setattr(eh.config, "PARQUET_DIR", ohlcv)
    _ohlcv(ohlcv, "BRK-A", 700_000.0, start="2024-01-01", periods=600)

    out = eh.sanitize_eps_artifacts(pd.DataFrame([
        _row("BRK-A", "2025-06-30", "2025-08-02", source="finviz",
             eps_rep=9030.0),
    ]))
    assert out.iloc[0]["eps_flag"] == eh.EPS_FLAG_NONE
    assert out.iloc[0]["reported_eps"] == 9030.0


def test_unpriced_row_within_the_cap_is_left_unjudged(tmp_parquets, monkeypatch):
    """Earnings run far deeper than OHLCV. A quarter with no contemporaneous
    close must read as "cannot verify", not "implausible"."""
    ohlcv = tmp_parquets / "ohlcv"
    ohlcv.mkdir()
    monkeypatch.setattr(eh.config, "PARQUET_DIR", ohlcv)
    _ohlcv(ohlcv, "OLD", 5.0, start="2024-01-01", periods=300)

    out = eh.sanitize_eps_artifacts(pd.DataFrame([
        _row("OLD", "2005-03-31", "2005-05-02", source="finviz", eps_rep=615.0),
    ]))
    assert out.iloc[0]["eps_flag"] == eh.EPS_FLAG_NONE
    assert out.iloc[0]["reported_eps"] == 615.0


def test_migrate_sanitize_absurd_eps_flags_without_destroying(
        tmp_parquets, monkeypatch):
    """The whole-store pass flags a genuine artifact, spares a legit
    high-priced stock, alters no value, and is re-runnable."""
    ohlcv = tmp_parquets / "ohlcv"
    ohlcv.mkdir()
    monkeypatch.setattr(eh.config, "PARQUET_DIR", ohlcv)
    _ohlcv(ohlcv, "CETX", 0.5, start="2022-01-03", periods=300)
    _ohlcv(ohlcv, "NVR", 5000.0, start="2022-01-03", periods=300)

    eh.save_earnings_history(pd.DataFrame([
        _row("CETX", "2022-03-31", "2022-05-15", source="finviz", eps_rep=615.0),
        _row("NVR",  "2022-03-31", "2022-04-20", source="finviz", eps_rep=120.0),
    ]), dedup=False, sort=False)

    flagged, n_priced = eh.migrate_sanitize_absurd_eps()
    assert flagged == 1
    df = eh.load_earnings_history()
    cetx = df.loc[df["ticker"] == "CETX"].iloc[0]
    nvr = df.loc[df["ticker"] == "NVR"].iloc[0]
    assert cetx["eps_flag"] == eh.EPS_FLAG_PRICE   # 615 > 10*0.5
    assert cetx["reported_eps"] == 615.0           # VALUE KEPT
    assert nvr["eps_flag"] == eh.EPS_FLAG_NONE     # 120 < 10*5000
    assert nvr["reported_eps"] == 120.0
    # Sentinel is written as a last-run marker but no longer GATES the pass:
    # it is recurring now, so a second call reaches the same verdict rather
    # than short-circuiting to (0, 0).
    assert eh._eps_sanitize_flag_path().exists()
    assert eh.migrate_sanitize_absurd_eps() == (flagged, n_priced)


def test_migrate_sanitize_is_idempotent_under_force(tmp_parquets, monkeypatch):
    """Flagging no longer has to be one-shot. Re-running must reach the same
    answer rather than compounding — the old nulling version could not be
    re-run at all, which is how a later backfill silently undid it."""
    ohlcv = tmp_parquets / "ohlcv"
    ohlcv.mkdir()
    monkeypatch.setattr(eh.config, "PARQUET_DIR", ohlcv)
    _ohlcv(ohlcv, "CETX", 0.5, start="2022-01-03", periods=300)

    eh.save_earnings_history(pd.DataFrame([
        _row("CETX", "2022-03-31", "2022-05-15", source="finviz", eps_rep=615.0),
    ]), dedup=False, sort=False)

    first = eh.migrate_sanitize_absurd_eps(force=True)
    again = eh.migrate_sanitize_absurd_eps(force=True)
    assert first[0] == again[0] == 1
    df = eh.load_earnings_history()
    assert df.iloc[0]["reported_eps"] == 615.0
    assert df.iloc[0]["eps_flag"] == eh.EPS_FLAG_PRICE


def test_sanitize_eps_artifacts_ingest_guard(tmp_parquets, monkeypatch):
    """The ingest stamp: price-relative where a contemporaneous close exists,
    absolute cap where it does not, nothing emptied."""
    ohlcv = tmp_parquets / "ohlcv"
    ohlcv.mkdir()
    monkeypatch.setattr(eh.config, "PARQUET_DIR", ohlcv)
    _ohlcv(ohlcv, "CETX", 0.5, start="2022-01-03", periods=300)
    _ohlcv(ohlcv, "NVR", 5000.0, start="2022-01-03", periods=300)

    df = pd.DataFrame([
        _row("CETX", "2022-03-31", "2022-05-15", source="finviz", eps_rep=615.0),
        _row("NVR",  "2022-03-31", "2022-04-20", source="finviz", eps_rep=120.0),
        _row("AAPL", "2022-03-31", "2022-04-28", source="finviz", eps_rep=1.5),
        _row("ADTX", "2021-01-31", "2021-03-01", source="finnhub", eps_rep=-4.3e11),
    ])
    out = eh.sanitize_eps_artifacts(df)
    by = {r.ticker: r for r in out.itertuples(index=False)}
    assert by["CETX"].eps_flag == eh.EPS_FLAG_PRICE
    assert by["CETX"].reported_eps == 615.0        # kept
    assert by["NVR"].eps_flag == eh.EPS_FLAG_NONE
    assert by["AAPL"].eps_flag == eh.EPS_FLAG_NONE  # <20, never a candidate
    assert by["ADTX"].eps_flag == eh.EPS_FLAG_ABS   # no OHLCV -> absolute cap
    assert by["ADTX"].reported_eps == -4.3e11       # kept


def test_odd_ohlcv_index_does_not_break_the_ingest_path(
        tmp_parquets, monkeypatch):
    """A cache file with a non-datetime index cannot answer "price on this
    date". It must leave the row unpriced, not take the flush down."""
    ohlcv = tmp_parquets / "ohlcv"
    ohlcv.mkdir()
    monkeypatch.setattr(eh.config, "PARQUET_DIR", ohlcv)
    pd.DataFrame({"Close": [1.0, 2.0]}).to_parquet(ohlcv / "ODD.parquet")

    out = eh.sanitize_eps_artifacts(pd.DataFrame([
        _row("ODD", "2022-03-31", "2022-05-15", source="finviz", eps_rep=615.0),
    ]))
    assert out.iloc[0]["eps_flag"] == eh.EPS_FLAG_NONE
    assert out.iloc[0]["reported_eps"] == 615.0


def test_precise_price_verdict_survives_the_canonical_write(
        tmp_parquets, monkeypatch):
    """save_earnings_history applies only the coarse absolute rule. It must not
    overwrite a "price" verdict the ingest guard already reached."""
    ohlcv = tmp_parquets / "ohlcv"
    ohlcv.mkdir()
    monkeypatch.setattr(eh.config, "PARQUET_DIR", ohlcv)
    _ohlcv(ohlcv, "CETX", 0.5, start="2022-01-03", periods=300)

    stamped = eh.sanitize_eps_artifacts(pd.DataFrame([
        _row("CETX", "2022-03-31", "2022-05-15", source="finviz", eps_rep=615.0),
    ]))
    eh.save_earnings_history(stamped, dedup=False, sort=False)
    df = eh.load_earnings_history()
    assert df.iloc[0]["eps_flag"] == eh.EPS_FLAG_PRICE


def test_eps_pass_is_recurring_so_a_later_writer_cannot_win(
        tmp_parquets, monkeypatch):
    """THE ORDERING HAZARD. Two sentinel-gated one-shots, where one's output is
    the other's input, only stay ordered within a single launch — and the
    backfill can defer to a later one. Whatever a subsequent writer reintroduces
    must be re-judged on the next pass, not permanently.
    """
    ohlcv = tmp_parquets / "ohlcv"
    ohlcv.mkdir()
    monkeypatch.setattr(eh.config, "PARQUET_DIR", ohlcv)
    _ohlcv(ohlcv, "CETX", 0.5, start="2022-01-03", periods=300)

    eh.save_earnings_history(pd.DataFrame([
        _row("CETX", "2022-03-31", "2022-05-15", source="finviz", eps_rep=615.0),
    ]), dedup=False, sort=False)
    assert eh.migrate_sanitize_absurd_eps()[0] == 1

    # A later writer reinstates the row with no verdict on it at all.
    stale = eh.load_earnings_history()
    stale["eps_flag"] = eh.EPS_FLAG_NONE
    eh.save_earnings_history(stale, dedup=False, sort=False)
    assert eh.load_earnings_history().iloc[0]["eps_flag"] == eh.EPS_FLAG_NONE

    # The pass is recurring, so the next launch re-judges it.
    assert eh.migrate_sanitize_absurd_eps()[0] == 1
    assert eh.load_earnings_history().iloc[0]["eps_flag"] == eh.EPS_FLAG_PRICE


def test_eps_pass_clears_a_verdict_that_no_longer_holds(
        tmp_parquets, monkeypatch):
    """A stale flag must be able to CLEAR. Keying the write on "anything is
    flagged" would strand it forever."""
    ohlcv = tmp_parquets / "ohlcv"
    ohlcv.mkdir()
    monkeypatch.setattr(eh.config, "PARQUET_DIR", ohlcv)
    _ohlcv(ohlcv, "AAA", 0.5, start="2022-01-03", periods=300)

    eh.save_earnings_history(pd.DataFrame([
        _row("AAA", "2022-03-31", "2022-05-15", source="finviz", eps_rep=615.0),
    ]), dedup=False, sort=False)
    assert eh.migrate_sanitize_absurd_eps()[0] == 1
    assert eh.load_earnings_history().iloc[0]["eps_flag"] == eh.EPS_FLAG_PRICE

    # Re-price the ticker so the row is comfortably plausible.
    _ohlcv(ohlcv, "AAA", 5000.0, start="2022-01-03", periods=300)
    assert eh.migrate_sanitize_absurd_eps()[0] == 0
    assert eh.load_earnings_history().iloc[0]["eps_flag"] == eh.EPS_FLAG_NONE


def test_backfill_from_raw_is_stamped_like_a_live_fill(
        tmp_parquets, monkeypatch):
    """The replay path used to bypass the guard entirely — that is how ABTC's
    artifacts returned after the cleanup pass had already run."""
    import inspect
    src = inspect.getsource(eh.migrate_backfill_finviz_history_from_raw)
    assert "sanitize_eps_artifacts" in src, (
        "the raw replay must stamp its rows like any other ingest path"
    )


def test_legacy_store_without_the_column_loads_unflagged(tmp_parquets):
    """A parquet written before eps_flag existed is UNJUDGED, not implausible."""
    eh.save_earnings_history(pd.DataFrame([
        _row("AAPL", "2022-03-31", "2022-04-28", source="finviz", eps_rep=1.5),
    ]), dedup=False, sort=False)
    raw = pd.read_parquet(eh.config.EARNINGS_HISTORY_PARQUET)
    raw = raw.drop(columns=["eps_flag"])
    raw.to_parquet(eh.config.EARNINGS_HISTORY_PARQUET, index=False)

    df = eh.load_earnings_history()
    assert "eps_flag" in df.columns
    assert df.iloc[0]["eps_flag"] == eh.EPS_FLAG_NONE


# ----------------------------------------------------------------------
# get_ticker_history / get_most_recent_quarter
# ----------------------------------------------------------------------

def _hist_df():
    return pd.DataFrame([
        _row("AAPL", "2025-12-01", "2026-01-29"),
        _row("AAPL", "2025-09-01", "2025-10-30"),
        _row("AAPL", "2025-06-01", "2025-07-31"),
        _row("MSFT", "2025-12-01", "2026-01-28"),
    ])


def test_get_ticker_history_returns_sorted_desc():
    sub = eh.get_ticker_history("AAPL", _hist_df())
    assert len(sub) == 3
    periods = list(sub["period_ending"])
    assert periods == sorted(periods, reverse=True)


def test_get_ticker_history_unknown_ticker_is_empty():
    sub = eh.get_ticker_history("NOPE", _hist_df())
    assert sub.empty


def test_get_ticker_history_handles_none():
    assert eh.get_ticker_history("AAPL", None).empty


def test_get_most_recent_quarter():
    row = eh.get_most_recent_quarter("AAPL", _hist_df())
    assert row is not None
    assert row["period_ending"] == pd.Timestamp("2025-12-01")


def test_get_most_recent_quarter_unknown_returns_none():
    assert eh.get_most_recent_quarter("ZZZZZ", _hist_df()) is None
    assert eh.get_most_recent_quarter("AAPL", None) is None


# ----------------------------------------------------------------------
# compute_consecutive_beats — every edge case from spec §9.1
# ----------------------------------------------------------------------

def _beat_history(surprise_pcts: list[float]) -> pd.DataFrame:
    """Build a synthetic ticker history with one quarterly cadence and
    the given surprise %s, newest-first."""
    base = pd.Timestamp("2026-01-01")
    rows = []
    for i, sp in enumerate(surprise_pcts):
        period = base - pd.DateOffset(months=3 * i)
        report = period + pd.DateOffset(months=1)
        rows.append({
            "ticker": "T",
            "period_ending": period,
            "report_date": report,
            "surprise_eps_pct": sp,
            "surprise_rev_pct": sp,
        })
    return pd.DataFrame(rows)


def test_beats_all_positive_returns_full_count():
    df = _beat_history([5, 4, 3, 2, 1])
    assert eh.compute_consecutive_beats(df, "eps", 0.0) == 5


def test_beats_most_recent_miss_returns_zero():
    df = _beat_history([-1, 5, 5, 5])
    assert eh.compute_consecutive_beats(df, "eps", 0.0) == 0


def test_beats_strict_gt_zero_at_threshold_does_not_count():
    """Spec §9.1: surprise = 0 with threshold = 0 does NOT count (strict >)."""
    df = _beat_history([0.0, 5, 5])
    assert eh.compute_consecutive_beats(df, "eps", 0.0) == 0


def test_beats_higher_threshold():
    """threshold = 1.0 means only surprises > 1% count."""
    df = _beat_history([5, 0.5, 5])
    # Q-1 surprise=5% > 1% ✓ (count=1)
    # Q-2 surprise=0.5% > 1%? No → break
    assert eh.compute_consecutive_beats(df, "eps", 1.0) == 1


def test_beats_nan_breaks_streak():
    df = _beat_history([5, 4, float("nan"), 5, 5])
    assert eh.compute_consecutive_beats(df, "eps", 0.0) == 2


def test_beats_missing_quarter_breaks_streak():
    """Q-1 present, Q-2 missing, Q-3 present → streak breaks at Q-1.
    Per spec §9.1 a 4+ month gap between consecutive period_endings
    is treated as a missing quarter."""
    rows = [
        {"ticker": "T", "period_ending": pd.Timestamp("2025-12-01"),
         "report_date": pd.Timestamp("2026-01-29"), "surprise_eps_pct": 5.0},
        # Skipping 2025-09-01 (Q-2)
        {"ticker": "T", "period_ending": pd.Timestamp("2025-03-01"),
         "report_date": pd.Timestamp("2025-04-29"), "surprise_eps_pct": 5.0},
    ]
    df = pd.DataFrame(rows)
    assert eh.compute_consecutive_beats(df, "eps", 0.0) == 1


def test_beats_late_announcement_does_not_break_streak():
    """BUG-9 regression: a ticker beats every quarter on a normal
    fiscal cadence but delays one announcement by 6+ weeks. Under the
    old report_date-based cadence detection, the > 135-day report_date
    gap would falsely truncate the streak. Under period_ending-based
    cadence the underlying quarterly cycle is intact, so all 5
    quarters count.

    Concrete numbers:
      - period_ending: 2024-03-31, 2023-12-31, 2023-09-30, 2023-06-30, 2023-03-31
        (all ~91-day gaps — normal cadence)
      - report_date:  2024-04-25, 2024-01-28, 2024-01-15, 2023-07-25, 2023-04-30
        (the 2024-01-15 announcement reports the 2023-09-30 quarter, but
        was delayed to land alongside the 2023-12-31 announcement —
        producing a 175-day gap from the 2023-07-25 report.)
    """
    rows = [
        # newest → oldest, all beats
        {"ticker": "T", "period_ending": pd.Timestamp("2024-03-31"),
         "report_date": pd.Timestamp("2024-04-25"), "surprise_eps_pct": 5.0},
        {"ticker": "T", "period_ending": pd.Timestamp("2023-12-31"),
         "report_date": pd.Timestamp("2024-01-28"), "surprise_eps_pct": 4.0},
        {"ticker": "T", "period_ending": pd.Timestamp("2023-09-30"),
         "report_date": pd.Timestamp("2024-01-15"),  # LATE — delayed 3.5 months
         "surprise_eps_pct": 3.0},
        {"ticker": "T", "period_ending": pd.Timestamp("2023-06-30"),
         "report_date": pd.Timestamp("2023-07-25"), "surprise_eps_pct": 2.0},
        {"ticker": "T", "period_ending": pd.Timestamp("2023-03-31"),
         "report_date": pd.Timestamp("2023-04-30"), "surprise_eps_pct": 1.0},
    ]
    df = pd.DataFrame(rows)
    # All 5 quarters beat AND the period_ending cadence is intact
    # (~91-day gaps). The streak must be 5.
    assert eh.compute_consecutive_beats(df, "eps", 0.0) == 5


def test_beats_period_ending_gap_still_breaks_streak():
    """Conversely: when the period_ending genuinely skips a quarter
    (Q-2's period_ending is missing), the streak breaks even if
    report_dates are close together. Confirms cadence detection
    actually triggers on real quarter gaps."""
    rows = [
        {"ticker": "T", "period_ending": pd.Timestamp("2024-03-31"),
         "report_date": pd.Timestamp("2024-04-25"), "surprise_eps_pct": 5.0},
        # 6-month gap in period_ending (skipping 2023-12-31 + 2023-09-30)
        {"ticker": "T", "period_ending": pd.Timestamp("2023-06-30"),
         "report_date": pd.Timestamp("2024-04-20"),  # report_date close to row 0
         "surprise_eps_pct": 4.0},
    ]
    df = pd.DataFrame(rows)
    # Q-1 beats, Q-2's period_ending is 9 months from Q-1 (way past 135d) → break
    assert eh.compute_consecutive_beats(df, "eps", 0.0) == 1


def test_beats_legacy_history_without_period_ending_falls_back_to_report_date():
    """Defensive: a synthetic / legacy frame missing the period_ending
    column should fall back to report_date for cadence detection
    rather than crashing."""
    rows = [
        {"ticker": "T",
         "report_date": pd.Timestamp("2024-04-25"), "surprise_eps_pct": 5.0},
        {"ticker": "T",
         "report_date": pd.Timestamp("2024-01-28"), "surprise_eps_pct": 4.0},
    ]
    df = pd.DataFrame(rows)
    # Should not raise; both rows are beats with ~88-day report_date gap
    assert eh.compute_consecutive_beats(df, "eps", 0.0) == 2


def test_beats_empty_history_returns_zero():
    assert eh.compute_consecutive_beats(pd.DataFrame(), "eps", 0.0) == 0
    assert eh.compute_consecutive_beats(None, "eps", 0.0) == 0


def test_beats_metric_rev_uses_rev_column():
    """surprise_rev_pct drives the count when metric='rev'."""
    rows = [
        {"ticker": "T", "period_ending": pd.Timestamp("2025-12-01"),
         "report_date": pd.Timestamp("2026-01-29"),
         "surprise_eps_pct": -5.0, "surprise_rev_pct": 5.0},
        {"ticker": "T", "period_ending": pd.Timestamp("2025-09-01"),
         "report_date": pd.Timestamp("2025-10-30"),
         "surprise_eps_pct": -10.0, "surprise_rev_pct": 3.0},
    ]
    df = pd.DataFrame(rows)
    assert eh.compute_consecutive_beats(df, "eps", 0.0) == 0
    assert eh.compute_consecutive_beats(df, "rev", 0.0) == 2


def test_beats_metric_unknown_returns_zero():
    df = _beat_history([5, 5, 5])
    assert eh.compute_consecutive_beats(df, "garbage", 0.0) == 0


# ----------------------------------------------------------------------
# Reconciliation of earnings_dates.parquet from new history rows
# ----------------------------------------------------------------------

def test_update_earnings_dates_writes_last_and_next(tmp_parquets):
    today = pd.Timestamp("2026-04-30")
    history = pd.DataFrame([
        # AAPL: most recent past = 2026-01-29; future = none
        _row("AAPL", "2025-12-01", "2026-01-29"),
        _row("AAPL", "2025-09-01", "2025-10-30"),
        # NEW_TICKER: future date present
        _row("NEW", "2026-03-01", "2026-05-15"),
    ])
    eh._update_earnings_dates_for_tickers(["AAPL", "NEW"], history, today=today)

    from trade_scanner_fh.earnings_cache import load_earnings_cache
    dates_df = load_earnings_cache()
    assert dates_df is not None
    aapl = dates_df.loc[dates_df["ticker"] == "AAPL"].iloc[0]
    assert aapl["last_earnings"] == pd.Timestamp("2026-01-29")
    assert pd.isna(aapl["next_earnings"])

    new = dates_df.loc[dates_df["ticker"] == "NEW"].iloc[0]
    assert pd.isna(new["last_earnings"])
    assert new["next_earnings"] == pd.Timestamp("2026-05-15")


def test_update_earnings_dates_preserves_other_tickers(tmp_parquets):
    """Reconciling X must not touch Y's row."""
    from trade_scanner_fh.earnings_cache import save_earnings_cache, load_earnings_cache
    seed = pd.DataFrame([{
        "ticker": "Y", "last_earnings": pd.Timestamp("2026-01-15"),
        "next_earnings": pd.Timestamp("2026-04-15"),
        "updated_at": pd.Timestamp("2026-01-15"),
    }])
    save_earnings_cache(seed)

    history = pd.DataFrame([_row("X", "2025-12-01", "2026-01-29")])
    eh._update_earnings_dates_for_tickers(["X"], history,
                                          today=pd.Timestamp("2026-04-30"))

    out = load_earnings_cache()
    assert set(out["ticker"]) == {"X", "Y"}
    y = out.loc[out["ticker"] == "Y"].iloc[0]
    assert y["last_earnings"] == pd.Timestamp("2026-01-15")
    assert y["next_earnings"] == pd.Timestamp("2026-04-15")


# ----------------------------------------------------------------------
# Bulk / targeted fill — uses a mocked ZacksSession (no network)
# ----------------------------------------------------------------------

class _FakeSession:
    """Stand-in for zacks_scraper.ZacksSession that returns canned data
    for known tickers and None for any others. Audit M1: also exposes
    `last_failure_kind` so the bulk-fill loop's auto-pause classifier
    works against this fake."""

    def __init__(self, canned: dict[str, list[dict] | None]):
        self._canned = canned
        self.last_failure_kind = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass

    def fetch(self, symbol, years=5):
        from trade_scanner_fh.zacks_scraper import FAIL_NOT_FOUND
        result = self._canned.get(symbol)
        if result is None:
            self.last_failure_kind = FAIL_NOT_FOUND
        else:
            self.last_failure_kind = None
        return result


def _fake_zacks_rows(period_str: str, report_str: str, surp_pct=5.0):
    return {
        "period_ending": pd.Timestamp(period_str),
        "report_date": pd.Timestamp(report_str),
        "report_time": "Close",
        "estimated_eps": 2.0, "reported_eps": 2.1,
        "surprise_eps": 0.1, "surprise_eps_pct": surp_pct,
        "estimated_rev": 100.0, "reported_rev": 105.0,
        "surprise_rev": 5.0, "surprise_rev_pct": surp_pct,
    }


def test_bulk_fill_writes_history_and_earnings_dates(tmp_parquets):
    canned = {
        "AAPL": [_fake_zacks_rows("2025-12-01", "2026-01-29")],
        "MSFT": [_fake_zacks_rows("2025-12-01", "2026-01-28")],
        "FAIL": None,
    }
    fake = _FakeSession(canned)

    with patch.object(eh, "ZacksSession", return_value=fake), \
         patch.object(eh.time, "sleep", lambda *_: None):
        filled, errors = eh.bulk_fill_zacks(
            ["AAPL", "MSFT", "FAIL"], blacklist=set(), delay_sec=0,
        )

    assert filled == 2
    assert errors == 1

    df = eh.load_earnings_history()
    assert len(df) == 2
    assert set(df["ticker"]) == {"AAPL", "MSFT"}

    from trade_scanner_fh.earnings_cache import load_earnings_cache
    dates_df = load_earnings_cache()
    assert dates_df is not None
    assert set(dates_df["ticker"]) == {"AAPL", "MSFT"}


def test_bulk_fill_respects_blacklist(tmp_parquets):
    canned = {"AAPL": [_fake_zacks_rows("2025-12-01", "2026-01-29")]}
    fake = _FakeSession(canned)
    with patch.object(eh, "ZacksSession", return_value=fake), \
         patch.object(eh.time, "sleep", lambda *_: None):
        filled, _ = eh.bulk_fill_zacks(
            ["AAPL", "BANNED"], blacklist={"BANNED"}, delay_sec=0,
        )
    assert filled == 1


def test_bulk_fill_flush_every_persists_partial_progress(tmp_parquets):
    """flush_every=2 → after 2 successful pulls, the parquet exists with
    those 2 tickers even if a stop_flag triggers afterwards."""
    canned = {
        "A": [_fake_zacks_rows("2025-12-01", "2026-01-29")],
        "B": [_fake_zacks_rows("2025-12-01", "2026-01-29")],
        "C": [_fake_zacks_rows("2025-12-01", "2026-01-29")],
    }
    fake = _FakeSession(canned)
    stop = [False]

    counter = {"n": 0}

    def cb(d, t):
        counter["n"] = d
        if d >= 2:
            stop[0] = True

    with patch.object(eh, "ZacksSession", return_value=fake), \
         patch.object(eh.time, "sleep", lambda *_: None):
        filled, _ = eh.bulk_fill_zacks(
            ["A", "B", "C"], blacklist=set(),
            progress_cb=cb, stop_flag=stop, flush_every=2, delay_sec=0,
        )

    df = eh.load_earnings_history()
    assert df is not None
    # At least the first flushed batch (A and B) is on disk
    assert {"A", "B"}.issubset(set(df["ticker"]))


def test_bulk_fill_replaces_rather_than_duplicating_a_refetched_quarter(
        tmp_parquets):
    """Refetching a quarter must REPLACE its prior row, not append a duplicate.

    This is the Phase 6.5 contract. Audit 2026-08-16 (F1) narrowed the scope of
    the replacement from "every row this (ticker, source) has" to "the span this
    response covers" — so the anti-duplicate property is asserted here on a
    response that actually re-covers the stored quarters, and the retention
    property gets its own test below.
    """
    seed = pd.DataFrame([
        _row("AAPL", "2024-12-01", "2025-01-30"),
        _row("AAPL", "2024-09-01", "2024-10-31"),
    ])
    eh.save_earnings_history(seed)

    # Response reaches back to (and past) the oldest stored quarter, so it is
    # authoritative over the whole stored span.
    canned = {"AAPL": [
        _fake_zacks_rows("2024-09-01", "2024-10-31"),
        _fake_zacks_rows("2024-12-01", "2025-01-30"),
        _fake_zacks_rows("2025-12-01", "2026-01-29"),
    ]}
    fake = _FakeSession(canned)
    with patch.object(eh, "ZacksSession", return_value=fake), \
         patch.object(eh.time, "sleep", lambda *_: None):
        eh.bulk_fill_zacks(["AAPL"], blacklist=set(), delay_sec=0)

    df = eh.load_earnings_history()
    aapl = df.loc[df["ticker"] == "AAPL"]
    assert len(aapl) == 3, "a re-covered quarter must not duplicate"
    assert not aapl.duplicated(subset=["period_ending"]).any()
    assert aapl["period_ending"].max() == pd.Timestamp("2025-12-01")


def test_bulk_fill_keeps_quarters_a_short_response_did_not_cover(tmp_parquets):
    """Audit 2026-08-16 (F1, CRITICAL): a 200-OK-but-SHORT response must not
    delete the quarters it simply didn't mention.

    Before F1 the flush dropped EVERY (ticker, source) row and wrote whatever
    came back, so one partially-rendered page permanently truncated years of
    history — silently, per ticker, on every run.
    """
    seed = pd.DataFrame([
        _row("AAPL", "2024-12-01", "2025-01-30"),
        _row("AAPL", "2024-09-01", "2024-10-31"),
    ])
    eh.save_earnings_history(seed)

    # Only the newest quarter comes back — the truncation signature.
    canned = {"AAPL": [_fake_zacks_rows("2025-12-01", "2026-01-29")]}
    fake = _FakeSession(canned)
    with patch.object(eh, "ZacksSession", return_value=fake), \
         patch.object(eh.time, "sleep", lambda *_: None):
        eh.bulk_fill_zacks(["AAPL"], blacklist=set(), delay_sec=0)

    df = eh.load_earnings_history()
    aapl = df.loc[df["ticker"] == "AAPL"]
    periods = set(aapl["period_ending"])
    assert periods == {
        pd.Timestamp("2024-09-01"),
        pd.Timestamp("2024-12-01"),
        pd.Timestamp("2025-12-01"),
    }, "the short response deleted history it never covered"


def test_targeted_fill_iterates_only_provided_tickers(tmp_parquets):
    """targeted_fill_zacks must NOT touch tickers outside `gap_tickers`."""
    canned = {
        "GAP1": [_fake_zacks_rows("2025-12-01", "2026-01-29")],
        "GAP2": [_fake_zacks_rows("2025-12-01", "2026-01-29")],
        "EXISTING": [_fake_zacks_rows("2024-12-01", "2025-01-29")],
    }
    fake = _FakeSession(canned)
    with patch.object(eh, "ZacksSession", return_value=fake), \
         patch.object(eh.time, "sleep", lambda *_: None):
        eh.targeted_fill_zacks(["GAP1", "GAP2"], blacklist=set(), delay_sec=0)

    df = eh.load_earnings_history()
    assert set(df["ticker"]) == {"GAP1", "GAP2"}


def test_find_gap_tickers(tmp_parquets):
    """Gap = universe ∩ (not blacklist) − tickers in earnings_history."""
    seed = pd.DataFrame([
        _row("HAVE1", "2025-12-01", "2026-01-29"),
        _row("HAVE2", "2025-09-01", "2025-10-30"),
    ])
    eh.save_earnings_history(seed)

    universe = ["HAVE1", "HAVE2", "GAP1", "GAP2", "BANNED"]
    blacklist = {"BANNED"}
    gaps = eh.find_gap_tickers(universe, blacklist)
    assert sorted(gaps) == ["GAP1", "GAP2"]


def test_find_gap_tickers_no_history_returns_full_universe_minus_blacklist(tmp_parquets):
    universe = ["A", "B", "C"]
    gaps = eh.find_gap_tickers(universe, blacklist={"B"})
    assert sorted(gaps) == ["A", "C"]
