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


@pytest.fixture
def tmp_parquets(tmp_path, monkeypatch):
    """Redirect both parquet paths to a tmp directory."""
    monkeypatch.setattr(eh.config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(
        eh.config, "EARNINGS_HISTORY_PARQUET",
        tmp_path / "earnings_history.parquet",
    )
    monkeypatch.setattr(
        eh.config, "EARNINGS_PARQUET",
        tmp_path / "earnings_dates.parquet",
    )
    return tmp_path


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
    raw_root.mkdir()
    for src in config.RAW_SOURCES:
        (raw_root / src).mkdir()

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


# ----------------------------------------------------------------------
# Phase 2 — dedupe helper (Zacks > Finnhub) + (ticker, source) PK flush
# ----------------------------------------------------------------------

def test_dedupe_drops_all_finnhub_rows_for_zacks_covered_ticker():
    """Binary source policy: any ticker with a Zacks row has ALL its
    Finnhub rows dropped — even on different periods. Replaces the
    field-level coalesce that mixed Zacks adjusted EPS with Finnhub
    GAAP EPS. Source consistency wins over data completeness."""
    rows = [
        _row("AAPL", "2025-12-01", "2026-01-29", source="zacks",
             eps_est=1.95, eps_rep=2.10),
        # Same period as Zacks — must be dropped.
        _row("AAPL", "2025-12-01", "2026-01-29", source="finnhub",
             eps_est=1.90, eps_rep=2.05),
        # DIFFERENT period — also dropped (binary, ticker-level).
        _row("AAPL", "2025-06-01", "2025-07-30", source="finnhub",
             eps_est=1.50, eps_rep=1.55),
    ]
    df = pd.DataFrame(rows)
    deduped = eh.dedupe_history(df)
    assert len(deduped) == 1
    out = deduped.iloc[0]
    assert out["source"] == "zacks"
    assert out["estimated_eps"] == 1.95
    assert out["reported_eps"] == 2.10


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


def test_verify_integrity_detects_cross_source_period_overlap():
    """Binary policy regression check: a ticker with both Zacks AND
    Finnhub rows must be flagged. Auto-fixable — drops the Finnhub
    rows, keeping Zacks as the authoritative source."""
    df = pd.DataFrame([
        _row("AAPL", "2025-12-01", "2026-01-29", source="zacks"),
        _row("AAPL", "2025-09-01", "2025-10-30", source="finnhub"),
        _row("MSFT", "2025-12-01", "2026-01-28", source="finnhub"),
    ])
    findings = eh.verify_integrity(history_df=df)
    cross = [f for f in findings if f.check == "cross_source_period_overlap"]
    assert len(cross) == 1
    assert cross[0].affected_rows == 1  # only the AAPL Finnhub row
    assert cross[0].auto_fixable is True


def test_fix_integrity_issues_drops_finnhub_on_cross_source_overlap():
    df = pd.DataFrame([
        _row("AAPL", "2025-12-01", "2026-01-29", source="zacks"),
        _row("AAPL", "2025-09-01", "2025-10-30", source="finnhub"),
        _row("MSFT", "2025-12-01", "2026-01-28", source="finnhub"),
    ])
    findings = eh.verify_integrity(history_df=df)
    fixed, msgs = eh.fix_integrity_issues(df, findings)
    # AAPL Finnhub row dropped; MSFT Finnhub row preserved (no zacks).
    assert len(fixed) == 2
    aapl = fixed.loc[fixed["ticker"] == "AAPL"]
    assert len(aapl) == 1
    assert aapl.iloc[0]["source"] == "zacks"
    assert any("cross_source_period_overlap" in m for m in msgs)


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


def test_bulk_fill_replaces_existing_ticker_rows(tmp_parquets):
    """Refetching a ticker should fully replace its prior history rows
    rather than appending duplicates."""
    seed = pd.DataFrame([
        _row("AAPL", "2024-12-01", "2025-01-30"),
        _row("AAPL", "2024-09-01", "2024-10-31"),
    ])
    eh.save_earnings_history(seed)

    canned = {"AAPL": [_fake_zacks_rows("2025-12-01", "2026-01-29")]}
    fake = _FakeSession(canned)
    with patch.object(eh, "ZacksSession", return_value=fake), \
         patch.object(eh.time, "sleep", lambda *_: None):
        eh.bulk_fill_zacks(["AAPL"], blacklist=set(), delay_sec=0)

    df = eh.load_earnings_history()
    aapl = df.loc[df["ticker"] == "AAPL"]
    assert len(aapl) == 1  # old rows replaced, not appended
    assert aapl.iloc[0]["period_ending"] == pd.Timestamp("2025-12-01")


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
