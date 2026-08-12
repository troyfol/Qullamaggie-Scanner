"""Scan context + parallel compute — audit 2026-08-12 (EFF-3, EFF-1).

EFF-3: `run_scan` rebuilt the entire earnings setup once per TIMEFRAME — a
148k-row parquet read, dedupe, YoY recompute, sort and a ~6,300-group groupby.
A sequenced run can emit 30 chunks, i.e. 30 identical rebuilds. `ScanWorker`
now builds one `ScanContext` per run and passes it to every timeframe.

EFF-1: the per-ticker compute loop was strictly single-threaded. Measured on
the live 14,300-ticker cache: 224.1s serial → 81.0s at 6 workers (2.77x) with
identical output. Output determinism is the load-bearing property here, so it
is pinned below.
"""
from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from trade_scanner_fh import config, data_engine, scanner
from trade_scanner_fh.scanner import (
    ScanContext, ScanParams, build_scan_context, run_scan,
)


def _write_parquets(tmp_path, monkeypatch, symbols, days=80):
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    data_engine.clear_ohlcv_cache()
    idx = pd.date_range("2026-01-01", periods=days, freq="B")
    for n, sym in enumerate(symbols):
        # Distinct price paths so a mis-ordered result is detectable.
        base = 100.0 + n
        pd.DataFrame({
            "Open": [base + i * 0.1 for i in range(days)],
            "High": [base + 1 + i * 0.1 for i in range(days)],
            "Low": [base - 1 + i * 0.1 for i in range(days)],
            "Close": [base + 0.5 + i * 0.1 for i in range(days)],
            "Volume": [1_000_000 + n] * days,
        }, index=idx).to_parquet(tmp_path / f"{sym}.parquet")


def _params(**kw) -> ScanParams:
    """Every filter OFF, so rows actually survive the funnel — otherwise the
    parity assertions below would compare two empty frames and pass
    vacuously."""
    base = dict(
        start_date=date(2026, 1, 1), end_date=date(2026, 3, 1),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False, top_pct_enabled=False,
        consec_gaps_enabled=False, consec_gaps_down_enabled=False,
        current_gap_enabled=False, max_gap_enabled=False,
        max_neg_gap_enabled=False, surge_enabled=False, adr_enabled=False,
        atr_enabled=False, bbw_enabled=False, atr_ratio_enabled=False,
        vol_dryup_enabled=False, min_price_enabled=False, avg_vol_enabled=False,
        dollar_vol_enabled=False, rs_market_enabled=False,
        rs_nasdaq_enabled=False, rs_sector_enabled=False,
        days_since_earnings_enabled=False, days_until_earnings_enabled=False,
        days_until_max_enabled=False,
    )
    base.update(kw)
    return ScanParams(**base)


# ── EFF-3: context construction ────────────────────────────────────────

def test_context_ors_flags_across_params_list(tmp_path, monkeypatch):
    """A context built for a multi-timeframe run must satisfy EVERY
    timeframe, not just the first — otherwise a later timeframe that needs
    the earnings dates would silently get an empty lookup."""
    monkeypatch.setattr(scanner.config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "none.parquet")
    calls = []

    def fake_load():
        calls.append(1)
        return pd.DataFrame([{
            "ticker": "AAA", "last_earnings": pd.Timestamp("2026-01-05"),
            "next_earnings": pd.Timestamp("2026-04-05"),
        }])

    monkeypatch.setattr("trade_scanner_fh.earnings_cache.load_earnings_cache",
                        fake_load)
    ctx = build_scan_context([
        _params(days_since_earnings_enabled=False),
        _params(days_until_earnings_display_only=True),   # only this one needs it
    ])
    assert calls, "earnings dates not loaded despite a later timeframe needing them"
    assert "AAA" in ctx.earnings_lookup


def test_context_skips_unneeded_lookups(tmp_path, monkeypatch):
    """No filter needs the dates → don't pay to read them."""
    monkeypatch.setattr(scanner.config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "none.parquet")
    calls = []
    monkeypatch.setattr("trade_scanner_fh.earnings_cache.load_earnings_cache",
                        lambda: calls.append(1))
    ctx = build_scan_context([_params()])
    assert not calls
    assert ctx.earnings_lookup == {}


def test_run_scan_uses_supplied_context_and_does_not_rebuild(
        tmp_path, monkeypatch):
    """The whole point of EFF-3: a supplied context means zero setup reads."""
    _write_parquets(tmp_path, monkeypatch, ["AAA"])
    monkeypatch.setattr(scanner.config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "none.parquet")
    rebuilt = []
    monkeypatch.setattr(
        "trade_scanner_fh.earnings_history.load_earnings_history",
        lambda: rebuilt.append(1),
    )
    ctx = ScanContext(earnings_history_lookup={"AAA": pd.DataFrame()})
    run_scan(["AAA"], _params(), context=ctx)
    assert not rebuilt, "run_scan rebuilt the history despite a supplied context"


def test_run_scan_without_context_still_works(tmp_path, monkeypatch):
    """Back-compat: every existing caller and test omits `context`."""
    _write_parquets(tmp_path, monkeypatch, ["AAA"])
    monkeypatch.setattr(scanner.config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "none.parquet")
    result = run_scan(["AAA"], _params())
    assert result.results_df is not None


# ── EFF-1: parallel determinism ────────────────────────────────────────

@pytest.mark.parametrize("workers", [1, 2, 6])
def test_parallel_output_is_identical_to_serial(tmp_path, monkeypatch, workers):
    """Bit-identical results at every pool width. Verified on the live cache
    too (14,300 tickers, 531 rows, byte-identical frames)."""
    syms = [f"S{i:03d}" for i in range(40)]
    _write_parquets(tmp_path, monkeypatch, syms)
    monkeypatch.setattr(scanner.config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "none.parquet")
    ctx = ScanContext()

    monkeypatch.setattr(scanner.config, "SCAN_MAX_WORKERS", 1)
    serial = run_scan(syms, _params(), context=ctx).results_df

    data_engine.clear_ohlcv_cache()
    monkeypatch.setattr(scanner.config, "SCAN_MAX_WORKERS", workers)
    parallel = run_scan(syms, _params(), context=ctx).results_df

    pd.testing.assert_frame_equal(serial, parallel, check_exact=True)


def test_parallel_preserves_symbol_order(tmp_path, monkeypatch):
    """pool.map yields in input order; row order must not depend on which
    thread finished first."""
    syms = [f"S{i:03d}" for i in range(40)]
    _write_parquets(tmp_path, monkeypatch, syms)
    monkeypatch.setattr(scanner.config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "none.parquet")
    monkeypatch.setattr(scanner.config, "SCAN_MAX_WORKERS", 6)
    df = run_scan(syms, _params(top_pct_enabled=False), context=ScanContext(
        )).results_df
    assert list(df["symbol"]) == sorted(df["symbol"]), \
        "results are no longer in universe order"


def test_cancel_token_still_interrupts_the_pool(tmp_path, monkeypatch):
    """Stop must work mid-scan, not only between timeframes (Phase 4 R2)."""
    syms = [f"S{i:03d}" for i in range(200)]
    _write_parquets(tmp_path, monkeypatch, syms)
    monkeypatch.setattr(scanner.config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "none.parquet")
    monkeypatch.setattr(scanner.config, "SCAN_MAX_WORKERS", 4)

    seen = {"n": 0}

    def cancel():
        seen["n"] += 1
        return seen["n"] > 20      # trip partway through

    result = run_scan(syms, _params(), context=ScanContext(),
                      cancel_token=cancel)
    n = 0 if result.results_df is None else len(result.results_df)
    assert n < len(syms), "cancel_token did not interrupt the scan"


def test_per_ticker_exception_does_not_kill_the_batch(tmp_path, monkeypatch):
    """One bad ticker is recorded in result.errors; the rest still compute —
    the serial loop's per-ticker try/except semantics, preserved."""
    syms = ["AAA", "BOOM", "CCC"]
    _write_parquets(tmp_path, monkeypatch, syms)
    monkeypatch.setattr(scanner.config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "none.parquet")
    monkeypatch.setattr(scanner.config, "SCAN_MAX_WORKERS", 4)

    real = scanner._compute_ticker

    def flaky(symbol, *a, **kw):
        if symbol == "BOOM":
            raise ValueError("synthetic failure")
        return real(symbol, *a, **kw)

    monkeypatch.setattr(scanner, "_compute_ticker", flaky)
    result = run_scan(syms, _params(), context=ScanContext())

    assert [e["symbol"] for e in result.errors] == ["BOOM"]
    assert "traceback" in result.errors[0]
    assert set(result.results_df["symbol"]) == {"AAA", "CCC"}


def test_scan_max_workers_of_one_is_supported(tmp_path, monkeypatch):
    """The documented escape hatch for diffing against an older build."""
    _write_parquets(tmp_path, monkeypatch, ["AAA", "BBB"])
    monkeypatch.setattr(scanner.config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "none.parquet")
    monkeypatch.setattr(scanner.config, "SCAN_MAX_WORKERS", 1)
    df = run_scan(["AAA", "BBB"], _params(), context=ScanContext()).results_df
    assert len(df) == 2
