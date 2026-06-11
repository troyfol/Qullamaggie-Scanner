"""Phase 9 §9.2 — automated end-to-end smoke.

Builds a synthetic OHLCV cache + earnings_history parquet, runs the
real `scanner.run_scan` pipeline, and verifies the new earnings
columns and consecutive-beats stages flow through correctly. This is
the automated counterpart of the manual smoke-test checklist in
TINYEARNINGS_FORK.md §9.2.
"""
from __future__ import annotations

from datetime import date, datetime

import numpy as np
import pandas as pd
import pytest

from trade_scanner_fh import config, scanner
from trade_scanner_fh.scanner import ScanParams


# ──────────────────────────────────────────────────────────────────────
# Fixtures — synthetic cache so the scanner has data to chew on
# ──────────────────────────────────────────────────────────────────────

@pytest.fixture
def fake_cache(tmp_path, monkeypatch):
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


def _write_ohlcv(symbol: str, end: pd.Timestamp, days: int = 200,
                 close: float = 100.0):
    """Write a synthetic per-ticker OHLCV parquet that survives the
    scanner's sanity checks."""
    idx = pd.bdate_range(end=end, periods=days)
    df = pd.DataFrame({
        "Open":   [close] * days,
        "High":   [close * 1.01] * days,
        "Low":    [close * 0.99] * days,
        "Close":  [close] * days,
        "Volume": [1_000_000] * days,
    }, index=idx)
    df.to_parquet(config.PARQUET_DIR / f"{symbol}.parquet")


def _hist_row(ticker: str, period_end: str, report_date: str,
              eps_pct: float, rev_pct: float, source: str = "zacks") -> dict:
    return {
        "ticker": ticker,
        "period_ending": pd.Timestamp(period_end),
        "report_date": pd.Timestamp(report_date),
        "report_time": "Close",
        "estimated_eps": 1.0, "reported_eps": 1.0 + eps_pct / 100.0,
        "surprise_eps": eps_pct / 100.0, "surprise_eps_pct": eps_pct,
        "estimated_rev": 100.0, "reported_rev": 100.0 + rev_pct,
        "surprise_rev": rev_pct, "surprise_rev_pct": rev_pct,
        "source": source,
        "updated_at": pd.Timestamp(datetime.now()),
    }


# ──────────────────────────────────────────────────────────────────────
# §9.2 step 2 — earnings columns appear post-scan with no filters
# ──────────────────────────────────────────────────────────────────────

def test_per_quarter_columns_appear_when_filter_enabled(fake_cache):
    """Run a scan with multiple Zacks per-quarter filters on; results
    must surface ONLY the columns whose specific filter is on (per
    Option B per-column gating). Tickers with history get real values;
    tickers without get NaN (per `include_no_data` policy)."""
    end = pd.Timestamp(date(2026, 4, 30))

    _write_ohlcv("AAPL", end)
    _write_ohlcv("MSFT", end)

    # AAPL has Zacks history; MSFT does not — must yield NaN cluster.
    pd.DataFrame([
        _hist_row("AAPL", "2026-01-31", "2026-02-15",
                  eps_pct=10.0, rev_pct=5.0),
    ]).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    p = ScanParams(
        start_date=date(2026, 1, 1), end_date=date(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        # Enable enough individual filters to surface all four columns
        # this test asserts on. Per Option B, each column appears only
        # when its corresponding filter (or display-only) is on.
        surprise_eps_pct_enabled=True,
        surprise_eps_pct_min=0.0,
        # Default earnings_data_only=False → MSFT (no Zacks data)
        # passes the surprise_eps_pct filter via NaN.
        reported_eps_display_only=True,
        reported_rev_display_only=True,
    )

    result = scanner.run_scan(["AAPL", "MSFT"], p)
    assert not result.results_df.empty
    df = result.results_df.set_index("symbol")

    # AAPL — values populated from Zacks
    assert df.loc["AAPL", "reported_eps"] == pytest.approx(1.10)
    assert df.loc["AAPL", "surprise_eps_pct"] == pytest.approx(10.0)
    assert df.loc["AAPL", "reported_rev"] == pytest.approx(105.0)
    assert pd.Timestamp(df.loc["AAPL", "last_report_date"]) == pd.Timestamp(
        "2026-02-15"
    )

    # MSFT — no Zacks data, all NaN; passed via earnings_data_only=False
    assert "MSFT" in df.index
    for col in ("reported_eps", "surprise_eps_pct", "reported_rev",
                "last_report_date"):
        assert pd.isna(df.loc["MSFT", col]), f"expected NaN for MSFT.{col}"


# ──────────────────────────────────────────────────────────────────────
# §9.2 step 3 — surprise EPS % filter actually filters
# ──────────────────────────────────────────────────────────────────────

def test_surprise_eps_pct_filter_excludes_low_surprises(fake_cache):
    end = pd.Timestamp(date(2026, 4, 30))
    for sym, surp in [("HIGH", 10.0), ("LOW", 1.0)]:
        _write_ohlcv(sym, end)

    pd.DataFrame([
        _hist_row("HIGH", "2026-01-31", "2026-02-15", eps_pct=10.0, rev_pct=5.0),
        _hist_row("LOW",  "2026-01-31", "2026-02-15", eps_pct=1.0,  rev_pct=2.0),
    ]).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    p = ScanParams(
        start_date=date(2026, 1, 1), end_date=date(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        surprise_eps_pct_enabled=True,
        surprise_eps_pct_min=5.0,
        # earnings_data_only=True → require Zacks data (NaN tickers fail)
        earnings_data_only=True,
    )

    result = scanner.run_scan(["HIGH", "LOW"], p)
    syms = set(result.results_df["symbol"]) if not result.results_df.empty else set()
    assert syms == {"HIGH"}


# ──────────────────────────────────────────────────────────────────────
# §9.2 step 4 — Consecutive EPS Beats filter populates streak + per-Q
# ──────────────────────────────────────────────────────────────────────

def test_consecutive_eps_beats_filter_populates_streak_and_quarters(fake_cache):
    """A 4-beat streak ticker passes a `>= 3` filter and emits q1..q4
    EPS columns; a non-streaking ticker is filtered out."""
    end = pd.Timestamp(date(2026, 4, 30))
    _write_ohlcv("BEAT", end)
    _write_ohlcv("MISS", end)

    rows = [
        # BEAT — 4 quarters all positive surprise → streak = 4
        _hist_row("BEAT", "2026-01-31", "2026-02-15", eps_pct=10.0, rev_pct=5.0),
        _hist_row("BEAT", "2025-10-31", "2025-11-15", eps_pct=8.0,  rev_pct=4.0),
        _hist_row("BEAT", "2025-07-31", "2025-08-15", eps_pct=6.0,  rev_pct=3.0),
        _hist_row("BEAT", "2025-04-30", "2025-05-15", eps_pct=4.0,  rev_pct=2.0),
        # MISS — most recent quarter is a miss → streak = 0
        _hist_row("MISS", "2026-01-31", "2026-02-15", eps_pct=-2.0, rev_pct=1.0),
        _hist_row("MISS", "2025-10-31", "2025-11-15", eps_pct=5.0,  rev_pct=2.0),
    ]
    pd.DataFrame(rows).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    p = ScanParams(
        start_date=date(2026, 1, 1), end_date=date(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        consec_eps_beats_enabled=True,
        consec_eps_beats_min=3,
        consec_eps_beats_threshold_pct=0.0,
    )

    result = scanner.run_scan(["BEAT", "MISS"], p)
    df = result.results_df
    assert set(df["symbol"]) == {"BEAT"}, "MISS should be filtered out"

    beat = df.iloc[0]
    assert int(beat["consec_eps_beats"]) == 4
    # Q-1..Q-4 EPS columns populated, descending values per the seed
    assert beat["q1_surprise_eps_pct"] == pytest.approx(10.0)
    assert beat["q2_surprise_eps_pct"] == pytest.approx(8.0)
    assert beat["q3_surprise_eps_pct"] == pytest.approx(6.0)
    assert beat["q4_surprise_eps_pct"] == pytest.approx(4.0)


# ──────────────────────────────────────────────────────────────────────
# §9.2 step 6 — per-period snapshot reflects the period's end date
# ──────────────────────────────────────────────────────────────────────

def test_per_period_uses_each_periods_end_date(fake_cache):
    """A scan run twice with different end_dates must show DIFFERENT
    most-recent quarters when the second end_date crosses a new
    earnings report — proving §8.1 'as of period end' semantics."""
    early_end = pd.Timestamp(date(2026, 1, 31))
    late_end = pd.Timestamp(date(2026, 4, 30))
    _write_ohlcv("ZQ", late_end, days=300)

    pd.DataFrame([
        _hist_row("ZQ", "2026-01-31", "2026-02-15", eps_pct=10.0, rev_pct=5.0),
        _hist_row("ZQ", "2025-10-31", "2025-11-15", eps_pct=8.0, rev_pct=4.0),
    ]).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    base = dict(
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        surprise_eps_pct_enabled=True,
        surprise_eps_pct_min=0.0,
        earnings_data_only=True,  # require Zacks data
    )
    early = ScanParams(start_date=date(2025, 9, 1),
                      end_date=date(2026, 1, 31), **base)
    late = ScanParams(start_date=date(2025, 9, 1),
                     end_date=date(2026, 4, 30), **base)

    r_early = scanner.run_scan(["ZQ"], early).results_df
    r_late = scanner.run_scan(["ZQ"], late).results_df

    assert pd.Timestamp(r_early.iloc[0]["last_report_date"]) == pd.Timestamp(
        "2025-11-15"
    ), "Pre-Feb-15 scan should still show the November quarter"
    assert pd.Timestamp(r_late.iloc[0]["last_report_date"]) == pd.Timestamp(
        "2026-02-15"
    )
