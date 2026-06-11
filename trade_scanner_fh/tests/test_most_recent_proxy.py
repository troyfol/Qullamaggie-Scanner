"""Fix B: the scanner's most-recent-quarter pick must skip finnhub
calendar-proxy rows (report_date_proxy=True) so the displayed Last Report
Date / Reported EPS come from the REAL announcement row, not a proxy whose
calendar-quarter-end stamp can out-sort the real date. Keep-if-orphan: a
ticker whose ONLY coverage is a proxy still shows it."""
from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from trade_scanner_fh import config, scanner
from trade_scanner_fh.scanner import ScanParams


@pytest.fixture
def fake_scan_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "PARQUET_DIR", tmp_path / "ohlcv")
    monkeypatch.setattr(config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "earnings_history.parquet")
    monkeypatch.setattr(config, "EARNINGS_PARQUET",
                        tmp_path / "earnings_dates.parquet")
    monkeypatch.setattr(config, "SECTOR_MAP_PARQUET",
                        tmp_path / "sector_map.parquet")
    (tmp_path / "ohlcv").mkdir(parents=True, exist_ok=True)
    return tmp_path


def _write_ohlcv(symbol, end, days=200, close=100.0):
    idx = pd.bdate_range(end=end, periods=days)
    pd.DataFrame({
        "Open": [close] * days, "High": [close * 1.01] * days,
        "Low": [close * 0.99] * days, "Close": [close] * days,
        "Volume": [1_000_000] * days,
    }, index=idx).to_parquet(config.PARQUET_DIR / f"{symbol}.parquet")


def _row(ticker, period_str, report_str, *, eps, source, proxy):
    return {
        "ticker": ticker,
        "period_ending": pd.Timestamp(period_str),
        "report_date": pd.Timestamp(report_str),
        "report_time": "Close",
        "estimated_eps": 1.0, "reported_eps": eps,
        "surprise_eps": 0.1, "surprise_eps_pct": 5.0,
        "estimated_rev": 100.0, "reported_rev": 105.0,
        "surprise_rev": 5.0, "surprise_rev_pct": 5.0,
        "source": source, "updated_at": pd.Timestamp(datetime.now()),
        "report_date_proxy": proxy,
    }


_DISABLE = dict(
    sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
    dist_high_enabled=False, pct_gain_enabled=False, adr_enabled=False,
    min_price_enabled=False, avg_vol_enabled=False, dollar_vol_enabled=False,
)


def test_most_recent_pick_skips_finnhub_proxy(fake_scan_cache):
    """Real finviz row (03/02, eps 1.07) + finnhub calendar PROXY stamped
    later (03/31, eps 9.99). The proxy's later date must NOT win — the
    real row's date + EPS are reported."""
    end = pd.Timestamp(date(2026, 6, 6))
    _write_ohlcv("CRDO", end)
    pd.DataFrame([
        _row("CRDO", "2026-01-01", "2026-03-02", eps=1.07, source="finviz", proxy=False),
        _row("CRDO", "2026-03-01", "2026-03-31", eps=9.99, source="finnhub", proxy=True),
    ]).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    p = ScanParams(start_date=date(2026, 1, 1), end_date=date(2026, 6, 6),
                   reported_eps_display_only=True, **_DISABLE)
    df = scanner.run_scan(["CRDO"], p).results_df
    row = df[df["symbol"] == "CRDO"].iloc[0]
    assert row["reported_eps"] == 1.07, row["reported_eps"]          # real, not proxy 9.99
    assert pd.Timestamp(row["last_report_date"]) == pd.Timestamp("2026-03-02")


def test_orphan_proxy_kept_when_only_coverage(fake_scan_cache):
    """Keep-if-orphan: when a ticker's ONLY row is a finnhub proxy, the
    pick still uses it (don't blank a quarter with no real alternative)."""
    end = pd.Timestamp(date(2026, 6, 6))
    _write_ohlcv("ONLYP", end)
    pd.DataFrame([
        _row("ONLYP", "2026-03-01", "2026-03-31", eps=4.2, source="finnhub", proxy=True),
    ]).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    p = ScanParams(start_date=date(2026, 1, 1), end_date=date(2026, 6, 6),
                   reported_eps_display_only=True, **_DISABLE)
    df = scanner.run_scan(["ONLYP"], p).results_df
    row = df[df["symbol"] == "ONLYP"].iloc[0]
    assert row["reported_eps"] == 4.2
    assert pd.Timestamp(row["last_report_date"]) == pd.Timestamp("2026-03-31")


def test_beats_block_skips_finnhub_proxy(fake_scan_cache):
    """proxy-1: the multi-quarter beats columns (q1_*) must ALSO use the
    real row — so Q1 of the beats block matches the single-quarter pick
    rather than showing the proxy's shifted date/EPS in the same row."""
    end = pd.Timestamp(date(2026, 6, 6))
    _write_ohlcv("CRDO", end)
    pd.DataFrame([
        _row("CRDO", "2026-01-01", "2026-03-02", eps=1.07, source="finviz", proxy=False),
        _row("CRDO", "2026-03-01", "2026-03-31", eps=9.99, source="finnhub", proxy=True),
    ]).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    p = ScanParams(start_date=date(2026, 1, 1), end_date=date(2026, 6, 6),
                   reported_eps_display_only=True,
                   consec_eps_beats_display_only=True, **_DISABLE)
    df = scanner.run_scan(["CRDO"], p).results_df
    row = df[df["symbol"] == "CRDO"].iloc[0]
    assert row["reported_eps"] == 1.07                       # single-quarter pick (real)
    assert row["q1_reported_eps"] == 1.07, row["q1_reported_eps"]   # Q1 of beats block agrees
    assert pd.Timestamp(row["q1_report_date_eps"]) == pd.Timestamp("2026-03-02")
