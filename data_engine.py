"""
Data Engine — OHLCV Download, Parquet Cache, Incremental Updates, Validation
=============================================================================

Public API
    download_all(symbols, max_workers=4, progress_cb=None) -> ScrapeReport
    download_one(symbol)          -> ScrapeResult
    load_ohlcv(symbol)            -> pd.DataFrame | None
    validate_ticker(symbol, df)   -> list[str]   (anomaly messages)

ScrapeReport / ScrapeResult are dataclasses with structured metadata.
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Optional

import numpy as np
import pandas as pd
import yfinance as yf

from . import config

log = logging.getLogger("scanner.data")


# ── Result types ───────────────────────────────────────────────────────

@dataclass
class ScrapeResult:
    symbol: str
    rows_received: int = 0
    status: str = "ok"          # "ok", "no_data", "error"
    anomalies: list[str] = field(default_factory=list)
    error_msg: str = ""
    was_incremental: bool = False


@dataclass
class ScrapeReport:
    total: int = 0
    ok: int = 0
    no_data: int = 0
    errors: int = 0
    results: list[ScrapeResult] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"Scrape complete: {self.total} tickers | "
            f"{self.ok} ok, {self.no_data} no data, {self.errors} errors"
        )


# ── Helpers ────────────────────────────────────────────────────────────

def _parquet_path(symbol: str) -> Path:
    """Return the canonical parquet path for a ticker symbol."""
    return config.PARQUET_DIR / f"{symbol.upper()}.parquet"


def _last_cached_date(symbol: str) -> Optional[pd.Timestamp]:
    """Return the most recent date in a ticker's cached parquet, or None."""
    p = _parquet_path(symbol)
    if not p.exists():
        return None
    try:
        df = pd.read_parquet(p)
        if df.empty:
            return None
        return pd.Timestamp(df.index.max())
    except Exception:
        return None


def _download_raw(symbol: str, start: str, end: str) -> pd.DataFrame:
    """
    Download OHLCV from yfinance for a single ticker between start and end.
    Returns a DataFrame indexed by Date with columns:
        Open, High, Low, Close, Volume
    """
    ticker = yf.Ticker(symbol)
    df = ticker.history(start=start, end=end, auto_adjust=True)

    if df.empty:
        return df

    # yfinance may return extra columns (Dividends, Stock Splits) — keep only OHLCV
    keep = ["Open", "High", "Low", "Close", "Volume"]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].copy()

    # Ensure the index is a DatetimeIndex named "Date"
    df.index = pd.to_datetime(df.index)
    df.index.name = "Date"

    return df


# ── Validation ─────────────────────────────────────────────────────────

def validate_ticker(symbol: str, df: pd.DataFrame) -> list[str]:
    """
    Run data-quality checks on a ticker's OHLCV DataFrame.
    Returns a list of human-readable anomaly strings (empty = clean).
    """
    issues: list[str] = []

    if df.empty:
        issues.append("DataFrame is empty")
        return issues

    # 1. NaN / null values
    nan_counts = df.isna().sum()
    for col, cnt in nan_counts.items():
        if cnt > 0:
            issues.append(f"{cnt} NaN values in {col}")

    # 2. Duplicate dates
    dup_count = df.index.duplicated().sum()
    if dup_count > 0:
        issues.append(f"{dup_count} duplicate date(s)")

    # 3. Zero-volume bars
    if "Volume" in df.columns:
        zero_vol = (df["Volume"] == 0).sum()
        if zero_vol > 0:
            issues.append(f"{zero_vol} zero-volume bar(s)")

    # 4. Price jumps exceeding threshold
    if "Close" in df.columns and len(df) > 1:
        close = df["Close"].dropna()
        pct_change = close.pct_change().abs() * 100
        big_jumps = pct_change[pct_change > config.PRICE_JUMP_PCT]
        if len(big_jumps) > 0:
            dates_str = ", ".join(
                str(d.date()) for d in big_jumps.index[:5]
            )
            issues.append(
                f"{len(big_jumps)} price jump(s) > {config.PRICE_JUMP_PCT}% "
                f"(first: {dates_str})"
            )

    # 5. Missing trading days (gaps > 4 calendar days ≈ long weekends ok)
    if len(df) > 1:
        date_diffs = pd.Series(df.index).diff().dt.days
        # Weekends = 3 days gap is normal; flag runs > threshold
        big_gaps = date_diffs[date_diffs > config.MAX_MISSING_DAYS_FLAG + 2]
        if len(big_gaps) > 0:
            issues.append(f"{len(big_gaps)} suspicious date gap(s) in history")

    return issues


# ── Single-ticker download ─────────────────────────────────────────────

def download_one(symbol: str) -> ScrapeResult:
    """
    Download (or incrementally update) OHLCV for one ticker.
    Saves/appends to a parquet file. Returns a ScrapeResult.
    """
    result = ScrapeResult(symbol=symbol)
    pq = _parquet_path(symbol)

    try:
        last_date = _last_cached_date(symbol)

        if last_date is not None:
            # Incremental: fetch from day after last cached date
            start = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
            result.was_incremental = True
        else:
            # Full pull: go back OHLCV_HISTORY_YEARS
            start = (
                datetime.now() - timedelta(days=365 * config.OHLCV_HISTORY_YEARS)
            ).strftime("%Y-%m-%d")

        end = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

        new_df = _download_raw(symbol, start, end)

        if new_df.empty and last_date is None:
            result.status = "no_data"
            result.rows_received = 0
            log.debug("%s — no data returned", symbol)
            return result

        # Merge with existing cache if incremental
        if last_date is not None and pq.exists():
            old_df = pd.read_parquet(pq)
            if not new_df.empty:
                combined = pd.concat([old_df, new_df])
                combined = combined[~combined.index.duplicated(keep="last")]
                combined.sort_index(inplace=True)
            else:
                combined = old_df
        else:
            combined = new_df

        if combined.empty:
            result.status = "no_data"
            return result

        # Validate
        result.anomalies = validate_ticker(symbol, combined)
        if result.anomalies:
            log.info("%s — anomalies: %s", symbol, "; ".join(result.anomalies))

        # Save
        combined.to_parquet(pq, engine="pyarrow")
        result.rows_received = len(new_df) if not new_df.empty else 0
        result.status = "ok"

    except Exception as exc:
        result.status = "error"
        result.error_msg = str(exc)
        log.warning("%s — error: %s", symbol, exc)

    return result


# ── Bulk download ──────────────────────────────────────────────────────

def download_all(
    symbols: list[str],
    progress_cb: Optional[Callable[[int, int, ScrapeResult], None]] = None,
) -> ScrapeReport:
    """
    Download OHLCV for every symbol in the list (sequentially with polite
    pauses). Calls progress_cb(done, total, result) after each ticker.

    Returns a ScrapeReport summarising the run.
    """
    report = ScrapeReport(total=len(symbols))
    log.info("Starting bulk download for %d tickers …", len(symbols))

    for i, sym in enumerate(symbols, 1):
        res = download_one(sym)
        report.results.append(res)

        if res.status == "ok":
            report.ok += 1
        elif res.status == "no_data":
            report.no_data += 1
        else:
            report.errors += 1

        if progress_cb:
            progress_cb(i, report.total, res)

        # Log periodic progress
        if i % 100 == 0 or i == report.total:
            log.info(
                "Progress: %d / %d  (ok=%d, no_data=%d, err=%d)",
                i, report.total, report.ok, report.no_data, report.errors,
            )

        # Polite pause
        if i < report.total:
            time.sleep(config.YFINANCE_PAUSE_SEC)

    log.info(report.summary())
    return report


# ── Load cached data ───────────────────────────────────────────────────

def load_ohlcv(symbol: str) -> Optional[pd.DataFrame]:
    """Load cached OHLCV parquet for a symbol. Returns None if not found."""
    p = _parquet_path(symbol)
    if not p.exists():
        return None
    try:
        return pd.read_parquet(p)
    except Exception:
        log.warning("Corrupt parquet for %s — returning None", symbol)
        return None
