"""
Data Engine — OHLCV Download, Parquet Cache, Incremental Updates, Validation
=============================================================================

Public API
    download_all(symbols, progress_cb=None)   -> ScrapeReport   (serial; legacy)
    download_many(symbols, ...)               -> list[ScrapeResult]  (parallel)
    download_one(symbol)                      -> ScrapeResult
    load_ohlcv(symbol)                        -> pd.DataFrame | None
    validate_ticker(symbol, df)               -> list[str]   (anomaly messages)

ScrapeReport / ScrapeResult are dataclasses with structured metadata.
"""

import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import lru_cache
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

_SAFE_TICKER_RE = re.compile(r"[^A-Z0-9.\-]")


def _parquet_path(symbol: str) -> Path:
    """Return the canonical parquet path for a ticker symbol.

    The ticker is whitelisted to ``[A-Z0-9.\\-]`` before being
    interpolated into the path so that a value containing ``..``, ``/``,
    ``\\``, null bytes, or other separators can't escape PARQUET_DIR.
    Real yfinance symbols only ever contain these characters; the
    whitelist is a defensive boundary against a future caller passing
    in a tainted value.
    """
    clean = _SAFE_TICKER_RE.sub("", symbol.upper()) or "INVALID"
    return config.PARQUET_DIR / f"{clean}.parquet"


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

    # yfinance may return extra columns (Dividends, etc.) — keep OHLCV + Stock Splits
    keep = ["Open", "High", "Low", "Close", "Volume", "Stock Splits"]
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

        # If incremental data contains a stock split, the cached
        # pre-split prices are stale.  Re-download the full history
        # so yfinance returns properly adjusted prices everywhere.
        if (
            last_date is not None
            and not new_df.empty
            and "Stock Splits" in new_df.columns
            and (new_df["Stock Splits"] != 0).any()
        ):
            split_dates = new_df.loc[
                new_df["Stock Splits"] != 0
            ].index.tolist()
            log.info(
                "%s — split detected on %s, re-downloading full history",
                symbol,
                ", ".join(str(d.date()) for d in split_dates),
            )
            full_start = (
                datetime.now() - timedelta(days=365 * config.OHLCV_HISTORY_YEARS)
            ).strftime("%Y-%m-%d")
            new_df = _download_raw(symbol, full_start, end)
            # Treat as a full pull from here on
            last_date = None
            result.was_incremental = False

        # Merge with existing cache if incremental
        if last_date is not None and pq.exists():
            old_df = pd.read_parquet(pq)
            if not new_df.empty:
                combined = pd.concat([old_df, new_df])
                combined = combined[~combined.index.duplicated(keep="last")]
                combined.sort_index(inplace=True)
            else:
                combined = old_df
        elif last_date is not None and not pq.exists():
            # The cache existed when we read its last date (top of this fn)
            # but has since vanished — a concurrent rebuild_ticker unlink,
            # antivirus, or a mid-flight failure. Writing only the incremental
            # tail (new_df starts at last_date+1) would truncate the ticker's
            # full history to a few recent bars. Re-pull the full window so the
            # written file always holds complete history, never a slice.
            log.warning(
                "%s — cache disappeared mid-update; re-fetching full history",
                symbol,
            )
            full_start = (
                datetime.now() - timedelta(days=365 * config.OHLCV_HISTORY_YEARS)
            ).strftime("%Y-%m-%d")
            new_df = _download_raw(symbol, full_start, end)
            last_date = None
            result.was_incremental = False
            combined = new_df
        else:
            combined = new_df

        if combined.empty:
            result.status = "no_data"
            return result

        # Validate
        result.anomalies = validate_ticker(symbol, combined)
        if result.anomalies:
            log.info("%s — anomalies: %s", symbol, "; ".join(result.anomalies))

        # Save via the atomic helper so a crash mid-write can't leave
        # half-written parquet that breaks the next load. The rest of
        # the project follows this invariant ("atomic writes everywhere"
        # per the project memory); this call site was the lone holdout.
        config.atomic_write_parquet(combined, pq, engine="pyarrow")
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


# ── Parallel download primitive (Phase 3 I1) ──────────────────────────

class _RateLimiter:
    """Thread-safe leaky-bucket rate limiter. Enforces a minimum time
    between acquire() calls across all threads so concurrent workers can
    share a global rate cap without tripping yfinance rate limits."""

    def __init__(self, min_interval_sec: float):
        self._min_interval = float(min_interval_sec)
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._next_allowed - now
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
            self._next_allowed = now + self._min_interval


def download_many(
    symbols: list[str],
    *,
    max_workers: int = 6,
    min_interval_sec: Optional[float] = None,
    progress_cb: Optional[Callable[[ScrapeResult], None]] = None,
    stop_flag: Optional[Callable[[], bool]] = None,
) -> list[ScrapeResult]:
    """Download OHLCV for multiple tickers in parallel with a shared rate
    limit. progress_cb is invoked in completion order as each result
    arrives; the returned list is also in completion order.

    max_workers threads each call download_one; a shared rate limiter
    enforces ≤ 1 request per min_interval_sec (defaults to
    config.YFINANCE_PAUSE_SEC) across all threads, so total rate matches
    the serial path while network latency overlaps.

    stop_flag is a callable returning True to request cancellation — pending
    futures are cancelled, in-flight downloads are allowed to finish.
    """
    min_interval = (
        min_interval_sec if min_interval_sec is not None
        else config.YFINANCE_PAUSE_SEC
    )
    limiter = _RateLimiter(min_interval)

    def _work(sym: str) -> ScrapeResult:
        if stop_flag and stop_flag():
            return ScrapeResult(symbol=sym, status="stopped")
        limiter.acquire()
        if stop_flag and stop_flag():
            return ScrapeResult(symbol=sym, status="stopped")
        return download_one(sym)

    results: list[ScrapeResult] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_work, sym): sym for sym in symbols}
        for fut in as_completed(futures):
            try:
                res = fut.result()
            except Exception as exc:
                sym = futures[fut]
                res = ScrapeResult(symbol=sym, status="error", error_msg=str(exc))
            results.append(res)
            if progress_cb:
                progress_cb(res)
            if stop_flag and stop_flag():
                # Cancel any not-yet-started futures; in-flight ones finish
                for f in futures:
                    if not f.done():
                        f.cancel()
                break
    return results


# ── Load cached data ───────────────────────────────────────────────────

def _cache_key(symbol: str) -> tuple[str, int]:
    """Return (symbol, mtime_ns) — cache key that invalidates whenever the
    underlying parquet is rewritten (e.g. by a fresh incremental update)."""
    p = _parquet_path(symbol)
    if not p.exists():
        return (symbol, 0)
    try:
        return (symbol, p.stat().st_mtime_ns)
    except OSError:
        return (symbol, 0)


# Sized above a full US common-stock universe (~12k) plus the 13 reference
# benchmarks + sector ETFs, so a single scan over the whole universe doesn't
# evict entries it will re-read within the same pass (audit: the old 10000 cap
# under-fit a >10k-ticker universe). Keyed by (symbol, mtime_ns) so a bigger
# cap can never serve stale data — a fresh download bumps the key.
@lru_cache(maxsize=24000)
def _load_ohlcv_cached(key: tuple[str, int]) -> Optional[pd.DataFrame]:
    """Cached parquet read. Keyed by (symbol, mtime_ns) so a fresh download
    automatically bumps the key and triggers a re-read."""
    symbol, _mtime = key
    p = _parquet_path(symbol)
    if not p.exists():
        return None
    try:
        return pd.read_parquet(p)
    except Exception:
        log.warning("Corrupt parquet for %s — returning None", symbol)
        return None


def load_ohlcv(symbol: str) -> Optional[pd.DataFrame]:
    """Load cached OHLCV parquet for a symbol. Returns None if not found.

    Phase 3 I11: results are LRU-cached across calls keyed by mtime, so
    multi-scan sessions avoid re-reading the same parquet. Callers MUST
    copy() before mutating the returned DataFrame — mutations leak into
    the cache otherwise."""
    return _load_ohlcv_cached(_cache_key(symbol))


def clear_ohlcv_cache() -> None:
    """Clear the load_ohlcv LRU cache. Call after bulk operations that
    may rewrite many parquets (not strictly necessary since mtime-keying
    self-invalidates, but useful for freeing memory)."""
    _load_ohlcv_cached.cache_clear()


# ── Parquet schema stamp (Phase 4 R18) ──────────────────────────────────

def stamp_schema_version() -> None:
    """Write the current PARQUET_SCHEMA_VERSION to the sidecar file. Safe
    to call repeatedly — overwrites with the same value."""
    try:
        config.PARQUET_DIR.mkdir(parents=True, exist_ok=True)
        config.PARQUET_SCHEMA_FILE.write_text(
            str(config.PARQUET_SCHEMA_VERSION), encoding="utf-8"
        )
    except OSError as exc:
        log.debug("Could not write schema version file: %s", exc)


def read_schema_version() -> Optional[int]:
    """Return the schema version stamped in the sidecar, or None if absent
    or unreadable."""
    try:
        raw = config.PARQUET_SCHEMA_FILE.read_text(encoding="utf-8").strip()
        return int(raw)
    except (OSError, ValueError):
        return None


def check_schema_version() -> None:
    """Inspect the parquet cache's schema version and log any mismatch.
    Stamps the current version if none is recorded (assumes existing cache
    matches the current code — zero-migration default)."""
    stamped = read_schema_version()
    if stamped is None:
        if config.PARQUET_DIR.exists() and any(config.PARQUET_DIR.glob("*.parquet")):
            log.info("Parquet cache has no schema stamp; treating as v%d.",
                     config.PARQUET_SCHEMA_VERSION)
        stamp_schema_version()
    elif stamped != config.PARQUET_SCHEMA_VERSION:
        log.warning(
            "Parquet cache is schema v%d but this build expects v%d — "
            "some tickers may need a cache rebuild.",
            stamped, config.PARQUET_SCHEMA_VERSION,
        )


# ── Manual ticker-cache rebuild (Phase 4 R9) ────────────────────────────

def rebuild_ticker(symbol: str) -> ScrapeResult:
    """Re-download a ticker's full price history from scratch. Use when a past
    stock split left cached prices unadjusted, or to recover from a corrupt
    cache for a single ticker.

    Download-then-swap: the existing parquet is moved ASIDE (not deleted) so
    that download_one does a full pull; on success the backup is dropped, and
    on ANY failure the original is restored. This guarantees a failed rebuild
    (network error / rate-limit / no data) never leaves the ticker with NO
    cached history — the previous (delete-then-download) order could."""
    p = _parquet_path(symbol)
    bak = None
    if p.exists():
        bak = p.with_suffix(p.suffix + ".rebuild_bak")
        try:
            if bak.exists():
                bak.unlink()
            p.rename(bak)  # move aside so download_one does a FULL pull
            log.info("Moved cached parquet aside for rebuild: %s", symbol)
        except OSError as exc:
            log.warning("Could not move %s aside for rebuild: %s", p, exc)
            bak = None
    # Invalidate LRU entry for this (symbol, mtime) key path
    _load_ohlcv_cached.cache_clear()
    result = download_one(symbol)

    if bak is not None:
        if result.status == "ok" and p.exists():
            # Fresh file written — drop the backup.
            try:
                bak.unlink()
            except OSError:
                pass
        else:
            # Rebuild produced no usable file — restore the original so the
            # ticker isn't left without any cached history.
            try:
                if p.exists():
                    p.unlink()
                bak.rename(p)
                log.warning(
                    "Rebuild of %s did not produce data (status=%s) — restored "
                    "the prior cache", symbol, result.status,
                )
                _load_ohlcv_cached.cache_clear()
            except OSError as exc:
                log.error(
                    "Could not restore %s after a failed rebuild: %s", p, exc,
                )
    return result
