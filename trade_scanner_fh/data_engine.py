"""
Data Engine — OHLCV Download, Parquet Cache, Incremental Updates, Validation
=============================================================================

Public API
    download_all(symbols, progress_cb=None)   -> ScrapeReport   (serial; legacy)
    download_many(symbols, ...)               -> list[ScrapeResult]  (parallel)
    download_one(symbol)                      -> ScrapeResult
    load_ohlcv(symbol)                        -> pd.DataFrame | None
    prefetch_ohlcv(symbols, ...)              -> int   (cache warmer; F5)
    validate_ticker(symbol, df)               -> list[str]   (anomaly messages)

ScrapeReport / ScrapeResult are dataclasses with structured metadata.
"""

import logging
import re
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, NamedTuple, Optional

import numpy as np
import pandas as pd
import pyarrow.parquet as pq_reader
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

# Columns persisted to ohlcv/<TICKER>.parquet. `Dividends` is fetched (so
# download_one can detect an ex-dividend date and re-anchor the adjustment
# basis — audit INT-3) but deliberately NOT stored, keeping the on-disk schema
# identical to every file already in the cache.
_STORED_COLUMNS = ["Open", "High", "Low", "Close", "Volume", "Stock Splits"]

# Columns the scanner actually reads. Audit 2026-08-12 (EFF-5): `Stock Splits`
# is never touched at scan time, and projecting it away measured 10.4 ms →
# 1.9 ms per file (155 s → 28 s across the universe).
_SCAN_COLUMNS = ["Open", "High", "Low", "Close", "Volume"]


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
    """Return the most recent date in a ticker's cached parquet, or None.

    Audit 2026-08-12 (EFF-2): this used to `pd.read_parquet` the WHOLE file to
    read one timestamp. UpdateWorker calls it once per universe symbol at
    launch, serially — measured at 10.4 ms/file, so ~155 s of I/O and ~650 MB of
    decompression per launch across 14.9k tickers, purely to compare a max date.

    Parquet footers carry per-column min/max statistics, so the answer is
    available without touching a single data page. Falls back to the full read
    when statistics are absent (older writers) so behaviour is unchanged for
    any file the fast path can't answer. Mirrors the metadata-only pattern
    already used for row counts in main_window.
    """
    p = _parquet_path(symbol)
    if not p.exists():
        return None
    try:
        md = pq_reader.read_metadata(p)
    except Exception:
        md = None
    if md is not None:
        try:
            if md.num_rows == 0:
                return None
            # The Date index is written as a column; find it by name.
            schema = md.schema.to_arrow_schema()
            idx = schema.get_field_index("Date")
            if idx >= 0:
                best = None
                for rg in range(md.num_row_groups):
                    st = md.row_group(rg).column(idx).statistics
                    if st is None or not st.has_min_max:
                        best = None
                        break
                    if best is None or st.max > best:
                        best = st.max
                if best is not None:
                    return pd.Timestamp(best)
        except Exception:
            pass   # fall through to the full read
    try:
        df = pd.read_parquet(p)
        if df.empty:
            return None
        return pd.Timestamp(df.index.max())
    except Exception:
        return None


def _bars_after(index: pd.Index, cutoff: pd.Timestamp) -> "pd.Series | list":
    """Boolean mask of `index` entries strictly after `cutoff`.

    `_last_cached_date` returns a tz-AWARE timestamp from its full-read
    fallback but a tz-NAIVE one from the parquet-statistics fast path, while a
    freshly downloaded index is always tz-aware. Normalising both sides keeps
    the comparison from raising depending on which branch happened to run.
    """
    idx = index
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_localize(None)
    cut = pd.Timestamp(cutoff)
    if cut.tzinfo is not None:
        cut = cut.tz_localize(None)
    return idx > cut


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

    # Keep OHLCV + both corporate-action columns.
    #
    # Audit 2026-08-12 (INT-3): `Dividends` used to be dropped here, which is
    # why dividend-driven re-adjustment went undetected for the life of the
    # cache. It is retained through the download so download_one can spot an
    # ex-dividend date and re-anchor, and is dropped again before the parquet is
    # written (see _STORED_COLUMNS) so the on-disk schema is unchanged.
    keep = ["Open", "High", "Low", "Close", "Volume", "Stock Splits", "Dividends"]
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

    # 2. Non-positive prices. A traded security cannot print <= 0: zeros are
    #    missing-data placeholders the provider sent instead of a gap, and
    #    negatives come from a split/dividend back-adjustment overshooting the
    #    price. Neither is salvageable downstream — every indicator that
    #    touches such a bar produces garbage — so they are reported ahead of
    #    the softer statistical checks below.
    price_cols = [c for c in ("Open", "High", "Low", "Close") if c in df.columns]
    if price_cols:
        prices = df[price_cols].apply(pd.to_numeric, errors="coerce")
        neg_bars = int((prices < 0).any(axis=1).sum())
        zero_bars = int((prices == 0).any(axis=1).sum())
        if neg_bars > 0:
            issues.append(f"{neg_bars} bar(s) with negative price(s)")
        if zero_bars > 0:
            issues.append(f"{zero_bars} bar(s) with zero price(s)")

    # 3. OHLC bound violations — High must bound the bar from above and Low
    #    from below. Compared against a relative tolerance because the
    #    provider's adjusted prices are rounded, which puts Close a few 1e-6
    #    above High often enough that an exact comparison fires on ~21% of the
    #    store and buries the real artifacts (see OHLC_INVARIANT_TOL_PCT).
    if {"Open", "High", "Low", "Close"}.issubset(df.columns):
        o, h, l, c = (
            pd.to_numeric(df[col], errors="coerce")
            for col in ("Open", "High", "Low", "Close")
        )
        level = pd.concat([o, h, l, c], axis=1).abs().max(axis=1)
        tol = level * (config.OHLC_INVARIANT_TOL_PCT / 100.0)
        bad = (
            (l - h > tol) | (o - h > tol) | (c - h > tol)
            | (l - o > tol) | (l - c > tol)
        ).fillna(False)
        if bad.any():
            dates_str = ", ".join(str(d.date()) for d in df.index[bad][:5])
            issues.append(
                f"{int(bad.sum())} bar(s) violating OHLC bounds "
                f"(first: {dates_str})"
            )

    # 4. Duplicate dates
    dup_count = df.index.duplicated().sum()
    if dup_count > 0:
        issues.append(f"{dup_count} duplicate date(s)")

    # 5. Zero-volume bars
    if "Volume" in df.columns:
        zero_vol = (df["Volume"] == 0).sum()
        if zero_vol > 0:
            issues.append(f"{zero_vol} zero-volume bar(s)")

    # 6. Price jumps exceeding threshold
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

    # 7. Missing trading days (gaps > 4 calendar days ≈ long weekends ok)
    if len(df) > 1:
        date_diffs = pd.Series(df.index).diff().dt.days
        # Weekends = 3 days gap is normal; flag runs > threshold
        big_gaps = date_diffs[date_diffs > config.MAX_MISSING_DAYS_FLAG + 2]
        if len(big_gaps) > 0:
            issues.append(f"{len(big_gaps)} suspicious date gap(s) in history")

    return issues


def _reject_conflicting_bars(
    symbol: str, old_df: pd.DataFrame, new_df: pd.DataFrame,
) -> pd.DataFrame:
    """Drop incoming bars that contradict an already-cached bar for the same
    date by more than ``config.PRICE_JUMP_PCT`` (audit 2026-08-12, INT-11).

    Only bars whose date ALREADY exists in the cache are considered — a normal
    incremental update appends new dates and is untouched. Overlap happens when
    a provider re-sends the boundary bar, which is exactly where a bad response
    used to overwrite good data permanently.

    Conservative by construction: a disagreement is resolved in favour of the
    data already on disk, and anything unexpected (missing Close, empty
    overlap, a comparison that raises) returns ``new_df`` unchanged so this can
    never make an update fail.
    """
    try:
        if "Close" not in old_df.columns or "Close" not in new_df.columns:
            return new_df
        overlap = new_df.index.intersection(old_df.index)
        if overlap.empty:
            return new_df
        old_close = pd.to_numeric(old_df.loc[overlap, "Close"], errors="coerce")
        new_close = pd.to_numeric(new_df.loc[overlap, "Close"], errors="coerce")
        base = old_close.abs()
        diff_pct = ((new_close - old_close).abs() / base.where(base > 0)) * 100.0
        conflicting = overlap[diff_pct > config.PRICE_JUMP_PCT]
        if len(conflicting) == 0:
            return new_df
        log.warning(
            "%s — %d re-sent bar(s) disagree with the cache by >%.0f%%; "
            "keeping the cached value(s) for %s",
            symbol, len(conflicting), config.PRICE_JUMP_PCT,
            ", ".join(str(d.date()) for d in conflicting[:5]),
        )
        return new_df.drop(index=conflicting)
    except Exception as exc:      # never let a guard break an update
        log.debug("%s — bar-conflict check skipped: %s", symbol, exc)
        return new_df


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
            # Incremental: re-request a short tail of already-cached bars along
            # with the new ones.
            #
            # This used to start at `last_date + 1 day`, which meant the last
            # cached date was never asked for again — and the provider's most
            # recent daily bar is still provisional for hours after the close.
            # Whatever a post-close refill captured that evening was therefore
            # PERMANENT. Measured 2026-08-13 on 60 random tickers against the
            # bars written by the 2026-08-12 17:21-19:10 refill: Volume
            # differed on 60/60 (52 understated — median 1.2%, p90 22.6%, IP
            # 2,894,764 vs 4,992,100), High on 30/60, Low on 31/60, and 854
            # tickers store-wide ended up with Open outside [Low, High]. That
            # corrupts RVOL, ADR% and ATR on the most recent session.
            #
            # Overlapping the window lets the finalized bars replace the
            # provisional ones. Safe by construction: `_reject_conflicting_bars`
            # still refuses any re-sent bar disagreeing by more than
            # PRICE_JUMP_PCT, and the `keep="last"` dedup below adopts
            # normal-sized corrections.
            start = (
                last_date
                - timedelta(days=config.OHLCV_REFETCH_OVERLAP_DAYS)
            ).strftime("%Y-%m-%d")
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

        # If incremental data contains a stock split OR a dividend, the cached
        # prices are stale. Re-download the full history so yfinance returns
        # properly adjusted prices everywhere.
        #
        # Audit 2026-08-12 (INT-3): dividends were NOT detected here, and the
        # `Dividends` column was dropped in _download_raw before it could be
        # inspected. Because `auto_adjust=True` back-adjusts for dividends as
        # well as splits, every ex-dividend date left a PERMANENT price
        # discontinuity inside the cached file: bars written before that update
        # kept the old adjustment basis while newly-appended bars used the new
        # one. Measured on 20 dividend payers — 19 drifted >0.5% from a fresh
        # pull (mean 0.79%, worst VZ at 1.76%), with the seam locatable to an
        # exact date (e.g. CSCO +0.374% before 2026-07-06, exact after). Any
        # SMA / ADR% / ATR / RS window spanning a seam mixed two price bases.
        #
        # Detecting dividends re-anchors the same way splits already did, and
        # the existing cache SELF-HEALS: the next dividend for each payer
        # triggers one full re-download, so within ~a quarter every affected
        # ticker is corrected with no bulk operation and no data deletion.
        corp_action = None
        if last_date is not None and not new_df.empty:
            # Only actions AFTER the last cached bar are new. The refetch
            # overlap above deliberately re-downloads bars already on disk, so
            # without this filter a dividend sitting inside the overlap window
            # would re-trigger the full-history re-anchor on every run until it
            # aged out — turning one 5-year re-download per ex-date into one
            # per day for OHLCV_REFETCH_OVERLAP_DAYS days, across every payer.
            unseen = new_df.loc[_bars_after(new_df.index, last_date)]
            if ("Stock Splits" in unseen.columns
                    and (unseen["Stock Splits"] != 0).any()):
                corp_action = ("split", unseen.loc[
                    unseen["Stock Splits"] != 0
                ].index.tolist())
            elif ("Dividends" in unseen.columns
                    and (unseen["Dividends"] != 0).any()):
                corp_action = ("dividend", unseen.loc[
                    unseen["Dividends"] != 0
                ].index.tolist())
        if corp_action is not None:
            kind, action_dates = corp_action
            log.info(
                "%s — %s detected on %s, re-downloading full history "
                "(re-anchors the adjustment basis)",
                symbol, kind,
                ", ".join(str(d.date()) for d in action_dates),
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
                # Audit 2026-08-12 (INT-11): gate the MERGE, not just the log.
                # `keep="last"` meant a bad yfinance response silently
                # overwrote a good cached bar and became load-bearing forever
                # — OHLCV parquets carry no source or fetch-time column, so a
                # cemented bad bar can't be identified or re-fetched
                # afterwards (the persisted CSCO 2026-06-12 anomaly is a live
                # instance). When a re-sent bar disagrees with the cached one
                # by more than PRICE_JUMP_PCT, keep the CACHED bar and warn.
                # Genuine corrections arrive via the split/dividend re-anchor
                # above, which replaces the whole file rather than one bar.
                new_df = _reject_conflicting_bars(symbol, old_df, new_df)
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

        # Drop the corporate-action helper column before persisting so the
        # on-disk schema is exactly what it has always been (audit INT-3).
        combined = combined[[c for c in _STORED_COLUMNS if c in combined.columns]]

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


def _read_scan_frame(p: Path) -> pd.DataFrame:
    """Read one OHLCV parquet in the shape the scanner wants.

    Three audit fixes land here, all in the hottest read path in the app:

    * EFF-5 — project to the five columns the scanner reads. `Stock Splits` is
      stored but never used at scan time; dropping it measured 10.4 ms → 1.9 ms
      per file, i.e. ~155 s → ~28 s of pure I/O across 14.9k tickers.
    * EFF-4 — strip the timezone ONCE, here. Every cached file has a
      `datetimetz America/New_York` index, so the "defensive" tz branch in
      scanner._compute_ticker fired on 100% of tickers and full-copied every
      frame, per timeframe. Handing it a naive index makes that branch dead.
    * EFF-6 — downcast OHLC to float32. Every consumer takes means, ratios or
      comparisons; none needs float64, and this halves the cached footprint.

    Volume stays integral (int64 → int32 would risk overflow on high-volume
    names; float32 would lose exactness on large share counts).
    """
    try:
        df = pd.read_parquet(p, columns=_SCAN_COLUMNS)
    except Exception:
        # pyarrow raises rather than projecting when a requested column is
        # absent, so a file that predates the current writer — or any partial
        # frame — would be swallowed by the caller's except and reported as
        # "corrupt", silently dropping that ticker from every scan. Fall back to
        # the full read and keep whatever OHLCV columns are actually present.
        # Only odd files pay the double read; the 14.9k well-formed ones don't.
        df = pd.read_parquet(p)
        present = [c for c in _SCAN_COLUMNS if c in df.columns]
        if present:
            df = df[present]
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_localize(None)
    for col in ("Open", "High", "Low", "Close"):
        if col in df.columns and df[col].dtype == "float64":
            df[col] = df[col].astype("float32")
    return df


# Sized above a full US common-stock universe (~12k) plus the reference
# benchmarks + sector ETFs, so a single scan over the whole universe doesn't
# evict entries it will re-read within the same pass.
#
# Audit 2026-08-12 (EFF-6): keyed by SYMBOL, with the mtime stored alongside the
# frame. The old design keyed the lru_cache on (symbol, mtime_ns), which
# guaranteed freshness but meant a re-downloaded ticker ADDED an entry while the
# stale-mtime entry stayed resident — verified at 4 cached frames for one ticker
# after 3 refreshes, and a 1.61 GB ceiling at the 24k cap. Keying by symbol makes
# a refresh REPLACE, so the cap is a true per-ticker bound.
_OHLCV_CACHE_MAX = 24000
_ohlcv_cache: "OrderedDict[str, tuple[int, Optional[pd.DataFrame]]]" = OrderedDict()
_ohlcv_cache_lock = threading.Lock()
_ohlcv_hits = 0
_ohlcv_misses = 0


def _load_ohlcv_cached(key: tuple[str, int]) -> Optional[pd.DataFrame]:
    """Cached parquet read, keyed by symbol and validated against mtime.

    Takes the same ``(symbol, mtime_ns)`` tuple the previous lru_cache version
    did, so existing callers and tests are unaffected.
    """
    global _ohlcv_hits, _ohlcv_misses
    symbol, mtime = key
    with _ohlcv_cache_lock:
        hit = _ohlcv_cache.get(symbol)
        if hit is not None and hit[0] == mtime:
            _ohlcv_cache.move_to_end(symbol)
            _ohlcv_hits += 1
            return hit[1]
    p = _parquet_path(symbol)
    if not p.exists():
        frame = None
    else:
        try:
            frame = _read_scan_frame(p)
        except Exception:
            log.warning("Corrupt parquet for %s — returning None", symbol)
            frame = None
    with _ohlcv_cache_lock:
        _ohlcv_misses += 1
        # Replaces any stale-mtime entry for this symbol instead of adding to it.
        _ohlcv_cache[symbol] = (mtime, frame)
        _ohlcv_cache.move_to_end(symbol)
        while len(_ohlcv_cache) > _OHLCV_CACHE_MAX:
            _ohlcv_cache.popitem(last=False)
    return frame


class OhlcvCacheInfo(NamedTuple):
    """Field-for-field replacement for ``functools._CacheInfo``, in the same
    ORDER as well — callers that unpack positionally must not silently swap
    maxsize and currsize when the lru_cache went away (audit EFF-6)."""
    hits: int
    misses: int
    maxsize: int
    currsize: int


def ohlcv_cache_info() -> OhlcvCacheInfo:
    """Cache statistics, drop-in compatible with the old
    ``_load_ohlcv_cached.cache_info()`` for the diagnostics panel and tests."""
    with _ohlcv_cache_lock:
        return OhlcvCacheInfo(
            _ohlcv_hits, _ohlcv_misses, _OHLCV_CACHE_MAX, len(_ohlcv_cache),
        )


def cached_symbols() -> set[str]:
    """Every symbol with an OHLCV parquet on disk, from ONE directory listing.

    Audit 2026-08-12 (EFF-8): the launch path called
    ``(PARQUET_DIR / f"{s}.parquet").exists()`` once per universe symbol —
    15,948 individual stat calls, repeated a second time in ``_on_update_done``
    — on the GUI thread. A single glob answers the same question with one
    directory enumeration.
    """
    try:
        return {p.stem for p in config.PARQUET_DIR.glob("*.parquet")}
    except OSError as exc:
        log.warning("Could not enumerate %s: %s", config.PARQUET_DIR, exc)
        return set()


def load_ohlcv(symbol: str) -> Optional[pd.DataFrame]:
    """Load cached OHLCV parquet for a symbol. Returns None if not found.

    Phase 3 I11: results are cached across calls and validated by mtime, so
    multi-scan sessions avoid re-reading the same parquet. Callers MUST
    copy() before mutating the returned DataFrame — mutations leak into
    the cache otherwise.

    The returned frame is tz-NAIVE and carries only OHLCV (audit EFF-4 / EFF-5).
    """
    return _load_ohlcv_cached(_cache_key(symbol))


def clear_ohlcv_cache() -> None:
    """Drop every cached OHLCV frame. Call after bulk operations that rewrite
    many parquets, or to release memory."""
    global _ohlcv_hits, _ohlcv_misses
    with _ohlcv_cache_lock:
        _ohlcv_cache.clear()
        _ohlcv_hits = 0
        _ohlcv_misses = 0


def prefetch_ohlcv(
    symbols: list[str],
    *,
    max_workers: int = 4,
    stop_flag: Optional[threading.Event] = None,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> int:
    """Warm the load_ohlcv LRU cache for `symbols` on a thread pool (F5).

    A cold first scan is ~85% parquet I/O (~8.7 ms/ticker); pre-loading the
    cache in the background hides that cost. Pure cache warmer — no network,
    no writes, and results are discarded (the lru_cache retains them keyed
    by (symbol, mtime_ns), so a later load_ohlcv is a hit unless the parquet
    was rewritten in between).

    Args:
        symbols: tickers to warm. An empty list is a no-op returning 0.
        max_workers: thread-pool width (parquet reads release the GIL).
        stop_flag: optional threading.Event — checked between submissions
            and again at the start of each worker, so setting it aborts
            promptly (already-loading symbols finish; the rest are skipped).
        progress_cb: optional callable(done, total), invoked in completion
            order as each symbol finishes (warmed, skipped, or failed).
            Called from this (caller's) thread, not the workers.

    Returns:
        Number of symbols actually warmed (parquet existed and loaded
        cleanly). Missing/corrupt parquets and unexpected per-symbol load
        errors are swallowed (debug-logged) and simply not counted.
    """
    if not symbols:
        return 0

    def _warm(sym: str) -> bool:
        if stop_flag is not None and stop_flag.is_set():
            return False
        try:
            return load_ohlcv(sym) is not None
        except Exception as exc:
            log.debug("prefetch_ohlcv: load failed for %s — %s", sym, exc)
            return False

    total = len(symbols)
    warmed = 0
    done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = []
        for sym in symbols:
            if stop_flag is not None and stop_flag.is_set():
                break
            futures.append(ex.submit(_warm, sym))
        for fut in as_completed(futures):
            if fut.result():  # _warm never raises
                warmed += 1
            done += 1
            if progress_cb:
                progress_cb(done, total)
    return warmed


# ── Parquet schema stamp (Phase 4 R18) ──────────────────────────────────

def stamp_schema_version() -> None:
    """Write the current PARQUET_SCHEMA_VERSION to the sidecar file. Safe
    to call repeatedly — overwrites with the same value."""
    try:
        config.PARQUET_DIR.mkdir(parents=True, exist_ok=True)
        # Audit 2026-08-12 (INT-17): atomic, like every other store in the
        # project. A crash mid-write left a truncated sidecar, which
        # read_schema_version then reports as a version MISMATCH against a
        # cache that is actually fine.
        config.atomic_write_text(
            config.PARQUET_SCHEMA_FILE, str(config.PARQUET_SCHEMA_VERSION),
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
    clear_ohlcv_cache()
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
                clear_ohlcv_cache()
            except OSError as exc:
                log.error(
                    "Could not restore %s after a failed rebuild: %s", p, exc,
                )
    return result
