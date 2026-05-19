"""QThread workers for background tasks: scan, universe refresh, OHLCV
update, sector/earnings fill, and TradeStation watchlist bridge."""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Union
from zoneinfo import ZoneInfo

import pandas as pd
from PyQt6.QtCore import QThread, pyqtSignal

from .. import config
from ..data_engine import (
    _last_cached_date, download_many, download_one,
)
from ..scanner import ScanParams, ScanResult, run_scan
from ..ticker_universe import refresh_universe
from ..tradestation import BridgeConfig, TradeStationBridge

log = logging.getLogger("scanner.gui")


@dataclass
class WorkerScanResult:
    """Result emitted by ScanWorker when one or more scans finish.

    Per-period DataFrames are kept separately rather than merged/deduped —
    the GUI's timeframe-selector dropdown picks which one to display, and
    the Excel export can write each as its own sheet.
    """
    period_results: dict[str, pd.DataFrame] = field(default_factory=dict)
    period_order: list[str] = field(default_factory=list)
    errors: list = field(default_factory=list)
    elapsed_sec: float = 0.0
    sequenced: bool = False

    def total_unique_symbols(self) -> set[str]:
        """Union of `symbol` across every period's DataFrame."""
        seen: set[str] = set()
        for df in self.period_results.values():
            if df is not None and not df.empty and "symbol" in df.columns:
                seen.update(df["symbol"].tolist())
        return seen


# ============================================================================
# Background scan worker
# ============================================================================

class ScanWorker(QThread):
    """Runs run_scan() on a background thread.
    Accepts a single ScanParams or a list for multi-timeframe scanning."""

    progress = pyqtSignal(int, int, str)   # done, total, symbol
    finished = pyqtSignal(object)          # ScanResult
    log_msg = pyqtSignal(str)              # for log panel

    def __init__(
        self,
        symbols: list[str],
        params: Union[ScanParams, list[ScanParams], list[tuple[str, ScanParams]]],
        sequenced: bool = False,
    ):
        super().__init__()
        self.symbols = symbols
        # Normalize params to list[(label, ScanParams)]. Bare ScanParams or
        # list[ScanParams] are wrapped with a default label derived from the
        # date range — keeps backward compat with any external callers and
        # tests that pass naked params.
        self.params_list: list[tuple[str, ScanParams]] = self._normalize(params)
        self.sequenced = sequenced
        # Phase 4 R16: Event instead of bool for cross-thread stop signaling
        self._stop = threading.Event()

    @staticmethod
    def _normalize(params) -> list[tuple[str, ScanParams]]:
        if isinstance(params, ScanParams):
            return [(f"{params.start_date} → {params.end_date}", params)]
        out: list[tuple[str, ScanParams]] = []
        for entry in params:
            if isinstance(entry, ScanParams):
                out.append((f"{entry.start_date} → {entry.end_date}", entry))
            else:
                # already (label, ScanParams)
                out.append(entry)
        return out

    def run(self):
        # Outer try/except: under PyInstaller windowed mode on Windows,
        # an uncaught exception in QThread.run() can abort the whole
        # process via Qt's fatal-error path rather than just killing
        # the worker. Convert any escaped exception to a logged-and-
        # finished verdict so the GUI receives a `finished` signal even
        # when the scan crashed mid-flight; partial period_results from
        # any earlier completed timeframes still surface.
        period_results: dict[str, pd.DataFrame] = {}
        period_order: list[str] = []
        all_errors: list = []
        total_elapsed = 0.0
        n = len(self.params_list)

        try:
            for idx, (label, params) in enumerate(self.params_list, 1):
                if self._stop.is_set():
                    break

                self.log_msg.emit(f"Timeframe [{idx}/{n}] {label}")

                def cb(done, total, sym, _lbl=label):
                    self.progress.emit(done, total, f"{_lbl}  {sym}")

                # Phase 4 R2: cancel_token lets run_scan interrupt within its
                # per-ticker compute loop (not just between timeframes)
                result = run_scan(
                    self.symbols, params,
                    progress_cb=cb,
                    cancel_token=self._stop.is_set,
                )
                total_elapsed += result.elapsed_sec
                all_errors.extend(result.errors)

                df = result.results_df
                if df is None:
                    df = pd.DataFrame()
                # Sort each period's results by pct_gain descending so the
                # table view is immediately useful when the user picks a period
                if not df.empty and "pct_gain" in df.columns:
                    df = df.sort_values("pct_gain", ascending=False).reset_index(drop=True)

                period_results[label] = df
                period_order.append(label)
        except Exception as exc:
            log.error("ScanWorker crashed: %s", exc, exc_info=True)
            self.log_msg.emit(f"Scan error: {exc} — partial results may be available.")
            all_errors.append({
                "symbol": "<scan>",
                "error": str(exc),
                "traceback": __import__("traceback").format_exc(),
            })

        final = WorkerScanResult(
            period_results=period_results,
            period_order=period_order,
            errors=all_errors,
            elapsed_sec=total_elapsed,
            sequenced=self.sequenced,
        )
        # Emit `finished` even after a crash — the slot needs to know
        # the scan ended so it can re-enable the Scan button.
        try:
            self.finished.emit(final)
        except Exception as exc:
            log.error("ScanWorker.finished.emit failed: %s", exc, exc_info=True)

    def request_stop(self):
        self._stop.set()


# ============================================================================
# Universe refresh workers
# ============================================================================

class UniverseWorker(QThread):
    """Downloads the ticker universe CSV in the background."""

    finished = pyqtSignal(object)  # pd.DataFrame or None
    log_msg = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        # Phase 4 R3: expose request_stop so closeEvent doesn't need
        # QThread.terminate(). The underlying refresh_universe call is a
        # blocking FTP/HTTP sequence — the flag is advisory and only takes
        # effect at the next source boundary, but having it means we can
        # wait() gracefully rather than killing the thread.
        self._stop = threading.Event()

    def request_stop(self):
        self._stop.set()

    def run(self):
        try:
            self.log_msg.emit("Downloading ticker universe (first run)...")
            df = refresh_universe(force=True, skip_validation=True)
            self.log_msg.emit(f"Universe downloaded: {len(df)} tickers")
            self.finished.emit(df)
        except Exception as exc:
            self.log_msg.emit(f"Universe download failed: {exc}")
            self.finished.emit(None)


class UniverseRefreshWorker(QThread):
    """Refreshes the universe CSV if stale (>7 days). Fast no-op if fresh."""

    finished = pyqtSignal(object)
    log_msg = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self._stop = threading.Event()

    def request_stop(self):
        self._stop.set()

    def run(self):
        try:
            df = refresh_universe(force=False, skip_validation=True)
            self.log_msg.emit(f"Universe: {len(df)} tickers")
            self.finished.emit(df)
        except Exception as exc:
            self.log_msg.emit(f"Universe refresh failed: {exc}")
            self.finished.emit(None)


# ============================================================================
# OHLCV update worker (incremental daily refresh)
# ============================================================================

class UpdateWorker(QThread):
    """Incrementally updates OHLCV parquet files in the background."""

    progress = pyqtSignal(int, int)     # done, total
    finished = pyqtSignal(int, int)     # updated, errors
    error_tickers = pyqtSignal(list)    # list of tickers that failed
    log_msg = pyqtSignal(str)

    def __init__(self, symbols: list[str], *,
                 backoff_enabled_ref: list | None = None,
                 backoff_threshold: int = 10,
                 backoff_wait: int = 30,
                 max_retries: int = 3):
        super().__init__()
        self.symbols = symbols
        # Mutable container so the GUI toggle takes effect immediately
        self._backoff_enabled_ref = backoff_enabled_ref or [True]
        self.backoff_threshold = backoff_threshold
        self.backoff_wait = backoff_wait
        self.max_retries = max_retries
        # Phase 4 R16: Event instead of bool for documented thread safety
        self._stop = threading.Event()
        self._failed_tickers: list[str] = []

    def request_stop(self):
        self._stop.set()

    def run(self):
        try:
            self._do_update()
        except Exception as exc:
            log.error("UpdateWorker crashed: %s", exc, exc_info=True)
            self.log_msg.emit(f"OHLCV update error: {exc}")
            self.finished.emit(0, 0)

    def _do_update(self):
        now_et = datetime.now(ZoneInfo("America/New_York"))
        today_et = now_et.date()
        market_closed = now_et.hour >= 17  # 5 PM ET (buffer past 4 PM close)

        # Determine the most recent trading day we should have data for
        if market_closed:
            target_date = today_et
        else:
            target_date = today_et - timedelta(days=1)

        # Walk back to the most recent NYSE trading day (weekends + holidays)
        target_date = config.most_recent_trading_day(target_date)

        target = pd.Timestamp(target_date)

        self.log_msg.emit("Checking OHLCV staleness...")
        log.info("Checking OHLCV staleness for %d tickers "
                 "(target date: %s, market_closed: %s)",
                 len(self.symbols), target_date, market_closed)

        stale = []
        missing = []
        for sym in self.symbols:
            last = _last_cached_date(sym)
            if last is None:
                missing.append(sym)
            else:
                # Normalize to tz-naive for comparison
                if last.tzinfo is not None:
                    last = last.tz_localize(None)
                if last < target:
                    stale.append(sym)

        to_update = stale + missing
        if not to_update:
            self.log_msg.emit("OHLCV data is up to date.")
            log.info("OHLCV data is up to date.")
            self.finished.emit(0, 0)
            return

        msg = (f"OHLCV update: {len(stale)} stale, {len(missing)} missing "
               f"-> updating {len(to_update)} tickers...")
        self.log_msg.emit(msg)
        log.info(msg)

        # Phase 3 I1: parallel download with shared rate limiter. Tickers
        # are processed in BATCH_SIZE chunks so rate-limit backoff can fire
        # between batches without cancelling the current one mid-flight.
        BATCH_SIZE = 200
        WORKERS = 6
        total = len(to_update)
        updated = 0
        errors = 0
        completed = 0
        max_retries = self.max_retries
        backoff_threshold = self.backoff_threshold
        backoff_wait_base = self.backoff_wait

        def _on_result(res):
            """Invoked from the as_completed loop for each finished ticker.
            Runs in the UpdateWorker thread (not a pool worker), so list
            mutation and signal emission are safe."""
            nonlocal updated, errors, completed
            completed += 1
            if res.status == "ok":
                updated += 1
            elif res.status == "stopped":
                pass  # cancelled via stop_flag — don't count as error
            else:
                errors += 1
                self._failed_tickers.append(res.symbol)

            if completed % 200 == 0 or completed == total:
                self.progress.emit(completed, total)
                self.log_msg.emit(
                    f"OHLCV update: {completed}/{total} "
                    f"({updated} ok, {errors} err)"
                )
                log.info("OHLCV update: %d/%d (%d ok, %d err)",
                         completed, total, updated, errors)

        batch_start = 0
        while batch_start < total:
            if self._stop.is_set():
                self.log_msg.emit(f"OHLCV update stopped at {completed}/{total}")
                break

            batch = to_update[batch_start:batch_start + BATCH_SIZE]
            batch_results = download_many(
                batch,
                max_workers=WORKERS,
                min_interval_sec=config.YFINANCE_PAUSE_SEC,
                progress_cb=_on_result,
                stop_flag=self._stop.is_set,
            )
            batch_start += len(batch)

            # Rate-limit detection — if a batch had many failures, back off
            # before starting the next one.
            batch_errors = sum(1 for r in batch_results if r.status == "error")
            if (self._backoff_enabled_ref[0]
                    and batch_errors >= backoff_threshold
                    and batch_start < total
                    and not self._stop.is_set()):
                backoff_cleared = False
                for retry in range(1, max_retries + 1):
                    if self._stop.is_set():
                        break
                    wait = backoff_wait_base * retry
                    self.log_msg.emit(
                        f"Rate limited ({batch_errors}/{len(batch)} errors "
                        f"in batch). Backing off {wait}s "
                        f"(retry {retry}/{max_retries})..."
                    )
                    log.warning("Rate limit detected, backing off %ds", wait)

                    # Event.wait() returns early if stop is set — more
                    # responsive than chunked time.sleep().
                    if self._stop.wait(timeout=wait):
                        break

                    # Probe with a single serial download to check if unblocked
                    probe_sym = to_update[batch_start]
                    try:
                        probe = download_one(probe_sym)
                        if probe.status == "ok":
                            updated += 1
                            completed += 1
                            batch_start += 1
                            self.log_msg.emit("Rate limit cleared, resuming...")
                            log.info("Rate limit cleared after %ds backoff", wait)
                            backoff_cleared = True
                            break
                    except Exception:
                        pass

                if not backoff_cleared and not self._stop.is_set():
                    self.log_msg.emit(
                        f"Rate limit persists after {max_retries} retries. "
                        f"Stopping update. Will retry remaining on next launch."
                    )
                    log.warning("Rate limit persists, stopping update")
                    break

        msg = f"OHLCV update complete: {updated} updated, {errors} errors"
        self.log_msg.emit(msg)
        log.info(msg)
        self.error_tickers.emit(self._failed_tickers)
        self.finished.emit(updated, errors)


# ============================================================================
# Sector / Earnings fill workers
# ============================================================================

class SectorFillWorker(QThread):
    """Background worker for sector map fill operations."""

    progress = pyqtSignal(int, int)
    finished = pyqtSignal(int, int)  # filled, errors
    log_msg = pyqtSignal(str)

    def __init__(self, symbols: list[str], blacklist: set[str], mode: str = "bulk"):
        super().__init__()
        self.symbols = symbols
        self.blacklist = blacklist
        self.mode = mode
        # Retained as [bool] because bulk_fill_sectors / targeted_fill_sectors
        # accept `stop_flag: list[bool]` — changing that signature would ripple
        # into sector_map.py and is out of scope for this phase.
        self._stop = [False]

    def request_stop(self):
        self._stop[0] = True

    def run(self):
        from ..sector_map import bulk_fill_sectors, targeted_fill_sectors, load_sector_map
        try:
            if self.mode == "bulk":
                self.log_msg.emit("Starting bulk sector fill (FinanceDatabase)...")
                filled, errors = bulk_fill_sectors(
                    self.symbols, self.blacklist,
                    progress_cb=lambda d, t: self.progress.emit(d, t),
                    stop_flag=self._stop,
                )
                if filled == 0 and errors == 0:
                    self.log_msg.emit(
                        "WARNING: Sector fill returned no data. "
                        "Is 'financedatabase' installed? "
                        "Run: pip install financedatabase"
                    )
            else:
                # Find gaps
                existing = load_sector_map()
                mapped = set(existing["ticker"]) if existing is not None else set()
                gaps = [s for s in self.symbols if s not in mapped and s not in self.blacklist]
                self.log_msg.emit(f"Targeted sector fill: {len(gaps)} gaps to process")
                filled, errors = targeted_fill_sectors(
                    gaps, self.blacklist,
                    progress_cb=lambda d, t: self.progress.emit(d, t),
                    stop_flag=self._stop,
                )
            self.log_msg.emit(f"Sector fill done: {filled} filled, {errors} errors/no-data")
            self.finished.emit(filled, errors)
        except Exception as exc:
            self.log_msg.emit(f"Sector fill error: {exc}")
            self.finished.emit(0, 0)


class EarningsFillWorker(QThread):
    """Background worker for earnings date fill operations."""

    progress = pyqtSignal(int, int)
    finished = pyqtSignal(int, int)  # filled, errors
    log_msg = pyqtSignal(str)

    def __init__(self, symbols: list[str], blacklist: set[str], mode: str = "bulk"):
        super().__init__()
        self.symbols = symbols
        self.blacklist = blacklist
        self.mode = mode
        self._stop = [False]

    def request_stop(self):
        self._stop[0] = True

    def run(self):
        # Phase 3: bulk uses nasdaq_fill (was earnings_cache.bulk_fill_earnings)
        # and targeted uses yahoo_fill (was earnings_cache.targeted_fill_earnings
        # which chained Finnhub→yfinance; the Finnhub branch has been removed
        # — Finnhub now has its own dedicated worker / fill module).
        from ..earnings_cache import load_earnings_cache
        from ..nasdaq_fill import bulk_fill_nasdaq
        from ..yahoo_fill import targeted_fill_yahoo
        try:
            if self.mode == "bulk":
                self.log_msg.emit(
                    "Starting bulk earnings fill (Nasdaq calendar, ±90 days)..."
                )
                filled, errors = bulk_fill_nasdaq(
                    self.symbols, self.blacklist,
                    progress_cb=lambda d, t: self.progress.emit(d, t),
                    stop_flag=self._stop,
                )
            else:
                # Find gaps (no entry or stale > 30 days)
                existing = load_earnings_cache()
                mapped = set(existing["ticker"]) if existing is not None else set()
                gaps = [s for s in self.symbols
                        if s not in mapped and s not in self.blacklist]
                self.log_msg.emit(
                    f"Targeted Yahoo fill: {len(gaps)} gaps to process"
                )
                filled, errors = targeted_fill_yahoo(
                    gaps, self.blacklist,
                    progress_cb=lambda d, t: self.progress.emit(d, t),
                    stop_flag=self._stop,
                )
            self.log_msg.emit(
                f"Earnings fill done: {filled} filled, {errors} errors/no-data"
            )
            self.finished.emit(filled, errors)
        except Exception as exc:
            self.log_msg.emit(f"Earnings fill error: {exc}")
            self.finished.emit(0, 0)


# ============================================================================
# Zacks earnings fill worker (Phase 5 + 6)
# ============================================================================

class ZacksFillWorker(QThread):
    """Background worker for Zacks earnings history fills.

    Modes:
        "bulk"     — full universe pull via bulk_fill_zacks
        "targeted" — caller-supplied gap_tickers list via targeted_fill_zacks
        "smart"    — auto-computes candidates via find_smart_refresh_candidates
                     (Phase 5 daily refresh)

    Skips the run gracefully when no Zacks cookies are configured rather
    than letting every ticker fail with an Imperva block — keeps the
    launch sequence quiet on a fresh install.
    """

    progress = pyqtSignal(int, int)
    finished = pyqtSignal(int, int, list)  # filled, errors, candidates
    log_msg = pyqtSignal(str)
    skipped = pyqtSignal(str)              # reason — when the run is short-circuited
    # Imperva block auto-pause: emitted from the worker thread when N
    # consecutive failures hit. The slot must call `resume_after_block()`
    # or `cancel_after_block()` once the user has resolved the dialog.
    imperva_block_detected = pyqtSignal(int)  # consecutive failure count
    # Per-ticker failure breakdown emitted at end of run. Maps each
    # FAIL_* sentinel ("blocked" / "not_found" / "http_error" /
    # "parse_error" / "unknown") to the list of tickers that failed
    # for that reason. Lets the GUI distinguish ETF/ADR coverage gaps
    # (legitimate, blacklist candidates) from Imperva blocks (cookie
    # refresh needed) from network glitches.
    failure_breakdown = pyqtSignal(dict)

    def __init__(
        self,
        symbols: list[str],
        blacklist: set[str],
        mode: str = "smart",
        *,
        delay_sec: float = 1.5,
        flush_every: int = 25,
        years: int = 5,
        consec_error_limit: int = 5,
    ):
        super().__init__()
        self.symbols = symbols
        self.blacklist = blacklist
        self.mode = mode
        self.delay_sec = delay_sec
        self.flush_every = flush_every
        self.years = years
        self.consec_error_limit = consec_error_limit
        # Audit L1: this worker mixes two stop-flag patterns. The
        # underlying earnings_history fill API accepts `stop_flag:
        # list[bool]` — same shape SectorFillWorker / EarningsFillWorker
        # use — so we keep that to avoid touching the wider fill API
        # signature. The auto-pause coordination needs cross-thread
        # signaling that's wake-able from a wait, which only Event
        # provides. The two pieces of state are synchronised inside
        # `request_stop` / `cancel_after_block`. A future cleanup pass
        # could unify by adding an Event-aware adapter to the fill API.
        self._stop = [False]
        self._block_resume_event = threading.Event()
        self._block_decision: str = "continue"
        # Captured during _on_imperva_block from the worker thread; used
        # by main thread's resume_after_block to refresh cookies on the
        # live session jar.
        #
        # Audit M6 — cross-thread contract: refresh_cookies is only
        # safe to call while the worker is blocked inside
        # `_block_resume_event.wait()`. That's the only state where
        # the worker thread is guaranteed not to be inside a
        # `session.get()` call. resume_after_block / cancel_after_block
        # honor this implicitly: they fire ONLY in response to an
        # imperva_block_detected signal, which fires ONLY from inside
        # `_on_imperva_block`, which calls `_block_resume_event.wait()`
        # immediately after emitting. Don't call resume_after_block /
        # refresh_cookies from any other context.
        self._active_session = None
        # Audit L9: track partial progress so an inner-fill exception
        # can still report what was attempted before the crash.
        self._last_candidates: list[str] = []
        # Per-ticker failure breakdown — populated via failed_cb during
        # the fill, emitted via failure_breakdown signal at end. Keys
        # are FAIL_* sentinels; values are ticker lists.
        self._failures_by_kind: dict[str, list[str]] = {}

    def _on_ticker_failed(self, symbol: str, kind: str):
        """Worker-thread callback invoked once per failed ticker by
        `_fill_via_zacks` via the `failed_cb` parameter. Stashes the
        symbol into `_failures_by_kind[kind]` so we can surface a
        breakdown to the GUI at end of run.

        Wrapped in try/except as belt-and-suspenders: the fill loop
        already catches exceptions from this callback (per the
        `try: failed_cb(...) except Exception: pass` at
        earnings_history.py:411-414), but if that wrapper is ever
        removed or refactored we want this method to be self-defensive
        — failure-breakdown bookkeeping must never be the thing that
        crashes a 6-hour fill."""
        try:
            bucket = self._failures_by_kind.setdefault(kind, [])
            bucket.append(symbol)
        except Exception as exc:
            log.warning("_on_ticker_failed(%s, %s) raised: %s",
                        symbol, kind, exc)

    def request_stop(self):
        self._stop[0] = True
        # Unblock any waiting auto-pause callback so the worker can exit
        # promptly even if a block dialog was up.
        self._block_decision = "stop"
        self._block_resume_event.set()

    def resume_after_block(self):
        """Main thread signals the worker to retry after the user pasted
        fresh cookies. Triggers a live cookie reload on the active
        ZacksSession before the loop retries the failed ticker."""
        if self._active_session is not None:
            try:
                n = self._active_session.refresh_cookies()
                self.log_msg.emit(f"Zacks session refreshed with {n} cookies")
            except Exception as exc:
                log.warning("refresh_cookies failed: %s", exc)
        self._block_decision = "continue"
        self._block_resume_event.set()

    def cancel_after_block(self):
        """Main thread signals the worker to stop after the user
        dismissed the cookie-refresh dialog without entering new
        cookies."""
        self._stop[0] = True
        self._block_decision = "stop"
        self._block_resume_event.set()

    def _on_imperva_block(self, count: int, session) -> str:
        """Worker-thread callback invoked by `_fill_via_zacks` once
        `consec_error_limit` consecutive failures hit. Blocks until the
        main thread responds via `resume_after_block` or
        `cancel_after_block`.

        Surfaces the event to the log panel synchronously (via log_msg)
        so the user sees it immediately even if the queued
        imperva_block_detected signal is delayed by main-thread
        contention. Without this, a stop click can race ahead of the
        signal and the dialog appears AFTER the worker has exited
        with no visible "block detected" line in the log.

        Wrapped in try/except so a Qt signal-emit failure (e.g., GUI
        already shutting down, slot connection broken) doesn't kill
        the fill mid-run — we fall back to "stop" cleanly so the
        worker exits on its next iteration check.
        """
        try:
            self.log_msg.emit(
                f"Zacks: {count} consecutive Imperva blocks detected — "
                "auto-pause requested. Cookie-refresh dialog should appear "
                "shortly."
            )
            self._active_session = session
            self._block_resume_event.clear()
            self._block_decision = "continue"
            self.imperva_block_detected.emit(count)
            # Block this worker thread. The main GUI thread is *not*
            # blocked — Qt signals deliver across threads via the event
            # loop, so the dialog runs concurrently.
            self._block_resume_event.wait()
            return self._block_decision
        except Exception as exc:
            log.error("_on_imperva_block crashed: %s", exc, exc_info=True)
            return "stop"

    def run(self):
        from ..earnings_history import (
            bulk_fill_zacks, targeted_fill_zacks,
            find_smart_refresh_candidates,
        )
        from ..earnings_reconcile import reconcile_earnings_dates
        from ..zacks_scraper import has_zacks_cookies

        try:
            if not has_zacks_cookies():
                msg = (
                    "Zacks cookies not configured — skipping Zacks fill. "
                    "Use Data → Set Zacks Cookies to enable."
                )
                self.log_msg.emit(msg)
                self.skipped.emit("no-cookies")
                self.finished.emit(0, 0, [])
                return

            if self.mode == "bulk":
                candidates = [s for s in self.symbols if s not in self.blacklist]
                self._last_candidates = candidates
                self.log_msg.emit(
                    f"Zacks bulk fill: {len(candidates)} tickers (this can run for hours)"
                )
                filled, errors = bulk_fill_zacks(
                    self.symbols, self.blacklist,
                    progress_cb=lambda d, t: self.progress.emit(d, t),
                    stop_flag=self._stop,
                    delay_sec=self.delay_sec,
                    flush_every=self.flush_every,
                    years=self.years,
                    consec_error_limit=self.consec_error_limit,
                    on_block_callback=self._on_imperva_block,
                    failed_cb=self._on_ticker_failed,
                )
            elif self.mode == "smart":
                candidates = find_smart_refresh_candidates(
                    self.symbols, self.blacklist,
                )
                self._last_candidates = candidates
                if not candidates:
                    self.log_msg.emit(
                        "Zacks smart refresh: no candidates due "
                        "(history is current)."
                    )
                    self.finished.emit(0, 0, [])
                    return
                self.log_msg.emit(
                    f"Zacks smart refresh: {len(candidates)} candidate ticker(s)"
                )
                filled, errors = targeted_fill_zacks(
                    candidates, self.blacklist,
                    progress_cb=lambda d, t: self.progress.emit(d, t),
                    stop_flag=self._stop,
                    delay_sec=self.delay_sec,
                    flush_every=self.flush_every,
                    years=self.years,
                    consec_error_limit=self.consec_error_limit,
                    on_block_callback=self._on_imperva_block,
                    failed_cb=self._on_ticker_failed,
                )
            else:  # "targeted"
                candidates = [s for s in self.symbols if s not in self.blacklist]
                self._last_candidates = candidates
                self.log_msg.emit(
                    f"Zacks targeted fill: {len(candidates)} ticker(s)"
                )
                filled, errors = targeted_fill_zacks(
                    self.symbols, self.blacklist,
                    progress_cb=lambda d, t: self.progress.emit(d, t),
                    stop_flag=self._stop,
                    delay_sec=self.delay_sec,
                    flush_every=self.flush_every,
                    years=self.years,
                    consec_error_limit=self.consec_error_limit,
                    on_block_callback=self._on_imperva_block,
                    failed_cb=self._on_ticker_failed,
                )

            # Per spec §5.2 step 4 / §4.2: belt-and-suspenders reconcile
            # of every candidate touched in this run, even ones that
            # errored — keeps earnings_dates.parquet aligned.
            if candidates:
                reconcile_earnings_dates(affected_tickers=candidates)

            # Surface the per-kind breakdown before finished — the
            # GUI's "Show Last Zacks Failures" menu reads it.
            if self._failures_by_kind:
                breakdown_summary = ", ".join(
                    f"{k}={len(v)}"
                    for k, v in sorted(self._failures_by_kind.items())
                )
                self.log_msg.emit(
                    f"Zacks fill failures by kind: {breakdown_summary}"
                )
            self.failure_breakdown.emit(dict(self._failures_by_kind))

            self.log_msg.emit(
                f"Zacks fill done: {filled} filled, {errors} errors "
                f"(of {len(candidates)} ticker(s))"
            )
            self.finished.emit(filled, errors, candidates)
        except Exception as exc:
            log.error("ZacksFillWorker crashed: %s", exc, exc_info=True)
            self.log_msg.emit(f"Zacks fill error: {exc}")
            # Surface partial breakdown captured before the crash so
            # the user can still see what was hit.
            self.failure_breakdown.emit(dict(self._failures_by_kind))
            # Audit L9: surface the candidate list that was in flight
            # when the crash happened so the slot can still report which
            # tickers were attempted (and the GUI can offer a retry).
            self.finished.emit(0, 0, list(self._last_candidates))


# ============================================================================
# Finnhub deep-history fill worker (Phase 2)
# ============================================================================

class FinnhubFillWorker(QThread):
    """Runs ``finnhub_fill.bulk_fill_finnhub`` / ``gap_fill_finnhub`` on
    a background thread. Mirrors the ZacksFillWorker control surface
    (start/stop/log/progress) and adds an ``etf_identified`` signal for
    live blacklist updates.

    On a Finnhub block trigger (≥ FINNHUB_CONSEC_BLOCK_LIMIT consecutive
    non-empty failures), the worker pauses + verifies the API key + rewinds
    to the first ticker in the failure window — same step-back pattern as
    the Zacks Imperva auto-pause. After FINNHUB_MAX_BLOCKS_PER_RUN total
    blocks, the worker halts. There's no GUI interaction during a block;
    the user just gets log output and can hit "Stop Finnhub Fill" if they
    want to abort.
    """

    log_msg = pyqtSignal(str)
    progress = pyqtSignal(int, int)              # done, total
    etf_identified = pyqtSignal(str)             # ticker — emitted live so the
                                                 # GUI can grow finnhub_blacklist
    finished = pyqtSignal(int, int)              # filled, errors

    def __init__(self, symbols: list[str], blacklist: set[str], *,
                 mode: str = "bulk", flush_every: int = 25):
        """``mode`` ∈ {"bulk", "gap"}.  Spot fills run inline in the main
        thread — no worker needed for one ticker."""
        super().__init__()
        self.symbols = list(symbols)
        self.blacklist = set(blacklist)
        self.mode = mode
        self.flush_every = flush_every
        self._stop = [False]

    def request_stop(self):
        self._stop[0] = True

    def run(self):
        from .. import finnhub_fill

        try:
            def progress_cb(d, t):
                self.progress.emit(d, t)

            def etf_cb(sym):
                # Forward to the main thread via signal so the GUI's
                # blacklist set update happens on the UI thread.
                try:
                    self.etf_identified.emit(sym)
                except Exception:
                    pass

            def block_cb(consec, blocks_so_far):
                self.log_msg.emit(
                    f"Finnhub: hit {blocks_so_far} block-pause(s) — halting."
                )
                return "stop"

            if self.mode == "bulk":
                self.log_msg.emit(
                    f"Finnhub bulk fill: {len(self.symbols)} ticker(s) "
                    f"(may run several hours)"
                )
                filled, errors = finnhub_fill.bulk_fill_finnhub(
                    self.symbols, self.blacklist,
                    progress_cb=progress_cb,
                    stop_flag=self._stop,
                    flush_every=self.flush_every,
                    on_block_callback=block_cb,
                    on_etf_identified=etf_cb,
                )
            elif self.mode == "gap":
                self.log_msg.emit(
                    f"Finnhub gap fill: {len(self.symbols)} ticker(s)"
                )
                filled, errors = finnhub_fill.gap_fill_finnhub(
                    self.symbols, self.blacklist,
                    progress_cb=progress_cb,
                    stop_flag=self._stop,
                    flush_every=self.flush_every,
                    on_block_callback=block_cb,
                    on_etf_identified=etf_cb,
                )
            else:
                raise ValueError(f"unknown FinnhubFillWorker mode: {self.mode!r}")

            self.finished.emit(filled, errors)
        except Exception as exc:
            log.error("FinnhubFillWorker crashed: %s", exc, exc_info=True)
            self.log_msg.emit(f"Finnhub fill error: {exc}")
            self.finished.emit(0, 0)


# ============================================================================
# TradeStation bridge worker
# ============================================================================

class BridgeWorker(QThread):
    """Runs TradeStationBridge.start() on a background thread."""

    countdown_tick = pyqtSignal(int)       # remaining seconds
    ticker_sent = pyqtSignal(int, int, str, bool)  # idx, total, sym, dry_run
    done = pyqtSignal(int, int)            # sent, skipped
    log_msg = pyqtSignal(str)
    batch_pause = pyqtSignal(int, int, int)  # batch_num, sent_so_far, total

    def __init__(self, symbols: list[str], cfg: BridgeConfig):
        super().__init__()
        self.bridge = TradeStationBridge(symbols, cfg)
        self.bridge.on_countdown = lambda r: self.countdown_tick.emit(r)
        self.bridge.on_ticker_sent = lambda i, t, s, d: self.ticker_sent.emit(i, t, s, d)
        self.bridge.on_done = lambda s, k: self.done.emit(s, k)
        self.bridge.on_log = lambda m: self.log_msg.emit(m)
        self.bridge.on_batch_pause = lambda bn, s, t: self.batch_pause.emit(bn, s, t)

    def run(self):
        self.bridge.start()

    def request_stop(self):
        self.bridge.request_stop()

    def resume_batch(self):
        self.bridge.resume_batch()


# ============================================================================
# Firefox cookie-capture worker (May 2026 rewrite)
# ============================================================================

class FirefoxCookieWaitWorker(QThread):
    """Captures Zacks cookies from the persistent Firefox profile.

    Two parallel completion paths run inside one polling loop:

      1. **Mid-flight capture (default).** Each tick, we read
         cookies.sqlite via stdlib sqlite3 (`mode=ro&immutable=1`,
         which works while Firefox still holds the file). If the DB
         contains BOTH `reese84` and a `visid_incap_*` cookie AND
         their signature differs from the pre-launch signature, the
         JS challenge has just completed — capture, persist, and
         finish without waiting for the user to close Firefox. The
         pre-signature check prevents insta-capture of stale cookies
         the profile carried over from a previous run.

      2. **On-close fallback.** If the user closes Firefox before
         tokens land (or no challenge fires because cookies were
         already valid), the legacy path applies: detect close via
         `is_firefox_holding_profile()` (psutil cmdline match), do a
         final read with a 0.5s grace period, then finalize.

    Both paths converge in `_finalize()`, which validates the
    Imperva-signature, persists, and emits the verdict. Pass
    `poll_for_tokens=False` to disable the mid-flight path (used by
    legacy tests / debugging). `request_stop()` wakes the loop early.
    """

    log_msg = pyqtSignal(str)
    # finished(success: bool, n_cookies: int, is_new_session: bool, signature_pre: str)
    finished = pyqtSignal(bool, int, bool, str)

    def __init__(
        self,
        profile_dir,
        *,
        pre_signature: str = "",
        poll_interval_sec: float = 1.5,
        # Defensive ceiling so a runaway worker can't hold the GUI
        # forever. 6 hours covers any realistic CAPTCHA-and-walk-away
        # scenario without becoming a foot-gun.
        max_wait_sec: float = 6 * 3600.0,
        poll_for_tokens: bool = True,
    ):
        super().__init__()
        self._profile_dir = profile_dir
        self._pre_sig = pre_signature
        self._poll = float(poll_interval_sec)
        self._max_wait = float(max_wait_sec)
        self._poll_for_tokens = bool(poll_for_tokens)
        self._stop = False

    def request_stop(self):
        """Wake the wait loop early. The worker still finalizes via
        `finished`, but with a "stopped" verdict (no cookies captured)."""
        self._stop = True

    def run(self):
        from ..zacks_scraper import (
            is_firefox_holding_profile,
            read_cookies_from_firefox_profile,
            _has_complete_imperva_tokens,
            _cookie_signature,
        )

        if self._poll_for_tokens:
            self.log_msg.emit(
                "Cookie capture: watching for Imperva tokens (reese84 + "
                "visid_incap). Will auto-capture once the JS challenge "
                "completes — or close Firefox manually to finalize early."
            )
        else:
            self.log_msg.emit(
                "Cookie capture: waiting for you to close Firefox..."
            )
        deadline = time.monotonic() + self._max_wait
        last_progress = time.monotonic()

        while not self._stop and time.monotonic() < deadline:
            # Mid-flight capture: cookies.sqlite is readable via
            # mode=ro&immutable=1 even while Firefox still holds the
            # file. Once both tokens are present AND the signature
            # differs from the pre-launch signature, the JS challenge
            # just completed — finalize immediately. The signature
            # check is what prevents capturing stale cookies the
            # profile already carried (e.g. from a prior session).
            if self._poll_for_tokens:
                try:
                    mid = read_cookies_from_firefox_profile(self._profile_dir)
                except Exception as exc:
                    log.debug("mid-flight cookie read failed: %s", exc)
                    mid = ""
                if mid and _has_complete_imperva_tokens(mid):
                    new_sig = _cookie_signature(mid)
                    if new_sig and new_sig != self._pre_sig:
                        self.log_msg.emit(
                            "Cookie capture: Imperva tokens detected — "
                            "capturing without waiting for Firefox close."
                        )
                        self._finalize(mid, captured_mid_flight=True)
                        return

            try:
                still_open = is_firefox_holding_profile(self._profile_dir)
            except Exception as exc:
                # psutil hiccup — assume open and keep polling.
                log.debug("is_firefox_holding_profile raised: %s", exc)
                still_open = True

            if not still_open:
                break

            now = time.monotonic()
            if now - last_progress > 30.0:
                last_progress = now
                self.log_msg.emit(
                    "  ...still watching. Close Firefox manually if "
                    "the page is stuck or you want to abort."
                )
            time.sleep(self._poll)

        if self._stop:
            self.log_msg.emit("Cookie capture cancelled.")
            self.finished.emit(False, 0, False, self._pre_sig)
            return

        if time.monotonic() >= deadline:
            self.log_msg.emit(
                f"Cookie capture: timed out after "
                f"{self._max_wait/3600:.1f}h waiting for Firefox to "
                "close. Close it manually and try again."
            )
            self.finished.emit(False, 0, False, self._pre_sig)
            return

        # Firefox closed. Give the OS a moment to release sqlite
        # handles, then read.
        time.sleep(0.5)
        try:
            cookie_str = read_cookies_from_firefox_profile(self._profile_dir)
        except Exception as exc:
            log.warning("read_cookies_from_firefox_profile failed: %s", exc)
            cookie_str = ""

        self._finalize(cookie_str, captured_mid_flight=False)

    def _finalize(self, cookie_str: str, *, captured_mid_flight: bool):
        """Validate, persist, and emit the verdict. Shared by both the
        mid-flight capture path and the on-close path. `cookie_str` is
        the raw cookie blob already filtered to zacks.com entries by
        `read_cookies_from_firefox_profile`."""
        from ..zacks_scraper import (
            _has_imperva_signature, _cookie_signature, set_zacks_cookies,
        )

        if not cookie_str:
            self.log_msg.emit(
                "Cookie capture: cookies.sqlite had no zacks.com "
                "entries. Did you visit the page?"
            )
            self.finished.emit(False, 0, False, self._pre_sig)
            return

        if not _has_imperva_signature(cookie_str):
            self.log_msg.emit(
                "Cookie capture: read cookies but Imperva tokens "
                "(reese84 / visid_incap) are missing. The JS challenge "
                "may not have completed — try reloading the page in "
                "Firefox before closing."
            )
            self.finished.emit(False, 0, False, self._pre_sig)
            return

        if not set_zacks_cookies(cookie_str):
            self.log_msg.emit(
                "Cookie capture: read OK but failed to write the "
                "cookie file under scanner_data/."
            )
            self.finished.emit(False, 0, False, self._pre_sig)
            return

        n = cookie_str.count(";") + 1 if cookie_str else 0
        post_sig = _cookie_signature(cookie_str)
        is_new = bool(post_sig) and post_sig != self._pre_sig
        when = (
            "while Firefox is still open"
            if captured_mid_flight else "after Firefox closed"
        )
        self.log_msg.emit(
            f"Cookie capture: saved {n} cookies "
            f"({'NEW session' if is_new else 'same session as before'}) "
            f"{when}."
        )
        self.finished.emit(True, n, is_new, self._pre_sig)
