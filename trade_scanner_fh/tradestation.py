"""
TradeStation Bridge — pyautogui Keyboard Automation (Phase 4)
==============================================================
Types ticker symbols into TradeStation's watchlist search bar
using simulated keyboard input.

Public API
    TradeStationBridge  — controller class used by the GUI
"""

import logging
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional

import pyautogui

log = logging.getLogger("scanner.tradestation")

# Keys the watchlist dialog offers for the per-ticker confirm keystroke. A
# corrupt/persisted config outside this set is coerced to "enter" before it
# reaches pyautogui.press, so an unexpected value can never drive a stray
# keystroke (audit: confirm_key was passed to press() unvalidated).
_ALLOWED_CONFIRM_KEYS = {"enter", "tab", "space"}


def _safe_confirm_key(key: str) -> str:
    k = (key or "").strip().lower()
    if k in _ALLOWED_CONFIRM_KEYS:
        return k
    log.warning("Bridge: invalid confirm_key %r — defaulting to 'enter'", key)
    return "enter"

# Phase 4 R5: pyautogui FAILSAFE / PAUSE globals are now scoped inside
# TradeStationBridge.start() so the scanner doesn't clobber settings for
# any other pyautogui consumer in the process.


@dataclass
class BridgeConfig:
    """User-tunable configuration for the watchlist auto-typer."""
    delay_between_tickers: float = 0.8   # seconds between each ticker entry
    countdown_seconds: int = 5           # countdown before typing starts
    dry_run: bool = False                # if True, log but don't type
    confirm_key: str = "enter"           # key pressed after each ticker
    start_index: int = 0                 # 0-based index to start from
    batch_size: int = 99                 # tickers per batch (0 = no batching)


class TradeStationBridge:
    """
    Types a list of ticker symbols via pyautogui.

    Usage from the GUI:
        bridge = TradeStationBridge(symbols, config)
        bridge.on_countdown = lambda remaining: ...
        bridge.on_ticker_sent = lambda idx, total, symbol: ...
        bridge.on_done = lambda sent, skipped: ...
        bridge.start()       # call from a background thread
        bridge.request_stop()  # from main thread to halt mid-sequence
    """

    def __init__(self, symbols: list[str], cfg: BridgeConfig):
        self.symbols = symbols
        self.cfg = cfg
        # Phase 4 R16: Event instead of plain bool for thread-safety docs
        self._stop = threading.Event()
        self._batch_continue = threading.Event()

        # Callbacks (set by caller before start)
        self.on_countdown: Optional[Callable[[int], None]] = None
        self.on_ticker_sent: Optional[Callable[[int, int, str, bool], None]] = None
        self.on_done: Optional[Callable[[int, int], None]] = None
        self.on_log: Optional[Callable[[str], None]] = None
        self.on_batch_pause: Optional[Callable[[int, int, int], None]] = None  # batch_num, sent_so_far, total

    def _log(self, msg: str):
        log.info(msg)
        if self.on_log:
            self.on_log(msg)

    def request_stop(self):
        """Request the typing loop to stop after the current ticker."""
        self._stop.set()
        self._batch_continue.set()  # unblock any batch wait
        self._log("Stop requested — will halt after current ticker.")

    def resume_batch(self):
        """Signal the bridge to continue after a batch pause."""
        self._batch_continue.set()

    def start(self):
        """
        Run the full sequence: countdown -> type each ticker -> done.
        Call this from a background thread.

        Phase 4 R5: pyautogui FAILSAFE/PAUSE are set on entry and restored
        on exit so the scanner doesn't leak global state to other consumers.
        """
        self._stop.clear()
        self._batch_continue.clear()

        # Scope pyautogui globals for the duration of this run
        _saved_failsafe = pyautogui.FAILSAFE
        _saved_pause = pyautogui.PAUSE
        pyautogui.FAILSAFE = True
        pyautogui.PAUSE = 0.05
        try:
            self._run_sequence()
        finally:
            pyautogui.FAILSAFE = _saved_failsafe
            pyautogui.PAUSE = _saved_pause

    def _run_sequence(self):
        """The actual typing sequence — wrapped by start() so pyautogui
        globals are restored on any exit path."""
        # Apply start_index — skip tickers before it
        work_list = self.symbols[self.cfg.start_index:]
        total = len(work_list)
        mode = "DRY RUN" if self.cfg.dry_run else "LIVE"

        start_label = (f" (starting at #{self.cfg.start_index + 1})"
                       if self.cfg.start_index > 0 else "")
        self._log(f"Watchlist Bridge [{mode}]: {total} tickers{start_label}, "
                  f"delay={self.cfg.delay_between_tickers}s")

        # ── Countdown ──
        self._log(f"Starting in {self.cfg.countdown_seconds}s — "
                  f"click into the target input field NOW")
        for remaining in range(self.cfg.countdown_seconds, 0, -1):
            if self._stop.is_set():
                self._log("Aborted during countdown.")
                if self.on_done:
                    self.on_done(0, total)
                return
            if self.on_countdown:
                self.on_countdown(remaining)
            self._log(f"  {remaining}...")
            time.sleep(1)

        # ── Type tickers ──
        sent = 0
        batch_num = 1
        batch_count = 0

        for i, sym in enumerate(work_list, 1):
            if self._stop.is_set():
                self._log(f"Stopped at ticker {i}/{total}.")
                break

            if self.cfg.dry_run:
                self._log(f"  [{i}/{total}] Would type: {sym}")
            else:
                self._log(f"  [{i}/{total}] Typing: {sym}")
                pyautogui.typewrite(sym, interval=0.03)
                pyautogui.press(_safe_confirm_key(self.cfg.confirm_key))

            sent += 1
            batch_count += 1

            if self.on_ticker_sent:
                self.on_ticker_sent(i, total, sym, self.cfg.dry_run)

            # ── Batch pause check ──
            if (self.cfg.batch_size > 0
                    and batch_count >= self.cfg.batch_size
                    and i < total
                    and not self._stop.is_set()):
                self._log(f"Batch {batch_num} complete ({batch_count} tickers). "
                          f"Waiting for Continue...")
                batch_num += 1
                batch_count = 0
                self._batch_continue.clear()

                if self.on_batch_pause:
                    self.on_batch_pause(batch_num - 1, sent, total)

                # Block until GUI signals continue or stop. Event.wait lets
                # us sleep efficiently instead of polling with time.sleep.
                while not self._batch_continue.is_set() and not self._stop.is_set():
                    self._batch_continue.wait(timeout=0.2)

                if self._stop.is_set():
                    self._log("Stopped during batch pause.")
                    break

                # Countdown before resuming (same as initial countdown)
                self._log(f"Resuming in {self.cfg.countdown_seconds}s — "
                          f"click into the target input field NOW")
                for remaining in range(self.cfg.countdown_seconds, 0, -1):
                    if self._stop.is_set():
                        break
                    if self.on_countdown:
                        self.on_countdown(remaining)
                    self._log(f"  {remaining}...")
                    time.sleep(1)

                if self._stop.is_set():
                    self._log("Stopped during batch countdown.")
                    break

                self._log("Resuming...")

            # Pause between tickers (except after the last one)
            elif i < total and not self._stop.is_set():
                time.sleep(self.cfg.delay_between_tickers)

        skipped = total - sent
        self._log(f"Done: {sent} sent, {skipped} skipped.")
        if self.on_done:
            self.on_done(sent, skipped)
