"""
TradeStation Bridge — pyautogui Keyboard Automation (Phase 4)
==============================================================
Types ticker symbols into TradeStation's watchlist search bar
using simulated keyboard input.

Public API
    TradeStationBridge  — controller class used by the GUI
"""

import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional

import pyautogui

log = logging.getLogger("scanner.tradestation")

# Safety: pyautogui failsafe — move mouse to top-left corner to abort
pyautogui.FAILSAFE = True
# Don't add pyautogui's default pause between calls
pyautogui.PAUSE = 0.05


@dataclass
class BridgeConfig:
    """User-tunable configuration for the TradeStation auto-typer."""
    delay_between_tickers: float = 0.8   # seconds between each ticker entry
    countdown_seconds: int = 5           # countdown before typing starts
    dry_run: bool = False                # if True, log but don't type
    confirm_key: str = "enter"           # key pressed after each ticker


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
        self._stop = False

        # Callbacks (set by caller before start)
        self.on_countdown: Optional[Callable[[int], None]] = None
        self.on_ticker_sent: Optional[Callable[[int, int, str, bool], None]] = None
        self.on_done: Optional[Callable[[int, int], None]] = None
        self.on_log: Optional[Callable[[str], None]] = None

    def _log(self, msg: str):
        log.info(msg)
        if self.on_log:
            self.on_log(msg)

    def request_stop(self):
        """Request the typing loop to stop after the current ticker."""
        self._stop = True
        self._log("Stop requested — will halt after current ticker.")

    def start(self):
        """
        Run the full sequence: countdown -> type each ticker -> done.
        Call this from a background thread.
        """
        self._stop = False
        total = len(self.symbols)
        mode = "DRY RUN" if self.cfg.dry_run else "LIVE"

        self._log(f"TradeStation Bridge [{mode}]: {total} tickers, "
                  f"delay={self.cfg.delay_between_tickers}s")

        # ── Countdown ──
        self._log(f"Starting in {self.cfg.countdown_seconds}s — "
                  f"click into TradeStation's watchlist search bar NOW")
        for remaining in range(self.cfg.countdown_seconds, 0, -1):
            if self._stop:
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
        for i, sym in enumerate(self.symbols, 1):
            if self._stop:
                self._log(f"Stopped at ticker {i}/{total}.")
                break

            if self.cfg.dry_run:
                self._log(f"  [{i}/{total}] Would type: {sym}")
            else:
                self._log(f"  [{i}/{total}] Typing: {sym}")
                # Type the ticker symbol
                pyautogui.typewrite(sym, interval=0.03)
                # Press confirm key
                pyautogui.press(self.cfg.confirm_key)

            sent += 1

            if self.on_ticker_sent:
                self.on_ticker_sent(i, total, sym, self.cfg.dry_run)

            # Pause between tickers (except after the last one)
            if i < total and not self._stop:
                time.sleep(self.cfg.delay_between_tickers)

        skipped = total - sent
        self._log(f"Done: {sent} sent, {skipped} skipped.")
        if self.on_done:
            self.on_done(sent, skipped)
