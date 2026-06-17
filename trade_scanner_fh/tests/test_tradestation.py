"""Tests for tradestation.py — Phase 4 R5 + R16."""
import threading

import pyautogui

from trade_scanner_fh import tradestation
from trade_scanner_fh.tradestation import BridgeConfig, TradeStationBridge


# ----------------------------------------------------------------------
# R16 — stop signaling is backed by threading.Event
# ----------------------------------------------------------------------

def test_bridge_stop_flag_is_threading_event():
    bridge = TradeStationBridge(["AAPL"], BridgeConfig(dry_run=True))
    assert isinstance(bridge._stop, threading.Event)
    assert isinstance(bridge._batch_continue, threading.Event)


def test_request_stop_sets_the_event():
    bridge = TradeStationBridge(["AAPL"], BridgeConfig(dry_run=True))
    assert not bridge._stop.is_set()
    bridge.request_stop()
    assert bridge._stop.is_set()
    # request_stop also unblocks any batch wait
    assert bridge._batch_continue.is_set()


def test_resume_batch_sets_the_event():
    bridge = TradeStationBridge(["AAPL"], BridgeConfig(dry_run=True))
    bridge._batch_continue.clear()
    bridge.resume_batch()
    assert bridge._batch_continue.is_set()


# ----------------------------------------------------------------------
# R5 — pyautogui globals are no longer set at module import time
# ----------------------------------------------------------------------

def test_pyautogui_globals_not_set_at_import():
    """Simply re-importing the module must NOT force pyautogui.FAILSAFE /
    PAUSE to specific values. They are set (and restored) inside
    TradeStationBridge.start() only."""
    # Save, clobber, re-import, verify the module doesn't reset them.
    saved_failsafe = pyautogui.FAILSAFE
    saved_pause = pyautogui.PAUSE
    try:
        pyautogui.FAILSAFE = False
        pyautogui.PAUSE = 0.77

        import importlib
        importlib.reload(tradestation)

        assert pyautogui.FAILSAFE is False
        assert pyautogui.PAUSE == 0.77
    finally:
        pyautogui.FAILSAFE = saved_failsafe
        pyautogui.PAUSE = saved_pause


# ----------------------------------------------------------------------
# Lowercase typing — keeps Shift off the wire so capital letters can't
# trigger platform Shift+letter order hotkeys (TradeStation Trade Bar).
# ----------------------------------------------------------------------

def test_bridge_types_symbols_lowercase(monkeypatch):
    """LIVE mode must type symbols LOWERCASE. pyautogui capitalizes by
    holding Shift, and Shift+letter lands on order-entry hotkeys; lowercase
    avoids that. Symbol entry is case-insensitive so the load still works."""
    typed: list[str] = []
    monkeypatch.setattr(tradestation.pyautogui, "typewrite",
                        lambda text, interval=0: typed.append(text))
    monkeypatch.setattr(tradestation.pyautogui, "press", lambda *a, **k: None)
    monkeypatch.setattr(tradestation.pyautogui, "click", lambda *a, **k: None)
    monkeypatch.setattr(tradestation.time, "sleep", lambda *a, **k: None)

    cfg = BridgeConfig(dry_run=False, countdown_seconds=0,
                       delay_between_tickers=0)
    TradeStationBridge(["AAPL", "SDOT"], cfg).start()

    assert typed == ["aapl", "sdot"]
    assert not any(ch.isupper() for s in typed for ch in s)
