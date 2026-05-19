"""Per-row hotkey ticker sender.

Independent of the bulk Send-to-Watchlist bridge in `tradestation.py`.
Used by the GUI's HOTKEY mode: when the user clicks a configured cue
(right-click, shift+left-click, etc.) on a results-table row, the
scanner clicks at a saved screen position, waits a configured delay,
types the row's ticker, and presses an optional end-sequence key.

Public API
    HotkeyConfig         — persisted dataclass (position + delay + cue + end seq)
    CUE_OPTIONS          — list of (id, label) pairs for the cue dropdown
    END_SEQUENCE_OPTIONS — list of (id, label) pairs for the end-key dropdown
    send_ticker(ticker, cfg)   — fires the click → wait → type → end-key sequence

Qt is intentionally NOT imported here; the cue→Qt-event mapping lives
in the GUI layer so this module stays unit-testable in headless envs.
pyautogui is imported lazily inside send_ticker for the same reason.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional

log = logging.getLogger("scanner.hotkey")


# ── Cue identifiers ───────────────────────────────────────────────────
CUE_RIGHT_CLICK = "right_click"
CUE_SHIFT_LEFT  = "shift_left_click"
CUE_CTRL_LEFT   = "ctrl_left_click"
CUE_MIDDLE      = "middle_click"
# Keyboard cue: Enter on the currently selected row. Pairs with arrow-
# key navigation through the results table — no mouse needed.
CUE_ENTER_KEY   = "enter_key"

CUE_OPTIONS: list[tuple[str, str]] = [
    (CUE_RIGHT_CLICK, "Right-click"),
    (CUE_SHIFT_LEFT,  "Shift + Left-click"),
    (CUE_CTRL_LEFT,   "Ctrl + Left-click"),
    (CUE_MIDDLE,      "Middle-click"),
    (CUE_ENTER_KEY,   "Enter key (selected row)"),
]
_CUE_IDS = {cid for cid, _ in CUE_OPTIONS}


def is_keyboard_cue(cue_id: str) -> bool:
    """True if the cue is triggered by a key press rather than a mouse
    event. Used by the GUI to decide whether to consult the mouse-event
    filter or the key-event filter."""
    return cue_id == CUE_ENTER_KEY


# ── End-sequence keystrokes ───────────────────────────────────────────
END_NONE        = "none"
END_ENTER       = "enter"
END_TAB         = "tab"
END_CTRL_ENTER  = "ctrl_enter"
END_SHIFT_ENTER = "shift_enter"
END_ALT_ENTER   = "alt_enter"

END_SEQUENCE_OPTIONS: list[tuple[str, str]] = [
    (END_NONE,        "None"),
    (END_ENTER,       "Enter"),
    (END_TAB,         "Tab"),
    (END_CTRL_ENTER,  "Ctrl + Enter"),
    (END_SHIFT_ENTER, "Shift + Enter"),
    (END_ALT_ENTER,   "Alt + Enter"),
]
_END_IDS = {eid for eid, _ in END_SEQUENCE_OPTIONS}


@dataclass
class HotkeyConfig:
    """Per-row ticker hotkey settings.

    Persisted across sessions via QSettings (see `gui/main_window.py`).
    The `enabled` toggle is a runtime concern owned by the GUI and is
    intentionally NOT stored here — the GUI defaults it to off on each
    launch so a stale screen position can't fire surprise input.

    Optional return click: if `return_click_{x,y}` are set, an extra
    click fires AFTER the end-sequence keystroke. Use case — bring
    keyboard focus back to the scanner window so arrow-key navigation
    through the results table works seamlessly between sends.
    """
    click_x: Optional[int] = None
    click_y: Optional[int] = None
    delay_ms: int = 200
    cue: str = CUE_RIGHT_CLICK
    end_sequence: str = END_ENTER
    return_click_x: Optional[int] = None
    return_click_y: Optional[int] = None

    @property
    def has_position(self) -> bool:
        return self.click_x is not None and self.click_y is not None

    @property
    def has_return_position(self) -> bool:
        return (self.return_click_x is not None
                and self.return_click_y is not None)

    def normalized(self) -> "HotkeyConfig":
        """Return a copy with out-of-range values clamped and unknown
        cue / end_sequence ids reset to the defaults. Used after loading
        from QSettings so corrupt prefs can't crash the sender."""
        cue = self.cue if self.cue in _CUE_IDS else CUE_RIGHT_CLICK
        end = self.end_sequence if self.end_sequence in _END_IDS else END_ENTER
        delay = max(0, min(int(self.delay_ms or 0), 5000))
        return HotkeyConfig(
            click_x=self.click_x,
            click_y=self.click_y,
            delay_ms=delay,
            cue=cue,
            end_sequence=end,
            return_click_x=self.return_click_x,
            return_click_y=self.return_click_y,
        )


def cue_label(cue_id: str) -> str:
    for cid, label in CUE_OPTIONS:
        if cid == cue_id:
            return label
    return cue_id


def end_sequence_label(end_id: str) -> str:
    for eid, label in END_SEQUENCE_OPTIONS:
        if eid == end_id:
            return label
    return end_id


def send_ticker(
    ticker: str,
    cfg: HotkeyConfig,
    *,
    on_log: Optional[Callable[[str], None]] = None,
    pyautogui_module=None,
) -> bool:
    """Click at `cfg`'s saved position → sleep `delay_ms` → type ticker →
    press end-sequence key. Returns True on success, False if no position
    is configured or input is empty.

    `pyautogui_module` is an injection seam for tests. Production callers
    leave it None and the real pyautogui is imported lazily.
    """
    msg: Callable[[str], None] = on_log if on_log is not None else (lambda s: None)

    if not ticker:
        msg("Hotkey: no ticker selected.")
        return False
    if not cfg.has_position:
        msg("Hotkey: no click position configured — open Settings → "
            "Hotkey Settings to set one.")
        return False

    if pyautogui_module is None:
        import pyautogui as pyautogui_module  # local import; see module docstring

    saved_failsafe = pyautogui_module.FAILSAFE
    saved_pause = pyautogui_module.PAUSE
    pyautogui_module.FAILSAFE = True
    pyautogui_module.PAUSE = 0.05
    try:
        pyautogui_module.click(int(cfg.click_x), int(cfg.click_y))
        if cfg.delay_ms > 0:
            time.sleep(cfg.delay_ms / 1000.0)
        pyautogui_module.typewrite(str(ticker), interval=0.03)
        _press_end_sequence(pyautogui_module, cfg.end_sequence)
        # Optional return click — bring keyboard focus back to the
        # scanner (or wherever the user wants) so arrow-key navigation
        # in the results table keeps working between sends. Reuses the
        # same delay knob to give the target app time to process the
        # end-sequence keystroke before we steal focus away.
        if cfg.has_return_position:
            if cfg.delay_ms > 0:
                time.sleep(cfg.delay_ms / 1000.0)
            pyautogui_module.click(
                int(cfg.return_click_x), int(cfg.return_click_y),
            )
    finally:
        pyautogui_module.FAILSAFE = saved_failsafe
        pyautogui_module.PAUSE = saved_pause

    return_suffix = (
        f" → return click ({cfg.return_click_x},{cfg.return_click_y})"
        if cfg.has_return_position else ""
    )
    log.info("Hotkey sent %s to (%d,%d) end=%s%s",
             ticker, cfg.click_x, cfg.click_y, cfg.end_sequence,
             return_suffix)
    msg(f"Hotkey: sent '{ticker}' to ({cfg.click_x},{cfg.click_y}) "
        f"[{end_sequence_label(cfg.end_sequence)}]{return_suffix}.")
    return True


def _press_end_sequence(pyautogui_module, end_seq: str) -> None:
    """Translate the end-sequence id into a pyautogui keystroke."""
    if end_seq == END_NONE:
        return
    if end_seq == END_ENTER:
        pyautogui_module.press("enter")
    elif end_seq == END_TAB:
        pyautogui_module.press("tab")
    elif end_seq == END_CTRL_ENTER:
        pyautogui_module.hotkey("ctrl", "enter")
    elif end_seq == END_SHIFT_ENTER:
        pyautogui_module.hotkey("shift", "enter")
    elif end_seq == END_ALT_ENTER:
        pyautogui_module.hotkey("alt", "enter")
