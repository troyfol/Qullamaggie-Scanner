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


def _virtual_screen_bounds() -> "Optional[tuple[int, int, int, int]]":
    """(min_x, min_y, max_x, max_y) of the FULL virtual desktop (all monitors),
    or None if it can't be determined. Multi-monitor aware — uses the virtual-
    screen metrics so a valid coordinate on a secondary monitor (which can have
    a negative origin) is never wrongly rejected. Returns None on any failure so
    callers fail OPEN (no rejection) rather than blocking a legitimate send."""
    try:
        import win32api  # part of pywin32, already a dependency (zacks_scraper)
        import win32con
        x = win32api.GetSystemMetrics(win32con.SM_XVIRTUALSCREEN)
        y = win32api.GetSystemMetrics(win32con.SM_YVIRTUALSCREEN)
        w = win32api.GetSystemMetrics(win32con.SM_CXVIRTUALSCREEN)
        h = win32api.GetSystemMetrics(win32con.SM_CYVIRTUALSCREEN)
        if w > 0 and h > 0:
            return (x, y, x + w, y + h)
    except Exception:
        pass
    return None


def _coord_on_screen(x: int, y: int) -> bool:
    """True if (x, y) is inside the virtual desktop, OR if the bounds can't be
    determined (fail open). Used to refuse firing a hotkey at a STALE saved
    coordinate (monitor unplugged / resolution change / window moved) that
    would otherwise click + type the ticker into whatever happens to be at a
    now-invalid screen location."""
    bounds = _virtual_screen_bounds()
    if bounds is None:
        return True  # can't tell — don't block a legitimate send
    min_x, min_y, max_x, max_y = bounds
    return (min_x <= int(x) < max_x) and (min_y <= int(y) < max_y)


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


# ── Held-input guard ──────────────────────────────────────────────────
# A mouse-button cue fires this send the instant the cue is detected,
# while the triggering button (and any Shift/Ctrl/Alt modifier) is still
# physically held. send_ticker waits for those to be released before it
# moves the cursor and types — see the call site for the full rationale.

# High bit of GetAsyncKeyState's return value => that key is down now.
_KEY_DOWN_BIT = 0x8000

# Ceiling on the release wait. Long enough to cover a normal press-and-
# release, short enough that an unusual sustained hold (or a misbehaving
# key-state API) can never wedge a send for more than this.
_INPUT_RELEASE_TIMEOUT_S = 1.5


def _any_cue_input_held() -> bool:
    """True if any mouse button or Ctrl/Shift/Alt modifier is physically
    down right now. Returns False if the Win32 key-state API is missing
    or errors, so callers FAIL OPEN and never block a legitimate send on
    a platform/environment where the state can't be read."""
    try:
        import win32api  # pywin32 — already a dep (see _virtual_screen_bounds)
        import win32con
    except Exception:
        return False
    vks = (
        win32con.VK_LBUTTON, win32con.VK_RBUTTON, win32con.VK_MBUTTON,
        win32con.VK_CONTROL, win32con.VK_SHIFT, win32con.VK_MENU,  # MENU = Alt
    )
    try:
        return any(win32api.GetAsyncKeyState(vk) & _KEY_DOWN_BIT for vk in vks)
    except Exception:
        return False


def _wait_for_input_release(
    timeout_s: float = _INPUT_RELEASE_TIMEOUT_S,
    poll_s: float = 0.01,
    held_check: Callable[[], bool] = _any_cue_input_held,
) -> None:
    """Block until no cue input (mouse button / modifier key) is
    physically held, or `timeout_s` elapses — whichever comes first.
    Returns immediately when nothing is held. `held_check` is an
    injection seam for tests."""
    deadline = time.monotonic() + max(0.0, timeout_s)
    while held_check():
        if time.monotonic() >= deadline:
            return
        time.sleep(max(0.0, poll_s))


# ── Focus diagnostics (TradeStation order-misfire investigation) ──────
# When a hotkey send lands in the wrong place — e.g. TradeStation's Trade
# Bar (firing an order) instead of the chart command line — the root cause
# is almost always WHERE keyboard focus actually is at each step of the
# send. TitanX accepts focus on the activating click; some TradeStation
# layouts eat the first click for window activation and leave focus on the
# Trade Bar, so the ticker types there and Enter submits an order ("partial
# ticker entries before/after an attempted order entry"). These helpers
# snapshot the foreground window + the control that actually holds keyboard
# focus, so the GUI log records exactly what received the click and the
# keystrokes. Flip HOTKEY_FOCUS_DIAGNOSTICS off once the cause is pinned.
# All helpers FAIL OPEN (return a short marker, never raise) so diagnostics
# can't block or crash a real send.

HOTKEY_FOCUS_DIAGNOSTICS = False


def _describe_hwnd(hwnd) -> str:
    """Compact 'title [class] hwnd=N' for a window handle, or 'none'."""
    if not hwnd:
        return "none"
    try:
        import win32gui
        title = (win32gui.GetWindowText(hwnd) or "")[:40]
        cls = win32gui.GetClassName(hwnd) or ""
        return f'"{title}" [{cls}] hwnd={int(hwnd)}'
    except Exception:
        return f"hwnd={hwnd}"


def _focus_snapshot() -> str:
    """Foreground window + the control holding keyboard focus on the
    foreground thread (GetGUIThreadInfo). The focused control is what a
    typewrite / press actually drives — the single most useful fact for
    diagnosing a misrouted send."""
    try:
        import win32gui
    except Exception:
        return "diag-unavailable"
    try:
        fg = win32gui.GetForegroundWindow()
    except Exception:
        fg = 0
    focus_desc = "?"
    try:
        import ctypes
        from ctypes import wintypes

        class _GUITHREADINFO(ctypes.Structure):
            _fields_ = [
                ("cbSize", wintypes.DWORD),
                ("flags", wintypes.DWORD),
                ("hwndActive", wintypes.HWND),
                ("hwndFocus", wintypes.HWND),
                ("hwndCapture", wintypes.HWND),
                ("hwndMenuOwner", wintypes.HWND),
                ("hwndMoveSize", wintypes.HWND),
                ("hwndCaret", wintypes.HWND),
                ("rcCaret", wintypes.RECT),
            ]

        gti = _GUITHREADINFO()
        gti.cbSize = ctypes.sizeof(_GUITHREADINFO)
        # Passing thread id 0 reports the GUI info for the FOREGROUND thread.
        if ctypes.windll.user32.GetGUIThreadInfo(0, ctypes.byref(gti)):
            focus_desc = _describe_hwnd(gti.hwndFocus)
        else:
            focus_desc = "no-gti"
    except Exception:
        focus_desc = "gti-error"
    return f"foreground={_describe_hwnd(fg)} | focus={focus_desc}"


def _window_at(x: int, y: int) -> str:
    """Describe the window directly under a screen point — i.e. what the
    click is about to land on."""
    try:
        import win32gui
        return _describe_hwnd(win32gui.WindowFromPoint((int(x), int(y))))
    except Exception:
        return "n/a"


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
    # Refuse a stale/off-screen saved coordinate rather than clicking + typing
    # the ticker into whatever is now at that location (a monitor unplug,
    # resolution change, or moved window can leave click_x/click_y dangling).
    # Fail-open when bounds are indeterminable so legitimate sends still work.
    if not _coord_on_screen(cfg.click_x, cfg.click_y):
        msg(f"Hotkey: saved click position ({cfg.click_x},{cfg.click_y}) is "
            f"off-screen — refusing to fire. Re-set it in Hotkey Settings.")
        return False
    if (cfg.has_return_position
            and not _coord_on_screen(cfg.return_click_x, cfg.return_click_y)):
        msg(f"Hotkey: saved return-click ({cfg.return_click_x},"
            f"{cfg.return_click_y}) is off-screen — refusing to fire.")
        return False

    injected = pyautogui_module is not None
    if pyautogui_module is None:
        import pyautogui as pyautogui_module  # local import; see module docstring

    # Diagnostics are skipped under test injection (they read live OS window
    # state) and when the flag is off. `_diag` is a no-op in those cases so
    # the send path stays identical for tests / production-once-resolved.
    diag_on = HOTKEY_FOCUS_DIAGNOSTICS and not injected

    def _diag(phase: str) -> None:
        if not diag_on:
            return
        try:
            snap = _focus_snapshot()
        except Exception:
            return
        line = f"Hotkey[diag] {phase}: {snap}"
        log.info(line)
        msg(line)

    saved_failsafe = pyautogui_module.FAILSAFE
    saved_pause = pyautogui_module.PAUSE
    pyautogui_module.FAILSAFE = True
    pyautogui_module.PAUSE = 0.05
    try:
        if diag_on:
            try:
                pre = (f"Hotkey[diag] pre-click: target-under-cursor="
                       f"{_window_at(int(cfg.click_x), int(cfg.click_y))} | "
                       f"{_focus_snapshot()} | input-held="
                       f"{_any_cue_input_held()}")
            except Exception:
                pre = "Hotkey[diag] pre-click: (snapshot failed)"
            log.info(pre)
            msg(pre)
        # A mouse-button cue (right-click, middle-click, Shift/Ctrl+Left-click)
        # dispatches this send while the triggering button — and any modifier —
        # is STILL physically held. Moving the cursor and clicking the target
        # right now would (a) leave the user's eventual button release to land
        # on the TARGET window (popping its context menu / triggering autoscroll
        # and stealing focus from the input field), and (b) run typewrite with
        # Ctrl/Shift held, turning it into keyboard chords instead of text.
        # Either way the ticker never lands. Waiting for release first makes
        # every mouse cue behave like the keyboard (Enter) cue, which has no
        # held pointer state and so never hit this. Skipped under test injection
        # — the wait reads real OS key state and would otherwise depend on the
        # test runner's live keyboard; it also fails open (see _any_cue_input_held).
        if not injected:
            _wait_for_input_release()
        pyautogui_module.click(int(cfg.click_x), int(cfg.click_y))
        if cfg.delay_ms > 0:
            time.sleep(cfg.delay_ms / 1000.0)
        # Where did focus actually land after the click + settle delay? If
        # this is the Trade Bar (or the click only activated the window
        # without focusing the command line), the ticker is about to type
        # into the wrong control — this is the line that pins the bug.
        _diag("post-click (focus before typing)")
        # Type the ticker in LOWERCASE. pyautogui.typewrite emits each
        # UPPERCASE letter as Shift+<letter> (it holds Shift to capitalize),
        # and some target platforms bind Shift/Ctrl/Alt + letter to order-entry
        # hotkeys — notably TradeStation's chart Trade Bar. Sending an uppercase
        # ticker there fires one Trade Bar order hotkey PER capital letter (e.g.
        # Shift+S, Shift+D …), staging/submitting orders and splitting the
        # symbol across the order bar and the command line. Lowercase letters
        # carry no modifier, so they can't trigger a modifier+key hotkey, and
        # symbol entry is case-insensitive (sdot → SDOT) so the search still
        # works. This is what makes the same send safe on TradeStation as it
        # already was on TitanX. Do NOT "restore" uppercase here.
        pyautogui_module.typewrite(str(ticker).lower(), interval=0.03)
        # Focus may have shifted DURING typing (e.g. a stray char opened a
        # symbol box, splitting the ticker) — snapshot before the end key.
        _diag(f"after typing '{ticker}' (focus before end-key)")
        _press_end_sequence(pyautogui_module, cfg.end_sequence)
        _diag("after end-key")
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
