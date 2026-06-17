"""Tests for hotkey.py — per-row ticker sender config + send sequence.

The send sequence is verified with an injected fake pyautogui module so
the suite stays headless / mouse-cursor-stable.
"""
from __future__ import annotations

import pytest

from trade_scanner_fh import hotkey
from trade_scanner_fh.hotkey import (
    CUE_OPTIONS, END_SEQUENCE_OPTIONS, HotkeyConfig,
    CUE_RIGHT_CLICK, CUE_SHIFT_LEFT, CUE_CTRL_LEFT, CUE_MIDDLE,
    CUE_ENTER_KEY, is_keyboard_cue,
    END_NONE, END_ENTER, END_TAB, END_CTRL_ENTER, END_SHIFT_ENTER,
    END_ALT_ENTER, send_ticker, cue_label, end_sequence_label,
)


# ──────────────────────────────────────────────────────────────────────
# Fake pyautogui — records every call so tests can assert the sequence
# ──────────────────────────────────────────────────────────────────────

class FakePyAutoGUI:
    def __init__(self):
        self.FAILSAFE = False
        self.PAUSE = 0.0
        self.calls: list[tuple] = []

    def click(self, x, y):
        self.calls.append(("click", x, y))

    def typewrite(self, text, interval=0):
        self.calls.append(("typewrite", text, interval))

    def press(self, key):
        self.calls.append(("press", key))

    def hotkey(self, *keys):
        self.calls.append(("hotkey", *keys))


# ──────────────────────────────────────────────────────────────────────
# HotkeyConfig defaults + normalization
# ──────────────────────────────────────────────────────────────────────

def test_default_config_has_no_position_and_sane_defaults():
    cfg = HotkeyConfig()
    assert cfg.click_x is None and cfg.click_y is None
    assert cfg.has_position is False
    assert cfg.delay_ms == 200
    assert cfg.cue == CUE_RIGHT_CLICK
    assert cfg.end_sequence == END_ENTER
    # Return click defaults to OFF — opt-in feature.
    assert cfg.return_click_x is None
    assert cfg.return_click_y is None
    assert cfg.has_return_position is False


def test_has_return_position_true_only_when_both_x_and_y_set():
    assert HotkeyConfig(return_click_x=10,
                        return_click_y=20).has_return_position is True
    assert HotkeyConfig(return_click_x=10,
                        return_click_y=None).has_return_position is False
    assert HotkeyConfig(return_click_x=None,
                        return_click_y=20).has_return_position is False


def test_normalize_preserves_return_position():
    cfg = HotkeyConfig(return_click_x=300, return_click_y=400).normalized()
    assert cfg.return_click_x == 300
    assert cfg.return_click_y == 400


def test_has_position_true_only_when_both_x_and_y_set():
    assert HotkeyConfig(click_x=10, click_y=20).has_position is True
    assert HotkeyConfig(click_x=10, click_y=None).has_position is False
    assert HotkeyConfig(click_x=None, click_y=20).has_position is False


def test_normalize_clamps_delay_and_resets_unknown_ids():
    cfg = HotkeyConfig(
        click_x=5, click_y=6,
        delay_ms=99999,
        cue="bogus_cue",
        end_sequence="bogus_end",
    )
    n = cfg.normalized()
    assert n.click_x == 5 and n.click_y == 6
    assert n.delay_ms == 5000  # clamped to ceiling
    assert n.cue == CUE_RIGHT_CLICK
    assert n.end_sequence == END_ENTER


def test_normalize_floors_negative_delay_to_zero():
    n = HotkeyConfig(delay_ms=-50).normalized()
    assert n.delay_ms == 0


def test_normalize_preserves_valid_values():
    cfg = HotkeyConfig(
        click_x=100, click_y=200,
        delay_ms=350,
        cue=CUE_SHIFT_LEFT,
        end_sequence=END_TAB,
    )
    n = cfg.normalized()
    assert n.click_x == 100 and n.click_y == 200
    assert n.delay_ms == 350
    assert n.cue == CUE_SHIFT_LEFT
    assert n.end_sequence == END_TAB


# ──────────────────────────────────────────────────────────────────────
# Option enumerations are exhaustive + uniquely keyed
# ──────────────────────────────────────────────────────────────────────

def test_cue_options_cover_all_five_kinds():
    ids = {cid for cid, _ in CUE_OPTIONS}
    assert ids == {
        CUE_RIGHT_CLICK, CUE_SHIFT_LEFT, CUE_CTRL_LEFT, CUE_MIDDLE,
        CUE_ENTER_KEY,
    }


def test_is_keyboard_cue_classifies_enter_key_only():
    assert is_keyboard_cue(CUE_ENTER_KEY) is True
    for mouse_id in (CUE_RIGHT_CLICK, CUE_SHIFT_LEFT,
                     CUE_CTRL_LEFT, CUE_MIDDLE):
        assert is_keyboard_cue(mouse_id) is False
    # Unknown ids are treated as non-keyboard (fall through to mouse path)
    assert is_keyboard_cue("nonsense") is False


def test_cue_label_for_enter_key_is_human_readable():
    assert cue_label(CUE_ENTER_KEY) == "Enter key (selected row)"


def test_end_sequence_options_cover_all_six_kinds():
    ids = {eid for eid, _ in END_SEQUENCE_OPTIONS}
    assert ids == {
        END_NONE, END_ENTER, END_TAB,
        END_CTRL_ENTER, END_SHIFT_ENTER, END_ALT_ENTER,
    }


def test_cue_label_returns_human_readable_string():
    assert cue_label(CUE_RIGHT_CLICK) == "Right-click"
    assert cue_label(CUE_SHIFT_LEFT) == "Shift + Left-click"
    # Unknown ids fall back to the raw id (no crash)
    assert cue_label("nonsense") == "nonsense"


def test_end_sequence_label_returns_human_readable_string():
    assert end_sequence_label(END_ENTER) == "Enter"
    assert end_sequence_label(END_CTRL_ENTER) == "Ctrl + Enter"
    assert end_sequence_label("nonsense") == "nonsense"


# ──────────────────────────────────────────────────────────────────────
# send_ticker — guards
# ──────────────────────────────────────────────────────────────────────

def test_send_ticker_refuses_when_no_position():
    fake = FakePyAutoGUI()
    cfg = HotkeyConfig()  # no position
    msgs: list[str] = []
    ok = send_ticker("AAPL", cfg, on_log=msgs.append, pyautogui_module=fake)
    assert ok is False
    assert fake.calls == []
    assert any("no click position" in m.lower() for m in msgs)


def test_send_ticker_refuses_when_ticker_empty():
    fake = FakePyAutoGUI()
    cfg = HotkeyConfig(click_x=10, click_y=20)
    msgs: list[str] = []
    ok = send_ticker("", cfg, on_log=msgs.append, pyautogui_module=fake)
    assert ok is False
    assert fake.calls == []
    assert any("no ticker" in m.lower() for m in msgs)


# ──────────────────────────────────────────────────────────────────────
# send_ticker — full sequence varies with end_sequence
# ──────────────────────────────────────────────────────────────────────

def test_send_ticker_default_sequence_is_click_type_enter():
    fake = FakePyAutoGUI()
    cfg = HotkeyConfig(click_x=100, click_y=200, delay_ms=0)
    ok = send_ticker("AAPL", cfg, pyautogui_module=fake)
    assert ok is True
    # Typed LOWERCASE so pyautogui never holds Shift (which would fire
    # platform Shift+letter order hotkeys — see send_ticker rationale).
    assert fake.calls == [
        ("click", 100, 200),
        ("typewrite", "aapl", 0.03),
        ("press", "enter"),
    ]


def test_send_ticker_end_none_skips_final_keypress():
    fake = FakePyAutoGUI()
    cfg = HotkeyConfig(
        click_x=1, click_y=2, delay_ms=0, end_sequence=END_NONE,
    )
    send_ticker("MSFT", cfg, pyautogui_module=fake)
    assert fake.calls == [
        ("click", 1, 2),
        ("typewrite", "msft", 0.03),
    ]


def test_send_ticker_end_tab_presses_tab():
    fake = FakePyAutoGUI()
    cfg = HotkeyConfig(
        click_x=1, click_y=2, delay_ms=0, end_sequence=END_TAB,
    )
    send_ticker("NVDA", cfg, pyautogui_module=fake)
    assert ("press", "tab") in fake.calls


@pytest.mark.parametrize("end_id,modifier", [
    (END_CTRL_ENTER, "ctrl"),
    (END_SHIFT_ENTER, "shift"),
    (END_ALT_ENTER, "alt"),
])
def test_send_ticker_modifier_variants_call_hotkey(end_id, modifier):
    fake = FakePyAutoGUI()
    cfg = HotkeyConfig(
        click_x=5, click_y=6, delay_ms=0, end_sequence=end_id,
    )
    send_ticker("AMD", cfg, pyautogui_module=fake)
    assert ("hotkey", modifier, "enter") in fake.calls


# ──────────────────────────────────────────────────────────────────────
# send_ticker — optional return click after the end-sequence
# ──────────────────────────────────────────────────────────────────────

def test_send_ticker_no_return_click_when_unset():
    """Default config has no return position — only one click should
    fire (the primary), not two."""
    fake = FakePyAutoGUI()
    cfg = HotkeyConfig(click_x=100, click_y=200, delay_ms=0)
    send_ticker("AAPL", cfg, pyautogui_module=fake)
    click_calls = [c for c in fake.calls if c[0] == "click"]
    assert click_calls == [("click", 100, 200)]


def test_send_ticker_fires_return_click_after_end_sequence():
    """With a return position set, sequence is:
    primary click → typewrite → end key → return click."""
    fake = FakePyAutoGUI()
    cfg = HotkeyConfig(
        click_x=100, click_y=200, delay_ms=0,
        return_click_x=500, return_click_y=600,
    )
    send_ticker("MSFT", cfg, pyautogui_module=fake)
    # Strip the interval arg from typewrite for a clean compare.
    expected = [
        ("click", 100, 200),
        ("typewrite", "msft", 0.03),
        ("press", "enter"),
        ("click", 500, 600),
    ]
    assert fake.calls == expected


def test_send_ticker_return_click_with_end_none():
    """Return click still fires when end-sequence is None — sequence:
    primary click → typewrite → return click (no end keypress)."""
    fake = FakePyAutoGUI()
    cfg = HotkeyConfig(
        click_x=1, click_y=2, delay_ms=0, end_sequence=END_NONE,
        return_click_x=99, return_click_y=88,
    )
    send_ticker("NVDA", cfg, pyautogui_module=fake)
    assert fake.calls == [
        ("click", 1, 2),
        ("typewrite", "nvda", 0.03),
        ("click", 99, 88),
    ]


def test_send_ticker_types_lowercase_to_avoid_modifier_hotkeys():
    """Regression: the ticker must be typed LOWERCASE. pyautogui holds Shift
    to produce capitals, and Shift+letter collides with platform order-entry
    hotkeys (TradeStation Trade Bar), placing orders. Lowercase carries no
    modifier; symbol entry is case-insensitive so the search still works."""
    fake = FakePyAutoGUI()
    cfg = HotkeyConfig(click_x=1, click_y=2, delay_ms=0, end_sequence=END_NONE)
    send_ticker("SDOT", cfg, pyautogui_module=fake)
    typed = [c for c in fake.calls if c[0] == "typewrite"]
    assert typed == [("typewrite", "sdot", 0.03)]
    # No capital letters reach pyautogui (which would imply a held Shift).
    assert not any(ch.isupper() for ch in typed[0][1])


def test_send_ticker_return_click_independent_of_primary_position():
    """The return click is keyed off `return_click_x/y`, NOT
    `click_x/y` — verify with deliberately-different coordinates."""
    fake = FakePyAutoGUI()
    cfg = HotkeyConfig(
        click_x=10, click_y=20,
        return_click_x=999, return_click_y=888,
        delay_ms=0,
    )
    send_ticker("AMD", cfg, pyautogui_module=fake)
    click_calls = [c for c in fake.calls if c[0] == "click"]
    assert click_calls == [("click", 10, 20), ("click", 999, 888)]


# ──────────────────────────────────────────────────────────────────────
# send_ticker — pyautogui globals restored on success and on error
# ──────────────────────────────────────────────────────────────────────

def test_send_ticker_restores_pyautogui_failsafe_and_pause():
    fake = FakePyAutoGUI()
    fake.FAILSAFE = "preserved-sentinel"
    fake.PAUSE = 0.77
    cfg = HotkeyConfig(click_x=1, click_y=2, delay_ms=0)
    send_ticker("AAPL", cfg, pyautogui_module=fake)
    assert fake.FAILSAFE == "preserved-sentinel"
    assert fake.PAUSE == 0.77


def test_send_ticker_restores_globals_even_when_typewrite_raises():
    fake = FakePyAutoGUI()
    fake.FAILSAFE = "kept"
    fake.PAUSE = 0.42

    def boom(*a, **k):
        raise RuntimeError("simulated typewrite failure")
    fake.typewrite = boom

    cfg = HotkeyConfig(click_x=1, click_y=2, delay_ms=0)
    with pytest.raises(RuntimeError):
        send_ticker("AAPL", cfg, pyautogui_module=fake)
    assert fake.FAILSAFE == "kept"
    assert fake.PAUSE == 0.42


# ──────────────────────────────────────────────────────────────────────
# Held-input guard — wait for mouse button / modifier release before typing
# (mouse cues fire while the triggering button/modifier is still held)
# ──────────────────────────────────────────────────────────────────────

def test_wait_for_input_release_returns_immediately_when_nothing_held():
    """held_check False on the first poll => no waiting, returns at once."""
    calls = {"n": 0}

    def never_held():
        calls["n"] += 1
        return False

    hotkey._wait_for_input_release(held_check=never_held)
    assert calls["n"] == 1  # checked once, found nothing held, returned


def test_wait_for_input_release_blocks_until_released():
    """Loops while held, returns as soon as held_check flips to False."""
    seq = iter([True, True, False])

    def held():
        return next(seq)

    # poll_s=0 keeps the test instant; large timeout so the timeout path
    # is NOT what ends the loop — the release is.
    hotkey._wait_for_input_release(timeout_s=99, poll_s=0, held_check=held)
    # If it didn't stop on the False, the iterator would raise StopIteration.


def test_wait_for_input_release_honors_timeout_when_held_forever():
    """A key held past the deadline must not wedge the send — the wait
    returns once the timeout elapses. timeout_s=0 makes the deadline
    'now', so it returns after a single held poll (no wall-clock sleep)."""
    held_calls = {"n": 0}

    def always_held():
        held_calls["n"] += 1
        return True

    hotkey._wait_for_input_release(
        timeout_s=0.0, poll_s=0, held_check=always_held,
    )
    # Returned despite never being released; didn't spin unbounded.
    assert held_calls["n"] >= 1


def test_any_cue_input_held_returns_bool_and_never_raises():
    """On the real platform nothing should be held during the test run,
    but the contract that matters is: it returns a bool and never raises
    (fails open). Don't assert the value — the runner's keyboard is live."""
    result = hotkey._any_cue_input_held()
    assert isinstance(result, bool)


def test_send_ticker_skips_release_wait_when_module_injected(monkeypatch):
    """Tests inject a fake pyautogui; the release wait reads real OS key
    state and must be skipped on that path so the suite stays hermetic."""
    called = {"wait": False}

    def _spy(*a, **k):
        called["wait"] = True

    monkeypatch.setattr(hotkey, "_wait_for_input_release", _spy)
    fake = FakePyAutoGUI()
    cfg = HotkeyConfig(click_x=1, click_y=2, delay_ms=0)
    ok = send_ticker("AAPL", cfg, pyautogui_module=fake)
    assert ok is True
    assert called["wait"] is False  # injected module => no real-input wait


# ──────────────────────────────────────────────────────────────────────
# Module-level pyautogui globals are NOT touched at import time
# (mirrors the tradestation.py guarantee)
# ──────────────────────────────────────────────────────────────────────

def test_importing_hotkey_does_not_clobber_pyautogui_globals():
    import pyautogui as real_pa
    saved_fs, saved_p = real_pa.FAILSAFE, real_pa.PAUSE
    try:
        real_pa.FAILSAFE = False
        real_pa.PAUSE = 0.99
        import importlib
        importlib.reload(hotkey)
        assert real_pa.FAILSAFE is False
        assert real_pa.PAUSE == 0.99
    finally:
        real_pa.FAILSAFE = saved_fs
        real_pa.PAUSE = saved_p
