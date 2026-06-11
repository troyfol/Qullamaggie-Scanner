"""GUI-side tests for the per-row HOTKEY sender's event filter.

Covers the right-click-cue context-menu suppression added alongside the
held-input release wait in hotkey.py: with the send now waiting for the
mouse button to be released, the cursor lingers over the results table
through the release, so the table's own row context menu must be
suppressed for the right-click cue (and ONLY that cue).
"""
from __future__ import annotations

import pytest

from trade_scanner_fh.hotkey import (
    HotkeyConfig, CUE_RIGHT_CLICK, CUE_MIDDLE, CUE_ENTER_KEY,
)


@pytest.fixture(autouse=True)
def _no_launch_data_pipeline(monkeypatch):
    """MainWindow.__init__ would otherwise kick off launch-time data/
    network workers (see test_audit_gui_fixes for the heap-corruption
    rationale). Stub the entry point so construction is inert."""
    from trade_scanner_fh.gui.main_window import MainWindow
    monkeypatch.setattr(
        MainWindow, "_load_universe_and_update", lambda self: None,
    )


def _context_menu_event():
    from PyQt6.QtCore import QPoint
    from PyQt6.QtGui import QContextMenuEvent
    return QContextMenuEvent(QContextMenuEvent.Reason.Mouse, QPoint(5, 5))


def _make_window():
    from trade_scanner_fh.gui.main_window import MainWindow
    return MainWindow()


def test_right_click_cue_suppresses_table_context_menu(_qapp):
    win = _make_window()
    try:
        win._hotkey_cfg = HotkeyConfig(
            click_x=10, click_y=10, cue=CUE_RIGHT_CLICK,
        )
        win._hotkey_enabled = True
        handled = win.eventFilter(
            win.results_table.viewport(), _context_menu_event(),
        )
        assert handled is True  # menu suppressed while right-click cue armed
    finally:
        win.close()


def test_context_menu_not_suppressed_when_hotkey_off(_qapp):
    win = _make_window()
    try:
        win._hotkey_cfg = HotkeyConfig(
            click_x=10, click_y=10, cue=CUE_RIGHT_CLICK,
        )
        win._hotkey_enabled = False  # hotkey OFF => normal context menu
        handled = win.eventFilter(
            win.results_table.viewport(), _context_menu_event(),
        )
        assert handled is False
    finally:
        win.close()


@pytest.mark.parametrize("cue", [CUE_MIDDLE, CUE_ENTER_KEY])
def test_context_menu_not_suppressed_for_non_right_click_cues(_qapp, cue):
    """Middle-click / Enter cues don't conflict with the context menu, so
    a plain right-click must still open it even with HOTKEY on."""
    win = _make_window()
    try:
        win._hotkey_cfg = HotkeyConfig(click_x=10, click_y=10, cue=cue)
        win._hotkey_enabled = True
        handled = win.eventFilter(
            win.results_table.viewport(), _context_menu_event(),
        )
        assert handled is False
    finally:
        win.close()
