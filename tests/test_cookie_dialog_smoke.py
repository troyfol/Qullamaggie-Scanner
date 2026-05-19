"""Smoke tests that actually CONSTRUCT the cookie dialogs — would have
caught the missing QDialogButtonBox import that crashed the v1 build.

Static-import audits don't help here: the NameError lives inside a slot
that fires at click time, so the bug can ship past pytest unless the
test exercises the actual dialog construction.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(scope="module")
def _qapp():
    from PyQt6.QtWidgets import QApplication
    import sys
    app = QApplication.instance() or QApplication(sys.argv[:1])
    yield app


# ──────────────────────────────────────────────────────────────────────
# Module-level helper used by both the menu actions and the startup prompt
# ──────────────────────────────────────────────────────────────────────

def test_module_level_cookie_textedit_widget_builds(_qapp):
    from trade_scanner_fh.gui.main_window import (
        _build_cookie_textedit_widget,
    )
    txt, btn = _build_cookie_textedit_widget("", hide_initially=False)
    assert txt is not None and btn is not None
    txt2, btn2 = _build_cookie_textedit_widget("k=v", hide_initially=True)
    assert btn2.text() == "Show"  # masked since initial content present


# ──────────────────────────────────────────────────────────────────────
# In-window cookie dialog: Set Zacks Cookies menu action
# ──────────────────────────────────────────────────────────────────────

def _bind_method(method_func, parent_widget, **stubs):
    """Bind a MainWindow method onto a real QWidget so QDialog(self)
    works. Avoids constructing the full MainWindow (which requires
    a universe / OHLCV cache) just to test a slot.

    Also patches `_build_cookie_textedit` to the module-level helper
    so the slots don't trip on the stub QWidget not being a MainWindow.
    """
    import types
    from trade_scanner_fh.gui.main_window import _build_cookie_textedit_widget
    for name, val in stubs.items():
        setattr(parent_widget, name, val)
    parent_widget._build_cookie_textedit = (
        lambda initial, *, hide_initially: _build_cookie_textedit_widget(
            initial, hide_initially=hide_initially,
        )
    )
    return types.MethodType(method_func, parent_widget)


def test_set_zacks_cookies_dialog_constructs_without_error(_qapp, tmp_path,
                                                            monkeypatch):
    """Bug fix verification: the slot used QDialogButtonBox without
    importing it, raising NameError on click. Patches `QDialog.exec` to
    return Cancel so the dialog isn't actually shown — but every line
    up to .exec() runs (including the QDialogButtonBox construction)."""
    from PyQt6.QtWidgets import QMainWindow
    from trade_scanner_fh import config as cfg
    from trade_scanner_fh.gui.main_window import MainWindow
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)

    parent = QMainWindow()
    bound = _bind_method(
        MainWindow._set_zacks_cookies_dialog, parent,
        log_panel=MagicMock(),
    )

    with patch(
        "trade_scanner_fh.gui.main_window.QDialog.exec",
        return_value=0,  # Rejected
    ):
        # If QDialogButtonBox (or anything else) isn't imported, this
        # raises NameError before the patched exec() is reached.
        bound()


# ──────────────────────────────────────────────────────────────────────
# Imperva auto-pause dialog
# ──────────────────────────────────────────────────────────────────────

def test_imperva_block_slot_runs_without_error(_qapp, tmp_path,
                                                 monkeypatch):
    """The Imperva-block slot now launches Firefox via the persistent-
    profile flow and arms a FirefoxCookieWaitWorker — no QDialog at
    entry. Smoke-test that the slot wires up the launch + worker
    without crashing on attribute lookups."""
    from PyQt6.QtWidgets import QMainWindow
    from trade_scanner_fh import config as cfg
    from trade_scanner_fh.gui.main_window import MainWindow
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)

    fake_worker = MagicMock()
    fake_worker.isRunning.return_value = True

    import types
    parent = QMainWindow()
    parent.log_panel = MagicMock()
    parent._zacks_worker = fake_worker
    parent._cookie_wait_worker = None
    parent._zacks_worker_awaiting_resume = None
    parent._cookie_monitor_geom = None
    parent.act_zacks_open_cookie_browser = MagicMock()
    # The slot delegates to _launch_cookie_browser_and_wait — bind
    # both so attribute lookup resolves.
    parent._on_imperva_block_detected = types.MethodType(
        MainWindow._on_imperva_block_detected, parent,
    )
    parent._launch_cookie_browser_and_wait = types.MethodType(
        MainWindow._launch_cookie_browser_and_wait, parent,
    )
    parent._on_cookie_wait_done = types.MethodType(
        MainWindow._on_cookie_wait_done, parent,
    )

    captured = {}

    def fake_launch(**kwargs):
        captured.update(kwargs)
        return 12345

    class FakeWaitWorker:
        def __init__(self, profile_dir, *, pre_signature=""):
            self.log_msg = MagicMock()
            self.finished = MagicMock()
        def isRunning(self):
            return False
        def start(self):
            captured["worker_started"] = True

    with patch(
        "trade_scanner_fh.zacks_scraper.launch_firefox_for_zacks_cookies",
        side_effect=fake_launch,
    ), patch(
        "trade_scanner_fh.gui.workers.FirefoxCookieWaitWorker",
        FakeWaitWorker,
    ):
        parent._on_imperva_block_detected(5)
    # Slot must have armed the resume-target and kicked off the wait
    assert parent._zacks_worker_awaiting_resume is fake_worker
    assert captured.get("worker_started") is True


# ──────────────────────────────────────────────────────────────────────
# First-launch Zacks prompt
# ──────────────────────────────────────────────────────────────────────

def test_zacks_startup_prompt_skips_when_cookies_set(_qapp, monkeypatch):
    """When `has_zacks_cookies()` is True, the startup prompt must
    return immediately without building a dialog."""
    from trade_scanner_fh.gui import main_window as mw
    monkeypatch.setattr(
        "trade_scanner_fh.zacks_scraper.has_zacks_cookies",
        lambda: True,
    )
    # Guard: if QDialog is built when it shouldn't be, this'd error.
    with patch(
        "trade_scanner_fh.gui.main_window.QDialog.exec",
        side_effect=AssertionError("dialog should not have been built"),
    ):
        mw.prompt_zacks_cookies_if_missing()


def test_zacks_startup_prompt_constructs_when_cookies_absent(
    _qapp, tmp_path, monkeypatch,
):
    """No cookies stored → dialog is built and exec()'d. Patching
    exec() to return Cancel covers the construction path without
    blocking on user input."""
    from trade_scanner_fh import config as cfg
    from trade_scanner_fh.gui import main_window as mw
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)
    monkeypatch.setattr(
        "trade_scanner_fh.zacks_scraper.has_zacks_cookies",
        lambda: False,
    )

    with patch(
        "trade_scanner_fh.gui.main_window.QDialog.exec",
        return_value=0,  # Skip
    ):
        mw.prompt_zacks_cookies_if_missing()


def test_zacks_startup_prompt_saves_cookies_on_accept(
    _qapp, tmp_path, monkeypatch,
):
    """Pasting a non-empty cookie string + clicking Save persists it."""
    from trade_scanner_fh import config as cfg
    from trade_scanner_fh.gui import main_window as mw
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)
    monkeypatch.setattr(
        "trade_scanner_fh.zacks_scraper.has_zacks_cookies",
        lambda: False,
    )

    captured = {}

    def fake_set(s):
        captured["value"] = s
        return True

    monkeypatch.setattr(
        "trade_scanner_fh.zacks_scraper.set_zacks_cookies", fake_set,
    )

    # Patch QDialog.exec to return Accepted, AND patch QTextEdit so the
    # textarea returns our test value when toPlainText() is called.
    with patch(
        "trade_scanner_fh.gui.main_window.QDialog.exec",
        return_value=1,  # Accepted
    ), patch(
        "trade_scanner_fh.gui.main_window.QTextEdit.toPlainText",
        return_value="reese84=abcd1234; visid_incap_2944342=xyz",
    ):
        mw.prompt_zacks_cookies_if_missing()

    assert captured["value"] == "reese84=abcd1234; visid_incap_2944342=xyz"


def test_zacks_startup_prompt_blank_save_is_skip(
    _qapp, tmp_path, monkeypatch,
):
    """Accepted with an empty textarea → no cookies persisted."""
    from trade_scanner_fh import config as cfg
    from trade_scanner_fh.gui import main_window as mw
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)
    monkeypatch.setattr(
        "trade_scanner_fh.zacks_scraper.has_zacks_cookies",
        lambda: False,
    )

    set_called = []
    monkeypatch.setattr(
        "trade_scanner_fh.zacks_scraper.set_zacks_cookies",
        lambda s: set_called.append(s) or True,
    )

    with patch(
        "trade_scanner_fh.gui.main_window.QDialog.exec",
        return_value=1,
    ), patch(
        "trade_scanner_fh.gui.main_window.QTextEdit.toPlainText",
        return_value="   ",  # whitespace-only
    ):
        mw.prompt_zacks_cookies_if_missing()

    assert set_called == [], "blank input must not call set_zacks_cookies"
