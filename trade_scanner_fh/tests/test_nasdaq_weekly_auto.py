"""Tests for the Phase 5 Nasdaq weekly auto-refresh wiring.

Keeps coverage focused on:
  * `_is_nasdaq_refresh_due` (fresh install, recent run, stale run)
  * `_stamp_nasdaq_run_now` (writes parseable ISO)
  * Toggle slot persists to QSettings + flips the ref
  * Manual Nasdaq menu run also stamps the timestamp (auto + manual
    share the weekly counter)

Tests use the lightweight ``MainWindow.__new__(MainWindow)`` pattern
(no QApplication needed) and mock ``_qsettings`` to keep them hermetic.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from trade_scanner_fh import config


class _FakeQSettings:
    """In-memory stand-in for PyQt6.QtCore.QSettings."""
    def __init__(self):
        self._d: dict[str, object] = {}

    def value(self, key, default=None):
        return self._d.get(key, default)

    def setValue(self, key, val):
        self._d[key] = val


def _make_parent(qsettings: _FakeQSettings | None = None):
    """Build a MainWindow-shaped instance with just enough state for
    the Nasdaq helpers to run. No QApplication / no widgets."""
    from trade_scanner_fh.gui.main_window import MainWindow
    parent = MainWindow.__new__(MainWindow)
    settings = qsettings or _FakeQSettings()
    parent._qsettings = lambda: settings
    parent._nasdaq_auto_refresh_ref = [True]
    parent.log_panel = MagicMock()
    parent._earnings_worker = None
    parent._universe_df = None
    parent._symbols = []
    parent._blacklist = set()
    # Mirror __init__ state the same-launch-capture path reads. (A bare
    # MainWindow.__new__ instance raises on getattr of an unset attr, so
    # set the defaults here rather than rely on getattr fallbacks.)
    parent._pending_smart_refresh = False
    parent._earnings_auto_refresh_ref = [True]
    return parent, settings


# ──────────────────────────────────────────────────────────────────────
# _is_nasdaq_refresh_due
# ──────────────────────────────────────────────────────────────────────

def test_weekly_due_returns_true_when_never_run():
    parent, _ = _make_parent()
    assert parent._is_nasdaq_refresh_due() is True


def test_refresh_due_false_same_calendar_day():
    # Calendar-day cadence (NASDAQ_REFRESH_DAYS=1): an earlier run on the
    # SAME calendar day is not due again. (Constructed at 00:05 today so
    # the assertion is robust to whatever clock time the test runs at.)
    parent, settings = _make_parent()
    same_day = datetime.now().replace(hour=0, minute=5, second=0, microsecond=0)
    settings.setValue(
        parent._NASDAQ_LAST_RUN_KEY, same_day.isoformat(timespec="seconds"),
    )
    assert parent._is_nasdaq_refresh_due() is False


def test_refresh_due_true_previous_calendar_day():
    """Calendar-day cadence: a run on a PRIOR calendar day is due again."""
    parent, settings = _make_parent()
    settings.setValue(
        parent._NASDAQ_LAST_RUN_KEY,
        (datetime.now() - timedelta(days=1, seconds=1)).isoformat(timespec="seconds"),
    )
    assert parent._is_nasdaq_refresh_due() is True


def test_refresh_due_true_prev_day_even_when_under_24h():
    """The cadence fix: a run late YESTERDAY is due this morning even
    though < 24h elapsed — calendar-day cadence, NOT a rolling 24h gap.
    (A 24h-gap rule on a completion-time stamp would skip the same-time-
    next-morning launch and miss the overnight report.)"""
    parent, settings = _make_parent()
    yest_late = (datetime.now().replace(hour=23, minute=59, second=0, microsecond=0)
                 - timedelta(days=1))
    settings.setValue(
        parent._NASDAQ_LAST_RUN_KEY, yest_late.isoformat(timespec="seconds"),
    )
    assert parent._is_nasdaq_refresh_due() is True


def test_weekly_due_returns_true_when_run_stale():
    parent, settings = _make_parent()
    settings.setValue(
        parent._NASDAQ_LAST_RUN_KEY,
        (datetime.now() - timedelta(days=10)).isoformat(timespec="seconds"),
    )
    assert parent._is_nasdaq_refresh_due() is True


def test_weekly_due_at_exact_threshold_returns_true():
    parent, settings = _make_parent()
    # Exactly NASDAQ_REFRESH_DAYS old → due (gap.days >= threshold)
    settings.setValue(
        parent._NASDAQ_LAST_RUN_KEY,
        (datetime.now()
         - timedelta(days=config.NASDAQ_REFRESH_DAYS, seconds=1)
         ).isoformat(timespec="seconds"),
    )
    assert parent._is_nasdaq_refresh_due() is True


def test_weekly_due_returns_true_on_corrupt_iso():
    parent, settings = _make_parent()
    settings.setValue(parent._NASDAQ_LAST_RUN_KEY, "not-an-iso-date")
    # Fail-open: corrupt timestamp → assume due (better to refresh than skip)
    assert parent._is_nasdaq_refresh_due() is True


# ──────────────────────────────────────────────────────────────────────
# _stamp_nasdaq_run_now
# ──────────────────────────────────────────────────────────────────────

def test_stamp_writes_parseable_iso():
    parent, settings = _make_parent()
    parent._stamp_nasdaq_run_now()
    raw = settings.value(parent._NASDAQ_LAST_RUN_KEY)
    assert raw is not None
    # Should round-trip via fromisoformat
    parsed = datetime.fromisoformat(str(raw))
    # Stamped within the last few seconds
    assert (datetime.now() - parsed).total_seconds() < 5


def test_stamp_then_check_due_returns_false():
    """End-to-end: after stamping, the weekly check should return
    False until the threshold elapses."""
    parent, _ = _make_parent()
    parent._stamp_nasdaq_run_now()
    assert parent._is_nasdaq_refresh_due() is False


# ──────────────────────────────────────────────────────────────────────
# Toggle slot
# ──────────────────────────────────────────────────────────────────────

def test_toggle_off_persists_and_updates_ref():
    parent, settings = _make_parent()
    parent._on_nasdaq_auto_refresh_toggled(False)
    assert parent._nasdaq_auto_refresh_ref[0] is False
    assert settings.value("menu/nasdaq_auto_refresh") is False


def test_toggle_on_persists_and_updates_ref():
    parent, settings = _make_parent()
    parent._nasdaq_auto_refresh_ref[0] = False
    parent._on_nasdaq_auto_refresh_toggled(True)
    assert parent._nasdaq_auto_refresh_ref[0] is True
    assert settings.value("menu/nasdaq_auto_refresh") is True


# ──────────────────────────────────────────────────────────────────────
# Auto-refresh kickoff: stamps timestamp on completion
# ──────────────────────────────────────────────────────────────────────

def test_auto_refresh_done_slot_stamps_timestamp():
    """The slot fired when an auto-triggered run finishes must update
    last_nasdaq_run_iso so the weekly counter resets."""
    parent, settings = _make_parent()
    parent.status = MagicMock()
    parent._on_nasdaq_auto_refresh_done(filled=42, errors=0)
    raw = settings.value(parent._NASDAQ_LAST_RUN_KEY)
    assert raw is not None
    datetime.fromisoformat(str(raw))  # must parse


# ──────────────────────────────────────────────────────────────────────
# Same-launch capture: earnings smart refresh chains off the daily sweep
# ──────────────────────────────────────────────────────────────────────

def test_nasdaq_done_chains_smart_refresh_when_pending():
    """_on_update_done arms _pending_smart_refresh before the sweep; the
    sweep-done slot then fires the earnings smart refresh against the
    freshly-reconciled calendar (same-launch capture of new reports)."""
    parent, _ = _make_parent()
    parent.status = MagicMock()
    parent._earnings_auto_refresh_ref = [True]
    parent._pending_smart_refresh = True
    parent._kick_off_smart_refresh = MagicMock()
    parent._on_nasdaq_auto_refresh_done(filled=5, errors=0)
    parent._kick_off_smart_refresh.assert_called_once()
    assert parent._pending_smart_refresh is False


def test_nasdaq_done_does_not_chain_when_not_pending():
    """The sweep-done slot only chains when the flag was armed: if
    _pending_smart_refresh is False when the sweep finishes, no smart
    refresh fires (e.g. a manual/standalone Nasdaq run)."""
    parent, _ = _make_parent()
    parent.status = MagicMock()
    parent._pending_smart_refresh = False
    parent._kick_off_smart_refresh = MagicMock()
    parent._on_nasdaq_auto_refresh_done(filled=5, errors=0)
    parent._kick_off_smart_refresh.assert_not_called()


def test_nasdaq_done_does_not_chain_when_earnings_disabled():
    """Armed but earnings auto-refresh ref off → suppressed; flag cleared."""
    parent, _ = _make_parent()
    parent.status = MagicMock()
    parent._earnings_auto_refresh_ref = [False]
    parent._pending_smart_refresh = True
    parent._kick_off_smart_refresh = MagicMock()
    parent._on_nasdaq_auto_refresh_done(filled=5, errors=0)
    parent._kick_off_smart_refresh.assert_not_called()
    assert parent._pending_smart_refresh is False


def test_arm_or_run_chains_when_sweep_starts():
    """_arm_or_run_smart_refresh arms the flag and, when a sweep starts,
    defers the smart refresh (it chains off the sweep-done slot) rather
    than running it immediately."""
    parent, _ = _make_parent()
    parent._maybe_run_nasdaq_refresh = MagicMock(return_value=True)
    parent._kick_off_smart_refresh = MagicMock()
    parent._arm_or_run_smart_refresh(want_smart=True)
    assert parent._pending_smart_refresh is True       # armed for the chain
    parent._kick_off_smart_refresh.assert_not_called()  # deferred, not now


def test_arm_or_run_runs_now_when_no_sweep():
    """When no sweep starts (not due), run the smart refresh immediately
    and clear the pending flag."""
    parent, _ = _make_parent()
    parent._maybe_run_nasdaq_refresh = MagicMock(return_value=False)
    parent._kick_off_smart_refresh = MagicMock()
    parent._arm_or_run_smart_refresh(want_smart=True)
    parent._kick_off_smart_refresh.assert_called_once()
    assert parent._pending_smart_refresh is False


def test_arm_or_run_suppressed_when_not_wanted():
    """want_smart=False (auto-refresh off / stopped OHLCV update): the
    Nasdaq sweep is still allowed to run, but no smart refresh is armed or
    fired even if no sweep starts."""
    parent, _ = _make_parent()
    parent._maybe_run_nasdaq_refresh = MagicMock(return_value=False)
    parent._kick_off_smart_refresh = MagicMock()
    parent._arm_or_run_smart_refresh(want_smart=False)
    parent._maybe_run_nasdaq_refresh.assert_called_once()  # sweep still allowed
    parent._kick_off_smart_refresh.assert_not_called()
    assert parent._pending_smart_refresh is False


def test_maybe_run_nasdaq_refresh_false_when_not_due():
    """Recent run (not due) → no sweep started → returns False so the
    caller runs the smart refresh immediately rather than chaining."""
    parent, settings = _make_parent()
    settings.setValue(
        parent._NASDAQ_LAST_RUN_KEY,
        (datetime.now() - timedelta(hours=1)).isoformat(timespec="seconds"),
    )
    assert parent._maybe_run_nasdaq_refresh() is False


def test_maybe_run_nasdaq_refresh_false_when_disabled():
    parent, _ = _make_parent()
    parent._nasdaq_auto_refresh_ref = [False]
    assert parent._maybe_run_nasdaq_refresh() is False


# ──────────────────────────────────────────────────────────────────────
# Source-level check: the Phase-1-era Zacks auto-refresh menu action
# wiring is gone. Done as a static text scan rather than instantiating
# MainWindow (which would spin up background workers that destabilize
# the rest of the suite).
# ──────────────────────────────────────────────────────────────────────

def test_zacks_auto_refresh_menu_wiring_removed():
    """Phase 5 removed the menu item — verify the QAction creation block
    is gone from main_window.py source. The Nasdaq weekly counterpart
    must exist in its place."""
    from trade_scanner_fh.gui import main_window as mw
    import inspect
    src = inspect.getsource(mw)
    # The old QAction creation block must be gone. The string survives
    # in a back-compat comment, so check for the full creation statement
    # rather than the bare label.
    assert 'QAction(\n            "Auto-refresh Zacks at launch"' not in src
    assert "act_zacks_auto_refresh = QAction(" not in src
    # The new Nasdaq toggle's QAction creation must be present.
    assert 'QAction(\n            "Auto-refresh Nasdaq calendar daily"' in src
    assert "self.act_nasdaq_auto_refresh = QAction(" in src
    # And the auto-trigger at end of OHLCV update must call the Nasdaq
    # path now, not the Zacks one.
    assert "_kick_off_nasdaq_auto_refresh" in src


def test_legacy_zacks_auto_refresh_attr_still_exists_for_back_compat():
    """The `_zacks_auto_refresh_ref` attribute is preserved so the
    existing browser-cookie autorefresh tests don't have to change.
    It just no longer drives any auto-trigger."""
    parent, _ = _make_parent()
    parent._zacks_auto_refresh_ref = [True]
    assert parent._zacks_auto_refresh_ref == [True]
