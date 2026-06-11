"""Tests for the launch-time OHLCV freshness gate (`_is_ohlcv_due`).

Bound to a bare object via __new__ (not a full MainWindow) so __init__'s
OHLCV worker / yfinance calls don't fire under pytest."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from trade_scanner_fh import config


class _FakeSettings:
    def __init__(self, value):
        self._value = value
    def value(self, key):
        return self._value


class _FakeLog:
    def __init__(self):
        self.lines = []
    def write_line(self, s):
        self.lines.append(s)


@pytest.fixture
def gate(_qapp):
    """A bare MainWindow with just the attributes `_is_ohlcv_due` touches."""
    from trade_scanner_fh.gui.main_window import MainWindow
    w = MainWindow.__new__(MainWindow)
    w.log_panel = _FakeLog()

    def _bind(stored):
        w._settings_obj = _FakeSettings(stored)
        w._qsettings = lambda: w._settings_obj
        return w
    return _bind


def test_no_record_is_due(gate):
    w = gate(None)
    assert w._is_ohlcv_due() is True
    assert any("no record" in ln for ln in w.log_panel.lines)


def test_empty_string_is_due(gate):
    w = gate("")
    assert w._is_ohlcv_due() is True


def test_run_after_last_close_is_not_due(gate):
    # Stamped 'now' (aware) — strictly after the most recent close.
    stored = datetime.now().astimezone().isoformat(timespec="seconds")
    w = gate(stored)
    assert w._is_ohlcv_due() is False
    assert any("skipping" in ln for ln in w.log_panel.lines)


def test_run_before_last_close_is_due(gate):
    # Stamped well before the most recent close.
    close = config.last_market_close()
    stored = (close - timedelta(days=3)).isoformat(timespec="seconds")
    w = gate(stored)
    assert w._is_ohlcv_due() is True
    assert any("running" in ln for ln in w.log_panel.lines)


def test_garbage_value_is_due(gate):
    w = gate("not-a-timestamp")
    assert w._is_ohlcv_due() is True


def test_naive_legacy_timestamp_after_close_not_due(gate):
    # Older builds may have stamped a naive local timestamp. A naive 'now'
    # should still read as fresh.
    stored = datetime.now().isoformat(timespec="seconds")  # naive
    w = gate(stored)
    assert w._is_ohlcv_due() is False
