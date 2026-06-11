"""Worker-spawn bodies in EarningsRefreshCoordinator (earnings_coordinator.py).

The coordinator resolves ``EarningsFillWorker`` / ``ZacksFillWorker``
through the ``main_window`` module namespace at call time
(``from . import main_window as _mw``) specifically so tests that patch
``trade_scanner_fh.gui.main_window.*`` keep working after the extraction
from MainWindow. These tests execute each spawn body against a bare
``MainWindow.__new__`` shell and prove that documented patch target is
honored: the patched class is constructed with the expected arguments and
``.start()`` fires. The finnhub/finviz bringups import their worker from
``.workers`` at call time, so their patch target is the workers module.

No QApplication needed — the spawn bodies build no widgets, and the
lazily-created coordinator is a plain (unparented) QObject.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def _make_win():
    """MainWindow-shaped shell with just enough state for the spawn
    bodies to run. Progress-panel bringup is stubbed out — the real
    `_earn_prog_begin` needs widgets built by the panel constructor."""
    from trade_scanner_fh.gui.main_window import MainWindow
    win = MainWindow.__new__(MainWindow)
    win.log_panel = MagicMock()
    win._earn_prog_begin = MagicMock()
    win._blacklist = set()
    return win


def test_kick_off_nasdaq_auto_refresh_spawns_patched_worker():
    """The Phase 5 auto-trigger constructs main_window.EarningsFillWorker
    (call-time `_mw` lookup), starts it, and reports the spawn."""
    win = _make_win()
    win._earnings_worker = None
    win._universe_df = None
    win._symbols = ["AAPL", "MSFT"]

    with patch(
        "trade_scanner_fh.gui.main_window.EarningsFillWorker",
    ) as worker_cls:
        started = win._kick_off_nasdaq_auto_refresh()

    assert started is True
    worker_cls.assert_called_once_with(["AAPL", "MSFT"], set(), mode="bulk")
    worker_cls.return_value.start.assert_called_once_with()
    assert win._earnings_worker is worker_cls.return_value


def test_start_zacks_worker_spawns_patched_worker():
    """_start_zacks_worker constructs main_window.ZacksFillWorker with the
    combined skip set, arms the zacks progress bar, and starts it."""
    win = _make_win()
    win._zacks_blacklist = set()
    win._universe_df = None  # _zacks_skip_set's ETF/ADR read → empty

    with patch(
        "trade_scanner_fh.gui.main_window.ZacksFillWorker",
    ) as worker_cls:
        win._start_zacks_worker(["AAA"], mode="targeted", label="Zacks fill")

    worker_cls.assert_called_once_with(["AAA"], set(), mode="targeted")
    worker_cls.return_value.start.assert_called_once_with()
    assert win._zacks_worker is worker_cls.return_value
    win._earn_prog_begin.assert_called_once_with("zacks", "Zacks fill")


def test_start_finnhub_worker_spawns_patched_worker():
    """_start_finnhub_worker imports FinnhubFillWorker from .workers at
    call time — patching the workers module intercepts the spawn."""
    win = _make_win()

    with patch(
        "trade_scanner_fh.gui.workers.FinnhubFillWorker",
    ) as worker_cls:
        win._start_finnhub_worker(
            ["AAA"], {"SKIP"}, mode="gap", label="Finnhub fill",
        )

    worker_cls.assert_called_once_with(["AAA"], {"SKIP"}, mode="gap")
    worker_cls.return_value.start.assert_called_once_with()
    assert win._finnhub_worker is worker_cls.return_value
    win._earn_prog_begin.assert_called_once_with("finnhub", "Finnhub fill")


def test_start_finviz_worker_spawns_patched_worker():
    """Same contract for the finviz bringup."""
    win = _make_win()

    with patch(
        "trade_scanner_fh.gui.workers.FinvizFillWorker",
    ) as worker_cls:
        win._start_finviz_worker(
            ["AAA"], {"SKIP"}, mode="bulk", label="Finviz fill",
        )

    worker_cls.assert_called_once_with(["AAA"], {"SKIP"}, mode="bulk")
    worker_cls.return_value.start.assert_called_once_with()
    assert win._finviz_worker is worker_cls.return_value
    win._earn_prog_begin.assert_called_once_with("finviz", "Finviz fill")
