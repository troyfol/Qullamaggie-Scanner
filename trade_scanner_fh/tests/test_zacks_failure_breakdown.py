"""Tests for the per-kind failure breakdown plumbing — ensures
ETFs/ADRs (FAIL_NOT_FOUND) and Imperva blocks (FAIL_BLOCKED) get
classified correctly and surfaced through the worker's
failure_breakdown signal."""
from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pandas as pd

from trade_scanner_fh import earnings_history as eh
from trade_scanner_fh.zacks_scraper import (
    FAIL_BLOCKED, FAIL_HTTP_ERROR, FAIL_NOT_FOUND, FAIL_PARSE_ERROR,
)


def _quarter():
    return {
        "period_ending": pd.Timestamp("2026-01-31"),
        "report_date": pd.Timestamp("2026-02-15"),
        "report_time": "Close",
        "estimated_eps": 2.0, "reported_eps": 2.1,
        "surprise_eps": 0.1, "surprise_eps_pct": 5.0,
        "estimated_rev": 100.0, "reported_rev": 105.0,
        "surprise_rev": 5.0, "surprise_rev_pct": 5.0,
    }


@contextmanager
def _patched_session(outcomes):
    """Patch ZacksSession.fetch to return outcomes in order, where
    each outcome is (rows_or_None, failure_kind)."""
    session = MagicMock()
    seq = iter(outcomes)

    def _fetch(*a, **kw):
        rows, kind = next(seq)
        session.last_failure_kind = kind
        return rows

    session.fetch.side_effect = _fetch

    @contextmanager
    def fake_ctx(*a, **kw):
        yield session

    with patch.object(eh, "ZacksSession", fake_ctx), \
         patch.object(eh.time, "sleep"):
        yield session


# ──────────────────────────────────────────────────────────────────────
# failed_cb invocation per failure kind
# ──────────────────────────────────────────────────────────────────────

def test_failed_cb_called_with_correct_kind(tmp_parquets):
    """Mixed run: 1 success + 1 each of blocked/not_found/http/parse →
    failed_cb gets called 4 times with the right (sym, kind) pairs."""
    outcomes = [
        ([_quarter()], None),               # AAPL: ok
        (None, FAIL_NOT_FOUND),             # SPY: ETF, no Zacks
        (None, FAIL_BLOCKED),               # MSFT: Imperva
        (None, FAIL_HTTP_ERROR),            # GOOG: network
        (None, FAIL_PARSE_ERROR),           # NVDA: bad page
    ]
    captured: list[tuple[str, str]] = []

    with _patched_session(outcomes):
        eh._fill_via_zacks(
            ["AAPL", "SPY", "MSFT", "GOOG", "NVDA"],
            blacklist=set(),
            failed_cb=lambda s, k: captured.append((s, k)),
            delay_sec=0,
        )

    assert ("SPY", FAIL_NOT_FOUND) in captured
    assert ("MSFT", FAIL_BLOCKED) in captured
    assert ("GOOG", FAIL_HTTP_ERROR) in captured
    assert ("NVDA", FAIL_PARSE_ERROR) in captured
    # AAPL succeeded, no callback for it
    assert all(s != "AAPL" for s, _ in captured)


def test_failed_cb_unknown_kind_when_session_returns_none_kind(tmp_parquets):
    """If something exotic happens and last_failure_kind is None
    on a failure, we tag it as 'unknown' so the breakdown still
    surfaces it."""
    outcomes = [(None, None)]  # rows=None but no kind set
    captured = []
    with _patched_session(outcomes):
        eh._fill_via_zacks(
            ["MYSTERY"], blacklist=set(),
            failed_cb=lambda s, k: captured.append((s, k)),
            delay_sec=0,
        )
    assert captured == [("MYSTERY", "unknown")]


def test_failed_cb_exception_does_not_crash_loop(tmp_parquets):
    """A buggy failed_cb shouldn't take down the entire fill."""
    outcomes = [
        (None, FAIL_NOT_FOUND),
        ([_quarter()], None),
    ]

    def boom(s, k):
        raise RuntimeError("oops")

    with _patched_session(outcomes):
        filled, errors = eh._fill_via_zacks(
            ["A", "B"], blacklist=set(),
            failed_cb=boom,
            delay_sec=0,
        )
    # Loop ran to completion despite the cb raising
    assert filled == 1
    assert errors == 1


# ──────────────────────────────────────────────────────────────────────
# ZacksFillWorker emits failure_breakdown
# ──────────────────────────────────────────────────────────────────────

def test_worker_emits_breakdown_signal(tmp_parquets, monkeypatch):
    """The worker's failure_breakdown signal carries a dict of
    {kind: [tickers]} matching what failed_cb received."""
    from PyQt6.QtWidgets import QApplication
    import sys
    app = QApplication.instance() or QApplication(sys.argv[:1])

    from trade_scanner_fh.gui.workers import ZacksFillWorker

    # Patch the underlying fill function to invoke failed_cb directly
    def fake_fill(symbols, blacklist, **kwargs):
        cb = kwargs.get("failed_cb")
        if cb:
            cb("SPY", FAIL_NOT_FOUND)
            cb("QQQ", FAIL_NOT_FOUND)
            cb("AAPL", FAIL_BLOCKED)
        return 0, 3

    monkeypatch.setattr(eh, "targeted_fill_zacks", fake_fill)
    monkeypatch.setattr(eh, "bulk_fill_zacks", fake_fill)
    # Pretend cookies exist so the worker doesn't short-circuit
    monkeypatch.setattr(
        "trade_scanner_fh.zacks_scraper.has_zacks_cookies",
        lambda: True,
    )

    worker = ZacksFillWorker(
        ["SPY", "QQQ", "AAPL"], blacklist=set(), mode="targeted",
    )
    captured = {}
    worker.failure_breakdown.connect(lambda d: captured.update(d))
    worker.run()  # synchronous

    assert captured.get(FAIL_NOT_FOUND) == ["SPY", "QQQ"]
    assert captured.get(FAIL_BLOCKED) == ["AAPL"]


# ──────────────────────────────────────────────────────────────────────
# Main window: _on_zacks_failures stores the dict
# ──────────────────────────────────────────────────────────────────────

def test_on_zacks_failures_auto_adds_not_found_to_skip_list(
    tmp_path, monkeypatch,
):
    """The slot should mirror the emitted breakdown into the instance
    attribute AND auto-add every FAIL_NOT_FOUND ticker to the
    Zacks-only skip list (persisted to disk)."""
    from PyQt6.QtWidgets import QApplication
    import sys
    app = QApplication.instance() or QApplication(sys.argv[:1])

    from trade_scanner_fh import config as cfg
    from trade_scanner_fh.gui.main_window import MainWindow
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)

    instance = MainWindow.__new__(MainWindow)
    instance._zacks_blacklist = set()
    instance._auto_added_zacks_skips = 0
    instance._last_zacks_failures = {}
    instance.log_panel = MagicMock()
    instance._ZACKS_BLACKLIST_FILE = tmp_path / "zacks_blacklist.txt"

    payload = {
        FAIL_NOT_FOUND: ["SPY", "QQQ", "ARKK"],
        FAIL_BLOCKED: ["AAPL"],
    }
    instance._on_zacks_failures(payload)

    # Stored breakdown for the menu action to read
    assert instance._last_zacks_failures == payload
    # All FAIL_NOT_FOUND auto-added to the skip list
    assert instance._zacks_blacklist == {"SPY", "QQQ", "ARKK"}
    # Counter for the end-of-run summary
    assert instance._auto_added_zacks_skips == 3
    # Persisted to disk (line-per-ticker)
    saved = instance._ZACKS_BLACKLIST_FILE.read_text(encoding="utf-8")
    assert "SPY" in saved
    assert "QQQ" in saved
    assert "ARKK" in saved
    # FAIL_BLOCKED should NOT have been added
    assert "AAPL" not in instance._zacks_blacklist


def test_on_zacks_failures_idempotent_for_already_listed(tmp_path, monkeypatch):
    """Running again with the same not_found list should NOT re-add
    tickers and should report 0 new additions."""
    from PyQt6.QtWidgets import QApplication
    import sys
    app = QApplication.instance() or QApplication(sys.argv[:1])

    from trade_scanner_fh import config as cfg
    from trade_scanner_fh.gui.main_window import MainWindow
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)

    instance = MainWindow.__new__(MainWindow)
    instance._zacks_blacklist = {"SPY", "QQQ"}
    instance._auto_added_zacks_skips = 0
    instance._last_zacks_failures = {}
    instance.log_panel = MagicMock()
    instance._ZACKS_BLACKLIST_FILE = tmp_path / "zacks_blacklist.txt"

    instance._on_zacks_failures({FAIL_NOT_FOUND: ["SPY", "QQQ"]})
    # No new additions, so no save needed
    assert instance._auto_added_zacks_skips == 0
    assert instance._zacks_blacklist == {"SPY", "QQQ"}


def test_zacks_skip_set_unions_universal_and_zacks_blacklists(tmp_path,
                                                              monkeypatch):
    """The combined skip set fed to ZacksFillWorker is the union of
    both blacklists. Universal-only tickers, Zacks-only tickers, and
    the overlap all appear."""
    from PyQt6.QtWidgets import QApplication
    import sys
    app = QApplication.instance() or QApplication(sys.argv[:1])

    from trade_scanner_fh import config as cfg
    from trade_scanner_fh.gui.main_window import MainWindow
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)

    instance = MainWindow.__new__(MainWindow)
    instance._blacklist = {"BAD1", "BAD2", "BOTH"}
    instance._zacks_blacklist = {"SPY", "QQQ", "BOTH"}

    skip = instance._zacks_skip_set()
    assert skip == {"BAD1", "BAD2", "BOTH", "SPY", "QQQ"}


def test_pre_skip_etfs_adrs_reads_universe_flags(tmp_path, monkeypatch):
    """`_pre_skip_known_etfs_adrs` walks `_universe_df` looking for
    `etf=True` or `adr=True` rows and adds those symbols to the
    Zacks skip list. Only NEW additions count."""
    from PyQt6.QtWidgets import QApplication
    import sys
    app = QApplication.instance() or QApplication(sys.argv[:1])

    from trade_scanner_fh import config as cfg
    from trade_scanner_fh.gui.main_window import MainWindow
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)

    universe = pd.DataFrame({
        "symbol": ["AAPL", "SPY", "QQQ", "BABA", "MSFT"],
        "etf":    [False, True, True, False, False],
        "adr":    [False, False, False, True, False],
    })

    instance = MainWindow.__new__(MainWindow)
    instance._universe_df = universe
    instance._zacks_blacklist = set()
    instance._ZACKS_BLACKLIST_FILE = tmp_path / "zacks_blacklist.txt"

    n = instance._pre_skip_known_etfs_adrs()
    assert n == 3  # SPY + QQQ + BABA
    assert instance._zacks_blacklist == {"SPY", "QQQ", "BABA"}
    assert "AAPL" not in instance._zacks_blacklist
    assert "MSFT" not in instance._zacks_blacklist

    # Calling again is idempotent
    n2 = instance._pre_skip_known_etfs_adrs()
    assert n2 == 0


def test_pre_skip_etfs_adrs_no_op_when_universe_missing(tmp_path,
                                                        monkeypatch):
    from trade_scanner_fh import config as cfg
    from trade_scanner_fh.gui.main_window import MainWindow
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)

    instance = MainWindow.__new__(MainWindow)
    instance._universe_df = None
    instance._zacks_blacklist = set()
    instance._ZACKS_BLACKLIST_FILE = tmp_path / "zacks_blacklist.txt"

    assert instance._pre_skip_known_etfs_adrs() == 0
    assert instance._zacks_blacklist == set()


def test_load_save_zacks_blacklist_round_trip(tmp_path, monkeypatch):
    """Persistence round-trip: save → fresh instance → load → identical."""
    from trade_scanner_fh import config as cfg
    from trade_scanner_fh.gui.main_window import MainWindow
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)

    inst1 = MainWindow.__new__(MainWindow)
    inst1._zacks_blacklist = {"SPY", "QQQ", "ARKK", "BABA"}
    inst1._ZACKS_BLACKLIST_FILE = tmp_path / "zacks_blacklist.txt"
    inst1._save_zacks_blacklist()

    inst2 = MainWindow.__new__(MainWindow)
    inst2._ZACKS_BLACKLIST_FILE = tmp_path / "zacks_blacklist.txt"
    inst2._load_zacks_blacklist()
    assert inst2._zacks_blacklist == {"SPY", "QQQ", "ARKK", "BABA"}
