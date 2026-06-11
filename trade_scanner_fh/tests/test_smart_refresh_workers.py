"""Regression tests for the concurrent earnings smart-refresh path.

Bug (2026-05-31): the launch-time smart refresh dispatches mode="targeted"
to all three fill workers, but FinvizFillWorker / FinnhubFillWorker only
recognised {"bulk", "gap"} and crashed with
``ValueError: unknown FinvizFillWorker mode: 'targeted'``. The fix maps
"targeted" onto the gap-fill primitive (iterate exactly the supplied
symbol list). These tests pin that contract so it can't regress.
"""
from __future__ import annotations

import pytest


@pytest.fixture(scope="module")
def _qapp():
    from PyQt6.QtWidgets import QApplication
    import sys
    app = QApplication.instance() or QApplication(sys.argv[:1])
    yield app


def test_finviz_worker_targeted_calls_gap_fill(_qapp, monkeypatch):
    """mode='targeted' must NOT raise — it routes to gap_fill_finviz with
    exactly the supplied symbols."""
    from trade_scanner_fh import finviz_fill
    from trade_scanner_fh.gui.workers import FinvizFillWorker

    seen = {}

    def fake_gap(symbols, blacklist, **kwargs):
        seen["symbols"] = list(symbols)
        seen["called"] = "gap"
        return 3, 1

    def fake_bulk(*a, **k):  # must NOT be called
        seen["called"] = "bulk"
        return 0, 0

    monkeypatch.setattr(finviz_fill, "gap_fill_finviz", fake_gap)
    monkeypatch.setattr(finviz_fill, "bulk_fill_finviz", fake_bulk)

    worker = FinvizFillWorker(["AAA", "BBB"], blacklist=set(), mode="targeted")
    results = {}
    worker.finished.connect(lambda f, e: results.update(filled=f, errors=e))
    worker.run()  # synchronous

    assert seen.get("called") == "gap"
    assert seen.get("symbols") == ["AAA", "BBB"]
    assert results == {"filled": 3, "errors": 1}


def test_finnhub_worker_targeted_calls_gap_fill(_qapp, monkeypatch):
    from trade_scanner_fh import finnhub_fill
    from trade_scanner_fh.gui.workers import FinnhubFillWorker

    seen = {}

    def fake_gap(symbols, blacklist, **kwargs):
        seen["symbols"] = list(symbols)
        seen["called"] = "gap"
        return 5, 0

    def fake_bulk(*a, **k):
        seen["called"] = "bulk"
        return 0, 0

    monkeypatch.setattr(finnhub_fill, "gap_fill_finnhub", fake_gap)
    monkeypatch.setattr(finnhub_fill, "bulk_fill_finnhub", fake_bulk)

    worker = FinnhubFillWorker(["AAA", "BBB"], blacklist=set(), mode="targeted")
    results = {}
    worker.finished.connect(lambda f, e: results.update(filled=f, errors=e))
    worker.run()

    assert seen.get("called") == "gap"
    assert seen.get("symbols") == ["AAA", "BBB"]
    assert results == {"filled": 5, "errors": 0}


@pytest.mark.parametrize("mode", ["bogus", "smartx", ""])
def test_finviz_worker_truly_unknown_mode_still_handled(_qapp, monkeypatch, mode):
    """A genuinely unknown mode must still be caught (worker emits a
    0/0 finished rather than tearing down the thread silently)."""
    from trade_scanner_fh.gui.workers import FinvizFillWorker

    worker = FinvizFillWorker(["AAA"], blacklist=set(), mode=mode)
    results = {}
    worker.finished.connect(lambda f, e: results.update(filled=f, errors=e))
    worker.run()
    # run() catches the ValueError and emits (0, 0)
    assert results == {"filled": 0, "errors": 0}


# ──────────────────────────────────────────────────────────────────────
# Earnings progress panel — the concurrent-progress surface
# ──────────────────────────────────────────────────────────────────────

class _Log:
    def __init__(self):
        self.lines = []
    def write_line(self, s):
        self.lines.append(s)


@pytest.fixture
def panel(_qapp):
    """A bare MainWindow carrying just the attributes the progress-panel
    helpers touch, plus a built panel hosted on a throwaway widget."""
    from PyQt6.QtWidgets import QVBoxLayout, QWidget
    from trade_scanner_fh.gui.main_window import MainWindow

    w = MainWindow.__new__(MainWindow)
    w.log_panel = _Log()
    # Mirror real __init__: the three earnings workers start as None.
    w._finviz_worker = None
    w._zacks_worker = None
    w._finnhub_worker = None
    host = QWidget()
    QVBoxLayout(host)
    w._build_earnings_progress_panel(host.layout())
    w._panel_host = host  # keep alive
    return w


def test_panel_begin_shows_and_marks_running(panel):
    w = panel
    assert w._earn_prog_panel.isVisibleTo(w._panel_host) is False
    w._earn_prog_begin("finviz", "Smart refresh (finviz)")
    assert w._earn_prog_panel.isVisibleTo(w._panel_host) is True
    assert w._earn_prog_state["finviz"]["status"] == "running"
    assert w._earn_state_active() is True


def test_panel_tick_updates_bar_and_tooltip(panel):
    w = panel
    w._earn_prog_begin("finviz", "x")
    w._earn_prog_tick("finviz", 25, 100)
    bar = w._earn_prog_bars["finviz"]
    assert bar.value() == 25
    assert bar.maximum() == 100
    assert "25/100" in bar.toolTip()


def test_panel_collapses_only_when_all_done(panel):
    w = panel
    w._earn_prog_begin("finviz", "x")
    w._earn_prog_begin("zacks", "y")
    w._earn_prog_finish("finviz", 90, 10)
    # zacks still running → no collapse
    assert w._earn_state_active() is True
    assert w._earn_stop_btn.text() == "Stop Earnings Refresh"
    w._earn_prog_finish("zacks", 5, 0)
    assert w._earn_state_active() is False
    assert w._earn_stop_btn.text() == "Hide"
    tip = w._earn_prog_bars["zacks"].toolTip()
    assert "Filled: 5" in tip and "Errors: 0" in tip


def test_panel_fresh_batch_resets_stale_done_bars(panel):
    w = panel
    w._earn_prog_begin("finviz", "x")
    w._earn_prog_finish("finviz", 1, 0)
    assert w._earn_state_active() is False
    # New single-source batch: the stale finviz 'done' bar must reset.
    w._earn_prog_begin("finnhub", "Finnhub gap fill")
    assert w._earn_prog_state["finviz"]["status"] == "idle"
    assert w._earn_prog_state["finnhub"]["status"] == "running"


def test_stop_button_hides_panel_when_idle(panel):
    w = panel
    w._earn_prog_begin("finviz", "x")
    w._earn_prog_finish("finviz", 1, 0)
    # Nothing running → Stop acts as Hide.
    w._stop_all_earnings_fills()
    assert w._earn_prog_panel.isVisibleTo(w._panel_host) is False


# ──────────────────────────────────────────────────────────────────────
# Manual "Run Earnings Smart Refresh Now" trigger
# ──────────────────────────────────────────────────────────────────────

@pytest.fixture
def manual(_qapp, monkeypatch):
    """Bare MainWindow with the manual-trigger collaborators stubbed and a
    recorder swapped in for the worker launcher."""
    from trade_scanner_fh.gui import main_window as mw
    from trade_scanner_fh import earnings_history as eh

    w = mw.MainWindow.__new__(mw.MainWindow)
    w.log_panel = _Log()
    w._universe_df = None
    w._symbols = ["SEED"]  # non-empty so the no-universe guard passes through
    w._blacklist = set()
    # Per-source skip lists — the manual smart-refresh now applies the same
    # all-three-sources-blocked candidate trim as the auto path (audit M5).
    w._finnhub_blacklist = set()
    w._finviz_blacklist = set()
    w._zacks_blacklist = set()
    w._get_universe_symbols = lambda: [f"T{i}" for i in range(60)]
    w._etf_adr_auto_skip_set = lambda: set()
    w._earn_threads_active = lambda: False

    launches = []
    w._launch_smart_refresh_workers = (
        lambda cands, *, due=True, include_finnhub=True:
            launches.append((list(cands), due, include_finnhub))
    )

    # Silence both dialog kinds; question() answer is overridden per test.
    monkeypatch.setattr(mw.QMessageBox, "information", lambda *a, **k: None)
    monkeypatch.setattr(
        mw.QMessageBox, "question",
        lambda *a, **k: mw.QMessageBox.StandardButton.Yes,
    )
    return w, mw, eh, launches, monkeypatch


def test_manual_refresh_launches_due_candidates(manual):
    w, mw, eh, launches, monkeypatch = manual
    monkeypatch.setattr(
        eh, "find_smart_refresh_candidates",
        lambda syms, skip, **k: ["AAA", "BBB", "CCC"],
    )
    w._run_earnings_smart_refresh_now()
    # Manual trigger keeps finnhub (include_finnhub=True).
    assert launches == [(["AAA", "BBB", "CCC"], True, True)]


def test_manual_refresh_offers_sample_when_nothing_due(manual):
    w, mw, eh, launches, monkeypatch = manual
    monkeypatch.setattr(
        eh, "find_smart_refresh_candidates", lambda syms, skip, **k: [],
    )
    w._run_earnings_smart_refresh_now()
    # Nothing due + user said Yes → sample test pass (<=25 tickers), due=False.
    assert len(launches) == 1
    cands, due, inc = launches[0]
    assert due is False
    assert inc is True  # manual sample run keeps finnhub
    assert len(cands) == 25
    assert cands == [f"T{i}" for i in range(25)]


def test_manual_refresh_sample_declined_launches_nothing(manual):
    w, mw, eh, launches, monkeypatch = manual
    monkeypatch.setattr(
        eh, "find_smart_refresh_candidates", lambda syms, skip, **k: [],
    )
    monkeypatch.setattr(
        mw.QMessageBox, "question",
        lambda *a, **k: mw.QMessageBox.StandardButton.No,
    )
    w._run_earnings_smart_refresh_now()
    assert launches == []


def test_manual_refresh_noop_when_already_running(manual):
    w, mw, eh, launches, monkeypatch = manual
    w._earn_threads_active = lambda: True
    monkeypatch.setattr(
        eh, "find_smart_refresh_candidates",
        lambda syms, skip, **k: ["AAA"],
    )
    w._run_earnings_smart_refresh_now()
    assert launches == []


# ──────────────────────────────────────────────────────────────────────
# Candidate trim: drop tickers every source has blacklisted.
#
# Bug (2026-06-02): find_smart_refresh_candidates Rule A flags every
# no-history ticker, including ~2.5k that all three sources have
# permanently blacklisted as uncoverable (warrants, preferreds, foreign
# OTC, SPACs). They re-trip Rule A on every launch — inflating the flagged
# count ~20× and false-firing the bulk-run prompt — even though every
# per-source worker skips them. _kick_off_smart_refresh now trims the
# 3-way blacklist intersection before the count / prompt.
# ──────────────────────────────────────────────────────────────────────

def test_kickoff_trims_candidates_blocked_by_all_three_sources(manual):
    """A ticker in all three per-source blacklists can never be refreshed
    and is dropped. A ticker blocked by only *some* sources survives —
    another source can still land it."""
    w, mw, eh, launches, monkeypatch = manual
    monkeypatch.setattr(
        eh, "find_smart_refresh_candidates",
        lambda syms, skip, **k: ["LIVE1", "LIVE2", "LIVE3", "DEAD1", "DEAD2"],
    )
    # DEAD1/DEAD2 are in all three; LIVE3 is in two (zacks can still try it).
    w._finnhub_blacklist = {"DEAD1", "DEAD2", "LIVE3"}
    w._finviz_blacklist = {"DEAD1", "DEAD2", "LIVE3"}
    w._zacks_blacklist = {"DEAD1", "DEAD2"}

    w._kick_off_smart_refresh()

    assert len(launches) == 1
    cands, due, inc = launches[0]
    assert cands == ["LIVE1", "LIVE2", "LIVE3"]  # DEAD1/DEAD2 trimmed
    assert due is True
    assert inc is False  # auto cycle excludes finnhub (manual-only)


def test_kickoff_trim_prevents_false_bulk_prompt(manual):
    """The bulk-run warning gates on the *trimmed* count: a pile of
    all-source-blocked candidates must not trip it when the genuinely
    refreshable remainder is below the threshold."""
    import trade_scanner_fh.config as config
    w, mw, eh, launches, monkeypatch = manual
    live = [f"LIVE{i}" for i in range(50)]
    dead = [f"DEAD{i}" for i in range(config.ZACKS_SMART_REFRESH_BULK_THRESHOLD + 500)]
    monkeypatch.setattr(
        eh, "find_smart_refresh_candidates",
        lambda syms, skip, **k: live + dead,
    )
    w._finnhub_blacklist = set(dead)
    w._finviz_blacklist = set(dead)
    w._zacks_blacklist = set(dead)

    # The prompt must never be reached — fail loudly if it is.
    def _boom(*a, **k):
        raise AssertionError("bulk-run prompt fired on trimmed candidates")
    monkeypatch.setattr(mw.QMessageBox, "question", _boom)

    w._kick_off_smart_refresh()

    assert len(launches) == 1
    cands, due, inc = launches[0]
    assert sorted(cands) == sorted(live)  # all dead trimmed, no prompt
    assert due is True


def test_kickoff_no_trim_when_blacklists_disjoint(manual):
    """When no candidate is blocked by all three sources, nothing is
    trimmed — the full candidate list flows through unchanged."""
    w, mw, eh, launches, monkeypatch = manual
    monkeypatch.setattr(
        eh, "find_smart_refresh_candidates",
        lambda syms, skip, **k: ["AAA", "BBB", "CCC"],
    )
    w._finnhub_blacklist = {"AAA"}
    w._finviz_blacklist = {"BBB"}
    w._zacks_blacklist = {"CCC"}  # intersection empty → no trim

    w._kick_off_smart_refresh()

    assert len(launches) == 1
    cands, due, inc = launches[0]
    assert cands == ["AAA", "BBB", "CCC"]
