"""Tests for skip-list re-validation — audit 2026-08-12 (INT-5).

The per-source skip lists were a one-way ratchet: a single "empty" response
(finviz 404, finnhub `[]`) excluded a ticker permanently, and the bare-ticker
file format carried no date, so a re-check rule was impossible to write. The
live lists had reached 10,246 / 10,048 / 6,394 entries against a 15,948-symbol
universe and could only grow.

Entries now carry ADDED_ON + REASON, and `_recheck_stale_skips` re-offers the
aged `empty` ones to their source — bounded and user-triggered, never automatic
(a full sweep of 10k finnhub skips would be ~11 hours of unrequested traffic).

Bound via ``__new__`` like the sibling GUI tests, so MainWindow.__init__ never
runs its network workers.
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from trade_scanner_fh import config
from trade_scanner_fh.gui.blacklists import BlacklistManager


@pytest.fixture
def mw(_qapp, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "SKIP_RECHECK_DAYS", 90)
    monkeypatch.setattr(config, "SKIP_RECHECK_MAX", 500)
    from trade_scanner_fh.gui.main_window import MainWindow
    w = MainWindow.__new__(MainWindow)
    w._FINNHUB_BLACKLIST_FILE = tmp_path / "finnhub_blacklist.txt"
    w._FINVIZ_BLACKLIST_FILE = tmp_path / "finviz_blacklist.txt"
    w._ZACKS_BLACKLIST_FILE = tmp_path / "zacks_blacklist.txt"
    w._finnhub_blacklist = set()
    w._finviz_blacklist = set()
    w._zacks_blacklist = set()
    return w


def _seed(path, entries):
    BlacklistManager(path).save_entries(entries)


OLD = date.today() - timedelta(days=200)
RECENT = date.today() - timedelta(days=10)


def test_only_aged_empty_entries_are_candidates(mw):
    _seed(mw._FINVIZ_BLACKLIST_FILE, {
        "AGEDEMPTY": (OLD, "empty"),        # eligible
        "FRESHEMPTY": (RECENT, "empty"),    # too recent
        "AGEDMANUAL": (OLD, "manual"),      # user-curated — never re-checked
        "LEGACY": (None, ""),               # no metadata — excluded by design
    })
    mw._finviz_blacklist = {"AGEDEMPTY", "FRESHEMPTY", "AGEDMANUAL", "LEGACY"}

    assert mw._stale_skip_candidates()["finviz"] == ["AGEDEMPTY"]


def test_candidates_are_capped_oldest_first(mw, monkeypatch):
    monkeypatch.setattr(config, "SKIP_RECHECK_MAX", 2)
    entries = {
        f"T{i}": (date.today() - timedelta(days=100 + i), "empty")
        for i in range(5)
    }
    _seed(mw._FINVIZ_BLACKLIST_FILE, entries)
    mw._finviz_blacklist = set(entries)

    # T4 is the oldest (100+4 days), T3 next.
    assert mw._stale_skip_candidates()["finviz"] == ["T4", "T3"]


def test_recheck_removes_entries_and_persists(mw, monkeypatch):
    _seed(mw._FINVIZ_BLACKLIST_FILE, {
        "AGED": (OLD, "empty"), "KEEP": (OLD, "manual"),
    })
    mw._finviz_blacklist = {"AGED", "KEEP"}
    mw.log_panel = type("P", (), {"write_line": lambda self, s: None})()

    from PyQt6.QtWidgets import QMessageBox
    monkeypatch.setattr(QMessageBox, "question",
                        lambda *a, **k: QMessageBox.StandardButton.Yes)
    monkeypatch.setattr(QMessageBox, "information", lambda *a, **k: None)

    mw._recheck_stale_skips()

    assert mw._finviz_blacklist == {"KEEP"}
    on_disk = BlacklistManager(mw._FINVIZ_BLACKLIST_FILE).load()
    assert on_disk == {"KEEP"}


def test_recheck_declined_changes_nothing(mw, monkeypatch):
    _seed(mw._FINVIZ_BLACKLIST_FILE, {"AGED": (OLD, "empty")})
    mw._finviz_blacklist = {"AGED"}

    from PyQt6.QtWidgets import QMessageBox
    monkeypatch.setattr(QMessageBox, "question",
                        lambda *a, **k: QMessageBox.StandardButton.No)
    monkeypatch.setattr(QMessageBox, "information", lambda *a, **k: None)

    mw._recheck_stale_skips()

    assert mw._finviz_blacklist == {"AGED"}
    assert BlacklistManager(mw._FINVIZ_BLACKLIST_FILE).load() == {"AGED"}


def test_recheck_with_no_candidates_is_a_noop(mw, monkeypatch):
    _seed(mw._FINVIZ_BLACKLIST_FILE, {"FRESH": (RECENT, "empty")})
    mw._finviz_blacklist = {"FRESH"}

    from PyQt6.QtWidgets import QMessageBox
    asked = []
    monkeypatch.setattr(QMessageBox, "question",
                        lambda *a, **k: asked.append(1))
    monkeypatch.setattr(QMessageBox, "information", lambda *a, **k: None)

    mw._recheck_stale_skips()

    assert asked == []          # never prompts when there's nothing to do
    assert mw._finviz_blacklist == {"FRESH"}


def test_recheck_preserves_untouched_entries_metadata(mw, monkeypatch):
    """Re-check must not reset the ADDED_ON of the entries it leaves behind —
    otherwise one click would push every remaining entry's re-check horizon
    out by another SKIP_RECHECK_DAYS."""
    _seed(mw._FINVIZ_BLACKLIST_FILE, {
        "AGED": (OLD, "empty"), "KEEPOLD": (OLD, "manual"),
    })
    mw._finviz_blacklist = {"AGED", "KEEPOLD"}
    mw.log_panel = type("P", (), {"write_line": lambda self, s: None})()

    from PyQt6.QtWidgets import QMessageBox
    monkeypatch.setattr(QMessageBox, "question",
                        lambda *a, **k: QMessageBox.StandardButton.Yes)
    monkeypatch.setattr(QMessageBox, "information", lambda *a, **k: None)

    mw._recheck_stale_skips()

    entries = BlacklistManager(mw._FINVIZ_BLACKLIST_FILE).load_entries()
    assert entries["KEEPOLD"] == (OLD, "manual")


def test_recheck_is_bounded_per_list_not_global(mw, monkeypatch):
    """The cap applies per source list, so one enormous list can't starve
    the others out of the batch."""
    monkeypatch.setattr(config, "SKIP_RECHECK_MAX", 1)
    _seed(mw._FINVIZ_BLACKLIST_FILE, {"FV1": (OLD, "empty"),
                                      "FV2": (OLD, "empty")})
    _seed(mw._FINNHUB_BLACKLIST_FILE, {"FH1": (OLD, "empty")})
    mw._finviz_blacklist = {"FV1", "FV2"}
    mw._finnhub_blacklist = {"FH1"}

    cands = mw._stale_skip_candidates()
    assert len(cands["finviz"]) == 1
    assert cands["finnhub"] == ["FH1"]
