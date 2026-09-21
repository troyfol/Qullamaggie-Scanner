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
    # v7.0.0: the finviz ATTRIBUTE skip list is a fourth recheckable source.
    # The shell has to declare it or `_stale_skip_candidates` raises when it
    # walks `_RECHECKABLE_SKIP_LISTS` — on a bypass-init MainWindow, Qt turns
    # a missing attribute into RuntimeError rather than AttributeError.
    w._FINVIZ_SNAPSHOT_BLACKLIST_FILE = (
        tmp_path / "finviz_snapshot_blacklist.txt")
    w._finviz_snapshot_blacklist = set()
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


def _stub_dialog(monkeypatch, *, accept=True, selection=None, settings=None):
    """Replace StaleSkipDialog with a stub. The dialog's own selection logic
    is covered directly further down; these tests drive the APPLY path."""
    from PyQt6.QtWidgets import QDialog
    from trade_scanner_fh.gui import dialogs

    captured = {}

    class _Stub:
        def __init__(self, entries, live, **kw):
            captured["entries"] = entries
            captured["live"] = live
            captured["kwargs"] = kw

        def exec(self):
            return (QDialog.DialogCode.Accepted if accept
                    else QDialog.DialogCode.Rejected)

        def selection(self):
            return dict(selection or {})

        def settings(self):
            return dict(settings or {
                "days": 90, "max_per_list": 500,
                "reasons": ["empty"], "include_undated": False,
                "sources": ["finviz", "finnhub", "zacks"],
            })

    monkeypatch.setattr(dialogs, "StaleSkipDialog", _Stub)
    return captured


def test_recheck_removes_entries_and_persists(mw, monkeypatch):
    _seed(mw._FINVIZ_BLACKLIST_FILE, {
        "AGED": (OLD, "empty"), "KEEP": (OLD, "manual"),
    })
    mw._finviz_blacklist = {"AGED", "KEEP"}
    mw.log_panel = type("P", (), {"write_line": lambda self, s: None})()

    from PyQt6.QtWidgets import QMessageBox
    monkeypatch.setattr(QMessageBox, "information", lambda *a, **k: None)
    _stub_dialog(monkeypatch, selection={"finviz": ["AGED"]})

    mw._recheck_stale_skips()

    assert mw._finviz_blacklist == {"KEEP"}
    assert BlacklistManager(mw._FINVIZ_BLACKLIST_FILE).load() == {"KEEP"}


def test_recheck_declined_changes_nothing(mw, monkeypatch):
    _seed(mw._FINVIZ_BLACKLIST_FILE, {"AGED": (OLD, "empty")})
    mw._finviz_blacklist = {"AGED"}

    from PyQt6.QtWidgets import QMessageBox
    monkeypatch.setattr(QMessageBox, "information", lambda *a, **k: None)
    _stub_dialog(monkeypatch, accept=False, selection={"finviz": ["AGED"]})

    mw._recheck_stale_skips()

    assert mw._finviz_blacklist == {"AGED"}
    assert BlacklistManager(mw._FINVIZ_BLACKLIST_FILE).load() == {"AGED"}


def test_recheck_with_empty_lists_never_opens_the_dialog(mw, monkeypatch):
    """Nothing on any list — the dialog would have nothing to show."""
    from PyQt6.QtWidgets import QMessageBox
    monkeypatch.setattr(QMessageBox, "information", lambda *a, **k: None)
    captured = _stub_dialog(monkeypatch)

    mw._recheck_stale_skips()

    assert "entries" not in captured


def test_recheck_preserves_untouched_entries_metadata(mw, monkeypatch):
    """Re-check must not reset the ADDED_ON of the entries it leaves behind —
    otherwise one click would push every remaining entry's re-check horizon
    out by another staleness window."""
    _seed(mw._FINVIZ_BLACKLIST_FILE, {
        "AGED": (OLD, "empty"), "KEEPOLD": (OLD, "manual"),
    })
    mw._finviz_blacklist = {"AGED", "KEEPOLD"}
    mw.log_panel = type("P", (), {"write_line": lambda self, s: None})()

    from PyQt6.QtWidgets import QMessageBox
    monkeypatch.setattr(QMessageBox, "information", lambda *a, **k: None)
    _stub_dialog(monkeypatch, selection={"finviz": ["AGED"]})

    mw._recheck_stale_skips()

    entries = BlacklistManager(mw._FINVIZ_BLACKLIST_FILE).load_entries()
    assert entries["KEEPOLD"] == (OLD, "manual")


def test_failed_save_rolls_back_the_in_memory_set(mw, monkeypatch):
    """Memory and disk must never disagree: if the write fails the ticker
    stays skipped, otherwise the next fill would try a ticker the file still
    excludes and re-add it on every run."""
    _seed(mw._FINVIZ_BLACKLIST_FILE, {"AGED": (OLD, "empty")})
    mw._finviz_blacklist = {"AGED"}
    mw.log_panel = type("P", (), {"write_line": lambda self, s: None})()
    errors = []
    mw._log_error = lambda *a, **k: errors.append(a)

    from PyQt6.QtWidgets import QMessageBox
    monkeypatch.setattr(QMessageBox, "information", lambda *a, **k: None)
    monkeypatch.setattr(
        type(mw), "_save_finviz_blacklist",
        lambda self: (_ for _ in ()).throw(OSError("disk full")),
    )
    _stub_dialog(monkeypatch, selection={"finviz": ["AGED"]})

    mw._recheck_stale_skips()

    assert mw._finviz_blacklist == {"AGED"}, "rollback did not happen"
    assert errors


# ── the configurable filter itself ─────────────────────────────────────

def test_candidates_accept_an_explicit_staleness_window(mw):
    _seed(mw._FINVIZ_BLACKLIST_FILE, {
        "OLD200": (date.today() - timedelta(days=200), "empty"),
        "OLD30": (date.today() - timedelta(days=30), "empty"),
    })
    mw._finviz_blacklist = {"OLD200", "OLD30"}

    assert mw._stale_skip_candidates(days=90)["finviz"] == ["OLD200"]
    assert sorted(mw._stale_skip_candidates(days=10)["finviz"]) == [
        "OLD200", "OLD30"]
    assert sorted(mw._stale_skip_candidates(days=0)["finviz"]) == [
        "OLD200", "OLD30"]


def test_candidates_accept_explicit_reason_codes(mw):
    """The 10,143-entry zacks list was 100% `manual` and therefore invisible
    to the fixed `empty`-only rule."""
    _seed(mw._ZACKS_BLACKLIST_FILE, {
        "MAN1": (OLD, "manual"), "MAN2": (OLD, "manual"),
        "EMP1": (OLD, "empty"),
    })
    mw._zacks_blacklist = {"MAN1", "MAN2", "EMP1"}

    assert mw._stale_skip_candidates()["zacks"] == ["EMP1"]
    assert sorted(
        mw._stale_skip_candidates(reasons={"manual"})["zacks"]
    ) == ["MAN1", "MAN2"]
    assert len(
        mw._stale_skip_candidates(reasons={"manual", "empty"})["zacks"]
    ) == 3


def test_candidates_accept_a_source_filter(mw):
    _seed(mw._FINVIZ_BLACKLIST_FILE, {"FV": (OLD, "empty")})
    _seed(mw._ZACKS_BLACKLIST_FILE, {"ZK": (OLD, "empty")})
    mw._finviz_blacklist = {"FV"}
    mw._zacks_blacklist = {"ZK"}

    got = mw._stale_skip_candidates(sources=("zacks",))
    assert got["zacks"] == ["ZK"]
    assert got["finviz"] == []


def test_undated_legacy_entries_are_opt_in(mw):
    # Written raw: `save_entries` stamps today onto a dateless entry, so a
    # genuine legacy row (bare ticker, no tabs) can only come from the file.
    mw._FINVIZ_BLACKLIST_FILE.write_text(
        "# header\nLEGACY\n", encoding="utf-8")
    mw._finviz_blacklist = {"LEGACY"}

    assert mw._stale_skip_candidates(reasons={"(none)"})["finviz"] == []
    assert mw._stale_skip_candidates(
        reasons={"(none)"}, include_undated=True,
    )["finviz"] == ["LEGACY"]


def test_entry_not_on_the_live_set_is_never_a_candidate(mw):
    """A file row for a ticker already re-enabled is bookkeeping, not work."""
    _seed(mw._FINVIZ_BLACKLIST_FILE, {"GONE": (OLD, "empty")})
    mw._finviz_blacklist = set()
    assert mw._stale_skip_candidates()["finviz"] == []


# ── StaleSkipDialog.selection(), driven directly ───────────────────────

@pytest.fixture
def dlg_factory(_qapp):
    from trade_scanner_fh.gui.dialogs import StaleSkipDialog

    def make(entries, live=None, **kw):
        live = live if live is not None else {
            k: set(v) for k, v in entries.items()}
        return StaleSkipDialog(
            entries, live,
            labels={k: k.title() for k in entries},
            default_days=kw.pop("default_days", 90),
            default_max=kw.pop("default_max", 500),
        )
    return make


def test_dialog_defaults_to_empty_reason_only(dlg_factory):
    d = dlg_factory({"zacks": {
        "MAN": (OLD, "manual"), "EMP": (OLD, "empty"),
    }})
    assert d.selection()["zacks"] == ["EMP"]
    assert d.settings()["reasons"] == ["empty"]


def test_dialog_ticking_a_reason_widens_the_selection(dlg_factory):
    d = dlg_factory({"zacks": {
        "MAN": (OLD, "manual"), "EMP": (OLD, "empty"),
    }})
    d.reason_checks["manual"].setChecked(True)
    assert sorted(d.selection()["zacks"]) == ["EMP", "MAN"]


def test_dialog_unticking_a_source_zeroes_it(dlg_factory):
    d = dlg_factory({
        "finviz": {"FV": (OLD, "empty")},
        "zacks": {"ZK": (OLD, "empty")},
    })
    d.src_checks["finviz"].setChecked(False)
    sel = d.selection()
    assert sel["finviz"] == [] and sel["zacks"] == ["ZK"]


def test_dialog_age_spin_controls_eligibility(dlg_factory):
    d = dlg_factory({"finviz": {
        "OLD200": (date.today() - timedelta(days=200), "empty"),
        "OLD30": (date.today() - timedelta(days=30), "empty"),
    }})
    d.sp_days.setValue(90)
    assert d.selection()["finviz"] == ["OLD200"]
    d.sp_days.setValue(10)
    assert sorted(d.selection()["finviz"]) == ["OLD200", "OLD30"]


def test_dialog_cap_is_per_list_and_oldest_first(dlg_factory):
    entries = {f"T{i}": (date.today() - timedelta(days=100 + i), "empty")
               for i in range(5)}
    d = dlg_factory({"finviz": entries, "zacks": dict(entries)})
    d.sp_max.setValue(2)
    sel = d.selection()
    assert sel["finviz"] == ["T4", "T3"]      # oldest first
    assert len(sel["zacks"]) == 2             # capped per list, not globally


def test_dialog_undated_entries_sort_last_so_the_cap_prefers_dated(dlg_factory):
    d = dlg_factory({"finviz": {
        "DATED": (OLD, "empty"),
        "LEGACY": (None, "empty"),
    }})
    d.cb_undated.setChecked(True)
    d.sp_max.setValue(1)
    assert d.selection()["finviz"] == ["DATED"]


def test_dialog_ok_is_disabled_when_nothing_matches(dlg_factory):
    from PyQt6.QtWidgets import QDialogButtonBox
    d = dlg_factory({"finviz": {"FRESH": (RECENT, "empty")}})
    ok = d.buttons.button(QDialogButtonBox.StandardButton.Ok)
    assert not ok.isEnabled()
    d.sp_days.setValue(0)
    assert ok.isEnabled()


def test_dialog_reports_its_settings(dlg_factory):
    d = dlg_factory({"finviz": {"A": (OLD, "empty")}})
    d.sp_days.setValue(45)
    d.sp_max.setValue(7)
    s = d.settings()
    assert s["days"] == 45 and s["max_per_list"] == 7
    assert s["sources"] == ["finviz"] and s["reasons"] == ["empty"]


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
