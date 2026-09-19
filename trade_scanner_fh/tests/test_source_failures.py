"""Per-source fill-failure reports with selective skip-listing (v6.3.2).

All three earnings sources now expose the same report. The classification in
`failure_kinds` decides what the dialog pre-selects, and the safety it encodes
is the point: the parse-spike alarm halts a run and blacklists NOTHING when a
page format breaks, so a UI that let you bulk-add `parse_error` by reflex
would quietly undo that guarantee.
"""
from __future__ import annotations

import pandas as pd
import pytest
from PyQt6.QtWidgets import QDialog, QMessageBox

from trade_scanner_fh import config
from trade_scanner_fh import earnings_history as eh
from trade_scanner_fh import failure_kinds as fk


# ── the taxonomy ───────────────────────────────────────────────────────

@pytest.mark.parametrize("kind, group", [
    ("not_found", fk.PERMANENT), ("empty", fk.PERMANENT),
    ("forbidden", fk.PERMANENT), ("rejected_symbol", fk.PERMANENT),
    ("http_4xx", fk.PERMANENT),
    ("network", fk.TRANSIENT), ("server_error", fk.TRANSIENT),
    ("rate_limited", fk.TRANSIENT), ("http_5xx", fk.TRANSIENT),
    ("http_429", fk.TRANSIENT),
    ("parse_error", fk.UPSTREAM), ("blocked", fk.UPSTREAM),
    ("oversized", fk.UPSTREAM), ("too_large", fk.UPSTREAM),
    ("auth", fk.NEVER_SKIP),
])
def test_every_known_kind_classifies(kind, group):
    assert fk.classify(kind) == group


def test_unknown_kinds_are_treated_as_upstream():
    """The conservative default. A kind we cannot reason about must not be
    offered as a safe skip."""
    assert fk.classify("something_new") == fk.UPSTREAM
    assert fk.classify("") == fk.UPSTREAM
    assert fk.classify(None) == fk.UPSTREAM
    assert fk.default_checked("something_new") is False


def test_only_permanent_is_checked_by_default():
    for k in ("not_found", "empty", "forbidden"):
        assert fk.default_checked(k) is True, k
    for k in ("network", "rate_limited", "parse_error", "blocked", "auth"):
        assert fk.default_checked(k) is False, k


def test_auth_can_never_be_skip_listed():
    """A revoked API key fails every symbol equally — skip-listing on it
    would empty the universe."""
    assert fk.is_skippable("auth") is False
    assert all(fk.is_skippable(k) for k in
               ("not_found", "network", "parse_error", "blocked"))


def test_every_kind_the_three_clients_emit_is_classified():
    """Drift guard: a new FAIL_* sentinel must not silently land in UPSTREAM
    by accident — this test fails loudly so the taxonomy gets updated."""
    from trade_scanner_fh import finviz_client, finnhub_client, zacks_scraper
    known = (set(fk._PERMANENT) | set(fk._TRANSIENT)
             | set(fk._UPSTREAM) | set(fk._NEVER_SKIP) | {"http_error"})
    emitted = set()
    for mod in (finviz_client, finnhub_client, zacks_scraper):
        for name in dir(mod):
            if name.startswith("FAIL_"):
                emitted.add(getattr(mod, name))
    missing = emitted - known
    assert not missing, f"unclassified failure kinds: {sorted(missing)}"


# ── persistence ────────────────────────────────────────────────────────

def test_report_round_trips(tmp_parquets):
    n = eh.write_source_failures(
        "finviz",
        {"empty": ["AAA", "BBB"], "network": ["CCC"]},
        {"CCC": "ReadTimeout"},
    )
    assert n == 3
    by_kind, details, run_at = eh.load_source_failures("finviz")
    assert sorted(by_kind["empty"]) == ["AAA", "BBB"]
    assert by_kind["network"] == ["CCC"]
    assert details["CCC"] == "ReadTimeout"
    assert run_at


def test_report_carries_the_group_so_the_csv_is_greppable(tmp_parquets):
    eh.write_source_failures("zacks", {"not_found": ["AAA"],
                                       "parse_error": ["BBB"]})
    df = pd.read_csv(eh.source_failure_csv_path("zacks"))
    assert list(df.columns) == eh.SOURCE_FAILURE_COLUMNS
    assert set(df.group) == {"permanent", "upstream"}


def test_a_clean_run_clears_the_file(tmp_parquets):
    """Unlike the disagreement report, a failure report describes ONE run —
    so an empty run means 'nothing failed', not 'keep the old rows'."""
    eh.write_source_failures("finnhub", {"empty": ["AAA"]})
    assert len(pd.read_csv(eh.source_failure_csv_path("finnhub"))) == 1
    eh.write_source_failures("finnhub", {})
    assert pd.read_csv(eh.source_failure_csv_path("finnhub")).empty


def test_each_source_gets_its_own_file(tmp_parquets):
    eh.write_source_failures("zacks", {"not_found": ["ZZZ"]})
    eh.write_source_failures("finviz", {"empty": ["FFF"]})
    assert eh.load_source_failures("zacks")[0] == {"not_found": ["ZZZ"]}
    assert eh.load_source_failures("finviz")[0] == {"empty": ["FFF"]}


def test_missing_and_corrupt_files_degrade_to_empty(tmp_parquets):
    assert eh.load_source_failures("finviz") == ({}, {}, "")
    eh.source_failure_csv_path("finviz").write_text("not,a,valid\nreport",
                                                    encoding="utf-8")
    by_kind, details, _ = eh.load_source_failures("finviz")
    assert by_kind == {} and details == {}


def test_a_write_failure_never_raises(tmp_parquets, monkeypatch):
    """Diagnostics must never be what kills a fill."""
    monkeypatch.setattr(config, "atomic_write_csv",
                        lambda *a, **k: (_ for _ in ()).throw(OSError("full")))
    assert eh.write_source_failures("zacks", {"not_found": ["AAA"]}) == 0


# ── the dialog ─────────────────────────────────────────────────────────

BREAKDOWN = {
    "not_found": ["AAA", "BBB"],      # permanent
    "network": ["CCC"],               # transient
    "parse_error": ["DDD", "EEE"],    # upstream
    "auth": ["FFF"],                  # never skippable
}


@pytest.fixture
def dlg(_qapp):
    from trade_scanner_fh.gui.dialogs import SourceFailureDialog
    return SourceFailureDialog("zacks", BREAKDOWN, {"CCC": "ReadTimeout"})


def test_dialog_preselects_only_permanent_kinds(dlg):
    assert dlg.selected_kinds() == ["not_found"]
    assert dlg.selected_tickers() == ["AAA", "BBB"]


def test_auth_is_present_but_disabled(dlg):
    cb = dlg.kind_checks["auth"]
    assert not cb.isEnabled() and not cb.isChecked()
    dlg.kind_checks["auth"].setChecked(True)     # cannot be forced in
    assert "auth" not in dlg.selected_kinds()


def test_ticking_more_kinds_widens_the_selection(dlg):
    dlg.kind_checks["network"].setChecked(True)
    dlg.kind_checks["parse_error"].setChecked(True)
    assert dlg.selected_tickers() == ["AAA", "BBB", "CCC", "DDD", "EEE"]


def test_tickers_already_on_the_list_are_excluded(_qapp):
    from trade_scanner_fh.gui.dialogs import SourceFailureDialog
    d = SourceFailureDialog("zacks", BREAKDOWN, {}, already_skipped={"AAA"})
    assert d.selected_tickers() == ["BBB"]
    assert d.selected_tickers(exclude_existing=False) == ["AAA", "BBB"]


def test_add_button_disabled_when_nothing_new_is_selected(_qapp):
    from trade_scanner_fh.gui.dialogs import SourceFailureDialog
    d = SourceFailureDialog("zacks", {"not_found": ["AAA"]}, {},
                            already_skipped={"AAA"})
    assert not d.btn_add.isEnabled()


def test_upstream_selection_requires_a_second_confirmation(dlg, monkeypatch):
    """The guard that keeps a page-format break from poisoning the list."""
    monkeypatch.setattr(QMessageBox, "question",
                        lambda *a, **k: QMessageBox.StandardButton.Yes)
    warned = []

    def _warn(*a, **k):
        warned.append(a)
        return QMessageBox.StandardButton.No      # user backs out
    monkeypatch.setattr(QMessageBox, "warning", _warn)

    dlg.kind_checks["parse_error"].setChecked(True)
    dlg._on_add()
    assert warned, "upstream kinds must trigger the extra warning"
    assert dlg.accepted_tickers() == [], "backing out must add nothing"


def test_permanent_only_selection_needs_no_second_confirmation(dlg, monkeypatch):
    monkeypatch.setattr(QMessageBox, "question",
                        lambda *a, **k: QMessageBox.StandardButton.Yes)
    warned = []
    monkeypatch.setattr(QMessageBox, "warning",
                        lambda *a, **k: warned.append(a))
    dlg._on_add()
    assert not warned
    assert dlg.accepted_tickers() == ["AAA", "BBB"]


def test_declining_the_first_confirm_adds_nothing(dlg, monkeypatch):
    monkeypatch.setattr(QMessageBox, "question",
                        lambda *a, **k: QMessageBox.StandardButton.No)
    dlg._on_add()
    assert dlg.accepted_tickers() == []


# ── MainWindow wiring ──────────────────────────────────────────────────

@pytest.fixture
def mw(_qapp, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    from trade_scanner_fh.gui.main_window import MainWindow
    w = MainWindow.__new__(MainWindow)
    w._FINVIZ_BLACKLIST_FILE = tmp_path / "finviz_blacklist.txt"
    w._FINNHUB_BLACKLIST_FILE = tmp_path / "finnhub_blacklist.txt"
    w._ZACKS_BLACKLIST_FILE = tmp_path / "zacks_blacklist.txt"
    w._zacks_blacklist, w._finviz_blacklist, w._finnhub_blacklist = set(), set(), set()
    w.log_panel = type("P", (), {"write_line": lambda self, s: None})()
    w._log_error = lambda *a, **k: None
    # MainWindow.__init__ defines these; a `__new__`-built QObject has no
    # __dict__ entry for them, and PyQt raises on getattr-with-default rather
    # than returning the default. Sibling GUI tests seed attributes the same
    # way.
    for _s in ("zacks", "finviz", "finnhub"):
        setattr(w, f"_last_{_s}_failures", {})
        setattr(w, f"_last_{_s}_failure_details", {})
    return w


def test_stash_persists_and_remembers(mw, tmp_path):
    mw._stash_source_failures("finviz", {"empty": ["AAA"]})
    assert mw._last_finviz_failures == {"empty": ["AAA"]}
    assert eh.load_source_failures("finviz")[0] == {"empty": ["AAA"]}


def test_add_to_skip_list_persists_and_dedupes(mw, monkeypatch):
    from trade_scanner_fh.gui.blacklists import BlacklistManager
    monkeypatch.setattr(QMessageBox, "information", lambda *a, **k: None)
    mw._add_to_source_skip_list("finviz", ["aaa", "BBB", "AAA"])
    assert mw._finviz_blacklist == {"AAA", "BBB"}
    assert BlacklistManager(mw._FINVIZ_BLACKLIST_FILE).load() == {"AAA", "BBB"}


def test_add_rolls_back_when_the_write_fails(mw, monkeypatch):
    monkeypatch.setattr(QMessageBox, "warning", lambda *a, **k: None)
    monkeypatch.setattr(
        type(mw), "_save_finnhub_blacklist",
        lambda self: (_ for _ in ()).throw(OSError("disk full")))
    mw._add_to_source_skip_list("finnhub", ["AAA"])
    assert mw._finnhub_blacklist == set(), "memory and disk must not diverge"


def test_dialog_falls_back_to_the_saved_report(mw, monkeypatch):
    """The reason the CSV exists: an overnight fill, reviewed next morning in
    a fresh process that never ran the source."""
    eh.write_source_failures("finnhub", {"empty": ["AAA"]})
    seen = {}
    from trade_scanner_fh.gui import dialogs

    class _Stub:
        def __init__(self, source, by_kind, details, **kw):
            seen["by_kind"] = by_kind
            seen["run_at"] = kw.get("run_at")
        def exec(self):
            return QDialog.DialogCode.Rejected
    monkeypatch.setattr(dialogs, "SourceFailureDialog", _Stub)

    mw._show_source_failures("finnhub")
    assert seen["by_kind"] == {"empty": ["AAA"]}
    assert seen["run_at"], "the saved report's timestamp should be shown"


def test_no_report_anywhere_shows_a_message_not_a_crash(mw, monkeypatch):
    msgs = []
    monkeypatch.setattr(QMessageBox, "information",
                        lambda *a, **k: msgs.append(a))
    mw._show_source_failures("finviz")
    assert msgs


def test_all_three_sources_are_wired(mw):
    from trade_scanner_fh.gui.main_window import MainWindow
    assert set(MainWindow._SOURCE_SKIP_ATTRS) == {"zacks", "finviz", "finnhub"}
    for src, (set_attr, saver) in MainWindow._SOURCE_SKIP_ATTRS.items():
        assert hasattr(mw, set_attr), src
        assert hasattr(MainWindow, saver), src


def test_finviz_and_finnhub_workers_expose_the_breakdown_signal(_qapp):
    """The plumbing that was missing: fill_framework always called
    `failed_cb`, but these two workers never passed one."""
    from trade_scanner_fh.gui.workers import FinvizFillWorker, FinnhubFillWorker
    for cls in (FinvizFillWorker, FinnhubFillWorker):
        w = cls(["AAA"], set(), mode="gap")
        assert hasattr(w, "failure_breakdown"), cls.__name__
        assert w._failures_by_kind == {}
        w._failures_by_kind.setdefault("empty", []).append("AAA")
        assert w._failures_by_kind == {"empty": ["AAA"]}


def test_a_numeric_looking_detail_survives_the_round_trip(tmp_parquets):
    """Caught by the smoketest: "404" is inferred as int64 on read-back and
    was silently dropped. Every column in this report is text."""
    eh.write_source_failures("zacks", {"http_4xx": ["AAA"]}, {"AAA": "404"})
    _by_kind, details, _ = eh.load_source_failures("zacks")
    assert details["AAA"] == "404"


def test_a_leading_zero_ticker_is_not_mangled(tmp_parquets):
    eh.write_source_failures("finviz", {"empty": ["0700"]})
    by_kind, _d, _r = eh.load_source_failures("finviz")
    assert by_kind["empty"] == ["0700"]
