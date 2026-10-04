"""v8.0.2 — every ticker box accepts a TradeStation column and finviz tags.

User decisions (2026-10-02) these tests pin:
  * every dialog that takes a ticker LIST — Lookup, Manual Input STW, Rebuild
    Tickers, the OHLCV Blacklist and Greylist editors, the Advanced Settings
    reference tickers and the four skip-list editors — accepts commas,
    semicolons, newlines (a TradeStation column), tabs and spaces, and drops
    parenthesised finviz qualifiers such as (HB) = hard to borrow;
  * the single-ticker Spot Fill prompts stay single-ticker but drop the tag,
    so a pasted ``AGL(HB)`` fills AGL.

Every list dialog is driven end to end through `_ScriptedDialog`, a stand-in
for main_window's QDialog that types into the dialog's text box instead of
showing it — the real handler code runs, including what it saves.
"""
from __future__ import annotations

import types

import pytest

from trade_scanner_fh import config
from trade_scanner_fh.gui import lookup as L
from trade_scanner_fh.gui import main_window as mw_mod
from trade_scanner_fh.gui.ticker_input import (
    FORMAT_NOTE, INPUT_PROMPT, parse_ticker_list, strip_qualifiers,
)

# The list the user pasted when asking for this (2026-10-02), verbatim.
PASTE = """MAN
AGL(HB)
APPS
PTRN(HB)
VPG(HB)
AMC(HB)
MEDP(HB)
NVEC(HB)
RELL(HB)
CMCO(HB)
FORM(HB)
COHU(HB)
NWL
FET
AXTI
GTE(HB)
BLMN
KTOS(HB)
FTK(HB)
SITM
INSM
SOUN(HB)
ASPN
HNST
NNBR
ORA(HB)
HTZ
PUBM
TEAM
FIGS
CRSR
MTW
INOD(HB)
WLDN(HB)
OMDA
FIVN(HB)
PSIX(HB)
NUTX(HB)
CLMT
BW
STIM(HB)
QMCO(HB)
HYLN
VELO(HB)
EROC"""

EXPECTED = [
    "MAN", "AGL", "APPS", "PTRN", "VPG", "AMC", "MEDP", "NVEC", "RELL",
    "CMCO", "FORM", "COHU", "NWL", "FET", "AXTI", "GTE", "BLMN", "KTOS",
    "FTK", "SITM", "INSM", "SOUN", "ASPN", "HNST", "NNBR", "ORA", "HTZ",
    "PUBM", "TEAM", "FIGS", "CRSR", "MTW", "INOD", "WLDN", "OMDA", "FIVN",
    "PSIX", "NUTX", "CLMT", "BW", "STIM", "QMCO", "HYLN", "VELO", "EROC",
]

# Real universe spellings the parser must leave alone: `$` and `^` inside a
# symbol (preferreds, rights), dashed share classes.
LIVE_SPELLINGS = ["ABR$D", "AIIA^", "AXIA$C", "BRK-B", "AAC-WT", "AAPL"]


# ======================================================================
# The parser
# ======================================================================

def test_the_users_tradestation_paste_parses_to_45_clean_tickers():
    assert parse_ticker_list(PASTE) == EXPECTED
    assert len(EXPECTED) == 45


def test_windows_line_endings_and_tabs_parse_the_same():
    assert parse_ticker_list(PASTE.replace("\n", "\r\n")) == EXPECTED
    assert parse_ticker_list(PASTE.replace("\n", "\t")) == EXPECTED


@pytest.mark.parametrize("text, want", [
    ("AGL(HB)", ["AGL"]),
    ("AGL (HB)", ["AGL"]),                 # tag after a space
    ("(HB) AGL", ["AGL"]),                 # tag first
    ("AGL(HB)APPS", ["AGL", "APPS"]),      # a tag still separates
    ("AGL(HB", ["AGL"]),                   # truncated tag
    ("agl(hb), apps; nwl", ["AGL", "APPS", "NWL"]),
    ("AGL APPS", ["AGL", "APPS"]),    # non-breaking space
    ("(HB)", []),
    ("  ", []),
    ("", []),
])
def test_qualifiers_and_separators(text, want):
    assert parse_ticker_list(text) == want


def test_symbols_keep_inner_dollar_caret_dot_and_dash():
    text = ", ".join(LIVE_SPELLINGS) + ", $NVDA, BRK.B, BRK–A"
    assert parse_ticker_list(text) == LIVE_SPELLINGS + ["NVDA", "BRK.B",
                                                        "BRK-A"]


@pytest.mark.parametrize("join", [", ", "\n"])
def test_reparsing_a_saved_list_changes_nothing(join):
    """The editors prefill with the stored list; OK without edits must give
    back exactly what was stored."""
    assert parse_ticker_list(join.join(LIVE_SPELLINGS)) == LIVE_SPELLINGS


def test_duplicates_are_dropped_in_order():
    assert parse_ticker_list("AMC, amc(HB)\nAMC") == ["AMC"]


def test_strip_qualifiers_feeds_the_single_ticker_prompts():
    assert strip_qualifiers("AGL(HB)").strip() == "AGL"
    assert strip_qualifiers(None) == ""


# ======================================================================
# Harness
# ======================================================================

def _scripted_dialog(monkeypatch, text=None, click=None):
    """Replace main_window's QDialog: exec() types `text` into the dialog's
    text box (None leaves the prefill) and accepts — or clicks the button
    labelled `click`. Returns a dict holding the dialog's label texts."""
    from PyQt6.QtWidgets import QDialog, QLabel, QPushButton, QTextEdit
    seen: dict = {}

    class _ScriptedDialog(QDialog):
        def __init__(self, parent=None):
            super().__init__(None)

        def exec(self):
            seen["labels"] = [lb.text() for lb in self.findChildren(QLabel)]
            box = self.findChildren(QTextEdit)[0]
            seen["prefill"] = box.toPlainText()
            if text is not None:
                box.setPlainText(text)
            if click is None:
                return QDialog.DialogCode.Accepted
            next(b for b in self.findChildren(QPushButton)
                 if b.text().strip() == click).click()
            return self.result()

    monkeypatch.setattr(mw_mod, "QDialog", _ScriptedDialog)
    return seen


@pytest.fixture
def win(_qapp):
    """A MainWindow shell — no __init__, so no store, no threads."""
    w = mw_mod.MainWindow.__new__(mw_mod.MainWindow)
    w._log = []
    w.log_panel = types.SimpleNamespace(write_line=w._log.append)
    w.status = types.SimpleNamespace(showMessage=lambda *_a: None)
    w._saved = []
    return w


def _record_save(w, name):
    setattr(w, name, lambda: w._saved.append(name))


# ======================================================================
# List dialogs
# ======================================================================

def test_manual_input_stw_sends_the_parsed_list(win, monkeypatch):
    seen = _scripted_dialog(monkeypatch, PASTE, click="Send")
    sent = {}

    class _FakeWatchlist:
        def __init__(self, n, _parent):
            sent["n"] = n
            self.start_requested = types.SimpleNamespace(
                connect=lambda fn: sent.__setitem__("go", fn))

        def exec(self):
            sent["go"]("cfg")

    monkeypatch.setattr(mw_mod, "WatchlistDialog", _FakeWatchlist)
    win._launch_bridge = lambda syms, cfg: sent.__setitem__("syms", syms)
    win._manual_input_stw()
    assert sent["syms"] == EXPECTED and sent["n"] == 45
    assert INPUT_PROMPT in seen["labels"]


def test_lookup_dialog_parses_the_paste_and_shares_the_stw_prompt(_qapp):
    from PyQt6.QtWidgets import QLabel
    d = L.LookupDialog(text=PASTE)
    assert d.tickers() == EXPECTED
    assert any(lb.text() == INPUT_PROMPT for lb in d.findChildren(QLabel))


def test_rebuild_tickers_rebuilds_each_parsed_ticker_once(win, monkeypatch):
    seen = _scripted_dialog(monkeypatch, PASTE + "\nAMC(HB)")
    rebuilt = []
    monkeypatch.setattr(mw_mod, "rebuild_ticker", lambda s: rebuilt.append(s)
                        or types.SimpleNamespace(status="ok", rows_received=1))
    win._ipo_row_count_cache = {}
    win._rebuild_tickers_dialog()
    assert rebuilt == EXPECTED
    assert any(FORMAT_NOTE in t for t in seen["labels"])


def test_blacklist_editor_takes_a_column_and_keeps_its_one_line_file(
        win, monkeypatch, tmp_path):
    """The bug this release fixes: the box split on commas ONLY, so a column
    became one newline-bearing entry and nothing was blacklisted."""
    seen = _scripted_dialog(monkeypatch, PASTE)
    win._blacklist = {"OLD"}
    win._BLACKLIST_FILE = tmp_path / "blacklist.txt"
    win._show_blacklist_editor()
    assert win._blacklist == set(EXPECTED)
    on_disk = win._BLACKLIST_FILE.read_text(encoding="utf-8")
    assert "\n" not in on_disk and "(" not in on_disk
    assert on_disk == ", ".join(sorted(EXPECTED))
    assert any(FORMAT_NOTE in t for t in seen["labels"])


def test_greylist_editor_takes_a_column_and_keeps_its_one_line_file(
        win, monkeypatch, tmp_path):
    seen = _scripted_dialog(monkeypatch, PASTE)
    win._greylist = set()
    win._GREYLIST_FILE = tmp_path / "greylist.txt"
    win._show_greylist_editor()
    assert win._greylist == set(EXPECTED)
    on_disk = win._GREYLIST_FILE.read_text(encoding="utf-8")
    assert on_disk == ", ".join(sorted(EXPECTED))
    assert any(FORMAT_NOTE in t for t in seen["labels"])


@pytest.mark.parametrize("opener, attr, saver", [
    ("_show_zacks_skip_list_editor", "_zacks_blacklist",
     "_save_zacks_blacklist"),
    ("_show_finnhub_skip_list_editor", "_finnhub_blacklist",
     "_save_finnhub_blacklist"),
    ("_show_finviz_skip_list_editor", "_finviz_blacklist",
     "_save_finviz_blacklist"),
    ("_show_finviz_snapshot_skip_list_editor", "_finviz_snapshot_blacklist",
     "_save_finviz_snapshot_blacklist"),
])
def test_skip_list_editors_take_the_paste(win, monkeypatch, opener, attr,
                                          saver):
    seen = _scripted_dialog(monkeypatch, PASTE)
    setattr(win, attr, {"OLD"})
    win._skip_reasons = {}
    _record_save(win, saver)
    getattr(win, opener)()
    assert getattr(win, attr) == set(EXPECTED)
    assert win._saved == [saver]
    assert any(FORMAT_NOTE in t for t in seen["labels"])


@pytest.mark.parametrize("opener, attr, saver", [
    ("_show_blacklist_editor", "_blacklist", "_save_blacklist"),
    ("_show_greylist_editor", "_greylist", "_save_greylist"),
    ("_show_zacks_skip_list_editor", "_zacks_blacklist",
     "_save_zacks_blacklist"),
    ("_show_finnhub_skip_list_editor", "_finnhub_blacklist",
     "_save_finnhub_blacklist"),
    ("_show_finviz_skip_list_editor", "_finviz_blacklist",
     "_save_finviz_blacklist"),
    ("_show_finviz_snapshot_skip_list_editor", "_finviz_snapshot_blacklist",
     "_save_finviz_snapshot_blacklist"),
])
def test_ok_on_an_unedited_editor_keeps_live_spellings(win, monkeypatch,
                                                       opener, attr, saver):
    """Re-reading the prefilled list must not alter `$`/`^`/dash symbols."""
    _scripted_dialog(monkeypatch, text=None)
    setattr(win, attr, set(LIVE_SPELLINGS))
    win._skip_reasons = {}
    _record_save(win, saver)
    getattr(win, opener)()
    assert getattr(win, attr) == set(LIVE_SPELLINGS)


def test_reference_tickers_accept_a_tagged_column(win, monkeypatch):
    seen = _scripted_dialog(monkeypatch, "SPY(HB)\nONEQ\nxlk")
    saved = {}
    monkeypatch.setattr(config, "save_user_config",
                        lambda vals: saved.update(vals) or True)
    win._confirm_history_depth_decrease = lambda *_a: True
    win._show_advanced_settings()
    assert saved["REFERENCE_TICKERS"] == ["SPY", "ONEQ", "XLK"]
    assert any(FORMAT_NOTE in t for t in seen["labels"])


# ======================================================================
# Single-ticker Spot Fill prompts
# ======================================================================

def _typed(monkeypatch, text):
    monkeypatch.setattr(mw_mod, "QInputDialog", types.SimpleNamespace(
        getText=lambda *_a, **_k: (text, True)))


@pytest.fixture
def spot_fills(win, monkeypatch):
    """Each spot fill's fetch replaced by a recorder; returns the calls."""
    from trade_scanner_fh import (
        earnings_history, finnhub_fill, finviz_fill, yahoo_fill, zacks_scraper,
    )
    calls = []

    def rec(name):
        return lambda sym, *_a, **_k: calls.append((name, sym)) or (1, "ok")

    monkeypatch.setattr(finnhub_fill, "spot_fill_finnhub", rec("finnhub"))
    monkeypatch.setattr(finviz_fill, "spot_fill_finviz", rec("finviz"))
    monkeypatch.setattr(yahoo_fill, "spot_fill_yahoo", rec("yahoo"))
    monkeypatch.setattr(earnings_history, "spot_fill_zacks", rec("zacks"))
    monkeypatch.setattr(zacks_scraper, "has_zacks_cookies", lambda: True)
    for attr in ("_finnhub_worker", "_finviz_worker", "_zacks_worker"):
        setattr(win, attr, None)
    for attr in ("_blacklist", "_finnhub_blacklist", "_finviz_blacklist",
                 "_zacks_blacklist"):
        setattr(win, attr, set())
    win._check_finnhub_key_or_warn = lambda: True
    return calls


_SPOT_FILLS = ["_spot_fill_finnhub", "_spot_fill_finviz", "_spot_fill_yahoo",
               "_spot_fill_zacks"]


@pytest.mark.parametrize("method", _SPOT_FILLS)
def test_spot_fill_drops_a_pasted_finviz_tag(win, spot_fills, monkeypatch,
                                             method):
    _typed(monkeypatch, " agl(HB) ")
    getattr(win, method)()
    assert [sym for _src, sym in spot_fills] == ["AGL"]


@pytest.mark.parametrize("method", _SPOT_FILLS)
def test_spot_fill_with_only_a_tag_does_nothing(win, spot_fills, monkeypatch,
                                                method):
    _typed(monkeypatch, "(HB)")
    getattr(win, method)()
    assert spot_fills == []
