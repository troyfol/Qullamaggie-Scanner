"""Tests for gui/blacklists.py — BlacklistManager (Step A2 extraction).

MainWindow's four persisted skip-lists (universal OHLCV blacklist +
Zacks/Finnhub/finviz per-source lists) all load/save through
BlacklistManager now. These tests lock the on-disk formats, the
missing/corrupt-file degradation, the normalization rules, and the
newline-injection guard — byte-identical to the pre-extraction
per-method behavior.

Pure filesystem tests against tmp_path — no Qt widgets, no network.
The MainWindow delegate tests bind via ``__new__`` (the established
pattern in test_zacks_failure_breakdown.py) so __init__ never runs.
"""
from __future__ import annotations

import logging

import pytest

from trade_scanner_fh.gui.blacklists import BlacklistManager, normalize_ticker


# ──────────────────────────────────────────────────────────────────────
# normalize_ticker
# ──────────────────────────────────────────────────────────────────────

def test_normalize_ticker_unicode_dashes_case_and_whitespace():
    """Minus sign / en dash / em dash all collapse to ASCII hyphen;
    input is stripped and uppercased."""
    assert normalize_ticker("brk—a") == "BRK-A"   # em dash
    assert normalize_ticker("bf–b") == "BF-B"     # en dash
    assert normalize_ticker("rds−a") == "RDS-A"   # minus sign
    assert normalize_ticker("  aapl  ") == "AAPL"


def test_mainwindow_normalize_ticker_delegates():
    """MainWindow._normalize_ticker stays as a staticmethod (dozens of
    call sites + tests reference it) but now delegates to the shared
    blacklists.normalize_ticker implementation."""
    from trade_scanner_fh.gui.main_window import MainWindow
    assert MainWindow._normalize_ticker("brk—a") == "BRK-A"


# ──────────────────────────────────────────────────────────────────────
# "csv" format — the universal OHLCV blacklist (blacklist.txt)
# ──────────────────────────────────────────────────────────────────────

def test_csv_round_trip(tmp_path):
    path = tmp_path / "blacklist.txt"
    mgr = BlacklistManager(path, fmt="csv", label="blacklist")
    mgr.save({"MSFT", "AAPL", "BRK-A"})
    # Exact on-disk format: comma-space joined, sorted, no trailing newline
    assert path.read_text(encoding="utf-8") == "AAPL, BRK-A, MSFT"
    assert BlacklistManager(path, fmt="csv").load() == {
        "AAPL", "BRK-A", "MSFT",
    }


def test_csv_load_normalizes_and_skips_blanks(tmp_path):
    path = tmp_path / "blacklist.txt"
    path.write_text(" aapl , , brk—a ,msft", encoding="utf-8")
    assert BlacklistManager(path, fmt="csv").load() == {
        "AAPL", "BRK-A", "MSFT",
    }


def test_csv_save_empty_set_writes_empty_file(tmp_path):
    path = tmp_path / "blacklist.txt"
    mgr = BlacklistManager(path, fmt="csv")
    mgr.save(set())
    assert path.read_text(encoding="utf-8") == ""
    assert mgr.load() == set()


# ──────────────────────────────────────────────────────────────────────
# "lines" format — the per-source skip lists (*_blacklist.txt)
# ──────────────────────────────────────────────────────────────────────

def test_lines_round_trip(tmp_path):
    path = tmp_path / "zacks_blacklist.txt"
    mgr = BlacklistManager(path, label="Zacks skip list")
    mgr.save({"SPY", "QQQ", "ARKK"})
    # Exact on-disk format: one ticker per line, sorted, trailing newline
    assert path.read_text(encoding="utf-8") == "ARKK\nQQQ\nSPY\n"
    assert BlacklistManager(path).load() == {"ARKK", "QQQ", "SPY"}


def test_lines_load_skips_comments_and_splits_commas(tmp_path):
    """Loader tolerates comma-separated entries within a line and
    ignores `#` comment lines (manual-edit affordances)."""
    path = tmp_path / "skip.txt"
    path.write_text(
        "# user-curated entries below\n"
        "SPY, QQQ\n"
        "  # indented comment\n"
        "arkk\n"
        "\n",
        encoding="utf-8",
    )
    assert BlacklistManager(path).load() == {"SPY", "QQQ", "ARKK"}


def test_lines_save_strips_embedded_newlines_and_blanks(tmp_path):
    """Newline-injection guard: a crafted symbol with embedded CR/LF
    can't create phantom entries on the next line; whitespace-only
    tickers are dropped entirely."""
    path = tmp_path / "skip.txt"
    BlacklistManager(path).save({"AB\nCD", "OK\r", "  ", ""})
    assert path.read_text(encoding="utf-8") == "ABCD\nOK\n"


def test_lines_save_empty_set_writes_lone_newline(tmp_path):
    path = tmp_path / "skip.txt"
    mgr = BlacklistManager(path)
    mgr.save(set())
    assert path.read_text(encoding="utf-8") == "\n"
    assert mgr.load() == set()


# ──────────────────────────────────────────────────────────────────────
# Missing / corrupt files degrade to an empty set (never raise)
# ──────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("fmt", ["csv", "lines"])
def test_load_missing_file_returns_empty_set(tmp_path, fmt):
    mgr = BlacklistManager(tmp_path / "nope.txt", fmt=fmt)
    assert mgr.load() == set()


@pytest.mark.parametrize("fmt,label,expected_prefix", [
    ("csv", "blacklist", "Failed to load blacklist:"),
    ("lines", "Zacks skip list", "Failed to load Zacks skip list:"),
    ("lines", "Finnhub skip list", "Failed to load Finnhub skip list:"),
    ("lines", "finviz skip list", "Failed to load finviz skip list:"),
])
def test_load_unreadable_file_warns_and_returns_empty(
        tmp_path, caplog, fmt, label, expected_prefix):
    """An unreadable path (a directory here — exists() is True but
    read_text raises) degrades to an empty set and emits the exact
    pre-extraction warning text on the scanner.gui logger."""
    bad = tmp_path / "actually_a_dir"
    bad.mkdir()
    with caplog.at_level(logging.WARNING, logger="scanner.gui"):
        result = BlacklistManager(bad, fmt=fmt, label=label).load()
    assert result == set()
    recs = [r for r in caplog.records if r.name == "scanner.gui"]
    assert recs, "expected a warning on the scanner.gui logger"
    assert recs[-1].getMessage().startswith(expected_prefix)


# ──────────────────────────────────────────────────────────────────────
# MainWindow delegates — original method names round-trip through the
# manager (zacks delegates are covered in test_zacks_failure_breakdown)
# ──────────────────────────────────────────────────────────────────────

def _bare_main_window():
    from trade_scanner_fh.gui.main_window import MainWindow
    return MainWindow.__new__(MainWindow)  # skip __init__ (no workers)


def test_delegate_ohlcv_blacklist_round_trip(tmp_path):
    inst1 = _bare_main_window()
    inst1._BLACKLIST_FILE = tmp_path / "blacklist.txt"
    inst1._blacklist = {"BAD1", "BAD2"}
    inst1._save_blacklist()
    # CSV format preserved on disk
    assert (tmp_path / "blacklist.txt").read_text(
        encoding="utf-8") == "BAD1, BAD2"

    inst2 = _bare_main_window()
    inst2._BLACKLIST_FILE = tmp_path / "blacklist.txt"
    inst2._load_blacklist()
    assert inst2._blacklist == {"BAD1", "BAD2"}


def test_delegate_finnhub_blacklist_round_trip(tmp_path):
    inst1 = _bare_main_window()
    inst1._FINNHUB_BLACKLIST_FILE = tmp_path / "finnhub_blacklist.txt"
    inst1._finnhub_blacklist = {"ETF1", "FUND2"}
    inst1._save_finnhub_blacklist()

    inst2 = _bare_main_window()
    inst2._FINNHUB_BLACKLIST_FILE = tmp_path / "finnhub_blacklist.txt"
    inst2._load_finnhub_blacklist()
    assert inst2._finnhub_blacklist == {"ETF1", "FUND2"}


def test_delegate_finviz_blacklist_round_trip(tmp_path):
    inst1 = _bare_main_window()
    inst1._FINVIZ_BLACKLIST_FILE = tmp_path / "finviz_blacklist.txt"
    inst1._finviz_blacklist = {"SPY", "QQQ"}
    inst1._save_finviz_blacklist()
    # line-per-ticker format preserved on disk
    assert (tmp_path / "finviz_blacklist.txt").read_text(
        encoding="utf-8") == "QQQ\nSPY\n"

    inst2 = _bare_main_window()
    inst2._FINVIZ_BLACKLIST_FILE = tmp_path / "finviz_blacklist.txt"
    inst2._load_finviz_blacklist()
    assert inst2._finviz_blacklist == {"SPY", "QQQ"}


def test_delegate_load_missing_files_default_to_empty(tmp_path):
    """All four loaders degrade to empty sets when files are absent —
    they run in __init__ before the window is shown."""
    inst = _bare_main_window()
    inst._BLACKLIST_FILE = tmp_path / "blacklist.txt"
    inst._ZACKS_BLACKLIST_FILE = tmp_path / "zacks_blacklist.txt"
    inst._FINNHUB_BLACKLIST_FILE = tmp_path / "finnhub_blacklist.txt"
    inst._FINVIZ_BLACKLIST_FILE = tmp_path / "finviz_blacklist.txt"
    inst._load_blacklist()
    inst._load_zacks_blacklist()
    inst._load_finnhub_blacklist()
    inst._load_finviz_blacklist()
    assert inst._blacklist == set()
    assert inst._zacks_blacklist == set()
    assert inst._finnhub_blacklist == set()
    assert inst._finviz_blacklist == set()
