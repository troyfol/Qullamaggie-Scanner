"""Tests for gui/blacklists.py — BlacklistManager (Step A2 extraction).

MainWindow's four persisted skip-lists (universal OHLCV blacklist +
Zacks/Finnhub/finviz per-source lists) all load/save through
BlacklistManager now. These tests lock the on-disk formats, the
missing/corrupt-file degradation, the normalization rules, and the
newline-injection guard.

The "lines" format gained TICKER<TAB>ADDED_ON<TAB>REASON metadata in audit
2026-08-12 (INT-5), so the format assertions read the ticker column rather
than the whole line; legacy bare-ticker files must still load, which
test_lines_load_accepts_legacy_bare_format pins.

Pure filesystem tests against tmp_path — no Qt widgets, no network.
The MainWindow delegate tests bind via ``__new__`` (the established
pattern in test_zacks_failure_breakdown.py) so __init__ never runs.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

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
    """One entry per line, sorted, trailing newline.

    Since audit 2026-08-12 (INT-5) each line is TICKER<TAB>ADDED_ON<TAB>REASON
    under a `#` header — the bare-ticker form carried no date, which made a
    re-validation cadence impossible and turned the lists into a one-way
    ratchet. `load()` still returns a plain set, so callers are unaffected.
    """
    path = tmp_path / "zacks_blacklist.txt"
    mgr = BlacklistManager(path, label="Zacks skip list")
    mgr.save({"SPY", "QQQ", "ARKK"})
    body = [ln for ln in path.read_text(encoding="utf-8").splitlines()
            if ln.strip() and not ln.startswith("#")]
    assert [ln.split("\t")[0] for ln in body] == ["ARKK", "QQQ", "SPY"]
    today = date.today().isoformat()
    assert all(ln.split("\t")[1] == today for ln in body)
    assert BlacklistManager(path).load() == {"ARKK", "QQQ", "SPY"}


def test_lines_load_accepts_legacy_bare_format(tmp_path):
    """A pre-INT-5 file of bare tickers must still load — the upgrade must not
    silently empty three skip lists totalling ~26k entries."""
    path = tmp_path / "legacy.txt"
    path.write_text("ARKK\nQQQ\nSPY\n", encoding="utf-8")
    assert BlacklistManager(path).load() == {"ARKK", "QQQ", "SPY"}
    # Legacy entries carry no date, so re-validation treats them as eligible.
    assert BlacklistManager(path).load_entries()["SPY"] == (None, "")
    assert BlacklistManager(path).stale_entries(90) == {"ARKK", "QQQ", "SPY"}


def test_lines_save_preserves_existing_added_on(tmp_path):
    """A plain set-based save must not reset the age of entries already on
    disk — otherwise every save would push the re-check horizon out forever."""
    path = tmp_path / "skip.txt"
    old = date.today() - timedelta(days=200)
    BlacklistManager(path).save_entries({"OLD": (old, "empty")})
    BlacklistManager(path).save({"OLD", "NEW"})
    entries = BlacklistManager(path).load_entries()
    assert entries["OLD"] == (old, "empty")
    assert entries["NEW"][0] == date.today()
    assert BlacklistManager(path).stale_entries(90, reasons={"empty"}) == {"OLD"}


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


def test_lines_save_strips_embedded_newlines_tabs_and_blanks(tmp_path):
    """Injection guard: a crafted symbol with embedded CR/LF can't create
    phantom entries on the next line, and — since the INT-5 format is
    tab-delimited — an embedded TAB can't forge the ADDED_ON / REASON columns.
    Whitespace-only tickers are dropped entirely."""
    path = tmp_path / "skip.txt"
    BlacklistManager(path).save({"AB\nCD", "OK\r", "EV\tIL", "  ", ""})
    body = [ln for ln in path.read_text(encoding="utf-8").splitlines()
            if ln.strip() and not ln.startswith("#")]
    assert [ln.split("\t")[0] for ln in body] == ["ABCD", "EVIL", "OK"]
    assert all(len(ln.split("\t")) == 3 for ln in body)
    assert BlacklistManager(path).load() == {"ABCD", "EVIL", "OK"}


def test_lines_save_empty_set_round_trips_empty(tmp_path):
    """An empty save must not leave anything the loader reads back as an
    entry — the header lines are comments and carry no ticker."""
    path = tmp_path / "skip.txt"
    mgr = BlacklistManager(path)
    mgr.save(set())
    assert mgr.load() == set()
    assert mgr.load_entries() == {}


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
    # One entry per line, sorted — TICKER<TAB>ADDED_ON<TAB>REASON since INT-5.
    # Entries with no recorded fill reason are stamped "manual": they came from
    # the editor dialog, not from an automated skip.
    body = [ln for ln in (tmp_path / "finviz_blacklist.txt").read_text(
        encoding="utf-8").splitlines() if ln.strip() and not ln.startswith("#")]
    assert [ln.split("\t")[0] for ln in body] == ["QQQ", "SPY"]
    assert all(ln.split("\t")[2] == "manual" for ln in body)

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
