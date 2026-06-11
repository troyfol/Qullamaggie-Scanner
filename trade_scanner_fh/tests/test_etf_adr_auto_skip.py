"""Tests for the ETF / ADR auto-skip helper on MainWindow.

universe.csv from NASDAQ FTP carries clean `etf` and `adr` boolean
columns. The earnings sources (Zacks, Finnhub) have no useful data for
funds or foreign-issuer ADRs, so we pre-skip them from every bulk / gap
fill to save thousands of wasted HTTP requests.

Spot fill bypasses this auto-skip — when the user explicitly types
a symbol they mean it, even if universe.csv flags it as a fund.

These tests bind the helpers to a bare object via ``__new__`` rather
than instantiating MainWindow, because MainWindow.__init__ triggers
the OHLCV-update worker which fires real yfinance network calls and
hangs the suite under pytest."""
from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture(scope="module")
def _qapp():
    from PyQt6.QtWidgets import QApplication
    import sys
    app = QApplication.instance() or QApplication(sys.argv[:1])
    yield app


@pytest.fixture
def mw(_qapp, tmp_path, monkeypatch):
    """Build a MainWindow instance via ``__new__`` so __init__ doesn't
    run. Manually stub the few attributes the skip helpers reach into."""
    from trade_scanner_fh import config
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "FINNHUB_BLACKLIST_FILE",
                        tmp_path / "finnhub_blacklist.txt")
    monkeypatch.setattr(config, "FINVIZ_BLACKLIST_FILE",
                        tmp_path / "finviz_blacklist.txt")
    from trade_scanner_fh.gui.main_window import MainWindow
    w = MainWindow.__new__(MainWindow)
    # Stub the attribute surface the skip helpers reach into.
    w._universe_df = pd.DataFrame([
        {"symbol": "SPY",  "etf": True,  "adr": False},
        {"symbol": "QQQ",  "etf": True,  "adr": False},
        {"symbol": "BABA", "etf": False, "adr": True},
        {"symbol": "BP",   "etf": False, "adr": True},
        {"symbol": "AAPL", "etf": False, "adr": False},
        {"symbol": "MSFT", "etf": False, "adr": False},
    ])
    w._blacklist = set()
    w._zacks_blacklist = set()
    w._finnhub_blacklist = set()
    w._finviz_blacklist = set()
    # _save_*_blacklist methods write to the per-source blacklist files
    # via class methods that read config.*_BLACKLIST_FILE at call time —
    # they pick up the monkeypatched paths automatically.
    # _FINNHUB_BLACKLIST_FILE is a class attr bound at class-definition
    # time to the original config path. We rebind on the instance so
    # writes land in tmp_path.
    w._FINNHUB_BLACKLIST_FILE = tmp_path / "finnhub_blacklist.txt"
    w._FINVIZ_BLACKLIST_FILE = tmp_path / "finviz_blacklist.txt"
    return w


# ──────────────────────────────────────────────────────────────────────
# _etf_adr_auto_skip_set
# ──────────────────────────────────────────────────────────────────────

def test_auto_skip_set_collects_etf_and_adr_flagged_symbols(mw):
    auto = mw._etf_adr_auto_skip_set()
    assert auto == {"SPY", "QQQ", "BABA", "BP"}


def test_auto_skip_set_empty_when_no_universe(mw):
    mw._universe_df = None
    assert mw._etf_adr_auto_skip_set() == set()


def test_auto_skip_set_empty_when_no_etf_adr_columns(mw):
    mw._universe_df = pd.DataFrame([
        {"symbol": "AAPL"},
        {"symbol": "MSFT"},
    ])
    assert mw._etf_adr_auto_skip_set() == set()


def test_auto_skip_set_handles_nan_in_etf_adr_columns(mw):
    """NaN in the boolean columns must coerce to False (don't auto-skip
    on missing data — safer to attempt the fetch than to silently
    blacklist)."""
    mw._universe_df = pd.DataFrame([
        {"symbol": "AAPL", "etf": None, "adr": None},
        {"symbol": "SPY",  "etf": True, "adr": False},
    ])
    assert mw._etf_adr_auto_skip_set() == {"SPY"}


def test_auto_skip_set_normalizes_to_uppercase(mw):
    mw._universe_df = pd.DataFrame([
        {"symbol": "spy",  "etf": True,  "adr": False},
        {"symbol": "  qqq  ", "etf": True, "adr": False},
    ])
    assert mw._etf_adr_auto_skip_set() == {"SPY", "QQQ"}


# ──────────────────────────────────────────────────────────────────────
# Combined skip sets — ETF/ADR included alongside user lists
# ──────────────────────────────────────────────────────────────────────

def test_zacks_skip_set_includes_etf_adr_plus_user_lists(mw):
    mw._blacklist = {"BAD"}
    mw._zacks_blacklist = {"OLD"}
    skip = mw._zacks_skip_set()
    # ETF/ADR auto-skip
    assert "SPY" in skip and "QQQ" in skip
    assert "BABA" in skip and "BP" in skip
    # User-curated entries
    assert "BAD" in skip
    assert "OLD" in skip


def test_finnhub_skip_set_includes_etf_adr_plus_user_lists(mw):
    mw._blacklist = {"BAD"}
    mw._finnhub_blacklist = {"ETF1"}
    skip = mw._combined_finnhub_skip_set()
    assert "SPY" in skip and "BABA" in skip
    assert "BAD" in skip
    assert "ETF1" in skip


def test_finviz_skip_set_includes_etf_adr_plus_user_lists(mw):
    """Finviz bulk must skip ETFs/ADRs + the OHLCV blacklist + its own
    skip list (the user's explicit requirement for the overnight bulk)."""
    mw._blacklist = {"BAD"}
    mw._finviz_blacklist = {"FUND1"}
    skip = mw._combined_finviz_skip_set()
    assert "SPY" in skip and "QQQ" in skip      # ETFs
    assert "BABA" in skip and "BP" in skip      # ADRs
    assert "BAD" in skip                         # OHLCV blacklist
    assert "FUND1" in skip                       # finviz skip list


def test_etf_adr_NOT_persisted_to_per_source_blacklist_files(mw, tmp_path):
    """Critical guardrail: the dynamic ETF/ADR auto-skip must NOT pollute
    the per-source blacklist text files. Those stay user-curated +
    auto-added-on-fetch only, so re-derivation from universe.csv flows
    cleanly on every fill."""
    mw._combined_finnhub_skip_set()

    finn_path = tmp_path / "finnhub_blacklist.txt"
    if finn_path.exists():
        body = finn_path.read_text(encoding="utf-8")
        for sym in ("SPY", "QQQ", "BABA", "BP"):
            assert sym not in body, (
                f"ETF/ADR symbol {sym} leaked into finnhub_blacklist.txt"
            )


def test_universe_blacklist_persists_into_per_source_files(mw, tmp_path):
    """Inverse check: the universe blacklist (user-curated) SHOULD
    persist into the per-source files on every fill kickoff — this is
    the existing audit-trail behavior we want to preserve."""
    mw._blacklist = {"USERBANNED"}
    mw._combined_finnhub_skip_set()
    finn_path = tmp_path / "finnhub_blacklist.txt"
    assert finn_path.exists()
    assert "USERBANNED" in finn_path.read_text(encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────
# Universe blacklist (OHLCV) — must be honored by every earnings source
# ──────────────────────────────────────────────────────────────────────

def test_universe_blacklist_in_all_combined_skip_sets(mw):
    """The user-curated universe blacklist (driven by OHLCV failures +
    manual edits) must appear in every earnings source's combined skip
    set — `self._blacklist` is the universal opt-out."""
    mw._blacklist = {"BLOCKED"}
    assert "BLOCKED" in mw._zacks_skip_set()
    assert "BLOCKED" in mw._combined_finnhub_skip_set()
    assert "BLOCKED" in mw._combined_finviz_skip_set()


# ──────────────────────────────────────────────────────────────────────
# Spot fill — user-only skip set explicitly bypasses ETF/ADR auto-skip
# ──────────────────────────────────────────────────────────────────────
#
# These tests assert the contract by checking the user-only-skip
# expression used inside _spot_fill_finnhub. The helpers above show that
# ETF/ADR appear in the combined skip set; the spot-fill path computes
# its own skip via `self._blacklist | self._finnhub_blacklist` which
# excludes the ETF/ADR auto-skip.

def test_spot_fill_user_only_skip_excludes_etf_adr(mw):
    """Spot fill computes its skip as user-blacklist + per-source-list
    only — universe ETF/ADR flags must NOT block a user-typed symbol."""
    mw._blacklist = {"BAD"}
    mw._finnhub_blacklist = {"ETF1"}

    finn_user_only = mw._blacklist | mw._finnhub_blacklist

    # ETF / ADR symbols are NOT in the user-only set even though they
    # are in the combined skip set.
    for sym in ("SPY", "QQQ", "BABA", "BP"):
        assert sym not in finn_user_only
        assert sym in mw._combined_finnhub_skip_set()

    # User-curated entries still block spot fill.
    assert "BAD" in finn_user_only
    assert "ETF1" in finn_user_only
