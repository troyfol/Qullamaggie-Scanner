"""Anti-truncation guards — audit 2026-08-12 (INT-1, INT-2 — both CRITICAL).

`load_earnings_history()` returned None for two semantically OPPOSITE
conditions — "no store yet" and "store exists but I couldn't read it" — and
both flush helpers treated None as "no history", writing the flush buffer as
the WHOLE store. One simulated `OSError(32)` (antivirus, backup agent, cloud
sync, another process's os.replace) destroyed 4,001 of 4,003 rows: 100% of the
store. `sector_map` had the identical bug with no retry at all (400 rows → 1).
This ran every 25-100 tickers of every fill, ~600 times per universe fill.

There were NO tests for this. The deferral branch in
`fill_framework.flush_pending_to_disk` referenced an undefined `log` and would
have raised NameError instead of deferring — caught only by re-running the
audit probe during release verification. Hence this file covers BOTH flush
helpers, and asserts the log line actually emits.
"""
from __future__ import annotations

import logging

import pandas as pd
import pytest

from trade_scanner_fh import config
from trade_scanner_fh import earnings_history as eh
from trade_scanner_fh import fill_framework as ff
from trade_scanner_fh import sector_map as sm


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "earnings_history.parquet")
    monkeypatch.setattr(config, "SECTOR_MAP_PARQUET",
                        tmp_path / "sector_map.parquet")
    rows = [{"ticker": f"T{i:04d}",
             "period_ending": pd.Timestamp("2026-03-31"),
             "report_date": pd.Timestamp("2026-04-15"),
             "reported_eps": 1.0, "source": "finviz"} for i in range(50)]
    eh.save_earnings_history(pd.DataFrame(rows), sort=True)
    return tmp_path


class _break_reads_of:
    """Context manager making pd.read_parquet raise a sharing violation for
    one store, then restoring it — WITHOUT touching the fixture's config
    patches (monkeypatch.undo() would revert those too, so the verification
    read afterwards would look at the real data dir)."""

    def __init__(self, needle: str):
        self.needle = needle
        self.calls = 0
        self._real = None

    def __enter__(self):
        self._real = pd.read_parquet

        def failing(path, *a, **k):
            if self.needle in str(path):
                self.calls += 1
                raise OSError(32, "The process cannot access the file")
            return self._real(path, *a, **k)

        pd.read_parquet = failing
        return self

    def __exit__(self, *exc):
        pd.read_parquet = self._real
        return False


NEW_ROW = {"ticker": "FLUSH1", "period_ending": pd.Timestamp("2026-06-30"),
           "report_date": pd.Timestamp("2026-07-15"),
           "reported_eps": 2.0, "source": "finviz"}


# ── INT-1: earnings_history ────────────────────────────────────────────

def test_framework_flush_defers_instead_of_truncating(store, monkeypatch):
    before = len(eh.load_earnings_history())
    with _break_reads_of("earnings_history") as broken:
        wrote = ff.flush_pending_to_disk({"FLUSH1": [NEW_ROW]}, source="finviz")

    assert wrote is False, "flush must report the deferral to its caller"
    assert len(eh.load_earnings_history()) == before, "store was truncated"
    assert broken.calls >= 2, "the read should be retried before giving up"


def test_framework_flush_deferral_logs_rather_than_raising(
        store, monkeypatch, caplog):
    """The regression that shipped: the deferral branch referenced an
    undefined `log`, so the guard raised NameError instead of deferring."""
    with _break_reads_of("earnings_history"), caplog.at_level(logging.ERROR):
        wrote = ff.flush_pending_to_disk({"FLUSH1": [NEW_ROW]}, source="finviz")

    assert wrote is False
    assert any("unreadable" in r.message or "unreadable" in r.getMessage()
               for r in caplog.records), "the deferral was not reported"


def test_history_module_flush_defers_too(store, monkeypatch):
    """`earnings_history._flush_pending_to_disk` is the zacks path — same
    guard, separate implementation."""
    before = len(eh.load_earnings_history())
    with _break_reads_of("earnings_history"):
        wrote = eh._flush_pending_to_disk(
            {"FLUSH1": [NEW_ROW]}, [], source="finviz")

    assert wrote is False
    assert len(eh.load_earnings_history()) == before


def test_a_healthy_flush_still_writes(store):
    """The guard must narrow WHEN a write happens, not whether it works."""
    before = len(eh.load_earnings_history())
    wrote = ff.flush_pending_to_disk({"FLUSH1": [NEW_ROW]}, source="finviz")
    after = eh.load_earnings_history()

    assert wrote is not False
    assert len(after) == before + 1
    assert "FLUSH1" in set(after["ticker"].astype(str))


def test_first_ever_write_is_not_mistaken_for_an_unreadable_store(
        tmp_path, monkeypatch):
    """A genuinely ABSENT store must still be created — the guard keys on
    "exists but unreadable", not on None alone."""
    monkeypatch.setattr(config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "fresh.parquet")
    wrote = ff.flush_pending_to_disk({"FLUSH1": [NEW_ROW]}, source="finviz")
    assert wrote is not False
    assert len(eh.load_earnings_history()) == 1


# ── INT-2: sector_map ──────────────────────────────────────────────────

def _seed_sectors(n=40):
    sm.save_sector_map(pd.DataFrame([
        {"ticker": f"S{i:04d}", "sector": "Tech", "sector_etf": "XLK",
         "updated_at": pd.Timestamp("2026-08-01")} for i in range(n)]))


def test_sector_flush_defers_instead_of_truncating(store, monkeypatch):
    _seed_sectors()
    before = len(sm.load_sector_map())
    with _break_reads_of("sector_map") as broken:
        wrote = sm._flush_sector_rows([
            {"ticker": "NEW", "sector": "Tech", "sector_etf": "XLK",
             "updated_at": pd.Timestamp("2026-08-12")}])

    assert wrote is False
    assert len(sm.load_sector_map()) == before, "sector map was truncated"
    assert broken.calls >= 2, "sector_map had NO retry at all before INT-2"


def test_sector_bulk_fill_does_not_truncate_either(store, monkeypatch):
    """`bulk_fill_sectors` has its own read-modify-write, separate from
    `_flush_sector_rows`."""
    _seed_sectors()
    before = len(sm.load_sector_map())
    # Stub financedatabase so no network is touched.
    import sys
    import types as _types
    fake = _types.ModuleType("financedatabase")

    class _Eq:
        def select(self):
            return pd.DataFrame(
                {"sector": ["Tech"]}, index=pd.Index(["NEWTICK"], name="symbol"))

    fake.Equities = _Eq
    monkeypatch.setitem(sys.modules, "financedatabase", fake)
    with _break_reads_of("sector_map"):
        filled, _skipped = sm.bulk_fill_sectors(["NEWTICK"], set())

    assert filled == 0, "bulk fill should abandon the merge, not truncate"
    assert len(sm.load_sector_map()) == before


def test_healthy_sector_flush_still_writes(store):
    _seed_sectors()
    before = len(sm.load_sector_map())
    wrote = sm._flush_sector_rows([
        {"ticker": "NEW", "sector": "Tech", "sector_etf": "XLK",
         "updated_at": pd.Timestamp("2026-08-12")}])
    assert wrote is not False
    assert len(sm.load_sector_map()) == before + 1
