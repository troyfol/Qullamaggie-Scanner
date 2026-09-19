"""Zacks brought to parity with finviz and finnhub (v6.3.3).

Three things were out of step:

  * its Gap Fill selected on "no rows from ANY source", so a ticker another
    source already covered was never offered to Zacks;
  * the source-agnostic helper was called `find_gap_tickers`, colliding with
    `fill_framework.find_gap_tickers` — same name, different meaning, one
    keyword-only argument apart;
  * Zacks was the only source with no spot fill.
"""
from __future__ import annotations

import pandas as pd
import pytest

from trade_scanner_fh import config
from trade_scanner_fh import earnings_history as eh
from trade_scanner_fh import fill_framework


def _row(ticker, period, source, eps=1.0):
    p = pd.Timestamp(period)
    return {
        "ticker": ticker, "period_ending": p,
        "report_date": p + pd.Timedelta(days=30), "report_time": "Close",
        "estimated_eps": None, "reported_eps": eps,
        "surprise_eps": None, "surprise_eps_pct": None,
        "estimated_rev": None, "reported_rev": 100.0,
        "surprise_rev": None, "surprise_rev_pct": None,
        "source": source, "updated_at": pd.Timestamp("2026-06-01"),
        "report_date_proxy": False,
    }


# ── the rename, and why it mattered ────────────────────────────────────

def test_the_two_helpers_no_longer_share_a_name():
    """The collision was the defect: importing the wrong `find_gap_tickers`
    silently changed what a fill targeted, with no error."""
    assert not hasattr(eh, "find_gap_tickers")
    assert hasattr(eh, "find_uncovered_tickers")
    assert hasattr(fill_framework, "find_gap_tickers")


def test_uncovered_and_per_source_gap_answer_different_questions(tmp_parquets):
    """COVERED_ELSEWHERE has finviz data but no zacks data. It is not
    'uncovered', but it IS a zacks gap — and the old menu never offered it."""
    eh.save_earnings_history(pd.DataFrame([
        _row("COVERED_ELSEWHERE", "2026-03-01", "finviz"),
        _row("HAS_ZACKS", "2026-03-01", "zacks"),
    ]))
    universe = ["COVERED_ELSEWHERE", "HAS_ZACKS", "NOTHING"]

    assert eh.find_uncovered_tickers(universe, set()) == ["NOTHING"]
    assert sorted(eh.find_zacks_gap_tickers(universe, set())) == [
        "COVERED_ELSEWHERE", "NOTHING"]


def test_zacks_gap_now_matches_the_other_two_sources(tmp_parquets):
    """Parity: each source's gap fill asks about its OWN coverage."""
    from trade_scanner_fh.finviz_fill import find_finviz_gap_tickers
    from trade_scanner_fh.finnhub_fill import find_finnhub_gap_tickers
    eh.save_earnings_history(pd.DataFrame([
        _row("ONLY_ZACKS", "2026-03-01", "zacks"),
    ]))
    universe = ["ONLY_ZACKS", "NOTHING"]
    assert eh.find_zacks_gap_tickers(universe, set()) == ["NOTHING"]
    assert sorted(find_finviz_gap_tickers(universe, set())) == [
        "NOTHING", "ONLY_ZACKS"]
    assert sorted(find_finnhub_gap_tickers(universe, set())) == [
        "NOTHING", "ONLY_ZACKS"]


def test_zacks_gap_honours_the_blacklist(tmp_parquets):
    eh.save_earnings_history(pd.DataFrame([_row("A", "2026-03-01", "finviz")]))
    assert eh.find_zacks_gap_tickers(["A", "B"], {"B"}) == ["A"]


def test_empty_store_returns_the_whole_universe(tmp_parquets):
    assert sorted(eh.find_zacks_gap_tickers(["A", "B"], set())) == ["A", "B"]
    assert sorted(eh.find_uncovered_tickers(["A", "B"], set())) == ["A", "B"]


# ── spot fill ──────────────────────────────────────────────────────────

class _FakeSession:
    """Stands in for ZacksSession. `rows` is what fetch() returns."""
    def __init__(self, rows=None, kind=None, raises=False):
        self._rows, self._kind, self._raises = rows, kind, raises
        self.last_failure_kind = kind
    def __enter__(self):
        return self
    def __exit__(self, *a):
        return False
    def fetch(self, symbol, years=None):
        if self._raises:
            raise RuntimeError("boom")
        return self._rows


def _zacks_row(period="2026-03-01", eps=1.23):
    """One raw scraper row, in the shape `_row_to_history_dict` expects."""
    return {
        "period_ending": period, "report_date": "2026-04-01",
        "report_time": "Close", "estimated_eps": 1.0, "reported_eps": eps,
        "surprise_eps": 0.23, "surprise_eps_pct": 23.0,
        "estimated_rev": None, "reported_rev": 500.0,
        "surprise_rev": None, "surprise_rev_pct": None,
    }


@pytest.mark.parametrize("symbol, expected", [
    ("", "invalid"), ("   ", "invalid"),
])
def test_spot_fill_rejects_an_empty_symbol(symbol, expected, tmp_parquets):
    assert eh.spot_fill_zacks(symbol, set()) == (0, expected)


def test_spot_fill_respects_the_blacklist(tmp_parquets):
    assert eh.spot_fill_zacks("AAA", {"AAA"}) == (0, "blacklisted")


def test_spot_fill_writes_the_quarters(tmp_parquets, monkeypatch):
    monkeypatch.setattr(eh, "ZacksSession",
                        lambda *a, **k: _FakeSession([_zacks_row()]))
    count, status = eh.spot_fill_zacks("AAA", set())
    assert (count, status) == (1, "ok")
    df = eh.load_earnings_history()
    assert list(df["ticker"]) == ["AAA"]
    assert df.iloc[0]["source"] == "zacks"
    assert float(df.iloc[0]["reported_eps"]) == pytest.approx(1.23)


def test_spot_fill_reports_an_uncovered_ticker(tmp_parquets, monkeypatch):
    monkeypatch.setattr(eh, "ZacksSession",
                        lambda *a, **k: _FakeSession([], kind="not_found"))
    assert eh.spot_fill_zacks("AAA", set()) == (0, "not_found")


def test_spot_fill_maps_a_parsed_but_empty_page_to_empty(tmp_parquets, monkeypatch):
    """`last_failure_kind` is None when the page read fine but held nothing."""
    monkeypatch.setattr(eh, "ZacksSession",
                        lambda *a, **k: _FakeSession([], kind=None))
    assert eh.spot_fill_zacks("AAA", set()) == (0, "empty")


def test_spot_fill_surfaces_the_failure_kind(tmp_parquets, monkeypatch):
    monkeypatch.setattr(eh, "ZacksSession",
                        lambda *a, **k: _FakeSession(None, kind="blocked"))
    assert eh.spot_fill_zacks("AAA", set()) == (0, "blocked")


def test_spot_fill_never_raises(tmp_parquets, monkeypatch):
    monkeypatch.setattr(eh, "ZacksSession",
                        lambda *a, **k: _FakeSession(raises=True))
    assert eh.spot_fill_zacks("AAA", set()) == (0, "unknown")


def test_spot_fill_applies_the_history_cap(tmp_parquets, monkeypatch):
    """A quarter older than the cap is not written, and the caller is told
    why rather than being shown a bare zero."""
    old = (pd.Timestamp.today().normalize()
           - pd.DateOffset(years=config.EARNINGS_HISTORY_YEARS + 3))
    monkeypatch.setattr(
        eh, "ZacksSession",
        lambda *a, **k: _FakeSession([_zacks_row(period=old.date().isoformat())]))
    assert eh.spot_fill_zacks("AAA", set()) == (0, "no_rows_in_window")


def test_spot_fill_applies_the_actual_value_ingest_gate(tmp_parquets, monkeypatch):
    """A scheduled-but-unreported quarter has neither an EPS nor a revenue
    and must not claim its slot — the same gate every other source applies."""
    blank = _zacks_row()
    blank["reported_eps"] = None
    blank["reported_rev"] = None
    monkeypatch.setattr(eh, "ZacksSession",
                        lambda *a, **k: _FakeSession([blank]))
    count, status = eh.spot_fill_zacks("AAA", set())
    assert (count, status) == (0, "no_rows_in_window")


def test_spot_fill_replaces_only_the_zacks_rows(tmp_parquets, monkeypatch):
    """Another source's row for the same ticker survives, exactly as it does
    for a bulk fill — the write goes through the shared flush path."""
    eh.save_earnings_history(pd.DataFrame([
        _row("AAA", "2025-12-01", "finviz", eps=9.9),
    ]))
    monkeypatch.setattr(eh, "ZacksSession",
                        lambda *a, **k: _FakeSession([_zacks_row()]))
    assert eh.spot_fill_zacks("AAA", set())[1] == "ok"
    df = eh.load_earnings_history()
    assert set(df["source"]) == {"finviz", "zacks"}


# ── GUI wiring ─────────────────────────────────────────────────────────

def test_the_menu_handler_was_renamed_and_the_spot_handler_exists():
    from trade_scanner_fh.gui.main_window import MainWindow
    assert hasattr(MainWindow, "_gap_fill_zacks")
    assert hasattr(MainWindow, "_spot_fill_zacks")
    assert not hasattr(MainWindow, "_targeted_fill_zacks")


def test_every_source_now_has_bulk_gap_and_spot():
    """The parity check, stated as one assertion."""
    from trade_scanner_fh.gui.main_window import MainWindow
    for src in ("zacks", "finviz", "finnhub"):
        assert hasattr(MainWindow, f"_gap_fill_{src}"), f"{src} gap"
        assert hasattr(MainWindow, f"_spot_fill_{src}"), f"{src} spot"
