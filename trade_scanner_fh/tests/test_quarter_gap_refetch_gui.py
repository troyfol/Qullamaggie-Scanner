"""The `missing_quarter` re-fetch action row (2026-09-18).

Covers the GUI half of the quarter-gap feature: the button offers exactly the
tickers the detector reports, stamps the ledger before launching, and hands
finviz + zacks (never finnhub) to the targeted fill.

Bound via ``__new__`` like the sibling GUI tests so MainWindow.__init__ never
runs its network workers.
"""
from __future__ import annotations

import pandas as pd
import pytest
from PyQt6.QtWidgets import QDialog, QMessageBox

from trade_scanner_fh import config
from trade_scanner_fh import earnings_history as eh


def _q(ticker: str, period) -> dict:
    return {
        "ticker": ticker,
        "period_ending": pd.Timestamp(period),
        "report_date": pd.Timestamp(period) + pd.Timedelta(days=30),
        "report_time": "Close",
        "estimated_eps": None, "reported_eps": 1.0,
        "surprise_eps": None, "surprise_eps_pct": None,
        "estimated_rev": None, "reported_rev": 100.0,
        "surprise_rev": None, "surprise_rev_pct": None,
        "source": "finviz",
        "updated_at": pd.Timestamp("2026-06-01"),
        "report_date_proxy": False,
    }


def _holed(ticker: str) -> list[dict]:
    end = pd.Timestamp.today().normalize()
    return [
        _q(ticker, end),
        _q(ticker, end - pd.Timedelta(days=91)),
        _q(ticker, end - pd.Timedelta(days=364)),   # ~273-day hole
        _q(ticker, end - pd.Timedelta(days=455)),
    ]


@pytest.fixture
def mw(_qapp, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    from trade_scanner_fh.gui.main_window import MainWindow
    w = MainWindow.__new__(MainWindow)
    w.log_panel = type("P", (), {"write_line": lambda self, s: None})()
    w._earn_threads_active = lambda: False
    w.launched = []
    w._launch_smart_refresh_workers = (
        lambda c, **kw: w.launched.append((list(c), kw)))
    return w


def _buttons(row):
    """The row's buttons, in order. Callers MUST keep `row` alive while using
    these: the layout is returned un-parented, so dropping the last Python
    reference lets Qt destroy it and its children mid-expression."""
    return [row.itemAt(i).widget()
            for i in range(row.count())
            if row.itemAt(i).widget() is not None]


def _button_texts(row):
    return [b.text() for b in _buttons(row)]


def test_row_offers_exactly_the_gapped_tickers(mw, tmp_path):
    df = pd.DataFrame(_holed("AAA") + _holed("BBB"))
    dlg = QDialog()
    row = mw._quarter_gap_refetch_row(dlg, df)
    assert any("Re-fetch these (2)" in t for t in _button_texts(row))


def test_row_button_disabled_when_everything_is_rested(mw, tmp_path):
    df = pd.DataFrame(_holed("AAA"))
    eh.record_earnings_gap_attempts(["AAA"])
    dlg = QDialog()
    row = mw._quarter_gap_refetch_row(dlg, df)
    btn = _buttons(row)[0]
    assert "Re-fetch these (0)" in btn.text()
    assert not btn.isEnabled()


def test_refetch_stamps_the_ledger_and_launches_finviz_plus_zacks(
    mw, tmp_path, monkeypatch,
):
    monkeypatch.setattr(
        QMessageBox, "question",
        lambda *a, **k: QMessageBox.StandardButton.Yes)
    df = pd.DataFrame(_holed("AAA") + _holed("BBB"))
    dlg = QDialog()
    row = mw._quarter_gap_refetch_row(dlg, df)
    _buttons(row)[0].click()

    assert len(mw.launched) == 1
    tickers, kwargs = mw.launched[0]
    assert sorted(tickers) == ["AAA", "BBB"]
    assert kwargs["include_finnhub"] is False
    assert kwargs["due"] is False
    # Stamped BEFORE the fill, so a ticker that errors still rests.
    assert set(eh.load_earnings_gap_attempts()) == {"AAA", "BBB"}


def test_declining_the_confirm_launches_nothing_and_stamps_nothing(
    mw, tmp_path, monkeypatch,
):
    monkeypatch.setattr(
        QMessageBox, "question",
        lambda *a, **k: QMessageBox.StandardButton.No)
    df = pd.DataFrame(_holed("AAA"))
    dlg = QDialog()
    row = mw._quarter_gap_refetch_row(dlg, df)   # keep the layout alive
    _buttons(row)[0].click()
    assert mw.launched == []
    assert eh.load_earnings_gap_attempts() == {}


def test_refetch_refuses_while_a_fill_is_running(mw, tmp_path, monkeypatch):
    warned = []
    monkeypatch.setattr(QMessageBox, "warning",
                        lambda *a, **k: warned.append(a))
    mw._earn_threads_active = lambda: True
    df = pd.DataFrame(_holed("AAA"))
    dlg = QDialog()
    row = mw._quarter_gap_refetch_row(dlg, df)   # keep the layout alive
    _buttons(row)[0].click()
    assert warned and mw.launched == []
    assert eh.load_earnings_gap_attempts() == {}


def test_reset_button_clears_the_ledger(mw, tmp_path, monkeypatch):
    monkeypatch.setattr(QMessageBox, "information", lambda *a, **k: None)
    eh.record_earnings_gap_attempts(["AAA", "BBB"])
    df = pd.DataFrame(_holed("AAA"))
    dlg = QDialog()
    row = mw._quarter_gap_refetch_row(dlg, df)
    reset = _buttons(row)[1]
    assert "Reset rested (2)" in reset.text()
    reset.click()
    assert eh.load_earnings_gap_attempts() == {}
