"""v8.0.0 phase 2 — hide / unhide rows and columns (replaced delete).

User decisions (2026-09-26) these tests pin:
  * hidden rows stay hidden across new scans and through presets until
    unhidden, with a visible indicator and a log line;
  * only the Ticker column is un-hideable;
  * Delete = hide, Ctrl+Z = unhide the last batch;
  * loading a preset always makes every hide set match the preset;
  * whatever is hidden is not exported (hidden columns re-offered unticked);
  * a Hide Q / Hide FV dropdown trumps the right-click (can't unhide there).
"""
from __future__ import annotations

import json

import pandas as pd
import pytest
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QMenu

from trade_scanner_fh.gui import widgets as W


@pytest.fixture(scope="module")
def qapp():
    from PyQt6.QtWidgets import QApplication
    return QApplication.instance() or QApplication([])


@pytest.fixture
def window(qapp, tmp_parquets, monkeypatch):
    from trade_scanner_fh.gui import main_window as mw_mod
    monkeypatch.setattr(mw_mod, "PRESETS_DIR", tmp_parquets / "presets")
    (tmp_parquets / "presets").mkdir(exist_ok=True)
    w = mw_mod.MainWindow()
    yield w
    w.close()
    w.deleteLater()


def _frame(symbols=("AAPL", "MSFT", "NVDA", "TSLA"), **extra):
    rows = []
    for i, s in enumerate(symbols):
        row = {"symbol": s, "close": 10.0 + i, "pct_gain": 1.0 + i,
               "gain_start_date": pd.Timestamp("2026-01-02"),
               "avg_vol": 1e6 * (i + 1), "rvol": 1.0 + i, "sti": 1.1}
        row.update({k: v[i] if isinstance(v, list) else v
                    for k, v in extra.items()})
        rows.append(row)
    return pd.DataFrame(rows)


def _load(w, periods: dict):
    w._period_results = dict(periods)
    w._period_order = list(periods)
    w._active_period = w._period_order[0]
    w.combo_timeframe.blockSignals(True)
    w.combo_timeframe.clear()
    for label in w._period_order:
        w.combo_timeframe.addItem(label, userData=label)
    w.combo_timeframe.setCurrentIndex(0)
    w.combo_timeframe.blockSignals(False)
    w._on_timeframe_changed(0)


def _keys(w):
    return [k for _h, k, _f in w.results_table.active_columns]


def _shown(w):
    """Visible tickers as a SET: the table starts sorted by Ticker
    descending (Qt's default sort indicator), so order is not the point."""
    return set(w.results_table.get_symbols())


def _select(w, sym):
    """Select the row showing `sym`, wherever the current sort put it."""
    t = w.results_table
    for r in range(t.proxy.rowCount()):
        if t.proxy.index(r, 0).data() == sym:
            t.selectRow(r)
            return r
    raise AssertionError(f"{sym} not shown")


def _capture_menu(monkeypatch):
    """Replace QMenu.exec so a context-menu handler returns immediately and
    the built menu can be inspected."""
    seen = {}

    def fake_exec(self, *a, **k):
        seen["menu"] = self
        return None
    monkeypatch.setattr(QMenu, "exec", fake_exec)
    return seen


def _texts(menu):
    return [a.text() for a in menu.actions() if not a.isSeparator()]


def _submenu(menu, prefix):
    for a in menu.actions():
        if a.menu() is not None and a.text().startswith(prefix):
            return a.menu()
    return None


# ======================================================================
# Rows
# ======================================================================

def test_hidden_rows_leave_every_view_but_not_the_results(window):
    _load(window, {"1D": _frame(), "1W": _frame()})
    window._on_rows_hide_requested(["MSFT", "TSLA"])
    assert _shown(window) == {"AAPL", "NVDA"}
    assert len(window._period_results["1D"]) == 4
    window.combo_timeframe.setCurrentIndex(1)
    assert window._active_period == "1W"
    assert _shown(window) == {"AAPL", "NVDA"}


def test_body_menu_offers_hide_and_unhide(window, monkeypatch):
    _load(window, {"1D": _frame()})
    window._on_rows_hide_requested(["MSFT"])
    t = window.results_table
    r = _select(window, "AAPL")
    seen = _capture_menu(monkeypatch)
    t._show_row_context_menu(t.visualRect(t.proxy.index(r, 0)).center())
    texts = _texts(seen["menu"])
    assert "Hide row 'AAPL'\tDel" in texts
    assert "Unhide all rows (1)" in texts
    assert "Undo hide\tCtrl+Z" in texts
    sub = _submenu(seen["menu"], "Unhide rows")
    assert sub is not None and _texts(sub) == ["MSFT"]
    sub.actions()[0].trigger()
    assert "MSFT" in _shown(window)


def test_unhide_list_explains_what_would_still_not_show(window):
    """Tickers in this period come first; one filtered out by a view filter
    or absent from the period is labelled, and stays clearable."""
    df = _frame(reported_eps=[1.0, None, 2.0, 3.0])
    _load(window, {"1D": df})
    window._on_rows_hide_requested(["MSFT", "NVDA", "ZZZZ"])
    window.chk_view_earnings_data_only.setChecked(True)
    items = window._row_unhide_items()
    assert items == [
        ("MSFT  (filtered by a view filter)", "MSFT", True),
        ("NVDA", "NVDA", True),
        ("ZZZZ  (not in this period)", "ZZZZ", True),
    ]


def test_unhide_all_rows(window):
    _load(window, {"1D": _frame()})
    window._on_rows_hide_requested(["MSFT", "NVDA"])
    window._unhide_all_rows()
    assert _shown(window) == {"AAPL", "MSFT", "NVDA", "TSLA"}


def test_delete_key_hides_and_ctrl_z_brings_back(window):
    from PyQt6.QtCore import QEvent
    from PyQt6.QtGui import QKeyEvent
    _load(window, {"1D": _frame()})
    t = window.results_table
    _select(window, "MSFT")
    t.keyPressEvent(QKeyEvent(QEvent.Type.KeyPress, Qt.Key.Key_Delete,
                              Qt.KeyboardModifier.NoModifier))
    assert "MSFT" not in _shown(window)
    t.keyPressEvent(QKeyEvent(QEvent.Type.KeyPress, Qt.Key.Key_Z,
                              Qt.KeyboardModifier.ControlModifier))
    assert "MSFT" in _shown(window)
    assert t.hide_undo_available() is False


def test_send_to_watchlist_symbols_exclude_hidden_rows(window):
    _load(window, {"1D": _frame()})
    window._on_rows_hide_requested(["NVDA"])
    assert "NVDA" not in window.results_table.get_symbols()


# ======================================================================
# Columns
# ======================================================================

def test_close_gain_and_gain_start_are_hideable_ticker_is_not(window):
    _load(window, {"1D": _frame()})
    window._on_columns_hide_requested(
        ["symbol", "close", "pct_gain", "gain_start_date"])
    keys = _keys(window)
    assert "symbol" in keys
    assert not ({"close", "pct_gain", "gain_start_date"} & set(keys))


def test_columns_dialog_locks_only_the_ticker(window):
    _load(window, {"1D": _frame()})
    window._open_columns_dialog()
    lst = window._columns_dialog._list
    locked = {lst.item(i).data(Qt.ItemDataRole.UserRole)
              for i in range(lst.count())
              if not lst.item(i).flags() & Qt.ItemFlag.ItemIsUserCheckable}
    assert locked == {"symbol"}
    window._columns_dialog.close()


def test_header_menu_offers_hide_and_locked_unhide_entries(window, monkeypatch):
    df = _frame()
    for k in (1, 2):
        df[f"q{k}_reported_eps"] = 1.0
        df[f"q{k}_report_date_eps"] = pd.Timestamp("2026-01-01")
    df["consec_eps_beats"] = 1
    _load(window, {"1D": df})
    window._on_columns_hide_requested(["avg_vol", "q1_reported_eps"])
    window._hidden_earnings_col_types = {"q_reported_eps"}
    window._apply_hidden_column_types()
    hdr = window.results_table._reorderable_header
    seen = _capture_menu(monkeypatch)
    hdr._on_context_menu(hdr.rect().center())
    texts = _texts(seen["menu"])
    assert any(t.startswith("Hide 1 column") for t in texts)
    sub = _submenu(seen["menu"], "Unhide columns")
    entries = {a.text(): a.isEnabled() for a in sub.actions()}
    assert entries["Avg Vol"] is True
    assert entries["Q-1 Reported EPS  (hidden by Hide Q Columns)"] is False
    assert "Unhide all columns (1)" in texts


def test_unhide_all_columns_leaves_type_hides_alone(window):
    df = _frame(pe=[10.0, 20.0, 30.0, 40.0], peg=[1.0, 2.0, 3.0, 4.0])
    _load(window, {"1D": df})
    window._on_columns_hide_requested(["avg_vol", "pe"])
    window._on_hide_fv_type_toggled("fv_valuation", True)
    window._unhide_all_columns()
    keys = set(_keys(window))
    assert "avg_vol" in keys
    assert "pe" not in keys and "peg" not in keys, "the dropdown still hides"
    assert window._hidden_fv_col_types == {"fv_valuation"}


def test_hidden_column_not_in_this_scan_is_listed_and_clearable(window):
    _load(window, {"1D": _frame()})
    window._deleted_column_keys = {"hv_rank"}
    items = window._column_unhide_items()
    assert items == [("HV Rank  (not in this scan)", "hv_rank", True)]


def test_column_undo_and_dialog_batches(window):
    _load(window, {"1D": _frame()})
    window._on_columns_hide_requested(["avg_vol"])
    order = [k for _h, k, _f in window._current_columns_for_dialog()]
    window._on_columns_dialog_updated(order, ["avg_vol", "rvol"])
    assert not ({"avg_vol", "rvol"} & set(_keys(window)))
    window._on_hide_undo_requested()          # the dialog batch: rvol
    assert "rvol" in _keys(window) and "avg_vol" not in _keys(window)
    window._on_hide_undo_requested()          # the right-click batch
    assert "avg_vol" in _keys(window)


def test_unhidden_column_returns_to_its_place_after_a_drag(window):
    """The header only reports VISIBLE columns when dragged; a hidden one
    must keep its slot in the saved order."""
    _load(window, {"1D": _frame()})
    before = _keys(window)
    # A column from the MIDDLE of the layout: one already at the end would
    # land "back in place" even if the merge were missing.
    mid = before[len(before) // 2]
    last = before[-1]
    assert mid not in ("symbol", last)
    window._on_columns_hide_requested([mid])
    visible = _keys(window)
    # User drags the last column to the front.
    dragged = [last] + [k for k in visible if k != last]
    window._on_results_column_order_changed(dragged)
    window._unhide_columns([mid])
    order = window.results_table.current_column_order()
    expected = [last] + [k for k in before if k != last]
    assert order == expected, (mid, order)


# ======================================================================
# Exports
# ======================================================================

def test_excel_export_omits_hidden_rows_in_every_period(window, tmp_path):
    _load(window, {"1D": _frame(), "1W": _frame()})
    window._on_rows_hide_requested(["MSFT"])
    window._on_columns_hide_requested(["avg_vol"])
    keys = [k for _h, k, _f in window._ordered_active_columns_for_export()]
    path = tmp_path / "x.xlsx"
    window._write_xlsx_multi_sheet(str(path), ["1D", "1W"], keys, False)
    sheets = pd.read_excel(path, sheet_name=None)
    for name, df in sheets.items():
        assert "MSFT" not in set(df["Ticker"]), name
        assert "Avg Volume" not in df.columns, name
        assert len(df) == 3


def test_csv_export_omits_hidden_rows(window, tmp_path):
    _load(window, {"1D": _frame(), "1W": _frame()})
    window._on_rows_hide_requested(["TSLA"])
    keys = [k for _h, k, _f in window._ordered_active_columns_for_export()]
    path = tmp_path / "x.csv"
    window._write_csv_export(str(path), ["1D", "1W"], keys, False)
    df = pd.read_csv(path)
    assert "TSLA" not in set(df["Ticker"]) and len(df) == 6


def test_export_dialog_reoffers_hidden_columns_unticked(window, monkeypatch):
    from PyQt6.QtWidgets import QDialog
    from trade_scanner_fh.gui import exports as ex
    seen = {}

    class _Dlg:
        def __init__(self, columns, periods=None, parent=None, prechecked=None):
            seen["keys"] = [k for _h, k, _f in columns]
            seen["pre"] = prechecked

        def exec(self):
            return QDialog.DialogCode.Rejected

    monkeypatch.setattr(ex, "ExcelExportDialog", _Dlg)
    _load(window, {"1D": _frame()})
    window._on_columns_hide_requested(["avg_vol"])
    window._excel_export_dialog()
    assert "avg_vol" in seen["keys"] and "avg_vol" not in seen["pre"]
    assert "rvol" in seen["pre"]


# ======================================================================
# Presets — always match the incoming preset
# ======================================================================

def test_preset_saves_and_restores_hidden_rows(window, tmp_parquets):
    _load(window, {"1D": _frame()})
    window._on_rows_hide_requested(["NVDA"])
    window.preset_combo.addItem("p1")
    window.preset_combo.setCurrentText("p1")
    window._save_preset()
    data = json.loads((tmp_parquets / "presets" / "p1.json")
                      .read_text(encoding="utf-8"))
    assert data["row_hidden"] == ["NVDA"]

    window._unhide_all_rows()
    window._load_preset()
    assert window._hidden_row_symbols == {"NVDA"}
    _load(window, {"1D": _frame()})
    assert "NVDA" not in _shown(window)


def test_loading_a_preset_replaces_every_hide_set(window, tmp_parquets):
    (tmp_parquets / "presets" / "bare.json").write_text(json.dumps({
        "_preset_version": 6, "indicators": {}}), encoding="utf-8")
    _load(window, {"1D": _frame()})
    window._on_rows_hide_requested(["AAPL"])
    window._on_columns_hide_requested(["avg_vol"])
    window._hidden_earnings_col_types = {"q_reported_eps"}
    window._hidden_fv_col_types = {"fv_info"}
    window.preset_combo.addItem("bare")
    window.preset_combo.setCurrentText("bare")
    window._load_preset()
    assert window._hidden_row_symbols == set()
    assert window._deleted_column_keys == set()
    assert window._hidden_earnings_col_types == set()
    assert window._hidden_fv_col_types == set()
    assert window._hide_undo_stack == []
    assert window.results_table.hide_undo_available() is False


def test_preset_hides_replace_rather_than_add(window, tmp_parquets):
    (tmp_parquets / "presets" / "p2.json").write_text(json.dumps({
        "_preset_version": 7, "indicators": {},
        "row_hidden": ["TSLA"], "column_hidden": ["rvol"]}),
        encoding="utf-8")
    _load(window, {"1D": _frame()})
    window._on_rows_hide_requested(["AAPL"])
    window.preset_combo.addItem("p2")
    window.preset_combo.setCurrentText("p2")
    window._load_preset()
    assert window._hidden_row_symbols == {"TSLA"}
    assert window._deleted_column_keys == {"rvol"}


# ======================================================================
# Indicators
# ======================================================================

def test_ribbon_caption_and_menu(window):
    _load(window, {"1D": _frame()})
    assert window.btn_hidden_items.text() == "Hidden: none ▾"
    window._on_rows_hide_requested(["AAPL", "MSFT"])
    window._on_columns_hide_requested(["rvol"])
    assert window.btn_hidden_items.text() == "Hidden: 2 rows · 1 col ▾"
    window._rebuild_hidden_items_menu()
    texts = _texts(window._hidden_items_menu)
    assert "Unhide all rows (2)" in texts
    assert "Unhide all columns (1)" in texts
    assert "Undo hide\tCtrl+Z" in texts


def test_ribbon_menu_works_when_nothing_is_hidden(window):
    window._rebuild_hidden_items_menu()
    acts = window._hidden_items_menu.actions()
    assert len(acts) == 1 and not acts[0].isEnabled()


def test_timeframe_labels_count_hidden_rows(window):
    _load(window, {"1D": _frame(), "1W": _frame(("AAPL", "XOM"))})
    window._on_rows_hide_requested(["MSFT", "XOM"])
    assert window.combo_timeframe.itemText(0) == "1D  —  4 results (1 hidden)"
    assert window.combo_timeframe.itemText(1) == "1W  —  2 results (1 hidden)"
