"""v8.0.2 — a date column for every earnings filter, and hover tooltips.

User decisions (2026-10-04) these tests pin:
  * every earnings filter produces a date column; only the Consecutive EPS /
    Rev Beats rows show theirs (the Q-X Dates) by default — every other one
    is HIDDEN by default through Hide Q Columns;
  * YoY Growth and Accelerating rows get a Start + End report-date pair for
    their run; the Current EPS / Rev rows always produce Last Report Date
    (it used to vanish beside Q-X blocks); Days Since / Until ER get none;
  * one Hide Q Columns tick per filter's dates; unticking shows them, and
    presets remember it;
  * hovering an earnings cell shows the quarter's report date and fiscal
    quarter (a run's cells: first -> last).
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from trade_scanner_fh import data_engine
from trade_scanner_fh import scanner as S
from trade_scanner_fh.gui import widgets as W

T = pd.Timestamp

# Newest first, the way the scanner's past_pref arrives. YoY EPS rises
# 1 -> 5 -> 10 -> 20 -> 30 -> 40 in fiscal order.
HIST = pd.DataFrame({
    "period_ending": pd.to_datetime([
        "2026-03-01", "2025-12-01", "2025-09-01", "2025-06-01",
        "2025-03-01", "2024-12-01"]),
    "report_date": pd.to_datetime([
        "2026-05-05", "2026-02-10", "2025-11-04", "2025-08-05",
        "2025-05-06", "2025-02-11"]),
    "reported_eps": [1.0, 0.9, 0.8, 0.7, 0.6, 0.5],
    "surprise_eps": 0.1, "surprise_eps_pct": 5.0,
    "yoy_eps_pct": [40.0, 30.0, 20.0, 10.0, 5.0, 1.0],
    "reported_rev": 100.0, "surprise_rev": 1.0, "surprise_rev_pct": 1.0,
    "yoy_rev_pct": np.nan,
})

DATE_TYPES = {"dates_last_report", "dates_consec_eps_growth",
              "dates_consec_rev_growth", "dates_accel_eps_surp",
              "dates_accel_rev_surp", "dates_accel_eps_yoy",
              "dates_accel_rev_yoy"}


# ======================================================================
# Scanner — the new columns and the tooltip keys
# ======================================================================

def test_growth_and_accelerating_rows_write_their_runs_report_dates():
    row: dict = {}
    S._populate_quarter_series(row, S.ScanParams(
        consec_eps_growth_enabled=True, consec_eps_growth_quarter_cap=4,
        accel_eps_yoy_enabled=True, accel_eps_yoy_min_step_pct=5.0,
        accel_eps_yoy_min_count=3), HIST)
    # Growth over the 4 newest quarters: 2025-06 .. 2026-03.
    assert row["consec_eps_growth_start_date"] == T("2025-08-05")
    assert row["consec_eps_growth_end_date"] == T("2026-05-05")
    assert row["_consec_eps_growth_start_period"] == T("2025-06-01")
    assert row["_consec_eps_growth_end_period"] == T("2026-03-01")
    # Accelerating: 1 -> 5 is a +4 step, so the series starts at 5.
    assert row["accel_eps_yoy_start_date"] == T("2025-05-06")
    assert row["accel_eps_yoy_end_date"] == T("2026-05-05")


def test_no_run_still_writes_the_date_columns_empty():
    row: dict = {}
    S._populate_quarter_series(row, S.ScanParams(
        consec_eps_growth_enabled=True, consec_eps_growth_threshold_pct=1e6,
        accel_rev_yoy_enabled=True), HIST)
    assert row["consec_eps_growth"] == 0
    assert row["consec_eps_growth_start_date"] is None
    assert row["consec_eps_growth_end_date"] is None
    # Rev YoY is all NaN: no series at all, but the columns still exist.
    assert row["accel_rev_yoy_start_date"] is None
    assert row["accel_rev_yoy_end_date"] is None


def _ticker_row(tmp_path, monkeypatch, **kw):
    """`_compute_ticker` over HIST with only the filters in `kw` on."""
    days = pd.bdate_range("2026-01-02", "2026-09-18")
    ohlcv = pd.DataFrame({"Open": 10.0, "High": 10.5, "Low": 9.5,
                          "Close": 10.0, "Volume": 1e6}, index=days)
    ohlcv.index.name = "Date"
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    data_engine.clear_ohlcv_cache()
    ohlcv.to_parquet(tmp_path / "TKR.parquet")
    p = S.ScanParams(start_date=T("2026-09-01").date(),
                     end_date=T("2026-09-18").date(), **kw)
    for f in S.ScanParams.__dataclass_fields__:
        if f.endswith("_enabled") and getattr(p, f) and f not in kw:
            setattr(p, f, False)
    return S._compute_ticker(
        "TKR", p, earnings_history_lookup={"TKR": HIST},
        earnings_lookup={"TKR": (T("2026-05-05"), T("2026-11-03"))})


def test_last_report_date_is_produced_beside_beats_blocks(tmp_path,
                                                          monkeypatch):
    row = _ticker_row(tmp_path, monkeypatch, reported_eps_display_only=True,
                      consec_eps_beats_display_only=True,
                      consec_eps_beats_min=0)
    assert row["last_report_date"] == T("2026-05-05")
    assert row["_last_period_ending"] == T("2026-03-01")
    assert row["q1_report_date_eps"] == T("2026-05-05")
    assert row["_q1_period_ending"] == T("2026-03-01")
    # Beats show their dates in the Q-X blocks; the run's dates are for the
    # tooltip only — no visible Start / End column.
    assert "consec_eps_beats_start_date" not in row
    assert row["_consec_eps_beats_end_date"] == T("2026-05-05")
    assert row["_consec_eps_beats_end_period"] == T("2026-03-01")


def test_days_since_and_until_carry_their_dates_for_the_tooltip_only(
        tmp_path, monkeypatch):
    row = _ticker_row(tmp_path, monkeypatch,
                      days_since_earnings_display_only=True,
                      days_until_earnings_display_only=True)
    assert row["_last_er_date"] == T("2026-05-05")
    assert row["_next_er_date"] == T("2026-11-03")
    assert not any(k in row for k in ("last_er_date", "next_er_date"))


# ======================================================================
# Hide Q Columns types
# ======================================================================

def test_one_dates_tick_per_non_beats_filter_all_hidden_by_default():
    assert set(W.DEFAULT_HIDDEN_EARNINGS_TYPES) == DATE_TYPES
    labels = W.EARNINGS_COLUMN_TYPE_LABELS
    assert labels["dates_accel_eps_surp"] == "Accel EPS Surp Dates"
    assert labels["dates_last_report"] == "Last Report Date"
    # The beats rows' dates are the Q-X Date types, never default-hidden.
    assert not {"q_report_date_eps", "q_report_date_rev",
                "series_consec_eps_beats"} & W.DEFAULT_HIDDEN_EARNINGS_TYPES


def test_a_run_date_has_two_ticks_and_either_hides_it():
    key = "accel_eps_yoy_start_date"
    assert W.earnings_column_types_of(key) == (
        "series_accel_eps_yoy", "dates_accel_eps_yoy")
    assert W.is_type_hidden(key, {"dates_accel_eps_yoy"})
    assert W.is_type_hidden(key, {"series_accel_eps_yoy"})
    assert not W.is_type_hidden(key, {"dates_accel_rev_yoy"})
    assert W.earnings_column_types_of("last_report_date") == \
        ("dates_last_report",)


def _all_frame(n=2):
    row = {"symbol": "AAA", "close": 10.0, "pct_gain": 1.0,
           "gain_start_date": T("2026-09-01"), "reported_eps": 1.0,
           "last_report_date": T("2026-05-05"), "consec_eps_beats": 3,
           "consec_eps_growth": 4, "consec_eps_growth_span": "s",
           "consec_eps_growth_vals": "v",
           "consec_eps_growth_start_date": T("2025-08-05"),
           "consec_eps_growth_end_date": T("2026-05-05"),
           "accel_eps_yoy_len": 5, "accel_eps_yoy_span": "s",
           "accel_eps_yoy_vals": "v",
           "accel_eps_yoy_start_date": T("2025-05-06"),
           "accel_eps_yoy_end_date": T("2026-05-05")}
    for k in range(1, n + 1):
        row.update({f"q{k}_report_date_eps": HIST["report_date"][k - 1],
                    f"q{k}_reported_eps": 1.0,
                    f"q{k}_surprise_eps_dollar": 0.1,
                    f"q{k}_surprise_eps_pct": 5.0,
                    f"q{k}_yoy_eps_pct": 10.0,
                    f"_q{k}_period_ending": HIST["period_ending"][k - 1]})
    return pd.DataFrame([row])


def test_default_hiding_removes_only_the_new_date_columns():
    df = _all_frame()
    full = [k for _h, k, _f in W._build_dynamic_columns(df)[0]]
    shown = [k for _h, k, _f in W._build_dynamic_columns(
        df, hidden_types=W.DEFAULT_HIDDEN_EARNINGS_TYPES)[0]]
    dates = {"last_report_date", "consec_eps_growth_start_date",
             "consec_eps_growth_end_date", "accel_eps_yoy_start_date",
             "accel_eps_yoy_end_date"}
    assert dates <= set(full)
    assert shown == [k for k in full if k not in dates]
    assert "q1_report_date_eps" in shown, "beats' Q-X Dates stay visible"


def test_the_menu_counts_a_run_date_under_both_of_its_ticks():
    counts = {t: n for t, _l, n in W.present_earnings_column_types(
        W._build_dynamic_columns(_all_frame())[0])}
    assert counts["dates_accel_eps_yoy"] == 2
    # Q, Span, V, Start, End — the frame carries no Avg / Max.
    assert counts["series_accel_eps_yoy"] == 5
    assert counts["dates_last_report"] == 1


def test_run_dates_take_part_in_date_matching():
    assert W._anchor_date_value("accel_eps_yoy_end_date",
                                {"accel_eps_yoy_end_date": T("2026-05-05")}) \
        == T("2026-05-05")
    assert W._is_earnings_anchor_key("consec_rev_growth_start_date")


# ======================================================================
# The window — default hidden, untick to show, presets, Show All, export
# ======================================================================

@pytest.fixture
def window(_qapp, tmp_parquets, monkeypatch):
    from trade_scanner_fh.gui import main_window as mw_mod
    monkeypatch.setattr(mw_mod, "PRESETS_DIR", tmp_parquets / "presets")
    (tmp_parquets / "presets").mkdir(exist_ok=True)
    monkeypatch.setattr(mw_mod.scan_history, "record_scan_results",
                        lambda *a, **k: {})
    w = mw_mod.MainWindow()
    yield w
    w.close()
    w.deleteLater()


def _scan(window, df):
    from trade_scanner_fh.gui.workers import WorkerScanResult
    window._on_scan_done(WorkerScanResult(period_results={"1D": df},
                                          period_order=["1D"]))
    return [k for _h, k, _f in window.results_table.active_columns]


def _menu(window):
    window._rebuild_hide_types_menu()
    return {a.text().rsplit("  (", 1)[0]: a.isChecked()
            for a in window._hide_types_menu.actions()}


def test_a_fresh_window_hides_every_new_date_column(window):
    assert window.btn_hide_col_types.text() == "Hide Q Columns ▾", \
        "nothing on screen yet, so no count"
    keys = _scan(window, _all_frame())
    for k in ("last_report_date", "consec_eps_growth_start_date",
              "accel_eps_yoy_end_date"):
        assert k not in keys
    assert "q1_report_date_eps" in keys
    menu = _menu(window)
    assert menu["Accel YoY EPS Dates"] is True
    assert menu["Last Report Date"] is True
    assert menu["Q-X Date (EPS)"] is False
    assert window.btn_hide_col_types.text() == "Hide Q Columns (3) ▾"


def test_unticking_shows_them_and_presets_remember(window, tmp_parquets):
    _scan(window, _all_frame())
    window._on_hide_type_toggled("dates_accel_eps_yoy", False)
    keys = [k for _h, k, _f in window.results_table.active_columns]
    assert {"accel_eps_yoy_start_date", "accel_eps_yoy_end_date"} <= set(keys)
    assert "consec_eps_growth_start_date" not in keys

    window.preset_combo.addItem("p")
    window.preset_combo.setCurrentText("p")
    window._save_preset()
    data = json.loads((tmp_parquets / "presets" / "p.json")
                      .read_text(encoding="utf-8"))
    assert data["shown_default_earnings_col_types"] == ["dates_accel_eps_yoy"]

    # A preset without the key (any pre-8.0.2 one) hides them again.
    (tmp_parquets / "presets" / "old.json").write_text(json.dumps({
        "_preset_version": 7, "indicators": {}}), encoding="utf-8")
    window.preset_combo.addItem("old")
    window.preset_combo.setCurrentText("old")
    window._load_preset()
    assert window._shown_default_earnings_col_types == set()
    assert "accel_eps_yoy_start_date" not in _scan(window, _all_frame())

    window.preset_combo.setCurrentText("p")
    window._load_preset()
    assert "accel_eps_yoy_start_date" in _scan(window, _all_frame())


def test_ticking_again_hides_and_all_cols_beats_a_shown_dates_tick(window):
    _scan(window, _all_frame())
    window._on_hide_type_toggled("dates_accel_eps_yoy", False)
    window._on_hide_type_toggled("series_accel_eps_yoy", True)
    keys = [k for _h, k, _f in window.results_table.active_columns]
    assert "accel_eps_yoy_start_date" not in keys, \
        "(all cols) hides the filter's dates too"
    window._on_hide_type_toggled("series_accel_eps_yoy", False)
    window._on_hide_type_toggled("dates_accel_eps_yoy", True)
    assert "accel_eps_yoy_start_date" not in [
        k for _h, k, _f in window.results_table.active_columns]
    assert "dates_accel_eps_yoy" not in window._shown_default_earnings_col_types


def test_show_all_shows_the_dates_too(window):
    _scan(window, _all_frame())
    window._on_show_all_column_types()
    keys = [k for _h, k, _f in window.results_table.active_columns]
    assert {"last_report_date", "consec_eps_growth_start_date",
            "accel_eps_yoy_end_date"} <= set(keys)
    assert window.btn_hide_col_types.text() == "Hide Q Columns ▾"


def test_export_reoffers_the_hidden_dates_unticked(window, monkeypatch):
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
    _scan(window, _all_frame())
    window._excel_export_dialog()
    assert "accel_eps_yoy_start_date" in seen["keys"]
    assert "accel_eps_yoy_start_date" not in seen["pre"]
    assert "q1_report_date_eps" in seen["pre"]


def test_the_unhide_menu_names_the_dropdown_for_a_default_hidden_date(window):
    _scan(window, _all_frame())
    window._on_columns_hide_requested(["last_report_date"])
    items = {label: enabled for label, _k, enabled
             in window._hide_mgr.column_unhide_items()}
    assert items["Last Report Date  (hidden by Hide Q Columns)"] is False


# ======================================================================
# Hover tooltip
# ======================================================================

def test_tooltip_text_for_each_kind_of_earnings_cell():
    row = _all_frame().iloc[0].to_dict()
    row.update({"_last_period_ending": T("2026-03-01"),
                "_accel_eps_yoy_start_date": T("2025-05-06"),
                "_accel_eps_yoy_end_date": T("2026-05-05"),
                "_accel_eps_yoy_start_period": T("2025-03-01"),
                "_accel_eps_yoy_end_period": T("2026-03-01"),
                "_consec_eps_beats_start_date": T("2025-11-04"),
                "_consec_eps_beats_end_date": T("2026-05-05"),
                "_last_er_date": T("2026-05-05"),
                "_next_er_date": T("2026-11-03")})
    tip = W.cell_tooltip
    assert tip("q2_surprise_eps_pct", row) == \
        "Q-2 · reported 2026-02-10 · fiscal quarter 2025-12"
    assert tip("reported_eps", row) == \
        "Most recent quarter · reported 2026-05-05 · fiscal quarter 2026-03"
    assert tip("last_report_date", row) == tip("reported_eps", row)
    assert tip("accel_eps_yoy_vals", row) == (
        "Run · reported 2025-05-06 → 2026-05-05 · "
        "fiscal quarters 2025-03 → 2026-03")
    # The visible Start / End columns stand in for the internal keys.
    assert tip("consec_eps_growth", row) == \
        "Run · reported 2025-08-05 → 2026-05-05"
    assert tip("consec_eps_beats", row) == \
        "Run · reported 2025-11-04 → 2026-05-05"
    assert tip("days_since_er", row) == "Last earnings report 2026-05-05"
    assert tip("days_until_er", row) == "Next earnings report 2026-11-03"
    for key in ("close", "symbol", "pct_gain", "gain_start_date"):
        assert tip(key, row) is None


def test_tooltip_falls_back_and_stays_quiet_without_a_date():
    assert W.cell_tooltip("q3_reported_eps", {}) is None
    assert W.cell_tooltip("reported_rev", {
        "q1_report_date_rev": T("2026-05-05"),
        "_q1_period_ending": T("2026-03-01")}) == \
        "Most recent quarter · reported 2026-05-05 · fiscal quarter 2026-03"
    assert W.cell_tooltip("accel_rev_surp_len", {
        "accel_rev_surp_start_date": None}) is None
    assert W.cell_tooltip("q1_report_date_eps", {
        "q1_report_date_eps": pd.NaT}) is None


def test_the_table_answers_hover_for_the_row_under_the_mouse(_qapp,
                                                             monkeypatch):
    """Real table, sorted so view row != frame row: the tooltip must come
    from the row actually under the cursor."""
    from PyQt6.QtCore import QEvent, QPoint
    from PyQt6.QtGui import QHelpEvent
    df = pd.concat([_all_frame(), _all_frame()], ignore_index=True)
    df.loc[0, "symbol"], df.loc[1, "symbol"] = "AAA", "ZZZ"
    df.loc[1, "q1_report_date_eps"] = T("2026-06-30")
    t = W.ResultsTable()
    t.resize(1600, 400)
    t.show()
    t.populate(df)
    t.sortByColumn(0, W.Qt.SortOrder.DescendingOrder)   # ZZZ first
    col = [k for _h, k, _f in t.active_columns].index("q1_reported_eps")
    rect = t.visualRect(t.proxy.index(0, col))
    assert t.proxy.index(0, 0).data() == "ZZZ"
    assert t._tooltip_at(rect.center()) == \
        "Q-1 · reported 2026-06-30 · fiscal quarter 2026-03"

    shown = []
    monkeypatch.setattr(W, "QToolTip", type("TT", (), {
        "showText": staticmethod(lambda _p, text, _w=None: shown.append(text)),
        "hideText": staticmethod(lambda: shown.append(None))}))
    ev = QHelpEvent(QEvent.Type.ToolTip, rect.center(),
                    t.viewport().mapToGlobal(rect.center()))
    assert t.viewportEvent(ev) is True
    assert shown == ["Q-1 · reported 2026-06-30 · fiscal quarter 2026-03"]
    close_col = [k for _h, k, _f in t.active_columns].index("close")
    ev2 = QHelpEvent(QEvent.Type.ToolTip,
                     t.visualRect(t.proxy.index(0, close_col)).center(),
                     QPoint(0, 0))
    assert t.viewportEvent(ev2) is True and shown[-1] is None
    t.close()
