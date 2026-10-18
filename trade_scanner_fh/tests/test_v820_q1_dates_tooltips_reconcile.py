"""v8.2.0 — "Q-1 Dates Only", V-column tooltips, plain-English reconcile.

User requests (2026-10-18) these tests pin:
  * a quick-select button that hides every Q-X Date column except Q-1's —
    built as a hide TYPE ("Q-X Dates except Q-1") so it covers any quarter
    depth, saves with the preset and is cleared by Show All;
  * the V columns explain themselves on hover (header and cell);
  * the reconcile's "z / y / aug" counters read as plain English.
"""
from __future__ import annotations

import json
import logging

import pandas as pd
import pytest
from PyQt6.QtCore import Qt

from trade_scanner_fh import earnings_cache as ec
from trade_scanner_fh import earnings_history as eh
from trade_scanner_fh import earnings_reconcile as er
from trade_scanner_fh.gui import widgets as W

T = pd.Timestamp
OLDER = W.OLDER_Q_DATES_TYPE


def _frame(n=3, **extra):
    row = {"symbol": "AAA", "close": 10.0, "pct_gain": 1.0,
           "gain_start_date": T("2026-09-01"), "consec_eps_beats": 3,
           "consec_rev_beats": 2}
    for k in range(1, n + 1):
        d = T("2026-08-01") - pd.DateOffset(months=3 * (k - 1))
        row.update({f"q{k}_report_date_eps": d, f"q{k}_reported_eps": 1.0,
                    f"q{k}_surprise_eps_dollar": 0.1,
                    f"q{k}_surprise_eps_pct": 5.0, f"q{k}_yoy_eps_pct": 10.0,
                    f"q{k}_report_date_rev": d, f"q{k}_reported_rev": 100.0,
                    f"q{k}_surprise_rev_dollar": 1.0,
                    f"q{k}_surprise_rev_pct": 2.0, f"q{k}_yoy_rev_pct": 8.0})
    row.update(extra)
    return pd.DataFrame([row])


def _keys(df, hidden=frozenset()):
    return [k for _h, k, _f in W._build_dynamic_columns(
        df, hidden_types=hidden)[0]]


# ======================================================================
# The hide type
# ======================================================================

def test_only_q2_and_later_dates_belong_to_the_type():
    assert W.earnings_column_types_of("q1_report_date_eps") == (
        "q_report_date_eps",)
    assert W.earnings_column_types_of("q1_report_date_rev") == (
        "q_report_date_rev",)
    assert W.earnings_column_types_of("q2_report_date_eps") == (
        "q_report_date_eps", OLDER)
    assert W.earnings_column_types_of("q17_report_date_rev") == (
        "q_report_date_rev", OLDER)
    for key in ("q2_reported_eps", "q3_yoy_rev_pct", "last_report_date",
                "accel_eps_yoy_start_date"):
        assert OLDER not in W.earnings_column_types_of(key), key


def test_hiding_it_keeps_q1_dates_and_every_metric():
    df = _frame(n=4)
    full = _keys(df)
    shown = _keys(df, {OLDER})
    gone = set(full) - set(shown)
    assert gone == {f"q{k}_report_date_{s}" for k in (2, 3, 4)
                    for s in ("eps", "rev")}
    assert {"q1_report_date_eps", "q1_report_date_rev"} <= set(shown)


def test_covers_any_depth_a_later_scan_draws():
    """Why a type and not a list of keys: a deeper scan is covered too."""
    shown = _keys(_frame(n=12), {OLDER})
    assert not [k for k in shown
                if k.startswith("q") and "_report_date_" in k
                and not k.startswith("q1_")]


def test_dropdown_lists_it_first_with_its_count():
    present = W.present_earnings_column_types(
        W._build_dynamic_columns(_frame(n=3))[0])
    assert present[0] == (OLDER, "Q-X Dates except Q-1", 4)   # Q-2..Q-3 × 2


def test_type_id_is_not_a_quarter_template():
    """The colour-rule editor turns every `q_` type into a `q{k}_<suffix>`
    column template; this one must not become `q{k}_...`."""
    assert not OLDER.startswith("q_")
    assert not OLDER.startswith("fv_")


# ======================================================================
# The button in the window
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


def test_button_hides_older_dates_and_a_second_click_restores(window):
    keys = _scan(window, _frame())
    assert "q2_report_date_eps" in keys
    window.btn_q1_dates_only.click()
    keys = [k for _h, k, _f in window.results_table.active_columns]
    assert "q2_report_date_eps" not in keys and "q3_report_date_rev" not in keys
    assert "q1_report_date_eps" in keys and "q2_reported_eps" in keys
    assert _menu(window)["Q-X Dates except Q-1"] is True
    assert "Q-X Dates except Q-1" in window.lbl_hidden_types.text()
    window.btn_q1_dates_only.click()
    assert "q2_report_date_eps" in [
        k for _h, k, _f in window.results_table.active_columns]
    assert OLDER not in window._hidden_earnings_col_types


def test_dropdown_and_show_all_move_the_button(window):
    _scan(window, _frame())
    window._on_hide_type_toggled(OLDER, True)
    assert window.btn_q1_dates_only.isChecked()
    window._on_show_all_column_types()
    assert not window.btn_q1_dates_only.isChecked()
    assert "q2_report_date_eps" in [
        k for _h, k, _f in window.results_table.active_columns]


def test_presets_remember_it_and_old_presets_leave_it_off(window, tmp_parquets):
    _scan(window, _frame())
    window.btn_q1_dates_only.click()
    window.preset_combo.addItem("p")
    window.preset_combo.setCurrentText("p")
    window._save_preset()
    data = json.loads((tmp_parquets / "presets" / "p.json")
                      .read_text(encoding="utf-8"))
    assert OLDER in data["hidden_earnings_col_types"]

    (tmp_parquets / "presets" / "old.json").write_text(json.dumps({
        "_preset_version": 7, "indicators": {}}), encoding="utf-8")
    window.preset_combo.addItem("old")
    window.preset_combo.setCurrentText("old")
    window._load_preset()
    assert not window.btn_q1_dates_only.isChecked()
    assert "q2_report_date_eps" in _scan(window, _frame())

    window.preset_combo.setCurrentText("p")
    window._load_preset()
    assert window.btn_q1_dates_only.isChecked()
    assert "q2_report_date_eps" not in _scan(window, _frame())


def test_quarter_dates_ticked_alone_still_hides_q1_too(window):
    """The existing whole-type tick is unchanged: it hides Q-1 as well."""
    _scan(window, _frame())
    window._on_hide_type_toggled("q_report_date_eps", True)
    keys = [k for _h, k, _f in window.results_table.active_columns]
    assert "q1_report_date_eps" not in keys
    assert not window.btn_q1_dates_only.isChecked()


# ======================================================================
# V tooltips
# ======================================================================

def test_every_v_column_has_a_header_tooltip_and_nothing_else_does():
    v_keys = {k for _h, k, _f in W.RESULT_COLUMNS if k.endswith("_vals")}
    assert v_keys == set(W.HEADER_TOOLTIPS)
    tip = W.header_tooltip("accel_rev_surp_vals")
    assert "Revenue surprise %" in tip and "oldest" in tip and "newest" in tip
    assert "Step pp" in tip
    grw = W.header_tooltip("consec_eps_growth_vals")
    assert "YoY EPS growth %" in grw and "Growth %" in grw
    assert "does not have to accelerate" in grw
    assert W.header_tooltip("accel_rev_surp_span") is None
    assert W.header_tooltip("symbol") is None


def test_table_headers_carry_it_and_a_relayout_clears_stale_tips(_qapp):
    t = W.ResultsTable()
    t.populate(_frame(n=1, accel_rev_surp_len=3,
                      accel_rev_surp_span="2025-09 -> 2026-03",
                      accel_rev_surp_vals="+3.00% -> +15.50%"))
    cols = [k for _h, k, _f in t.active_columns]
    tips = {k: t.model_src.headerData(i, Qt.Orientation.Horizontal,
                                      Qt.ItemDataRole.ToolTipRole)
            for i, k in enumerate(cols)}
    assert "Revenue surprise %" in tips["accel_rev_surp_vals"]
    assert all(v is None for k, v in tips.items()
               if k != "accel_rev_surp_vals")
    # Header reached through the proxy too (what QHeaderView asks).
    i = cols.index("accel_rev_surp_vals")
    assert t.proxy.headerData(i, Qt.Orientation.Horizontal,
                              Qt.ItemDataRole.ToolTipRole) == tips[
        "accel_rev_surp_vals"]
    # A different layout must not inherit the tip at that index.
    t.populate(_frame(n=3))
    cols2 = [k for _h, k, _f in t.active_columns]
    for j, k in enumerate(cols2):
        assert t.model_src.headerData(
            j, Qt.Orientation.Horizontal, Qt.ItemDataRole.ToolTipRole) is None, k


def test_v_cell_tooltip_explains_before_the_dates():
    row = {"accel_rev_surp_vals": "+3.00% -> +15.50%",
           "_accel_rev_surp_start_date": T("2025-11-05"),
           "_accel_rev_surp_end_date": T("2026-05-06")}
    tip = W.cell_tooltip("accel_rev_surp_vals", row)
    first, second = tip.split("\n")
    assert first.startswith("Revenue surprise %")
    assert second.startswith("Run · reported 2025-11-05")
    # Other run cells keep the dates-only tip.
    assert W.cell_tooltip("accel_rev_surp_span", row).startswith("Run · ")
    # An N/A V cell still explains itself.
    assert W.cell_tooltip("consec_rev_growth_vals", {}).startswith(
        "YoY revenue growth %")


# ======================================================================
# Plain-English reconcile
# ======================================================================

def test_describe_counts_has_no_bare_letters():
    text = er.describe_reconcile_counts(5120, 1840, 310)
    assert text.startswith("Earnings dates reconciled for 7,270 ticker(s):")
    assert "5,120 from past earnings reports (Finviz / Zacks / Finnhub)" in text
    assert "1,840 from the earnings calendars (Nasdaq / Yahoo)" in text
    assert "310 using both" in text
    assert "z=" not in text and "aug" not in text


def _hist(ticker, report, source="zacks"):
    return {"ticker": ticker, "period_ending": T(report) - pd.Timedelta(days=40),
            "report_date": T(report), "report_time": "Close",
            "estimated_eps": 2.0, "reported_eps": 2.1, "surprise_eps": 0.1,
            "surprise_eps_pct": 5.0, "estimated_rev": 100.0,
            "reported_rev": 105.0, "surprise_rev": 5.0,
            "surprise_rev_pct": 5.0, "source": source,
            "updated_at": T("2026-01-01")}


def test_reconcile_log_line_is_plain_english(tmp_parquets, caplog):
    eh.save_earnings_history(pd.DataFrame([_hist("AAA", "2026-01-29")]))
    ec.save_earnings_cache(pd.DataFrame([{
        "ticker": "BBB", "last_earnings": T("2026-02-02"),
        "next_earnings": T("2026-07-30"), "updated_at": T("2026-01-01"),
        "source": "nasdaq"}]))
    with caplog.at_level(logging.INFO, logger="scanner.earnings_reconcile"):
        z, y, aug = er.reconcile_earnings_dates(
            ["AAA", "BBB"], today=T("2026-04-30"))
    assert (z, y, aug) == (1, 1, 0)           # the counters are unchanged
    msgs = [r.getMessage() for r in caplog.records
            if r.name == "scanner.earnings_reconcile"]
    line = next(m for m in msgs if m.startswith("Earnings dates reconciled"))
    assert "1 from past earnings reports" in line
    assert "1 from the earnings calendars" in line
    assert "z=" not in line
