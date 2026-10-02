"""v8.0.1 — colour-rule favorites in the Color Rules dialog.

User decisions (2026-09-27) these tests pin:
  * a favorite is ONE rule, saved by right-clicking it; favorites are shared
    by every preset;
  * the Favorites dropdown adds a copy of a favorite to the regular rule list
    — it does NOT paint; Apply / OK paints, as with any edit;
  * a favorite the scan ON SCREEN cannot feed (a column its conditions read
    is missing) is greyed out and cannot be picked; before any scan, all are.
"""
from __future__ import annotations

import json

import pandas as pd
import pytest
from PyQt6.QtWidgets import QMenu

from trade_scanner_fh.gui import color_favorites as F
from trade_scanner_fh.gui import color_rules_dialog as D
from trade_scanner_fh.gui import coloring as C


@pytest.fixture(scope="module", autouse=True)
def qapp():
    from PyQt6.QtWidgets import QApplication
    return QApplication.instance() or QApplication([])


COLS = [("RVOL", "rvol", "num"), ("% Gain", "pct_gain", "num"),
        ("Close", "close", "num"),
        ("Consec EPS Beats", "consec_eps_beats", "num"),
        ("Q-X Surp EPS %", "q{k}_surprise_eps_pct", "num"),
        ("Q-X Date (EPS)", "q{k}_report_date_eps", "date")]
TARGETS = [("Q-X Reported EPS", "q_reported_eps"), ("RVOL", "rvol"),
           ("Close", "close"), ("% Gain", "pct_gain")]

SCAN = {"symbol", "close", "pct_gain", "rvol", "consec_eps_beats",
        "_consec_eps_beats_qs", "q1_reported_eps", "q2_reported_eps",
        "q1_surprise_eps_pct", "q2_surprise_eps_pct"}


def _leaders(rid="lead"):
    return C.Rule(id=rid, name="Leaders",
                  conditions=[C.Condition("value", "rvol", ">=", 2.0)],
                  target="row",
                  style=C.Style(background=C.ColorSpec("fixed", "#1b5e20"),
                                bold=True))


def _needs_beta():
    return C.Rule(id="beta", name="High beta",
                  conditions=[C.Condition("value", "beta_calc", ">", 2.0)],
                  target="matched",
                  style=C.Style(text=C.ColorSpec("fixed", "#ffd54f")))


def _dialog(rules=None, cols=SCAN):
    fn = (lambda: set(cols)) if cols is not None else (lambda: None)
    return D.ColorRulesDialog(rules or [_leaders()], COLS, TARGETS,
                              available_columns_fn=fn)


def _answer(dlg, name, confirm=True):
    dlg._ask_text = lambda title, label, default="": name
    dlg._confirm = lambda title, text: confirm


# ======================================================================
# Store
# ======================================================================

def test_store_round_trip_sorted_and_case_insensitive():
    favs = F.upsert([], "zeta", _leaders())
    favs = F.upsert(favs, "Alpha", _needs_beta())
    assert F.save_favorites(favs)
    back = F.load_favorites()
    assert [f.name for f in back] == ["Alpha", "zeta"]
    assert back[1].rule.to_dict() == _leaders().to_dict()
    assert F.find(back, "ALPHA").name == "Alpha"
    replaced = F.upsert(back, "ZETA", _needs_beta())
    assert [f.name for f in replaced] == ["Alpha", "ZETA"]


def test_rename_and_delete():
    favs = F.upsert(F.upsert([], "a", _leaders()), "b", _needs_beta())
    with pytest.raises(ValueError):
        F.rename(favs, "a", "B")                   # clash, any case
    with pytest.raises(ValueError):
        F.rename(favs, "a", "  ")
    favs = F.rename(favs, "a", "A2")
    assert [f.name for f in favs] == ["A2", "b"]
    assert [f.name for f in F.delete(favs, "B")] == ["A2"]


def test_a_favorite_is_a_copy_and_comes_back_as_a_fresh_rule():
    rule = _leaders()
    favs = F.upsert([], "Mine", rule)
    rule.conditions[0].value = 99.0                # edit after saving
    assert favs[0].rule.conditions[0].value == 2.0
    new = F.instantiate(favs[0])
    assert new.id != rule.id and new.enabled and new.name == "Mine"
    assert F.signature(new) == F.signature(favs[0].rule)


def test_unreadable_files_and_entries(monkeypatch, tmp_path):
    path = tmp_path / "f.json"
    monkeypatch.setattr(F, "_favorites_path", lambda: path)
    assert F.load_favorites() == []
    path.write_text("{ nope", encoding="utf-8")
    assert F.load_favorites() == []
    path.write_text(json.dumps({"favorites": [
        {"name": "ok", "rule": _leaders().to_dict()},
        {"name": "", "rule": _leaders().to_dict()},
        {"name": "OK", "rule": _needs_beta().to_dict()},      # duplicate name
        "garbage"]}), encoding="utf-8")
    assert [f.name for f in F.load_favorites()] == ["ok"]


# ======================================================================
# What a rule needs from a scan
# ======================================================================

def test_value_rule_needs_its_columns():
    assert C.unmet_requirements(_leaders(), SCAN) == ([], [])
    assert C.unmet_requirements(_needs_beta(), SCAN) == (["beta_calc"], [])


def test_run_membership_needs_the_series_filter():
    streak = next(r for r in C.default_rules()
                  if r.id == "default_eps_streak")
    growth = next(r for r in C.default_rules()
                  if r.id == "default_eps_growth_run")
    assert C.unmet_requirements(streak, SCAN) == ([], [])
    assert C.unmet_requirements(growth, SCAN)[0] == ["_consec_eps_growth_qs"]


def test_quarter_templates_match_any_quarter():
    rule = C.Rule(scope="quarter", conditions=[
        C.Condition("value", "q{k}_surprise_eps_pct", ">", 5.0)],
        target="columns", target_columns=["q_reported_eps"],
        style=C.Style(text=C.ColorSpec("fixed", "#ffffff")))
    assert C.unmet_requirements(rule, SCAN) == ([], [])
    no_q = {"symbol", "close", "rvol"}
    assert C.unmet_requirements(rule, no_q) == (
        ["q{k}_surprise_eps_pct"], ["q_reported_eps"])
    # All inputs present but nothing to test per quarter: the generic reason.
    run_only = C.Rule(scope="quarter", conditions=[
        C.Condition("quarter", op="<=", other="consec_eps_beats")],
        style=C.Style(text=C.ColorSpec("fixed", "#ffffff")))
    assert C.unmet_requirements(run_only, {"consec_eps_beats"})[0] == \
        [C.ANY_QUARTER]


def test_scan_wide_tests_need_nothing():
    for rid in ("default_earnings_match", "default_display_only_fail"):
        rule = next(r for r in C.default_rules() if r.id == rid)
        assert C.unmet_requirements(rule, {"symbol"}) == ([], [])


def test_chosen_columns_need_only_one_present():
    rule = _leaders()
    rule.target, rule.target_columns = "columns", ["beta_calc", "close"]
    assert C.unmet_requirements(rule, SCAN) == ([], [])
    rule.target_columns = ["beta_calc", "hv"]
    assert C.unmet_requirements(rule, SCAN) == ([], ["beta_calc", "hv"])


# ======================================================================
# Dialog
# ======================================================================

def test_save_as_favorite_prompts_with_the_rule_name():
    dlg = _dialog()
    asked = {}

    def ask(title, label, default=""):
        asked["default"] = default
        return "My leaders"
    dlg._ask_text = ask
    assert dlg.save_rule_as_favorite(0) == "My leaders"
    assert asked["default"] == "Leaders"
    (fav,) = F.load_favorites()
    assert fav.name == "My leaders" and fav.rule.to_dict() == _leaders().to_dict()
    assert "Saved" in dlg.fav_note.text()


def test_save_honours_cancel_blank_and_a_declined_overwrite():
    dlg = _dialog()
    _answer(dlg, None)
    assert dlg.save_rule_as_favorite(0) is None
    _answer(dlg, "   ")
    assert dlg.save_rule_as_favorite(0) is None
    assert F.load_favorites() == []
    F.save_favorites(F.upsert([], "Leaders", _needs_beta()))
    _answer(dlg, "leaders", confirm=False)
    assert dlg.save_rule_as_favorite(0) is None
    assert F.load_favorites()[0].rule.id == "beta", "declined: untouched"
    _answer(dlg, "leaders", confirm=True)
    assert dlg.save_rule_as_favorite(0) == "leaders"
    (fav,) = F.load_favorites()
    assert fav.rule.id == "lead"


def test_save_takes_unapplied_edits_in_the_editor():
    dlg = _dialog()
    dlg.editor.name.setText("Edited name")
    _answer(dlg, "x")
    dlg.save_rule_as_favorite(0)
    assert F.load_favorites()[0].rule.name == "Edited name"


def test_right_click_on_a_rule_offers_save_as_favorite(monkeypatch):
    dlg = _dialog([_needs_beta(), _leaders()])
    dlg.show()
    _answer(dlg, "From the menu")
    shown = []

    def fake_exec(menu, *a, **k):
        shown.append([a.text() for a in menu.actions()])
        next(a for a in menu.actions()
             if a.text().startswith("Save as favorite")).trigger()
    monkeypatch.setattr(QMenu, "exec", fake_exec)
    rect = dlg.list.visualItemRect(dlg.list.item(1))
    dlg._rule_context_menu(rect.center())
    assert shown == [["Save as favorite…"]]
    assert dlg.list.currentRow() == 1, "right-click selects the rule"
    assert F.load_favorites()[0].rule.id == "lead"
    dlg.close()


def _menu_entries(dlg):
    dlg._populate_favorites_menu()
    return {a.text(): a for a in dlg.fav_menu.actions() if a.text()}


def test_menu_greys_out_what_the_scan_cannot_feed():
    F.save_favorites(F.upsert(F.upsert([], "Leaders", _leaders()),
                              "High beta", _needs_beta()))
    dlg = _dialog()
    entries = _menu_entries(dlg)
    assert entries["Leaders"].isEnabled()
    (beta_text,) = [t for t in entries if t.startswith("High beta")]
    assert not entries[beta_text].isEnabled()
    assert "needs beta_calc" in beta_text
    assert "Not available" in entries[beta_text].toolTip()
    assert entries["Manage favorites…"].isEnabled()
    assert entries["Save selected rule as favorite…"].isEnabled()


def test_reasons_name_inputs_first_then_targets_and_stay_short():
    dlg = _dialog()
    no_target = _leaders()
    no_target.target, no_target.target_columns = "columns", ["beta_calc"]
    ok, why = dlg.favorite_availability(F.Favorite("t", no_target))
    assert not ok and why.startswith("none of its colored columns")
    many = C.Rule(name="m", conditions=[
        C.Condition("value", k, ">", 1.0)
        for k in ("hv", "yz_vol", "atr_pct", "beta_calc", "hv_rank")],
        target="columns", target_columns=["nope"],
        style=C.Style(bold=True))
    ok, why = dlg.favorite_availability(F.Favorite("m", many))
    assert why == "needs hv, yz_vol, atr_pct +2 more", why


def test_menu_before_any_scan_greys_everything():
    F.save_favorites(F.upsert([], "Leaders", _leaders()))
    entries = _menu_entries(_dialog(cols=None))
    (text,) = [t for t in entries if t.startswith("Leaders")]
    assert not entries[text].isEnabled() and "run a scan first" in text


def test_menu_with_no_favorites():
    entries = _menu_entries(_dialog())
    empty = [a for t, a in entries.items() if t.startswith("No favorites")]
    assert empty and not empty[0].isEnabled()
    assert not entries["Manage favorites…"].isEnabled()


def test_the_menu_is_rebuilt_against_the_scan_each_time_it_opens():
    """A scan that finishes while the dialog is open is picked up."""
    F.save_favorites(F.upsert([], "High beta", _needs_beta()))
    scan = set(SCAN)
    dlg = D.ColorRulesDialog([_leaders()], COLS, TARGETS,
                             available_columns_fn=lambda: scan)
    assert not any(a.isEnabled() for t, a in _menu_entries(dlg).items()
                   if t.startswith("High beta"))
    scan.add("beta_calc")
    assert _menu_entries(dlg)["High beta"].isEnabled()


def test_picking_a_favorite_adds_it_without_painting():
    """User decision: it lands in the regular list; Apply paints."""
    F.save_favorites(F.upsert([], "Big RVOL", _leaders("fav_src")))
    dlg = _dialog([_needs_beta(), C.default_rules()[0]])
    emitted = []
    dlg.rules_applied.connect(emitted.append)
    dlg.list.setCurrentRow(1)
    _menu_entries(dlg)["Big RVOL"].trigger()
    rules = dlg.rules()
    assert [r.name for r in rules] == ["High beta", "Big RVOL",
                                       "Earnings date match"]
    added = rules[1]
    assert added.id != "fav_src" and added.enabled
    assert dlg.list.currentRow() == 1
    assert emitted == [], "nothing painted until Apply"
    assert "press Apply" in dlg.fav_note.text()
    dlg.apply()
    assert [r.name for r in emitted[0]][1] == "Big RVOL"


def test_picking_a_favorite_already_listed_selects_it():
    F.save_favorites(F.upsert([], "Leaders copy", _leaders("other_id")))
    dlg = _dialog([_needs_beta(), _leaders()])
    assert dlg.apply_favorite("Leaders copy") == 1
    assert len(dlg.rules()) == 2
    assert "already in the list" in dlg.fav_note.text()


def test_an_unavailable_favorite_cannot_be_added_even_directly():
    F.save_favorites(F.upsert([], "High beta", _needs_beta()))
    dlg = _dialog([_leaders()])
    assert dlg.apply_favorite("High beta") is None
    assert len(dlg.rules()) == 1


def test_manage_dialog_renames_and_deletes():
    F.save_favorites(F.upsert(F.upsert([], "a", _leaders()), "b",
                              _needs_beta()))
    m = D.ManageFavoritesDialog()
    m.list.setCurrentRow(0)
    m._ask_text = lambda title, label, default="": "renamed"
    m.rename_selected()
    assert [f.name for f in F.load_favorites()] == ["b", "renamed"]
    assert m.list.currentItem().text() == "renamed"
    m._confirm = lambda title, text: False
    m.delete_selected()
    assert len(F.load_favorites()) == 2
    m._confirm = lambda title, text: True
    m.delete_selected()
    assert [f.name for f in F.load_favorites()] == ["b"]


def test_manage_dialog_refuses_a_clashing_name(monkeypatch):
    F.save_favorites(F.upsert(F.upsert([], "a", _leaders()), "b",
                              _needs_beta()))
    m = D.ManageFavoritesDialog()
    m.list.setCurrentRow(0)
    warned = []
    monkeypatch.setattr(D.QMessageBox, "warning",
                        lambda *a, **k: warned.append(a[-1]))
    m._ask_text = lambda title, label, default="": "B"
    m.rename_selected()
    assert warned and [f.name for f in F.load_favorites()] == ["a", "b"]


# ======================================================================
# Window
# ======================================================================

@pytest.fixture
def window(tmp_parquets, monkeypatch):
    from trade_scanner_fh.gui import main_window as mw_mod
    monkeypatch.setattr(mw_mod, "PRESETS_DIR", tmp_parquets / "presets")
    (tmp_parquets / "presets").mkdir(exist_ok=True)
    w = mw_mod.MainWindow()
    yield w
    w.close()
    w.deleteLater()


def _frame():
    return pd.DataFrame([
        {"symbol": s, "close": c, "pct_gain": g, "rvol": r,
         "gain_start_date": pd.Timestamp("2026-01-02")}
        for s, c, g, r in (("AAA", 10.0, 20.0, 3.0), ("BBB", 5.0, 2.0, 1.0),
                           ("CCC", 7.0, 9.0, 2.5))])


def _load(w, periods):
    w._period_results = dict(periods)
    w._period_order = list(periods)
    w._active_period = w._period_order[0]
    w.combo_timeframe.blockSignals(True)
    w.combo_timeframe.clear()
    for label in periods:
        w.combo_timeframe.addItem(label, userData=label)
    w.combo_timeframe.blockSignals(False)
    w._on_timeframe_changed(0)


def _cell(w, sym, key):
    t = w.results_table
    keys = [k for _h, k, _f in t.active_columns]
    c, sc = keys.index(key), keys.index("symbol")
    for r in range(t.model_src.rowCount()):
        if t.model_src.item(r, sc).text() == sym:
            return t.model_src.item(r, c)
    raise AssertionError(sym)


def test_window_offers_the_scan_on_screen(window):
    assert window._color_rule_available_columns() is None
    _load(window, {"1D": _frame()})
    window._on_columns_hide_requested(["rvol"])
    cols = window._color_rule_available_columns()
    assert {"close", "rvol"} <= cols, "hidden columns still count"


def test_end_to_end_save_pick_apply_across_presets(window, tmp_parquets):
    """Save a rule as a favorite, switch to a preset without it, add it back
    from the dropdown, and only Apply paints the table."""
    _load(window, {"1D": _frame()})
    window._apply_color_rules([_leaders()])
    window._open_color_rules_dialog()
    dlg = window._color_rules_dialog
    _answer(dlg, "Leaders fav")
    dlg.save_rule_as_favorite(0)

    (tmp_parquets / "presets" / "p.json").write_text(json.dumps({
        "_preset_version": 7, "indicators": {},
        "color_rules": C.rules_to_json([])}), encoding="utf-8")
    window.preset_combo.addItem("p")
    window.preset_combo.setCurrentText("p")
    window._load_preset()
    assert dlg.rules() == []
    _load(window, {"1D": _frame()})
    assert _cell(window, "AAA", "close").background().color().name() \
        != "#1b5e20"

    _menu_entries(dlg)["Leaders fav"].trigger()
    assert [r.name for r in dlg.rules()] == ["Leaders fav"]
    assert _cell(window, "AAA", "close").background().color().name() \
        != "#1b5e20", "picking does not paint"
    dlg.apply()
    assert _cell(window, "AAA", "close").background().color().name() \
        == "#1b5e20"
    assert _cell(window, "BBB", "close").background().color().name() \
        != "#1b5e20", "rvol 1.0 fails the rule"
    dlg.close()
