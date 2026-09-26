"""v8.0.0 phase 1 — the eight bug fixes and the Hide FV Columns dropdown.

Reported:
  1. every FV column showed whether or not its row was on;
  2. deleting a column blanked its values but left the column;
  3. the Finviz Additional spinbox ranges were not scaled to their fields
     (and "Income >= 0" could not be expressed at all);
  4. Beta (calc) never appeared.
Found while diagnosing those:
  5. an RS row in Display Only mode never got its column either;
  6. display-only red-on-fail did nothing for Beta (calc) or any finviz row;
  7. the Columns dialog could not bring a hidden column back;
  8. `max_trailing_bars` ignored Beta (calc)'s lookback.
Added: Hide FV Columns — the finviz counterpart of Hide Q Columns.
"""
from __future__ import annotations

import datetime as dt
import json

import numpy as np
import pandas as pd
import pytest

from trade_scanner_fh import data_engine, finviz_snapshot as fs, scanner
from trade_scanner_fh.gui import widgets as W
from trade_scanner_fh.scanner import ScanParams


@pytest.fixture(scope="module")
def qapp():
    from PyQt6.QtWidgets import QApplication
    return QApplication.instance() or QApplication([])


# ----------------------------------------------------------------------
# Shared synthetic-scan fixtures
# ----------------------------------------------------------------------

_DAYS = 320


def _write_ohlcv(root, symbol, closes, volume=2_000_000):
    idx = pd.date_range("2025-01-01", periods=len(closes), freq="B")
    closes = np.asarray(closes, dtype=float)
    df = pd.DataFrame({
        "Open": closes, "High": closes * 1.01, "Low": closes * 0.99,
        "Close": closes, "Volume": [volume] * len(closes),
    }, index=idx)
    df.index.name = "Date"
    df.to_parquet(root / "ohlcv" / f"{symbol}.parquet")
    return idx


@pytest.fixture
def scan_world(fake_scan_cache):
    """SPY plus three stocks whose daily returns are exact multiples of
    SPY's, so their true beta is known: HI = 1.5, LO = 0.5, NEG = -1.0."""
    rng = np.random.default_rng(7)
    spy_r = rng.normal(0.0005, 0.01, _DAYS - 1)
    def path(mult):
        return 100.0 * np.exp(np.concatenate([[0.0], np.cumsum(mult * spy_r)]))
    idx = _write_ohlcv(fake_scan_cache, "SPY", path(1.0))
    _write_ohlcv(fake_scan_cache, "HI", path(1.5))
    _write_ohlcv(fake_scan_cache, "LO", path(0.5))
    _write_ohlcv(fake_scan_cache, "NEG", path(-1.0))
    data_engine.clear_ohlcv_cache()

    class _World:
        root = fake_scan_cache
        index_end = idx[-1].date()

    yield _World
    data_engine.clear_ohlcv_cache()


def _write_store(rows):
    from trade_scanner_fh import config
    df = pd.DataFrame(rows)
    df.to_parquet(config.FINVIZ_SNAPSHOT_PARQUET)


def _bare_params(end, **kw) -> ScanParams:
    """Every default-on filter off, so rows survive on data alone."""
    base = dict(
        start_date=end - dt.timedelta(days=5), end_date=end,
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False, top_pct_enabled=False,
        consec_gaps_enabled=False, consec_gaps_down_enabled=False,
        current_gap_enabled=False, max_gap_enabled=False,
        max_neg_gap_enabled=False, surge_enabled=False, adr_enabled=False,
        atr_enabled=False, bbw_enabled=False, atr_ratio_enabled=False,
        vol_dryup_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        rs_market_enabled=False, rs_nasdaq_enabled=False,
        rs_sector_enabled=False, days_since_earnings_enabled=False,
        days_until_earnings_enabled=False, days_until_max_enabled=False,
    )
    base.update(kw)
    return ScanParams(**base)


def _scan(world, **kw):
    params = _bare_params(world.index_end, **kw)
    return scanner.run_scan(["HI", "LO", "NEG"], params)


# ======================================================================
# 1. FV columns only when their row is on
# ======================================================================

def test_shown_finviz_fields_are_the_filter_or_display_only_rows():
    p = ScanParams(finviz_filters={
        "pe": {"enabled": True, "min": 5.0, "max": None},
        "peg": {"enabled": False, "display_only": True},
        "roe_pct": {"enabled": False, "display_only": False},
    })
    assert p.shown_finviz_fields() == ["pe", "peg"]


def test_no_finviz_row_on_means_no_fv_columns(scan_world):
    _write_store([{"symbol": "HI", "pe": 20.0, "market_cap": 5e9}])
    df = _scan(scan_world).results_df
    fv_keys = set(fs.SNAPSHOT_FIELDS) | {fs.FETCHED_COL}
    assert not (set(df.columns) & fv_keys), sorted(set(df.columns) & fv_keys)


def test_no_finviz_row_on_does_not_even_read_the_store(scan_world,
                                                       monkeypatch):
    calls = []
    monkeypatch.setattr(fs, "load_store", lambda: calls.append(1))
    _scan(scan_world)
    assert calls == []


def test_only_the_asked_for_fields_are_joined(scan_world):
    _write_store([
        {"symbol": "HI", "pe": 20.0, "market_cap": 5e9, "peg": 1.2},
        {"symbol": "LO", "pe": 8.0, "market_cap": 1e9, "peg": 0.7},
    ])
    df = _scan(scan_world, finviz_filters={
        "pe": {"enabled": False, "display_only": True,
               "min": None, "max": None}}).results_df
    assert "pe" in df.columns
    assert "market_cap" not in df.columns and "peg" not in df.columns
    assert fs.FETCHED_COL not in df.columns
    assert df.set_index("symbol").loc["HI", "pe"] == 20.0


def test_info_row_is_display_only_and_asks_for_its_column(qapp):
    panel = W.IndicatorPanel()
    row = panel.rows["fv_finviz_price"]
    assert row.toggle.isEnabled() is False
    assert row.display_only is not None
    row.display_only.setChecked(True)
    p = panel.build_scan_params(dt.date(2026, 1, 1), dt.date(2026, 2, 1))
    assert p.finviz_filters["finviz_price"] == {
        "enabled": False, "display_only": True, "min": None, "max": None}
    assert p.active_finviz_filters() == []
    assert p.shown_finviz_fields() == ["finviz_price"]


def test_info_row_forced_on_by_a_hand_edited_preset_still_cannot_filter(qapp):
    panel = W.IndicatorPanel()
    panel.from_dict({"fv_index_membership": {"enabled": True}})
    p = panel.build_scan_params(dt.date(2026, 1, 1), dt.date(2026, 2, 1))
    assert p.finviz_filters["index_membership"]["enabled"] is False
    assert p.active_finviz_filters() == []


def test_info_fields_are_exactly_the_column_only_fields():
    assert set(k for k, _l in fs.INFO_FIELDS) == set(fs.COLUMN_ONLY_FIELDS)


def test_info_columns_carry_the_panel_labels():
    by_key = {k: h for h, k, _f in W.RESULT_COLUMNS}
    assert by_key["finviz_price"] == "FV Price (latest)"
    assert by_key["index_membership"] == "FV Index"


# ======================================================================
# 2. A deleted column leaves the layout (not just its values)
# ======================================================================

def _beats_frame(n_q=3):
    row = {"symbol": "A", "close": 1.0, "pct_gain": 1.0,
           "gain_start_date": pd.Timestamp("2026-01-02"),
           "consec_eps_beats": 2, "consec_rev_beats": 1}
    for k in range(1, n_q + 1):
        for side in ("eps", "rev"):
            row[f"q{k}_report_date_{side}"] = pd.Timestamp("2026-01-01")
            row[f"q{k}_reported_{side}"] = 1.0
            row[f"q{k}_surprise_{side}_dollar"] = 0.1
            row[f"q{k}_surprise_{side}_pct"] = 5.0
            row[f"q{k}_yoy_{side}_pct"] = 10.0
    return pd.DataFrame([row])


def _keys(cols):
    return [k for _h, k, _f in cols]


def test_hidden_quarter_date_leaves_the_layout():
    """The user's case: 2026-09-24 22:56 `Hid 1 column(s): q2_report_date_eps`
    — the column stayed on screen with blank values."""
    cols, n_eps, _ = W._build_dynamic_columns(
        _beats_frame(), hidden_keys={"q2_report_date_eps"})
    keys = _keys(cols)
    assert "q2_report_date_eps" not in keys
    assert "q2_reported_eps" in keys and "q1_report_date_eps" in keys
    assert n_eps == 3


def test_hidden_beats_counter_leaves_the_layout():
    keys = _keys(W._build_dynamic_columns(
        _beats_frame(), hidden_keys={"consec_eps_beats"})[0])
    assert "consec_eps_beats" not in keys
    assert "q1_reported_eps" in keys


def test_hiding_the_last_quarters_reported_eps_keeps_its_block():
    """Dropping `q3_reported_eps` from the FRAME (the old mechanism) would
    shrink n_eps to 2 and take the whole Q-3 block with it."""
    cols, n_eps, n_rev = W._build_dynamic_columns(
        _beats_frame(3), hidden_keys={"q3_reported_eps"})
    keys = _keys(cols)
    assert n_eps == 3
    assert "q3_reported_eps" not in keys
    for k in ("q3_report_date_eps", "q3_surprise_eps_dollar",
              "q3_surprise_eps_pct", "q3_yoy_eps_pct"):
        assert k in keys, k


def test_hiding_does_not_switch_interleave_off():
    full = _keys(W._build_dynamic_columns(
        _beats_frame(), interleave_quarters=True)[0])
    hidden = _keys(W._build_dynamic_columns(
        _beats_frame(), interleave_quarters=True,
        hidden_keys={"q1_reported_eps"})[0])
    assert hidden == [k for k in full if k != "q1_reported_eps"]


def test_only_the_ticker_column_cannot_be_hidden():
    """v8.0.0: Close / % Gain / Gain Start became hideable; Ticker stays."""
    keys = _keys(W._build_dynamic_columns(
        _beats_frame(),
        hidden_keys={"symbol", "close", "pct_gain", "gain_start_date"})[0])
    assert "symbol" in keys
    assert not ({"close", "pct_gain", "gain_start_date"} & set(keys))


def test_table_hidden_keys_setter(qapp):
    t = W.ResultsTable()
    assert t.hidden_column_keys == frozenset()
    t._cached_column_widths = {("x",): [1]}
    t.set_hidden_column_keys({"a"})
    assert t.hidden_column_keys == frozenset({"a"})
    assert t._cached_column_widths == {}
    t._cached_column_widths = {("x",): [1]}
    t.set_hidden_column_keys({"a"})
    assert t._cached_column_widths == {("x",): [1]}, "unchanged -> no-op"


@pytest.fixture
def window(qapp, tmp_parquets, monkeypatch):
    from trade_scanner_fh.gui import main_window as mw_mod
    monkeypatch.setattr(mw_mod, "PRESETS_DIR", tmp_parquets / "presets")
    (tmp_parquets / "presets").mkdir(exist_ok=True)
    w = mw_mod.MainWindow()
    yield w
    w.close()
    w.deleteLater()


def _load_period(w, df, label="1D"):
    w._period_results = {label: df}
    w._period_order = [label]
    w._active_period = label
    w.combo_timeframe.blockSignals(True)
    w.combo_timeframe.clear()
    w.combo_timeframe.addItem(label, userData=label)
    w.combo_timeframe.blockSignals(False)
    w._on_timeframe_changed(0)


def test_header_delete_removes_the_column_and_keeps_the_data(window):
    _load_period(window, _beats_frame())
    window._on_columns_hide_requested(["q2_report_date_eps"])
    keys = _keys(window.results_table.active_columns)
    assert "q2_report_date_eps" not in keys
    # Data untouched, so an unhide is lossless.
    assert "q2_report_date_eps" in window._period_results["1D"].columns


def test_hidden_column_survives_a_timeframe_switch(window):
    _load_period(window, _beats_frame())
    window._on_columns_hide_requested(["q2_report_date_eps"])
    window._on_timeframe_changed(0)
    assert "q2_report_date_eps" not in _keys(window.results_table.active_columns)


# ======================================================================
# 7. The Columns dialog can bring a hidden column back
# ======================================================================

def test_columns_dialog_lists_a_hidden_column_unticked(window):
    from PyQt6.QtCore import Qt
    _load_period(window, _beats_frame())
    window._on_columns_hide_requested(["q2_report_date_eps"])
    listed = _keys(window._current_columns_for_dialog())
    assert "q2_report_date_eps" in listed
    window._open_columns_dialog()
    lst = window._columns_dialog._list
    state = {lst.item(i).data(Qt.ItemDataRole.UserRole):
             lst.item(i).checkState() for i in range(lst.count())}
    assert state["q2_report_date_eps"] == Qt.CheckState.Unchecked
    assert state["q1_report_date_eps"] == Qt.CheckState.Checked


def test_reticking_in_the_columns_dialog_restores_the_column(window):
    _load_period(window, _beats_frame())
    window._on_columns_hide_requested(["q2_report_date_eps"])
    order = _keys(window._current_columns_for_dialog())
    window._on_columns_dialog_updated(order, [])
    assert "q2_report_date_eps" in _keys(window.results_table.active_columns)
    assert window._deleted_column_keys == set()


def test_columns_dialog_still_omits_type_hidden_columns(window):
    _load_period(window, _beats_frame())
    window._hidden_earnings_col_types = {"q_reported_eps"}
    window._apply_hidden_column_types()
    listed = _keys(window._current_columns_for_dialog())
    assert "q1_reported_eps" not in listed


# ======================================================================
# 3. Finviz spinbox ranges
# ======================================================================

@pytest.mark.parametrize("name", [
    "income", "enterprise_value", "ev_ebitda", "ev_sales", "eps_ttm",
    "eps_next_y", "eps_surprise_pct", "sma20_pct", "sma50_pct",
    "perf_week_pct", "roe_pct", "oper_margin_pct", "insider_trans_pct",
])
def test_zero_is_a_typeable_threshold_for_signed_fields(name):
    """A bound AT a limit means open, so 0 must sit strictly inside."""
    lo, hi = fs.field_range(name)[:2]
    assert lo < 0.0 < hi, (name, lo, hi)


@pytest.mark.parametrize("name", ["payout_pct", "inst_own_pct"])
def test_hundred_is_a_typeable_threshold_where_values_exceed_it(name):
    lo, hi = fs.field_range(name)[:2]
    assert lo < 100.0 < hi, (name, lo, hi)


def test_ranges_are_no_longer_absurdly_wide():
    """v7.0.1 ran all of these 0..1000 against data that is mostly < 50."""
    for name in ("peg", "ps", "pb", "debt_eq", "short_ratio"):
        assert fs.field_range(name)[1] <= 200.0, name


def test_income_at_or_above_zero_is_now_expressible(qapp):
    panel = W.IndicatorPanel()
    panel.rows["fv_income"].set_enabled(True)
    panel.rows["fv_income"].set_value("fv_min", 0.0)
    p = panel.build_scan_params(dt.date(2026, 1, 1), dt.date(2026, 2, 1))
    assert p.finviz_filters["income"]["min"] == 0.0
    assert p.finviz_filters["income"]["max"] is None
    assert "Income >= 0" in [n for n, _f in scanner._build_filter_stages(p)]


def test_big_number_rows_use_the_suffix_spinbox(qapp):
    panel = W.IndicatorPanel()
    for name in fs.FILTERABLE_FIELDS:
        sb = panel.rows[f"fv_{name}"].spinboxes["fv_min"]
        assert isinstance(sb, W.HumanDoubleSpinBox) == fs.is_big_number(name), name


def test_suffix_spinbox_renders_readably(qapp):
    panel = W.IndicatorPanel()
    assert panel.rows["fv_market_cap"].spinboxes["fv_max"].text() == "5.00T"
    assert panel.rows["fv_income"].spinboxes["fv_min"].text() == "-100.00B"


@pytest.mark.parametrize("text,value", [
    ("2.5B", 2.5e9), ("2.5b", 2.5e9), ("300M", 3e8), ("-50B", -5e10),
    ("1,000", 1000.0), ("2500000000", 2.5e9), ("1.5T", 1.5e12),
    ("750K", 7.5e5), (" 4 B ", 4e9),
])
def test_parse_human_number(text, value):
    assert W.parse_human_number(text) == pytest.approx(value)


@pytest.mark.parametrize("text", ["", "-", ".", "abc", "2.5X", "B"])
def test_parse_human_number_rejects_non_numbers(text):
    assert W.parse_human_number(text) is None


@pytest.mark.parametrize("value,text", [
    (2.5e9, "2.50B"), (-5e10, "-50.00B"), (632, "632"), (0, "0"),
    (5e12, "5.00T"), (1.5e6, "1.50M"),
])
def test_format_human_number(value, text):
    assert W.format_human_number(value) == text


def test_suffix_spinbox_accepts_typed_suffixes(qapp):
    from PyQt6.QtGui import QValidator
    sb = W.HumanDoubleSpinBox()
    sb.setRange(-1e11, 5e12)
    sb.setDecimals(0)
    assert sb.validate("2.5B", 4)[0] == QValidator.State.Acceptable
    assert sb.validate("-", 1)[0] == QValidator.State.Intermediate
    assert sb.validate("9T", 2)[0] == QValidator.State.Intermediate
    assert sb.validate("x", 1)[0] == QValidator.State.Invalid
    assert sb.valueFromText("2.5B") == pytest.approx(2.5e9)


def test_suffix_spinbox_steps_by_magnitude(qapp):
    sb = W.HumanDoubleSpinBox()
    sb.setRange(0.0, 5e12)
    sb.setDecimals(0)
    sb.setSingleStep(1e8)
    # Below 1B the configured 100M step is the floor: 300M -> 400M.
    sb.setValue(3e8)
    sb.stepBy(1)
    assert sb.value() == pytest.approx(4e8)
    # Above it the step follows magnitude: 2.5B -> 2.6B, 25B -> 26B.
    sb.setValue(2.5e9)
    sb.stepBy(1)
    assert sb.value() == pytest.approx(2.6e9)
    sb.setValue(0.0)
    sb.stepBy(1)
    assert sb.value() == pytest.approx(1e8), "floor is the configured step"


# --- migration of pre-v8 presets -------------------------------------

def test_v701_open_bounds_stay_open():
    # money_large was 0 .. 5e12 in v7.0.1
    assert fs.migrate_v7_bounds("income", 0.0, 5e12) == \
        fs.field_range("income")[:2]
    # pct_signed was -100 .. 1000
    assert fs.migrate_v7_bounds("eps_surprise_pct", -100.0, 1000.0) == \
        fs.field_range("eps_surprise_pct")[:2]


def test_v701_genuine_bounds_are_untouched():
    assert fs.migrate_v7_bounds("pe", 5.0, 30.0) == (5.0, 30.0)
    assert fs.migrate_v7_bounds("income", 1e9, 5e12) == \
        (1e9, fs.field_range("income")[1])


def test_v700_sentinels_stay_open_and_block_the_kind_rule():
    new_lo, new_hi = fs.field_range("income")[:2]
    assert fs.migrate_v7_bounds("income", -1e12, 1e12) == (new_lo, new_hi)
    # A 7.0.0 row with a sentinel on one side is recognisably 7.0.0: its
    # genuine 0.0 on the other side is NOT read as a v7.0.1 kind limit.
    assert fs.migrate_v7_bounds("income", 0.0, 1e12) == (0.0, new_hi)


def test_from_dict_migrates_only_when_told_the_preset_is_old(qapp):
    old = {"fv_income": {"enabled": True, "display_only": False,
                         "fv_min": 0.0, "fv_max": 5e12}}
    panel = W.IndicatorPanel()
    panel.from_dict(old, preset_version=6)
    p = panel.build_scan_params(dt.date(2026, 1, 1), dt.date(2026, 2, 1))
    assert p.finviz_filters["income"]["min"] is None, "open stays open"

    panel2 = W.IndicatorPanel()
    panel2.from_dict(old)   # no version -> current -> no migration
    p2 = panel2.build_scan_params(dt.date(2026, 1, 1), dt.date(2026, 2, 1))
    assert p2.finviz_filters["income"]["min"] == 0.0


def test_v8_round_trip_keeps_income_at_or_above_zero(qapp):
    panel = W.IndicatorPanel()
    panel.rows["fv_income"].set_enabled(True)
    panel.rows["fv_income"].set_value("fv_min", 0.0)
    other = W.IndicatorPanel()
    other.from_dict(panel.to_dict(), preset_version=7)
    p = other.build_scan_params(dt.date(2026, 1, 1), dt.date(2026, 2, 1))
    assert p.finviz_filters["income"]["min"] == 0.0


def test_loading_a_v6_preset_file_keeps_its_open_bounds_open(window,
                                                             tmp_parquets):
    preset = {
        "_preset_version": 6,
        "indicators": {"fv_income": {
            "enabled": True, "display_only": False,
            "fv_min": 0.0, "fv_max": 5e12}},
    }
    path = tmp_parquets / "presets" / "old7.json"
    path.write_text(json.dumps(preset), encoding="utf-8")
    window.preset_combo.addItem("old7")
    window.preset_combo.setCurrentText("old7")
    assert window._load_preset() is True
    p = window.indicator_panel.build_scan_params(
        dt.date(2026, 1, 1), dt.date(2026, 2, 1))
    assert p.finviz_filters["income"] == {
        "enabled": True, "display_only": False, "min": None, "max": None}


# ======================================================================
# 4 + 5. Benchmarks load for Beta (calc) and for display-only RS
# ======================================================================

@pytest.mark.parametrize("flags,expected", [
    ({}, False),
    ({"beta_calc_display_only": True}, True),
    ({"beta_calc_enabled": True}, True),
    ({"rs_market_display_only": True}, True),
    ({"rs_nasdaq_display_only": True}, True),
    ({"rs_sector_display_only": True}, True),
    ({"rs_market_enabled": True}, True),
])
def test_needs_benchmarks(flags, expected):
    base = {"rs_market_enabled": False, "rs_nasdaq_enabled": False,
            "rs_sector_enabled": False}
    base.update(flags)
    assert scanner._needs_benchmarks(ScanParams(**base)) is expected


def test_beta_calc_display_only_produces_the_true_beta(scan_world):
    df = _scan(scan_world, beta_calc_display_only=True).results_df
    got = df.set_index("symbol")["beta_calc"]
    assert got["HI"] == pytest.approx(1.5, abs=1e-6)
    assert got["LO"] == pytest.approx(0.5, abs=1e-6)
    assert got["NEG"] == pytest.approx(-1.0, abs=1e-6)


def test_beta_calc_filter_no_longer_zeroes_the_scan(scan_world):
    """Before v8.0.0 the missing column failed the stage closed: 0 results."""
    df = _scan(scan_world, beta_calc_enabled=True,
               beta_calc_min=1.0, beta_calc_max=2.0).results_df
    assert list(df["symbol"]) == ["HI"]


def test_rs_display_only_alone_gets_its_column(scan_world):
    df = _scan(scan_world, rs_market_display_only=True).results_df
    assert "rs_market" in df.columns
    assert df["rs_market"].notna().all()


# ======================================================================
# 8. Seam quarantine reach counts Beta (calc)
# ======================================================================

def test_max_trailing_bars_counts_beta_only_when_live():
    off = ScanParams(sma1_period=10, sma2_period=10, beta_calc_lookback=400)
    on = ScanParams(sma1_period=10, sma2_period=10, beta_calc_lookback=400,
                    beta_calc_display_only=True)
    assert on.max_trailing_bars() >= 401
    assert off.max_trailing_bars() < 401


# ======================================================================
# 6. Display-only red-on-fail for Beta (calc) and finviz rows
# ======================================================================

def test_beta_calc_display_only_flags_out_of_band(scan_world):
    df = _scan(scan_world, beta_calc_display_only=True,
               beta_calc_min=0.0, beta_calc_max=1.0).results_df.set_index("symbol")
    fails = df["_display_only_fails"]
    assert fails["HI"].get("beta_calc") is True
    assert fails["NEG"].get("beta_calc") is True
    lo = fails["LO"]
    assert not (isinstance(lo, dict) and lo.get("beta_calc"))


def test_finviz_display_only_flags_merge_into_existing_dicts():
    df = pd.DataFrame({
        "symbol": ["A", "B", "C", "D"],
        "pe": [5.0, 50.0, np.nan, 20.0],
        "_display_only_fails": [{"hv": True}, None, None, None],
    })
    p = ScanParams(finviz_filters={
        "pe": {"enabled": False, "display_only": True,
               "min": 10.0, "max": 30.0}})
    out = scanner._apply_finviz_display_only_fails(df, p)
    assert out.at[0, "_display_only_fails"] == {"hv": True, "pe": True}
    assert out.at[1, "_display_only_fails"] == {"pe": True}
    assert out.at[2, "_display_only_fails"] is None, "blank is not a fail"
    assert out.at[3, "_display_only_fails"] is None, "inside the band"


def test_finviz_display_only_open_side_flags_nothing():
    df = pd.DataFrame({"symbol": ["A"], "pe": [900.0]})
    p = ScanParams(finviz_filters={
        "pe": {"enabled": False, "display_only": True,
               "min": 10.0, "max": None}})
    out = scanner._apply_finviz_display_only_fails(df, p)
    assert "_display_only_fails" not in out.columns


def test_finviz_filter_rows_are_not_flagged_only_display_only():
    df = pd.DataFrame({"symbol": ["A"], "pe": [900.0]})
    p = ScanParams(finviz_filters={
        "pe": {"enabled": True, "min": 10.0, "max": 30.0}})
    out = scanner._apply_finviz_display_only_fails(df, p)
    assert "_display_only_fails" not in out.columns


def test_finviz_yes_no_row_flags_the_opposite_answer():
    df = pd.DataFrame({"symbol": ["A", "B"], "optionable": [1.0, 0.0]})
    p = ScanParams(finviz_filters={
        "optionable": {"enabled": False, "display_only": True,
                       "min": 1.0, "max": 1.0}})
    out = scanner._apply_finviz_display_only_fails(df, p)
    assert out.at[1, "_display_only_fails"] == {"optionable": True}
    assert out.at[0, "_display_only_fails"] is None


def test_finviz_display_only_red_end_to_end(scan_world):
    _write_store([
        {"symbol": "HI", "pe": 50.0}, {"symbol": "LO", "pe": 20.0},
        {"symbol": "NEG", "pe": np.nan},
    ])
    df = _scan(scan_world, finviz_filters={
        "pe": {"enabled": False, "display_only": True,
               "min": 10.0, "max": 30.0}}).results_df.set_index("symbol")
    fails = df["_display_only_fails"]
    assert fails["HI"] == {"pe": True}
    assert not isinstance(fails["LO"], dict)
    assert not isinstance(fails["NEG"], dict)


# ======================================================================
# Found by the phase-1 smoke test
# ======================================================================

def test_preset_without_a_row_resets_that_row_to_its_default(qapp):
    """Every preset written before v7.0.0 omits the finviz / Beta / HV rows.
    Loading one must switch those rows OFF, not keep whatever the session
    had — otherwise a filter from the previous scan rides along invisibly."""
    panel = W.IndicatorPanel()
    panel.rows["fv_pe"].set_enabled(True)
    panel.rows["fv_pe"].set_value("fv_min", 5.0)
    panel.rows["beta_calc"].display_only.setChecked(True)
    panel.from_dict({"sma1": {"enabled": True}})
    p = panel.build_scan_params(dt.date(2026, 1, 1), dt.date(2026, 2, 1))
    assert p.finviz_filters == {}
    assert p.beta_calc_display_only is False
    assert panel.rows["fv_pe"].value("fv_min") == fs.field_range("pe")[0]


def test_preset_still_restores_every_row_it_does_mention(qapp):
    panel = W.IndicatorPanel()
    saved = panel.to_dict()
    saved["fv_pe"] = {**saved["fv_pe"], "enabled": True, "fv_min": 7.0}
    other = W.IndicatorPanel()
    other.from_dict(saved, preset_version=7)
    p = other.build_scan_params(dt.date(2026, 1, 1), dt.date(2026, 2, 1))
    assert p.finviz_filters["pe"]["min"] == 7.0


def test_hide_menus_are_empty_before_any_scan(window):
    """Before v8.0.0 they listed every type from the static column list."""
    assert window._hide_types_source_columns() == []
    window._rebuild_hide_types_menu()
    window._rebuild_hide_fv_types_menu()
    for menu in (window._hide_types_menu, window._hide_fv_types_menu):
        acts = menu.actions()
        assert len(acts) == 1 and not acts[0].isEnabled()


# ======================================================================
# Hide FV Columns
# ======================================================================

def test_every_fv_column_belongs_to_exactly_one_category():
    fv_keys = set(fs.SNAPSHOT_FIELDS)
    for key in fv_keys:
        assert W.fv_column_type_of(key) is not None, key
    ids = [gid for gid, _t, _k in fs.COLUMN_GROUPS]
    assert len(ids) == len(set(ids))
    all_keys = [k for _g, _t, keys in fs.COLUMN_GROUPS for k in keys]
    assert len(all_keys) == len(set(all_keys)) == len(fv_keys)


def test_fv_categories_follow_the_panel_headings():
    assert W.fv_column_type_of("finviz_beta") == "fv_options"
    assert W.fv_column_type_of("optionable") == "fv_options"
    assert W.fv_column_type_of("market_cap") == "fv_valuation"
    assert W.fv_column_type_of("eps_next_5y_pct") == "fv_growth_estimates"
    assert W.fv_column_type_of("short_float_pct") == "fv_ownership_short"
    assert W.fv_column_type_of("finviz_price") == "fv_info"


def test_non_fv_columns_have_no_fv_category():
    for key in ("beta_calc", "symbol", "q1_reported_eps", "hv", "rs_market"):
        assert W.fv_column_type_of(key) is None, key


def test_fv_and_earnings_type_ids_never_collide():
    fv_ids = {t for t, _l in W.FV_COLUMN_TYPES}
    earnings_ids = {t for t, _l in W.EARNINGS_COLUMN_TYPES}
    assert not fv_ids & earnings_ids


def _fv_frame():
    return pd.DataFrame([{
        "symbol": "A", "close": 1.0, "pct_gain": 1.0,
        "gain_start_date": pd.Timestamp("2026-01-02"),
        "pe": 10.0, "peg": 1.0, "short_float_pct": 5.0,
        "finviz_beta": 1.1, "beta_calc": 1.2, "finviz_price": 12.0,
    }])


def test_present_fv_types_counts_what_the_scan_produced():
    cols, _a, _b = W._build_dynamic_columns(_fv_frame())
    present = {t: n for t, _l, n in W.present_fv_column_types(cols)}
    assert present == {"fv_options": 1, "fv_valuation": 2,
                       "fv_ownership_short": 1, "fv_info": 1}


def test_hiding_an_fv_category_hides_exactly_its_columns():
    cols, _a, _b = W._build_dynamic_columns(
        _fv_frame(), hidden_types={"fv_valuation"})
    keys = set(_keys(cols))
    assert "pe" not in keys and "peg" not in keys
    assert {"short_float_pct", "finviz_beta", "beta_calc",
            "finviz_price"} <= keys


def test_hiding_options_leaves_the_computed_beta(qapp):
    keys = set(_keys(W._build_dynamic_columns(
        _fv_frame(), hidden_types={"fv_options"})[0]))
    assert "finviz_beta" not in keys and "beta_calc" in keys


def test_fv_dropdown_lists_toggles_and_shows_all(window):
    _load_period(window, _fv_frame())
    window._rebuild_hide_fv_types_menu()
    labels = [a.text() for a in window._hide_fv_types_menu.actions()]
    assert labels == ["Options  (1)", "Valuation  (2)",
                      "Ownership & Short  (1)", "Info  (1)"]

    window._on_hide_fv_type_toggled("fv_valuation", True)
    keys = set(_keys(window.results_table.active_columns))
    assert "pe" not in keys and "short_float_pct" in keys
    assert window.btn_hide_fv_types.text() == "Hide FV Columns (1) ▾"
    assert window.lbl_hidden_fv_types.text() == "hidden: Valuation"

    # Show All on the EARNINGS side must not touch FV choices.
    window._hidden_earnings_col_types = {"q_reported_eps"}
    window._on_show_all_column_types()
    assert window._hidden_fv_col_types == {"fv_valuation"}

    window._on_show_all_fv_types()
    assert "pe" in set(_keys(window.results_table.active_columns))
    assert window.btn_hide_fv_types.text() == "Hide FV Columns ▾"


def test_fv_dropdown_says_so_when_the_scan_has_no_fv_columns(window):
    _load_period(window, _beats_frame())
    window._rebuild_hide_fv_types_menu()
    acts = window._hide_fv_types_menu.actions()
    assert len(acts) == 1 and not acts[0].isEnabled()


def test_hidden_fv_types_round_trip_through_a_preset(window, tmp_parquets):
    window._hidden_fv_col_types = {"fv_valuation", "fv_info"}
    window.preset_combo.addItem("fvp")
    window.preset_combo.setCurrentText("fvp")
    window._save_preset()
    data = json.loads((tmp_parquets / "presets" / "fvp.json")
                      .read_text(encoding="utf-8"))
    assert data["_preset_version"] == 7
    assert data["hidden_fv_col_types"] == ["fv_info", "fv_valuation"]

    window._hidden_fv_col_types = set()
    assert window._load_preset() is True
    assert window._hidden_fv_col_types == {"fv_valuation", "fv_info"}
    assert {"fv_valuation", "fv_info"} <= window.results_table.hidden_column_types


def test_a_preset_without_fv_categories_unhides_them(window, tmp_parquets):
    """The user's rule (2026-09-26): changing presets always hides / unhides
    to match the incoming preset, so a preset that carries no FV categories
    shows them all — even a pre-v7 preset that predates the dropdown."""
    (tmp_parquets / "presets" / "v6.json").write_text(json.dumps({
        "_preset_version": 6, "indicators": {},
        "hidden_earnings_col_types": []}), encoding="utf-8")
    window._hidden_fv_col_types = {"fv_info"}
    window.preset_combo.addItem("v6")
    window.preset_combo.setCurrentText("v6")
    window._load_preset()
    assert window._hidden_fv_col_types == set()
    assert "fv_info" not in window.results_table.hidden_column_types


def test_export_dialog_reoffers_fv_hidden_columns_unticked(window,
                                                           monkeypatch):
    from PyQt6.QtWidgets import QDialog
    from trade_scanner_fh.gui import exports as ex
    seen = {}

    class _FakeDialog:
        def __init__(self, columns, periods=None, parent=None,
                     prechecked=None):
            seen["keys"] = [k for _h, k, _f in columns]
            seen["prechecked"] = prechecked

        def exec(self):
            return QDialog.DialogCode.Rejected

    monkeypatch.setattr(ex, "ExcelExportDialog", _FakeDialog)
    _load_period(window, _fv_frame())
    window._on_hide_fv_type_toggled("fv_valuation", True)
    window._excel_export_dialog()
    assert "pe" in seen["keys"] and "peg" in seen["keys"]
    assert "pe" not in seen["prechecked"]
    assert "short_float_pct" in seen["prechecked"]
