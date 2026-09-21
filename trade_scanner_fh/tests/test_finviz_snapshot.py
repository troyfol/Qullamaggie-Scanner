"""v7.0.0 chunks C + D: the finviz snapshot attribute layer.

Parsing is tested against synthetic HTML shaped like the real page rather than
against a live fetch, so the suite stays offline. The shapes encoded here were
all verified against live responses first:

  * ``EPS next Y`` appears TWICE with different meanings
  * seven cells pack two values
  * ETFs return a shorter grid
  * an unknown ticker is 404 + ``Ticker <b>"X"</b> not found.`` — with the
    symbol wrapped in markup, which is what broke the first regex
"""

import os

import numpy as np
import pandas as pd
import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt6.QtWidgets import QApplication  # noqa: E402

from trade_scanner_fh import config  # noqa: E402
from trade_scanner_fh import finviz_snapshot as fs  # noqa: E402
from trade_scanner_fh import finviz_snapshot_fill as ff  # noqa: E402
from trade_scanner_fh import scanner as sc  # noqa: E402
from trade_scanner_fh.gui import widgets as W  # noqa: E402


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance() or QApplication([])
    yield app


@pytest.fixture
def store(tmp_path, monkeypatch):
    """Redirect the snapshot store and clear the module write buffer."""
    monkeypatch.setattr(config, "FINVIZ_SNAPSHOT_PARQUET",
                        tmp_path / "finviz_snapshot.parquet")
    fs._pending.clear()
    yield tmp_path
    fs._pending.clear()


def _page(pairs) -> str:
    """Synthetic quote page carrying `pairs` in the snapshot grid."""
    cells = "".join(
        f'<td class="snapshot-td2">{k}</td><td class="snapshot-td2">{v}</td>'
        for k, v in pairs
    )
    return f"<html><body><table>{cells}</table></body></html>"


# ----------------------------------------------------------------------
# Parsing
# ----------------------------------------------------------------------

def test_duplicate_eps_next_y_resolves_positionally():
    """The trap a label-keyed dict falls into: finviz emits `EPS next Y`
    twice, once as an absolute estimate and once as a growth rate."""
    d = fs.parse_snapshot(_page([
        ("EPS next Y", "9.61"), ("EPS this Y", "18.42%"),
        ("EPS next Y", "8.76%"),
    ]))
    assert d["eps_next_y"] == 9.61
    assert d["eps_next_y_pct"] == 8.76


def test_two_value_cells_split_with_weekly_on_the_left():
    d = fs.parse_snapshot(_page([("Volatility", "4.28% 4.71%")]))
    assert d["volatility_week_pct"] == 4.28
    assert d["volatility_month_pct"] == 4.71


def test_52w_high_splits_level_from_distance():
    d = fs.parse_snapshot(_page([("52W High", "344.57 -2.45%")]))
    assert d["high_52w"] == 344.57
    assert d["high_52w_pct"] == -2.45


def test_parenthesised_dividend_splits():
    d = fs.parse_snapshot(_page([("Dividend Est.", "1.10 (0.33%)")]))
    assert d["dividend_est"] == 1.10
    assert d["dividend_est_pct"] == 0.33


def test_partially_missing_two_value_cell():
    """`2.27% -` is a real shape: 3Y present, 5Y absent."""
    d = fs.parse_snapshot(_page([("Dividend Gr. 3/5Y", "2.27% -")]))
    assert d["dividend_gr_3y"] == 2.27
    assert d["dividend_gr_5y"] is None


def test_double_dash_is_missing_not_zero():
    d = fs.parse_snapshot(_page([("EPS past 3/5Y", "- -")]))
    assert d["eps_past_3y_pct"] is None
    assert d["eps_past_5y_pct"] is None


def test_zero_percent_is_a_real_value_not_missing():
    """`Payout 0.00%` must stay 0.0 — a company that pays nothing is a fact,
    and collapsing it to None would make it unfilterable."""
    d = fs.parse_snapshot(_page([("Payout", "0.00%")]))
    assert d["payout_pct"] == 0.0


def test_magnitude_suffixes():
    d = fs.parse_snapshot(_page([
        ("Market Cap", "4905.54B"), ("Income", "128.93M"),
        ("Short Interest", "139.75M"), ("Employees", "166000"),
    ]))
    assert d["market_cap"] == pytest.approx(4.90554e12)
    assert d["income"] == pytest.approx(1.2893e8)
    assert d["employees"] == 166000.0


def test_option_short_yields_two_booleans():
    d = fs.parse_snapshot(_page([("Option/Short", "Yes / No")]))
    assert d["optionable"] is True
    assert d["shortable"] is False


def test_change_pct_label_carries_the_percent_sign():
    """The label is literally `Change %`. Spelling it `Change` produced no
    value at all, which is how this was found."""
    d = fs.parse_snapshot(_page([("Change %", "-0.26%")]))
    assert d["finviz_change_pct"] == -0.26


def test_short_etf_grid_yields_fewer_fields_without_error():
    d = fs.parse_snapshot(_page([
        ("Market Cap", "500.00B"), ("Beta", "1.01"),
        ("Volatility", "0.78% 1.02%"),
    ]))
    assert set(d) == {"market_cap", "finviz_beta",
                      "volatility_week_pct", "volatility_month_pct"}


def test_unknown_labels_are_skipped_not_fatal():
    d = fs.parse_snapshot(_page([
        ("Some New Finviz Row", "1.23"), ("P/E", "38.53"),
    ]))
    assert d == {"pe": 38.53}


def test_no_grid_returns_empty_dict():
    assert fs.parse_snapshot("<html><body>blocked</body></html>") == {}
    assert fs.parse_snapshot("") == {}


def test_to_number_handles_every_missing_marker():
    for raw in ("-", "--", "- -", "", "N/A", None):
        assert fs.to_number(raw) is None
    assert fs.to_number("(1.5)") == -1.5
    assert fs.to_number("1,234.5") == 1234.5


# ----------------------------------------------------------------------
# The not-found gate — decides permanent skip-listing
# ----------------------------------------------------------------------

_NOT_FOUND_BODY = '<tr><td>Ticker <b>"ZZZZQQ9"</b> not found.<br></td></tr>'


def test_404_plus_body_text_is_definitive():
    assert fs.is_ticker_not_found(404, _NOT_FOUND_BODY) is True


def test_symbol_wrapped_in_markup_still_matches():
    """The live page wraps the symbol in <b>. A regex run against raw HTML
    matched nothing, so every uncovered ticker would be retried forever."""
    assert fs.is_ticker_not_found(404, 'Ticker <b>"X"</b> not found.') is True


def test_block_responses_never_count_as_not_found():
    """429 / 403 say we were blocked, which says nothing about coverage.
    Treating them as uncovered is how a skip list fills with good tickers."""
    for code in (429, 403, 500, 200):
        assert fs.is_ticker_not_found(code, _NOT_FOUND_BODY) is False


def test_bare_404_without_the_text_is_not_definitive():
    assert fs.is_ticker_not_found(404, "<html>nope</html>") is False


# ----------------------------------------------------------------------
# Store
# ----------------------------------------------------------------------

def test_empty_store_reads_clean(store):
    assert fs.load_store().empty
    assert fs.last_updated() is None


def test_merge_normalises_symbols(store):
    fs.merge_rows([{"symbol": " aapl ", "pe": 10.0}])
    assert fs.load_store()["symbol"].tolist() == ["AAPL"]


def test_merge_never_replaces_the_other_producer(store):
    """Two producers write here — the earnings scavenge and the sweep. A
    wholesale write from either would discard the other's work."""
    fs.merge_rows([{"symbol": "AAA", "pe": 10.0},
                   {"symbol": "BBB", "pe": 20.0, "finviz_beta": 0.8}])
    fs.merge_rows([{"symbol": "AAA", "pe": 99.0}])
    got = fs.load_store().set_index("symbol")
    assert got.loc["AAA", "pe"] == 99.0
    assert got.loc["BBB", "pe"] == 20.0
    assert got.loc["BBB", "finviz_beta"] == 0.8


def test_duplicate_symbol_in_one_batch_keeps_the_last(store):
    fs.merge_rows([{"symbol": "C", "pe": 1.0}, {"symbol": "C", "pe": 2.0}])
    assert fs.load_store().set_index("symbol").loc["C", "pe"] == 2.0


def test_corrupt_store_degrades_to_empty(store):
    config.FINVIZ_SNAPSHOT_PARQUET.write_bytes(b"not a parquet")
    assert fs.load_store().empty       # logs, does not raise


def test_stale_symbols_puts_never_fetched_first_then_oldest(store):
    now = pd.Timestamp.now(tz="UTC")
    fs.merge_rows([
        {"symbol": "RECENT", "fetched_at": now - pd.Timedelta(days=1)},
        {"symbol": "OLD", "fetched_at": now - pd.Timedelta(days=40)},
        {"symbol": "OLDEST", "fetched_at": now - pd.Timedelta(days=90)},
    ])
    got = fs.stale_symbols(["RECENT", "OLD", "OLDEST", "NEVER"], stale_days=7)
    assert got == ["NEVER", "OLDEST", "OLD"]


def test_stale_symbols_honours_the_skip_set(store):
    assert fs.stale_symbols(["A", "B"], stale_days=7, skip={"a"}) == ["B"]


def test_write_buffer_flushes_on_threshold(store):
    for i in range(fs._FLUSH_EVERY - 1):
        assert fs.queue_row({"symbol": f"T{i}"}) == 0
    assert fs.pending_count() == fs._FLUSH_EVERY - 1
    assert fs.load_store().empty
    assert fs.queue_row({"symbol": "LAST"}) == fs._FLUSH_EVERY
    assert fs.pending_count() == 0


def test_flush_queue_is_safe_when_empty(store):
    assert fs.flush_queue() == 0


# ----------------------------------------------------------------------
# Sweep
# ----------------------------------------------------------------------

@pytest.fixture
def fast_sweep(store, monkeypatch):
    monkeypatch.setattr(config, "FINVIZ_SNAPSHOT_MIN_INTERVAL_SEC", 0.0)
    monkeypatch.setattr(config, "FINVIZ_SNAPSHOT_JITTER_SEC", 0.0)
    monkeypatch.setattr(config, "FINVIZ_INITIAL_BLOCK_PAUSE_SEC", 0)

    def scripted(mapping):
        def fake(sym, timeout=25.0):
            st = mapping.get(sym, ff.OK)
            if st == ff.OK:
                return ff.OK, {fs.SYMBOL_COL: sym, "pe": 1.0,
                               fs.FETCHED_COL: pd.Timestamp.now(tz="UTC")}
            return st, None
        monkeypatch.setattr(ff, "fetch_one", fake)
    return scripted


def test_sweep_writes_and_counts(fast_sweep):
    fast_sweep({})
    s = ff.run_sweep([f"T{i}" for i in range(5)])
    assert s["ok"] == 5 and s["written"] == 5
    assert len(fs.load_store()) == 5


def test_sweep_honours_the_combined_skip_set(fast_sweep):
    fast_sweep({})
    assert ff.run_sweep(["A", "B", "C"], skip={"b"})["requested"] == 2


def test_only_a_definitive_404_skip_lists(fast_sweep):
    """The whole point of the skip policy: a block must never look like
    non-coverage."""
    fast_sweep({"DEAD": ff.NOT_FOUND, "THROTTLED": ff.BLOCKED,
                "WEIRD": ff.EMPTY, "FLAKY": ff.NETWORK})
    added = []
    s = ff.run_sweep(["DEAD", "THROTTLED", "WEIRD", "FLAKY", "GOOD"],
                     on_not_found=added.append)
    assert added == ["DEAD"]
    assert s["blocked"] == 1 and s["empty_grid"] == 1 and s["network"] == 1


def test_not_found_does_not_trip_the_block_backoff(fast_sweep):
    """A cluster of dead tickers must not read as a throttle."""
    fast_sweep({f"D{i}": ff.NOT_FOUND for i in range(10)})
    s = ff.run_sweep([f"D{i}" for i in range(10)], max_consecutive_blocks=2)
    assert s["not_found"] == 10 and not s["aborted"]


def test_persistent_blocking_aborts_instead_of_hammering(fast_sweep):
    fast_sweep({f"B{i}": ff.BLOCKED for i in range(200)})
    s = ff.run_sweep([f"B{i}" for i in range(200)], max_consecutive_blocks=3)
    assert s["aborted"] is True
    assert s["blocked"] < 200      # stopped early


def test_stop_flushes_what_was_already_fetched(fast_sweep):
    fast_sweep({})
    calls = {"n": 0}

    def stop():
        calls["n"] += 1
        return calls["n"] > 5
    s = ff.run_sweep([f"S{i}" for i in range(50)], should_stop=stop)
    assert s["stopped"] and s["written"] == s["ok"] > 0
    assert len(fs.load_store()) == s["ok"]


# ----------------------------------------------------------------------
# Field taxonomy / panel / columns
# ----------------------------------------------------------------------

def test_every_parsed_field_is_reachable():
    """A field that is parsed but neither filterable nor a column would be
    silently dead weight."""
    covered = (set(fs.FILTERABLE_FIELDS) | set(fs.COLUMN_ONLY_FIELDS)
               | set(fs.BOOL_FIELDS))
    assert set(fs.SNAPSHOT_FIELDS) - covered == set()


def test_withheld_fields_are_columns_but_not_filters():
    """Latest-day values and per-period duplicates: kept as data, withheld
    as filters."""
    for key in ("finviz_price", "finviz_volume", "finviz_atr14",
                "finviz_rel_volume", "finviz_avg_volume",
                "finviz_prev_close", "finviz_change_pct"):
        assert key not in fs.FILTERABLE_FIELDS, key
        assert key in fs.COLUMN_ONLY_FIELDS, key


def test_rsi_is_kept_filterable():
    """Nothing else in the app computes RSI, so it is additive rather than
    conflicting with a per-period column."""
    assert "finviz_rsi14" in fs.FILTERABLE_FIELDS


def test_panel_has_a_row_for_every_filterable_field(qapp):
    panel = W.IndicatorPanel()
    for key in fs.FILTERABLE_FIELDS:
        assert f"fv_{key}" in panel.rows, key


def test_panel_sections_all_start_collapsed(qapp):
    """80 rows laid out flat is unusable."""
    panel = W.IndicatorPanel()
    assert len(panel._collapsible_sections) == len(fs.FINVIZ_GROUPS)
    for title, (_btn, body) in panel._collapsible_sections.items():
        assert body.isVisibleTo(panel) is False, title


def test_collapsible_toggles_visibility(qapp):
    panel = W.IndicatorPanel()
    btn, body = panel._collapsible_sections["Valuation"]
    btn.setChecked(True)
    assert body.isVisibleTo(panel) is True
    btn.setChecked(False)
    assert body.isVisibleTo(panel) is False


def test_every_snapshot_field_has_a_result_column(qapp):
    keys = {k for _h, k, _f in W.RESULT_COLUMNS}
    assert [f for f in fs.SNAPSHOT_FIELDS if f not in keys] == []


def test_finviz_columns_are_prefixed(qapp):
    """`FV ` marks a scraped static value so it is never confused with a
    per-period computed one."""
    by_key = {k: h for h, k, _f in W.RESULT_COLUMNS}
    assert by_key["finviz_beta"].startswith("FV ")
    assert by_key["pe"].startswith("FV ")


# ----------------------------------------------------------------------
# ScanParams / filter stages
# ----------------------------------------------------------------------

def test_finviz_filters_default_empty_and_inert():
    p = sc.ScanParams()
    assert p.finviz_filters == {}
    assert p.active_finviz_filters() == []
    assert p.finviz_filter("pe") == {
        "enabled": False, "display_only": False, "min": None, "max": None}


def test_enabled_filter_appends_a_labelled_stage():
    p = sc.ScanParams(finviz_filters={
        "pe": {"enabled": True, "min": 5.0, "max": 30.0}})
    assert "P/E >= 5 and <= 30" in [n for n, _f in sc._build_filter_stages(p)]


def test_open_ended_bound_is_supported():
    p = sc.ScanParams(finviz_filters={
        "short_float_pct": {"enabled": True, "min": 20.0, "max": None}})
    assert "Short Float % >= 20" in [n for n, _f in sc._build_filter_stages(p)]


def test_display_only_and_disabled_do_not_gate():
    p = sc.ScanParams(finviz_filters={
        "peg": {"enabled": True, "display_only": True, "min": 0.0, "max": 1.0},
        "roe_pct": {"enabled": False, "min": 10.0, "max": None}})
    labels = [n for n, _f in sc._build_filter_stages(p)]
    assert not [s for s in labels if s.startswith(("PEG", "ROE"))]


def test_both_bounds_none_appends_nothing():
    p = sc.ScanParams(finviz_filters={
        "pe": {"enabled": True, "min": None, "max": None}})
    assert not [s for s in [n for n, _f in sc._build_filter_stages(p)]
                if s.startswith("P/E")]


def test_finviz_stage_fails_nan_and_missing_columns():
    p = sc.ScanParams(finviz_filters={
        "short_float_pct": {"enabled": True, "min": 20.0, "max": None}})
    stage = dict(sc._build_filter_stages(p))["Short Float % >= 20"]
    df = pd.DataFrame({"short_float_pct": [25.0, 19.9, np.nan]})
    assert list(stage(df)) == [True, False, False]
    assert not stage(pd.DataFrame({"symbol": ["A"]})).any()


def test_panel_round_trips_finviz_filters(qapp):
    import datetime as dt
    panel = W.IndicatorPanel()
    panel.rows["fv_pe"].set_enabled(True)
    panel.rows["fv_pe"].set_value("fv_min", 5.0)
    panel.rows["fv_pe"].set_value("fv_max", 30.0)
    panel.rows["fv_optionable"].set_enabled(True)
    panel.rows["fv_optionable"].set_value("want", "yes")

    p = panel.build_scan_params(dt.date(2025, 1, 1), dt.date(2025, 6, 1))
    assert p.finviz_filters["pe"]["min"] == 5.0
    assert p.finviz_filters["optionable"] == {
        "enabled": True, "display_only": False, "min": 1.0, "max": 1.0}

    other = W.IndicatorPanel()
    other.from_dict(panel.to_dict())
    p2 = other.build_scan_params(dt.date(2025, 1, 1), dt.date(2025, 6, 1))
    assert p2.finviz_filters["pe"]["min"] == 5.0
    assert p2.finviz_filters["optionable"]["min"] == 1.0


def test_untouched_rows_contribute_no_keys(qapp):
    """80 rows must not mean 80 preset entries when none were touched."""
    import datetime as dt
    panel = W.IndicatorPanel()
    p = panel.build_scan_params(dt.date(2025, 1, 1), dt.date(2025, 6, 1))
    assert p.finviz_filters == {}


def test_sentinel_bounds_become_open_sides(qapp):
    """A row enabled but with Max left at the sentinel must not assert
    `<= 1e12` — harmless for a percentage, wrong for a market cap."""
    import datetime as dt
    panel = W.IndicatorPanel()
    panel.rows["fv_market_cap"].set_enabled(True)
    panel.rows["fv_market_cap"].set_value("fv_min", 1e9)
    p = panel.build_scan_params(dt.date(2025, 1, 1), dt.date(2025, 6, 1))
    assert p.finviz_filters["market_cap"]["max"] is None


# ----------------------------------------------------------------------
# Sweep cadence — the only automatic trigger
# ----------------------------------------------------------------------

class _FakeSettings:
    """In-memory stand-in for QSettings.

    The real one writes to the user's actual registry hive, so a test that
    stamped a run would change what the installed app does at next launch.
    """

    def __init__(self):
        self._d = {}

    def value(self, key, default=None):
        return self._d.get(key, default)

    def setValue(self, key, val):
        self._d[key] = val

    def remove(self, key):
        self._d.pop(key, None)


@pytest.fixture
def win(qapp, store, monkeypatch):
    """Bypass-init MainWindow shell carrying just what the due-check reads.

    Deliberately NOT a real `MainWindow()`: constructing one runs `_startup`,
    which downloads a universe and starts OHLCV threads. These tests are
    about the cadence logic, so the shell declares the handful of attributes
    that logic touches — the same pattern the rest of the suite uses.
    """
    from trade_scanner_fh.gui.main_window import MainWindow
    w = MainWindow.__new__(MainWindow)
    settings = _FakeSettings()
    w._qsettings = lambda: settings
    w._fv_sweep_worker = None
    w._finviz_snapshot_blacklist = set()
    w._blacklist = set()
    w._finviz_universe_symbols = lambda: ["AAA", "BBB", "CCC"]
    w.status = type("S", (), {"showMessage": lambda self, *a, **k: None})()
    # No network: the sweep worker would otherwise hit finviz for real.
    monkeypatch.setattr(ff, "fetch_one",
                        lambda sym, timeout=25.0: (ff.OK, {
                            fs.SYMBOL_COL: sym, "pe": 1.0,
                            fs.FETCHED_COL: pd.Timestamp.now(tz="UTC")}))
    monkeypatch.setattr(config, "FINVIZ_SNAPSHOT_MIN_INTERVAL_SEC", 0.0)
    monkeypatch.setattr(config, "FINVIZ_SNAPSHOT_JITTER_SEC", 0.0)
    return w


def _stamp(w, days_ago):
    from datetime import datetime, timedelta
    w._qsettings().setValue(
        w._FINVIZ_SWEEP_LAST_RUN_KEY,
        (datetime.now() - timedelta(days=days_ago)).isoformat(
            timespec="seconds"))


def test_never_run_is_due(win):
    assert win._is_finviz_sweep_due() is True


def test_within_the_window_is_not_due(win):
    _stamp(win, 6)
    assert win._is_finviz_sweep_due() is False


def test_at_the_window_is_due(win):
    _stamp(win, config.FINVIZ_SNAPSHOT_STALE_DAYS)
    assert win._is_finviz_sweep_due() is True


def test_corrupt_stamp_errs_toward_offering(win):
    win._qsettings().setValue(win._FINVIZ_SWEEP_LAST_RUN_KEY, "garbage")
    assert win._is_finviz_sweep_due() is True


def test_overdue_prompts_but_declining_starts_nothing(win, monkeypatch):
    """The sweep is ~10.8 hours; it must never begin unannounced."""
    from PyQt6.QtWidgets import QMessageBox
    asked = []
    monkeypatch.setattr(QMessageBox, "question", staticmethod(
        lambda *a, **k: (asked.append(a[2]),
                         QMessageBox.StandardButton.No)[1]))
    _stamp(win, 9)
    win._maybe_prompt_finviz_sweep()
    assert asked, "an overdue sweep should have been offered"
    assert "hours" in asked[0]
    assert win._fv_sweep_worker is None


def test_due_but_nothing_stale_stamps_instead_of_nagging(win, monkeypatch):
    """The free earnings scavenge can keep everything current between
    sweeps; asking anyway would be noise."""
    from PyQt6.QtWidgets import QMessageBox
    asked = []
    monkeypatch.setattr(QMessageBox, "question", staticmethod(
        lambda *a, **k: (asked.append(a), QMessageBox.StandardButton.No)[1]))
    fs.merge_rows([{"symbol": s, "fetched_at": pd.Timestamp.now(tz="UTC")}
                   for s in ("AAA", "BBB", "CCC")])
    _stamp(win, 9)
    win._maybe_prompt_finviz_sweep()
    assert not asked
    assert win._is_finviz_sweep_due() is False


def test_not_due_is_silent(win, monkeypatch):
    from PyQt6.QtWidgets import QMessageBox
    asked = []
    monkeypatch.setattr(QMessageBox, "question", staticmethod(
        lambda *a, **k: (asked.append(a), QMessageBox.StandardButton.No)[1]))
    _stamp(win, 1)
    win._maybe_prompt_finviz_sweep()
    assert not asked


def test_stamping_clears_the_due_flag(win):
    assert win._is_finviz_sweep_due() is True
    win._stamp_finviz_sweep_now()
    assert win._is_finviz_sweep_due() is False


def test_start_stamps_before_launching_the_worker():
    """Stamped on START, not completion: a run the user stops still spent
    hours of requests, and re-offering it next launch would nag.

    Asserted on source order rather than by running a sweep - the worker is
    parented to the window, which a bypass-init shell cannot satisfy, and
    spawning a real QThread to check an ordering is not worth the flakiness.
    """
    import inspect
    from trade_scanner_fh.gui.main_window import MainWindow
    src = inspect.getsource(MainWindow._start_finviz_sweep)
    assert src.index("_stamp_finviz_sweep_now") < src.index("worker.start()")


def test_scavenge_is_wired_to_the_earnings_path_not_the_sweep():
    """The two producers are independent: the scavenge rides the earnings
    fill, so every earnings request refreshes attributes for free even if
    the sweep never runs."""
    import inspect
    from trade_scanner_fh import finviz_fill
    src = inspect.getsource(finviz_fill._fetch_one_ticker)
    assert "last_snapshot()" in src
    assert "queue_row" in src
