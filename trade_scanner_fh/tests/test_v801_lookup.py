"""v8.0.1 — Lookup mode: the current filters over a typed list of tickers.

User decisions (2026-10-01) these tests pin:
  * Scans → Lookup… opens a dialog with the Manual Input STW form factor;
  * a lookup reads the same stores and the SAME timeframes / custom range /
    Sequenced Run as a scan — only the ticker list differs;
  * "as set": a ticker failing any filter is not shown; "display-only": every
    filter is display-only and no ticker is filtered out;
  * Top X% is never applied in a lookup (it ranks against the whole scan);
  * tickers with no cached OHLCV are reported and skipped;
  * row hides and view toggles apply as for any scan, and the report names
    the passers they removed;
  * lookups never touch scan history, the session counter or the scheduler.
"""
from __future__ import annotations

import dataclasses
from datetime import date

import pandas as pd
import pytest

from trade_scanner_fh import config, data_engine, scanner as S
from trade_scanner_fh.gui import lookup as L
from trade_scanner_fh.scanner import ScanContext, ScanParams, run_scan


# ======================================================================
# Parsing + matching the typed list
# ======================================================================

def test_parse_accepts_commas_newlines_spaces_and_dollar_signs():
    assert L.parse_ticker_list(" aapl, $MSFT;nvda\nTSLA  amd,,AAPL ") == \
        ["AAPL", "MSFT", "NVDA", "TSLA", "AMD"]
    assert L.parse_ticker_list("  ") == []
    assert L.parse_ticker_list("rddt") == ["RDDT"], "a single ticker works"


def test_resolve_maps_share_classes_to_the_store_spelling():
    got = L.resolve_tickers(["BRK.B", "BRK/A", "AAPL", "NOPE"],
                            {"BRK-B", "BRK-A", "AAPL"})
    assert got == [("BRK.B", "BRK-B"), ("BRK/A", "BRK-A"),
                   ("AAPL", "AAPL"), ("NOPE", None)]


# ======================================================================
# lookup_params
# ======================================================================

def _everything_on() -> ScanParams:
    """Every filter switch on, Period Avg / Max gates included, plus two
    finviz rows — the hardest case for the display-only conversion."""
    p = ScanParams()
    for f in dataclasses.fields(p):
        if f.name.endswith("_enabled"):
            setattr(p, f.name, True)
    p.finviz_filters = {
        "pe": {"enabled": True, "display_only": False, "min": 0, "max": 30},
        "beta": {"enabled": False, "display_only": False, "min": 0, "max": 2},
    }
    return p


def test_as_set_only_drops_top_pct_and_never_mutates_the_input():
    p = _everything_on()
    before = dataclasses.asdict(p)
    adj, notes = S.lookup_params(p, display_only=False)
    assert dataclasses.asdict(p) == before, "caller's params untouched"
    assert adj.top_pct_enabled is False
    assert any("Top" in n and "not applied" in n for n in notes)
    assert adj.min_price_enabled and not adj.sma1_display_only
    assert adj.finviz_filters["pe"]["display_only"] is False
    adj.finviz_filters["pe"]["enabled"] = False
    assert p.finviz_filters["pe"]["enabled"] is True, "finviz dict copied"


def test_display_only_leaves_no_filter_stage_at_all():
    """The completeness property: with every switch on, the display-only
    lookup builds ZERO funnel stages — so no ticker can be filtered out."""
    p = _everything_on()
    assert len(S._build_filter_stages(p)) > 40, "sanity: the input filters"
    adj, notes = S.lookup_params(p, display_only=True)
    assert S._build_filter_stages(adj) == []
    assert adj.min_price_enabled is False and adj.top_pct_enabled is False
    assert any("Min Price" in n for n in notes)
    assert adj.finviz_filters["pe"]["display_only"] is True
    assert adj.finviz_filters["beta"] == p.finviz_filters["beta"], \
        "a finviz row that was off stays off"


def test_display_only_does_not_switch_on_filters_that_were_off():
    p = ScanParams(rvol_enabled=False, sma1_enabled=True)
    adj, _ = S.lookup_params(p, display_only=True)
    assert adj.sma1_display_only is True
    assert adj.rvol_enabled is False and adj.rvol_display_only is False


# ======================================================================
# run_scan(lookup=True) on synthetic data
# ======================================================================

def _write(tmp_path, monkeypatch, spec):
    """spec: {symbol: (close_level, bars, volume)}"""
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    data_engine.clear_ohlcv_cache()
    for sym, (level, bars, vol) in spec.items():
        idx = pd.date_range("2026-01-01", periods=bars, freq="B")
        pd.DataFrame({
            "Open": [level + i * 0.1 for i in range(bars)],
            "High": [level + 1 + i * 0.1 for i in range(bars)],
            "Low": [level - 1 + i * 0.1 for i in range(bars)],
            "Close": [level + 0.5 + i * 0.1 for i in range(bars)],
            "Volume": [vol] * bars,
        }, index=idx).to_parquet(tmp_path / f"{sym}.parquet")


def _params(**kw) -> ScanParams:
    base = {f.name: False for f in dataclasses.fields(ScanParams)
            if f.name.endswith("_enabled")}
    base.update(start_date=date(2026, 1, 1), end_date=date(2026, 3, 1))
    base.update(kw)
    return ScanParams(**base)


SPEC = {"HIGH": (300.0, 80, 2_000_000), "LOW": (50.0, 80, 50_000),
        "STUB": (100.0, 1, 1_000_000), "SPY": (500.0, 80, 9_000_000)}


@pytest.fixture
def data(tmp_path, monkeypatch):
    _write(tmp_path, monkeypatch, SPEC)
    return tmp_path


def test_outcomes_name_every_requested_ticker(data):
    p = _params(min_price_enabled=True, min_price_floor=150.0)
    res = run_scan(["HIGH", "LOW", "STUB", "SPY", "QUAR"], p, lookup=True,
                   context=ScanContext(seam_quarantine={"QUAR": None}))
    o = res.outcomes
    assert o["HIGH"] == S.OUTCOME_PASSED
    assert o["LOW"] == S.FAILED_PREFIX + "Min Price ($150)"
    assert o["STUB"] == S.OUTCOME_NO_DATA
    assert o["QUAR"] == S.OUTCOME_QUARANTINED
    assert o["SPY"] == S.OUTCOME_PASSED, "a benchmark the user typed is kept"
    assert set(res.results_df["symbol"]) == {"HIGH", "SPY"}


def test_ordinary_scans_record_nothing_and_still_drop_benchmarks(data):
    res = run_scan(["HIGH", "SPY"], _params())
    assert res.outcomes == {}
    assert list(res.results_df["symbol"]) == ["HIGH"]


def test_errors_are_named(data, monkeypatch):
    real = S._compute_ticker

    def boom(sym, *a, **k):
        if sym == "LOW":
            raise ValueError("bad frame")
        return real(sym, *a, **k)
    monkeypatch.setattr(S, "_compute_ticker", boom)
    res = run_scan(["HIGH", "LOW"], _params(), lookup=True)
    assert res.outcomes["LOW"] == S.ERROR_PREFIX + "bad frame"


def test_top_pct_never_applies_in_a_lookup(data):
    """Over a typed list the percentile would rank the tickers against each
    other — with two tickers and Top 10%, one of them would be cut."""
    p = _params(top_pct_enabled=True, top_pct_cutoff=10.0)
    res = run_scan(["HIGH", "LOW"], p, lookup=True)
    assert set(res.results_df["symbol"]) == {"HIGH", "LOW"}
    assert len(run_scan(["HIGH", "LOW"], p).results_df) == 1, \
        "sanity: an ordinary scan does apply it"


def test_display_only_lookup_shows_failures_coloured_not_dropped(data):
    p = _params(min_price_enabled=True, min_price_floor=150.0,
                avg_vol_enabled=True, avg_vol_min=1_000_000.0)
    adj, _ = S.lookup_params(p, display_only=True)
    res = run_scan(["HIGH", "LOW"], adj, lookup=True)
    df = res.results_df.set_index("symbol")
    assert set(df.index) == {"HIGH", "LOW"}
    assert df.loc["LOW", "_display_only_fails"].get("avg_vol") is True
    high = df.loc["HIGH", "_display_only_fails"]
    # A ticker with no failures carries no flag dict at all (NaN in the frame).
    assert not (isinstance(high, dict) and high.get("avg_vol"))


def test_worker_carries_lookup_outcomes_per_period(_qapp, data):
    from trade_scanner_fh.gui.workers import ScanWorker
    p1 = _params(min_price_enabled=True, min_price_floor=150.0)
    p2 = _params()
    w = ScanWorker(["HIGH", "LOW"], [("1D", p1), ("1W", p2)], lookup=True)
    got = []
    w.finished.connect(got.append)
    w.run()
    (res,) = got
    assert res.lookup is True and res.period_order == ["1D", "1W"]
    assert res.period_outcomes["1D"]["LOW"].startswith(S.FAILED_PREFIX)
    assert res.period_outcomes["1W"]["LOW"] == S.OUTCOME_PASSED


# ======================================================================
# The report
# ======================================================================

def test_report_names_every_outcome():
    req = [("AAPL", "AAPL"), ("BRK.B", "BRK-B"), ("NOPE", None),
           ("LOW", "LOW"), ("STUB", "STUB"), ("HID", "HID"), ("VIEW", "VIEW")]
    outcomes = {"AAPL": "passed", "BRK-B": "passed", "HID": "passed",
                "VIEW": "passed", "LOW": "failed: Min Price ($10)",
                "STUB": S.OUTCOME_NO_DATA}
    lines = L.report_lines(
        display_only=False, requested=req, period_order=["1D"],
        period_outcomes={"1D": outcomes},
        view_split={"1D": ({"AAPL", "BRK-B"}, {"HID"}, {"VIEW"})},
        notes=["Top 10% Gain not applied — …"])
    text = "\n".join(lines)
    assert "filters as set" in text
    assert "BRK.B → BRK-B" in text
    assert "Not in the OHLCV cache (skipped): NOPE" in text
    assert "Note: Top 10% Gain not applied" in text
    assert "[1D] 2 of 6 shown — AAPL, BRK-B" in text
    assert "failed — LOW (Min Price ($10))" in text
    assert "no data in the window — STUB" in text
    assert "passed but hidden by you — HID" in text
    assert "passed but hidden by a view toggle" in text and "VIEW" in text


def test_report_explains_intra_run_omission():
    req = [("A", "A"), ("B", "B")]
    lines = L.report_lines(
        display_only=False, requested=req, period_order=["1D", "1W"],
        period_outcomes={"1D": {"A": "passed", "B": "failed: X"},
                         "1W": {"B": "passed"}},
        view_split={"1D": ({"A"}, set(), set()), "1W": ({"B"}, set(), set())},
        intra_run_omit=True)
    assert any("already shown in an earlier period" in ln and "A (1D)" in ln
               for ln in lines), lines


# ======================================================================
# The dialog
# ======================================================================

def test_dialog_matches_the_manual_input_stw_form_factor(_qapp):
    from PyQt6.QtWidgets import QLabel
    d = L.LookupDialog(text="AAPL, msft", display_only=True)
    assert d.minimumWidth() == 500
    m = d.layout().contentsMargins()
    assert (m.left(), m.top(), m.right(), m.bottom()) == (30, 20, 30, 20)
    assert d.txt.minimumHeight() == 120
    assert any(lbl.text() == "Enter tickers (comma-separated):"
               for lbl in d.findChildren(QLabel))
    assert d.tickers() == ["AAPL", "MSFT"] and d.display_only() is True
    assert L.LookupDialog().display_only() is False, "as set by default"


def test_dialog_refuses_an_empty_list(_qapp):
    d = L.LookupDialog()
    warned = []
    d._warn_empty = lambda: warned.append(1)
    d.btn_go.click()
    assert warned and d.result() == 0
    d.txt.setPlainText("AAPL")
    d.btn_go.click()
    assert d.result() == 1


# ======================================================================
# The window
# ======================================================================

@pytest.fixture
def window(_qapp, tmp_parquets, monkeypatch):
    from trade_scanner_fh.gui import main_window as mw_mod
    monkeypatch.setattr(mw_mod, "PRESETS_DIR", tmp_parquets / "presets")
    (tmp_parquets / "presets").mkdir(exist_ok=True)
    w = mw_mod.MainWindow()
    yield w
    w.close()
    w.deleteLater()


def _capture_worker(w, monkeypatch):
    started = {}

    def fake_start(worker, **slots):
        started["worker"] = worker
        return worker
    monkeypatch.setattr(w, "_start_worker", fake_start)
    return started


def test_scans_menu_has_lookup(window):
    menus = {a.text(): a for a in window.menuBar().actions()}
    texts = [a.text() for a in menus["Scans"].menu().actions()]
    assert "Lookup…" in texts


def test_run_lookup_uses_the_scan_periods_and_converts_filters(window,
                                                               monkeypatch):
    from trade_scanner_fh.gui import main_window as mw_mod
    monkeypatch.setattr(mw_mod, "cached_symbols", lambda: {"AAPL", "BRK-B"})
    for d, chk in window.tf_checks.items():
        chk.setChecked(d in (1, 30))
    window.chk_omit_intra_run.setChecked(True)
    started = _capture_worker(window, monkeypatch)
    assert window._run_lookup(["AAPL", "BRK.B", "NOPE"], display_only=True)
    wk = started["worker"]
    assert wk.lookup is True
    assert wk.symbols == ["AAPL", "BRK-B"], "uncached NOPE skipped"
    assert [lbl for lbl, _p in wk.params_list] == ["1D", "1M"], \
        "same periods a scan would run"
    assert all(S._build_filter_stages(p) == [] for _l, p in wk.params_list)
    assert wk.omit_intra_run is False, "display-only ignores Omit intra-run"
    window._worker = None

    started.clear()
    assert window._run_lookup(["AAPL"], display_only=False)
    assert started["worker"].omit_intra_run is True, "as set honours it"
    assert all(not p.top_pct_enabled for _l, p in started["worker"].params_list)


def test_run_lookup_matches_the_scan_param_builder(window, monkeypatch):
    """As set, every period's params equal the scan's own except Top X%."""
    from trade_scanner_fh.gui import main_window as mw_mod
    monkeypatch.setattr(mw_mod, "cached_symbols", lambda: {"AAPL"})
    window.indicator_panel.rows["top_pct"].set_enabled(True)
    tfs, _seq = window._scan_timeframes()
    scan_params = window._scan_params_list(tfs)
    started = _capture_worker(window, monkeypatch)
    window._run_lookup(["AAPL"], display_only=False)
    for (l1, p1), (l2, p2) in zip(scan_params, started["worker"].params_list):
        assert l1 == l2
        a, b = dataclasses.asdict(p1), dataclasses.asdict(p2)
        assert a.pop("top_pct_enabled") is True and b.pop("top_pct_enabled") is False
        assert a == b


def test_run_lookup_blocks_when_nothing_is_cached_or_a_scan_runs(window,
                                                                 monkeypatch):
    from trade_scanner_fh.gui import main_window as mw_mod
    monkeypatch.setattr(mw_mod, "cached_symbols", lambda: set())
    blocked, lines = [], []
    monkeypatch.setattr(window, "_notify_scan_blocked",
                        lambda title, msg, **k: blocked.append(title))
    window.log_panel.write_line = lines.append
    started = _capture_worker(window, monkeypatch)
    assert window._run_lookup(["ZZZZ"], display_only=False) is False
    assert blocked == ["Lookup"] and not started
    assert any("Not in the OHLCV cache (skipped): ZZZZ" in ln for ln in lines)
    window._worker = object()
    assert window._run_lookup(["ZZZZ"], display_only=False) is False
    assert blocked[-1] == "Scan Running"
    window._worker = None
    assert window._run_lookup([], display_only=False) is False
    assert blocked[-1] == "Lookup" and not started


def _lookup_result(periods, outcomes):
    from trade_scanner_fh.gui.workers import WorkerScanResult
    return WorkerScanResult(period_results=periods,
                            period_order=list(periods), lookup=True,
                            period_outcomes=outcomes)


def _frame(syms):
    return pd.DataFrame([{"symbol": s, "close": 10.0 + i, "pct_gain": 1.0}
                         for i, s in enumerate(syms)])


def test_completion_reports_and_leaves_history_and_session_alone(window,
                                                                 monkeypatch):
    from trade_scanner_fh.gui import main_window as mw_mod
    monkeypatch.setattr(
        mw_mod.scan_history, "record_scan_results",
        lambda *a, **k: pytest.fail("a lookup must not touch scan history"))
    window._session_scan_count = 7
    window._lookup_context = {
        "requested": [("AAPL", "AAPL"), ("MSFT", "MSFT"), ("NVDA", "NVDA"),
                      ("NOPE", None)],
        "display_only": False, "notes": [], "intra_run_omit": False}
    window._hidden_row_symbols = {"MSFT"}
    lines = []
    window.log_panel.write_line = lines.append
    window._on_scan_done(_lookup_result(
        {"1D": _frame(["AAPL", "MSFT"])},
        {"1D": {"AAPL": "passed", "MSFT": "passed",
                "NVDA": "failed: Min Price ($10)"}}))
    assert window._session_scan_count == 7, "session counter untouched"
    assert window._lookup_context is None and window._worker is None
    text = "\n".join(lines)
    assert "[1D] 1 of 3 shown — AAPL" in text
    assert "passed but hidden by you — MSFT" in text
    assert "failed — NVDA (Min Price ($10))" in text
    assert "Not in the OHLCV cache (skipped): NOPE" in text
    assert window.summary_label.text().strip() == \
        "Lookup (as set): 1 of 3 shown | 1 not cached"
    assert set(window.results_table.get_symbols()) == {"AAPL"}


def test_an_ordinary_scan_still_records_history(window, monkeypatch):
    from trade_scanner_fh.gui import main_window as mw_mod
    from trade_scanner_fh.gui.workers import WorkerScanResult
    seen = []
    monkeypatch.setattr(mw_mod.scan_history, "record_scan_results",
                        lambda preset, syms: seen.append(syms) or {})
    window._session_scan_count = 0
    window._on_scan_done(WorkerScanResult(
        period_results={"1D": _frame(["AAPL"])}, period_order=["1D"]))
    assert seen == [{"1D": ["AAPL"]}] and window._session_scan_count == 1


# ======================================================================
# Pre-lookup earnings refresh (v8.0.1, second addition)
# ======================================================================

from PyQt6.QtCore import QObject, pyqtSignal


class _FakeFill(QObject):
    """Stands in for a fill worker: a real `finished` signal, the list-style
    stop flag all three real workers use, and request_stop."""
    finished = pyqtSignal(int, int)

    def __init__(self):
        super().__init__()
        self._stop = [False]

    def request_stop(self):
        self._stop[0] = True

    def isRunning(self):
        return False


def test_dialog_refresh_boxes_are_exclusive_but_both_may_be_off(_qapp):
    d = L.LookupDialog()
    assert d.refresh() is None
    d.chk_refresh_finviz.setChecked(True)
    assert d.refresh() == L.REFRESH_FINVIZ
    d.chk_refresh_all.setChecked(True)
    assert not d.chk_refresh_finviz.isChecked() and d.refresh() == L.REFRESH_ALL
    d.chk_refresh_all.setChecked(False)
    assert d.refresh() is None
    assert L.LookupDialog(refresh=L.REFRESH_ALL).refresh() == L.REFRESH_ALL
    assert L.REFRESH_SOURCES[L.REFRESH_ALL] == ("finviz", "zacks", "finnhub")


@pytest.fixture
def refreshing(window, monkeypatch):
    """Window with fake fill starters and a recorded _run_lookup."""
    from trade_scanner_fh.gui import main_window as mw_mod
    monkeypatch.setattr(mw_mod, "cached_symbols", lambda: {"AAPL", "MSFT"})
    calls = {"finviz": [], "smart": [], "lookup": []}
    window._earn_threads_active = lambda: False

    def start_finviz(symbols, skip, *, mode, label):
        calls["finviz"].append((list(symbols), mode))
        window._finviz_worker = _FakeFill()

    def launch(symbols, *, due=True, include_finnhub=True):
        calls["smart"].append((list(symbols), due, include_finnhub))
        for attr in ("_finviz_worker", "_zacks_worker", "_finnhub_worker"):
            setattr(window, attr, _FakeFill())

    window._start_finviz_worker = start_finviz
    window._launch_smart_refresh_workers = launch
    window._run_lookup = lambda tickers, *, display_only, extra_notes=(): (
        calls["lookup"].append((tickers, display_only, list(extra_notes)))
        or True)
    return window, calls


def test_no_refresh_runs_the_lookup_straight_away(refreshing):
    w, calls = refreshing
    w._start_lookup(["AAPL"], display_only=False, refresh=None)
    assert calls["lookup"] and not calls["finviz"] and not calls["smart"]


def test_finviz_refresh_then_lookup(refreshing):
    w, calls = refreshing
    assert w._start_lookup(["AAPL", "NOPE", "MSFT"], display_only=True,
                           refresh=L.REFRESH_FINVIZ)
    assert calls["finviz"] == [(["AAPL", "MSFT"], "targeted")], \
        "only the tickers the lookup can evaluate"
    assert not calls["lookup"], "the lookup waits for the refresh"
    assert not w.btn_scan.isEnabled() and w.btn_stop.isEnabled()
    w._finviz_worker.finished.emit(2, 0)
    (tickers, display_only, notes), = calls["lookup"]
    assert tickers == ["AAPL", "NOPE", "MSFT"] and display_only is True
    assert "refreshed from finviz for 2 ticker(s)" in notes[0]
    assert w._lookup_refresh is None


def test_all_sources_wait_for_every_source(refreshing):
    w, calls = refreshing
    w._start_lookup(["AAPL"], display_only=False, refresh=L.REFRESH_ALL)
    assert calls["smart"] == [(["AAPL"], False, True)]
    w._finviz_worker.finished.emit(1, 0)
    w._zacks_worker.finished.emit(1, 0)
    assert not calls["lookup"], "finnhub still running"
    w._finnhub_worker.finished.emit(0, 0)
    assert len(calls["lookup"]) == 1
    assert "finviz + zacks + finnhub" in calls["lookup"][0][2][0]


def test_refuses_while_an_earnings_fill_runs(refreshing, monkeypatch):
    w, calls = refreshing
    blocked = []
    monkeypatch.setattr(w, "_notify_scan_blocked",
                        lambda title, msg, **k: blocked.append(title))
    w._earn_threads_active = lambda: True
    assert w._start_lookup(["AAPL"], display_only=False,
                           refresh=L.REFRESH_FINVIZ) is False
    assert blocked == ["Earnings Refresh Running"]
    assert not calls["finviz"] and not calls["lookup"]


def test_refuses_a_second_lookup_while_one_waits(refreshing, monkeypatch):
    w, calls = refreshing
    blocked = []
    monkeypatch.setattr(w, "_notify_scan_blocked",
                        lambda title, msg, **k: blocked.append(title))
    w._start_lookup(["AAPL"], display_only=False, refresh=L.REFRESH_FINVIZ)
    assert w._start_lookup(["MSFT"], display_only=False,
                           refresh=L.REFRESH_ALL) is False
    assert blocked == ["Scan Running"] and len(calls["finviz"]) == 1


def test_scan_stop_cancels_the_whole_lookup(refreshing):
    w, calls = refreshing
    w._start_lookup(["AAPL"], display_only=False, refresh=L.REFRESH_ALL)
    workers = [w._finviz_worker, w._zacks_worker, w._finnhub_worker]
    w._stop_scan()
    assert all(x._stop[0] for x in workers), "every refresh source stopped"
    for x in workers:
        x.finished.emit(0, 0)
    assert not calls["lookup"], "cancelled: no lookup"
    assert w.btn_scan.isEnabled() and w._lookup_refresh is None
    assert "cancelled" in w.summary_label.text()


def test_panel_stop_still_runs_the_lookup_with_a_note(refreshing):
    w, calls = refreshing
    w._start_lookup(["AAPL"], display_only=False, refresh=L.REFRESH_FINVIZ)
    w._finviz_worker.request_stop()       # what Stop Earnings Refresh does
    w._finviz_worker.finished.emit(0, 0)
    (_t, _d, notes), = calls["lookup"]
    assert "stopped part-way" in notes[0]


def test_refresh_with_nothing_cached_just_reports(refreshing):
    w, calls = refreshing
    w._start_lookup(["NOPE"], display_only=False, refresh=L.REFRESH_FINVIZ)
    assert not calls["finviz"] and calls["lookup"] == [(["NOPE"], False, [])]


def test_dialog_choice_is_remembered_and_passed_on(refreshing, monkeypatch):
    w, calls = refreshing

    def fake_exec(dlg):
        dlg.txt.setPlainText("AAPL")
        dlg.chk_refresh_finviz.setChecked(True)
        return 1
    monkeypatch.setattr(L.LookupDialog, "exec", fake_exec)
    w._open_lookup_dialog()
    assert w._lookup_last_refresh == L.REFRESH_FINVIZ and calls["finviz"]
