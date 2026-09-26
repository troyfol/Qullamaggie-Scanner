"""
F2 — watchlist diffing + undo for deleted rows.

Part A: scan_history core — diff math, atomic persistence round-trip,
        corrupt-file = no-prior, adhoc vs named preset keys, summary
        bounding, log-line formatting. No Qt required.
Part B: GUI glue — `_apply_watchlist_diff` populates the "Chg" column
        and writes log-panel lines; the Chg column renders via
        `_build_dynamic_columns`; preset-key resolution.
Part C: undo — delete→undo restores rows and order; undo cleared by a
        new scan; double-undo no-op; Ctrl+Z emission gating; the pure
        positional-reinsert helper.
"""

import json
import threading
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from trade_scanner_fh import config, scan_history
from trade_scanner_fh.scan_history import (
    ADHOC_KEY, LATEST_MAX_AGE_DAYS, SUMMARY_MAX_ENTRIES, ScanDiff,
    diff_and_record, is_one_off_period, load_history, prune_latest,
    record_scan_results, save_history,
)


@pytest.fixture(autouse=True)
def _no_launch_data_pipeline(monkeypatch):
    """The full-``MainWindow()`` tests below construct a real window to
    exercise the scan-done path end-to-end — they don't test the
    launch-time data pipeline. ``MainWindow.__init__`` calls
    ``_startup()``, which starts a real UniverseRefreshWorker thread
    against the import-time-baked REAL ``config.TICKER_CSV`` (network
    NASDAQ fetch + rewrite of the user's scanner_data/universe.csv when
    stale), and whose queued ``finished`` → ``_load_universe_and_update``
    callback can fire the real launch fill pipeline on a closed window
    whenever a later test processes Qt events (the 0xc0000374
    heap-corruption scenario). Stub BOTH: ``_startup`` so the worker
    thread never starts, and ``_load_universe_and_update`` as
    belt-and-suspenders (same guard as test_audit_gui_fixes.py). These
    tests seed results via ``_on_scan_done_impl`` and need neither."""
    from trade_scanner_fh.gui.main_window import MainWindow
    monkeypatch.setattr(MainWindow, "_startup", lambda self: None)
    monkeypatch.setattr(
        MainWindow, "_load_universe_and_update",
        lambda self, force=False: None,
    )


# ──────────────────────────────────────────────────────────────────────
# Part A — scan_history core
# ──────────────────────────────────────────────────────────────────────

def _hist_path(tmp_path) -> Path:
    return tmp_path / "scan_history.json"


def test_first_run_records_and_reports_no_prior(tmp_path):
    p = _hist_path(tmp_path)
    diffs = record_scan_results("MyPreset", {"1M": ["A", "B"]}, path=p)
    d = diffs["1M"]
    assert d.has_prior is False
    assert d.new == []
    assert d.dropped == []
    # Snapshot persisted for the next run
    data = json.loads(p.read_text(encoding="utf-8"))
    assert data["latest"]["MyPreset"]["1M"]["symbols"] == ["A", "B"]
    assert data["summary"][-1]["preset"] == "MyPreset"
    assert data["summary"][-1]["period"] == "1M"
    assert data["summary"][-1]["count"] == 2


def test_second_run_diffs_new_and_dropped(tmp_path):
    p = _hist_path(tmp_path)
    record_scan_results("MyPreset", {"1M": ["A", "B", "C"]}, path=p)
    diffs = record_scan_results("MyPreset", {"1M": ["B", "C", "D"]}, path=p)
    d = diffs["1M"]
    assert d.has_prior is True
    assert d.new == ["D"]
    assert d.dropped == ["A"]
    # Latest snapshot is the SECOND run now (single-level prior)
    data = json.loads(p.read_text(encoding="utf-8"))
    assert data["latest"]["MyPreset"]["1M"]["symbols"] == ["B", "C", "D"]
    assert len(data["summary"]) == 2


def test_diff_preserves_result_order_and_dedups(tmp_path):
    p = _hist_path(tmp_path)
    record_scan_results("X", {"1D": ["A"]}, path=p)
    diffs = record_scan_results(
        "X", {"1D": ["C", "A", "B", "C", "B"]}, path=p,
    )
    d = diffs["1D"]
    # NEW preserves current-scan order; duplicates collapse
    assert d.new == ["C", "B"]
    data = json.loads(p.read_text(encoding="utf-8"))
    assert data["latest"]["X"]["1D"]["symbols"] == ["C", "A", "B"]


def test_persistence_roundtrip_is_atomic(tmp_path, monkeypatch):
    """save_history must go through config.atomic_write_text and the
    result must load back identically."""
    p = _hist_path(tmp_path)
    calls = []
    orig = config.atomic_write_text

    def spy(path, content, encoding="utf-8"):
        calls.append(Path(path))
        orig(path, content, encoding)

    monkeypatch.setattr(config, "atomic_write_text", spy)
    hist = load_history(p)
    diff_and_record(hist, "P", "1W", ["A", "B"], timestamp="t0")
    save_history(hist, p)
    assert calls == [p], "save_history must write via atomic_write_text"
    assert load_history(p) == hist


def test_corrupt_json_treated_as_no_prior(tmp_path):
    p = _hist_path(tmp_path)
    p.write_text("{not valid json!!", encoding="utf-8")
    assert load_history(p) == {
        "version": scan_history.HISTORY_VERSION, "latest": {}, "summary": [],
    }
    # Recording over the corrupt file works and reports no-prior
    diffs = record_scan_results("P", {"1M": ["A"]}, path=p)
    assert diffs["1M"].has_prior is False
    # File is healed (valid JSON) afterwards
    data = json.loads(p.read_text(encoding="utf-8"))
    assert data["latest"]["P"]["1M"]["symbols"] == ["A"]


def test_wrong_shape_json_treated_as_no_prior(tmp_path):
    p = _hist_path(tmp_path)
    # Top-level list instead of dict
    p.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
    h = load_history(p)
    assert h["latest"] == {} and h["summary"] == []
    # Dict with wrong-typed slots
    p.write_text(
        json.dumps({"latest": "nope", "summary": {"also": "nope"}}),
        encoding="utf-8",
    )
    h = load_history(p)
    assert h["latest"] == {} and h["summary"] == []
    # Missing file
    assert load_history(tmp_path / "absent.json")["latest"] == {}


def test_adhoc_and_named_preset_keys_are_independent(tmp_path):
    p = _hist_path(tmp_path)
    record_scan_results(ADHOC_KEY, {"1M": ["A", "B"]}, path=p)
    # Same period, named preset: must NOT see the adhoc run as prior
    diffs = record_scan_results("Named", {"1M": ["A", "B"]}, path=p)
    assert diffs["1M"].has_prior is False
    # And the adhoc snapshot is untouched by the named run
    diffs2 = record_scan_results(ADHOC_KEY, {"1M": ["B", "C"]}, path=p)
    assert diffs2["1M"].has_prior is True
    assert diffs2["1M"].new == ["C"]
    assert diffs2["1M"].dropped == ["A"]


def test_periods_within_preset_are_independent(tmp_path):
    p = _hist_path(tmp_path)
    record_scan_results("P", {"1D": ["A"], "1M": ["B"]}, path=p)
    diffs = record_scan_results("P", {"1D": ["A"], "1M": ["A"]}, path=p)
    assert diffs["1D"].new == [] and diffs["1D"].dropped == []
    assert diffs["1M"].new == ["A"] and diffs["1M"].dropped == ["B"]


def test_summary_is_bounded(tmp_path):
    p = _hist_path(tmp_path)
    hist = load_history(p)
    for i in range(SUMMARY_MAX_ENTRIES + 25):
        diff_and_record(hist, "P", "1M", [f"S{i}"], timestamp=f"t{i}")
    save_history(hist, p)
    data = json.loads(p.read_text(encoding="utf-8"))
    assert len(data["summary"]) == SUMMARY_MAX_ENTRIES
    # Newest entries are the survivors
    assert data["summary"][-1]["timestamp"] == f"t{SUMMARY_MAX_ENTRIES + 24}"
    assert data["summary"][0]["timestamp"] == "t25"


def test_is_one_off_period_label_shapes():
    # Sequenced-run chunk / custom-range / date-picker fallback labels
    assert is_one_off_period("2025-01-01 → 2025-02-01") is True
    assert is_one_off_period("Custom 2025-01-01 → 2025-06-01") is True
    assert is_one_off_period("2025-01-01 → 2025-06-01") is True
    # The fixed timeframe labels are diffable
    for label in ("1D", "1W", "1M", "3M", "6M", "45D"):
        assert is_one_off_period(label) is False


def test_one_off_labels_are_not_recorded(tmp_path):
    """Sequenced/custom date-stamped labels must not become permanent
    'latest' keys (each would be a never-diffable dead entry) nor emit
    'no prior run' noise — only the repeatable labels are processed."""
    p = _hist_path(tmp_path)
    diffs = record_scan_results("P", {
        "2025-01-01 → 2025-02-01": ["A", "B"],
        "Custom 2025-01-01 → 2025-06-01": ["C"],
        "1D": ["D"],
    }, path=p)
    assert set(diffs.keys()) == {"1D"}
    data = json.loads(p.read_text(encoding="utf-8"))
    assert set(data["latest"]["P"].keys()) == {"1D"}
    assert [e["period"] for e in data["summary"]] == ["1D"]


def test_all_one_off_run_writes_nothing(tmp_path):
    """A pure sequenced run (every chunk label one-off) records nothing
    and doesn't even create the store file."""
    p = _hist_path(tmp_path)
    diffs = record_scan_results(
        "P",
        {"2025-01-01 → 2025-02-01": ["A"], "2025-02-01 → 2025-03-01": ["B"]},
        path=p,
    )
    assert diffs == {}
    assert not p.exists()


def test_prune_latest_drops_stale_and_malformed_entries():
    old_ts = (datetime.now() - timedelta(days=LATEST_MAX_AGE_DAYS + 1)
              ).isoformat(timespec="seconds")
    fresh_ts = datetime.now().isoformat(timespec="seconds")
    hist = {
        "version": 1,
        "latest": {
            "DeadPreset": {"1M": {"timestamp": old_ts, "symbols": ["X"]}},
            "Live": {
                "1M": {"timestamp": fresh_ts, "symbols": ["A"]},
                "1W": {"timestamp": old_ts, "symbols": ["B"]},
                "broken": "not-a-dict",
            },
        },
        "summary": [],
    }
    removed = prune_latest(hist)
    assert removed == 3
    # Preset key emptied by pruning is dropped entirely (orphan cleanup)
    assert "DeadPreset" not in hist["latest"]
    assert set(hist["latest"]["Live"].keys()) == {"1M"}
    # Idempotent — nothing left to prune
    assert prune_latest(hist) == 0


def test_record_pass_prunes_and_treats_stale_prior_as_no_prior(tmp_path):
    """record_scan_results prunes BEFORE diffing: a baseline older than
    LATEST_MAX_AGE_DAYS yields a fresh 'no prior run', and orphaned
    preset keys are dropped from the persisted store."""
    p = _hist_path(tmp_path)
    old_ts = (datetime.now() - timedelta(days=LATEST_MAX_AGE_DAYS + 1)
              ).isoformat(timespec="seconds")
    save_history({
        "version": 1,
        "latest": {
            "P": {"1M": {"timestamp": old_ts, "symbols": ["A", "B"]}},
            "RenamedAway": {"1D": {"timestamp": old_ts, "symbols": ["Z"]}},
        },
        "summary": [],
    }, path=p)

    diffs = record_scan_results("P", {"1M": ["A", "C"]}, path=p)
    assert diffs["1M"].has_prior is False     # stale baseline pruned first

    data = json.loads(p.read_text(encoding="utf-8"))
    assert "RenamedAway" not in data["latest"]
    assert data["latest"]["P"]["1M"]["symbols"] == ["A", "C"]


def test_log_line_no_prior():
    d = ScanDiff(preset="adhoc", period="1M", has_prior=False)
    assert d.log_line() == "vs last adhoc/1M run: no prior run"


def test_log_line_with_new_and_dropped():
    d = ScanDiff(
        preset="P", period="1W", new=["X", "Y"], dropped=["A", "B"],
        has_prior=True,
    )
    line = d.log_line()
    assert line.startswith("vs last P/1W run: 2 new, 2 dropped")
    assert "(DROPPED: A, B)" in line


def test_log_line_caps_dropped_listing():
    dropped = [f"S{i}" for i in range(30)]
    d = ScanDiff(
        preset="P", period="1M", new=[], dropped=dropped, has_prior=True,
    )
    line = d.log_line()
    assert "30 dropped" in line
    assert "S19" in line          # 20th symbol listed
    assert "S20" not in line      # 21st truncated
    assert "+10 more" in line
    # Zero-drop line omits the DROPPED listing entirely
    d2 = ScanDiff(preset="P", period="1M", new=["A"], dropped=[],
                  has_prior=True)
    assert "DROPPED" not in d2.log_line()


# ──────────────────────────────────────────────────────────────────────
# Part B — GUI glue (Chg column + log lines + preset key)
# ──────────────────────────────────────────────────────────────────────

def _row(sym, **extra):
    base = {"symbol": sym, "close": 10.0, "pct_gain": 5.0}
    base.update(extra)
    return base


def _diff_shell(period_results, period_order, preset_text="(select preset)"):
    """Bare MainWindow shell wired with just what _apply_watchlist_diff
    touches: preset combo text, period state, and a log-line collector."""
    from trade_scanner_fh.gui.main_window import MainWindow
    win = MainWindow.__new__(MainWindow)
    win._period_results = period_results
    win._period_order = period_order
    win.preset_combo = MagicMock()
    win.preset_combo.currentText.return_value = preset_text
    win.log_panel = MagicMock()
    win._log_lines = []
    win.log_panel.write_line.side_effect = win._log_lines.append
    return win


def test_apply_watchlist_diff_populates_chg_column(_qapp, tmp_path,
                                                   monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    # First run — no prior: blank Chg everywhere + "no prior run" line.
    df1 = pd.DataFrame([_row("AAA"), _row("BBB"), _row("CCC")])
    win1 = _diff_shell({"1M": df1}, ["1M"])
    win1._apply_watchlist_diff()
    assert "chg" in df1.columns
    assert list(df1["chg"]) == ["", "", ""]
    assert win1._log_lines == ["vs last adhoc/1M run: no prior run"]
    assert (tmp_path / "scan_history.json").exists()

    # Second run — BBB carried over, DDD new, AAA + CCC dropped.
    df2 = pd.DataFrame([_row("BBB"), _row("DDD")])
    win2 = _diff_shell({"1M": df2}, ["1M"])
    win2._apply_watchlist_diff()
    assert list(df2["chg"]) == ["", "NEW"]
    assert win2._log_lines == [
        "vs last adhoc/1M run: 1 new, 2 dropped (DROPPED: AAA, CCC)",
    ]


def test_apply_watchlist_diff_keys_on_preset_name(_qapp, tmp_path,
                                                  monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    df_adhoc = pd.DataFrame([_row("AAA")])
    _diff_shell({"1D": df_adhoc}, ["1D"])._apply_watchlist_diff()

    # Same period under a NAMED preset → independent key, no prior.
    df_named = pd.DataFrame([_row("AAA")])
    win = _diff_shell({"1D": df_named}, ["1D"], preset_text="Momo Setup")
    win._apply_watchlist_diff()
    assert win._log_lines == ["vs last Momo Setup/1D run: no prior run"]

    data = json.loads(
        (tmp_path / "scan_history.json").read_text(encoding="utf-8"))
    assert set(data["latest"].keys()) == {ADHOC_KEY, "Momo Setup"}


def test_apply_watchlist_diff_empty_period_records_all_dropped(
        _qapp, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    df1 = pd.DataFrame([_row("AAA"), _row("BBB")])
    _diff_shell({"1W": df1}, ["1W"])._apply_watchlist_diff()

    win = _diff_shell({"1W": pd.DataFrame()}, ["1W"])
    win._apply_watchlist_diff()
    assert win._log_lines == [
        "vs last adhoc/1W run: 0 new, 2 dropped (DROPPED: AAA, BBB)",
    ]


def test_scan_history_key_adhoc_vs_preset(_qapp):
    from trade_scanner_fh.gui.main_window import MainWindow
    win = MainWindow.__new__(MainWindow)
    win.preset_combo = MagicMock()
    win.preset_combo.currentText.return_value = "(select preset)"
    assert win._scan_history_key() == ADHOC_KEY
    win.preset_combo.currentText.return_value = "  "
    assert win._scan_history_key() == ADHOC_KEY
    win.preset_combo.currentText.return_value = "Breakouts"
    assert win._scan_history_key() == "Breakouts"


def test_chg_column_renders_in_dynamic_columns(_qapp):
    from trade_scanner_fh.gui.widgets import (
        RESULT_COLUMNS, _build_dynamic_columns,
    )
    # The static definition carries the Chg column...
    assert any(k == "chg" for _h, k, _f in RESULT_COLUMNS)
    # ...rendered iff the df actually has the key (present-in-df gate).
    df = pd.DataFrame([_row("AAA", chg="NEW")])
    cols, _, _ = _build_dynamic_columns(df)
    keys = [k for _h, k, _f in cols]
    assert "chg" in keys
    headers = {k: h for h, k, _f in cols}
    assert headers["chg"] == "Chg"
    df_no = pd.DataFrame([_row("AAA")])
    cols_no, _, _ = _build_dynamic_columns(df_no)
    assert "chg" not in [k for _h, k, _f in cols_no]


# ──────────────────────────────────────────────────────────────────────
# Part C — hide / undo for rows (v8.0.0; replaced delete + single-level undo)
# ──────────────────────────────────────────────────────────────────────

def _hide_shell(df, period="P1"):
    """Bare MainWindow shell for the hide -> undo path: real logic, stub
    table, the real `_apply_view_filters`."""
    from trade_scanner_fh.gui.main_window import MainWindow
    win = MainWindow.__new__(MainWindow)
    win._active_period = period
    win._period_results = {period: df}
    win._hidden_row_symbols = set()
    win._deleted_column_keys = set()
    win._hide_undo_stack = []
    win.results_table = MagicMock()
    # A bypass-init shell cannot answer `hasattr` for Qt widgets (Qt raises
    # RuntimeError, not AttributeError), so give the three view toggles
    # the one method `_apply_view_filters` calls.
    off = SimpleNamespace(isChecked=lambda: False)
    win.chk_view_earnings_data_only = off
    win.chk_view_earnings_dates_only = off
    win.chk_view_color_match_only = off
    return win


def _visible(win):
    return list(win._apply_view_filters(
        win._period_results[win._active_period])["symbol"])


def test_hide_then_undo_restores_rows_and_order(_qapp):
    df = pd.DataFrame([_row(s) for s in ["A", "B", "C", "D", "E"]])
    win = _hide_shell(df)

    win._on_rows_hide_requested(["B", "D"])
    assert _visible(win) == ["A", "C", "E"]
    # Hiding never touches the results themselves.
    pd.testing.assert_frame_equal(win._period_results["P1"], df)
    win.results_table.set_hide_undo_available.assert_called_with(True)

    win._on_hide_undo_requested()
    assert _visible(win) == ["A", "B", "C", "D", "E"]
    assert win._hidden_row_symbols == set()
    win.results_table.set_hide_undo_available.assert_called_with(False)


def test_undo_is_multi_level_newest_first(_qapp):
    """Unlike the old single-level delete undo, each Ctrl+Z unhides the
    next most recent batch."""
    df = pd.DataFrame([_row(s) for s in ["A", "B", "C"]])
    win = _hide_shell(df)
    win._on_rows_hide_requested(["A"])
    win._on_rows_hide_requested(["C"])
    win._on_hide_undo_requested()
    assert _visible(win) == ["B", "C"]
    win._on_hide_undo_requested()
    assert _visible(win) == ["A", "B", "C"]


def test_undo_skips_a_batch_already_unhidden_by_hand(_qapp):
    df = pd.DataFrame([_row(s) for s in ["A", "B", "C"]])
    win = _hide_shell(df)
    win._on_rows_hide_requested(["A"])
    win._on_rows_hide_requested(["B"])
    win._unhide_rows(["B"])
    win._on_hide_undo_requested()          # skips the B batch, undoes A
    assert _visible(win) == ["A", "B", "C"]


def test_undo_with_nothing_to_undo_is_noop(_qapp):
    df = pd.DataFrame([_row("A")])
    win = _hide_shell(df)
    win._on_hide_undo_requested()
    assert _visible(win) == ["A"]


def test_hidden_rows_apply_to_every_period(_qapp):
    """One hidden set for the whole run — switching timeframe or exporting
    another period must not bring a hidden ticker back."""
    df = pd.DataFrame([_row(s) for s in ["A", "B"]])
    win = _hide_shell(df)
    win._period_results["P2"] = df.copy()
    win._on_rows_hide_requested(["A"])
    for label in ("P1", "P2"):
        out = win._apply_view_filters(win._period_results[label])
        assert list(out["symbol"]) == ["B"], label


class _FakeScanResult:
    """Duck-typed stand-in for scanner.ScanResult — only the attributes
    _on_scan_done_impl reads."""

    def __init__(self, period_results, period_order):
        self.period_results = period_results
        self.period_order = period_order
        self.errors = []
        self.elapsed_sec = 0.01

    def total_unique_symbols(self):
        syms = set()
        for df in self.period_results.values():
            if df is not None and not df.empty and "symbol" in df.columns:
                syms.update(map(str, df["symbol"]))
        return syms


def test_hides_survive_a_new_scan(_qapp, tmp_path, monkeypatch):
    """End-to-end through the real scan-done path (v8.0.0): a hidden
    ticker stays hidden in the next scan's results, the undo history
    survives with it, and the scan says so in the log."""
    from trade_scanner_fh.gui.main_window import MainWindow
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    mw = MainWindow()
    try:
        df1 = pd.DataFrame([_row(s) for s in ["AAA", "BBB", "CCC"]])
        mw._on_scan_done_impl(_FakeScanResult({"P1": df1}, ["P1"]))
        assert mw._active_period == "P1"

        mw._on_rows_hide_requested(["BBB"])
        assert mw.results_table.hide_undo_available() is True

        lines = []
        mw.log_panel.write_line = lines.append
        df2 = pd.DataFrame([_row(s) for s in ["AAA", "BBB", "CCC"]])
        mw._on_scan_done_impl(_FakeScanResult({"P1": df2}, ["P1"]))
        assert "BBB" in set(mw._period_results["P1"]["symbol"])
        shown = mw.results_table.get_symbols()
        assert "BBB" not in shown and len(shown) == 2
        assert mw.results_table.hide_undo_available() is True
        assert any("Hidden rows" in ln and "BBB" in ln for ln in lines)
        assert "(1 hidden)" in mw.combo_timeframe.itemText(0)

        mw._on_hide_undo_requested()
        assert "BBB" in mw.results_table.get_symbols()

        # The scan-done path also ran the watchlist diff — a hidden ticker
        # is hidden, not absent, so nothing is flagged NEW on its return.
        assert list(mw._period_results["P1"]["chg"]) == ["", "", ""]
        assert (tmp_path / "scan_history.json").exists()
    finally:
        mw.close()
        mw.deleteLater()


def _stop_stub(stopped: bool):
    """Minimal ScanWorker stand-in carrying just the `_stop` event the
    scan-done partial-result guard inspects."""
    stub = SimpleNamespace(_stop=threading.Event())
    if stopped:
        stub._stop.set()
    return stub


def test_stopped_scan_preserves_diff_baseline(_qapp, tmp_path, monkeypatch):
    """A user-Stopped scan emits PARTIAL results; recording them would
    overwrite scan_history.json 'latest' with the truncated set (false
    DROPPED now + false NEW flood next run). The scan-done path must
    skip the diff and leave the prior baseline untouched."""
    from trade_scanner_fh.gui.main_window import MainWindow
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    mw = MainWindow()
    try:
        # Full run establishes the baseline.
        df1 = pd.DataFrame([_row(s) for s in ["AAA", "BBB", "CCC"]])
        mw._worker = _stop_stub(stopped=False)
        mw._on_scan_done_impl(_FakeScanResult({"1M": df1}, ["1M"]))
        p = tmp_path / "scan_history.json"
        baseline = p.read_text(encoding="utf-8")
        assert json.loads(baseline)["latest"][ADHOC_KEY]["1M"]["symbols"] == [
            "AAA", "BBB", "CCC",
        ]

        # Stopped mid-scan: worker's stop event is set, results truncated.
        mw._worker = _stop_stub(stopped=True)
        df2 = pd.DataFrame([_row("AAA")])
        mw._on_scan_done_impl(_FakeScanResult({"1M": df2}, ["1M"]))

        # Store untouched — latest AND summary identical byte-for-byte.
        assert p.read_text(encoding="utf-8") == baseline
        # No Chg stamping happened on the partial frame either.
        assert "chg" not in mw._period_results["1M"].columns
        # The guard must not leave the worker reference behind.
        assert mw._worker is None

        # Next FULL run diffs against the ORIGINAL baseline: BBB + CCC
        # dropped, nothing falsely NEW.
        mw._worker = _stop_stub(stopped=False)
        df3 = pd.DataFrame([_row("AAA")])
        mw._on_scan_done_impl(_FakeScanResult({"1M": df3}, ["1M"]))
        data = json.loads(p.read_text(encoding="utf-8"))
        assert data["latest"][ADHOC_KEY]["1M"]["symbols"] == ["AAA"]
        assert list(mw._period_results["1M"]["chg"]) == [""]
    finally:
        mw.close()
        mw.deleteLater()


def test_crashed_scan_preserves_diff_baseline(_qapp, tmp_path, monkeypatch):
    """A worker crash appends a '<scan>' sentinel error and emits the
    partial period_results gathered so far — same baseline-poisoning
    hazard as a Stop, same skip."""
    from trade_scanner_fh.gui.main_window import MainWindow
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    mw = MainWindow()
    try:
        df1 = pd.DataFrame([_row(s) for s in ["AAA", "BBB"]])
        mw._on_scan_done_impl(_FakeScanResult({"1M": df1}, ["1M"]))
        p = tmp_path / "scan_history.json"
        baseline = p.read_text(encoding="utf-8")

        crashed = _FakeScanResult({"1M": pd.DataFrame([_row("AAA")])}, ["1M"])
        crashed.errors = [{"symbol": "<scan>", "error": "boom",
                           "traceback": "tb"}]
        mw._worker = _stop_stub(stopped=False)   # crash ≠ stop
        mw._on_scan_done_impl(crashed)

        assert p.read_text(encoding="utf-8") == baseline
        assert "chg" not in mw._period_results["1M"].columns
        # Ordinary per-ticker errors must NOT trip the crash guard.
        ok = _FakeScanResult(
            {"1M": pd.DataFrame([_row("AAA"), _row("BBB")])}, ["1M"])
        ok.errors = [{"symbol": "ZZZ", "error": "no data"}]
        mw._on_scan_done_impl(ok)
        data = json.loads(p.read_text(encoding="utf-8"))
        assert data["latest"][ADHOC_KEY]["1M"]["symbols"] == ["AAA", "BBB"]
    finally:
        mw.close()
        mw.deleteLater()


def test_ctrl_z_emits_hide_undo_only_when_available(_qapp):
    from PyQt6.QtCore import QEvent, Qt as _Qt
    from PyQt6.QtGui import QKeyEvent
    from trade_scanner_fh.gui.widgets import ResultsTable

    table = ResultsTable()
    table.populate(pd.DataFrame([_row("A")]))
    hits = []
    table.hide_undo_requested.connect(lambda: hits.append(1))

    evt = QKeyEvent(QEvent.Type.KeyPress, _Qt.Key.Key_Z,
                    _Qt.KeyboardModifier.ControlModifier)
    table.keyPressEvent(evt)
    assert hits == [], "Ctrl+Z with no pending undo must not emit"

    table.set_hide_undo_available(True)
    assert table.hide_undo_available() is True
    evt2 = QKeyEvent(QEvent.Type.KeyPress, _Qt.Key.Key_Z,
                     _Qt.KeyboardModifier.ControlModifier)
    table.keyPressEvent(evt2)
    assert hits == [1]

    # Plain Z (no Ctrl) never triggers
    evt3 = QKeyEvent(QEvent.Type.KeyPress, _Qt.Key.Key_Z,
                     _Qt.KeyboardModifier.NoModifier)
    table.keyPressEvent(evt3)
    assert hits == [1]
