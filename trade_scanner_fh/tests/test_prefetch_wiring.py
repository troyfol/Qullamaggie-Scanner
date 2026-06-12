"""Wiring tests for the launch-time OHLCV cache prefetch (F5).

config-side validation of PREFETCH_OHLCV_AT_LAUNCH lives in
test_user_config.py and the data_engine.prefetch_ohlcv engine tests in
test_data_engine.py. This module covers the GUI glue added on top:

  - PrefetchWorker (gui/workers.py): runs the warmer, honors
    request_stop, never raises, always emits finished(warmed, total).
  - MainWindow._maybe_start_ohlcv_prefetch: gated on the config flag
    (default OFF), one-shot per launch, prefixes the reference tickers.
  - Source pins (same style as the audit-H1 connection pin): the
    prefetch starts only from the two launch-path terminals, the worker
    is in the closeEvent stop sweep, and the Advanced dialog persists
    the toggle.
"""
from __future__ import annotations

import pandas as pd
import pytest

from trade_scanner_fh import config


def _write_tiny_parquets(tmp_path, symbols):
    """Seed one tiny per-symbol parquet each (mirrors test_data_engine)."""
    for i, sym in enumerate(symbols):
        pd.DataFrame(
            {"Close": [100.0 + i]},
            index=pd.to_datetime(["2026-04-01"]),
        ).to_parquet(tmp_path / f"{sym}.parquet")


# ──────────────────────────────────────────────────────────────────────
# PrefetchWorker
# ──────────────────────────────────────────────────────────────────────

def test_prefetch_worker_warms_and_emits_finished(_qapp, tmp_path,
                                                  monkeypatch):
    """Synchronous run() warms the cache for present symbols, skips the
    missing one, and emits finished(warmed, total) + a completion log."""
    from trade_scanner_fh import data_engine
    from trade_scanner_fh.gui.workers import PrefetchWorker

    monkeypatch.setattr(config, "PARQUET_DIR", tmp_path)
    data_engine.clear_ohlcv_cache()
    _write_tiny_parquets(tmp_path, ["AAA", "BBB"])

    captured = {}
    logs = []
    worker = PrefetchWorker(["AAA", "BBB", "MISSING"])
    worker.finished.connect(
        lambda warmed, total: captured.update(warmed=warmed, total=total))
    worker.log_msg.connect(logs.append)
    worker.run()  # synchronous — no thread start

    assert captured == {"warmed": 2, "total": 3}
    assert any("prefetch complete" in m for m in logs)
    data_engine.clear_ohlcv_cache()


def test_prefetch_worker_request_stop_aborts(_qapp, tmp_path, monkeypatch):
    """request_stop() before run(): nothing is warmed, the 'stopped' log
    line fires, and finished still emits (the GUI never waits forever)."""
    from trade_scanner_fh import data_engine
    from trade_scanner_fh.gui.workers import PrefetchWorker

    monkeypatch.setattr(config, "PARQUET_DIR", tmp_path)
    data_engine.clear_ohlcv_cache()
    _write_tiny_parquets(tmp_path, ["AAA", "BBB"])

    captured = {}
    logs = []
    worker = PrefetchWorker(["AAA", "BBB"])
    worker.request_stop()
    worker.finished.connect(
        lambda warmed, total: captured.update(warmed=warmed, total=total))
    worker.log_msg.connect(logs.append)
    worker.run()

    assert captured == {"warmed": 0, "total": 2}
    assert any("prefetch stopped" in m for m in logs)
    data_engine.clear_ohlcv_cache()


def test_prefetch_worker_swallows_engine_exception(_qapp, monkeypatch):
    """An unexpected blow-up inside the warmer must be swallowed
    (error-logged) — finished(0, total) still emits so the worker never
    looks hung to the close sweep."""
    from trade_scanner_fh.gui import workers as workers_mod

    def boom(*a, **kw):
        raise RuntimeError("simulated warmer blow-up")

    monkeypatch.setattr(workers_mod, "prefetch_ohlcv", boom)

    captured = {}
    logs = []
    worker = workers_mod.PrefetchWorker(["AAA"])
    worker.finished.connect(
        lambda warmed, total: captured.update(warmed=warmed, total=total))
    worker.log_msg.connect(logs.append)
    worker.run()  # must not raise

    assert captured == {"warmed": 0, "total": 1}
    assert any("prefetch error" in m for m in logs)


# ──────────────────────────────────────────────────────────────────────
# MainWindow._maybe_start_ohlcv_prefetch (unbound, __new__-style shell)
# ──────────────────────────────────────────────────────────────────────

class _FakeLogPanel:
    def __init__(self):
        self.lines: list[str] = []

    def write_line(self, msg: str) -> None:
        self.lines.append(msg)


def _bare_window(symbols):
    """MainWindow shell via __new__ — _maybe_start_ohlcv_prefetch is
    pure Python (no Qt calls on self), so only its instance inputs are
    needed. _start_worker is stubbed to record without starting a real
    thread."""
    from trade_scanner_fh.gui.main_window import MainWindow

    win = MainWindow.__new__(MainWindow)
    win._symbols = list(symbols)
    win._prefetch_started = False
    win._prefetch_worker = None
    win.log_panel = _FakeLogPanel()
    win._started_workers = []
    win._start_worker = lambda worker, **conn: (
        win._started_workers.append(worker) or worker)
    return win


def test_maybe_start_prefetch_off_by_default(_qapp, monkeypatch):
    """Flag OFF (the baked-in default) → no worker, no one-shot latch,
    no log chatter."""
    from trade_scanner_fh.gui.main_window import MainWindow

    monkeypatch.setattr(config, "PREFETCH_OHLCV_AT_LAUNCH", False)
    win = _bare_window(["AAA"])
    MainWindow._maybe_start_ohlcv_prefetch(win)
    assert win._started_workers == []
    assert win._prefetch_started is False
    assert win.log_panel.lines == []


def test_maybe_start_prefetch_starts_once_and_appends_refs(_qapp,
                                                           monkeypatch):
    """Flag ON → one PrefetchWorker over the cached symbols plus the
    reference benchmarks (deduped, refs appended). A second call is a
    no-op (one-shot per launch)."""
    from trade_scanner_fh.gui.main_window import MainWindow
    from trade_scanner_fh.gui.workers import PrefetchWorker

    monkeypatch.setattr(config, "PREFETCH_OHLCV_AT_LAUNCH", True)
    monkeypatch.setattr(config, "REFERENCE_TICKERS", ["SPY", "ONEQ"])
    win = _bare_window(["AAA", "SPY"])

    MainWindow._maybe_start_ohlcv_prefetch(win)
    assert len(win._started_workers) == 1
    worker = win._started_workers[0]
    assert isinstance(worker, PrefetchWorker)
    # SPY already in the symbol list — not duplicated; ONEQ appended.
    assert worker.symbols == ["AAA", "SPY", "ONEQ"]
    assert win._prefetch_started is True
    assert any("Prefetching OHLCV cache for 3" in m
               for m in win.log_panel.lines)

    # One-shot: e.g. a later manual update's _on_update_done re-entry.
    MainWindow._maybe_start_ohlcv_prefetch(win)
    assert len(win._started_workers) == 1


def test_maybe_start_prefetch_no_symbols_no_latch(_qapp, monkeypatch):
    """Nothing to warm (no cached symbols, no refs) → no worker AND the
    one-shot latch stays unarmed so a later terminal can still start it."""
    from trade_scanner_fh.gui.main_window import MainWindow

    monkeypatch.setattr(config, "PREFETCH_OHLCV_AT_LAUNCH", True)
    monkeypatch.setattr(config, "REFERENCE_TICKERS", [])
    win = _bare_window([])
    MainWindow._maybe_start_ohlcv_prefetch(win)
    assert win._started_workers == []
    assert win._prefetch_started is False


# ──────────────────────────────────────────────────────────────────────
# Source pins — launch sequencing, close sweep, dialog persistence
# ──────────────────────────────────────────────────────────────────────

def _main_window_source() -> str:
    import trade_scanner_fh.gui.main_window as mw
    return open(mw.__file__, encoding="utf-8").read()


def _method_body(text: str, name: str) -> str:
    start = text.index(f"    def {name}(")
    ends = [i for i in (text.find("\n    def ", start + 1),
                        text.find("\ndef ", start + 1)) if i != -1]
    return text[start:min(ends)] if ends else text[start:]


def test_pin_prefetch_starts_only_from_launch_terminals():
    """The prefetch must start strictly AFTER the startup OHLCV update
    finished (in _on_update_done) or was skipped (the cache-current
    early return in _load_universe_and_update) — never alongside a
    running startup UpdateWorker. Exactly two call sites."""
    text = _main_window_source()
    assert "_maybe_start_ohlcv_prefetch()" in _method_body(
        text, "_on_update_done")
    assert "_maybe_start_ohlcv_prefetch()" in _method_body(
        text, "_load_universe_and_update")
    assert text.count("self._maybe_start_ohlcv_prefetch()") == 2


def test_pin_close_event_stops_prefetch_worker():
    """closeEvent's stop sweep (request_stop + wait) must include the
    prefetch worker so app close doesn't tear down a QThread mid-read."""
    body = _method_body(_main_window_source(), "closeEvent")
    assert "_prefetch_worker" in body


def test_pin_advanced_dialog_persists_prefetch_flag():
    """The Advanced settings dialog must round-trip the toggle through
    save_user_config — omitting it would silently revert a user's saved
    True to the baked-in default (full-state save semantics)."""
    body = _method_body(_main_window_source(), "_show_advanced_settings")
    assert '"PREFETCH_OHLCV_AT_LAUNCH": chk_prefetch.isChecked()' in body
    assert "config.PREFETCH_OHLCV_AT_LAUNCH" in body  # checkbox seeding
