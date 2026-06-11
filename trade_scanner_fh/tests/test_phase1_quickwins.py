"""Phase 1 quick wins — version in window title, the `_log_error`
dual-channel error reporter (incl. the panel_skip de-dup filter on
QtLogHandler), and the `_start_worker` wiring helper."""
from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def _no_launch_data_pipeline(monkeypatch):
    """Same guard as test_audit_gui_fixes: a full ``MainWindow()``
    construction must not start the launch-time data pipeline. The
    title test below additionally stubs ``_startup`` outright so no
    universe-refresh QThread is ever spawned (the title is set before
    ``_startup`` runs, so this doesn't shadow what's under test)."""
    from trade_scanner_fh.gui.main_window import MainWindow
    monkeypatch.setattr(MainWindow, "_startup", lambda self: None)
    monkeypatch.setattr(
        MainWindow, "_load_universe_and_update", lambda self: None,
    )


def _shell():
    """MainWindow shell that bypasses __init__ — the helpers under test
    only touch the module logger + ``self.log_panel``."""
    from trade_scanner_fh.gui.main_window import MainWindow
    shell = MainWindow.__new__(MainWindow)  # skip __init__
    shell.log_panel = MagicMock()
    return shell


# ──────────────────────────────────────────────────────────────────────
# Window title carries the package version
# ──────────────────────────────────────────────────────────────────────

def test_window_title_contains_package_version(_qapp):
    from trade_scanner_fh import __version__
    from trade_scanner_fh.gui.main_window import MainWindow

    mw = MainWindow()
    try:
        assert __version__ in mw.windowTitle()
        assert mw.windowTitle() == f"Trading Scanner v{__version__}"
    finally:
        mw.close()
        mw.deleteLater()


# ──────────────────────────────────────────────────────────────────────
# _log_error — one call, both channels (rotating log + GUI panel)
# ──────────────────────────────────────────────────────────────────────

def test_log_error_writes_to_logger_and_panel(_qapp, caplog):
    shell = _shell()
    with caplog.at_level(logging.ERROR, logger="scanner.gui"):
        shell._log_error("unit-test", "operation X failed: boom")

    # Panel gets the message verbatim (user-facing wording preserved)
    shell.log_panel.write_line.assert_called_once_with(
        "operation X failed: boom"
    )
    # Logger record carries the category tag + message at ERROR
    rec = caplog.records[-1]
    assert rec.levelno == logging.ERROR
    assert "unit-test" in rec.getMessage()
    assert "operation X failed: boom" in rec.getMessage()


def test_log_error_with_exception_attaches_traceback(_qapp, caplog):
    shell = _shell()
    caught: Exception | None = None
    try:
        raise ValueError("kapow")
    except ValueError as exc:
        caught = exc  # `exc` is unbound once the except block exits
        with caplog.at_level(logging.ERROR, logger="scanner.gui"):
            shell._log_error("unit-test", f"save failed: {exc}", exc)

    rec = caplog.records[-1]
    assert rec.exc_info is not None, "exc must carry exc_info to the record"
    assert rec.exc_info[1] is caught
    shell.log_panel.write_line.assert_called_once_with("save failed: kapow")


def test_log_error_survives_dead_log_panel(_qapp, caplog):
    """The panel write is guarded — reporting from teardown / crash
    paths must never raise, and the logger half still fires."""
    shell = _shell()
    shell.log_panel.write_line.side_effect = RuntimeError("widget deleted")
    with caplog.at_level(logging.ERROR, logger="scanner.gui"):
        shell._log_error("unit-test", "late failure")  # must not raise
    assert any("late failure" in r.getMessage() for r in caplog.records)


# ──────────────────────────────────────────────────────────────────────
# _log_error + QtLogHandler — panel shows ONE line, not two
# ──────────────────────────────────────────────────────────────────────

class _PanelStub:
    """Minimal LogPanel stand-in that mirrors the real widget's flow:
    ``write_line`` emits ``append_signal``, and every emission is what
    the user would see in the panel (and in the panel's session log).
    Records each line so tests can count panel-visible output exactly."""

    def __init__(self):
        self.lines: list[str] = []
        stub = self

        class _Sig:
            def emit(self, msg):
                stub.lines.append(msg)

        self.append_signal = _Sig()

    def write_line(self, msg: str):
        self.append_signal.emit(msg)


@pytest.fixture
def _handler_shell(_qapp, tmp_path, monkeypatch):
    """Shell with the REAL handler stack installed (QtLogHandler +
    panel_skip filter) against a recording panel stub. LOG_DIR is
    redirected so the subsystem file handlers write under tmp. Every
    handler is detached on teardown — leaked handlers on the global
    `scanner` logger would push later tests' log lines into this
    test's stub (the same hazard closeEvent guards against)."""
    from trade_scanner_fh import config
    from trade_scanner_fh.gui.main_window import MainWindow

    monkeypatch.setattr(config, "LOG_DIR", tmp_path)
    shell = MainWindow.__new__(MainWindow)  # skip __init__
    shell.log_panel = _PanelStub()
    shell._install_log_handler()
    yield shell
    logging.getLogger("scanner").removeHandler(shell._gui_log_handler)
    for logger_name, h in zip(
        ("scanner.data", "scanner.universe", "scanner.tradestation"),
        shell._subsystem_log_handlers,
    ):
        logging.getLogger(logger_name).removeHandler(h)
        h.close()


def test_log_error_emits_exactly_one_panel_line(_handler_shell, caplog):
    """The double-emit regression: `_log_error` writes the message to
    the panel AND logs at ERROR on `scanner.gui`; QtLogHandler (on the
    parent `scanner` logger) used to re-emit the formatted record —
    plus traceback — into the SAME panel, so every site showed twice.
    The panel_skip filter must hold it to ONE clean verbatim line."""
    shell = _handler_shell
    caught: Exception | None = None
    try:
        raise ValueError("kapow")
    except ValueError as exc:
        caught = exc
        with caplog.at_level(logging.ERROR, logger="scanner.gui"):
            shell._log_error("unit-test", f"save failed: {exc}", exc)

    # Panel: exactly one line, the user-facing message verbatim — no
    # second formatted copy, no traceback text.
    assert shell.log_panel.lines == ["save failed: kapow"]

    # Logger: the record still propagates in full (category tag +
    # traceback) — only the GUI handler drops it.
    rec = caplog.records[-1]
    assert rec.levelno == logging.ERROR
    assert "unit-test" in rec.getMessage()
    assert rec.exc_info is not None
    assert rec.exc_info[1] is caught


def test_log_error_panel_single_line_without_exception(_handler_shell, caplog):
    """Same single-line guarantee on the no-traceback path."""
    shell = _handler_shell
    with caplog.at_level(logging.ERROR, logger="scanner.gui"):
        shell._log_error("unit-test", "operation X failed: boom")
    assert shell.log_panel.lines == ["operation X failed: boom"]
    assert any(
        "operation X failed: boom" in r.getMessage() for r in caplog.records
    )


def test_qt_log_handler_still_emits_untagged_records(_handler_shell):
    """The filter must drop ONLY panel_skip-tagged records — ordinary
    `log.info(...)` / `log.error(...)` lines from workers and modules
    still reach the panel through QtLogHandler as before."""
    shell = _handler_shell
    logging.getLogger("scanner.gui").error("plain failure")
    assert len(shell.log_panel.lines) == 1
    assert "plain failure" in shell.log_panel.lines[0]
    # Formatted by the GUI handler's formatter (level column present)
    assert "ERROR" in shell.log_panel.lines[0]


# ──────────────────────────────────────────────────────────────────────
# _start_worker — connect kwargs, auto log_msg, start, return worker
# ──────────────────────────────────────────────────────────────────────

class _StubSignal:
    def __init__(self):
        self.slots = []

    def connect(self, slot):
        self.slots.append(slot)


class _StubWorker:
    def __init__(self, with_log_msg: bool = True):
        self.finished = _StubSignal()
        self.progress = _StubSignal()
        if with_log_msg:
            self.log_msg = _StubSignal()
        self.started = False
        self.slots_at_start: dict[str, list] | None = None

    def start(self):
        # Snapshot the wiring the instant start() runs — pins the
        # connect-BEFORE-start ordering (a worker that emits as soon as
        # it starts must not lose those early signals to late connects).
        self.slots_at_start = {
            name: list(sig.slots)
            for name, sig in vars(self).items()
            if isinstance(sig, _StubSignal)
        }
        self.started = True


def test_start_worker_wires_connections_and_starts(_qapp):
    shell = _shell()
    worker = _StubWorker()

    def on_finished(*_a):
        pass

    def on_progress(*_a):
        pass

    returned = shell._start_worker(
        worker, finished=on_finished, progress=on_progress,
    )
    assert returned is worker, "helper must return the worker for assignment"
    assert worker.started is True
    assert worker.finished.slots == [on_finished]
    assert worker.progress.slots == [on_progress]
    # log_msg auto-connects to the panel when not supplied by the caller
    assert worker.log_msg.slots == [shell.log_panel.write_line]
    # All of the above must already be wired when start() ran — moving
    # worker.start() above the connect loop would drop early emissions
    assert worker.slots_at_start == {
        "finished": [on_finished],
        "progress": [on_progress],
        "log_msg": [shell.log_panel.write_line],
    }


def test_start_worker_respects_explicit_log_msg_override(_qapp):
    shell = _shell()
    worker = _StubWorker()

    def custom_sink(_msg):
        pass

    shell._start_worker(worker, log_msg=custom_sink)
    assert worker.log_msg.slots == [custom_sink]
    assert worker.started is True
    assert worker.slots_at_start["log_msg"] == [custom_sink]


def test_start_worker_tolerates_worker_without_log_msg(_qapp):
    shell = _shell()
    worker = _StubWorker(with_log_msg=False)
    shell._start_worker(worker, finished=lambda *_a: None)
    assert worker.started is True
    assert len(worker.finished.slots) == 1
    assert worker.slots_at_start["finished"] == worker.finished.slots
