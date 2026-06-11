"""Hotkey settings dialog + cursor-capture countdown.

Used by the Settings → Hotkey Settings... menu entry. The dialog edits a
`HotkeyConfig` (position, delay, cue, end-sequence). Clicking Set Click
Position hides the parent dialog, shows a small countdown widget in the
top-right corner of the primary screen, then captures `QCursor.pos()` —
GLOBAL screen coords across every monitor — when the countdown reaches 0.

Why a countdown instead of click-capture overlay? An earlier overlay
implementation tried to intercept the user's next mouse click anywhere
on the virtual desktop. That approach was unreliable on multi-monitor
setups: WA_TranslucentBackground makes sub-255-alpha pixels click-
through on Windows, single-window virtual-desktop spans land off-screen
on secondaries with mixed DPI, and even per-screen frameless top-most
windows don't always activate / receive input depending on the active
window stack. Cursor polling sidesteps all of that — it doesn't depend
on intercepting any cross-screen events.
"""
from __future__ import annotations

from typing import Optional

from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtGui import QCursor, QGuiApplication
from PyQt6.QtWidgets import (
    QComboBox, QDialog, QDialogButtonBox, QGridLayout, QHBoxLayout,
    QLabel, QPushButton, QSpinBox, QVBoxLayout, QWidget,
)

from ..hotkey import (
    CUE_OPTIONS, END_SEQUENCE_OPTIONS, HotkeyConfig,
)


class PositionCaptureCountdown(QDialog):
    """Cursor-position capture via countdown.

    Shows a small modal dialog in the top-right corner of the primary
    screen with a live cursor-coords readout and a countdown number.
    When the countdown reaches 0, `QCursor.pos()` is captured. Esc or
    the Cancel button aborts.

    `QCursor.pos()` returns global screen coordinates across the entire
    virtual desktop, so this works transparently on any number of
    monitors with any orientation / DPI mix.

    API:
        dlg = PositionCaptureCountdown(parent, seconds=5)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            x, y = dlg.captured_pos
    """

    def __init__(self, parent=None, seconds: int = 5):
        super().__init__(parent)
        self.setWindowTitle("Set Hotkey Click Position")
        self.setModal(True)
        # Stay-on-top so the user can see the countdown while their
        # cursor sits over a target in another window. Combined with
        # the small footprint + corner placement, this leaves the
        # rest of the screen completely usable.
        self.setWindowFlags(
            self.windowFlags() | Qt.WindowType.WindowStaysOnTopHint
        )
        self.setFixedSize(420, 240)

        self.captured_pos: Optional[tuple[int, int]] = None
        self._remaining = max(1, int(seconds))

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 16, 20, 16)
        layout.setSpacing(8)

        instruction = QLabel(
            "Move your cursor over the target field or button.\n"
            "The cursor's screen position will be captured when "
            "the countdown reaches 0."
        )
        instruction.setAlignment(Qt.AlignmentFlag.AlignCenter)
        instruction.setWordWrap(True)
        instruction.setStyleSheet("color: #e0e0e0; font-size: 12px;")
        layout.addWidget(instruction)

        self.lbl_count = QLabel(str(self._remaining))
        self.lbl_count.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_count.setStyleSheet(
            "color: #ff1493; font-size: 56px; font-weight: bold;"
        )
        layout.addWidget(self.lbl_count)

        self.lbl_pos = QLabel("Cursor: —")
        self.lbl_pos.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_pos.setStyleSheet(
            "color: #c000ff; font-size: 13px; font-family: Consolas, "
            "'Courier New', monospace; font-weight: bold;"
        )
        layout.addWidget(self.lbl_pos)

        btn_row = QHBoxLayout()
        btn_row.addStretch()
        self.btn_cancel = QPushButton("Cancel (Esc)")
        self.btn_cancel.clicked.connect(self.reject)
        btn_row.addWidget(self.btn_cancel)
        btn_row.addStretch()
        layout.addLayout(btn_row)

        # 1-second tick for the countdown number.
        self._timer = QTimer(self)
        self._timer.setInterval(1000)
        self._timer.timeout.connect(self._tick)
        # 80ms tick for the live cursor-position readout — fast enough
        # to feel responsive without thrashing the GUI thread.
        self._cursor_timer = QTimer(self)
        self._cursor_timer.setInterval(80)
        self._cursor_timer.timeout.connect(self._update_cursor_label)

    def showEvent(self, ev):
        super().showEvent(ev)
        # Park the dialog in the top-right corner of the primary screen
        # so it doesn't sit over the area the user is targeting. Done
        # in showEvent (not __init__) because the geometry isn't fully
        # resolved until the OS has actually shown the window.
        self._reposition_to_top_right()
        self._update_cursor_label()
        self._timer.start()
        self._cursor_timer.start()

    def closeEvent(self, ev):
        self._timer.stop()
        self._cursor_timer.stop()
        super().closeEvent(ev)

    def keyPressEvent(self, ev):
        if ev.key() == Qt.Key.Key_Escape:
            self.reject()
        else:
            super().keyPressEvent(ev)

    def _reposition_to_top_right(self):
        screen = QGuiApplication.primaryScreen()
        if screen is None:
            return
        avail = screen.availableGeometry()
        margin = 20
        x = avail.right() - self.width() - margin
        y = avail.top() + margin
        self.move(x, y)

    def _tick(self):
        self._remaining -= 1
        if self._remaining <= 0:
            self._timer.stop()
            self._cursor_timer.stop()
            pos = QCursor.pos()
            self.captured_pos = (pos.x(), pos.y())
            self.accept()
        else:
            self.lbl_count.setText(str(self._remaining))

    def _update_cursor_label(self):
        pos = QCursor.pos()
        self.lbl_pos.setText(f"Cursor: ({pos.x()}, {pos.y()})")


# Backwards-compat alias — `_capture_position` used to instantiate a
# class named `PositionCaptureOverlay`. Keeping the name available here
# means future imports (or stale .pyc files in development checkouts)
# don't crash on `ImportError` after the rewrite.
PositionCaptureOverlay = PositionCaptureCountdown


class HotkeySettingsDialog(QDialog):
    """Modal editor for a `HotkeyConfig`.

    Use:
        dlg = HotkeySettingsDialog(current_cfg, parent)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            new_cfg = dlg.result_config()
    """

    # Capture countdown duration (seconds). Starts when the user
    # clicks "Set Click Position…" and gives them time to move the
    # cursor over the target before `QCursor.pos()` is captured.
    CAPTURE_SECONDS = 5

    def __init__(self, cfg: HotkeyConfig, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Hotkey Settings")
        self.setModal(True)
        self.setMinimumWidth(480)

        self._click_xy: Optional[tuple[int, int]] = (
            (cfg.click_x, cfg.click_y) if cfg.has_position else None
        )
        self._return_xy: Optional[tuple[int, int]] = (
            (cfg.return_click_x, cfg.return_click_y)
            if cfg.has_return_position else None
        )

        # Inline-capture state. We deliberately do NOT spawn a nested
        # modal dialog for cursor capture — an earlier implementation
        # did that and the parent dialog's modal exec loop got unwound
        # in some unclear way (Windows + multi-monitor + parent.hide()
        # interaction), leaving the position unset and no log output.
        # An inline countdown owned by THIS dialog avoids the entire
        # nested-modal class of bugs.
        #
        # `_capture_target` ∈ {"primary", "return"} routes the captured
        # cursor coords to the right slot when capture finishes.
        self._capturing: bool = False
        self._capture_target: str = "primary"
        self._capture_remaining: int = 0
        self._capture_timer: QTimer = QTimer(self)
        self._capture_timer.setInterval(1000)
        self._capture_timer.timeout.connect(self._capture_tick)
        self._cursor_timer: QTimer = QTimer(self)
        self._cursor_timer.setInterval(80)
        self._cursor_timer.timeout.connect(self._update_capture_label)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 18, 20, 18)
        layout.setSpacing(12)

        intro = QLabel(
            "Configure the per-row HOTKEY ticker sender. With HOTKEY on, "
            "the configured cue on a results-table row clicks the saved "
            "position, types the row's ticker, then presses the end key."
        )
        intro.setWordWrap(True)
        intro.setStyleSheet("color: #aaa; font-size: 11px;")
        layout.addWidget(intro)

        grid = QGridLayout()
        grid.setColumnStretch(1, 1)
        grid.setHorizontalSpacing(10)
        grid.setVerticalSpacing(8)

        # ── Click position ──
        grid.addWidget(QLabel("Click position:"), 0, 0)
        self.lbl_position = QLabel(self._position_text())
        self.lbl_position.setStyleSheet(
            "color: #e0e0e0; font-family: Consolas, 'Courier New', "
            "monospace;"
        )
        grid.addWidget(self.lbl_position, 0, 1)
        self.btn_set_position = QPushButton("Set Click Position…")
        self.btn_set_position.setToolTip(
            f"Start a {self.CAPTURE_SECONDS}-second countdown. Move "
            "your cursor over the target field/button before the "
            "countdown reaches 0. The cursor's screen position is "
            "captured at zero. Click again to cancel mid-countdown."
        )
        self.btn_set_position.clicked.connect(
            lambda: self._on_set_position_clicked("primary")
        )
        grid.addWidget(self.btn_set_position, 0, 2)

        # ── Send cue ──
        grid.addWidget(QLabel("Send cue:"), 1, 0)
        self.combo_cue = QComboBox()
        for cid, label in CUE_OPTIONS:
            self.combo_cue.addItem(label, cid)
        self._select_combo(self.combo_cue, cfg.cue)
        grid.addWidget(self.combo_cue, 1, 1, 1, 2)

        # ── Delay ──
        grid.addWidget(QLabel("Delay click → type (ms):"), 2, 0)
        self.spin_delay = QSpinBox()
        self.spin_delay.setRange(0, 5000)
        self.spin_delay.setSingleStep(50)
        self.spin_delay.setValue(int(cfg.delay_ms))
        self.spin_delay.setSuffix(" ms")
        self.spin_delay.setToolTip(
            "Pause between the click and typing the ticker. Increase if "
            "the target field needs time to gain focus."
        )
        grid.addWidget(self.spin_delay, 2, 1, 1, 2)

        # ── End sequence ──
        grid.addWidget(QLabel("End sequence:"), 3, 0)
        self.combo_end = QComboBox()
        for eid, label in END_SEQUENCE_OPTIONS:
            self.combo_end.addItem(label, eid)
        self._select_combo(self.combo_end, cfg.end_sequence)
        self.combo_end.setToolTip(
            "Key pressed after the ticker is typed. None = stop after "
            "typing; Enter = standard submit; Ctrl/Shift/Alt+Enter for "
            "platform-specific submit variants."
        )
        grid.addWidget(self.combo_end, 3, 1, 1, 2)

        # ── Return click position (optional) ──
        # Fires AFTER the end-sequence keystroke. Use case: park the
        # cursor back over the scanner so arrow-key navigation through
        # the results table keeps working between sends. Same delay
        # knob applies (post-end-key wait before clicking back).
        grid.addWidget(QLabel("Return click position:"), 4, 0)
        self.lbl_return = QLabel(self._return_text())
        self.lbl_return.setStyleSheet(
            "color: #e0e0e0; font-family: Consolas, 'Courier New', "
            "monospace;"
        )
        grid.addWidget(self.lbl_return, 4, 1)

        return_btn_box = QHBoxLayout()
        return_btn_box.setContentsMargins(0, 0, 0, 0)
        return_btn_box.setSpacing(4)
        self.btn_set_return = QPushButton("Set Return Click…")
        self.btn_set_return.setToolTip(
            f"Optional. Start a {self.CAPTURE_SECONDS}-second "
            "countdown to capture a SECOND click position that fires "
            "AFTER the end-sequence keystroke. Use this to click back "
            "into the scanner so arrow keys keep navigating the table "
            "between sends. Click again to cancel mid-countdown."
        )
        self.btn_set_return.clicked.connect(
            lambda: self._on_set_position_clicked("return")
        )
        return_btn_box.addWidget(self.btn_set_return)

        self.btn_clear_return = QPushButton("Clear")
        self.btn_clear_return.setToolTip(
            "Remove the return click. The send sequence will end after "
            "the end-sequence keystroke."
        )
        self.btn_clear_return.clicked.connect(self._clear_return)
        return_btn_box.addWidget(self.btn_clear_return)

        return_wrap = QWidget()
        return_wrap.setLayout(return_btn_box)
        grid.addWidget(return_wrap, 4, 2)

        layout.addLayout(grid)

        # ── Reset to Defaults ──
        reset_row = QHBoxLayout()
        self.btn_reset = QPushButton("Reset to Defaults")
        self.btn_reset.setToolTip(
            "Clear saved click position and restore the default delay, "
            "cue, and end sequence."
        )
        self.btn_reset.clicked.connect(self._reset_defaults)
        reset_row.addWidget(self.btn_reset)
        reset_row.addStretch()
        layout.addLayout(reset_row)

        # ── OK / Cancel ──
        # Stored on `self` so capture can disable them mid-countdown.
        self.button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel
        )
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        layout.addWidget(self.button_box)

    # ── helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _select_combo(combo: QComboBox, data_value: str) -> None:
        idx = combo.findData(data_value)
        if idx >= 0:
            combo.setCurrentIndex(idx)

    def _position_text(self) -> str:
        if self._click_xy is None:
            return "(not set)"
        x, y = self._click_xy
        return f"({x}, {y})"

    def _return_text(self) -> str:
        if self._return_xy is None:
            return "(not set)"
        x, y = self._return_xy
        return f"({x}, {y})"

    def _clear_return(self) -> None:
        if self._capturing:
            return
        self._return_xy = None
        self.lbl_return.setText(self._return_text())

    # ── Inline cursor capture ─────────────────────────────────────────
    # Same state machine drives both the primary and return-click
    # capture flows. `_capture_target` ∈ {"primary", "return"} routes
    # the captured cursor coords to the right slot when the countdown
    # reaches 0 in `_finish_capture`.

    def _on_set_position_clicked(self, target: str) -> None:
        """Set Position button for either the primary OR return click.
        If a capture is already in progress, treats the click as a
        cancel (whether it's the same target or the other one)."""
        if self._capturing:
            self._finish_capture(captured=False)
            return
        self._start_capture(target)

    def _start_capture(self, target: str) -> None:
        """Begin an inline countdown for the given capture target. The
        dialog stays shown — the user moves the cursor anywhere on
        screen (including over this dialog), and `QCursor.pos()` is
        captured when the timer reaches 0."""
        self._capturing = True
        self._capture_target = target
        self._capture_remaining = self.CAPTURE_SECONDS

        # Lock everything but the (now-Cancel) capture button so the
        # user can't accept the dialog with a half-captured state. The
        # OTHER position's set button is also disabled so the user
        # can't try to start a second capture mid-countdown.
        self.btn_reset.setEnabled(False)
        self.combo_cue.setEnabled(False)
        self.spin_delay.setEnabled(False)
        self.combo_end.setEnabled(False)
        self.btn_clear_return.setEnabled(False)
        for b in self.button_box.buttons():
            b.setEnabled(False)

        if target == "primary":
            self.btn_set_position.setText("Cancel Capture")
            self.btn_set_return.setEnabled(False)
            active_label = self.lbl_position
            other_label = self.lbl_return
        else:  # "return"
            self.btn_set_return.setText("Cancel Capture")
            self.btn_set_position.setEnabled(False)
            active_label = self.lbl_return
            other_label = self.lbl_position

        # Pink/purple styling on the active label so it's obvious
        # which capture is in flight. Other label stays in default
        # style + shows its current value (no overlap).
        active_label.setStyleSheet(
            "color: #ff1493; font-family: Consolas, 'Courier New', "
            "monospace; font-weight: bold;"
        )
        other_label.setStyleSheet(
            "color: #e0e0e0; font-family: Consolas, 'Courier New', "
            "monospace;"
        )

        self._update_capture_label()
        self._capture_timer.start()
        self._cursor_timer.start()

    def _capture_tick(self) -> None:
        """1Hz countdown. On reaching 0, capture the cursor and end."""
        self._capture_remaining -= 1
        if self._capture_remaining <= 0:
            self._finish_capture(captured=True)
        else:
            self._update_capture_label()

    def _update_capture_label(self) -> None:
        """80ms cursor-position readout while capturing. Lets the user
        verify they've parked the cursor where they intend before the
        timer fires. Updates whichever label corresponds to the
        active capture target."""
        pos = QCursor.pos()
        text = (
            f"Capturing in {self._capture_remaining}s — "
            f"Cursor at ({pos.x()}, {pos.y()})"
        )
        if self._capture_target == "primary":
            self.lbl_position.setText(text)
        else:
            self.lbl_return.setText(text)

    def _finish_capture(self, *, captured: bool) -> None:
        """Stop timers and restore UI state. If `captured` is True,
        snapshot `QCursor.pos()` into the active target slot. If False,
        the capture was cancelled and the slot is left unchanged."""
        self._capture_timer.stop()
        self._cursor_timer.stop()
        if captured:
            pos = QCursor.pos()
            new_xy = (pos.x(), pos.y())
            if self._capture_target == "primary":
                self._click_xy = new_xy
            else:
                self._return_xy = new_xy

        # Restore button text, enable everything that was locked.
        self.btn_set_position.setText("Set Click Position…")
        self.btn_set_return.setText("Set Return Click…")
        self.btn_set_position.setEnabled(True)
        self.btn_set_return.setEnabled(True)
        self.btn_clear_return.setEnabled(True)
        self.btn_reset.setEnabled(True)
        self.combo_cue.setEnabled(True)
        self.spin_delay.setEnabled(True)
        self.combo_end.setEnabled(True)
        for b in self.button_box.buttons():
            b.setEnabled(True)
        # Restore default styling and re-render both labels from state.
        self.lbl_position.setStyleSheet(
            "color: #e0e0e0; font-family: Consolas, 'Courier New', "
            "monospace;"
        )
        self.lbl_return.setStyleSheet(
            "color: #e0e0e0; font-family: Consolas, 'Courier New', "
            "monospace;"
        )
        self.lbl_position.setText(self._position_text())
        self.lbl_return.setText(self._return_text())
        self._capturing = False

    def reject(self) -> None:
        """Override so closing the dialog mid-countdown stops the
        timers cleanly and doesn't leave them firing on a deleted
        widget."""
        if self._capturing:
            self._finish_capture(captured=False)
        super().reject()

    def closeEvent(self, ev) -> None:
        """Same teardown for window-close (X button / Esc / system)."""
        if self._capturing:
            self._capture_timer.stop()
            self._cursor_timer.stop()
            self._capturing = False
        super().closeEvent(ev)

    def _reset_defaults(self) -> None:
        defaults = HotkeyConfig()
        self._click_xy = None
        self._return_xy = None
        self.lbl_position.setText(self._position_text())
        self.lbl_return.setText(self._return_text())
        self.spin_delay.setValue(defaults.delay_ms)
        self._select_combo(self.combo_cue, defaults.cue)
        self._select_combo(self.combo_end, defaults.end_sequence)

    def result_config(self) -> HotkeyConfig:
        cx, cy = (None, None) if self._click_xy is None else self._click_xy
        rx, ry = (None, None) if self._return_xy is None else self._return_xy
        return HotkeyConfig(
            click_x=cx,
            click_y=cy,
            delay_ms=int(self.spin_delay.value()),
            cue=self.combo_cue.currentData(),
            end_sequence=self.combo_end.currentData(),
            return_click_x=rx,
            return_click_y=ry,
        )
