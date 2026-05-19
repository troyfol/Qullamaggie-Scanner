"""UI widgets for the Trading Scanner GUI.

Contains reusable components that the main window composes:
  - QtLogHandler      routes Python logging records to a Qt signal
  - IndicatorRow      one toggle + label + N spinboxes for a single indicator
  - IndicatorPanel    scrollable column of all indicator rows
  - NumericSortProxy  QSortFilterProxyModel that sorts numerically by UserRole
  - ResultsTable      sortable scan-results table
  - LogPanel          expandable log panel with persistent disk handle
  - RESULT_COLUMNS    results-table column definitions (header, key, formatter)
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from PyQt6.QtCore import QSortFilterProxyModel, Qt, pyqtSignal, pyqtSlot
from PyQt6.QtGui import QColor, QFont, QStandardItem, QStandardItemModel
from PyQt6.QtWidgets import (
    QCheckBox, QComboBox, QDoubleSpinBox, QHBoxLayout, QHeaderView, QLabel,
    QMenu, QPushButton, QScrollArea, QSpinBox, QTableView, QTextEdit,
    QVBoxLayout, QWidget,
)

from .. import config

log = logging.getLogger("scanner.gui")


# ============================================================================
# NaN-safe coercion helpers
# ============================================================================

def _safe_streak(v) -> int:
    """Coerce a possibly-NaN streak count to a plain int.

    Pandas writes NaN into a column whenever a ticker's row dict didn't
    set that key (typically because the ticker lacks the underlying
    data — e.g., a stock with no Zacks history doesn't get
    `consec_eps_beats`). The naive `int(v or 0)` blew up here: Python
    treats `nan` as truthy (`bool(nan) is True`), so `nan or 0` returns
    `nan` rather than 0, and `int(nan)` raises ValueError. Since
    `populate()` runs on the main GUI thread and isn't wrapped in
    try/except, a single NaN-streak ticker would crash the whole
    table render — and on Windows under PyInstaller windowed mode an
    uncaught slot exception aborts the exe. This helper converts the
    "missing data" sentinel cleanly to 0 so the streak-green logic
    treats those rows as "no streak."
    """
    if v is None:
        return 0
    try:
        if pd.isna(v):
            return 0
    except (TypeError, ValueError):
        # pd.isna on a non-scalar (list, dict) raises — those values
        # are obviously not streak counts; treat as missing.
        return 0
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


# ============================================================================
# Log handler that emits to the GUI
# ============================================================================

class QtLogHandler(logging.Handler):
    """Routes log records to a signal so the GUI can display them."""

    def __init__(self, signal):
        super().__init__()
        self._signal = signal

    def emit(self, record):
        try:
            msg = self.format(record)
            self._signal.emit(msg)
        except Exception:
            pass


# ============================================================================
# Indicator control row widget
# ============================================================================

class IndicatorRow(QWidget):
    """A single indicator: checkbox toggle + label + param spinboxes."""

    # Stylesheet for the Display Only checkbox: red fill on :checked
    # makes the "this row is in display-only mode" state immediately
    # obvious. Border tone matches the dark theme. Mutex with `toggle`
    # is enforced in __init__ so only one of {Filter, Display Only}
    # can be on at a time per row.
    _DISPLAY_ONLY_STYLE = """
        QCheckBox { color: #aaa; }
        QCheckBox::indicator {
            width: 14px; height: 14px;
            border: 1px solid #555; border-radius: 2px;
            background: #3c3c3c;
        }
        QCheckBox::indicator:checked {
            background: #c0392b;
            border: 1px solid #962d22;
        }
    """
    # Threshold-input style applied while a row is in Display Only
    # mode. `setEnabled(False)` alone produces only a "you can't type"
    # state on the dark theme that's easy to misread. Explicit muted
    # foreground + dimmed background makes the state unambiguous.
    _GREYED_INPUT_STYLE = (
        "color: #666; background: #2a2a2a; border: 1px solid #3a3a3a;"
    )

    def __init__(self, label: str, params: list[dict], parent=None,
                 *, display_only_supported: bool = True):
        """
        params: list of dicts, each with keys:
            'name'    — internal key
            'label'   — display label
            'type'    — 'int', 'float', 'checkbox', or 'combo'
            'default' — default value
            'min', 'max', 'step' — optional

        `display_only_supported`: when True (default), a red-styled
        "Display Only" checkbox sits immediately to the right of the
        Filter toggle. The two checkboxes are mutually exclusive: the
        Filter toggle gates whether the indicator's threshold filter is
        applied; Display Only computes the value and surfaces it in
        the results table without filtering. Both can be off (row is
        inert). When Display Only is on, threshold inputs render in
        muted grey to make the inactive state unambiguous. Pass False
        for filters where Display Only would be a no-op (e.g.,
        min_price has no separate value to display, top_pct is purely
        population-level).
        """
        super().__init__(parent)
        self.toggle = QCheckBox()
        self.toggle.setChecked(True)
        self.toggle.setToolTip(
            "Filter: enable this indicator's threshold filter. "
            "Mutually exclusive with Display Only — turning this on "
            "automatically turns Display Only off."
        )

        lbl = QLabel(label)
        lbl.setMinimumWidth(180)
        lbl.setFont(QFont("Segoe UI", 9, QFont.Weight.Bold))

        self.spinboxes: dict[str, QDoubleSpinBox | QSpinBox | QCheckBox] = {}
        # Display-only checkbox lives outside `spinboxes` so it's
        # always findable AND so `set_filter_locked` can leave it
        # interactable while disabling the threshold inputs. None when
        # display-only isn't supported for this row.
        self.display_only: Optional[QCheckBox] = None

        layout = QHBoxLayout(self)
        layout.setContentsMargins(4, 2, 4, 2)
        layout.addWidget(self.toggle)
        # Display Only sits immediately to the right of the Filter
        # toggle so the two are visually grouped at the head of the
        # row. The two-checkbox cluster lets the user see "is this row
        # filtering, displaying, or off?" at a glance — far clearer
        # than the prior layout which buried Display Only at the far
        # right after a stretch.
        if display_only_supported:
            self.display_only = QCheckBox()
            self.display_only.setStyleSheet(self._DISPLAY_ONLY_STYLE)
            self.display_only.setToolTip(
                "Display Only: compute and display this indicator's "
                "value in results, but bypass the filter (every ticker "
                "passes). Mutually exclusive with Filter — turning this "
                "on automatically turns Filter off."
            )
            self.display_only.setFixedWidth(20)
            layout.addWidget(self.display_only)
            # Mutex wiring. Both connections only fire on transitions
            # to checked, so the converse setChecked(False) doesn't
            # ping-pong (the False branch short-circuits the `on and
            # ...` check). Display Only's internal grey-out logic is
            # also called so the row's threshold inputs reflect the
            # new state.
            self.toggle.toggled.connect(self._on_filter_toggled)
            self.display_only.toggled.connect(self._on_display_only_toggled)
        layout.addWidget(lbl)

        for p in params:
            if p["type"] == "checkbox":
                cb = QCheckBox(p["label"])
                cb.setChecked(p.get("default", True))
                cb.setStyleSheet("color: #ccc;")
                self.spinboxes[p["name"]] = cb
                layout.addWidget(cb)
                continue

            if p["type"] == "combo":
                # `choices` is list[(value, display_label)]; the stored
                # value side is what set_value / value() round-trips.
                plbl = QLabel(p["label"] + ":")
                plbl.setStyleSheet("color: #888;")
                layout.addWidget(plbl)
                cb = QComboBox()
                choices = p.get("choices", [])
                for val, label in choices:
                    cb.addItem(label, userData=val)
                default_val = p.get("default")
                if default_val is not None:
                    for i in range(cb.count()):
                        if cb.itemData(i) == default_val:
                            cb.setCurrentIndex(i)
                            break
                cb.setFixedWidth(p.get("width", 160))
                self.spinboxes[p["name"]] = cb
                layout.addWidget(cb)
                continue

            plbl = QLabel(p["label"] + ":")
            plbl.setStyleSheet("color: #888;")
            layout.addWidget(plbl)

            if p["type"] == "int":
                sb = QSpinBox()
                sb.setMinimum(p.get("min", 0))
                sb.setMaximum(p.get("max", 999999))
                sb.setSingleStep(p.get("step", 1))
                sb.setValue(p["default"])
            else:
                sb = QDoubleSpinBox()
                sb.setMinimum(p.get("min", 0.0))
                sb.setMaximum(p.get("max", 999999999.0))
                sb.setSingleStep(p.get("step", 0.01))
                sb.setDecimals(p.get("decimals", 2))
                sb.setValue(p["default"])

            sb.setFixedWidth(100)
            self.spinboxes[p["name"]] = sb
            layout.addWidget(sb)

        layout.addStretch()

    def is_enabled(self) -> bool:
        return self.toggle.isChecked()

    def is_display_only(self) -> bool:
        return self.display_only is not None and self.display_only.isChecked()

    def _on_filter_toggled(self, on: bool):
        """Mutex partner of `_on_display_only_toggled`. When the user
        flips Filter on, Display Only auto-turns off — the two states
        are conceptually exclusive (a row is either filtering OR
        passive-displaying, never both). The False branch is a no-op
        so unchecking Filter doesn't disturb Display Only."""
        if on and self.display_only is not None and self.display_only.isChecked():
            # blockSignals around setChecked so the partner toggle's
            # slot doesn't re-fire and try to undo this transition.
            self.display_only.blockSignals(True)
            try:
                self.display_only.setChecked(False)
            finally:
                self.display_only.blockSignals(False)
            # We bypassed the partner slot, so apply the grey-out
            # rollback ourselves.
            self._apply_display_only_styling(False)

    def _on_display_only_toggled(self, on: bool):
        """Apply the dimmed-input styling that makes the display-only
        state unambiguous, AND enforce the mutex with the Filter
        toggle. Filter auto-turns-off when Display Only goes on."""
        if on and self.toggle.isChecked():
            self.toggle.blockSignals(True)
            try:
                self.toggle.setChecked(False)
            finally:
                self.toggle.blockSignals(False)
        self._apply_display_only_styling(on)

    def _apply_display_only_styling(self, on: bool):
        """Display Only mode no longer greys out threshold inputs —
        the threshold is the cutoff for cell-level red-on-fail
        coloring in the results table. Threshold enablement is
        managed solely by `set_filter_locked` (the EPS/Rev beats-lock
        mutex), so we deliberately do NOT touch `setEnabled` here:
        re-enabling a locked row's inputs would defeat the lock.
        Defensive cleanup only — strip any leftover legacy greyed
        inline stylesheet that earlier versions may have applied."""
        for sb in self.spinboxes.values():
            if sb.styleSheet():
                sb.setStyleSheet("")

    def set_filter_locked(self, locked: bool):
        """Lock down the FILTER aspects of this row (the enabled
        toggle + threshold inputs) while leaving Display-Only
        clickable. Used by the EPS/Rev mutual-exclusion logic so the
        user can still display the individual quarter values
        alongside an active consec-beats filter. Threshold inputs
        stay editable in display-only mode (they're the cutoff for
        red-on-fail coloring)."""
        self.toggle.setEnabled(not locked)
        # Threshold inputs are disabled when locked but stay editable
        # in display-only mode (display-only retains the threshold
        # for cell-level red-on-fail in the results table).
        for sb in self.spinboxes.values():
            sb.setEnabled(not locked)
            if sb.styleSheet():
                sb.setStyleSheet("")
        if self.display_only is not None:
            self.display_only.setEnabled(True)

    def value(self, name: str):
        # Special-case display_only — it lives outside `spinboxes` so
        # it can stay interactable when set_filter_locked greys out
        # the threshold inputs.
        if name == "display_only":
            return self.is_display_only()
        widget = self.spinboxes[name]
        if isinstance(widget, QCheckBox):
            return widget.isChecked()
        if isinstance(widget, QComboBox):
            return widget.currentData()
        return widget.value()

    def set_enabled(self, on: bool):
        self.toggle.setChecked(on)

    def set_value(self, name: str, val):
        if name == "display_only":
            if self.display_only is not None:
                self.display_only.setChecked(bool(val))
            return
        if name in self.spinboxes:
            widget = self.spinboxes[name]
            if isinstance(widget, QCheckBox):
                widget.setChecked(bool(val))
            elif isinstance(widget, QComboBox):
                # Match by stored value (userData), not display label
                for i in range(widget.count()):
                    if widget.itemData(i) == val:
                        widget.setCurrentIndex(i)
                        break
            else:
                widget.setValue(val)


# ============================================================================
# Indicator controls panel
# ============================================================================

class IndicatorPanel(QScrollArea):
    """Scrollable panel with all indicator controls."""

    # Phase 7 §7.2: emitted whenever EITHER consecutive-beats checkbox
    # toggles, with the new "any beats checked" state. Main window
    # connects this to disable / re-enable the Sequenced Run controls.
    beats_filter_toggled = pyqtSignal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        # Import here to avoid a circular import via scanner → indicators →
        # config → ... at module load time.
        from ..scanner import ScanParams
        self._ScanParams = ScanParams

        self.setWidgetResizable(True)
        self.setMinimumWidth(580)
        # No maximum-width cap — the user drags the top splitter to
        # resize the filter pane to whatever width is comfortable on
        # their monitor. The earlier 720px ceiling truncated long
        # rows on wide monitors and forced ellipsis on the surge
        # mode dropdown labels.

        container = QWidget()
        self.vbox = QVBoxLayout(container)
        self.vbox.setSpacing(2)
        self.vbox.setContentsMargins(6, 6, 16, 6)

        # Column-header caption above the row stack so users see at a
        # glance what the two leading checkboxes are. Aligned over the
        # first ~36px of each row (toggle is 16px + display_only 20px,
        # plus the row's left margin of 4px). The caption sits in its
        # own thin row above the first section header.
        col_header = QWidget()
        col_header_layout = QHBoxLayout(col_header)
        col_header_layout.setContentsMargins(8, 0, 0, 0)
        col_header_layout.setSpacing(0)
        small_font = QFont("Segoe UI", 7)
        f_lbl = QLabel("Filter")
        f_lbl.setFont(small_font)
        f_lbl.setStyleSheet("color: #888;")
        f_lbl.setFixedWidth(20)
        f_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        d_lbl = QLabel("Display")
        d_lbl.setFont(small_font)
        d_lbl.setStyleSheet("color: #c0392b;")
        d_lbl.setFixedWidth(28)
        d_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        col_header_layout.addWidget(f_lbl)
        col_header_layout.addWidget(d_lbl)
        col_header_layout.addStretch()
        self.vbox.addWidget(col_header)

        self.rows: dict[str, IndicatorRow] = {}

        # --- Trend Filters ---
        self._section("Trend Filters")

        self._add("sma1", "Price > SMA (1st)", [
            {"name": "period", "label": "Period", "type": "int", "default": 200, "min": 1, "max": 500},
        ])
        self._add("sma2", "Price > SMA (2nd)", [
            {"name": "period", "label": "Period", "type": "int", "default": 50, "min": 1, "max": 500},
        ])
        self._add("sti", "Stockbee Trend Intensity", [
            {"name": "short_lb", "label": "Short", "type": "int", "default": 7, "min": 1, "max": 200},
            {"name": "long_lb", "label": "Long", "type": "int", "default": 65, "min": 1, "max": 500},
            {"name": "threshold", "label": "Min", "type": "float", "default": 1.05, "min": 0.5, "max": 3.0, "step": 0.01},
        ])
        self._add("dist_high", "Distance from High (%)", [
            {"name": "max_pct", "label": "Max %", "type": "float", "default": 5.0, "min": 0.0, "max": 100.0, "step": 0.5},
        ])

        self._add("rs_market", "RS vs S&P 500 (SPY)", [
            {"name": "lookback", "label": "Days", "type": "int", "default": 20, "min": 5, "max": 252},
            {"name": "min_ratio", "label": "Min", "type": "float", "default": 1.0, "min": 0.0, "max": 10.0, "step": 0.05},
        ])
        self.rows["rs_market"].set_enabled(False)

        self._add("rs_nasdaq", "RS vs NASDAQ (ONEQ)", [
            {"name": "lookback", "label": "Days", "type": "int", "default": 20, "min": 5, "max": 252},
            {"name": "min_ratio", "label": "Min", "type": "float", "default": 1.0, "min": 0.0, "max": 10.0, "step": 0.05},
        ])
        self.rows["rs_nasdaq"].set_enabled(False)

        self._add("rs_sector", "RS vs Sector ETF", [
            {"name": "lookback", "label": "Days", "type": "int", "default": 20, "min": 5, "max": 252},
            {"name": "min_ratio", "label": "Min", "type": "float", "default": 1.0, "min": 0.0, "max": 10.0, "step": 0.05},
        ])
        self.rows["rs_sector"].set_enabled(False)

        # --- Momentum / Prior Move ---
        self._section("Momentum / Prior Move")

        self._add("pct_gain", "% Gain over Period", [
            {"name": "min_gain", "label": "Min %", "type": "float", "default": 20.0, "min": -999.0, "max": 9999.0, "step": 1.0},
        ])
        # top_pct is a population-level filter — there's no per-ticker
        # "value" to display alongside the existing pct_gain column, so
        # display-only would be a no-op. Suppress the checkbox.
        self._add("top_pct", "Top X Percentile (of Gain)", [
            {"name": "cutoff", "label": "Top %", "type": "float", "default": 10.0, "min": 0.1, "max": 100.0, "step": 1.0},
        ], display_only_supported=False)
        self.rows["top_pct"].set_enabled(False)

        self._add("consec_gaps", "Consecutive Gap-Ups", [
            {"name": "min_gaps", "label": "Min", "type": "int", "default": 2, "min": 1, "max": 50},
        ])
        self.rows["consec_gaps"].set_enabled(False)

        self._add("consec_gaps_down", "Consecutive Gap-Downs", [
            {"name": "min_gaps", "label": "Min", "type": "int", "default": 2, "min": 1, "max": 50},
        ])
        self.rows["consec_gaps_down"].set_enabled(False)

        self._add("current_gap", "Current Gap %", [
            {"name": "min_pct", "label": "Min %", "type": "float", "default": 2.0, "min": 0.0, "max": 100.0, "step": 0.5},
        ])
        self.rows["current_gap"].set_enabled(False)

        self._add("max_gap", "Positive Gap %", [
            {"name": "min_pct", "label": "Lowest Acceptable %", "type": "float", "default": 5.0, "min": 0.0, "max": 9999.0, "step": 0.5},
        ])
        self.rows["max_gap"].set_enabled(False)

        self._add("max_neg_gap", "Negative Gap %", [
            {"name": "min_pct", "label": "Highest Acceptable %", "type": "float", "default": -5.0, "min": -9999.0, "max": 0.0, "step": 0.5},
        ])
        self.rows["max_neg_gap"].set_enabled(False)

        self._add("surge", "Surge Detection", [
            {"name": "min_pct", "label": "Min %", "type": "float", "default": 40.0, "min": 1.0, "max": 9999.0, "step": 5.0, "decimals": 1},
            # Mode selector — Trend-Continuous is the default for
            # studying episodic pivots (large sustained moves).
            # Ignition builds on top of trend (same drawdown gating)
            # but re-anchors the START to the first high-volume up
            # day inside the rally — the catalyst bar.
            # The legacy fixed-window modes stay available for users
            # comparing against the prior behavior.
            {"name": "mode", "label": "Mode", "type": "combo",
             "choices": [
                 ("trend",    "Trend-Continuous"),
                 ("ignition", "Ignition (Trend + Catalyst Bar)"),
                 ("close",    "Close-to-Close (N-day)"),
                 ("high_low", "Low→High (N-day)"),
             ],
             "default": "trend", "width": 220},
            {"name": "max_dd", "label": "Max DD %", "type": "float",
             "default": 25.0, "min": 1.0, "max": 95.0, "step": 1.0, "decimals": 1},
            {"name": "days", "label": "Days", "type": "int", "default": 7, "min": 1, "max": 252},
            # Ignition-only knobs — greyed out in every other mode.
            # vol_mult: ignition bar's volume must be >= N x median of
            #           the prior 20 days' volumes.
            # ig_min_pct: ignition bar's day-over-day close gain >= N%.
            {"name": "vol_mult", "label": "Vol x", "type": "float",
             "default": 2.0, "min": 1.0, "max": 50.0, "step": 0.25, "decimals": 2},
            {"name": "ig_min_pct", "label": "Day %", "type": "float",
             "default": 5.0, "min": 0.5, "max": 100.0, "step": 0.5, "decimals": 1},
        ])
        self.rows["surge"].set_enabled(False)
        # Wire the mode combobox to grey out the field that doesn't
        # apply: `days` is unused in Trend-Continuous mode (the rally
        # is bounded by the drawdown threshold, not a fixed window),
        # and `Max DD %` is unused in the legacy fixed-window modes.
        self._wire_surge_mode_dependent_fields()

        self._add("adr", "ADR% (Avg Daily Range)", [
            {"name": "lookback", "label": "Days", "type": "int", "default": 14, "min": 1, "max": 200},
            {"name": "min_pct", "label": "Min %", "type": "float", "default": 3.0, "min": 0.0, "max": 100.0, "step": 0.5},
        ])
        self._add("atr", "ATR (Avg True Range $)", [
            {"name": "period", "label": "Period", "type": "int", "default": 14, "min": 1, "max": 200},
            {"name": "min_val", "label": "Min $", "type": "float", "default": 0.50, "min": 0.0, "max": 9999.0, "step": 0.25},
            {"name": "max_val", "label": "Max $", "type": "float", "default": 999.0, "min": 0.0, "max": 9999.0, "step": 1.0},
        ])
        self.rows["atr"].set_enabled(False)

        # --- Volatility Contraction ---
        self._section("Volatility Contraction")

        self._add("bbw", "Bollinger Band Width", [
            {"name": "period", "label": "Period", "type": "int", "default": 20, "min": 1, "max": 200},
            {"name": "num_std", "label": "Std", "type": "float", "default": 2.0, "min": 0.1, "max": 5.0, "step": 0.1},
            {"name": "max_bbw", "label": "Max", "type": "float", "default": 0.10, "min": 0.0, "max": 5.0, "step": 0.01},
        ])
        self.rows["bbw"].set_enabled(False)

        self._add("atr_ratio", "ATR Ratio (Short/Long)", [
            {"name": "short", "label": "Short", "type": "int", "default": 5, "min": 1, "max": 100},
            {"name": "long", "label": "Long", "type": "int", "default": 50, "min": 1, "max": 500},
            {"name": "max_ratio", "label": "Max", "type": "float", "default": 0.75, "min": 0.0, "max": 5.0, "step": 0.05},
        ])
        self.rows["atr_ratio"].set_enabled(False)

        # --- Volume / Liquidity ---
        self._section("Volume / Liquidity")

        self._add("vol_dryup", "Volume Dry-Up Ratio", [
            {"name": "recent", "label": "Recent", "type": "int", "default": 10, "min": 1, "max": 200},
            {"name": "prior", "label": "Prior", "type": "int", "default": 20, "min": 1, "max": 200},
            {"name": "max_ratio", "label": "Max", "type": "float", "default": 0.70, "min": 0.0, "max": 5.0, "step": 0.05},
        ])
        self.rows["vol_dryup"].set_enabled(False)

        # min_price's "value" is just `price`, which is always shown.
        # A display-only mode would be a no-op — suppress the checkbox.
        self._add("min_price", "Minimum Price", [
            {"name": "floor", "label": "$", "type": "float", "default": 10.0, "min": 0.0, "max": 99999.0, "step": 1.0},
        ], display_only_supported=False)
        self._add("avg_vol", "Average Volume (shares)", [
            {"name": "lookback", "label": "Days", "type": "int", "default": 20, "min": 1, "max": 200},
            {"name": "min_vol", "label": "Min", "type": "float", "default": 200000.0, "min": 0.0, "max": 999999999.0, "step": 10000.0, "decimals": 0},
        ])
        self._add("dollar_vol", "Average Dollar Volume", [
            {"name": "lookback", "label": "Days", "type": "int", "default": 20, "min": 1, "max": 200},
            {"name": "min_dv", "label": "Min $", "type": "float", "default": 5000000.0, "min": 0.0, "max": 999999999999.0, "step": 1000000.0, "decimals": 0},
        ])

        # --- Earnings Filters ---
        self._section("Earnings Filters")

        # NOTE: per-row "Include No Data" checkboxes were removed —
        # NaN handling is now driven by TWO global toolbar toggles in
        # MainWindow: "Earnings Dates" (gates days_since/until filters)
        # and "Earnings Data" (gates the 6 Zacks per-quarter filters
        # and consec beats). Defaults (both off) are "NaN passes the
        # filter" so non-earnings filters can still operate on tickers
        # Zacks doesn't cover. See ScanParams.earnings_dates_only and
        # ScanParams.earnings_data_only.
        self._add("days_since_er", "Days Since Last Earnings", [
            {"name": "min_days", "label": "Min", "type": "int", "default": 0, "min": 0, "max": 365},
            {"name": "max_days", "label": "Max", "type": "int", "default": 90, "min": 0, "max": 365},
        ])
        self.rows["days_since_er"].set_enabled(False)

        self._add("days_until_er", "Days Until Next Earnings", [
            {"name": "min_days", "label": "Min", "type": "int", "default": 5, "min": 0, "max": 365},
        ])
        self.rows["days_until_er"].set_enabled(False)

        self._add("days_until_er_max", "Days Until ER (Max)", [
            {"name": "max_days", "label": "Max", "type": "int", "default": 0, "min": 0, "max": 365},
        ])
        self.rows["days_until_er_max"].set_enabled(False)

        # --- Per-quarter Earnings (Zacks) — Phase 7 §7.1 ---
        self._section("Per-Quarter Earnings (Zacks)")

        self._add("reported_eps", "Current Reported EPS (min)", [
            {"name": "min_val", "label": "Min", "type": "float", "default": 0.0, "min": -9999.0, "max": 9999.0, "step": 0.05},
        ])
        self.rows["reported_eps"].set_enabled(False)

        self._add("surprise_eps_dollar", "Current Surprise EPS $ (min)", [
            {"name": "min_val", "label": "Min $", "type": "float", "default": 0.0, "min": -9999.0, "max": 9999.0, "step": 0.05},
        ])
        self.rows["surprise_eps_dollar"].set_enabled(False)

        self._add("surprise_eps_pct", "Current Surprise EPS % (min)", [
            {"name": "min_val", "label": "Min %", "type": "float", "default": 0.0, "min": -9999.0, "max": 9999.0, "step": 1.0},
        ])
        self.rows["surprise_eps_pct"].set_enabled(False)

        self._add("reported_rev", "Current Reported Revenue (min, $M)", [
            {"name": "min_val", "label": "Min $M", "type": "float", "default": 0.0, "min": 0.0, "max": 9999999.0, "step": 10.0},
        ])
        self.rows["reported_rev"].set_enabled(False)

        self._add("surprise_rev_dollar", "Current Surprise Rev $ (min, $M)", [
            {"name": "min_val", "label": "Min $M", "type": "float", "default": 0.0, "min": -999999.0, "max": 999999.0, "step": 10.0},
        ])
        self.rows["surprise_rev_dollar"].set_enabled(False)

        self._add("surprise_rev_pct", "Current Surprise Rev % (min)", [
            {"name": "min_val", "label": "Min %", "type": "float", "default": 0.0, "min": -9999.0, "max": 9999.0, "step": 1.0},
        ])
        self.rows["surprise_rev_pct"].set_enabled(False)

        # YoY % filters — derived at fill-finalize time from the same-
        # quarter-prior-year row. NaN when prior year missing; behaves
        # like the surprise % filters under earnings_data_only.
        self._add("yoy_eps_pct", "Current YoY EPS % (min)", [
            {"name": "min_val", "label": "Min %", "type": "float", "default": 0.0, "min": -9999.0, "max": 9999.0, "step": 1.0},
        ])
        self.rows["yoy_eps_pct"].set_enabled(False)

        self._add("yoy_rev_pct", "Current YoY Rev % (min)", [
            {"name": "min_val", "label": "Min %", "type": "float", "default": 0.0, "min": -9999.0, "max": 9999.0, "step": 1.0},
        ])
        self.rows["yoy_rev_pct"].set_enabled(False)

        # --- Consecutive Beats — Phase 7 §7.3 ---
        # When checked, the individual EPS/Rev filters above are greyed
        # out (handled by _on_beats_toggled below) and Sequenced Run is
        # locked out (main_window listens to beats_filter_toggled).

        # Min spinbox allows 0 — when set to 0, the streak filter passes
        # everyone (streak >= 0 is always true) AND the display-only
        # red-on-fail never fires (streak < 0 is impossible). Lets the
        # user surface the streak count + Q-i blocks for context without
        # any threshold gating or red coloring.
        # Quarter Cap spinbox: 0 = no cap (use full MAX_BEATS_QUARTERS=20),
        # any value 1-20 limits the rendered Q-i columns to that count.
        self._add("consec_eps_beats", "Consecutive EPS Beats", [
            {"name": "min_count", "label": "Min", "type": "int", "default": 3, "min": 0, "max": 50},
            {"name": "threshold_pct", "label": "Threshold %", "type": "float", "default": 0.0, "min": -9999.0, "max": 9999.0, "step": 0.5},
            {"name": "quarter_cap", "label": "Q Cap", "type": "int", "default": 0, "min": 0, "max": 20},
        ])
        self.rows["consec_eps_beats"].set_enabled(False)

        self._add("consec_rev_beats", "Consecutive Rev Beats", [
            {"name": "min_count", "label": "Min", "type": "int", "default": 3, "min": 0, "max": 50},
            {"name": "threshold_pct", "label": "Threshold %", "type": "float", "default": 0.0, "min": -9999.0, "max": 9999.0, "step": 0.5},
            {"name": "quarter_cap", "label": "Q Cap", "type": "int", "default": 0, "min": 0, "max": 20},
        ])
        self.rows["consec_rev_beats"].set_enabled(False)

        # Wire grey-out behavior — beats checkbox disables the matching
        # individual filter rows, and emits beats_filter_toggled so the
        # main window can lock out Sequenced Run.
        self.rows["consec_eps_beats"].toggle.toggled.connect(
            self._on_eps_beats_toggled
        )
        self.rows["consec_rev_beats"].toggle.toggled.connect(
            self._on_rev_beats_toggled
        )

        self.vbox.addStretch()
        self.setWidget(container)

    # ── Phase 7 §7.2 grey-out wiring ──────────────────────────────────

    @pyqtSlot(bool)
    def _on_eps_beats_toggled(self, checked: bool):
        """Lock the FILTER aspects of the three individual EPS rows
        while beats is active, but leave Display-Only clickable so the
        user can still surface those values alongside the active beats
        filter (Phase 8 §8.5 relaxation of §7.2). Reverse on uncheck."""
        for key in ("reported_eps", "surprise_eps_dollar", "surprise_eps_pct",
                    "yoy_eps_pct"):
            row = self.rows.get(key)
            if row is not None:
                row.set_filter_locked(checked)
        self._emit_beats_state()

    @pyqtSlot(bool)
    def _on_rev_beats_toggled(self, checked: bool):
        """Mirror of _on_eps_beats_toggled for the three revenue filters."""
        for key in ("reported_rev", "surprise_rev_dollar", "surprise_rev_pct",
                    "yoy_rev_pct"):
            row = self.rows.get(key)
            if row is not None:
                row.set_filter_locked(checked)
        self._emit_beats_state()

    def _emit_beats_state(self):
        """Tell the main window whether either beats filter is active."""
        any_active = (
            self.rows["consec_eps_beats"].is_enabled()
            or self.rows["consec_rev_beats"].is_enabled()
        )
        self.beats_filter_toggled.emit(any_active)

    def is_beats_filter_active(self) -> bool:
        """Public accessor — main window uses this on startup / preset
        load to decide whether Sequenced Run should be locked."""
        return (
            self.rows["consec_eps_beats"].is_enabled()
            or self.rows["consec_rev_beats"].is_enabled()
        )

    # ── Surge-mode field greyout ─────────────────────────────────────

    def _wire_surge_mode_dependent_fields(self):
        """Connect the surge-mode combobox so only fields relevant to
        the selected algorithm are interactive.

        Per-mode field activation:
            trend     → max_dd; days / vol_mult / ig_min_pct greyed
            ignition  → max_dd, vol_mult, ig_min_pct; days greyed
            close     → days;   max_dd / vol_mult / ig_min_pct greyed
            high_low  → days;   max_dd / vol_mult / ig_min_pct greyed

        Visual: in addition to `setEnabled(False)`, apply
        `IndicatorRow._GREYED_INPUT_STYLE` so the disabled state is
        unambiguously visible on the dark theme — Qt's default
        `:disabled` look only mutes text by ~10% which is easy to
        miss on `#3c3c3c` background. The explicit style drops text
        to `#666` and background to `#2a2a2a`."""
        row = self.rows["surge"]
        combo = row.spinboxes["mode"]
        days_widget = row.spinboxes.get("days")
        max_dd_widget = row.spinboxes.get("max_dd")
        vol_mult_widget = row.spinboxes.get("vol_mult")
        ig_min_pct_widget = row.spinboxes.get("ig_min_pct")
        muted = IndicatorRow._GREYED_INPUT_STYLE

        def _set_state(widget, enabled: bool):
            if widget is None:
                return
            widget.setEnabled(enabled)
            widget.setStyleSheet("" if enabled else muted)

        def _apply():
            mode = combo.currentData()
            uses_dd = mode in ("trend", "ignition")
            uses_ignition = (mode == "ignition")
            uses_days = mode in ("close", "high_low")
            _set_state(max_dd_widget, uses_dd)
            _set_state(vol_mult_widget, uses_ignition)
            _set_state(ig_min_pct_widget, uses_ignition)
            _set_state(days_widget, uses_days)

        combo.currentIndexChanged.connect(lambda _i: _apply())
        _apply()  # Apply once with the default mode

    def _section(self, title: str):
        lbl = QLabel(f"  {title}")
        lbl.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        lbl.setStyleSheet("color: #4a90d9; margin-top: 8px;")
        self.vbox.addWidget(lbl)

    def _add(self, key: str, label: str, params: list[dict],
             *, display_only_supported: bool = True):
        row = IndicatorRow(label, params,
                           display_only_supported=display_only_supported)
        self.rows[key] = row
        self.vbox.addWidget(row)

    def build_scan_params(self, start: date, end: date,
                          earnings_dates_only: bool = False,
                          earnings_data_only: bool = False,
                          match_color_tolerance_days: int = 1):
        """Read all controls and build a ScanParams.

        Both `earnings_dates_only` and `earnings_data_only` come from
        MainWindow toolbar checkboxes (replace the per-row
        include_no_data flags). When True, the corresponding subset of
        earnings filters require actual data; when False (default),
        NaN passes those filters cleanly so non-earnings filters can
        still operate on tickers Zacks doesn't cover.

        `match_color_tolerance_days` (0–7) is the fuzzy tolerance for
        matching an indicator date against an earnings report date in
        the ResultsTable color-pairing. Default 1 — covers timing
        offsets like after-hours report vs next-day price reaction.
        Persisted via QSettings; passed through from MainWindow."""
        r = self.rows
        return self._ScanParams(
            start_date=start,
            end_date=end,
            earnings_dates_only=earnings_dates_only,
            earnings_data_only=earnings_data_only,
            match_color_tolerance_days=int(match_color_tolerance_days),
            # SMA 1
            sma1_enabled=r["sma1"].is_enabled(),
            sma1_display_only=r["sma1"].is_display_only(),
            sma1_period=r["sma1"].value("period"),
            # SMA 2
            sma2_enabled=r["sma2"].is_enabled(),
            sma2_display_only=r["sma2"].is_display_only(),
            sma2_period=r["sma2"].value("period"),
            # STI
            sti_enabled=r["sti"].is_enabled(),
            sti_display_only=r["sti"].is_display_only(),
            sti_short_lb=r["sti"].value("short_lb"),
            sti_long_lb=r["sti"].value("long_lb"),
            sti_threshold=r["sti"].value("threshold"),
            # Distance from high
            dist_high_enabled=r["dist_high"].is_enabled(),
            dist_high_display_only=r["dist_high"].is_display_only(),
            dist_high_max_pct=r["dist_high"].value("max_pct"),
            # % Gain
            pct_gain_enabled=r["pct_gain"].is_enabled(),
            pct_gain_display_only=r["pct_gain"].is_display_only(),
            pct_gain_min=r["pct_gain"].value("min_gain"),
            # Top percentile (no display-only — population-level filter)
            top_pct_enabled=r["top_pct"].is_enabled(),
            top_pct_cutoff=r["top_pct"].value("cutoff"),
            # Consecutive gaps
            consec_gaps_enabled=r["consec_gaps"].is_enabled(),
            consec_gaps_display_only=r["consec_gaps"].is_display_only(),
            consec_gaps_min=int(r["consec_gaps"].value("min_gaps")),
            # Consecutive gap-downs
            consec_gaps_down_enabled=r["consec_gaps_down"].is_enabled(),
            consec_gaps_down_display_only=r["consec_gaps_down"].is_display_only(),
            consec_gaps_down_min=int(r["consec_gaps_down"].value("min_gaps")),
            # Current gap
            current_gap_enabled=r["current_gap"].is_enabled(),
            current_gap_display_only=r["current_gap"].is_display_only(),
            current_gap_min_pct=r["current_gap"].value("min_pct"),
            # Max Positive Gap
            max_gap_enabled=r["max_gap"].is_enabled(),
            max_gap_display_only=r["max_gap"].is_display_only(),
            max_gap_min_pct=r["max_gap"].value("min_pct"),
            # Max Negative Gap
            max_neg_gap_enabled=r["max_neg_gap"].is_enabled(),
            max_neg_gap_display_only=r["max_neg_gap"].is_display_only(),
            max_neg_gap_min_pct=r["max_neg_gap"].value("min_pct"),
            # Surge Detection — see ScanParams.surge_mode for semantics.
            # `surge_use_high_low` is preserved for back-compat (kept in
            # sync with mode="high_low" so legacy callers reading the
            # bool still see consistent state).
            surge_enabled=r["surge"].is_enabled(),
            surge_display_only=r["surge"].is_display_only(),
            surge_min_pct=r["surge"].value("min_pct"),
            surge_days=int(r["surge"].value("days")),
            surge_mode=r["surge"].value("mode") or "trend",
            surge_use_high_low=(r["surge"].value("mode") == "high_low"),
            surge_max_drawdown_pct=r["surge"].value("max_dd"),
            surge_ignition_vol_mult=float(
                r["surge"].value("vol_mult") or 2.0
            ),
            surge_ignition_min_pct=float(
                r["surge"].value("ig_min_pct") or 5.0
            ),
            # ADR (momentum — minimum)
            adr_enabled=r["adr"].is_enabled(),
            adr_display_only=r["adr"].is_display_only(),
            adr_lookback=r["adr"].value("lookback"),
            adr_min_pct=r["adr"].value("min_pct"),
            # ATR (absolute)
            atr_enabled=r["atr"].is_enabled(),
            atr_display_only=r["atr"].is_display_only(),
            atr_period=r["atr"].value("period"),
            atr_min=r["atr"].value("min_val"),
            atr_max=r["atr"].value("max_val"),
            # BBW
            bbw_enabled=r["bbw"].is_enabled(),
            bbw_display_only=r["bbw"].is_display_only(),
            bbw_period=r["bbw"].value("period"),
            bbw_num_std=r["bbw"].value("num_std"),
            bbw_max=r["bbw"].value("max_bbw"),
            # ATR ratio
            atr_ratio_enabled=r["atr_ratio"].is_enabled(),
            atr_ratio_display_only=r["atr_ratio"].is_display_only(),
            atr_short=r["atr_ratio"].value("short"),
            atr_long=r["atr_ratio"].value("long"),
            atr_max_ratio=r["atr_ratio"].value("max_ratio"),
            # Volume dry-up
            vol_dryup_enabled=r["vol_dryup"].is_enabled(),
            vol_dryup_display_only=r["vol_dryup"].is_display_only(),
            vol_dryup_recent=r["vol_dryup"].value("recent"),
            vol_dryup_prior=r["vol_dryup"].value("prior"),
            vol_dryup_max_ratio=r["vol_dryup"].value("max_ratio"),
            # Min price (no display-only — `price` always shown)
            min_price_enabled=r["min_price"].is_enabled(),
            min_price_floor=r["min_price"].value("floor"),
            # Avg volume
            avg_vol_enabled=r["avg_vol"].is_enabled(),
            avg_vol_display_only=r["avg_vol"].is_display_only(),
            avg_vol_lookback=r["avg_vol"].value("lookback"),
            avg_vol_min=r["avg_vol"].value("min_vol"),
            # Dollar volume
            dollar_vol_enabled=r["dollar_vol"].is_enabled(),
            dollar_vol_display_only=r["dollar_vol"].is_display_only(),
            dollar_vol_lookback=r["dollar_vol"].value("lookback"),
            dollar_vol_min=r["dollar_vol"].value("min_dv"),
            # RS vs S&P 500
            rs_market_enabled=r["rs_market"].is_enabled(),
            rs_market_display_only=r["rs_market"].is_display_only(),
            rs_market_lookback=r["rs_market"].value("lookback"),
            rs_market_min=r["rs_market"].value("min_ratio"),
            # RS vs NASDAQ
            rs_nasdaq_enabled=r["rs_nasdaq"].is_enabled(),
            rs_nasdaq_display_only=r["rs_nasdaq"].is_display_only(),
            rs_nasdaq_lookback=r["rs_nasdaq"].value("lookback"),
            rs_nasdaq_min=r["rs_nasdaq"].value("min_ratio"),
            # RS vs Sector
            rs_sector_enabled=r["rs_sector"].is_enabled(),
            rs_sector_display_only=r["rs_sector"].is_display_only(),
            rs_sector_lookback=r["rs_sector"].value("lookback"),
            rs_sector_min=r["rs_sector"].value("min_ratio"),
            # Days since earnings
            days_since_earnings_enabled=r["days_since_er"].is_enabled(),
            days_since_earnings_display_only=r["days_since_er"].is_display_only(),
            days_since_min=r["days_since_er"].value("min_days"),
            days_since_max=r["days_since_er"].value("max_days"),
            # Days until earnings (min)
            days_until_earnings_enabled=r["days_until_er"].is_enabled(),
            days_until_earnings_display_only=r["days_until_er"].is_display_only(),
            days_until_min=r["days_until_er"].value("min_days"),
            # Days until earnings (max)
            days_until_max_enabled=r["days_until_er_max"].is_enabled(),
            days_until_max_display_only=r["days_until_er_max"].is_display_only(),
            days_until_max=r["days_until_er_max"].value("max_days"),
            # Per-quarter Zacks earnings (Phase 7 §7.1).
            # NOTE: per-row include_no_data was removed — NaN behavior
            # is now driven by the global earnings_data_only flag set
            # at the MainWindow toolbar level.
            reported_eps_enabled=r["reported_eps"].is_enabled(),
            reported_eps_display_only=r["reported_eps"].is_display_only(),
            reported_eps_min=r["reported_eps"].value("min_val"),
            surprise_eps_dollar_enabled=r["surprise_eps_dollar"].is_enabled(),
            surprise_eps_dollar_display_only=r["surprise_eps_dollar"].is_display_only(),
            surprise_eps_dollar_min=r["surprise_eps_dollar"].value("min_val"),
            surprise_eps_pct_enabled=r["surprise_eps_pct"].is_enabled(),
            surprise_eps_pct_display_only=r["surprise_eps_pct"].is_display_only(),
            surprise_eps_pct_min=r["surprise_eps_pct"].value("min_val"),
            reported_rev_enabled=r["reported_rev"].is_enabled(),
            reported_rev_display_only=r["reported_rev"].is_display_only(),
            reported_rev_min=r["reported_rev"].value("min_val"),
            surprise_rev_dollar_enabled=r["surprise_rev_dollar"].is_enabled(),
            surprise_rev_dollar_display_only=r["surprise_rev_dollar"].is_display_only(),
            surprise_rev_dollar_min=r["surprise_rev_dollar"].value("min_val"),
            surprise_rev_pct_enabled=r["surprise_rev_pct"].is_enabled(),
            surprise_rev_pct_display_only=r["surprise_rev_pct"].is_display_only(),
            surprise_rev_pct_min=r["surprise_rev_pct"].value("min_val"),
            yoy_eps_pct_enabled=r["yoy_eps_pct"].is_enabled(),
            yoy_eps_pct_display_only=r["yoy_eps_pct"].is_display_only(),
            yoy_eps_pct_min=r["yoy_eps_pct"].value("min_val"),
            yoy_rev_pct_enabled=r["yoy_rev_pct"].is_enabled(),
            yoy_rev_pct_display_only=r["yoy_rev_pct"].is_display_only(),
            yoy_rev_pct_min=r["yoy_rev_pct"].value("min_val"),
            # Consecutive Beats (Phase 7 §7.3)
            consec_eps_beats_enabled=r["consec_eps_beats"].is_enabled(),
            consec_eps_beats_display_only=r["consec_eps_beats"].is_display_only(),
            consec_eps_beats_min=int(r["consec_eps_beats"].value("min_count")),
            consec_eps_beats_threshold_pct=r["consec_eps_beats"].value("threshold_pct"),
            consec_eps_beats_quarter_cap=int(r["consec_eps_beats"].value("quarter_cap")),
            consec_rev_beats_enabled=r["consec_rev_beats"].is_enabled(),
            consec_rev_beats_display_only=r["consec_rev_beats"].is_display_only(),
            consec_rev_beats_min=int(r["consec_rev_beats"].value("min_count")),
            consec_rev_beats_threshold_pct=r["consec_rev_beats"].value("threshold_pct"),
            consec_rev_beats_quarter_cap=int(r["consec_rev_beats"].value("quarter_cap")),
        )

    def to_dict(self) -> dict:
        """Serialize all control states to a dict (for preset save)."""
        out = {}
        for key, row in self.rows.items():
            entry = {"enabled": row.is_enabled()}
            # Phase 8 §8.5: persist display-only state when the row
            # supports it. Stored as `display_only` so legacy presets
            # without the key just default to False on load.
            if row.display_only is not None:
                entry["display_only"] = row.is_display_only()
            for pname, sb in row.spinboxes.items():
                if isinstance(sb, QCheckBox):
                    entry[pname] = sb.isChecked()
                elif isinstance(sb, QComboBox):
                    # Persist the stored value (combo userData), not the
                    # display label, so future label changes don't break
                    # saved presets.
                    entry[pname] = sb.currentData()
                else:
                    entry[pname] = sb.value()
            out[key] = entry
        return out

    def from_dict(self, d: dict):
        """Restore control states from a dict (for preset load).

        Legacy-preset migration (surge row): pre-trend-mode presets
        store `use_hl: bool`. If we see one without a `mode` key, map
        it to the equivalent new mode value. Newer presets save `mode`
        directly and the boolean is ignored.
        """
        # Surge legacy migration — apply BEFORE the generic loop so the
        # mode value lands in the combo properly.
        surge_entry = d.get("surge")
        if surge_entry is not None and "mode" not in surge_entry:
            if surge_entry.get("use_hl"):
                surge_entry = {**surge_entry, "mode": "high_low"}
            else:
                # Pre-fork preset: default to "close" (the prior behavior)
                # so we don't silently switch a saved preset to the new
                # trend-continuous default.
                surge_entry = {**surge_entry, "mode": "close"}
            d = {**d, "surge": surge_entry}

        for key, entry in d.items():
            if key not in self.rows:
                continue
            row = self.rows[key]
            row.set_enabled(entry.get("enabled", True))
            for pname, val in entry.items():
                if pname == "enabled":
                    continue
                row.set_value(pname, val)
        # Phase 7 §7.2: reapply grey-out after preset load. set_enabled
        # only fires the `toggled` signal on a state change, so a preset
        # that re-asserts the same beats state needs a manual sync to
        # land the per-row disabled flags.
        self._on_eps_beats_toggled(self.rows["consec_eps_beats"].is_enabled())
        self._on_rev_beats_toggled(self.rows["consec_rev_beats"].is_enabled())
        # Resync surge field-greyout after loading. Mirror
        # `_wire_surge_mode_dependent_fields` — apply the same explicit
        # muted style so a preset that lands on trend mode greys days
        # visibly (and vice versa).
        if "surge" in self.rows and "mode" in self.rows["surge"].spinboxes:
            row = self.rows["surge"]
            mode = row.value("mode")
            days_widget = row.spinboxes.get("days")
            max_dd_widget = row.spinboxes.get("max_dd")
            vol_mult_widget = row.spinboxes.get("vol_mult")
            ig_min_pct_widget = row.spinboxes.get("ig_min_pct")
            muted = IndicatorRow._GREYED_INPUT_STYLE
            uses_dd = mode in ("trend", "ignition")
            uses_ignition = (mode == "ignition")
            uses_days = mode in ("close", "high_low")
            for w, on in (
                (days_widget, uses_days),
                (max_dd_widget, uses_dd),
                (vol_mult_widget, uses_ignition),
                (ig_min_pct_widget, uses_ignition),
            ):
                if w is None:
                    continue
                w.setEnabled(on)
                w.setStyleSheet("" if on else muted)


# ============================================================================
# Results table
# ============================================================================

def _fmt_date(x) -> str:
    """Format a date value for the results table — short MM/DD/YY.
    Returns '' for None / NaT / unparseable input so empty cells stay clean."""
    if x is None:
        return ""
    try:
        ts = pd.Timestamp(x)
        if pd.isna(ts):
            return ""
        return ts.strftime("%m/%d/%y")
    except Exception:
        return str(x)


# Column definitions: (header, dict_key, format_func)
# Date sub-columns (Gain Start / Up Gap Start / Down Gap Start / Max Gap Date /
# Min Gap Date / Surge Start) sit immediately to the right of the indicator
# they index, so the value and the date that produced it travel together.
RESULT_COLUMNS = [
    ("Ticker",            "symbol",              str),
    ("Close",             "close",               lambda x: f"${x:.2f}"),
    ("% Gain",            "pct_gain",            lambda x: f"{x:.1f}%"),
    ("Gain Start",        "gain_start_date",     _fmt_date),
    ("STI",               "sti",                 lambda x: f"{x:.3f}"),
    ("Dist High %",       "dist_high_pct",       lambda x: f"{x:.1f}%"),
    ("ADR%",              "adr_pct",             lambda x: f"{x:.2f}%"),
    ("ATR",               "atr",                 lambda x: f"${x:.2f}"),
    ("BBW",               "bbw",                 lambda x: f"{x:.4f}"),
    ("ATR Ratio",         "atr_ratio",           lambda x: f"{x:.3f}"),
    ("ConsecGaps",        "consec_gaps",         lambda x: str(int(x))),
    ("Up Gap Start",      "up_gap_start_date",   _fmt_date),
    ("ConsecGapDn",       "consec_gaps_down",    lambda x: str(int(x))),
    ("Down Gap Start",    "down_gap_start_date", _fmt_date),
    ("Gap%",              "current_gap_pct",     lambda x: f"{x:.2f}%"),
    ("Max Gap%",          "max_gap_pct",         lambda x: f"{x:.2f}%"),
    ("Max Gap Date",      "max_gap_date",        _fmt_date),
    ("Max NegGap%",       "max_neg_gap_pct",     lambda x: f"{x:.2f}%"),
    ("Min Gap Date",      "min_gap_date",        _fmt_date),
    ("Surge%",            "surge_pct",           lambda x: f"{x:.1f}%"),
    ("Surge Start",       "surge_start_date",    _fmt_date),
    ("Surge Window",      "surge_window",        lambda x: str(x) if x else ""),
    ("VolDryUp",          "vol_dryup",           lambda x: f"{x:.3f}"),
    ("Avg Vol",           "avg_vol",             lambda x: f"{x:,.0f}"),
    ("$ Vol",             "dollar_vol",          lambda x: f"${x:,.0f}"),
    ("RS S&P",            "rs_market",           lambda x: f"{x:.2f}"),
    ("RS Nas",            "rs_nasdaq",           lambda x: f"{x:.2f}"),
    ("RS Sec",            "rs_sector",           lambda x: f"{x:.2f}"),
    ("Days Since ER",     "days_since_er",       lambda x: str(int(x))),
    ("Days Until ER",     "days_until_er",       lambda x: str(int(x))),
    # Phase 8 §8.2: per-quarter Zacks earnings cluster — rightmost group.
    # All seven values come from the same quarterly event for the period,
    # so a single shared `Last Report Date` covers the whole cluster.
    ("Curr Reported EPS", "reported_eps",        lambda x: f"{x:.2f}"),
    ("Curr Surp EPS $",   "surprise_eps_dollar", lambda x: f"{x:+.2f}"),
    ("Curr Surp EPS %",   "surprise_eps_pct",    lambda x: f"{x:+.2f}%"),
    ("Curr YoY EPS %",    "yoy_eps_pct",         lambda x: f"{x:+.2f}%"),
    ("Curr Reported Rev", "reported_rev",        lambda x: f"{x:,.1f}"),
    ("Curr Surp Rev $",   "surprise_rev_dollar", lambda x: f"{x:+,.1f}"),
    ("Curr Surp Rev %",   "surprise_rev_pct",    lambda x: f"{x:+.2f}%"),
    ("Curr YoY Rev %",    "yoy_rev_pct",         lambda x: f"{x:+.2f}%"),
    ("Last Report Date",  "last_report_date",    _fmt_date),
]
# Period column intentionally absent — the timeframe selector dropdown above
# the results table now identifies which period is being viewed. Multi-period
# Excel/CSV exports re-add a `Period` column at write time when needed.

# Date column keys (used by export's "Add News columns" feature). Phase 8
# §8.4: Last Report Date joins this list so an export with News on inserts
# a News_LastReportDate placeholder to its right.
DATE_COLUMN_KEYS = {
    "gain_start_date", "up_gap_start_date", "down_gap_start_date",
    "max_gap_date", "min_gap_date", "surge_start_date",
    "last_report_date",
}


def _format_optional(val, fmt) -> str:
    """Format a possibly-None / NaN cell value, returning '' for missing."""
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    try:
        return fmt(val)
    except Exception:
        return str(val)


# Audit L10: precompiled patterns for the dynamic-column extraction.
# `_Q_COL_RE` matches `q{int}_{rest}`, capturing both halves so the helper
# below can decide membership without splitting + rejoining strings.
_Q_COL_RE = re.compile(r"^q(\d+)_(.+)$")


# Columns that always appear regardless of which filters are enabled —
# the row identity / sort-anchor cluster. Everything else gets filtered
# out unless its key is present in the result DataFrame (which means
# `_compute_ticker` populated it, which means the matching filter /
# indicator was enabled).
_ALWAYS_VISIBLE_KEYS = {
    "symbol", "close", "pct_gain", "gain_start_date",
}


# ── Match-color anchoring (generalized 2026-05) ─────────────────────
#
# When an indicator date matches an earnings report date, the cell
# containing that date gets a palette color from `_ALIGN_PALETTE`.
# Generalized: every CELL has an "anchor date" — the date that drives
# the color for that cell — and every cell whose anchor matches gets
# colored. This means the entire "unit" (gap value + gap date, Q-i
# triplet of date + reported + surp $ + surp %) shares one color when
# the unit's date is part of an alignment match.
#
# Anchor relationships:
#   - Date columns anchor themselves (their own value is the anchor).
#   - Static indicator value columns map to their date column via
#     `_INDICATOR_VALUE_TO_DATE` (e.g., max_gap_pct → max_gap_date).
#   - Q-i value columns anchor to the same-quarter date column
#     (e.g., q3_reported_eps → q3_report_date_eps).
#   - Most-recent earnings columns anchor to last_report_date if
#     present, falling back to q1_report_date_eps / q1_report_date_rev
#     when last_report_date is suppressed (which happens when beats
#     is active — same date, different column).
#
# Columns NOT anchored (no match-coloring): symbol, close, pct_gain,
# the streak counts (consec_*_beats), and any column not listed below.
_INDICATOR_VALUE_TO_DATE: dict[str, str] = {
    "max_gap_pct": "max_gap_date",
    "max_neg_gap_pct": "min_gap_date",
    "consec_gaps": "up_gap_start_date",
    "consec_gaps_down": "down_gap_start_date",
    "surge_pct": "surge_start_date",
    "surge_window": "surge_start_date",
}

_SELF_ANCHOR_DATE_COLS: frozenset[str] = frozenset({
    "max_gap_date", "min_gap_date", "up_gap_start_date",
    "down_gap_start_date", "surge_start_date", "surge_end_date",
    "last_report_date", "gain_start_date",
})


# Column keys whose anchor IS an earnings report_date by construction.
# Used to gate match-color group rendering: a color group must include
# at least one of these (i.e. the canonical earnings date must be
# represented by a visible earnings cell). Without this gate, pairs of
# non-earnings indicator dates (e.g. gap_date + surge_date) that both
# happen to land near the SAME earnings event end up sharing a color
# even though no visible earnings cell anchors the match — misleading.
_EARNINGS_ANCHOR_TOP_LEVEL: frozenset[str] = frozenset({
    "last_report_date",
    "reported_eps", "surprise_eps_dollar", "surprise_eps_pct",
    "yoy_eps_pct",
    "reported_rev", "surprise_rev_dollar", "surprise_rev_pct",
    "yoy_rev_pct",
})


def _is_earnings_anchor_key(key: str) -> bool:
    """True iff the cell's anchor (per `_anchor_date_value`) is an
    earnings report_date — i.e., the key represents an earnings-source
    column. Q-i columns are always earnings-related (they exist only
    when earnings_history is loaded)."""
    if key in _EARNINGS_ANCHOR_TOP_LEVEL:
        return True
    return _Q_COL_RE.match(key) is not None


def _first_present(row_data, *keys):
    """Return the first key in `keys` whose value in `row_data` is
    not None and not NaN. Used by the anchor-date lookup so the
    most-recent earnings columns can fall back from `last_report_date`
    (suppressed when beats is on) to `q1_report_date_eps` / `_rev`."""
    for k in keys:
        v = row_data.get(k)
        if v is None:
            continue
        try:
            if pd.isna(v):
                continue
        except (TypeError, ValueError):
            # Non-scalar like a list — treat as present
            return v
        return v
    return None


def _anchor_date_value(key: str, row_data):
    """Return the date value (Timestamp / scalar / None) that anchors
    the match-color for `key`'s cell. None means this column doesn't
    participate in match-coloring (e.g., symbol, close)."""
    # Q-i columns first (most common shape after a beats scan).
    qm = _Q_COL_RE.match(key)
    if qm is not None:
        q_num = qm.group(1)
        suffix = qm.group(2)
        if suffix in ("report_date_eps", "report_date_rev"):
            return row_data.get(key)  # self
        if (suffix.endswith("_eps") or suffix.startswith("reported_eps")
                or suffix.startswith("surprise_eps")
                or suffix.startswith("yoy_eps")):
            return row_data.get(f"q{q_num}_report_date_eps")
        if (suffix.endswith("_rev") or suffix.startswith("reported_rev")
                or suffix.startswith("surprise_rev")
                or suffix.startswith("yoy_rev")):
            return row_data.get(f"q{q_num}_report_date_rev")
        return None
    # Date columns anchor themselves.
    if key in _SELF_ANCHOR_DATE_COLS:
        return row_data.get(key)
    # Static indicator value columns.
    if key in _INDICATOR_VALUE_TO_DATE:
        return row_data.get(_INDICATOR_VALUE_TO_DATE[key])
    # Most-recent earnings (the 3 EPS / 3 Rev "current quarter"
    # columns). Anchor: last_report_date if present, else the
    # corresponding q1 date — which holds the same value when beats
    # is active and last_report_date is suppressed.
    if key in ("reported_eps", "surprise_eps_dollar", "surprise_eps_pct",
               "yoy_eps_pct"):
        return _first_present(row_data, "last_report_date", "q1_report_date_eps")
    if key in ("reported_rev", "surprise_rev_dollar", "surprise_rev_pct",
               "yoy_rev_pct"):
        return _first_present(row_data, "last_report_date", "q1_report_date_rev")
    return None


def _build_dynamic_columns(
    df, *, interleave_quarters: bool = False,
) -> tuple[list[tuple], int, int]:
    """Phase 8 §8.3 + 2026-05 update: build the table's column set
    from RESULT_COLUMNS, filtered to only the columns whose key is
    actually populated in `df` (or in `_ALWAYS_VISIBLE_KEYS`). This
    means disabled filters / indicators no longer take up table real
    estate at all.

    Then extend with the dynamic Q-i blocks when beats are present.

    `interleave_quarters` (default False) controls how the EPS and Rev
    Q-i blocks are arranged WHEN BOTH ARE PRESENT:

      False (default — back-compat layout):
        ... | EPS Beats | Q-1 EPS block | Q-2 EPS block | ... | Q-N EPS
            | Rev Beats | Q-1 Rev block | Q-2 Rev block | ... | Q-N Rev

      True (interleaved by quarter):
        ... | EPS Beats | Rev Beats | Q-1 EPS block | Q-1 Rev block
            | Q-2 EPS block | Q-2 Rev block | ... | Q-N EPS+Rev

    The interleave flag is a no-op when only ONE side has data — the
    layout falls back to the single-block default. Empty df → no Q-i
    columns regardless of flag. Asymmetric n_eps != n_rev is handled
    by emitting whichever side still has data for each quarter index.

    Returns:
      (full_columns, n_eps_quarters, n_rev_quarters)
    """
    if df is None or df.empty:
        # Nothing to render — return a minimal column list anchored on
        # the always-visible keys. Lets the empty table show a row of
        # blank headers rather than a frozen prior layout.
        return [
            (h, k, f) for h, k, f in RESULT_COLUMNS
            if k in _ALWAYS_VISIBLE_KEYS
        ], 0, 0

    df_columns = set(df.columns)

    # Render every populated Q-i column when beats data is present —
    # NOT just the quarters inside the longest streak. Prior behavior
    # capped n_eps / n_rev at the max consec_*_beats value across all
    # tickers, which meant a streak break at Q-3 hid Q-3..Q-N entirely
    # from the table even though the underlying earnings data was in
    # the row dict. Earnings-related cells should always be visible
    # (and thus eligible for match-coloring against non-earnings
    # indicator dates) regardless of whether each quarter contributed
    # to a beat streak. The streak count itself still drives the
    # green-text coloring inside `_populate_row` — only display
    # gating is decoupled.
    def _max_present(suffix: str) -> int:
        """Return the highest k such that q{k}_<suffix> is a column in
        df, or 0 if no such columns exist. Audit L10: regex match
        instead of split+join — same correctness, cleaner and faster."""
        present = []
        for c in df.columns:
            m = _Q_COL_RE.match(c)
            if m and m.group(2) == suffix:
                present.append(int(m.group(1)))
        return max(present) if present else 0

    n_eps = _max_present("reported_eps")
    n_rev = _max_present("reported_rev")

    # Filter the static result-column list to only entries that actually
    # have data in this frame. Always-visible keys stay regardless.
    cols = [
        (h, k, f) for h, k, f in RESULT_COLUMNS
        if k in _ALWAYS_VISIBLE_KEYS or k in df_columns
    ]

    def _eps_block_for_quarter(k: int) -> list[tuple]:
        return [
            (f"Q-{k} Date", f"q{k}_report_date_eps", _fmt_date),
            (f"Q-{k} Reported EPS", f"q{k}_reported_eps",
             lambda x: f"{x:.2f}"),
            (f"Q-{k} Surp EPS $", f"q{k}_surprise_eps_dollar",
             lambda x: f"{x:+.2f}"),
            (f"Q-{k} Surp EPS %", f"q{k}_surprise_eps_pct",
             lambda x: f"{x:+.2f}%"),
            (f"Q-{k} YoY EPS %", f"q{k}_yoy_eps_pct",
             lambda x: f"{x:+.2f}%"),
        ]

    def _rev_block_for_quarter(k: int) -> list[tuple]:
        return [
            (f"Q-{k} Date", f"q{k}_report_date_rev", _fmt_date),
            (f"Q-{k} Reported Rev", f"q{k}_reported_rev",
             lambda x: f"{x:,.1f}"),
            (f"Q-{k} Surp Rev $", f"q{k}_surprise_rev_dollar",
             lambda x: f"{x:+,.1f}"),
            (f"Q-{k} Surp Rev %", f"q{k}_surprise_rev_pct",
             lambda x: f"{x:+.2f}%"),
            (f"Q-{k} YoY Rev %", f"q{k}_yoy_rev_pct",
             lambda x: f"{x:+.2f}%"),
        ]

    # Interleave only kicks in when both sides have data. Single-side
    # case ignores the flag entirely → no behavior change for users
    # who only run EPS or only Rev beats.
    use_interleave = bool(interleave_quarters) and n_eps > 0 and n_rev > 0

    if use_interleave:
        # Both Consec counter columns up front — cheaper to scan for
        # the user when reviewing per-ticker streak counts side by side.
        cols.append(("Consec EPS Beats", "consec_eps_beats", lambda x: str(int(x))))
        cols.append(("Consec Rev Beats", "consec_rev_beats", lambda x: str(int(x))))
        # Walk by quarter index up to whichever side runs longer; emit
        # whatever blocks each side still has data for at index k.
        max_q = max(n_eps, n_rev)
        for k in range(1, max_q + 1):
            if k <= n_eps:
                cols.extend(_eps_block_for_quarter(k))
            if k <= n_rev:
                cols.extend(_rev_block_for_quarter(k))
    else:
        # Default layout: all EPS quarters first (preceded by Consec
        # EPS Beats), then all Rev quarters (preceded by Consec Rev
        # Beats). Date sits to the LEFT of each quarterly triplet so
        # the user can see *when* each quarter was reported. The
        # date cell is _fmt_date-formatted, so the existing earnings-
        # aligned-blue-highlight logic applies automatically when an
        # indicator-date for this row matches this report date.
        if n_eps > 0:
            cols.append(("Consec EPS Beats", "consec_eps_beats", lambda x: str(int(x))))
            for k in range(1, n_eps + 1):
                cols.extend(_eps_block_for_quarter(k))
        if n_rev > 0:
            cols.append(("Consec Rev Beats", "consec_rev_beats", lambda x: str(int(x))))
            for k in range(1, n_rev + 1):
                cols.extend(_rev_block_for_quarter(k))

    return cols, n_eps, n_rev


def _date_to_ordinal(val):
    """Convert a date-like value to a sortable ordinal int (or None)."""
    if val is None:
        return None
    try:
        ts = pd.Timestamp(val)
        if pd.isna(ts):
            return None
        return ts.toordinal()
    except Exception:
        return None


class NumericSortProxy(QSortFilterProxyModel):
    """Proxy model that sorts numerically by UserRole data."""

    def lessThan(self, left, right):
        lhs = left.data(Qt.ItemDataRole.UserRole)
        rhs = right.data(Qt.ItemDataRole.UserRole)
        if lhs is None:
            return True
        if rhs is None:
            return False
        try:
            return float(lhs) < float(rhs)
        except (ValueError, TypeError):
            return str(lhs) < str(rhs)


class ReorderableHeader(QHeaderView):
    """Horizontal header with multi-select + right-click context menu
    for `Send to Front` / `Send to End` / `Reset Order`.

    Single-column reorder uses Qt's built-in `setSectionsMovable(True)`
    drag. Multi-column block-move is exposed via the right-click menu —
    Qt's built-in drag only supports single-section moves, so we don't
    fight it; the menu gives the user the multi-select alternative.

    Multi-select: shift/ctrl-click on header cells toggles a
    `_selected_logical: set[int]` of logical-section indices. Selected
    sections paint with a subtle background tint so the user can see
    what's currently grouped.
    """

    # Emitted whenever the visual order changes — either via Qt's
    # built-in drag or via our context-menu actions. Carries the new
    # order as a list of *logical* indices in visual order. Receivers
    # turn that into a list of column keys via the model.
    order_changed = pyqtSignal(list)

    # Emitted when the user picks "Delete column(s)" from the header
    # right-click menu. Payload is the list of LOGICAL section indices
    # to remove. Receivers (`ResultsTable`) translate the logical
    # indices into column keys via `_active_columns` and forward to
    # MainWindow which tracks deleted keys + re-renders. Always-visible
    # core columns (symbol/close/pct_gain/gain_start_date) are filtered
    # out by the menu BEFORE the action is offered, so this signal
    # never carries them.
    columns_deletion_requested = pyqtSignal(list)

    # Column cut+paste — symmetric with the row cut/paste. Cut stashes
    # the selected logical-index list on a single-shot clipboard.
    # Paste targets a column under the cursor and asks the receiver to
    # move the cut block to immediately after the target's visual
    # position. Single-shot — clipboard cleared on paste / on the
    # next column drag / on column delete.
    columns_paste_requested = pyqtSignal(list, int)

    # User picked "Reset to Default" from the right-click menu. Full
    # reset semantics: MainWindow drops both the saved column order
    # AND the hidden-column set, then re-renders. The local
    # `_reset_order()` helper only flips visual sections back to
    # canonical — fine for in-header bookkeeping but it leaves the
    # MainWindow-side `_deleted_column_keys` stale. We delegate the
    # full reset to MainWindow via this signal.
    reset_to_default_requested = pyqtSignal()

    # Convenience constants — context-menu actions report which one
    # fired so the slot doesn't need to inspect text strings.
    ACTION_FRONT = "front"
    ACTION_END = "end"
    ACTION_RESET = "reset"

    def __init__(self, parent=None):
        super().__init__(Qt.Orientation.Horizontal, parent)
        self.setSectionsMovable(True)
        self.setStretchLastSection(True)
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.customContextMenuRequested.connect(self._on_context_menu)
        self.sectionMoved.connect(self._on_section_moved)
        # Logical indices currently in the multi-select group.
        self._selected_logical: set[int] = set()
        # Allow shift/ctrl-click multi-select on column headers.
        self.setSectionsClickable(True)
        # Discoverability: the multi-select-then-drag gesture isn't
        # obvious from the UI alone. Header-wide tooltip explains it
        # so the user finds it on hover instead of guessing.
        self.setToolTip(
            "Click + drag: move this column.\n"
            "Ctrl/Shift+click multiple headers: build a multi-column "
            "selection (selected columns highlight in gold).\n"
            "Drag any selected column to move the whole block at once.\n"
            "Right-click for Send to Front / End / Reset."
        )
        # Multi-section drag tracking. Qt's built-in `setSectionsMovable`
        # only handles single-section drag; for multi-select drag, we
        # intercept mouse events here. `_block_drag_pending` is True
        # after a press on a multi-selected section but before the
        # cursor has moved enough to qualify as a drag — lets the user
        # click without inadvertently moving anything.
        self._block_drag_pending = False
        self._block_drag_active = False
        self._block_drag_press_x = 0
        self._block_drag_threshold_px = 8

        # Column-cut clipboard: list of LOGICAL indices stashed by the
        # most recent "Cut N columns" action. Empty when no cut is
        # pending. Cleared on paste / column delete / column drag /
        # next populate (fresh column set).
        self._cut_columns_clipboard: list[int] = []

    def selected_logical_indices(self) -> set[int]:
        return set(self._selected_logical)

    def clear_selection(self):
        if self._selected_logical:
            cleared = list(self._selected_logical)
            self._selected_logical.clear()
            self._refresh_selection_markers(touched_logical=cleared)
            self.viewport().update()

    def _toggle_selection(self, logical: int):
        """Toggle a section's membership in the multi-select group AND
        update its header-label marker. Centralized so both the
        ctrl-click path and any future programmatic selection share
        the same marker bookkeeping."""
        if logical in self._selected_logical:
            self._selected_logical.discard(logical)
        else:
            self._selected_logical.add(logical)
        self._refresh_selection_markers(touched_logical=[logical])

    def _refresh_selection_markers(self, *, touched_logical=None):
        """Apply (or remove) the prefix marker on the headerData text
        for every touched section. Reads the current text, strips any
        existing marker, and re-applies the marker only when the
        section is in `_selected_logical`. Operates on the SOURCE
        model (walks past any QSortFilterProxyModel wrapper) so the
        edit sticks across re-sorts."""
        from PyQt6.QtCore import Qt as _Qt
        model = self.model()
        if model is None:
            return
        # Drill through any proxy chain to reach the source model —
        # editing headerData on a proxy is silently dropped.
        try:
            while hasattr(model, "sourceModel") and model.sourceModel() is not None:
                model = model.sourceModel()
        except Exception:
            pass
        if touched_logical is None:
            touched_logical = list(range(self.count()))
        for li in touched_logical:
            if li < 0:
                continue
            try:
                cur = model.headerData(
                    li, _Qt.Orientation.Horizontal,
                    _Qt.ItemDataRole.DisplayRole,
                )
                cur_text = "" if cur is None else str(cur)
                # Strip an existing marker so we don't accumulate.
                if cur_text.startswith(self._SELECTED_MARKER):
                    cur_text = cur_text[len(self._SELECTED_MARKER):]
                new_text = (self._SELECTED_MARKER + cur_text
                            if li in self._selected_logical
                            else cur_text)
                if new_text != cur:
                    model.setHeaderData(
                        li, _Qt.Orientation.Horizontal, new_text,
                        _Qt.ItemDataRole.DisplayRole,
                    )
            except Exception as exc:
                log.debug("setHeaderData failed for logical %d: %s", li, exc)

    def mousePressEvent(self, ev):
        # Shift/Ctrl on a header cell → toggle that section's selection
        # for our multi-select group, but DON'T let the click also fire
        # Qt's built-in section-drag (it only moves one section at a
        # time and would defeat the multi-select).
        if ev.button() == Qt.MouseButton.LeftButton:
            mods = ev.modifiers()
            if mods & (Qt.KeyboardModifier.ControlModifier
                       | Qt.KeyboardModifier.ShiftModifier):
                pos = ev.position().toPoint()
                logical = self.logicalIndexAt(pos)
                if logical >= 0:
                    self._toggle_selection(logical)
                    self.viewport().update()
                    return  # swallow — don't trigger sort or drag

            # Plain left-click. If it lands on a section that's part of
            # an existing multi-select group AND the group has 2+
            # entries, intercept Qt's default single-section drag and
            # arm our own block-drag tracker. The actual move only
            # fires if the cursor moves past the drag threshold.
            pos = ev.position().toPoint()
            logical = self.logicalIndexAt(pos)
            if (logical >= 0 and logical in self._selected_logical
                    and len(self._selected_logical) >= 2):
                self._block_drag_pending = True
                self._block_drag_active = False
                self._block_drag_press_x = pos.x()
                ev.accept()
                return  # don't let Qt start its own drag

            # Plain click on a column NOT in the multi-select group →
            # clear the group and let Qt do its default thing.
            if logical >= 0 and logical not in self._selected_logical:
                self.clear_selection()
        super().mousePressEvent(ev)

    def mouseMoveEvent(self, ev):
        # If a block-drag is pending, watch for the cursor to move past
        # the threshold. Once it does, switch to "active" mode and
        # show a horizontal-resize cursor as a drag affordance. Don't
        # forward to super while active (Qt would try to also drag
        # one section internally).
        if self._block_drag_pending:
            x = ev.position().toPoint().x()
            if abs(x - self._block_drag_press_x) >= self._block_drag_threshold_px:
                self._block_drag_active = True
                self.setCursor(Qt.CursorShape.SizeHorCursor)
        if self._block_drag_active:
            ev.accept()
            return
        super().mouseMoveEvent(ev)

    def mouseReleaseEvent(self, ev):
        if self._block_drag_active:
            target_logical = self.logicalIndexAt(ev.position().toPoint())
            if target_logical >= 0:
                target_visual = self.visualIndex(target_logical)
                # If the user dropped onto one of the dragged sections
                # itself, anchor at the leftmost selected — same effect
                # as not moving in that case (block stays where it was).
                self._move_block_to_visual(
                    sorted(self._selected_logical),
                    target_visual,
                )
            self._cancel_block_drag()
            ev.accept()
            return
        if self._block_drag_pending:
            # Press-without-drag → user just clicked on a multi-selected
            # column with no movement. Cancel the pending state and
            # don't move anything.
            self._cancel_block_drag()
            ev.accept()
            return
        super().mouseReleaseEvent(ev)

    def _cancel_block_drag(self):
        self._block_drag_pending = False
        self._block_drag_active = False
        self.unsetCursor()

    def _move_block_to_visual(self, logical_targets: list[int],
                                target_visual: int):
        """Move every section in `logical_targets` to consecutive
        visual positions starting at `target_visual`, preserving the
        block's internal left-to-right relative order.

        Subtlety: `moveSection(a, b)` shifts every section between
        a and b by one slot. Naive iteration `for i, li in
        enumerate(ordered_targets)` therefore breaks down when the
        block moves through itself. We resolve by:
          • Move forward (target > current min): iterate targets in
            REVERSE so each move's final-position calculation isn't
            invalidated by subsequent moves of left neighbors.
          • Move backward: iterate forward; same logic mirrored.
        """
        if not logical_targets:
            return
        n = self.count()
        ordered_targets = sorted(
            logical_targets, key=lambda li: self.visualIndex(li)
        )
        target_visual = max(
            0, min(target_visual, n - len(ordered_targets))
        )
        block_size = len(ordered_targets)
        cur_min = self.visualIndex(ordered_targets[0])
        moving_forward = target_visual > cur_min

        if moving_forward:
            # Place rightmost target first at the rightmost destination
            # slot, then walk leftward.
            for offset, li in enumerate(reversed(ordered_targets)):
                final_visual = target_visual + (block_size - 1 - offset)
                cur = self.visualIndex(li)
                if cur != final_visual:
                    self.moveSection(cur, final_visual)
        else:
            # Place leftmost target first at the leftmost destination,
            # walk rightward. Symmetric to the forward case.
            for offset, li in enumerate(ordered_targets):
                final_visual = target_visual + offset
                cur = self.visualIndex(li)
                if cur != final_visual:
                    self.moveSection(cur, final_visual)

        self.clear_selection()
        order = [self.logicalIndex(v) for v in range(n)]
        self.order_changed.emit(order)

    # Marker text we prepend to a selected section's header label so
    # the user has unmistakable visual feedback that ctrl-click
    # registered. Custom paintSection overlays were proven empirically
    # unreliable on this build's Qt6 + dark-theme stylesheet
    # combination — `painter.fillRect` after `super().paintSection()`
    # was silently dropped by QStyleSheetStyle, leaving selection
    # totally invisible. Modifying the model's headerData text routes
    # through Qt's standard text-rendering pipeline which the
    # stylesheet engine doesn't intercept.
    _SELECTED_MARKER = "◆ "  # ◆ U+25C6 BLACK DIAMOND, prepended

    def paintSection(self, painter, rect, logicalIndex):
        # No paint-side override — we communicate selection via a
        # text marker on the header label (see _apply_selection_marker)
        # rather than fighting the stylesheet's paint pipeline. Plain
        # delegation to super().
        super().paintSection(painter, rect, logicalIndex)

    @pyqtSlot(int, int, int)
    def _on_section_moved(self, _logical, _old_visual, _new_visual):
        # User dragged a section. Capture the new full order and
        # broadcast.
        n = self.count()
        order = [self.logicalIndex(v) for v in range(n)]
        self.order_changed.emit(order)

    @pyqtSlot('QPoint')
    def _on_context_menu(self, point):
        logical_under_cursor = self.logicalIndexAt(point)
        # Targets: explicit multi-select group if any, else the column
        # under the cursor.
        if self._selected_logical:
            targets = sorted(self._selected_logical)
        elif logical_under_cursor >= 0:
            targets = [logical_under_cursor]
        else:
            return

        menu = QMenu(self)
        n_targets = len(targets)
        clipboard = list(self._cut_columns_clipboard)

        a_front = menu.addAction(
            f"Send {n_targets} column{'s' if n_targets > 1 else ''} to Front"
        )
        a_end = menu.addAction(
            f"Send {n_targets} column{'s' if n_targets > 1 else ''} to End"
        )
        menu.addSeparator()
        # Cut columns: stash on clipboard for later paste.
        a_cut = menu.addAction(
            f"Cut {n_targets} column{'s' if n_targets > 1 else ''}"
        )
        # Paste columns: only when clipboard non-empty AND the cursor
        # column isn't part of the cut set.
        a_paste = None
        if (clipboard
                and logical_under_cursor >= 0
                and logical_under_cursor not in clipboard):
            n_cut = len(clipboard)
            a_paste = menu.addAction(
                f"Paste {n_cut} column{'s' if n_cut > 1 else ''} after this"
            )
        menu.addSeparator()
        # Delete column(s) — receiver decides whether targets include
        # always-visible core columns and trims them. Single signal
        # round-trip; no header-side knowledge of `_ALWAYS_VISIBLE_KEYS`.
        a_delete = menu.addAction(
            f"Delete {n_targets} column{'s' if n_targets > 1 else ''}"
        )
        menu.addSeparator()
        a_reset = menu.addAction("Reset to Default (order + visibility)")
        a_clear = menu.addAction("Clear Multi-Select")

        chosen = menu.exec(self.mapToGlobal(point))
        if chosen is None:
            return
        if chosen is a_clear:
            self.clear_selection()
            return
        if chosen is a_reset:
            # Full reset: MainWindow handles dropping `_deleted_column_keys`
            # AND clearing `_results_column_order`, then re-rendering.
            # This signal is the public surface; `_reset_order()` is
            # an internal-only fallback used by tests.
            self.reset_to_default_requested.emit()
            return
        if chosen is a_delete:
            # Deleting cut columns invalidates the clipboard.
            self._cut_columns_clipboard = []
            self.columns_deletion_requested.emit(list(targets))
            self.clear_selection()
            return
        if chosen is a_cut:
            self._cut_columns_clipboard = list(targets)
            self.clear_selection()
            return
        if chosen is a_paste and a_paste is not None:
            cut = list(self._cut_columns_clipboard)
            self._cut_columns_clipboard = []  # single-shot
            if cut and logical_under_cursor not in cut:
                # "Paste after target" semantics: the target column
                # stays put; the cut block lands immediately to its
                # right. Compute the destination visual position the
                # block should START at after the cut columns are
                # logically removed:
                #   target's effective position post-removal =
                #     target_visual_now - (cut columns to the left)
                #   block goes one slot after that.
                # Adjusts for the fact that `_move_block_to_visual`
                # treats `target_visual` as an absolute slot in the
                # post-move table, where the cut block IS still
                # present (it gets re-inserted at that slot). Without
                # this compensation, "paste after rightmost column"
                # gets clamped to the table's right edge and the cut
                # block lands AFTER the rightmost section instead of
                # right after the target.
                target_visual_now = self.visualIndex(logical_under_cursor)
                cut_to_left = sum(
                    1 for li in cut
                    if self.visualIndex(li) < target_visual_now
                )
                target_visual = target_visual_now - cut_to_left + 1
                self._move_block_to_visual(cut, target_visual)
                # Mirror via signal so test harness / external
                # listeners can verify the operation happened.
                self.columns_paste_requested.emit(cut, logical_under_cursor)
            return
        if chosen is a_front:
            self._move_block(targets, to_front=True)
        elif chosen is a_end:
            self._move_block(targets, to_front=False)

    def _move_block(self, logical_targets: list[int], *, to_front: bool):
        """Right-click context-menu helper: move sections to the front
        (visual 0) or the end (visual n - len(targets))."""
        if not logical_targets:
            return
        if to_front:
            target_visual = 0
        else:
            target_visual = self.count() - len(logical_targets)
        self._move_block_to_visual(logical_targets, target_visual)

    def _reset_order(self):
        """Restore the canonical 0..N-1 visual order. Triggers
        `order_changed` so listeners can persist the reset."""
        n = self.count()
        for visual_target in range(n):
            cur = self.visualIndex(visual_target)
            if cur != visual_target:
                self.moveSection(cur, visual_target)
        self.clear_selection()
        order = list(range(n))
        self.order_changed.emit(order)


class ResultsTable(QTableView):
    """Sortable results table."""

    # Phase 8 §8.3: green text for cells inside a contiguous beat streak.
    _STREAK_GREEN = QColor("#4caf50")
    # Display-only "would-have-failed" color. Each cell whose key is
    # marked True in `row['_display_only_fails']` renders in this red
    # — the user's signal that the value, while shown for context,
    # would not have made it through the corresponding filter. Picked
    # to be unmistakable against both the dark-theme #1e1e1e
    # background and the alternating-row #252525, while remaining
    # distinct from the streak green and every alignment palette entry.
    _FAIL_RED = QColor("#e74c3c")
    # Earnings-aligned date highlight palette. Curated 10-color set
    # restricted to the cool half of the wheel (cyan → teal → blue →
    # indigo → purple → violet) plus deliberate exclusions:
    #   - NO reds / red-adjacent oranges (hue [330°, 30°])
    #   - NO yellows / ambers / mustards (hue [30°, 90°])
    #   - NO greens or limes (hue [90°, 165°])
    #   - NO dark colors (value < 150)
    # The exclusions match the user's stated colorblind / readability
    # preferences. Within a row, distinct earnings dates each draw a
    # distinct entry via the per-match seeded picker in populate().
    _ALIGN_PALETTE = [
        QColor("#4a90d9"),  # blue
        QColor("#26c6da"),  # cyan
        QColor("#00bcd4"),  # bright cyan
        QColor("#42a5f5"),  # light blue
        QColor("#5c6bc0"),  # indigo
        QColor("#3949ab"),  # strong indigo
        QColor("#ab47bc"),  # purple
        QColor("#7e57c2"),  # deep purple
        QColor("#c084fc"),  # light violet
        QColor("#26a69a"),  # teal
    ]
    # Backwards-compat alias for tests + any future "force monochrome"
    # toggle. Points at the palette's first entry.
    _EARNINGS_BLUE = _ALIGN_PALETTE[0]

    # Emitted when the user reorders columns (drag or right-click
    # context menu). Carries the new visual order as a list of column
    # keys. MainWindow listens to this to persist across timeframe
    # switches and Excel exports.
    column_order_changed = pyqtSignal(list)

    # Emitted when the user requests deletion of selected rows (via
    # Delete key or right-click → "Delete selected row(s)"). Payload is
    # the list of symbols to remove from the underlying scan results
    # for the current period. MainWindow's slot mutates
    # `_period_results` and re-renders so the deletion persists across
    # view-filter toggles, sort changes, and tab switches. Reset on
    # next scan.
    rows_deletion_requested = pyqtSignal(list)

    # Cut+paste row-reorder. Cut just stashes selected symbols on the
    # clipboard (no display change). Paste targets a row and asks the
    # MainWindow to mutate `_period_results[active]` so the cut symbols
    # are inserted right after the target. Single-shot — clipboard is
    # cleared on paste, on delete (any row in clipboard might no
    # longer exist), and on next scan.
    #
    # Signature: (cut_symbols, target_symbol). MainWindow does the
    # actual reorder so the underlying df is the source of truth and
    # the move persists across re-renders.
    rows_paste_requested = pyqtSignal(list, str)

    # Emitted when the user requests deletion of one or more columns
    # via the header right-click menu. Payload is the list of column
    # KEYS (already filtered to exclude always-visible core columns).
    # MainWindow tracks the deleted keys in a per-active-period set
    # and excludes them from rendering on every re-populate. Reset on
    # next scan.
    columns_deletion_requested = pyqtSignal(list)

    # Forwarded straight from ReorderableHeader. Full-reset semantics —
    # MainWindow clears both `_results_column_order` and
    # `_deleted_column_keys`, then re-renders.
    columns_reset_requested = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.model_src = QStandardItemModel()
        self.model_src.setHorizontalHeaderLabels([c[0] for c in RESULT_COLUMNS])

        self.proxy = NumericSortProxy()
        self.proxy.setSourceModel(self.model_src)
        self.setModel(self.proxy)

        self.setSortingEnabled(True)
        self.setAlternatingRowColors(True)
        self.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self._reorderable_header = ReorderableHeader(self)
        self.setHorizontalHeader(self._reorderable_header)
        self._reorderable_header.order_changed.connect(self._on_header_reordered)
        self._reorderable_header.columns_deletion_requested.connect(
            self._on_header_columns_deletion_requested
        )
        self._reorderable_header.reset_to_default_requested.connect(
            self.columns_reset_requested
        )
        self.verticalHeader().setDefaultSectionSize(24)
        # Re-entrancy guard for populate(). The chunked-render loop
        # yields to QApplication.processEvents() every 200 rows so
        # the GUI stays responsive at 15k+ rows. processEvents lets
        # any pending signal fire — including a queued dropdown click
        # whose slot calls populate() again. Without this guard, the
        # inner populate would `setRowCount(0)` on the model the
        # outer one is still writing to. Defense-in-depth: callers
        # SHOULD also guard at their slot level, but `populate` is
        # the public entry point so it owns the final invariant.
        self._populate_in_flight = False
        # Column-width cache for the timeframe-switch fast path.
        # Key = tuple of column keys (the "shape" of the table);
        # value = list of column widths in pixels. After the first
        # render of a given column set we snapshot the widths from
        # `resizeColumnsToContents`, then on subsequent renders with
        # the same column set we restore widths via setColumnWidth
        # (a constant-time call per column) instead of re-running
        # the per-cell measurement loop (which is O(rows × cols) and
        # was the dominant cost of the timeframe-switch hang the
        # user reported — `resizeColumnsToContents` over 100+ cols ×
        # ~400 rows takes 5+ seconds, long enough for Windows to
        # flag the process as Not Responding). Cache invalidates
        # automatically when the column set changes (new scan with
        # different filters, etc.).
        self._cached_column_widths: dict[tuple, list[int]] = {}

        # The dynamic column set used in the most recent populate() call —
        # exposed so the GUI's export dialog hands the user the same
        # columns it sees in the table (including the dynamic Q-i block).
        self._active_columns: list[tuple] = list(RESULT_COLUMNS)
        self._active_n_eps_quarters: int = 0
        self._active_n_rev_quarters: int = 0
        # Persistent visual order — list of column keys in the user's
        # preferred display order. Survives timeframe switches and is
        # consulted by Excel export. Keys absent from the current
        # frame's column set are skipped at apply time; new keys not
        # in this list get appended at the end (preserves intent
        # across re-scans where filter on/off may change).
        self._saved_column_keys: list[str] = []

        # Interleave-quarters toggle (default off — back-compat layout).
        # When True AND both EPS and Rev beats data are present, the
        # `_build_dynamic_columns` helper interleaves the per-quarter
        # blocks (Q-1 EPS+Rev together, Q-2 EPS+Rev together, ...) so
        # all data for one quarter sits adjacent. No-op when only one
        # side has data — guarantees zero behavior change for users
        # who run EPS-only or Rev-only beats scans.
        self._interleave_quarters: bool = False

        # Delete-rows wiring: enable the multi-select + custom context
        # menu so the user can right-click → "Delete selected row(s)".
        # Delete key press is caught by keyPressEvent below.
        self.setSelectionMode(QTableView.SelectionMode.ExtendedSelection)
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.customContextMenuRequested.connect(self._show_row_context_menu)

        # Cut+paste reorder clipboard — list of symbols stashed by the
        # most recent "Cut" action. Empty when no cut is pending.
        # Cleared on paste, on row deletion (deleted symbols may have
        # been in the clipboard), and on next scan (fresh populate
        # with a different df).
        self._cut_clipboard: list[str] = []

    # ── Delete-rows: collect symbols + emit the request signal ───────

    def _selected_symbols(self) -> list[str]:
        """Return the symbols (in the underlying df 'symbol' column) for
        every currently-selected row, deduped, in selection order. The
        proxy's row index ≠ source row, so we go through the proxy's
        mapToSource → source model item lookup."""
        sel = self.selectionModel()
        if sel is None:
            return []
        symbol_col = None
        for c, (_h, key, _f) in enumerate(self._active_columns):
            if key == "symbol":
                symbol_col = c
                break
        if symbol_col is None:
            return []
        out: list[str] = []
        seen: set[str] = set()
        for idx in sel.selectedRows():
            src_idx = self.proxy.mapToSource(idx)
            item = self.model_src.item(src_idx.row(), symbol_col)
            if item is None:
                continue
            sym = item.text().strip()
            if sym and sym not in seen:
                seen.add(sym)
                out.append(sym)
        return out

    def _symbol_at_viewport_pos(self, pos) -> str:
        """Return the symbol of the row at the given viewport-relative
        position, or empty string if no row hit. Used by the right-
        click menu so Paste can target the row under the cursor —
        independent of what's currently SELECTED (the selection might
        be empty after a Cut, which clears it)."""
        idx = self.indexAt(pos)
        if not idx.isValid():
            return ""
        symbol_col = None
        for c, (_h, key, _f) in enumerate(self._active_columns):
            if key == "symbol":
                symbol_col = c
                break
        if symbol_col is None:
            return ""
        src_idx = self.proxy.mapToSource(idx)
        item = self.model_src.item(src_idx.row(), symbol_col)
        if item is None:
            return ""
        return item.text().strip()

    def _show_row_context_menu(self, pos):
        """Right-click context menu on the table body. Offers:
          - Cut N selected row(s): stash on clipboard for later paste.
          - Paste N row(s) after this row: only when clipboard is
            non-empty AND the target row isn't itself in the clipboard.
          - Delete N selected row(s): hard-delete from active period.

        Cut + Paste together let the user reorder rows without changing
        scan output — the MainWindow paste handler mutates
        `_period_results[active]` so the new ordering persists across
        sort / view-filter / tab switches.
        """
        from PyQt6.QtWidgets import QMenu
        selected = self._selected_symbols()
        target = self._symbol_at_viewport_pos(pos)
        clipboard = list(self._cut_clipboard)

        # No menu when there's nothing useful to offer:
        # - no selection AND no clipboard AND no target row (clicked
        #   on empty space) → bail.
        if not selected and not clipboard and not target:
            return

        menu = QMenu(self)

        if selected:
            n = len(selected)
            cut_label = (
                f"Cut {n} selected rows" if n > 1
                else f"Cut row '{selected[0]}'"
            )
            cut_act = menu.addAction(cut_label)
            cut_act.triggered.connect(
                lambda: self._set_cut_clipboard(list(selected))
            )

        # Paste only when we have a clipboard AND a valid target that
        # isn't part of the cut set (would orphan the target).
        if clipboard and target and target not in clipboard:
            n = len(clipboard)
            paste_label = (
                f"Paste {n} rows after '{target}'" if n > 1
                else f"Paste '{clipboard[0]}' after '{target}'"
            )
            paste_act = menu.addAction(paste_label)
            paste_act.triggered.connect(
                lambda: self._fire_paste(list(clipboard), target)
            )

        if selected:
            menu.addSeparator()
            n = len(selected)
            del_label = (
                f"Delete {n} selected rows" if n > 1
                else f"Delete row '{selected[0]}'"
            )
            del_act = menu.addAction(del_label)
            del_act.triggered.connect(
                lambda: self.rows_deletion_requested.emit(selected)
            )

        if menu.isEmpty():
            return
        menu.exec(self.viewport().mapToGlobal(pos))

    def _set_cut_clipboard(self, symbols: list[str]) -> None:
        """Stash symbols on the cut clipboard. Public for tests."""
        self._cut_clipboard = [s for s in symbols if s]

    def cut_clipboard(self) -> list[str]:
        """Return a copy of the cut clipboard. Public for tests + the
        view-filter pipeline (which clears it on row deletion)."""
        return list(self._cut_clipboard)

    def clear_cut_clipboard(self) -> None:
        """Clear the cut clipboard. Called by MainWindow after a paste,
        a delete, or on every fresh scan populate."""
        self._cut_clipboard = []

    def _fire_paste(self, cut_symbols: list[str], target: str) -> None:
        """Emit the paste signal. The MainWindow handler does the
        actual df reorder + re-render and is responsible for clearing
        the clipboard via clear_cut_clipboard()."""
        if not cut_symbols or not target or target in cut_symbols:
            return
        self.rows_paste_requested.emit(cut_symbols, target)

    def keyPressEvent(self, event):
        """Delete key on a selected row → emit deletion request.
        Falls through to the default handler for everything else so
        sort hotkeys / arrow navigation still work."""
        from PyQt6.QtCore import Qt as _Qt
        if event.key() in (_Qt.Key.Key_Delete, _Qt.Key.Key_Backspace):
            symbols = self._selected_symbols()
            if symbols:
                self.rows_deletion_requested.emit(symbols)
                event.accept()
                return
        super().keyPressEvent(event)

    @property
    def active_columns(self) -> list[tuple]:
        return list(self._active_columns)

    @property
    def active_beats_quarters(self) -> tuple[int, int]:
        """Return (n_eps_q, n_rev_q) — width of the per-quarter blocks
        in the most recent populate() call."""
        return self._active_n_eps_quarters, self._active_n_rev_quarters

    def current_column_order(self) -> list[str]:
        """Return the column keys in their CURRENT visual order
        (after any user reorder). Used by Excel export to write
        sheets that match what the user sees in the table."""
        header = self.horizontalHeader()
        n = self.model_src.columnCount()
        return [
            self._active_columns[header.logicalIndex(v)][1]
            for v in range(n)
            if header.logicalIndex(v) < len(self._active_columns)
        ]

    def set_saved_column_order(self, keys: list[str]):
        """Restore a previously-saved column order. Applied after the
        next populate() call (which is when the model has columns to
        move). Keys not present in the current model are ignored;
        keys in the model but not in `keys` go to the end in their
        canonical order."""
        self._saved_column_keys = list(keys)
        # If the table is already populated, apply immediately.
        if self.model_src.columnCount() > 0:
            self._apply_saved_order()

    def set_interleave_quarters(self, on: bool) -> None:
        """Enable/disable the interleave-quarters layout. No-op when
        the value is unchanged. Caller is responsible for triggering
        a re-render (typically via MainWindow's view-filter pipeline)
        — this setter only flips the flag so the next `populate()`
        picks the new layout."""
        on = bool(on)
        if self._interleave_quarters == on:
            return
        self._interleave_quarters = on
        # Invalidate the column-width cache since the column SET
        # changed (different ordering / both Consec counters move
        # adjacent in interleave mode).
        self._cached_column_widths = {}

    @property
    def interleave_quarters(self) -> bool:
        return self._interleave_quarters

    def _apply_saved_order(self):
        """Walk `_saved_column_keys`; for each key that exists in the
        current `_active_columns`, move its section to the next
        position in line. Untouched columns (keys not in the saved
        list) remain in their canonical positions, which lands them
        at the end after all explicitly-ordered columns."""
        if not self._saved_column_keys:
            return
        header = self.horizontalHeader()
        keys_in_model = [c[1] for c in self._active_columns]
        target_visual = 0
        for saved_key in self._saved_column_keys:
            try:
                logical_idx = keys_in_model.index(saved_key)
            except ValueError:
                continue  # column not in this frame (filter is off)
            current_visual = header.visualIndex(logical_idx)
            if current_visual != target_visual:
                # Block our own signal — we're not "user-reordering",
                # we're restoring a saved order. The MainWindow already
                # has _saved_column_keys, so re-emitting is redundant
                # noise.
                header.blockSignals(True)
                try:
                    header.moveSection(current_visual, target_visual)
                finally:
                    header.blockSignals(False)
            target_visual += 1

    @pyqtSlot(list)
    def _on_header_reordered(self, logical_order):
        """Convert the header's logical-index order into a list of
        column keys and broadcast via column_order_changed."""
        keys = []
        for li in logical_order:
            if 0 <= li < len(self._active_columns):
                keys.append(self._active_columns[li][1])
        self._saved_column_keys = keys
        self.column_order_changed.emit(keys)

    @pyqtSlot(list)
    def _on_header_columns_deletion_requested(self, logical_indices: list):
        """Translate the header's logical-index payload into column
        keys, filter out the always-visible core columns (symbol,
        close, pct_gain, gain_start_date — deleting these would break
        export and core display invariants), and forward to
        MainWindow via the public signal."""
        keys: list[str] = []
        for li in logical_indices:
            if 0 <= li < len(self._active_columns):
                key = self._active_columns[li][1]
                if key in _ALWAYS_VISIBLE_KEYS:
                    continue
                if key not in keys:
                    keys.append(key)
        if keys:
            self.columns_deletion_requested.emit(keys)

    def populate(self, df):
        """Fill table from a scan results DataFrame.

        Phase 7 I10: pre-sizes the model and disables sorting during the
        bulk insert so Qt doesn't re-layout after each row.

        Phase 8 §8.3: when the frame contains a `consec_eps_beats` or
        `consec_rev_beats` column, dynamically extend the column set
        with Q-i triplets up to the longest streak in the result set,
        and apply green ForegroundRole to cells inside each row's
        streak boundary.

        Re-entrancy guard: the chunked-render loop yields to
        QApplication.processEvents() every 200 rows so the GUI stays
        responsive at 15k+ rows. processEvents lets any pending signal
        fire — including a queued dropdown click whose slot calls
        populate() again. If we're already mid-render we MUST drop the
        re-entry; the inner populate would `setRowCount(0)` on the
        model the outer one is still writing to, corrupting state.
        Callers (notably `_on_timeframe_changed`) also guard at slot
        level — this is the defense-in-depth backstop.
        """
        if self._populate_in_flight:
            log.debug(
                "populate() re-entered — dropping inner call "
                "(outer render still in progress)."
            )
            return
        self._populate_in_flight = True
        try:
            self._populate_impl(df)
        finally:
            self._populate_in_flight = False

    def _populate_impl(self, df):
        was_sortable = self.isSortingEnabled()
        self.setSortingEnabled(False)
        # Critical perf wrappers — measured against a 379-row × 97-col
        # synthetic frame (the user's actual sequenced-run shape):
        #
        #   pre-fix populate (#2 onward):     124,608 ms  (HANG)
        #   + setUpdatesEnabled wrap:         124,608 ms  (no help)
        #   + width cache:                    124,608 ms  (no help)
        #   + proxy.setSourceModel(None):         363 ms  ← THE fix
        #
        # The proxy-detach is what actually matters. With the proxy
        # attached to the source model, every `setItem` call during
        # the bulk insert triggers a `dataChanged` signal that the
        # proxy processes (re-checks sort/filter for that cell). For
        # 38k cells that's 38k proxy round-trips, dominating the
        # render. Detaching before the loop suppresses all those
        # notifications; one `modelReset` fires when we reattach,
        # and the proxy rebuilds its mapping in a single pass.
        #
        # `setUpdatesEnabled(False)` is kept as a secondary
        # optimization — stops the view from repainting per cell
        # during the loop. Smaller win than proxy detach but free.
        self.setUpdatesEnabled(False)
        self.proxy.setSourceModel(None)
        try:
            self.model_src.setRowCount(0)  # clear

            if df is None or df.empty:
                return

            cols, n_eps, n_rev = _build_dynamic_columns(
                df, interleave_quarters=self._interleave_quarters,
            )
            self._active_columns = cols
            self._active_n_eps_quarters = n_eps
            self._active_n_rev_quarters = n_rev
            self.model_src.setColumnCount(len(cols))
            self.model_src.setHorizontalHeaderLabels([c[0] for c in cols])
            # Column set just changed — clear any stale multi-select
            # marker on the header. Setting fresh headers wiped them
            # from the model anyway; keeping the in-memory set in
            # sync prevents a re-applied marker on a now-different
            # column.
            self._reorderable_header._selected_logical.clear()

            n = len(df)
            self.model_src.setRowCount(n)

            # Audit ISSUE-10: chunked population to keep the GUI
            # responsive while building large result tables. With
            # updates disabled (above) each setItem is much faster,
            # but for very large renders (15k+ rows) we still yield
            # to processEvents periodically so Windows doesn't flag
            # the process as Not Responding. Lowered from 500 → 200
            # so even mid-size renders (~400 rows) yield once.
            # The re-entrancy guard in `populate()` handles the
            # "user clicked dropdown again during yield" case.
            from PyQt6.QtWidgets import QApplication as _QApp
            _CHUNK = 200
            for r in range(n):
                try:
                    self._populate_row(r, df.iloc[r], cols)
                except Exception as exc:
                    # Per-row safety net: a single bad row MUST NOT
                    # crash the table render. Log and continue with the
                    # next row — leaves an empty row in the table for
                    # the affected ticker, which is far better than
                    # losing the entire result set (and on the main
                    # thread under PyInstaller windowed mode, far
                    # better than a process-level crash).
                    log.error(
                        "populate row %d crashed: %s", r, exc, exc_info=True,
                    )
                if (r + 1) % _CHUNK == 0:
                    try:
                        _QApp.processEvents()
                    except Exception:
                        pass
        finally:
            # Reattach the proxy BEFORE re-enabling sorting / updates
            # so the view sees a consistent (model attached + sorting
            # ready) state when it next paints. The reattach fires a
            # single `modelReset` signal which the proxy uses to
            # rebuild its mapping in one pass.
            self.proxy.setSourceModel(self.model_src)
            self.setSortingEnabled(was_sortable)
            self.setUpdatesEnabled(True)

        # Re-apply user's saved column order (drag / context-menu
        # moves) so timeframe switches don't reset what they did. Runs
        # AFTER the proxy is reattached so header.visualIndex /
        # moveSection see the correct column count.
        self._apply_saved_order()

        # Column-width handling — the OTHER dominant cost of the
        # timeframe-switch hang. `resizeColumnsToContents()` measures
        # every cell in every column to find the max width per
        # column, an O(rows × cols) operation. For 100+ columns
        # × 400 rows that's ~40k font-metric calls = multiple seconds.
        # Cache the computed widths keyed by the column-set tuple,
        # so subsequent renders with the same shape (which is the
        # common case for sequenced runs — every period has the same
        # filter shape) restore widths via setColumnWidth (constant
        # time per column) instead of re-measuring.
        col_key_tuple = tuple(c[1] for c in cols)
        cached = self._cached_column_widths.get(col_key_tuple)
        if cached is not None and len(cached) == len(cols):
            for i, w in enumerate(cached):
                self.setColumnWidth(i, w)
        else:
            self.resizeColumnsToContents()
            # Snapshot the widths Qt just computed so the next
            # same-shape render hits the fast path.
            self._cached_column_widths[col_key_tuple] = [
                self.columnWidth(i) for i in range(len(cols))
            ]

    def _populate_row(self, r: int, row_data, cols):
        """Render a single result row into the model. Extracted from
        `populate()` so each row can be wrapped in its own try/except
        without polluting the bulk-insert loop. Operates only on the
        current model — no signal emissions, no I/O — so it's cheap
        to call per-row even at 15k rows."""
        # Per-row streak counts drive the green-text decision.
        # NaN-safe coerce: when consec-beats display-only is on
        # but the ticker has no Zacks history, pandas writes NaN
        # into the column. `int(nan or 0)` raises ValueError
        # because Python evaluates `bool(nan) is True`, so
        # `nan or 0` returns nan rather than 0. The dedicated
        # helper preserves the "missing data → 0 streak" intent
        # and prevents one untested ticker from crashing the
        # entire populate loop (which on the main thread would
        # take down the GUI).
        eps_streak = _safe_streak(row_data.get("consec_eps_beats"))
        rev_streak = _safe_streak(row_data.get("consec_rev_beats"))

        # Display-only fail flags from the scanner. Keys are column
        # names; True means "would have failed the filter currently
        # in display-only mode." Empty / None means no display-only
        # filters apply to this row.
        fail_flags = row_data.get("_display_only_fails")
        if not isinstance(fail_flags, dict):
            fail_flags = {}

        # Per-row earnings-aligned date map drives the multi-color
        # highlight. Each unique (ticker, date) pair seeds its own
        # pseudo-random palette pick; the within-row loop linear-probes
        # forward through the palette to avoid collisions when two
        # seeds happen to land on the same index.
        aligned_iso = row_data.get("_earnings_aligned_dates")
        # Fuzzy match support: when set, this dict maps any matched
        # date (indicator OR report) to a CANONICAL iso (always the
        # report date). The color map is then keyed by canonical iso
        # so paired-but-not-identical dates share one color. Absent
        # for exact-only match results — falls back to the legacy
        # one-color-per-iso behavior.
        canon_map = row_data.get("_earnings_aligned_canon")
        if not isinstance(canon_map, dict):
            canon_map = None
        aligned_color_map = {}
        if isinstance(aligned_iso, list) and aligned_iso:
            import random as _random
            # Seed colors off canonical isos when present (one color
            # per match group), else off the raw iso list (legacy).
            seed_isos = (
                sorted(set(canon_map.values()))
                if canon_map else sorted(set(aligned_iso))
            )
            # Earnings-anchor gate: a color group must include at least
            # one earnings-source cell anchoring to the canonical iso.
            # Without this gate, pairs of non-earnings indicator dates
            # (e.g. gap_date + surge_date) that both land near the same
            # earnings event would share a color even with no visible
            # earnings cell — a false visual pairing the user can't
            # interpret. Walk the rendered cols once to collect the set
            # of canonical isos that have an earnings cell on this row.
            earnings_anchored_isos: set[str] = set()
            for _h, _k, _f in cols:
                if not _is_earnings_anchor_key(_k):
                    continue
                anchor_v = _anchor_date_value(_k, row_data)
                if anchor_v is None:
                    continue
                try:
                    _ts = pd.Timestamp(anchor_v)
                    if pd.isna(_ts):
                        continue
                except (TypeError, ValueError):
                    continue
                _v_iso = _ts.normalize().date().isoformat()
                _lookup = (
                    canon_map.get(_v_iso, _v_iso) if canon_map else _v_iso
                )
                earnings_anchored_isos.add(_lookup)
            seed_isos = [
                iso for iso in seed_isos if iso in earnings_anchored_isos
            ]

            palette_n = len(self._ALIGN_PALETTE)
            used: set[int] = set()
            symbol = row_data.get("symbol", "")
            for iso in seed_isos:
                seed = f"{symbol}|{iso}"
                base = _random.Random(seed).randrange(palette_n)
                idx = base
                for _ in range(palette_n):
                    if idx not in used:
                        break
                    idx = (idx + 1) % palette_n
                used.add(idx)
                aligned_color_map[iso] = self._ALIGN_PALETTE[idx]

        for c, (header, key, fmt) in enumerate(cols):
            val = row_data.get(key)
            item = QStandardItem()
            if val is not None and str(val) != "nan":
                try:
                    item.setText(fmt(val))
                except Exception:
                    item.setText(str(val))
                if fmt is _fmt_date:
                    raw = _date_to_ordinal(val)
                else:
                    raw = val.item() if hasattr(val, "item") else val
                item.setData(raw, Qt.ItemDataRole.UserRole)
            else:
                item.setText("N/A")
                item.setData(None, Qt.ItemDataRole.UserRole)

            # Phase 8 §8.3: green-text the streak cells (Q-1..
            # Q-{streak}) so the user can see exactly where the
            # streak breaks. Audit L6: explicit suffix-based metric
            # detection — substring `"rev" in key` would silently
            # misclassify a future column rename. The Q-i Date column
            # uses `report_date_eps` / `report_date_rev` suffixes so
            # each block colors its own date independently.
            qm = _Q_COL_RE.match(key)
            if qm is not None:
                q_num = int(qm.group(1))
                suffix = qm.group(2)
                is_rev = (
                    suffix.endswith("_rev")
                    or suffix.startswith("reported_rev")
                    or suffix.startswith("surprise_rev")
                    or suffix.startswith("yoy_rev")
                    or suffix == "report_date_rev"
                )
                is_eps = (
                    suffix.endswith("_eps")
                    or suffix.startswith("reported_eps")
                    or suffix.startswith("surprise_eps")
                    or suffix.startswith("yoy_eps")
                    or suffix == "report_date_eps"
                )
                if is_rev and q_num <= rev_streak:
                    item.setForeground(self._STREAK_GREEN)
                elif is_eps and q_num <= eps_streak:
                    item.setForeground(self._STREAK_GREEN)

            # Display-only red-on-fail: paint values that would have
            # been filtered out in red. Runs AFTER streak-green so a
            # Q-i cell that broke the streak AND fails the threshold
            # ends up red (the more specific signal). Date columns
            # don't have thresholds, so they never receive fail flags.
            if fail_flags.get(key) is True:
                item.setForeground(self._FAIL_RED)

            # Earnings-alignment color (generalized 2026-05): every
            # cell has an "anchor date" (see `_anchor_date_value`).
            # If that anchor matches one of the row's
            # aligned_color_map entries, the cell gets the matching
            # palette color. This means the entire "unit" — gap value
            # + gap date, or a Q-i triplet of date / reported / surp $
            # / surp % — shares a color when the unit's date is part
            # of an alignment match. Runs AFTER streak-green and
            # red-on-fail so the alignment color wins on conflict
            # (date-pair signal is more specific than the others).
            if aligned_color_map:
                anchor_val = _anchor_date_value(key, row_data)
                if anchor_val is not None:
                    try:
                        ts = pd.Timestamp(anchor_val)
                        if not pd.isna(ts):
                            v_iso = ts.normalize().date().isoformat()
                            # When a canonical map is present, route
                            # the cell's iso through it so paired-
                            # but-not-identical dates land on the
                            # same color entry. No-op for exact-only
                            # match results (legacy behavior).
                            lookup_iso = (
                                canon_map.get(v_iso, v_iso)
                                if canon_map else v_iso
                            )
                            color = aligned_color_map.get(lookup_iso)
                            if color is not None:
                                item.setForeground(color)
                    except Exception:
                        pass

            item.setEditable(False)
            self.model_src.setItem(r, c, item)

    def get_symbols(self) -> list[str]:
        """Return ticker symbols in current sort order (via proxy model)."""
        symbols = []
        for r in range(self.proxy.rowCount()):
            idx = self.proxy.index(r, 0)
            val = idx.data(Qt.ItemDataRole.DisplayRole)
            if val:
                symbols.append(val)
        return symbols


# ============================================================================
# Log panel
# ============================================================================

class LogPanel(QWidget):
    """Expandable log panel with real-time output and disk persistence."""

    append_signal = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.text = QTextEdit()
        self.text.setReadOnly(True)
        self.text.setFont(QFont("Consolas", 9))
        self.text.setStyleSheet("background: #1e1e1e; color: #d4d4d4;")

        btn_clear = QPushButton("Clear")
        btn_clear.setFixedWidth(60)
        btn_clear.clicked.connect(self.text.clear)

        self.btn_blacklist_errors = QPushButton("Send Errors to Blacklist")
        self.btn_blacklist_errors.setFixedWidth(160)
        self.btn_blacklist_errors.setVisible(False)
        self.btn_blacklist_errors.setStyleSheet(
            "background: #8b0000; color: white; font-weight: bold;"
        )

        top = QHBoxLayout()
        top.addWidget(QLabel("Log"))
        top.addStretch()
        top.addWidget(self.btn_blacklist_errors)
        top.addWidget(btn_clear)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addLayout(top)
        layout.addWidget(self.text)

        self.append_signal.connect(self._append)

        # Disk log file — Phase 2 I6: keep a single handle open (line-buffered)
        # instead of opening+closing on every line. Closed in close_log() from
        # MainWindow.closeEvent.
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        config.LOG_DIR.mkdir(parents=True, exist_ok=True)
        self._log_path = config.LOG_DIR / f"scan_{ts}.log"
        try:
            self._log_file = open(self._log_path, "a", encoding="utf-8", buffering=1)
        except Exception as exc:
            log.warning("Could not open log file %s: %s", self._log_path, exc)
            self._log_file = None

    @pyqtSlot(str)
    def _append(self, msg: str):
        self.text.append(msg)
        # Auto-scroll
        sb = self.text.verticalScrollBar()
        sb.setValue(sb.maximum())
        # Persist to disk via the long-lived line-buffered handle.
        # Audit ISSUE-11: explicit `flush()` after every write.
        # Line-buffering (buffering=1) flushes on newline IN PYTHON, but
        # the OS-level disk write is still buffered until close_log.
        # If the process aborts (e.g., uncaught exception in another
        # slot, OOM, segfault), the most recent N seconds of log lines
        # — exactly the lines that contain the crash context — are
        # lost. Explicit flush keeps the on-disk log within ~1 line
        # of reality so the post-mortem actually has data.
        if self._log_file is not None:
            try:
                self._log_file.write(msg + "\n")
                self._log_file.flush()
            except Exception:
                pass

    def write_line(self, msg: str):
        self.append_signal.emit(msg)

    def close_log(self):
        """Flush and close the log file handle. Called from MainWindow.closeEvent."""
        if self._log_file is not None:
            try:
                self._log_file.flush()
                self._log_file.close()
            except Exception:
                pass
            self._log_file = None
