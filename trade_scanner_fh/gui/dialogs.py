"""Reusable dialog classes used by the main window.

WatchlistDialog is the only fully-typed standalone dialog; the rest of the
GUI's modal prompts (backoff settings, blacklist editor, coverage gaps,
rebuild tickers, manual input) live inline inside MainWindow methods because
they're small and tightly coupled to that window's state."""
from __future__ import annotations

from datetime import date
from typing import Optional

from PyQt6.QtCore import QDate, Qt, pyqtSignal
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QAbstractItemView, QCheckBox, QComboBox, QDateEdit, QDialog,
    QDialogButtonBox, QDoubleSpinBox, QGridLayout, QGroupBox,
    QHBoxLayout, QLabel, QListWidget, QListWidgetItem, QPushButton,
    QScrollArea, QSpinBox, QVBoxLayout, QWidget,
)

from ..tradestation import BridgeConfig


class WatchlistDialog(QDialog):
    """Modal config dialog shown before launching the watchlist bridge."""

    start_requested = pyqtSignal(BridgeConfig)
    cancelled = pyqtSignal()

    def __init__(self, n_tickers: int, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Send to Watchlist")
        self.setModal(True)
        self.setMinimumWidth(520)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(40, 30, 40, 30)

        title = QLabel(f"Send {n_tickers} Tickers to Watchlist")
        title.setFont(QFont("Segoe UI", 14, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        layout.addWidget(QLabel(""))

        # Starting value (1-based for user, converted to 0-based internally)
        row_sv = QHBoxLayout()
        row_sv.addWidget(QLabel("Starting at ticker #:"))
        self.spin_start = QSpinBox()
        self.spin_start.setRange(1, max(n_tickers, 1))
        self.spin_start.setValue(1)
        self.spin_start.setMinimumWidth(80)
        self.spin_start.setToolTip(
            "Which ticker to start from (1 = first). "
            "Use this to resume a partially-completed transfer."
        )
        row_sv.addWidget(self.spin_start)
        row_sv.addStretch()
        layout.addLayout(row_sv)

        # Batch size
        row_bs = QHBoxLayout()
        row_bs.addWidget(QLabel("Batch size (0 = no batching):"))
        self.spin_batch = QSpinBox()
        self.spin_batch.setRange(0, 9999)
        self.spin_batch.setValue(99)
        self.spin_batch.setMinimumWidth(80)
        self.spin_batch.setToolTip(
            "After this many tickers, pause and ask to continue. "
            "Set to 0 to send all tickers without pausing."
        )
        row_bs.addWidget(self.spin_batch)
        row_bs.addStretch()
        layout.addLayout(row_bs)

        # Delay — Phase 4 R19: min 0.3s so a mis-focused scanner window can't
        # be hammered with high-rate keystrokes before the user can react.
        row1 = QHBoxLayout()
        row1.addWidget(QLabel("Delay between tickers (sec):"))
        self.spin_delay = QDoubleSpinBox()
        self.spin_delay.setRange(0.3, 10.0)
        self.spin_delay.setSingleStep(0.1)
        self.spin_delay.setValue(0.8)
        self.spin_delay.setMinimumWidth(80)
        row1.addWidget(self.spin_delay)
        row1.addStretch()
        layout.addLayout(row1)

        # Countdown
        row2 = QHBoxLayout()
        row2.addWidget(QLabel("Countdown before start (sec):"))
        self.spin_countdown = QSpinBox()
        self.spin_countdown.setRange(1, 30)
        self.spin_countdown.setValue(5)
        self.spin_countdown.setMinimumWidth(80)
        row2.addWidget(self.spin_countdown)
        row2.addStretch()
        layout.addLayout(row2)

        # Confirm key
        row3 = QHBoxLayout()
        row3.addWidget(QLabel("Key to press after each ticker:"))
        self.combo_key = QComboBox()
        self.combo_key.addItems(["enter", "tab", "space"])
        self.combo_key.setMinimumWidth(100)
        row3.addWidget(self.combo_key)
        row3.addStretch()
        layout.addLayout(row3)

        # Dry run
        self.chk_dry = QCheckBox("Dry run (log only, don't type)")
        layout.addWidget(self.chk_dry)

        layout.addWidget(QLabel(""))

        # Instructions
        instr = QLabel(
            "1. Click Start below\n"
            "2. During the countdown, click into the target input field\n"
            "3. The scanner will type each ticker and press the confirm key\n"
            "4. Press Escape or move mouse to top-left corner to abort"
        )
        instr.setStyleSheet("color: #aaa; font-style: italic;")
        layout.addWidget(instr)

        layout.addWidget(QLabel(""))

        # Buttons
        btn_row = QHBoxLayout()
        btn_start = QPushButton("  Start  ")
        btn_start.setStyleSheet(
            "QPushButton { background: #2e7d32; color: white; font-size: 13px; "
            "font-weight: bold; padding: 8px 24px; border-radius: 4px; }"
        )
        btn_start.clicked.connect(self._on_start)
        btn_row.addWidget(btn_start)

        btn_cancel = QPushButton("  Cancel  ")
        btn_cancel.setStyleSheet(
            "QPushButton { background: #555; color: white; font-size: 13px; "
            "padding: 8px 24px; border-radius: 4px; }"
        )
        btn_cancel.clicked.connect(self._on_cancel)
        btn_row.addWidget(btn_cancel)

        btn_row.addStretch()
        layout.addLayout(btn_row)

    def _on_start(self):
        cfg = BridgeConfig(
            delay_between_tickers=self.spin_delay.value(),
            countdown_seconds=self.spin_countdown.value(),
            dry_run=self.chk_dry.isChecked(),
            confirm_key=self.combo_key.currentText(),
            start_index=self.spin_start.value() - 1,  # convert to 0-based
            batch_size=self.spin_batch.value(),
        )
        self.start_requested.emit(cfg)
        self.accept()

    def _on_cancel(self):
        self.cancelled.emit()
        self.reject()


# ============================================================================
# Excel-export dialog: column selector + format choice + News toggle
# ============================================================================

class ExcelExportDialog(QDialog):
    """Modal dialog for the green Excel button. Lets the user choose:
      - format (CSV or XLSX)
      - which columns to export (everything checked by default; the
        auto-data-presence shortcut was dropped 2026-05 because it
        kept losing track of dynamic q-i columns and the user found
        the Select All / Select None pair sufficient)
      - whether to insert empty `News_<header>` columns next to date columns

    Returns three values via accessors after exec():
        selected_keys() → ordered list of dict keys (matches RESULT_COLUMNS)
        wants_news()    → bool
        format_choice() → "csv" | "xlsx"
    """

    def __init__(
        self, columns,
        periods: list[str] | None = None, parent=None,
    ):
        """`columns` is a list of (header, key, fmt) tuples (RESULT_COLUMNS).
        Every column in `columns` is pre-checked; the user uses Select
        None / Select All / individual checkboxes to refine.
        `periods` is the ordered list of period labels for the most recent
        scan (single-element list for single-timeframe scans). When the
        list has more than one entry, a period multi-select section is
        shown so the user can export multiple periods (multi-sheet XLSX)."""
        super().__init__(parent)
        self.setWindowTitle("Export to Excel / CSV")
        self.setModal(True)
        self.setMinimumWidth(440)
        self.setMinimumHeight(620)

        self._columns = list(columns)
        self._checks: dict[str, QCheckBox] = {}
        self._period_checks: dict[str, QCheckBox] = {}
        # Phase 8 §8.4: optional grouped beats-column toggles. Populated
        # below if the dynamic q* columns are present in self._columns.
        self._group_eps_checkbox: Optional[QCheckBox] = None
        self._group_eps_keys: list[str] = []
        self._group_rev_checkbox: Optional[QCheckBox] = None
        self._group_rev_keys: list[str] = []
        periods = list(periods or [])

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 18, 20, 18)

        title = QLabel("Export Results")
        title.setFont(QFont("Segoe UI", 13, QFont.Weight.Bold))
        layout.addWidget(title)

        # --- Format + News toggle ---
        opts_box = QGroupBox("Options")
        opts_layout = QGridLayout(opts_box)

        opts_layout.addWidget(QLabel("Format:"), 0, 0)
        self._combo_format = QComboBox()
        self._combo_format.addItems(["XLSX", "CSV"])
        opts_layout.addWidget(self._combo_format, 0, 1)

        self._chk_news = QCheckBox("Add News columns (empty, named News_<column>)")
        self._chk_news.setToolTip(
            "Insert an empty column named News_<header> directly to the "
            "right of every date column (Gain Start, Up Gap Start, Down "
            "Gap Start, Max Gap Date, Min Gap Date, Surge Start). Useful "
            "for pasting news/comments while reviewing the export."
        )
        opts_layout.addWidget(self._chk_news, 1, 0, 1, 2)

        # Color export — XLSX only. CSV has no concept of cell colors,
        # so the checkbox greys out and unchecks itself when CSV is
        # selected. When XLSX + checked, the writer mirrors the on-
        # screen foreground colors (palette match-color + streak green
        # + display-only fail red) into per-cell font colors via openpyxl.
        self._chk_colors = QCheckBox("Include cell colors (.xlsx only)")
        self._chk_colors.setToolTip(
            "Carry the on-screen text colors (match-color palette, "
            "streak green, display-only red) into the exported workbook. "
            "Greyed out for CSV — CSV has no cell-color concept."
        )
        self._chk_colors.setChecked(True)
        opts_layout.addWidget(self._chk_colors, 2, 0, 1, 2)
        # Sync grey-out with the format combo.
        self._combo_format.currentTextChanged.connect(self._on_format_changed)
        self._on_format_changed(self._combo_format.currentText())

        layout.addWidget(opts_box)

        # --- Period multi-select (only shown when > 1 period available) ---
        if len(periods) > 1:
            periods_box = QGroupBox("Time periods to export")
            pv = QVBoxLayout(periods_box)

            blurb = QLabel(
                "XLSX: each selected period becomes its own sheet "
                "(multi-page workbook). CSV with multiple periods: a "
                "single file with a Period column added at the front."
            )
            blurb.setWordWrap(True)
            blurb.setStyleSheet("color: #aaa; font-style: italic;")
            pv.addWidget(blurb)

            shortcut_p = QHBoxLayout()
            btn_all_p = QPushButton("Select All")
            btn_all_p.clicked.connect(self._select_all_periods)
            btn_none_p = QPushButton("Select None")
            btn_none_p.clicked.connect(self._select_none_periods)
            shortcut_p.addWidget(btn_all_p)
            shortcut_p.addWidget(btn_none_p)
            shortcut_p.addStretch()
            pv.addLayout(shortcut_p)

            for label in periods:
                cb = QCheckBox(label)
                cb.setChecked(True)
                self._period_checks[label] = cb
                pv.addWidget(cb)

            layout.addWidget(periods_box)
        elif len(periods) == 1:
            # Single period — store but don't show a UI section
            self._period_checks[periods[0]] = QCheckBox()
            self._period_checks[periods[0]].setChecked(True)

        # --- Column checkboxes ---
        cols_box = QGroupBox("Columns to export")
        cols_outer = QVBoxLayout(cols_box)

        # Select-all / select-none shortcuts. (The auto-data-only
        # button was dropped 2026-05 — the bundle checkboxes already
        # collapse the dynamic q-i columns into two toggles, so
        # Select-All gets the user 95% of the way there in one click.)
        shortcut_row = QHBoxLayout()
        btn_all = QPushButton("Select All")
        btn_all.clicked.connect(self._select_all)
        btn_none = QPushButton("Select None")
        btn_none.clicked.connect(self._select_none)
        shortcut_row.addWidget(btn_all)
        shortcut_row.addWidget(btn_none)
        shortcut_row.addStretch()
        cols_outer.addLayout(shortcut_row)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        inner = QWidget()
        inner_layout = QVBoxLayout(inner)
        inner_layout.setContentsMargins(4, 4, 4, 4)

        # Phase 8 §8.4: collapse the dynamic Q-i columns into two single
        # grouped toggles (EPS and Rev) so the user doesn't see N×5
        # individual checkboxes when streaks are deep. A regular column
        # gets its own checkbox; q* columns get bundled.
        # Bucketing covers all q-i suffixes:
        #   - reported_{eps,rev}
        #   - surprise_{eps,rev}_{dollar,pct}
        #   - yoy_{eps,rev}_pct (added 2026-05)
        #   - report_date_{eps,rev} (always was per-block via suffix)
        # Anything matching q\d+_*eps* goes EPS, q\d+_*rev* goes REV.
        # Falling through to `regular_columns` would leak q-i columns
        # as individual checkboxes — the user reported clutter + the
        # bundle toggle missing some columns.
        import re as _re
        _Q_RE = _re.compile(r"^q(\d+)_(.+)$")
        eps_q_keys: list[str] = []
        rev_q_keys: list[str] = []
        regular_columns: list[tuple] = []
        for entry in self._columns:
            header, key, _fmt = entry
            m = _Q_RE.match(key)
            if m is not None:
                suffix = m.group(2)
                # `_eps` suffix or `_rev` suffix at end OR start with
                # known eps/rev prefix. Order matters: check _rev first
                # since "_eps" could appear as substring of bigger words.
                if (suffix.endswith("_rev") or suffix.startswith("reported_rev")
                        or suffix.startswith("surprise_rev")
                        or suffix.startswith("yoy_rev")
                        or suffix == "report_date_rev"):
                    rev_q_keys.append(key)
                    continue
                if (suffix.endswith("_eps") or suffix.startswith("reported_eps")
                        or suffix.startswith("surprise_eps")
                        or suffix.startswith("yoy_eps")
                        or suffix == "report_date_eps"):
                    eps_q_keys.append(key)
                    continue
            regular_columns.append(entry)

        for header, key, _fmt in regular_columns:
            cb = QCheckBox(header)
            cb.setChecked(True)
            self._checks[key] = cb
            inner_layout.addWidget(cb)

        if eps_q_keys:
            cb = QCheckBox(
                f"Consecutive EPS Beats per-quarter columns ({len(eps_q_keys)} cols)"
            )
            cb.setToolTip(
                "Toggle the Q-1 / Q-2 / … / Q-N Reported / Surp $ / Surp % "
                "EPS columns as a single group. Streak cell colors are not "
                "preserved in XLSX exports — values only."
            )
            cb.setChecked(True)
            self._group_eps_checkbox = cb
            self._group_eps_keys = eps_q_keys
            inner_layout.addWidget(cb)

        if rev_q_keys:
            cb = QCheckBox(
                f"Consecutive Rev Beats per-quarter columns ({len(rev_q_keys)} cols)"
            )
            cb.setToolTip(
                "Toggle the Q-1 / Q-2 / … / Q-N Reported / Surp $ / Surp % "
                "Revenue columns as a single group."
            )
            cb.setChecked(True)
            self._group_rev_checkbox = cb
            self._group_rev_keys = rev_q_keys
            inner_layout.addWidget(cb)

        inner_layout.addStretch()
        scroll.setWidget(inner)
        cols_outer.addWidget(scroll)

        layout.addWidget(cols_box, 1)

        # --- OK / Cancel ---
        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def _select_all(self):
        for cb in self._checks.values():
            cb.setChecked(True)
        if self._group_eps_checkbox is not None:
            self._group_eps_checkbox.setChecked(True)
        if self._group_rev_checkbox is not None:
            self._group_rev_checkbox.setChecked(True)

    def _select_none(self):
        for cb in self._checks.values():
            cb.setChecked(False)
        if self._group_eps_checkbox is not None:
            self._group_eps_checkbox.setChecked(False)
        if self._group_rev_checkbox is not None:
            self._group_rev_checkbox.setChecked(False)

    def _select_all_periods(self):
        for cb in self._period_checks.values():
            cb.setChecked(True)

    def _select_none_periods(self):
        for cb in self._period_checks.values():
            cb.setChecked(False)

    def selected_keys(self) -> list[str]:
        """Return checked column keys in the same order as the columns
        list passed in. The grouped beats checkboxes (when present)
        expand to all the q* keys they cover."""
        included: set[str] = set()
        for _h, key, _f in self._columns:
            cb = self._checks.get(key)
            if cb is not None and cb.isChecked():
                included.add(key)
        if (self._group_eps_checkbox is not None
                and self._group_eps_checkbox.isChecked()):
            included.update(self._group_eps_keys)
        if (self._group_rev_checkbox is not None
                and self._group_rev_checkbox.isChecked()):
            included.update(self._group_rev_keys)
        return [key for _h, key, _f in self._columns if key in included]

    def selected_periods(self) -> list[str]:
        """Return checked period labels in original order."""
        return [label for label, cb in self._period_checks.items()
                if cb.isChecked()]

    def wants_news(self) -> bool:
        return self._chk_news.isChecked()

    def format_choice(self) -> str:
        return self._combo_format.currentText().lower()  # "xlsx" | "csv"

    def wants_colors(self) -> bool:
        """True iff format=XLSX AND the user kept "Include cell colors"
        checked. Always False for CSV regardless of checkbox state."""
        if self.format_choice() != "xlsx":
            return False
        return self._chk_colors.isChecked()

    def _on_format_changed(self, fmt_text: str):
        """Grey-out the colors checkbox for CSV. Doesn't reset its state
        — re-selecting XLSX restores the user's prior choice."""
        is_xlsx = fmt_text.upper() == "XLSX"
        self._chk_colors.setEnabled(is_xlsx)


# ============================================================================
# Sequenced-run config dialog
# ============================================================================

class SequencedRunDialog(QDialog):
    """Configure a Sequenced Run: a date range plus a chunk size that the
    scanner will walk backwards through. Each chunk is its own scan; the
    results table shows all hits across all periods, tagged by Period."""

    def __init__(
        self, default_start: date, default_end: date,
        default_n: int = 2, default_unit: str = "months",
        parent=None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Sequenced Run")
        self.setModal(True)
        self.setMinimumWidth(420)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 18, 20, 18)

        title = QLabel("Sequenced Run")
        title.setFont(QFont("Segoe UI", 13, QFont.Weight.Bold))
        layout.addWidget(title)

        blurb = QLabel(
            "Scan walks BACKWARDS from the most recent date in the range, "
            "in chunks of the size below. Same symbol may qualify in "
            "multiple chunks — each appears as its own row tagged with "
            "the Period it came from. Leftover (oldest) chunk runs even "
            "if smaller than the chunk size."
        )
        blurb.setWordWrap(True)
        blurb.setStyleSheet("color: #aaa; font-style: italic;")
        layout.addWidget(blurb)

        # --- Date range ---
        range_box = QGroupBox("Date range")
        rg = QGridLayout(range_box)
        rg.addWidget(QLabel("Start:"), 0, 0)
        self._date_start = QDateEdit()
        self._date_start.setCalendarPopup(True)
        self._date_start.setDisplayFormat("yyyy-MM-dd")
        self._date_start.setDate(QDate(default_start.year, default_start.month, default_start.day))
        rg.addWidget(self._date_start, 0, 1)

        rg.addWidget(QLabel("End:"), 1, 0)
        self._date_end = QDateEdit()
        self._date_end.setCalendarPopup(True)
        self._date_end.setDisplayFormat("yyyy-MM-dd")
        self._date_end.setDate(QDate(default_end.year, default_end.month, default_end.day))
        rg.addWidget(self._date_end, 1, 1)

        # Quick-select: snap End to today's date in one click.
        btn_today = QPushButton("Today")
        btn_today.setToolTip("Set End date to today")
        btn_today.clicked.connect(
            lambda: self._date_end.setDate(QDate.currentDate())
        )
        rg.addWidget(btn_today, 1, 2)
        layout.addWidget(range_box)

        # --- Chunk size ---
        chunk_box = QGroupBox("Chunk size")
        cg = QGridLayout(chunk_box)
        cg.addWidget(QLabel("Amount:"), 0, 0)
        self._spin_n = QSpinBox()
        self._spin_n.setRange(1, 999)
        self._spin_n.setValue(default_n)
        cg.addWidget(self._spin_n, 0, 1)

        cg.addWidget(QLabel("Unit:"), 0, 2)
        self._combo_unit = QComboBox()
        self._combo_unit.addItems(["days", "weeks", "months"])
        if default_unit in ("days", "weeks", "months"):
            self._combo_unit.setCurrentText(default_unit)
        cg.addWidget(self._combo_unit, 0, 3)
        layout.addWidget(chunk_box)

        # --- OK / Cancel ---
        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def get_config(self) -> tuple[date, date, int, str]:
        """Return (start_date, end_date, chunk_n, unit) after exec()."""
        s = self._date_start.date()
        e = self._date_end.date()
        return (
            date(s.year(), s.month(), s.day()),
            date(e.year(), e.month(), e.day()),
            int(self._spin_n.value()),
            self._combo_unit.currentText(),
        )


# ============================================================================
# Columns dropdown — at-a-glance reorder + visibility for the results table
# ============================================================================

class ColumnsManagerDialog(QDialog):
    """Popup dialog reached via the toolbar `Columns ▾` button.

    Shows the currently-active output columns as a checkable list:
      • top-of-list = leftmost in the results table
      • check ON = visible, check OFF = hidden (mirrors the existing
        `Delete column` right-click action on the table header)
      • drag-and-drop (multi-select supported) reorders the block;
        list-internal moves only — never crosses to other widgets

    Designed to coexist with the header drag/right-click flows: this
    dialog is just another front-end onto the same `_results_column_order`
    + `_deleted_column_keys` state on MainWindow. Real-time updates are
    emitted as the user changes things, so closing the popup just
    dismisses it — there's no Apply step.

    Empty until a scan has populated columns; the toolbar button in
    that pre-scan state is disabled. After a preset load with no fresh
    scan, the dialog still shows the preset's saved order so the user
    can preview / tweak before running.
    """

    # (ordered_keys, hidden_keys) emitted on every reorder / check toggle.
    columns_updated = pyqtSignal(list, list)
    # Reset to canonical (current scan settings, no manual layout).
    reset_requested = pyqtSignal()

    def __init__(
        self, columns: list[tuple[str, str, object]],
        hidden_keys: set[str],
        always_visible_keys: set[str],
        parent=None,
    ):
        """`columns` is the LIVE active column layout already reordered
        to the user's preferred sequence (so the list reflects what's on
        screen). `hidden_keys` is the current `_deleted_column_keys`.
        `always_visible_keys` blocks unchecking core columns whose
        absence would break sort / send / export."""
        super().__init__(parent)
        self.setWindowTitle("Columns")
        # Non-modal so the user can drag the table header alongside.
        self.setModal(False)
        self.setMinimumWidth(300)
        self.setMinimumHeight(420)

        self._always_visible = set(always_visible_keys)
        self._suppress_signals = False

        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 12, 14, 12)

        title = QLabel("Columns")
        title.setFont(QFont("Segoe UI", 12, QFont.Weight.Bold))
        layout.addWidget(title)

        blurb = QLabel(
            "Top → bottom maps to leftmost → rightmost in the results "
            "table. Drag (multi-select supported) to reorder; uncheck "
            "to hide. Saved with the active preset."
        )
        blurb.setWordWrap(True)
        blurb.setStyleSheet("color: #aaa; font-style: italic;")
        layout.addWidget(blurb)

        self._list = QListWidget(self)
        self._list.setDragDropMode(QAbstractItemView.DragDropMode.InternalMove)
        self._list.setSelectionMode(
            QAbstractItemView.SelectionMode.ExtendedSelection
        )
        self._list.setDefaultDropAction(Qt.DropAction.MoveAction)
        layout.addWidget(self._list, 1)

        self._populate(columns, hidden_keys)

        # `model().rowsMoved` is the canonical signal for an internal
        # drag move; covers multi-select drag too. itemChanged covers
        # the checkbox toggle.
        self._list.model().rowsMoved.connect(self._on_rows_moved)
        self._list.itemChanged.connect(self._on_item_changed)

        btn_row = QHBoxLayout()
        btn_reset = QPushButton("Reset to Default")
        btn_reset.setToolTip(
            "Restore the column order + visibility to the canonical "
            "layout produced by the current scan settings. Equivalent "
            "to the table header right-click → Reset Column Order."
        )
        btn_reset.clicked.connect(self._on_reset_clicked)
        btn_row.addWidget(btn_reset)

        btn_close = QPushButton("Close")
        btn_close.clicked.connect(self.close)
        btn_row.addStretch()
        btn_row.addWidget(btn_close)
        layout.addLayout(btn_row)

    # ── public state-sync helpers (called by MainWindow when results
    #    repopulate / preset loads / right-click reset fires) ────────

    def update_columns(
        self, columns: list[tuple[str, str, object]],
        hidden_keys: set[str],
    ) -> None:
        """Repopulate the list to match new active columns. Called by
        MainWindow when a scan completes, a preset is loaded, or
        anything else changes the underlying column layout."""
        self._suppress_signals = True
        try:
            self._list.clear()
            self._populate(columns, hidden_keys)
        finally:
            self._suppress_signals = False

    def _populate(
        self, columns: list[tuple[str, str, object]],
        hidden_keys: set[str],
    ) -> None:
        for header, key, _fmt in columns:
            item = QListWidgetItem(header)
            item.setData(Qt.ItemDataRole.UserRole, key)
            flags = (Qt.ItemFlag.ItemIsSelectable
                     | Qt.ItemFlag.ItemIsEnabled
                     | Qt.ItemFlag.ItemIsDragEnabled
                     | Qt.ItemFlag.ItemIsUserCheckable)
            item.setFlags(flags)
            checked = key not in hidden_keys
            item.setCheckState(
                Qt.CheckState.Checked if checked else Qt.CheckState.Unchecked
            )
            # Always-visible core columns get their checkbox display-
            # locked: the user can drag them around but can't hide
            # them. Foreground tint hints at the special status.
            if key in self._always_visible:
                f = item.flags()
                f &= ~Qt.ItemFlag.ItemIsUserCheckable
                item.setFlags(f)
                item.setForeground(Qt.GlobalColor.lightGray)
                item.setToolTip(
                    "Core column — always visible (cannot be hidden)"
                )
            self._list.addItem(item)

    # ── change handlers ──────────────────────────────────────────────

    def _on_rows_moved(self, *_args) -> None:
        if self._suppress_signals:
            return
        self._emit_state()

    def _on_item_changed(self, item: QListWidgetItem) -> None:
        if self._suppress_signals:
            return
        # Block unchecking of always-visible columns. The flag-strip
        # above usually prevents the toggle reaching here, but Qt has
        # been known to reset the check state when items reflow during
        # a drag, so guard explicitly.
        key = item.data(Qt.ItemDataRole.UserRole)
        if (key in self._always_visible
                and item.checkState() != Qt.CheckState.Checked):
            self._suppress_signals = True
            try:
                item.setCheckState(Qt.CheckState.Checked)
            finally:
                self._suppress_signals = False
            return
        self._emit_state()

    def _on_reset_clicked(self) -> None:
        self.reset_requested.emit()

    def _emit_state(self) -> None:
        ordered_keys: list[str] = []
        hidden_keys: list[str] = []
        for r in range(self._list.count()):
            item = self._list.item(r)
            key = item.data(Qt.ItemDataRole.UserRole)
            ordered_keys.append(key)
            if item.checkState() != Qt.CheckState.Checked:
                hidden_keys.append(key)
        self.columns_updated.emit(ordered_keys, hidden_keys)

    # ── accessors used by tests / MainWindow snapshot helpers ────────

    def ordered_keys(self) -> list[str]:
        return [
            self._list.item(r).data(Qt.ItemDataRole.UserRole)
            for r in range(self._list.count())
        ]

    def hidden_keys(self) -> list[str]:
        return [
            self._list.item(r).data(Qt.ItemDataRole.UserRole)
            for r in range(self._list.count())
            if self._list.item(r).checkState() != Qt.CheckState.Checked
        ]
