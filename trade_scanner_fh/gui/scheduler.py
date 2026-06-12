"""
F3 — in-app scan scheduler with quick-export + toast alerts.

``ScanScheduler`` is a QObject owned by MainWindow. Entries
({label, preset_name, time "HH:MM" local, days as weekday list,
enabled}) persist atomically to ``scanner_data/schedules.json``. A
~30s QTimer fires due entries — each at most once per day (the last
fired date is recorded per entry) — by loading the named preset via
the EXISTING preset-load path and triggering the EXISTING scan path.
On completion of a scheduled scan the normal scan-done pipeline has
already extended the F2 scan_history.json rolling summary (scheduled
scans reuse ``_apply_watchlist_diff`` wholesale); the scheduler then
(ii) auto Quick-Exports the results via the F3(a) pipeline and
(iii) shows a Windows toast via ``QSystemTrayIcon.showMessage`` —
no new third-party dependency. Firing is skipped (and logged) when a
scan is already running or the preset no longer exists.

Design notes (load-bearing for the test suite — same conventions as
``exports.py`` / ``earnings_coordinator.py``):

- The scheduler holds a plain back-reference to the window
  (``self.win``) and is NOT parented to it as a QObject: tests build
  bare ``MainWindow.__new__(MainWindow)`` shells (or duck-typed
  stubs) whose C++ side is uninitialized, and passing one as a
  QObject parent would raise.
- Every cross-method orchestration call routes through the window's
  delegates (``self.win._load_preset()``, ``self.win._run_scan()``,
  ``self.win._quick_export()``) so tests can override them as
  instance attributes / MagicMocks.
- Due-time logic is pure (``entry_is_due``) and takes an explicit
  ``now`` so tests never depend on wall-clock; only the production
  QTimer slot reaches for ``datetime.now()``.
- Persistence mirrors ``scan_history.py``: lazy ``config.DATA_DIR``
  resolution (tests monkeypatch it), atomic writes via
  ``config.atomic_write_text``, and corrupt-file tolerance (any
  unreadable / malformed file degrades to "no schedules" rather than
  raising).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Optional

from PyQt6.QtCore import QObject, Qt, QTime, QTimer
from PyQt6.QtWidgets import (
    QCheckBox, QComboBox, QDialog, QDialogButtonBox, QHBoxLayout,
    QLabel, QLineEdit, QMessageBox, QPushButton, QSystemTrayIcon,
    QTableWidget, QTableWidgetItem, QTimeEdit, QVBoxLayout,
)

from .. import config

# Same logger channel as main_window so scheduler lines keep the
# historical "scanner.gui" tag in the panel / subsystem files.
log = logging.getLogger("scanner.gui")

SCHEDULES_VERSION = 1

# Production timer cadence (~30s). Tests drive `check_due` directly.
CHECK_INTERVAL_MS = 30_000

# Toast display duration.
TOAST_MSECS = 10_000

# Weekday labels indexed by Python's date.weekday() (0=Mon .. 6=Sun).
DAY_LABELS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


def schedules_path() -> Path:
    """Resolved lazily so tests that monkeypatch ``config.DATA_DIR``
    are honored (mirrors scan_history.history_path)."""
    return config.DATA_DIR / "schedules.json"


def presets_dir() -> Path:
    """Lazy preset-dir resolution (same directory main_window's
    PRESETS_DIR points at in production, but monkeypatch-friendly)."""
    return config.DATA_DIR / "presets"


def list_preset_names() -> list[str]:
    """Sorted stems of the saved presets on disk."""
    try:
        return sorted(p.stem for p in presets_dir().glob("*.json"))
    except OSError:
        return []


@dataclass
class ScheduleEntry:
    """One scheduled scan. ``days`` uses Python weekday numbering
    (0=Monday .. 6=Sunday). ``time`` is local "HH:MM". An entry is due
    once its time has passed on a configured weekday and it hasn't
    already fired that day (``last_fired_date`` = ISO date of the last
    fire, "" = never)."""
    label: str = ""
    preset_name: str = ""
    time: str = "09:00"
    days: list[int] = field(default_factory=list)
    enabled: bool = True
    last_fired_date: str = ""

    def to_dict(self) -> dict:
        return {
            "label": self.label,
            "preset_name": self.preset_name,
            "time": self.time,
            "days": list(self.days),
            "enabled": bool(self.enabled),
            "last_fired_date": self.last_fired_date,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ScheduleEntry":
        """Tolerant loader — missing keys default, day values are
        coerced to ints and clamped to the 0–6 weekday range."""
        days_raw = d.get("days", [])
        days: list[int] = []
        if isinstance(days_raw, list):
            for v in days_raw:
                try:
                    iv = int(v)
                except (TypeError, ValueError):
                    continue
                if 0 <= iv <= 6 and iv not in days:
                    days.append(iv)
        return cls(
            label=str(d.get("label", "") or ""),
            preset_name=str(d.get("preset_name", "") or ""),
            time=str(d.get("time", "09:00") or "09:00"),
            days=days,
            enabled=bool(d.get("enabled", True)),
            last_fired_date=str(d.get("last_fired_date", "") or ""),
        )

    def days_text(self) -> str:
        return ", ".join(DAY_LABELS[d] for d in sorted(set(self.days))
                         if 0 <= d <= 6)


def _parse_hhmm(s: str) -> Optional[dtime]:
    """'HH:MM' → datetime.time, or None when malformed."""
    try:
        parts = str(s).strip().split(":")
        if len(parts) != 2:
            return None
        h, m = int(parts[0]), int(parts[1])
        if not (0 <= h <= 23 and 0 <= m <= 59):
            return None
        return dtime(hour=h, minute=m)
    except (TypeError, ValueError):
        return None


def entry_is_due(entry: ScheduleEntry, now: datetime) -> bool:
    """Pure due check: enabled, today's weekday is configured, the
    scheduled time has passed (>=, so a launch after the configured
    time still fires that day's entry), and it hasn't fired today.
    Malformed times / empty day lists are never due."""
    if not entry.enabled:
        return False
    if now.weekday() not in entry.days:
        return False
    t = _parse_hhmm(entry.time)
    if t is None:
        return False
    if now.time() < t:
        return False
    return entry.last_fired_date != now.date().isoformat()


def load_schedules(path: Optional[Path] = None) -> list[ScheduleEntry]:
    """Load the schedule store. Missing, unreadable, or malformed files
    all yield an empty list — the scheduler must never block launch."""
    p = path if path is not None else schedules_path()
    try:
        raw = p.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return []
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        log.warning("schedules.json is corrupt — treating as no schedules")
        return []
    if not isinstance(data, dict) or not isinstance(data.get("entries"), list):
        log.warning("schedules.json has wrong shape — treating as no schedules")
        return []
    out: list[ScheduleEntry] = []
    for item in data["entries"]:
        if isinstance(item, dict):
            out.append(ScheduleEntry.from_dict(item))
    return out


def save_schedules(entries: list[ScheduleEntry],
                   path: Optional[Path] = None) -> None:
    """Persist atomically via ``config.atomic_write_text`` so a crash
    mid-write can never corrupt the store."""
    p = path if path is not None else schedules_path()
    content = json.dumps(
        {"version": SCHEDULES_VERSION,
         "entries": [e.to_dict() for e in entries]},
        indent=1,
    )
    config.atomic_write_text(p, content)


class ScanScheduler(QObject):
    """Owns the schedule entries, the ~30s due-check timer, and the
    completion side effects (quick-export + toast) for scheduled scans.

    MainWindow notifies it from the scan-done path via
    ``on_scan_completed`` / ``on_scan_failed``; manual scans are a
    no-op there (no ``_pending`` entry)."""

    def __init__(self, window):
        # No QObject parent: `window` may be a bare __new__ test shell
        # whose C++ side was never initialized (see module docstring).
        super().__init__()
        self.win = window
        self.entries: list[ScheduleEntry] = load_schedules()
        self._timer: Optional[QTimer] = None
        self._tray: Optional[QSystemTrayIcon] = None
        # Entry whose scan is currently in flight (None = no scheduled
        # scan running; manual scans never set it).
        self._pending: Optional[ScheduleEntry] = None
        # (label, iso-date) pairs already logged as "deferred: scan
        # running" so the 30s tick doesn't spam the log panel all day.
        self._busy_logged: set[tuple[str, str]] = set()

    # ── lifecycle ────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the production due-check timer. Idempotent."""
        if self._timer is not None:
            return
        t = QTimer(self)
        t.setInterval(CHECK_INTERVAL_MS)
        t.timeout.connect(self._on_timer)
        t.start()
        self._timer = t

    def stop(self) -> None:
        """Stop the timer + hide the tray icon (app close)."""
        if self._timer is not None:
            try:
                self._timer.stop()
            except RuntimeError:
                pass
            self._timer = None
        if self._tray is not None:
            try:
                self._tray.hide()
            except RuntimeError:
                pass

    def persist(self) -> None:
        save_schedules(self.entries)

    # ── due-check / firing ───────────────────────────────────────────

    def _on_timer(self) -> None:
        # Production-only wall-clock entry point; tests call check_due
        # with an injected `now`.
        try:
            self.check_due(datetime.now())
        except Exception as exc:
            log.error("scheduler tick failed: %s", exc, exc_info=True)

    def _scan_busy(self) -> bool:
        return (getattr(self.win, "_worker", None) is not None
                or self._pending is not None)

    def _preset_exists(self, name: str) -> bool:
        if not name:
            return False
        return (presets_dir() / f"{name}.json").exists()

    def check_due(self, now: Optional[datetime] = None) -> None:
        """Fire every due entry (at most one scan launch per tick — a
        second due entry stays due and fires on the next tick once the
        first scan finishes). Missing presets are skipped + logged and
        consumed for the day; a busy scanner defers WITHOUT consuming
        so the entry retries on a later tick."""
        now = now if now is not None else datetime.now()
        today = now.date().isoformat()
        fired = False
        changed = False
        for entry in self.entries:
            if not entry_is_due(entry, now):
                continue
            if not self._preset_exists(entry.preset_name):
                self._log(
                    f"Scheduled scan '{entry.label}' skipped: preset "
                    f"'{entry.preset_name}' no longer exists."
                )
                entry.last_fired_date = today  # consume — once per day
                changed = True
                continue
            if fired or self._scan_busy():
                key = (entry.label, today)
                if key not in self._busy_logged:
                    self._busy_logged.add(key)
                    self._log(
                        f"Scheduled scan '{entry.label}' deferred: a scan "
                        f"is already running — will retry."
                    )
                continue
            self._fire(entry, now)
            entry.last_fired_date = today
            changed = True
            fired = True
        if changed:
            try:
                self.persist()
            except OSError as exc:
                log.error("could not persist schedules.json: %s", exc)

    def _fire(self, entry: ScheduleEntry, now: datetime) -> None:
        """Load the named preset via the existing preset-load path and
        trigger the existing scan path.

        Unattended-machine guarantees: ``win._scheduled_fire_active``
        is held True around the load + run so the window routes its
        "scan cannot start" notices to the log panel instead of modal
        QMessageBoxes (``MainWindow._notify_scan_blocked``); and the
        fire ABORTS — logged, entry stays consumed for the day by
        check_due — when the combo select didn't land on the scheduled
        preset or ``_load_preset`` reported failure, so the scan never
        runs (and is never diffed/exported) under the wrong settings."""
        win = self.win
        self._log(
            f"Scheduled scan '{entry.label}': loading preset "
            f"'{entry.preset_name}' and starting scan."
        )
        try:
            combo = win.preset_combo
            idx = combo.findText(entry.preset_name)
            if idx is None or idx < 0:
                # Saved after the combo was last refreshed — sync it.
                win._refresh_preset_list()
                idx = combo.findText(entry.preset_name)
            if idx is not None and idx >= 0:
                combo.setCurrentIndex(idx)
        except Exception as exc:
            log.debug("scheduler preset-combo select failed: %s", exc)
        # Verify the combo actually shows the scheduled preset before
        # loading. The isinstance gate keeps duck-typed test windows
        # (MagicMock combos) on the legacy path; a real QComboBox
        # always returns str.
        try:
            current = win.preset_combo.currentText()
        except Exception:
            current = None
        if isinstance(current, str) and current.strip() != entry.preset_name:
            self._log(
                f"Scheduled scan '{entry.label}' aborted: could not "
                f"select preset '{entry.preset_name}' "
                f"(combo shows '{current.strip()}')."
            )
            return
        try:
            win._scheduled_fire_active = True
        except Exception as exc:
            log.debug("could not set _scheduled_fire_active: %s", exc)
        try:
            loaded = win._load_preset()
            if loaded is not None and not loaded:
                # _load_preset reports failure explicitly (missing /
                # corrupt preset). None = legacy/stub override without
                # a return value — treated as success for back-compat.
                self._log(
                    f"Scheduled scan '{entry.label}' aborted: preset "
                    f"'{entry.preset_name}' failed to load."
                )
                return
            self._pending = entry
            win._run_scan()
        finally:
            try:
                win._scheduled_fire_active = False
            except Exception as exc:
                log.debug("could not clear _scheduled_fire_active: %s", exc)
        if getattr(win, "_worker", None) is None:
            # _run_scan declined (no cached data / all tickers filtered
            # / empty chunking...). Clear pending so future fires aren't
            # blocked; the entry stays consumed for today.
            self._pending = None
            self._log(
                f"Scheduled scan '{entry.label}': scan did not start "
                f"(see messages above)."
            )

    # ── completion side effects ──────────────────────────────────────

    def on_scan_completed(self) -> None:
        """Called by MainWindow at the end of the scan-done path. For a
        scheduled scan: the F2 scan_history.json summary has already
        been extended by ``_apply_watchlist_diff`` (scheduled scans
        reuse the normal scan path), so this only (ii) quick-exports
        the results and (iii) shows the toast. Manual scans (no pending
        entry) are a no-op."""
        self._busy_logged.clear()
        entry = self._pending
        if entry is None:
            return
        self._pending = None
        win = self.win

        # Result counts from the primary (first) period — same frame
        # the F2 diff stamped, so "new" mirrors the Chg column.
        n_results = 0
        n_new = 0
        try:
            order = list(getattr(win, "_period_order", []) or [])
            primary = order[0] if order else None
            df = win._period_results.get(primary) if primary else None
            if df is not None and not df.empty:
                n_results = len(df)
                if "chg" in df.columns:
                    n_new = int((df["chg"] == "NEW").sum())
        except Exception as exc:
            log.debug("scheduler result count failed: %s", exc)

        # (ii) auto quick-export via the F3(a) pipeline.
        path = None
        try:
            path = win._quick_export()
        except Exception as exc:
            log.error("scheduled quick-export failed: %s", exc)

        # (iii) Windows toast.
        msg = (f"Scheduled scan '{entry.label}': "
               f"{n_results} results, {n_new} new")
        self.show_toast("Trading Scanner", msg)
        self._log(msg + (f" — exported to {path}" if path else ""))

    def on_scan_failed(self) -> None:
        """Called from the scan-crash path so a failed scheduled scan
        can't leave ``_pending`` dangling (which would block every
        future fire via the busy check)."""
        self._busy_logged.clear()
        entry = self._pending
        if entry is None:
            return
        self._pending = None
        self._log(
            f"Scheduled scan '{entry.label}' failed — see log for details."
        )

    # ── toast ────────────────────────────────────────────────────────

    def _ensure_tray(self) -> QSystemTrayIcon:
        """Lazily create the tray icon from the app icon. Kept hidden
        until the first toast needs it."""
        if self._tray is None:
            from PyQt6.QtGui import QIcon
            from PyQt6.QtWidgets import QApplication, QStyle
            icon = QIcon()
            try:
                icon = self.win.windowIcon()
            except (AttributeError, RuntimeError):
                pass
            app = QApplication.instance()
            if icon.isNull() and app is not None:
                icon = app.windowIcon()
            if icon.isNull() and app is not None:
                # Last resort: a stock icon so the tray entry is valid.
                icon = app.style().standardIcon(
                    QStyle.StandardPixmap.SP_ComputerIcon
                )
            self._tray = QSystemTrayIcon(icon, self)
        return self._tray

    def show_toast(self, title: str, message: str) -> None:
        """Windows toast via QSystemTrayIcon.showMessage (no extra
        dependency). Failures are logged, never raised — a toast must
        not break scan completion."""
        try:
            tray = self._ensure_tray()
            if not tray.isVisible():
                tray.show()
            tray.showMessage(
                title, message,
                QSystemTrayIcon.MessageIcon.Information, TOAST_MSECS,
            )
        except Exception as exc:
            log.error("toast failed: %s", exc)

    # ── logging ──────────────────────────────────────────────────────

    def _log(self, line: str) -> None:
        try:
            self.win.log_panel.write_line(line)
        except (AttributeError, RuntimeError):
            log.info("%s", line)


# ============================================================================
# Management UI — Scans → Schedule…
# ============================================================================

class ScheduleEntryDialog(QDialog):
    """Add/Edit form for one schedule entry."""

    def __init__(self, presets: list[str],
                 entry: Optional[ScheduleEntry] = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Edit Scheduled Scan" if entry
                            else "Add Scheduled Scan")
        self.setModal(True)
        self.setMinimumWidth(440)

        layout = QVBoxLayout(self)

        row_label = QHBoxLayout()
        row_label.addWidget(QLabel("Label:"))
        self.edit_label = QLineEdit()
        self.edit_label.setPlaceholderText("e.g. Morning momo sweep")
        row_label.addWidget(self.edit_label)
        layout.addLayout(row_label)

        row_preset = QHBoxLayout()
        row_preset.addWidget(QLabel("Preset:"))
        self.combo_preset = QComboBox()
        self.combo_preset.addItems(presets)
        self.combo_preset.setMinimumWidth(220)
        self.combo_preset.setToolTip(
            "The saved preset to load before the scheduled scan. The "
            "entry is skipped (with a log line) if the preset is "
            "deleted later."
        )
        row_preset.addWidget(self.combo_preset)
        row_preset.addStretch()
        layout.addLayout(row_preset)

        row_time = QHBoxLayout()
        row_time.addWidget(QLabel("Time (local):"))
        self.time_edit = QTimeEdit()
        self.time_edit.setDisplayFormat("HH:mm")
        self.time_edit.setTime(QTime(9, 0))
        self.time_edit.setToolTip(
            "Fires within ~30 seconds after this time on the checked "
            "days (at most once per day). If the app launches after "
            "this time, the entry fires shortly after launch."
        )
        row_time.addWidget(self.time_edit)
        row_time.addStretch()
        layout.addLayout(row_time)

        row_days = QHBoxLayout()
        row_days.addWidget(QLabel("Days:"))
        self.day_checks: list[QCheckBox] = []
        for i, name in enumerate(DAY_LABELS):
            chk = QCheckBox(name)
            chk.setChecked(i < 5)  # default Mon–Fri
            self.day_checks.append(chk)
            row_days.addWidget(chk)
        row_days.addStretch()
        layout.addLayout(row_days)

        self.chk_enabled = QCheckBox("Enabled")
        self.chk_enabled.setChecked(True)
        layout.addWidget(self.chk_enabled)

        if entry is not None:
            self.edit_label.setText(entry.label)
            idx = self.combo_preset.findText(entry.preset_name)
            if idx >= 0:
                self.combo_preset.setCurrentIndex(idx)
            t = _parse_hhmm(entry.time)
            if t is not None:
                self.time_edit.setTime(QTime(t.hour, t.minute))
            for i, chk in enumerate(self.day_checks):
                chk.setChecked(i in entry.days)
            self.chk_enabled.setChecked(entry.enabled)
        self._orig = entry

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self._on_accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _on_accept(self):
        if not self.edit_label.text().strip():
            QMessageBox.warning(self, "Scheduled Scan",
                                "Please enter a label.")
            return
        if not self.combo_preset.currentText().strip():
            QMessageBox.warning(
                self, "Scheduled Scan",
                "No preset selected — save a preset first (toolbar → "
                "Save), then schedule it.",
            )
            return
        if not any(chk.isChecked() for chk in self.day_checks):
            QMessageBox.warning(self, "Scheduled Scan",
                                "Please check at least one day.")
            return
        self.accept()

    def result_entry(self) -> ScheduleEntry:
        t = self.time_edit.time()
        return ScheduleEntry(
            label=self.edit_label.text().strip(),
            preset_name=self.combo_preset.currentText().strip(),
            time=f"{t.hour():02d}:{t.minute():02d}",
            days=[i for i, chk in enumerate(self.day_checks)
                  if chk.isChecked()],
            enabled=self.chk_enabled.isChecked(),
            # Preserve the once-per-day marker when editing so an edit
            # doesn't re-fire an entry that already ran today.
            last_fired_date=(self._orig.last_fired_date
                             if self._orig is not None else ""),
        )


class ScheduleDialog(QDialog):
    """Scans → Schedule… — table of entries with Add/Edit/Remove and an
    in-place Enabled checkbox. Every mutation applies to the live
    scheduler immediately and persists atomically to
    scanner_data/schedules.json."""

    _COLS = ("Enabled", "Label", "Preset", "Time", "Days")

    def __init__(self, scheduler: ScanScheduler, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Scheduled Scans")
        self.setModal(True)
        self.setMinimumSize(640, 360)
        self.scheduler = scheduler

        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(
            "Each entry loads its preset and runs a scan at the set "
            "time on the checked days (at most once per day), then "
            "Quick-Exports the results and shows a toast."
        ))

        self.table = QTableWidget(0, len(self._COLS))
        self.table.setHorizontalHeaderLabels(list(self._COLS))
        self.table.setSelectionBehavior(
            QTableWidget.SelectionBehavior.SelectRows
        )
        self.table.setSelectionMode(
            QTableWidget.SelectionMode.SingleSelection
        )
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)

        btn_row = QHBoxLayout()
        btn_add = QPushButton("Add…")
        btn_add.clicked.connect(self._on_add)
        btn_row.addWidget(btn_add)
        btn_edit = QPushButton("Edit…")
        btn_edit.clicked.connect(self._on_edit)
        btn_row.addWidget(btn_edit)
        btn_remove = QPushButton("Remove")
        btn_remove.clicked.connect(self._on_remove)
        btn_row.addWidget(btn_remove)
        btn_row.addStretch()
        btn_close = QPushButton("Close")
        btn_close.clicked.connect(self.accept)
        btn_row.addWidget(btn_close)
        layout.addLayout(btn_row)

        self._rebuild()
        self.table.itemChanged.connect(self._on_item_changed)

    def _rebuild(self):
        self.table.blockSignals(True)
        try:
            self.table.setRowCount(len(self.scheduler.entries))
            for r, entry in enumerate(self.scheduler.entries):
                chk_item = QTableWidgetItem()
                chk_item.setFlags(
                    Qt.ItemFlag.ItemIsUserCheckable
                    | Qt.ItemFlag.ItemIsEnabled
                    | Qt.ItemFlag.ItemIsSelectable
                )
                chk_item.setCheckState(
                    Qt.CheckState.Checked if entry.enabled
                    else Qt.CheckState.Unchecked
                )
                self.table.setItem(r, 0, chk_item)
                for c, text in enumerate(
                    (entry.label, entry.preset_name, entry.time,
                     entry.days_text()), start=1,
                ):
                    item = QTableWidgetItem(text)
                    item.setFlags(
                        Qt.ItemFlag.ItemIsEnabled
                        | Qt.ItemFlag.ItemIsSelectable
                    )
                    self.table.setItem(r, c, item)
        finally:
            self.table.blockSignals(False)

    def _on_item_changed(self, item: QTableWidgetItem):
        if item.column() != 0:
            return
        row = item.row()
        if not (0 <= row < len(self.scheduler.entries)):
            return
        self.scheduler.entries[row].enabled = (
            item.checkState() == Qt.CheckState.Checked
        )
        self.scheduler.persist()

    def _selected_row(self) -> int:
        rows = self.table.selectionModel().selectedRows()
        return rows[0].row() if rows else -1

    def _on_add(self):
        presets = list_preset_names()
        if not presets:
            QMessageBox.information(
                self, "Scheduled Scans",
                "No saved presets yet — save a preset first (toolbar → "
                "Save), then schedule it.",
            )
            return
        dlg = ScheduleEntryDialog(presets, parent=self)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        self.scheduler.entries.append(dlg.result_entry())
        self.scheduler.persist()
        self._rebuild()

    def _on_edit(self):
        row = self._selected_row()
        if row < 0:
            return
        entry = self.scheduler.entries[row]
        presets = list_preset_names()
        # Keep a deleted preset visible in the editor so the user can
        # see what the entry pointed at (the scheduler skips it anyway).
        if entry.preset_name and entry.preset_name not in presets:
            presets = [entry.preset_name] + presets
        dlg = ScheduleEntryDialog(presets, entry=entry, parent=self)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        self.scheduler.entries[row] = dlg.result_entry()
        self.scheduler.persist()
        self._rebuild()

    def _on_remove(self):
        row = self._selected_row()
        if row < 0:
            return
        del self.scheduler.entries[row]
        self.scheduler.persist()
        self._rebuild()
