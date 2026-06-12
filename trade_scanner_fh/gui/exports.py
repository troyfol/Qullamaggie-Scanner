"""
Excel / CSV export logic — extracted from MainWindow (Step A4).

Owns the Excel-export dialog handoff (ExcelExportDialog in dialogs.py),
multi-sheet XLSX generation, single/multi-period CSV concatenation, the
on-screen-color → workbook-font-color mapping, and the legacy CSV/TXT
quick export.

Design notes (load-bearing for the test suite — do not "simplify"):

- The controller holds a plain back-reference to the window
  (``self.win``) and is NOT parented to it as a QObject: tests build
  bare ``MainWindow.__new__(MainWindow)`` shells whose C++ side is
  uninitialized, and passing one as a QObject parent would raise.
- Every cross-method orchestration call routes through the window's
  delegate (``self.win._build_export_df(...)``, not a direct
  ``self._build_export_df(...)``) — preserving exactly the
  pre-extraction dynamic-dispatch semantics for tests that override
  window methods as instance attributes or call the unbound
  ``MainWindow._build_export_df(shell, ...)`` form.
- Window-owned state (``_period_results``, ``_period_order``,
  ``_active_period``, ``_results_column_order``, ``results_table``,
  status bar, log panel) is read via ``self.win`` so tests that seed
  them on bare window shells keep working.
"""

from __future__ import annotations

import csv
import logging
import re
from datetime import datetime
from typing import Optional

import pandas as pd

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QDialog, QFileDialog, QMessageBox

from .. import config
from .dialogs import ExcelExportDialog
from .widgets import _fmt_date, RESULT_COLUMNS

# Same logger channel as main_window so the extracted log lines keep
# their historical "scanner.gui" tag in the panel / subsystem files.
log = logging.getLogger("scanner.gui")


class ExportsController:
    """Excel / CSV export pipeline for MainWindow.

    MainWindow keeps every historical method name as a thin delegate onto
    this object, so button wiring and tests are untouched.
    """

    def __init__(self, window):
        self.win = window

    def _ordered_active_columns_for_export(self) -> list[tuple]:
        """Return the active columns reordered per the user's saved
        layout. Columns whose key is in `_results_column_order` come
        first in that order; any remaining columns keep their
        canonical position at the end."""
        active = list(self.win.results_table.active_columns)
        if not self.win._results_column_order:
            return active
        by_key = {c[1]: c for c in active}
        ordered: list = []
        seen: set[str] = set()
        for key in self.win._results_column_order:
            entry = by_key.get(key)
            if entry is not None:
                ordered.append(entry)
                seen.add(key)
        for entry in active:
            if entry[1] not in seen:
                ordered.append(entry)
        return ordered

    def _build_export_df(
        self, df: "pd.DataFrame", keys: list[str], wants_news: bool,
        prepend_period: Optional[str] = None,
    ) -> "pd.DataFrame":
        """Apply column selection + News injection to a single period df.
        When prepend_period is provided, prepend a `Period` column of that
        constant value (used for multi-period CSV concatenation).

        Iterates `keys` IN THE ORDER PROVIDED so the user's manual
        column drag (and any preset-saved layout) flows through to the
        export. The dialog hands back keys in the visual order they were
        passed in (`_ordered_active_columns_for_export()` already encodes
        the drag order); we just need to honor that ordering instead of
        falling back to canonical iteration. The lookup table below
        carries header text + format info so per-quarter dynamic
        columns still round-trip correctly.
        """
        # Build a key → (header, fmt_func) lookup from the live active
        # columns so dynamic q-i blocks resolve. Falls back to canonical
        # RESULT_COLUMNS when no scan has populated the table yet (so
        # CSV export from a fresh shell still works).
        try:
            active = list(self.win.results_table.active_columns)
        except (AttributeError, RuntimeError):
            active = []
        layout = active if active else list(RESULT_COLUMNS)
        by_key: dict[str, tuple[str, object]] = {
            k: (h, f) for h, k, f in layout
        }

        out_cols: list[tuple[str, Optional[str]]] = []
        if prepend_period is not None:
            out_cols.append(("Period", "_period_synthetic"))
        for key in keys:
            entry = by_key.get(key)
            if entry is None:
                continue
            header, fmt_func = entry
            out_cols.append((header, key))
            if wants_news and fmt_func is _fmt_date:
                out_cols.append((f"News_{header}", None))

        out: dict[str, object] = {}
        n = len(df) if df is not None else 0
        for display_header, source_key in out_cols:
            if source_key == "_period_synthetic":
                out[display_header] = [prepend_period] * n
            elif source_key is None:
                out[display_header] = [""] * n
            elif df is not None and source_key in df.columns:
                out[display_header] = df[source_key].values
            else:
                out[display_header] = [""] * n
        return pd.DataFrame(out)

    @staticmethod
    def _sanitize_sheet_name(label: str, used: set[str]) -> str:
        """Excel sheet names: max 31 chars, no \\ / ? * [ ]. Also enforce
        uniqueness within `used` by appending _N when a collision occurs."""
        safe = label
        for ch in r'\/?*[]:':
            safe = safe.replace(ch, "_")
        safe = safe[:31] or "Sheet"
        base = safe
        i = 2
        while safe in used:
            tail = f"_{i}"
            safe = (base[:31 - len(tail)] + tail)
            i += 1
        used.add(safe)
        return safe

    @staticmethod
    def _safe_filename_component(name: str) -> str:
        """Strip Windows-illegal / control characters from a preset
        name so it can be embedded in the quick-export filename.
        Falls back to 'adhoc' when nothing usable remains."""
        safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", (name or "").strip())
        safe = safe.strip(" .")
        return safe or "adhoc"

    def quick_export(self, now: Optional[datetime] = None) -> Optional[str]:
        """F3 (a): one-click XLSX snapshot of the current results — NO
        dialog. Writes ALL periods of the cached scan with the current
        visible columns (the table's active layout in the user's drag
        order, view filters applied — same pipeline as the Excel
        dialog) to::

            scanner_data/exports/scan_<presetOrAdhoc>_<YYYYMMDD-HHMMSS>.xlsx

        with defaults (no News columns, no colors). The exports dir is
        created lazily. Returns the written path (str), or None when
        there was nothing to export or the write failed. ``now`` is
        injectable for tests; production callers use the wall clock.
        """
        win = self.win
        if not getattr(win, "_period_results", None):
            win.log_panel.write_line(
                "Quick Export: no results to export — run a scan first."
            )
            return None
        when = now if now is not None else datetime.now()

        try:
            preset = win._scan_history_key()
        except (AttributeError, RuntimeError):
            preset = "adhoc"
        safe = self._safe_filename_component(preset)

        # Current visible columns: the table's active layout already
        # excludes user-hidden columns; the export helper reorders per
        # the user's saved drag order. Falls back to the canonical set
        # for shells without a populated table.
        try:
            keys = [
                k for _h, k, _f in win._ordered_active_columns_for_export()
            ]
        except (AttributeError, RuntimeError):
            keys = [k for _h, k, _f in RESULT_COLUMNS]

        periods = list(getattr(win, "_period_order", []) or [])
        if not periods:
            periods = list(win._period_results.keys())

        # The exports-dir mkdir lives INSIDE the OSError guard: this
        # method runs in a Qt-slot path (Scans → Quick Export), and an
        # OSError escaping a slot aborts the whole exe under PyInstaller
        # windowed mode (see the _on_scan_done docstring). A full disk /
        # readonly DATA_DIR / an 'exports' FILE squatting on the dir
        # name must degrade to a logged failure, not a crash.
        try:
            exports_dir = config.DATA_DIR / "exports"
            exports_dir.mkdir(parents=True, exist_ok=True)
            path = exports_dir / (
                f"scan_{safe}_{when.strftime('%Y%m%d-%H%M%S')}.xlsx"
            )
            win._write_xlsx_multi_sheet(
                str(path), periods, keys, False, apply_colors=False,
            )
        except OSError as exc:
            win.log_panel.write_line(f"Quick Export failed: {exc}")
            return None
        except ImportError as exc:
            win.log_panel.write_line(
                f"Quick Export failed — XLSX export requires the "
                f"openpyxl package: {exc}"
            )
            return None

        win.log_panel.write_line(
            f"Quick Export: XLSX → {path} "
            f"({len(periods)} period(s), {len(keys)} columns)"
        )
        try:
            win.status.showMessage(f"Quick Export → {path}")
        except (AttributeError, RuntimeError):
            pass
        return str(path)

    def _excel_export_dialog(self):
        """Open column / period / format / News dialog, then write the
        selected periods of the cached scan to disk. XLSX with multiple
        periods → multi-sheet workbook (one sheet per period). CSV with
        multiple periods → single concatenated file with a Period column
        prepended at the front."""
        win = self.win
        if not win._period_results:
            QMessageBox.information(
                win, "Excel Export", "No results to export — run a scan first.",
            )
            return

        # Phase 8 §8.4 + 2026-05 update: pass the table's CURRENTLY
        # ORDERED column set (after any user drag / right-click
        # reorder), not just the canonical RESULT_COLUMNS. The
        # exporter will write columns in this order so saved sheets
        # match the on-screen layout for all selected periods.
        # Auto-data-presence preselection was dropped 2026-05 — the
        # dialog opens with everything checked and the user uses
        # Select-None / individual unchecks to refine.
        export_columns = win._ordered_active_columns_for_export()
        dlg = ExcelExportDialog(
            export_columns,
            periods=win._period_order, parent=win,
        )
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return

        keys = dlg.selected_keys()
        selected_periods = dlg.selected_periods()
        fmt = dlg.format_choice()
        wants_news = dlg.wants_news()
        wants_colors = dlg.wants_colors()

        if not keys:
            QMessageBox.information(
                win, "Excel Export", "No columns selected — nothing to export.",
            )
            return
        if not selected_periods:
            QMessageBox.information(
                win, "Excel Export", "No time periods selected — nothing to export.",
            )
            return

        suffix = ".xlsx" if fmt == "xlsx" else ".csv"
        filt = ("Excel Workbook (*.xlsx)" if fmt == "xlsx"
                else "CSV Files (*.csv)")
        path, _ = QFileDialog.getSaveFileName(
            win, "Export Results", f"trading_scanner_results{suffix}", filt,
        )
        if not path:
            return

        try:
            if fmt == "xlsx":
                win._write_xlsx_multi_sheet(
                    path, selected_periods, keys, wants_news,
                    apply_colors=wants_colors,
                )
            else:
                win._write_csv_export(
                    path, selected_periods, keys, wants_news,
                )
        except OSError as exc:
            QMessageBox.warning(win, "Export Error", f"Could not write file:\n{exc}")
            return
        except ImportError as exc:
            QMessageBox.warning(
                win, "Export Error",
                f"XLSX export requires the openpyxl package.\n{exc}",
            )
            return

        news_note = " (with News columns)" if wants_news else ""
        win.status.showMessage(
            f"Exported {len(selected_periods)} period(s) to {path}{news_note}"
        )
        win.log_panel.write_line(
            f"Excel export: {fmt.upper()} → {path} "
            f"({len(selected_periods)} period(s), {len(keys)} columns{news_note})"
        )

    def _write_xlsx_multi_sheet(
        self, path: str, periods: list[str], keys: list[str], wants_news: bool,
        *, apply_colors: bool = False,
    ):
        """Write each selected period as its own sheet. Applies the
        active view filters (Earnings Only / Color Match Only) so the
        export matches what the user sees in the table.

        When `apply_colors=True`, mirrors the on-screen text colors
        (match-color palette + streak green + display-only red) into
        per-cell font colors via openpyxl. Colors are only applied to
        the active period's sheet — non-active periods retain their
        underlying values (the table model only holds one period at a
        time so we can't read foregrounds for the others)."""
        win = self.win
        used_names: set[str] = set()
        sheet_to_period: dict[str, str] = {}
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for label in periods:
                raw = win._period_results.get(label, pd.DataFrame())
                df = win._apply_view_filters(raw)
                export_df = win._build_export_df(df, keys, wants_news)
                sheet = win._sanitize_sheet_name(label, used_names)
                sheet_to_period[sheet] = label
                export_df.to_excel(writer, sheet_name=sheet, index=False)

            if apply_colors:
                # Colors come from the live table model. The model only
                # holds the active period's render, so we apply colors
                # only to that sheet. Other sheets keep plain values —
                # acceptable: typical sequenced-run workflow exports the
                # current view, and if the user really wants every
                # period colored they can switch tabs and re-export.
                active_period = win._active_period or ""
                active_sheet = next(
                    (s for s, p in sheet_to_period.items()
                     if p == active_period),
                    None,
                )
                if active_sheet is not None:
                    win._apply_xlsx_cell_colors(
                        writer.book[active_sheet], keys, wants_news,
                    )

    def _apply_xlsx_cell_colors(self, ws, keys: list[str], wants_news: bool):
        """Walk the live ResultsTable model and stamp matching font
        colors onto each cell of `ws`. The export sheet's column layout
        is what `_build_export_df` produced — same `keys` order plus
        any News_<header> insertions when `wants_news` is True. Row
        order matches `_apply_view_filters(active_df)` since
        `_build_export_df` doesn't re-sort.
        """
        from openpyxl.styles import Font

        win = self.win
        # Map exported column index → live-table source column index.
        # The export's column order is the user's drag-reordered visual
        # order from `_ordered_active_columns_for_export`. We need the
        # SOURCE-MODEL column for each exported header.
        active = win.results_table.active_columns
        key_to_src_col = {k: c for c, (_h, k, _f) in enumerate(active)}

        export_cols = win._ordered_export_columns_with_news(keys, wants_news)
        # export_cols is the list of (header, source_key_or_None,
        # is_news_placeholder) for each column in the sheet.

        n_rows = win.results_table.model_src.rowCount()
        # Skip header row (xlsx row 1 is the header).
        for r_excel in range(2, n_rows + 2):
            r_src = r_excel - 2
            for c_excel, (_header, src_key, is_news) in enumerate(
                export_cols, start=1,
            ):
                if is_news or src_key is None:
                    continue
                src_col = key_to_src_col.get(src_key)
                if src_col is None:
                    continue
                item = win.results_table.model_src.item(r_src, src_col)
                if item is None:
                    continue
                qcolor = item.foreground().color()
                # Default brush returns invalid color (alpha=0 / no
                # explicit setForeground call). Skip those — leaves
                # cell at openpyxl default.
                if not qcolor.isValid() or qcolor.alpha() == 0:
                    continue
                # Treat near-white default as "no color set". Qt's
                # invalid foreground brush sometimes returns black or
                # near-black depending on context. Only apply colors
                # that ARE in the curated palette / streak-green /
                # fail-red set — others are likely the implicit
                # default and shouldn't pollute the export.
                rgb = (qcolor.red(), qcolor.green(), qcolor.blue())
                if not win._is_export_color(rgb):
                    continue
                hex_rgb = f"FF{qcolor.red():02X}{qcolor.green():02X}{qcolor.blue():02X}"
                cell = ws.cell(row=r_excel, column=c_excel)
                old = cell.font
                cell.font = Font(
                    name=old.name, size=old.size, bold=old.bold,
                    italic=old.italic, color=hex_rgb,
                )

    def _ordered_export_columns_with_news(
        self, keys: list[str], wants_news: bool,
    ) -> list[tuple[str, "str | None", bool]]:
        """Return [(header, source_key|None, is_news), ...] matching
        the layout produced by `_build_export_df`. News placeholders
        carry source_key=None and is_news=True; real columns carry
        their key plus is_news=False. Used by the color exporter to
        align Excel cells with live-table source columns.

        Iterates the LIVE active layout (same as `_build_export_df`)
        so dynamic q-i columns are included when their keys are in
        `keys` (i.e., the user checked the EPS / Rev bundle toggle)."""
        try:
            active = list(self.win.results_table.active_columns)
        except (AttributeError, RuntimeError):
            active = []
        layout = active if active else list(RESULT_COLUMNS)
        out: list[tuple[str, "str | None", bool]] = []
        for header, key, fmt_func in layout:
            if key not in keys:
                continue
            out.append((header, key, False))
            if wants_news and fmt_func is _fmt_date:
                out.append((f"News_{header}", None, True))
        return out

    def _is_export_color(self, rgb: tuple[int, int, int]) -> bool:
        """True iff the (r, g, b) tuple is one of the colors we
        deliberately apply in the table render (palette match-color,
        streak green, or display-only fail red). All other colors
        (including the implicit default foreground) are skipped."""
        from .widgets import ResultsTable
        # Curated palette
        for c in ResultsTable._ALIGN_PALETTE:
            if (c.red(), c.green(), c.blue()) == rgb:
                return True
        # Streak green
        sg = ResultsTable._STREAK_GREEN
        if (sg.red(), sg.green(), sg.blue()) == rgb:
            return True
        # Fail red
        fr = ResultsTable._FAIL_RED
        if (fr.red(), fr.green(), fr.blue()) == rgb:
            return True
        return False

    def _write_csv_export(
        self, path: str, periods: list[str], keys: list[str], wants_news: bool,
    ):
        """Single-period: just dump that period's selected columns. Multi-
        period: concatenate, prepending a `Period` column to identify rows.
        Both paths run the active view filters so the CSV matches the
        on-screen table."""
        win = self.win
        if len(periods) == 1:
            raw = win._period_results.get(periods[0], pd.DataFrame())
            df = win._apply_view_filters(raw)
            win._build_export_df(df, keys, wants_news).to_csv(path, index=False)
            return
        pieces = []
        for label in periods:
            raw = win._period_results.get(label, pd.DataFrame())
            df = win._apply_view_filters(raw)
            if df is None or df.empty:
                continue
            pieces.append(
                win._build_export_df(df, keys, wants_news, prepend_period=label)
            )
        if not pieces:
            # All selected periods empty (after view filters) — write a
            # header-only CSV
            win._build_export_df(
                pd.DataFrame(), keys, wants_news, prepend_period="",
            ).to_csv(path, index=False)
            return
        pd.concat(pieces, ignore_index=True).to_csv(path, index=False)

    def _export_results(self):
        """Export results table to CSV (full data) or TXT (tickers only).

        Phase 4 R13: CSV writing uses the stdlib csv.writer so embedded
        commas, quotes, and newlines are correctly escaped (QUOTE_MINIMAL).
        """
        win = self.win
        path, _ = QFileDialog.getSaveFileName(
            win, "Export Results", "",
            "CSV Files (*.csv);;Text Files (*.txt)"
        )
        if not path:
            return

        proxy = win.results_table.proxy
        rows = proxy.rowCount()
        cols = proxy.columnCount()

        try:
            if path.lower().endswith(".txt"):
                # TXT: comma-separated tickers only
                tickers = []
                for r in range(rows):
                    val = proxy.index(r, 0).data(Qt.ItemDataRole.DisplayRole)
                    if val:
                        tickers.append(val)
                with open(path, "w", encoding="utf-8") as f:
                    f.write(",".join(tickers))
            else:
                # CSV: full table with headers — csv.writer handles quoting.
                # Build headers from the LIVE model layout (active_columns),
                # not the static RESULT_COLUMNS: the model drops unpopulated
                # columns and appends dynamic q-i beats columns, so a static,
                # positional header list mislabels the data beneath it (and
                # loses headers entirely once beats columns push past
                # len(RESULT_COLUMNS)). active_columns is the exact list the
                # model was built from, so one header maps to each data column.
                try:
                    active = list(win.results_table.active_columns)
                except (AttributeError, RuntimeError):
                    active = list(RESULT_COLUMNS)
                headers = [h for (h, _key, _fmt) in active][:cols]
                with open(path, "w", encoding="utf-8", newline="") as f:
                    w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                    w.writerow(headers)
                    for r in range(rows):
                        row_data = [
                            proxy.index(r, c).data(Qt.ItemDataRole.DisplayRole) or ""
                            for c in range(cols)
                        ]
                        w.writerow(row_data)

            win.status.showMessage(f"Exported {rows} tickers to {path}")
        except OSError as exc:
            QMessageBox.warning(win, "Export Error", f"Could not write file:\n{exc}")
