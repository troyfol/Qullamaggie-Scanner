"""
Hide / unhide rows and columns (v8.0.0) — replaced row and column delete.

State lives ON THE WINDOW, as it does for `ColumnManager`, so preset
save/load, `_apply_view_filters` and tests read it directly:

  ``_hidden_row_symbols``  tickers hidden from the results table. One set for
                           every period, kept across new scans, saved with a
                           preset as ``row_hidden``.
  ``_deleted_column_keys`` column keys hidden individually (the historical
                           name predates hiding; saved as ``column_hidden``).
  ``_hide_undo_stack``     batches for Ctrl+Z, newest last.

Rows are hidden in `MainWindow._apply_view_filters`, the single point every
render and every export passes through, so a hidden ticker is absent from the
table, every timeframe period, Excel/CSV/Quick Export and Send to Watchlist
without any of those knowing about hiding. Columns are hidden from the table's
column LAYOUT (`ResultsTable.set_hidden_column_keys`), never from the data, so
the values survive for an unhide and for colouring rules.

Precedence: a column suppressed by Hide Q Columns / Hide FV Columns cannot be
unhidden from here — it is listed, disabled, with the dropdown named.

Design notes carried over from ColumnManager (load-bearing for tests):
the manager holds a plain back-reference (``self.win``), is not a QObject, and
calls back through window methods so bypass-init test shells keep working.
"""

from __future__ import annotations

import logging

log = logging.getLogger("scanner.gui")

# Bounded so a long session of hides cannot grow without limit; far more
# than anyone undoes in practice.
UNDO_LIMIT = 50


class HideManager:
    def __init__(self, window):
        self.win = window

    # ── state accessors (tolerant of bypass-init shells) ─────────────

    def _rows(self) -> set:
        try:
            return self.win._hidden_row_symbols
        except (AttributeError, RuntimeError):
            return set()

    def _cols(self) -> set:
        try:
            return self.win._deleted_column_keys
        except (AttributeError, RuntimeError):
            return set()

    def _stack(self) -> list:
        try:
            return self.win._hide_undo_stack
        except (AttributeError, RuntimeError):
            return []

    def _push_undo(self, kind: str, keys: list) -> None:
        if not keys:
            return
        stack = self._stack()
        stack.append({"kind": kind, "keys": list(keys)})
        del stack[:-UNDO_LIMIT]

    # ── rows ─────────────────────────────────────────────────────────

    def hide_rows(self, symbols: list) -> None:
        rows = self._rows()
        added = [s for s in dict.fromkeys(str(x) for x in symbols or [])
                 if s and s not in rows]
        if not added:
            return
        rows.update(added)
        self._push_undo("rows", added)
        # A pending row cut may name a now-hidden ticker; pasting it would
        # move a row the user cannot see.
        try:
            self.win.results_table.clear_cut_clipboard()
        except Exception as exc:
            log.debug("cut-clipboard clear after hide failed: %s", exc)
        log.info("Hid %d row(s): %s", len(added), ", ".join(added))
        self._changed()

    def unhide_rows(self, symbols: list) -> None:
        rows = self._rows()
        gone = [s for s in symbols or [] if s in rows]
        if not gone:
            return
        rows.difference_update(gone)
        log.info("Unhid %d row(s): %s", len(gone), ", ".join(gone))
        self._changed()

    def unhide_all_rows(self) -> None:
        rows = self._rows()
        if not rows:
            return
        n = len(rows)
        rows.clear()
        log.info("Unhid all %d hidden row(s)", n)
        self._changed()

    # ── columns ──────────────────────────────────────────────────────

    def hide_columns(self, keys: list) -> None:
        from .widgets import _UNHIDEABLE_KEYS
        cols = self._cols()
        added = [k for k in dict.fromkeys(keys or [])
                 if k and k not in cols and k not in _UNHIDEABLE_KEYS]
        if not added:
            return
        cols.update(added)
        self._push_undo("columns", added)
        log.info("Hid %d column(s) from view: %s", len(added), ", ".join(added))
        self._changed(columns=True)

    def unhide_columns(self, keys: list) -> None:
        cols = self._cols()
        gone = [k for k in keys or [] if k in cols]
        if not gone:
            return
        cols.difference_update(gone)
        log.info("Unhid %d column(s): %s", len(gone), ", ".join(gone))
        self._changed(columns=True)

    def unhide_all_columns(self) -> None:
        """Every individually hidden column, including ones this scan does
        not produce. Columns a Hide Q / Hide FV dropdown suppresses stay
        suppressed — that is the dropdown's state, not this set's."""
        cols = self._cols()
        if not cols:
            return
        n = len(cols)
        cols.clear()
        log.info("Unhid all %d hidden column(s)", n)
        self._changed(columns=True)

    def record_dialog_hides(self, before: set, after: set) -> None:
        """Columns unticked in the Columns dialog count as one hide batch,
        so Ctrl+Z treats them like a right-click hide."""
        self._push_undo("columns", sorted(set(after) - set(before)))
        self.refresh_indicators()

    # ── undo ─────────────────────────────────────────────────────────

    def undo(self) -> bool:
        """Unhide the most recent batch that is still (partly) hidden.

        A batch the user already unhid by hand is skipped rather than
        consuming the keypress on a no-op. Returns True if anything changed.
        """
        stack = self._stack()
        while stack:
            entry = stack.pop()
            live = self._rows() if entry["kind"] == "rows" else self._cols()
            still = [k for k in entry["keys"] if k in live]
            if not still:
                continue
            live.difference_update(still)
            log.info("Undo hide: %d %s restored", len(still), entry["kind"])
            self._changed(columns=entry["kind"] == "columns")
            return True
        self.refresh_indicators()
        return False

    # ── presets ──────────────────────────────────────────────────────

    def apply_preset(self, data: dict) -> None:
        """Make every hide set match the incoming preset (v8.0.0).

        The user's rule: changing presets always hides/unhides to match the
        preset. A key the preset does not carry therefore means NOTHING
        hidden — not "keep whatever the session had". The undo stack refers
        to the outgoing preset's hides and is dropped.
        """
        win = self.win
        win._hidden_row_symbols = set(data.get("row_hidden") or [])
        win._deleted_column_keys = set(data.get("column_hidden") or [])
        win._hidden_earnings_col_types = set(
            data.get("hidden_earnings_col_types") or [])
        win._hidden_fv_col_types = set(data.get("hidden_fv_col_types") or [])
        win._hide_undo_stack = []

    # ── menus ────────────────────────────────────────────────────────

    def row_unhide_items(self) -> list:
        """`[(label, symbol, enabled)]` for the Unhide rows submenu.

        Tickers in the active period come first. A ticker that would still
        not show after unhiding — absent from this period, or removed by an
        Earnings / Color Match view filter — is annotated so the list
        explains itself; it stays enabled so it can be cleared.
        """
        rows = self._rows()
        if not rows:
            return []
        win = self.win
        raw_syms, view_syms = set(), set()
        try:
            label = getattr(win, "_active_period", None)
            raw = win._period_results.get(label) if label else None
            if raw is not None and not raw.empty and "symbol" in raw.columns:
                raw_syms = set(raw["symbol"].astype(str))
                shown = win._apply_view_filters(raw, include_hidden=False)
                if shown is not None and "symbol" in shown.columns:
                    view_syms = set(shown["symbol"].astype(str))
        except Exception as exc:
            log.debug("row unhide items: period unavailable: %s", exc)
        here, elsewhere = [], []
        for sym in sorted(rows):
            if sym in view_syms:
                here.append((sym, sym, True))
            elif sym in raw_syms:
                here.append((f"{sym}  (filtered by a view filter)", sym, True))
            else:
                elsewhere.append((f"{sym}  (not in this period)", sym, True))
        return here + elsewhere

    def column_unhide_items(self) -> list:
        """`[(label, key, enabled)]` for the Unhide columns submenu."""
        cols = self._cols()
        if not cols:
            return []
        from .widgets import (
            RESULT_COLUMNS, earnings_column_type_of, fv_column_type_of,
        )
        win = self.win
        try:
            layout = win._hide_types_source_columns()
        except Exception:
            layout = []
        header_of = {k: h for h, k, _f in RESULT_COLUMNS}
        header_of.update({k: h for h, k, _f in layout})
        in_scan = [k for _h, k, _f in layout if k in cols]
        try:
            q_hidden = set(win._hidden_earnings_col_types)
        except (AttributeError, RuntimeError):
            q_hidden = set()
        try:
            fv_hidden = set(win._hidden_fv_col_types)
        except (AttributeError, RuntimeError):
            fv_hidden = set()
        items, locked = [], []
        for key in in_scan:
            head = header_of.get(key, key)
            if earnings_column_type_of(key) in q_hidden:
                locked.append((f"{head}  (hidden by Hide Q Columns)", key, False))
            elif fv_column_type_of(key) in fv_hidden:
                locked.append((f"{head}  (hidden by Hide FV Columns)", key, False))
            else:
                items.append((head, key, True))
        absent = sorted(k for k in cols if k not in set(in_scan))
        items += [(f"{header_of.get(k, k)}  (not in this scan)", k, True)
                  for k in absent]
        return items + locked

    # ── indicators ───────────────────────────────────────────────────

    def hidden_in_period(self, label) -> int:
        """How many hidden tickers the period's results actually contain."""
        rows = self._rows()
        if not rows:
            return 0
        try:
            raw = self.win._period_results.get(label)
        except (AttributeError, RuntimeError):
            return 0
        if raw is None or raw.empty or "symbol" not in raw.columns:
            return 0
        return int(raw["symbol"].astype(str).isin(rows).sum())

    def refresh_indicators(self) -> None:
        """Ribbon button caption, table undo flag, timeframe labels."""
        win = self.win
        n_rows, n_cols = len(self._rows()), len(self._cols())
        try:
            if n_rows or n_cols:
                parts = []
                if n_rows:
                    parts.append(f"{n_rows} row{'s' if n_rows != 1 else ''}")
                if n_cols:
                    parts.append(f"{n_cols} col{'s' if n_cols != 1 else ''}")
                win.btn_hidden_items.setText(f"Hidden: {' · '.join(parts)} ▾")
            else:
                win.btn_hidden_items.setText("Hidden: none ▾")
        except (AttributeError, RuntimeError):
            pass
        try:
            win.results_table.set_hide_undo_available(bool(self._stack()))
        except (AttributeError, RuntimeError):
            pass
        self.refresh_timeframe_labels()

    def refresh_timeframe_labels(self) -> None:
        win = self.win
        try:
            combo = win.combo_timeframe
            results = win._period_results
        except (AttributeError, RuntimeError):
            return
        combo.blockSignals(True)
        try:
            for i in range(combo.count()):
                label = combo.itemData(i)
                df = results.get(label)
                n = 0 if df is None else len(df)
                h = self.hidden_in_period(label)
                text = f"{label}  —  {n} results"
                if h:
                    text += f" ({h} hidden)"
                combo.setItemText(i, text)
        finally:
            combo.blockSignals(False)

    def after_scan(self) -> None:
        """Say so when hidden tickers are in the new results — a hide that
        outlives the scan it was made in must never make a ticker vanish
        silently."""
        rows = self._rows()
        if rows:
            present = set()
            for df in (getattr(self.win, "_period_results", {}) or {}).values():
                if df is not None and not df.empty and "symbol" in df.columns:
                    present |= set(df["symbol"].astype(str)) & rows
            if present:
                names = sorted(present)
                shown = ", ".join(names[:12]) + (
                    f" … (+{len(names) - 12})" if len(names) > 12 else "")
                try:
                    self.win.log_panel.write_line(
                        f"Hidden rows: {len(names)} hidden ticker(s) are in "
                        f"these results and not shown — {shown}. Right-click "
                        f"the table (or Hidden ▾) to unhide."
                    )
                except (AttributeError, RuntimeError):
                    pass
        self.refresh_indicators()

    # ── re-render ────────────────────────────────────────────────────

    def _changed(self, *, columns: bool = False) -> None:
        win = self.win
        try:
            win._reapply_view_filters_for_active_period()
        except Exception as exc:
            log.debug("re-render after hide/unhide failed: %s", exc)
        if columns:
            try:
                win._sync_columns_dialog()
            except Exception as exc:
                log.debug("columns dialog sync after hide failed: %s", exc)
        self.refresh_indicators()
