"""
Results-table column layout management — extracted from MainWindow
(Step A4).

Owns the Columns ▾ dropdown wiring, header-drag order persistence, the
per-session hidden-column set, the interleave-quarters layout flip, and
the scan-time reconcile rule (prepend additions / drop removals).

Design notes (load-bearing for the test suite — do not "simplify"):

- The manager holds a plain back-reference to the window (``self.win``)
  and is NOT parented to it as a QObject: tests build bare
  ``MainWindow.__new__(MainWindow)`` shells whose C++ side is
  uninitialized, and passing one as a QObject parent would raise.
- The mutable layout state stays ON THE WINDOW: tests, ``__init__``,
  preset save/load, and ``_apply_view_filters`` all read/write
  ``_results_column_order`` / ``_deleted_column_keys`` /
  ``_columns_dialog`` directly as window attributes. The manager only
  reads/writes them via ``self.win`` so those access patterns are
  untouched.
- Every cross-method orchestration call routes through the window's
  delegate (``self.win._sync_columns_dialog()``, not a direct
  ``self._sync_columns_dialog()``) — preserving exactly the
  pre-extraction dynamic-dispatch semantics for tests that override
  window methods as instance attributes.
"""

from __future__ import annotations

import logging

from .dialogs import ColumnsManagerDialog
from .widgets import RESULT_COLUMNS

# Same logger channel as main_window so the extracted log lines keep
# their historical "scanner.gui" tag in the panel / subsystem files.
log = logging.getLogger("scanner.gui")

# key → (header, fmt) lookup over the canonical RESULT_COLUMNS, used by
# `_current_columns_for_dialog`'s preset-loaded (no-scan) branch.
# RESULT_COLUMNS is a static module-level list, so the dict is built
# ONCE at import instead of being rebuilt on every dialog open / sync.
_RESULT_COLUMNS_BY_KEY: dict[str, tuple] = {
    k: (h, f) for h, k, f in RESULT_COLUMNS
}


class ColumnManager:
    """Column reorder / hide / persist logic for MainWindow.

    MainWindow keeps every historical method name as a thin delegate onto
    this object, so signal wiring, menu actions, and tests are untouched.
    """

    def __init__(self, window):
        self.win = window

    def _on_results_column_order_changed(self, keys: list):
        """Persist the user's column order. Threaded into Excel
        export and re-applied across timeframe switches. Also pushes
        into the open Columns dropdown so it stays in sync with a
        header drag."""
        self.win._results_column_order = list(keys)
        self.win._sync_columns_dialog()

    def _on_interleave_quarters_toggled(self, checked: bool):
        """User toggled the Interleave Q EPS+Rev checkbox. Flip the
        table's flag and re-render the active period so the new
        column layout takes effect immediately.

        The saved column order is cleared as part of the toggle: any
        prior manual reorder captured the OLD per-quarter layout, and
        re-applying it after populate would put the q-i blocks back
        where they were and visibly undo the interleave flip. Treating
        the toggle as an explicit "rearrange columns" action means the
        new canonical order wins; the user can re-drag afterwards if
        they want a custom layout on top of the new flag.
        """
        try:
            self.win.results_table.set_interleave_quarters(bool(checked))
            self.win._results_column_order = []
            self.win.results_table.set_saved_column_order([])
            self.win._reapply_view_filters_for_active_period()
            self.win._sync_columns_dialog()
        except Exception as exc:
            log.debug("interleave toggle failed: %s", exc)

    # ── Columns dropdown wiring ──────────────────────────────────────

    def _current_columns_for_dialog(self) -> list[tuple]:
        """Build the (header, key, fmt) list the popup should show.

        Sources, in order of preference:
          1. The live `_active_columns` if a scan has populated the
             table (this is post-`_build_dynamic_columns` AND already
             reordered per `_results_column_order`).
          2. A canonical `_build_dynamic_columns` build of the cached
             active period — used when a preset just loaded and wiped
             the results so the dialog still shows the preset's
             column intent.
          3. Empty list (pre-scan, no preset applied) — caller should
             treat the dialog as a no-op.
        """
        try:
            active = list(self.win.results_table.active_columns)
        except (AttributeError, RuntimeError):
            active = []
        if active and self.win.results_table.model_src.rowCount() > 0:
            # Live populated table — already in the user's visual order
            # via `_apply_saved_order` after populate. Mirror that order
            # in the dialog.
            return self.win._reorder_for_visual(active)
        # No scan in flight; consult the saved order if we have one
        # (preset-loaded scenario). Use a synthetic canonical build
        # from RESULT_COLUMNS so headers / formatters resolve.
        if self.win._results_column_order:
            by_key = _RESULT_COLUMNS_BY_KEY
            out: list[tuple] = []
            seen: set[str] = set()
            for k in self.win._results_column_order:
                entry = by_key.get(k)
                if entry is not None:
                    out.append((entry[0], k, entry[1]))
                    seen.add(k)
            return out
        return []

    def _reorder_for_visual(self, active: list[tuple]) -> list[tuple]:
        """Return `active` reordered to match the table header's
        current visual position. Falls back to canonical order on any
        Qt failure (uninitialized header during shell-mode tests)."""
        try:
            header = self.win.results_table.horizontalHeader()
            n = header.count()
            if n != len(active):
                return list(active)
            return [active[header.logicalIndex(v)] for v in range(n)]
        except Exception:
            return list(active)

    def _open_columns_dialog(self):
        """Open / focus the modeless ColumnsManagerDialog. Singleton —
        re-clicking the toolbar button raises the existing window
        rather than spawning a second copy. Pre-scan + no-preset state
        falls through to a status-bar nudge instead of a useless empty
        popup."""
        from .widgets import _ALWAYS_VISIBLE_KEYS as _CORE_KEYS
        win = self.win
        cols = win._current_columns_for_dialog()
        if not cols:
            win.status.showMessage(
                "No columns to manage yet — run a scan or load a preset.",
                4000,
            )
            return
        hidden = set(getattr(win, "_deleted_column_keys", set()))
        if win._columns_dialog is None:
            win._columns_dialog = ColumnsManagerDialog(
                cols, hidden, _CORE_KEYS, parent=win,
            )
            win._columns_dialog.columns_updated.connect(
                win._on_columns_dialog_updated
            )
            win._columns_dialog.reset_requested.connect(
                win._on_columns_dialog_reset
            )
        else:
            win._columns_dialog.update_columns(cols, hidden)
        win._columns_dialog.show()
        win._columns_dialog.raise_()
        win._columns_dialog.activateWindow()

    def _on_columns_dialog_updated(self, ordered_keys: list, hidden_keys: list):
        """Drag/check change inside the dropdown popup. Mirror onto
        MainWindow state and re-render. Always-visible keys can never
        appear in `hidden_keys` (the dialog blocks that), so this is a
        plain assignment."""
        from .widgets import _ALWAYS_VISIBLE_KEYS as _CORE_KEYS
        # Belt-and-suspenders: drop core keys from the hidden set in
        # case a future bug lets them slip through the dialog filter.
        sanitized_hidden = {k for k in hidden_keys if k not in _CORE_KEYS}
        self.win._results_column_order = list(ordered_keys)
        self.win._deleted_column_keys = sanitized_hidden
        try:
            self.win._reapply_view_filters_for_active_period()
        except Exception as exc:
            log.debug("re-render after columns dialog update failed: %s", exc)

    def _on_columns_dialog_reset(self):
        """User clicked Reset to Default inside the popup. Clears both
        the manual order and the hidden set, re-renders, and refreshes
        the popup so it shows the canonical layout."""
        self.win._reset_columns_to_default()

    def _reset_columns_to_default(self) -> None:
        """Shared helper for Reset to Default — invoked from the
        popup, the header right-click, and the preset-load flow when
        the saved preset has no column metadata."""
        win = self.win
        win._results_column_order = []
        win._deleted_column_keys = set()
        try:
            win.results_table.set_saved_column_order([])
        except Exception as exc:
            # The table widget may now hold a stale saved order that no
            # longer matches the (reset/canonical) MainWindow state —
            # log the divergence so a wrong column layout after Reset
            # to Default is diagnosable.
            log.debug(
                "column reset: results_table.set_saved_column_order([]) "
                "failed — table may keep a stale saved order while "
                "MainWindow order is reset to canonical: %s", exc,
            )
        try:
            win._reapply_view_filters_for_active_period()
        except Exception as exc:
            log.debug("re-render after column reset failed: %s", exc)
        win._sync_columns_dialog()

    def _sync_columns_dialog(self) -> None:
        """Push the current MainWindow column state into the open
        dropdown (no-op when the popup hasn't been spawned yet). Called
        from every code path that mutates `_results_column_order` or
        `_deleted_column_keys` outside the dialog itself."""
        win = self.win
        if win._columns_dialog is None:
            return
        cols = win._current_columns_for_dialog()
        hidden = set(getattr(win, "_deleted_column_keys", set()))
        win._columns_dialog.update_columns(cols, hidden)

    def _reconcile_column_order_for_scan(
        self, canonical_keys: list[str],
    ) -> None:
        """Apply the prepend-additions / drop-removals rule to
        `_results_column_order` based on a fresh canonical column
        list (output of `_build_dynamic_columns` for the active
        period). No-op when the saved order is empty (canonical
        already wins).

        Behavior:
          • Saved order empty → leave it empty (canonical applies).
          • Saved order non-empty → drop any saved keys NOT in the
            new canonical set; prepend any new canonical keys NOT
            already in the saved order, in their canonical order
            (which mirrors the indicator panel's top-to-bottom
            arrangement, i.e. "first added = leftmost in table").
        """
        win = self.win
        if not win._results_column_order:
            return
        canonical_set = set(canonical_keys)
        saved = list(win._results_column_order)
        kept = [k for k in saved if k in canonical_set]
        saved_set = set(kept)
        additions = [k for k in canonical_keys if k not in saved_set]
        win._results_column_order = additions + kept

    def _on_columns_deletion_requested(self, keys: list):
        """User hid one or more columns via the header right-click
        menu. Add the keys to the per-session hide-set and re-render.
        Always-visible core columns are filtered upstream by the
        ResultsTable translator slot, so this slot doesn't need to
        re-check them."""
        if not keys:
            return
        added = False
        for k in keys:
            if k and k not in self.win._deleted_column_keys:
                self.win._deleted_column_keys.add(k)
                added = True
        if not added:
            return
        log.info(
            "Hid %d column(s) from view: %s",
            len(keys), ", ".join(keys),
        )
        try:
            self.win._reapply_view_filters_for_active_period()
        except Exception as exc:
            log.debug("re-render after column delete failed: %s", exc)
