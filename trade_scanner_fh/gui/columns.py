"""
Results-table column layout management — extracted from MainWindow
(Step A4).

Owns the Columns ▾ dropdown wiring, header-drag order persistence, the
per-session hidden-column set, the interleave-quarters layout flip, and
the scan-time reconcile rule (new columns to the right / drop removals).

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
from .widgets import _Q_COL_RE, RESULT_COLUMNS

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
        header drag.

        `keys` comes from the header, which only knows VISIBLE columns. A
        hidden column is merged back into the order next to the column it
        followed before (v8.0.0) — otherwise any drag would silently drop it
        from the saved order, and on unhide it would land at the far end
        instead of where the user last had it.
        """
        self.win._results_column_order = self._merge_hidden_into_order(
            list(keys))
        self.win._sync_columns_dialog()

    def _merge_hidden_into_order(self, visible: list) -> list:
        win = self.win
        try:
            hidden = set(win._deleted_column_keys)
        except (AttributeError, RuntimeError):
            hidden = set()
        if not hidden:
            return visible
        base = list(getattr(win, "_results_column_order", []) or [])
        if not base:
            full = self._layout_with_hidden_columns()
            base = [c[1] for c in full] if full else []
        out = list(visible)
        placed = set(out)
        # Walk the previous order so a run of adjacent hidden columns keeps
        # its internal order: each is anchored on whatever precedes it in
        # `base` — which, for the second of two hidden columns, is the first.
        for i, key in enumerate(base):
            if key not in hidden or key in placed:
                continue
            anchor_pos = -1
            for j in range(i - 1, -1, -1):
                if base[j] in placed:
                    anchor_pos = out.index(base[j])
                    break
            out.insert(anchor_pos + 1, key)
            placed.add(key)
        return out

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
            # Live populated table. List the layout BEFORE individual hiding
            # so an unticked column stays in the list, unticked, and can be
            # ticked back (v8.0.0). Listing the live `active` layout — as
            # this did before — dropped every hidden column from the dialog
            # on the next sync, leaving Reset to Default (which also throws
            # away the column order) as the only way to get one back.
            full = self._layout_with_hidden_columns()
            if full is not None:
                return full
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

    def _layout_with_hidden_columns(self):
        """The active period's column layout with individually hidden columns
        INCLUDED, in the order the table uses, or None if no frame is
        available.

        Order mirrors `ResultsTable._apply_saved_order`: keys in the saved
        order first, in that order; every other key after them in canonical
        order. Type-hidden columns (Hide Q / Hide FV) stay out — their
        dropdowns are the control for them.
        """
        win = self.win
        try:
            label = getattr(win, "_active_period", None)
            df = win._period_results.get(label) if label else None
            if df is None:
                return None
            layout = win.results_table.columns_before_key_hiding(df)
        except (AttributeError, RuntimeError, TypeError) as exc:
            log.debug("columns dialog: full layout unavailable: %s", exc)
            return None
        saved = list(getattr(win, "_results_column_order", []) or [])
        if not saved:
            return layout
        by_key = {c[1]: c for c in layout}
        ordered = [by_key[k] for k in saved if k in by_key]
        seen = {c[1] for c in ordered}
        ordered.extend(c for c in layout if c[1] not in seen)
        return ordered

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
        # Only Ticker is locked (v8.0.0); Close / % Gain / Gain Start became
        # hideable along with every other column.
        from .widgets import _UNHIDEABLE_KEYS as _CORE_KEYS
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
        MainWindow state and re-render. The Ticker column can never appear
        in `hidden_keys` (the dialog locks it).

        Columns newly unticked here are recorded as one hide batch, so
        Ctrl+Z undoes them exactly as it undoes a right-click hide."""
        from .widgets import _UNHIDEABLE_KEYS as _CORE_KEYS
        # Belt-and-suspenders: drop the locked key from the hidden set in
        # case a future bug lets it slip through the dialog filter.
        sanitized_hidden = {k for k in hidden_keys if k not in _CORE_KEYS}
        before = set(getattr(self.win, "_deleted_column_keys", set()))
        self.win._results_column_order = list(ordered_keys)
        self.win._deleted_column_keys = sanitized_hidden
        try:
            self.win._reapply_view_filters_for_active_period()
        except Exception as exc:
            log.debug("re-render after columns dialog update failed: %s", exc)
        try:
            self.win._hide_mgr.record_dialog_hides(before, sanitized_hidden)
        except Exception as exc:
            log.debug("recording dialog hides failed: %s", exc)

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
        try:
            win._hide_mgr.refresh_indicators()
        except Exception as exc:
            log.debug("hidden indicator refresh after reset failed: %s", exc)

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
        """Fold a fresh scan's columns into `_results_column_order`: what was
        already on screen keeps its place, a column whose filter was switched
        off drops out, and a NEW column goes to the RIGHT of everything that
        was there (v8.0.2, the user's rule: "their related metrics … always go
        to the right of the stuff that was already in the scan").

        `canonical_keys` is `_build_dynamic_columns` for the new results.
        The layout they join is the saved order or, when nothing is saved
        (never reordered, Reset, an interleave flip), the layout the table is
        showing for the previous results. With neither — the first scan, or
        the first after loading a preset — the canonical order stands.
        Either way the result is stored, so the next scan has a layout to
        append to; before v8.0.2 an empty order stayed empty (new columns
        landed wherever their panel row sits) and a saved one took new
        columns at the FAR LEFT.

        One exception, also the user's choice: extra Q-X blocks on a side
        that already shows blocks extend that run — Q-5 lands after Q-4, not
        at the right edge. A side appearing for the first time is new like
        anything else.
        """
        win = self.win
        base = list(getattr(win, "_results_column_order", []) or [])
        if not base:
            base = self._previous_layout_keys()
        win._results_column_order = merge_new_columns_right(
            base, list(canonical_keys))

    def _previous_layout_keys(self) -> list[str]:
        """Keys of the layout the table shows for the results being replaced,
        hidden columns (by key or by type) included, or [] when there are
        none. With no saved order that layout is the canonical one."""
        win = self.win
        try:
            df = win._last_results_df
            if df is None or df.empty:
                return []
            return [c[1] for c in win.results_table.unfiltered_columns_for(df)]
        except (AttributeError, RuntimeError, TypeError) as exc:
            log.debug("column reconcile: previous layout unavailable: %s", exc)
            return []


def _q_block_side(key: str):
    """'eps' / 'rev' for a Q-X block column key (``q3_reported_eps``), else
    None."""
    m = _Q_COL_RE.match(key)
    if m is None:
        return None
    suffix = m.group(2)
    if "eps" in suffix:
        return "eps"
    if "rev" in suffix:
        return "rev"
    return None


def merge_new_columns_right(base: list, canonical: list) -> list:
    """`base` without the keys `canonical` no longer has, then every new key
    at the right in canonical order — except a new Q-X block key on a side
    `base` already shows, which goes straight after its canonical
    predecessor so the quarter run stays contiguous. Empty `base` → the
    canonical order."""
    in_scan = set(canonical)
    out = [k for k in base if k in in_scan]
    placed = set(out)
    sides_shown = {s for s in map(_q_block_side, out) if s}
    for i, key in enumerate(canonical):
        if key in placed:
            continue
        if _q_block_side(key) in sides_shown and i > 0 \
                and canonical[i - 1] in placed:
            out.insert(out.index(canonical[i - 1]) + 1, key)
        else:
            out.append(key)
        placed.add(key)
    return out
