"""Tests for the 2026-05 results-window changes:

  1. Multi-color earnings-aligned palette (Option A) — distinct dates
     in a row get distinct palette colors; same date = same color.
  2. Hide-unselected-filter columns — RESULT_COLUMNS entries whose
     key isn't in the result DataFrame get filtered out.
  3. Drag-to-reorder + right-click context menu — `ReorderableHeader`
     supports multi-select, send-to-front / send-to-end, reset.
     `ResultsTable` re-applies saved order on every populate so
     timeframe switches preserve the user's layout.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest


def _row(symbol="A", **extras):
    base = {"symbol": symbol, "close": 100.0,
            "price": 100.0, "pct_gain": 10.0}
    base.update(extras)
    return base


# ──────────────────────────────────────────────────────────────────────
# 1. Multi-color palette
# ──────────────────────────────────────────────────────────────────────

def test_aligned_palette_assigns_distinct_colors_to_distinct_dates(_qapp):
    """Two distinct aligned dates in a row → two different palette
    colors (not both the legacy single blue). Each canonical needs
    an earnings cell anchoring it — provided here via Q-1 / Q-2 EPS
    blocks so both groups have a visible earnings anchor."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    df = pd.DataFrame([_row(
        "A",
        max_gap_pct=12.0,
        max_gap_date=pd.Timestamp("2026-02-15"),
        min_gap_date=pd.Timestamp("2025-11-15"),
        max_neg_gap_pct=-8.0,
        consec_eps_beats=2,
        q1_report_date_eps=pd.Timestamp("2026-02-15"),
        q1_reported_eps=2.0, q1_surprise_eps_dollar=0.1, q1_surprise_eps_pct=5.0,
        q2_report_date_eps=pd.Timestamp("2025-11-15"),
        q2_reported_eps=1.9, q2_surprise_eps_dollar=0.05, q2_surprise_eps_pct=3.0,
        _earnings_aligned_dates=["2026-02-15", "2025-11-15"],
    )])
    table = ResultsTable()
    table.populate(df)
    headers = [c[0] for c in table.active_columns]
    max_gap_idx = headers.index("Max Gap Date")
    min_gap_idx = headers.index("Min Gap Date")

    max_color = table.model_src.item(0, max_gap_idx).foreground().color()
    min_color = table.model_src.item(0, min_gap_idx).foreground().color()

    # Distinct dates get distinct colors.
    assert max_color != min_color
    # Both colors come from the curated palette.
    palette_rgbs = {(c.red(), c.green(), c.blue())
                    for c in ResultsTable._ALIGN_PALETTE}
    assert (max_color.red(), max_color.green(), max_color.blue()) in palette_rgbs
    assert (min_color.red(), min_color.green(), min_color.blue()) in palette_rgbs


def test_aligned_palette_same_date_same_color(_qapp):
    """When two cells in the same row hold the same date value,
    they MUST get the same color so the user can visually pair them."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    same = pd.Timestamp("2026-02-15")
    df = pd.DataFrame([_row(
        "A",
        max_gap_pct=12.0, max_gap_date=same,
        last_report_date=same,
        reported_eps=2.0,
        _earnings_aligned_dates=["2026-02-15"],
    )])
    table = ResultsTable()
    table.populate(df)
    headers = [c[0] for c in table.active_columns]
    gap_color = table.model_src.item(0, headers.index("Max Gap Date")).foreground().color()
    lrd_color = table.model_src.item(0, headers.index("Last Report Date")).foreground().color()
    assert gap_color == lrd_color


def test_aligned_palette_per_ticker_randomization_avoids_cross_ticker_collision(_qapp):
    """Per-match randomization: when ticker A and ticker B both have an
    earnings match on the same date, they must (almost) always render
    that date in DIFFERENT colors. Different ticker → different seed
    → different palette pick. Collision is possible (12 colors, finite
    seed space) but has to be exceedingly rare. Two known symbols
    that don't collide pin the contract."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    same_date = pd.Timestamp("2026-02-15")
    df = pd.DataFrame([
        _row("AAPL",
             max_gap_pct=12.0, max_gap_date=same_date,
             last_report_date=same_date,
             _earnings_aligned_dates=["2026-02-15"]),
        _row("MSFT",
             max_gap_pct=8.0, max_gap_date=same_date,
             last_report_date=same_date,
             _earnings_aligned_dates=["2026-02-15"]),
    ])
    table = ResultsTable()
    table.populate(df)

    headers = [c[0] for c in table.active_columns]
    idx = headers.index("Max Gap Date")
    aapl_color = table.model_src.item(0, idx).foreground().color()
    msft_color = table.model_src.item(1, idx).foreground().color()

    # Both come from the curated palette
    palette_rgbs = {(c.red(), c.green(), c.blue())
                    for c in ResultsTable._ALIGN_PALETTE}
    assert (aapl_color.red(), aapl_color.green(), aapl_color.blue()) in palette_rgbs
    assert (msft_color.red(), msft_color.green(), msft_color.blue()) in palette_rgbs
    # AAPL and MSFT should NOT share a color on the same date.
    # (If this ever fails because the seed strings happen to collide,
    # swap one of the symbols rather than weakening the guarantee.)
    assert aapl_color != msft_color


def test_aligned_palette_per_match_randomization_is_stable_across_renders(_qapp):
    """Calling populate() twice with the same data must reproduce
    the same colors — the seed is (ticker, date), not random. Stable
    across re-sorts, timeframe switches, and re-runs of identical
    scans is what makes the per-match scheme usable."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    df = pd.DataFrame([_row(
        "AAPL",
        max_gap_pct=12.0,
        max_gap_date=pd.Timestamp("2026-02-15"),
        min_gap_date=pd.Timestamp("2025-11-15"),
        max_neg_gap_pct=-8.0,
        consec_eps_beats=2,
        q1_report_date_eps=pd.Timestamp("2026-02-15"),
        q1_reported_eps=2.0, q1_surprise_eps_dollar=0.1, q1_surprise_eps_pct=5.0,
        q2_report_date_eps=pd.Timestamp("2025-11-15"),
        q2_reported_eps=1.9, q2_surprise_eps_dollar=0.05, q2_surprise_eps_pct=3.0,
        _earnings_aligned_dates=["2026-02-15", "2025-11-15"],
    )])
    t1 = ResultsTable()
    t1.populate(df)
    t2 = ResultsTable()
    t2.populate(df)

    h1 = [c[0] for c in t1.active_columns]
    h2 = [c[0] for c in t2.active_columns]
    for label in ("Max Gap Date", "Min Gap Date"):
        c1 = t1.model_src.item(0, h1.index(label)).foreground().color()
        c2 = t2.model_src.item(0, h2.index(label)).foreground().color()
        assert c1 == c2, f"{label} color drifted between renders"


def test_aligned_palette_excludes_red_yellow_green_families():
    """Curated palette must avoid the red wedge AND the yellow/green
    band — user prefs are cool tones only (cyan / blue / indigo /
    purple / teal). Hue thresholds:
      - reds:    [0°, 30°] ∪ [330°, 360°]
      - yellows: [30°, 90°]
      - greens:  [90°, 165°]
    Allowed band is therefore [165°, 330°]. Also enforces a minimum
    brightness so dark muddy entries don't sneak in."""
    from trade_scanner_fh.gui.widgets import ResultsTable
    for c in ResultsTable._ALIGN_PALETTE:
        h = c.hue()
        if h < 0:
            pytest.fail(f"Palette contains greyscale: {c.name()}")
        assert 165 <= h <= 330, (
            f"Palette color {c.name()} has hue {h}° outside the "
            "allowed cool-tone band [165°, 330°]"
        )
        assert c.value() >= 150, f"Palette color {c.name()} too dark"


# ──────────────────────────────────────────────────────────────────────
# 2. Hide unselected filter columns
# ──────────────────────────────────────────────────────────────────────

def test_only_present_keys_appear_in_active_columns():
    """Result frame only has core + a couple of filter columns →
    `_build_dynamic_columns` strips out everything else."""
    from trade_scanner_fh.gui.widgets import _build_dynamic_columns

    df = pd.DataFrame([_row(
        "A",
        # Filters that ARE on:
        sti=1.10, dist_high_pct=2.0,
        # Note: rs_market, atr, bbw etc. are NOT in this df
    )])
    cols, _, _ = _build_dynamic_columns(df)
    keys = [c[1] for c in cols]
    # Active filters surface
    assert "sti" in keys
    assert "dist_high_pct" in keys
    # Disabled filters don't take up space
    assert "rs_market" not in keys
    assert "atr" not in keys
    assert "bbw" not in keys
    assert "consec_gaps" not in keys


def test_always_visible_keys_remain_even_in_minimal_frame():
    """Core anchor columns (symbol, close, pct_gain) always show."""
    from trade_scanner_fh.gui.widgets import _build_dynamic_columns
    df = pd.DataFrame([{"symbol": "A", "close": 50.0,
                        "price": 50.0, "pct_gain": 5.0,
                        "gain_start_date": pd.Timestamp("2025-01-01")}])
    cols, _, _ = _build_dynamic_columns(df)
    keys = [c[1] for c in cols]
    assert "symbol" in keys
    assert "close" in keys
    assert "pct_gain" in keys


# ──────────────────────────────────────────────────────────────────────
# 3. ReorderableHeader + ResultsTable order persistence
# ──────────────────────────────────────────────────────────────────────

def test_ctrl_click_marks_section_label_with_diamond(_qapp):
    """Multi-select feedback: ctrl-clicking a header section prepends
    a `◆ ` marker to that section's display label, and toggling it
    off removes the marker. Replaces the prior paint-based highlight
    that was silently dropped by Qt's stylesheet engine."""
    from trade_scanner_fh.gui.widgets import ReorderableHeader
    from PyQt6.QtGui import QStandardItemModel
    from PyQt6.QtWidgets import QTableView
    from PyQt6.QtCore import Qt

    model = QStandardItemModel()
    model.setHorizontalHeaderLabels(["A", "B", "C", "D", "E"])
    view = QTableView()
    header = ReorderableHeader(view)
    view.setModel(model)
    view.setHorizontalHeader(header)

    marker = ReorderableHeader._SELECTED_MARKER
    # Select section 2.
    header._toggle_selection(2)
    text = model.headerData(
        2, Qt.Orientation.Horizontal, Qt.ItemDataRole.DisplayRole,
    )
    assert text.startswith(marker), (
        f"selected section text {text!r} should start with marker"
    )
    assert text == marker + "C"

    # Selecting another doesn't disturb the first.
    header._toggle_selection(4)
    assert model.headerData(
        4, Qt.Orientation.Horizontal, Qt.ItemDataRole.DisplayRole,
    ) == marker + "E"
    assert model.headerData(
        2, Qt.Orientation.Horizontal, Qt.ItemDataRole.DisplayRole,
    ) == marker + "C"

    # Toggling off removes the marker cleanly (no accumulation).
    header._toggle_selection(2)
    assert model.headerData(
        2, Qt.Orientation.Horizontal, Qt.ItemDataRole.DisplayRole,
    ) == "C"
    # Section 4 still selected.
    assert model.headerData(
        4, Qt.Orientation.Horizontal, Qt.ItemDataRole.DisplayRole,
    ) == marker + "E"

    # clear_selection() removes ALL markers.
    header.clear_selection()
    assert model.headerData(
        4, Qt.Orientation.Horizontal, Qt.ItemDataRole.DisplayRole,
    ) == "E"


def test_header_send_to_front_moves_logical_block(_qapp):
    """`_move_block(targets, to_front=True)` must move the chosen
    logical sections to visual indices 0..N-1, preserving their
    original relative order."""
    from trade_scanner_fh.gui.widgets import ReorderableHeader
    from PyQt6.QtGui import QStandardItemModel
    from PyQt6.QtWidgets import QTableView

    model = QStandardItemModel()
    model.setHorizontalHeaderLabels(["A", "B", "C", "D", "E"])
    view = QTableView()
    header = ReorderableHeader(view)
    view.setModel(model)
    view.setHorizontalHeader(header)

    # Move logical 2 (C) and logical 3 (D) to the front.
    # Their current visual indices are also 2 and 3 (no prior moves).
    header._move_block([2, 3], to_front=True)

    # New visual order: C, D, A, B, E
    visual_to_logical = [header.logicalIndex(v) for v in range(5)]
    assert visual_to_logical == [2, 3, 0, 1, 4]


def test_header_send_to_end_moves_block_to_tail(_qapp):
    from trade_scanner_fh.gui.widgets import ReorderableHeader
    from PyQt6.QtGui import QStandardItemModel
    from PyQt6.QtWidgets import QTableView

    model = QStandardItemModel()
    model.setHorizontalHeaderLabels(["A", "B", "C", "D", "E"])
    view = QTableView()
    header = ReorderableHeader(view)
    view.setModel(model)
    view.setHorizontalHeader(header)

    header._move_block([0, 1], to_front=False)
    # New visual order: C, D, E, A, B
    visual_to_logical = [header.logicalIndex(v) for v in range(5)]
    assert visual_to_logical == [2, 3, 4, 0, 1]


def test_block_drag_moves_multi_select_to_drop_target(_qapp):
    """Simulate the multi-column drag flow end-to-end: select 2
    sections via the multi-select set, then call
    `_move_block_to_visual` (the same path the mouse-release handler
    uses). The selected block must land starting at the requested
    visual index, preserving internal relative order."""
    from trade_scanner_fh.gui.widgets import ReorderableHeader
    from PyQt6.QtGui import QStandardItemModel
    from PyQt6.QtWidgets import QTableView

    model = QStandardItemModel()
    model.setHorizontalHeaderLabels(["A", "B", "C", "D", "E", "F"])
    view = QTableView()
    header = ReorderableHeader(view)
    view.setModel(model)
    view.setHorizontalHeader(header)

    # Multi-select B (logical 1) + D (logical 3).
    header._selected_logical = {1, 3}
    # Drop them onto the visual position of E (logical 4 = visual 4).
    header._move_block_to_visual([1, 3], 4)

    # Block lands starting at visual 4: visual order should be
    #   A, C, E, F, B, D
    # because B/D moved out, leaving A/C/E/F shifted down, and
    # B/D land starting at visual index 4.
    visual_to_logical = [header.logicalIndex(v) for v in range(6)]
    # B and D should be at visuals 4 and 5 in their original
    # left-to-right order.
    assert visual_to_logical[4] == 1
    assert visual_to_logical[5] == 3
    # The non-target sections retain their relative order.
    non_targets = [
        visual_to_logical[i] for i in range(4)
        if visual_to_logical[i] not in (1, 3)
    ]
    assert non_targets == [0, 2, 4, 5]


def test_block_drag_clamps_target_to_valid_range(_qapp):
    """Dropping past the end clamps so the block fits within the
    visible columns — never drops sections off the right edge."""
    from trade_scanner_fh.gui.widgets import ReorderableHeader
    from PyQt6.QtGui import QStandardItemModel
    from PyQt6.QtWidgets import QTableView

    model = QStandardItemModel()
    model.setHorizontalHeaderLabels(["A", "B", "C", "D"])
    view = QTableView()
    header = ReorderableHeader(view)
    view.setModel(model)
    view.setHorizontalHeader(header)

    # Try to drop A and B (logical 0, 1) at visual 99 — should clamp
    # so the block ends at visual 3 (n - 2 = 2 starting position).
    header._move_block_to_visual([0, 1], 99)
    visual_to_logical = [header.logicalIndex(v) for v in range(4)]
    assert visual_to_logical == [2, 3, 0, 1]


def test_header_reset_restores_canonical_order(_qapp):
    from trade_scanner_fh.gui.widgets import ReorderableHeader
    from PyQt6.QtGui import QStandardItemModel
    from PyQt6.QtWidgets import QTableView

    model = QStandardItemModel()
    model.setHorizontalHeaderLabels(["A", "B", "C", "D"])
    view = QTableView()
    header = ReorderableHeader(view)
    view.setModel(model)
    view.setHorizontalHeader(header)

    # Scramble the order, then reset.
    header._move_block([3, 1], to_front=True)
    assert [header.logicalIndex(v) for v in range(4)] != [0, 1, 2, 3]
    header._reset_order()
    assert [header.logicalIndex(v) for v in range(4)] == [0, 1, 2, 3]


def test_results_table_emits_column_order_changed(_qapp):
    """ResultsTable.column_order_changed fires whenever a header
    reorder happens, carrying the new key order."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    df = pd.DataFrame([_row(
        "A",
        sti=1.1, dist_high_pct=2.0, max_gap_pct=5.0,
        max_gap_date=pd.Timestamp("2026-02-15"),
    )])
    table = ResultsTable()
    table.populate(df)

    captured: list = []
    table.column_order_changed.connect(lambda keys: captured.append(list(keys)))

    # Move the last column to the front via the header API.
    header = table.horizontalHeader()
    n = table.model_src.columnCount()
    last_logical = header.logicalIndex(n - 1)
    header._move_block([last_logical], to_front=True)

    assert captured  # at least one emission
    new_order = captured[-1]
    # First key now equals the originally-last column's key
    expected_first_key = table._active_columns[last_logical][1]
    assert new_order[0] == expected_first_key


def test_results_table_reapplies_saved_order_across_populate(_qapp):
    """The user's saved order must survive a populate() call (which
    happens on every timeframe switch)."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    df1 = pd.DataFrame([_row(
        "A", sti=1.1, dist_high_pct=2.0,
        max_gap_pct=5.0, max_gap_date=pd.Timestamp("2026-02-15"),
    )])
    table = ResultsTable()
    table.populate(df1)

    # Save an order that puts max_gap_pct first.
    table.set_saved_column_order(["max_gap_pct", "max_gap_date", "symbol"])
    # Re-populate with a fresh df; saved order should be re-applied.
    df2 = pd.DataFrame([_row(
        "B", sti=1.2, dist_high_pct=2.5,
        max_gap_pct=6.0, max_gap_date=pd.Timestamp("2026-03-15"),
    )])
    table.populate(df2)

    # Verify the visual order: max_gap_pct should be at visual 0.
    header = table.horizontalHeader()
    keys = [c[1] for c in table._active_columns]
    visual_to_key = [
        keys[header.logicalIndex(v)] for v in range(table.model_src.columnCount())
    ]
    assert visual_to_key[0] == "max_gap_pct"
    assert visual_to_key[1] == "max_gap_date"
    assert visual_to_key[2] == "symbol"


def test_current_column_order_returns_visual_order(_qapp):
    """`current_column_order` must reflect the user's reorder, not
    the canonical model order."""
    from trade_scanner_fh.gui.widgets import ResultsTable
    df = pd.DataFrame([_row(
        "A", sti=1.1, dist_high_pct=2.0,
        max_gap_pct=5.0, max_gap_date=pd.Timestamp("2026-02-15"),
    )])
    table = ResultsTable()
    table.populate(df)

    canonical = table.current_column_order()
    # Move "sti" to the front via the header.
    keys = [c[1] for c in table._active_columns]
    sti_logical = keys.index("sti")
    table.horizontalHeader()._move_block([sti_logical], to_front=True)
    new_order = table.current_column_order()
    assert new_order[0] == "sti"
    assert new_order != canonical


# ──────────────────────────────────────────────────────────────────────
# Excel export integration
# ──────────────────────────────────────────────────────────────────────

def test_main_window_export_uses_reordered_columns(_qapp, tmp_path,
                                                     monkeypatch):
    """`_ordered_active_columns_for_export` rearranges the table's
    active columns according to the saved order, so Excel sheets
    match the on-screen layout. Columns not in the saved order
    fall to the end in their canonical positions."""
    from trade_scanner_fh import config as cfg
    from trade_scanner_fh.gui.main_window import MainWindow
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)

    instance = MainWindow.__new__(MainWindow)
    fake_active = [
        ("A", "ka", str), ("B", "kb", str), ("C", "kc", str),
        ("D", "kd", str), ("E", "ke", str),
    ]
    fake_table = MagicMock()
    fake_table.active_columns = fake_active
    instance.results_table = fake_table

    # No saved order → canonical
    instance._results_column_order = []
    assert instance._ordered_active_columns_for_export() == fake_active

    # Saved order moves kc, ka to front; others to end in canonical order
    instance._results_column_order = ["kc", "ka"]
    out = instance._ordered_active_columns_for_export()
    out_keys = [c[1] for c in out]
    assert out_keys == ["kc", "ka", "kb", "kd", "ke"]

    # Saved order with a stale key (no longer in active_columns) is
    # silently skipped — robust to filters being toggled on/off
    # between scans.
    instance._results_column_order = ["kc", "STALE_KEY", "ka"]
    out = instance._ordered_active_columns_for_export()
    out_keys = [c[1] for c in out]
    assert out_keys == ["kc", "ka", "kb", "kd", "ke"]
