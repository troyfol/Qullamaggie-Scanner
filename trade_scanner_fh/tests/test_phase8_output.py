"""Tests for Phase 8 — per-quarter earnings columns in result table,
dynamic Q-i beats columns, green-highlight metadata, and the export
dialog's grouped beats toggle."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from trade_scanner_fh.gui.widgets import (
    RESULT_COLUMNS, DATE_COLUMN_KEYS, _build_dynamic_columns,
)


@pytest.fixture(scope="module")
def _qapp():
    from PyQt6.QtWidgets import QApplication
    import sys
    app = QApplication.instance() or QApplication(sys.argv[:1])
    yield app


def _build_row(symbol: str, **extras) -> dict:
    base = {
        "symbol": symbol,
        "close": 100.0, "price": 100.0, "pct_gain": 25.0,
    }
    base.update(extras)
    return base


# ──────────────────────────────────────────────────────────────────────
# RESULT_COLUMNS — single-window earnings cluster
# ──────────────────────────────────────────────────────────────────────

def test_result_columns_includes_seven_earnings_columns():
    """The seven §8.2 single-window columns must be present in the
    base RESULT_COLUMNS list, in spec order, immediately after Days
    Until ER."""
    keys = [k for _h, k, _f in RESULT_COLUMNS]
    expected = [
        "reported_eps", "surprise_eps_dollar", "surprise_eps_pct",
        "reported_rev", "surprise_rev_dollar", "surprise_rev_pct",
        "last_report_date",
    ]
    for k in expected:
        assert k in keys, f"missing {k} from RESULT_COLUMNS"
    # Order check: each subsequent earnings col index must be greater
    indices = [keys.index(k) for k in expected]
    assert indices == sorted(indices)
    # And they must come after days_until_er
    assert keys.index("days_until_er") < indices[0]


def test_last_report_date_is_a_date_column():
    """News-column toggle scans DATE_COLUMN_KEYS — last_report_date
    must be in there per §8.4."""
    assert "last_report_date" in DATE_COLUMN_KEYS


# ──────────────────────────────────────────────────────────────────────
# _build_dynamic_columns — beats-aware column extension
# ──────────────────────────────────────────────────────────────────────

def test_no_beats_columns_when_no_streak_data():
    """A vanilla scan frame (no consec_*_beats columns) → no Q-i cols.
    Filter columns whose key isn't in the DataFrame are also dropped
    (2026-05 update: hide unselected filter columns), so the column
    count is shorter than the canonical RESULT_COLUMNS list."""
    df = pd.DataFrame([_build_row("AAPL"), _build_row("MSFT")])
    cols, n_eps, n_rev = _build_dynamic_columns(df)
    assert n_eps == 0
    assert n_rev == 0
    keys = [c[1] for c in cols]
    # Always-visible keys present
    assert "symbol" in keys
    assert "close" in keys
    assert "pct_gain" in keys
    # Disabled-filter keys absent
    assert "sti" not in keys
    assert "consec_gaps" not in keys
    assert "rs_market" not in keys


def test_eps_beats_extends_columns_to_max_streak():
    """N = max consec_eps_beats across surviving rows."""
    df = pd.DataFrame([
        _build_row("A", consec_eps_beats=3,
                   q1_reported_eps=2.0, q1_surprise_eps_dollar=0.1,
                   q1_surprise_eps_pct=5.0,
                   q2_reported_eps=1.9, q2_surprise_eps_dollar=0.05,
                   q2_surprise_eps_pct=3.0,
                   q3_reported_eps=1.7, q3_surprise_eps_dollar=0.02,
                   q3_surprise_eps_pct=1.5),
        _build_row("B", consec_eps_beats=5,
                   q1_reported_eps=3.0, q1_surprise_eps_dollar=0.2,
                   q1_surprise_eps_pct=8.0,
                   q2_reported_eps=2.8, q2_surprise_eps_dollar=0.18,
                   q2_surprise_eps_pct=7.0,
                   q3_reported_eps=2.5, q3_surprise_eps_dollar=0.1,
                   q3_surprise_eps_pct=4.5,
                   q4_reported_eps=2.2, q4_surprise_eps_dollar=0.08,
                   q4_surprise_eps_pct=3.0,
                   q5_reported_eps=2.0, q5_surprise_eps_dollar=0.05,
                   q5_surprise_eps_pct=2.5),
    ])
    cols, n_eps, n_rev = _build_dynamic_columns(df)
    assert n_eps == 5
    assert n_rev == 0
    keys = [k for _h, k, _f in cols]
    # Consec column appears once
    assert keys.count("consec_eps_beats") == 1
    # All Q-1..Q-5 EPS triplets present
    for k in range(1, 6):
        assert f"q{k}_reported_eps" in keys
        assert f"q{k}_surprise_eps_dollar" in keys
        assert f"q{k}_surprise_eps_pct" in keys
    # Q-6 NOT extended past max streak
    assert "q6_reported_eps" not in keys


def test_max_streak_capped_to_columns_present():
    """When streak claims N=5 but only Q-1..Q-3 columns are populated,
    cap at 3 so we don't render empty headers."""
    df = pd.DataFrame([
        _build_row("A", consec_eps_beats=5,
                   q1_reported_eps=2.0, q1_surprise_eps_dollar=0.1, q1_surprise_eps_pct=5.0,
                   q2_reported_eps=1.9, q2_surprise_eps_dollar=0.05, q2_surprise_eps_pct=3.0,
                   q3_reported_eps=1.7, q3_surprise_eps_dollar=0.02, q3_surprise_eps_pct=1.5),
    ])
    cols, n_eps, _ = _build_dynamic_columns(df)
    assert n_eps == 3
    keys = [k for _h, k, _f in cols]
    assert "q3_reported_eps" in keys
    assert "q4_reported_eps" not in keys


def test_both_eps_and_rev_blocks_render_side_by_side():
    """Per §8.3: EPS block on the left, Rev block to its right when
    both are active."""
    df = pd.DataFrame([_build_row(
        "A",
        consec_eps_beats=2, consec_rev_beats=3,
        q1_reported_eps=1.0, q1_surprise_eps_dollar=0.05, q1_surprise_eps_pct=2.0,
        q2_reported_eps=0.9, q2_surprise_eps_dollar=0.02, q2_surprise_eps_pct=1.0,
        q1_reported_rev=100.0, q1_surprise_rev_dollar=2.0, q1_surprise_rev_pct=2.0,
        q2_reported_rev=98.0, q2_surprise_rev_dollar=1.5, q2_surprise_rev_pct=1.5,
        q3_reported_rev=95.0, q3_surprise_rev_dollar=1.0, q3_surprise_rev_pct=1.0,
    )])
    cols, n_eps, n_rev = _build_dynamic_columns(df)
    assert n_eps == 2
    assert n_rev == 3
    keys = [k for _h, k, _f in cols]
    eps_idx = keys.index("consec_eps_beats")
    rev_idx = keys.index("consec_rev_beats")
    assert eps_idx < rev_idx, "EPS block must precede Rev block"


# ──────────────────────────────────────────────────────────────────────
# ResultsTable green highlighting — Phase 8 §8.3
# ──────────────────────────────────────────────────────────────────────

def test_results_table_green_highlight_inside_streak(_qapp):
    """Cells for Q-1..Q-{this_row's_streak} must carry the streak-green
    foreground color; later quarters must use the default. N is the
    longest streak across the result set, so a row with a shorter
    streak still has its later columns rendered in default color."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    df = pd.DataFrame([
        # Row 0: streak=2 — Q-1, Q-2 should be green; Q-3 default.
        _build_row(
            "A", consec_eps_beats=2,
            q1_reported_eps=2.0, q1_surprise_eps_dollar=0.1, q1_surprise_eps_pct=5.0,
            q2_reported_eps=1.9, q2_surprise_eps_dollar=0.05, q2_surprise_eps_pct=3.0,
            q3_reported_eps=1.7, q3_surprise_eps_dollar=0.02, q3_surprise_eps_pct=1.5,
        ),
        # Row 1: streak=3 — drives N to 3 so Q-3 column appears.
        _build_row(
            "B", consec_eps_beats=3,
            q1_reported_eps=3.0, q1_surprise_eps_dollar=0.2, q1_surprise_eps_pct=8.0,
            q2_reported_eps=2.8, q2_surprise_eps_dollar=0.18, q2_surprise_eps_pct=7.0,
            q3_reported_eps=2.5, q3_surprise_eps_dollar=0.1, q3_surprise_eps_pct=4.5,
        ),
    ])
    table = ResultsTable()
    table.populate(df)

    cols = table.active_columns
    headers = [c[0] for c in cols]
    q1_idx = headers.index("Q-1 Reported EPS")
    q3_idx = headers.index("Q-3 Reported EPS")

    # Row 0 (streak=2): Q-1 green, Q-3 default
    a_q1 = table.model_src.item(0, q1_idx)
    a_q3 = table.model_src.item(0, q3_idx)
    assert a_q1.foreground().color() == ResultsTable._STREAK_GREEN
    assert a_q3.foreground().color() != ResultsTable._STREAK_GREEN

    # Row 1 (streak=3): Q-3 also green
    b_q3 = table.model_src.item(1, q3_idx)
    assert b_q3.foreground().color() == ResultsTable._STREAK_GREEN


def test_results_table_no_dynamic_columns_for_vanilla_scan(_qapp):
    """A vanilla df produces a table with no Q-i headers and no
    disabled-filter columns. The 2026-05 update filters
    RESULT_COLUMNS down to keys actually present in the DataFrame."""
    from trade_scanner_fh.gui.widgets import ResultsTable
    df = pd.DataFrame([_build_row("A"), _build_row("B")])
    table = ResultsTable()
    table.populate(df)
    keys = [c[1] for c in table.active_columns]
    assert "symbol" in keys
    assert "close" in keys
    # No Q-i columns expected
    assert not any(k.startswith("q") and "_report_date" in k for k in keys)
    assert table.active_beats_quarters == (0, 0)


# ──────────────────────────────────────────────────────────────────────
# ExcelExportDialog — grouped beats toggle (§8.4)
# ──────────────────────────────────────────────────────────────────────

def test_export_dialog_groups_q_columns(_qapp):
    """When the columns include q* keys, the dialog must NOT render
    individual checkboxes for each one — only a single grouped toggle."""
    from trade_scanner_fh.gui.dialogs import ExcelExportDialog

    cols = list(RESULT_COLUMNS)
    cols.extend([
        ("Consec EPS Beats", "consec_eps_beats", lambda x: str(int(x))),
        ("Q-1 Reported EPS", "q1_reported_eps", lambda x: f"{x:.2f}"),
        ("Q-1 Surp EPS $",   "q1_surprise_eps_dollar", lambda x: f"{x:+.2f}"),
        ("Q-1 Surp EPS %",   "q1_surprise_eps_pct", lambda x: f"{x:+.2f}%"),
        ("Q-2 Reported EPS", "q2_reported_eps", lambda x: f"{x:.2f}"),
        ("Q-2 Surp EPS $",   "q2_surprise_eps_dollar", lambda x: f"{x:+.2f}"),
        ("Q-2 Surp EPS %",   "q2_surprise_eps_pct", lambda x: f"{x:+.2f}%"),
    ])

    dlg = ExcelExportDialog(cols, periods=["a"])

    # Individual q* keys must NOT have a per-key checkbox
    for q_key in ("q1_reported_eps", "q1_surprise_eps_dollar",
                  "q1_surprise_eps_pct", "q2_reported_eps"):
        assert q_key not in dlg._checks, (
            f"{q_key} should be folded into the group toggle"
        )

    # The grouped EPS checkbox should exist and be pre-checked
    # (everything starts checked since the auto-data preselect was
    # dropped 2026-05).
    assert dlg._group_eps_checkbox is not None
    assert dlg._group_eps_checkbox.isChecked()
    # Selected_keys must expand the group back into all q* keys
    selected = dlg.selected_keys()
    for k in ("q1_reported_eps", "q1_surprise_eps_dollar",
              "q1_surprise_eps_pct", "q2_reported_eps"):
        assert k in selected


def test_export_dialog_select_none_clears_group(_qapp):
    from trade_scanner_fh.gui.dialogs import ExcelExportDialog
    cols = list(RESULT_COLUMNS) + [
        ("Q-1 Reported EPS", "q1_reported_eps", lambda x: f"{x:.2f}"),
        ("Q-1 Surp EPS $", "q1_surprise_eps_dollar", lambda x: f"{x:+.2f}"),
    ]
    dlg = ExcelExportDialog(cols, periods=["a"])
    assert dlg._group_eps_checkbox.isChecked()
    dlg._select_none()
    assert not dlg._group_eps_checkbox.isChecked()
    assert "q1_reported_eps" not in dlg.selected_keys()
    dlg._select_all()
    assert dlg._group_eps_checkbox.isChecked()
    assert "q1_reported_eps" in dlg.selected_keys()
