"""Tests for the per-quarter date column added to the LEFT of every
beats triplet (EPS and Rev), and its blue/green highlighting."""
from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture(scope="module")
def _qapp():
    from PyQt6.QtWidgets import QApplication
    import sys
    app = QApplication.instance() or QApplication(sys.argv[:1])
    yield app


def _row(symbol: str, **extras) -> dict:
    base = {
        "symbol": symbol,
        "close": 100.0, "price": 100.0, "pct_gain": 25.0,
    }
    base.update(extras)
    return base


# ──────────────────────────────────────────────────────────────────────
# Column layout
# ──────────────────────────────────────────────────────────────────────

def test_eps_block_has_date_column_left_of_each_triplet():
    """Q-i Date should sit immediately left of Q-i Reported EPS."""
    from trade_scanner_fh.gui.widgets import _build_dynamic_columns

    df = pd.DataFrame([_row(
        "A", consec_eps_beats=2,
        q1_report_date_eps=pd.Timestamp("2026-02-15"),
        q1_reported_eps=2.0, q1_surprise_eps_dollar=0.1, q1_surprise_eps_pct=5.0,
        q2_report_date_eps=pd.Timestamp("2025-11-15"),
        q2_reported_eps=1.9, q2_surprise_eps_dollar=0.05, q2_surprise_eps_pct=3.0,
    )])
    cols, n_eps, n_rev = _build_dynamic_columns(df)
    headers = [c[0] for c in cols]
    keys = [c[1] for c in cols]

    # Q-1 Date must come immediately before Q-1 Reported EPS.
    q1_date_idx = headers.index("Q-1 Date")
    q1_rep_idx = headers.index("Q-1 Reported EPS")
    assert q1_date_idx + 1 == q1_rep_idx
    assert keys[q1_date_idx] == "q1_report_date_eps"

    q2_date_idx = headers.index("Q-2 Date", q1_rep_idx)
    q2_rep_idx = headers.index("Q-2 Reported EPS", q2_date_idx)
    assert q2_date_idx + 1 == q2_rep_idx
    assert keys[q2_date_idx] == "q2_report_date_eps"


def test_rev_block_has_date_column_left_of_each_triplet():
    from trade_scanner_fh.gui.widgets import _build_dynamic_columns

    df = pd.DataFrame([_row(
        "A", consec_rev_beats=2,
        q1_report_date_rev=pd.Timestamp("2026-02-15"),
        q1_reported_rev=100.0, q1_surprise_rev_dollar=2.0, q1_surprise_rev_pct=2.0,
        q2_report_date_rev=pd.Timestamp("2025-11-15"),
        q2_reported_rev=98.0, q2_surprise_rev_dollar=1.5, q2_surprise_rev_pct=1.5,
    )])
    cols, _, n_rev = _build_dynamic_columns(df)
    headers = [c[0] for c in cols]
    keys = [c[1] for c in cols]

    q1_date_idx = headers.index("Q-1 Date")
    q1_rep_idx = headers.index("Q-1 Reported Rev")
    assert q1_date_idx + 1 == q1_rep_idx
    assert keys[q1_date_idx] == "q1_report_date_rev"


def test_both_blocks_get_their_own_date_column(_qapp):
    """When both EPS + Rev beats are active, EACH block has its own
    Q-i Date column with distinct keys (`_eps` vs `_rev`) so the
    green-streak coloring can apply per-block."""
    from trade_scanner_fh.gui.widgets import _build_dynamic_columns

    df = pd.DataFrame([_row(
        "A",
        consec_eps_beats=1, consec_rev_beats=1,
        q1_report_date_eps=pd.Timestamp("2026-02-15"),
        q1_reported_eps=2.0, q1_surprise_eps_dollar=0.1, q1_surprise_eps_pct=5.0,
        q1_report_date_rev=pd.Timestamp("2026-02-15"),
        q1_reported_rev=100.0, q1_surprise_rev_dollar=2.0, q1_surprise_rev_pct=2.0,
    )])
    cols, n_eps, n_rev = _build_dynamic_columns(df)
    keys = [c[1] for c in cols]
    # Both date columns present, distinct keys
    assert "q1_report_date_eps" in keys
    assert "q1_report_date_rev" in keys


# ──────────────────────────────────────────────────────────────────────
# Green-streak highlighting on Q-i Date cells
# ──────────────────────────────────────────────────────────────────────

def test_q_date_within_eps_streak_renders_green(_qapp):
    """Two rows with different streak lengths drive `n_eps = max(streaks)`
    so the column block goes up to the longest. Row 0's Q-3 Date is
    past its own streak boundary (streak=2 < 3) → default color.
    Row 1's Q-3 Date is inside its streak → green."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    df = pd.DataFrame([
        _row(
            "A", consec_eps_beats=2,
            q1_report_date_eps=pd.Timestamp("2026-02-15"),
            q1_reported_eps=2.0, q1_surprise_eps_dollar=0.1, q1_surprise_eps_pct=5.0,
            q2_report_date_eps=pd.Timestamp("2025-11-15"),
            q2_reported_eps=1.9, q2_surprise_eps_dollar=0.05, q2_surprise_eps_pct=3.0,
            q3_report_date_eps=pd.Timestamp("2025-08-15"),
            q3_reported_eps=1.7, q3_surprise_eps_dollar=0.02, q3_surprise_eps_pct=1.5,
        ),
        _row(
            "B", consec_eps_beats=3,
            q1_report_date_eps=pd.Timestamp("2026-02-20"),
            q1_reported_eps=3.0, q1_surprise_eps_dollar=0.2, q1_surprise_eps_pct=8.0,
            q2_report_date_eps=pd.Timestamp("2025-11-20"),
            q2_reported_eps=2.8, q2_surprise_eps_dollar=0.15, q2_surprise_eps_pct=5.0,
            q3_report_date_eps=pd.Timestamp("2025-08-20"),
            q3_reported_eps=2.5, q3_surprise_eps_dollar=0.10, q3_surprise_eps_pct=3.0,
        ),
    ])
    table = ResultsTable()
    table.populate(df)
    headers = [c[0] for c in table.active_columns]

    q1_date_idx = headers.index("Q-1 Date")
    q3_date_idx = headers.index("Q-3 Date", q1_date_idx + 1)

    # Row 0 (streak=2): Q-1 Date in-streak → green, Q-3 Date past → default
    a_q1 = table.model_src.item(0, q1_date_idx)
    a_q3 = table.model_src.item(0, q3_date_idx)
    assert a_q1.foreground().color() == ResultsTable._STREAK_GREEN
    assert a_q3.foreground().color() != ResultsTable._STREAK_GREEN
    # Row 1 (streak=3): Q-3 Date in-streak → green
    b_q3 = table.model_src.item(1, q3_date_idx)
    assert b_q3.foreground().color() == ResultsTable._STREAK_GREEN


def test_q_date_alignment_wins_over_streak_green_when_indicator_aligned(_qapp):
    """Spec: earnings-alignment color takes precedence over green
    (in-streak) when both apply to the same Q-i Date cell. The
    alignment color is per-match randomized from the palette — the
    contract is "any palette entry, NOT the streak-green", which
    proves alignment wins over the streak-green ForegroundRole."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    df = pd.DataFrame([_row(
        "A", consec_eps_beats=2,
        q1_report_date_eps=pd.Timestamp("2026-02-15"),
        q1_reported_eps=2.0, q1_surprise_eps_dollar=0.1, q1_surprise_eps_pct=5.0,
        q2_report_date_eps=pd.Timestamp("2025-11-15"),
        q2_reported_eps=1.9, q2_surprise_eps_dollar=0.05, q2_surprise_eps_pct=3.0,
        # Q-1's date is also flagged as earnings-aligned
        _earnings_aligned_dates=["2026-02-15"],
    )])
    table = ResultsTable()
    table.populate(df)
    headers = [c[0] for c in table.active_columns]
    q1_date_idx = headers.index("Q-1 Date")
    item = table.model_src.item(0, q1_date_idx)
    c = item.foreground().color()
    palette_rgbs = {(p.red(), p.green(), p.blue())
                    for p in ResultsTable._ALIGN_PALETTE}
    # In-streak AND aligned → alignment palette color wins over green
    assert (c.red(), c.green(), c.blue()) in palette_rgbs
    sg = ResultsTable._STREAK_GREEN
    assert (c.red(), c.green(), c.blue()) != (sg.red(), sg.green(), sg.blue())


def test_eps_streak_does_not_color_rev_block_dates(_qapp):
    """Per-block date keys mean a row's EPS streak doesn't bleed into
    its Rev block's Q-i Date cells. Two rows: row 0 has eps_streak=1
    rev_streak=0; row 1 has eps_streak=0 rev_streak=1 (drives
    n_rev=1 so the Rev block renders at all)."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    df = pd.DataFrame([
        _row(
            "A", consec_eps_beats=1, consec_rev_beats=0,
            q1_report_date_eps=pd.Timestamp("2026-02-15"),
            q1_reported_eps=2.0, q1_surprise_eps_dollar=0.1, q1_surprise_eps_pct=5.0,
            q1_report_date_rev=pd.Timestamp("2026-02-15"),
            q1_reported_rev=100.0, q1_surprise_rev_dollar=2.0, q1_surprise_rev_pct=2.0,
        ),
        _row(
            "B", consec_eps_beats=0, consec_rev_beats=1,
            q1_report_date_eps=pd.Timestamp("2026-02-20"),
            q1_reported_eps=3.0, q1_surprise_eps_dollar=0.2, q1_surprise_eps_pct=8.0,
            q1_report_date_rev=pd.Timestamp("2026-02-20"),
            q1_reported_rev=110.0, q1_surprise_rev_dollar=5.0, q1_surprise_rev_pct=5.0,
        ),
    ])
    table = ResultsTable()
    table.populate(df)

    keys = [c[1] for c in table.active_columns]
    eps_date_idx = keys.index("q1_report_date_eps")
    rev_date_idx = keys.index("q1_report_date_rev")

    # Row 0: eps_streak=1 → EPS-block Q-1 Date green; rev_streak=0 →
    # Rev-block Q-1 Date default color (per-block isolation)
    a_eps = table.model_src.item(0, eps_date_idx)
    a_rev = table.model_src.item(0, rev_date_idx)
    assert a_eps.foreground().color() == ResultsTable._STREAK_GREEN
    assert a_rev.foreground().color() != ResultsTable._STREAK_GREEN

    # Row 1: eps_streak=0 → EPS-block default; rev_streak=1 → Rev
    # green. Confirms each block's coloring respects ITS OWN streak.
    b_eps = table.model_src.item(1, eps_date_idx)
    b_rev = table.model_src.item(1, rev_date_idx)
    assert b_eps.foreground().color() != ResultsTable._STREAK_GREEN
    assert b_rev.foreground().color() == ResultsTable._STREAK_GREEN
