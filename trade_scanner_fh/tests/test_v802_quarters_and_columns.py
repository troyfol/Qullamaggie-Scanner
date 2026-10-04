"""v8.0.2 — shared quarter-block count, and new columns to the right.

User decisions (2026-10-02) these tests pin:
  * Quarter blocks: with several quarter filters active (display-only
    included), the HIGHEST Q Cap decides how many Q-X blocks are shown, and
    no cap (0) beats any number; one count for the EPS and Rev blocks; never
    more than 20. Display only — each filter still evaluates over its own
    cap (pinned end to end in test_display_only.py).
  * Columns: when filters are added, their columns go to the RIGHT of what
    the scan already showed; removed filters' columns drop out; extra Q-X
    blocks on a side already shown extend that run instead.
"""
from __future__ import annotations

import pandas as pd
import pytest

from trade_scanner_fh import scanner as S
from trade_scanner_fh.gui.columns import merge_new_columns_right

# ======================================================================
# q_block_count
# ======================================================================

_ALL_CAP_ROWS = ["consec_eps_beats", "consec_rev_beats", "consec_eps_growth",
                 "consec_rev_growth", "accel_eps_surp", "accel_rev_surp",
                 "accel_eps_yoy", "accel_rev_yoy"]


def _params(**rows):
    """`rows`: prefix → cap (enabled) or ("display", cap) (display-only)."""
    kw = {}
    for prefix, spec in rows.items():
        mode, cap = spec if isinstance(spec, tuple) else ("enabled", spec)
        kw[f"{prefix}_{'display_only' if mode == 'display' else 'enabled'}"] = True
        kw[f"{prefix}_quarter_cap"] = cap
    return S.ScanParams(**kw)


def test_every_q_cap_row_is_counted():
    assert sorted(S._Q_CAP_FILTERS) == sorted(_ALL_CAP_ROWS)


def test_no_quarter_filter_draws_no_blocks():
    assert S.q_block_count(S.ScanParams()) == 0


@pytest.mark.parametrize("rows, want", [
    ({"consec_eps_beats": 4}, 4),
    ({"consec_eps_beats": 4, "consec_rev_growth": 8}, 8),        # highest
    ({"consec_eps_beats": 8, "consec_rev_beats": 2}, 8),
    ({"consec_eps_beats": 4, "consec_eps_growth": 0}, 20),       # 0 = no cap
    ({"consec_eps_beats": 4, "accel_rev_yoy": ("display", 0)}, 20),
    ({"consec_eps_beats": 4, "consec_rev_beats": ("display", 12)}, 12),
    ({"consec_eps_growth": 35}, 20),                             # ceiling
    ({"accel_eps_surp": 6}, 6),
])
def test_highest_cap_wins_and_no_cap_beats_every_number(rows, want):
    assert S.q_block_count(_params(**rows)) == want


def test_a_switched_off_rows_cap_is_ignored():
    p = _params(consec_eps_beats=4)
    p.consec_eps_growth_quarter_cap = 0       # set, but the row is off
    assert S.q_block_count(p) == 4


def test_beats_and_growth_draw_the_shared_count_on_both_sides():
    """Straight through `_populate_quarter_series`: a Rev growth row capped
    at 2 draws 5 Rev blocks when the EPS growth row is capped at 5."""
    hist = pd.DataFrame({
        "period_ending": pd.date_range("2024-03-01", periods=8, freq="QS")[::-1],
        "report_date": pd.date_range("2024-05-01", periods=8, freq="QS")[::-1],
        "reported_eps": 1.0, "surprise_eps": 0.1, "surprise_eps_pct": 5.0,
        "yoy_eps_pct": 10.0, "reported_rev": 100.0, "surprise_rev": 1.0,
        "surprise_rev_pct": 1.0, "yoy_rev_pct": 10.0,
    })
    row: dict = {}
    S._populate_quarter_series(row, _params(consec_eps_growth=5,
                                            consec_rev_growth=2), hist)
    for side in ("eps", "rev"):
        assert f"q5_reported_{side}" in row and f"q6_reported_{side}" not in row


# ======================================================================
# merge_new_columns_right
# ======================================================================

def _block(k, side="eps"):
    return [f"q{k}_report_date_{side}", f"q{k}_reported_{side}",
            f"q{k}_surprise_{side}_dollar", f"q{k}_surprise_{side}_pct",
            f"q{k}_yoy_{side}_pct"]


def test_new_filter_columns_go_right_even_when_their_panel_row_is_left():
    base = ["symbol", "close", "adr_pct"]
    canon = ["symbol", "close", "sti", "adr_pct", "hv"]
    assert merge_new_columns_right(base, canon) == \
        ["symbol", "close", "adr_pct", "sti", "hv"]


def test_removed_filters_drop_out_and_the_rest_keep_their_places():
    base = ["adr_pct", "symbol", "sti", "close"]
    assert merge_new_columns_right(base, ["symbol", "close", "adr_pct"]) == \
        ["adr_pct", "symbol", "close"]


def test_extra_quarter_blocks_extend_the_run_not_the_right_edge():
    base = ["symbol", "consec_eps_beats", *_block(1), *_block(2), "sti"]
    canon = ["symbol", "sti", "consec_eps_beats",
             *_block(1), *_block(2), *_block(3), *_block(4)]
    assert merge_new_columns_right(base, canon) == \
        ["symbol", "consec_eps_beats", *_block(1), *_block(2),
         *_block(3), *_block(4), "sti"]


def test_interleaved_blocks_extend_in_interleaved_order():
    run = [*_block(1), *_block(1, "rev"), *_block(2), *_block(2, "rev")]
    base = ["symbol", *run, "sti"]
    canon = ["symbol", "sti", *run, *_block(3), *_block(3, "rev")]
    assert merge_new_columns_right(base, canon) == \
        ["symbol", *run, *_block(3), *_block(3, "rev"), "sti"]


def test_a_side_shown_for_the_first_time_is_new_and_goes_right():
    base = ["symbol", *_block(1), *_block(2), "sti"]
    canon = ["symbol", "sti", *_block(1), *_block(2),
             "consec_rev_beats", *_block(1, "rev"), *_block(2, "rev")]
    assert merge_new_columns_right(base, canon) == \
        base + ["consec_rev_beats", *_block(1, "rev"), *_block(2, "rev")]


@pytest.mark.parametrize("interleave", [False, True])
def test_a_new_side_without_a_counter_still_goes_right(interleave):
    """A Rev Growth row has no counter in front of its blocks, so its Q-1 Rev
    block directly follows an EPS block in the canonical layout. It is still
    a new filter's columns: right edge, not spliced into the EPS run."""
    eps = [*_block(1), *_block(2)]
    rev = [*_block(1, "rev"), *_block(2, "rev")]
    base = ["symbol", *eps, "sti"]
    blocks = ([*_block(1), *_block(1, "rev"), *_block(2), *_block(2, "rev")]
              if interleave else eps + rev)
    canon = ["symbol", "sti", "consec_rev_growth", *blocks]
    got = merge_new_columns_right(base, canon)
    assert got[:len(base)] == base
    assert sorted(got[len(base):]) == sorted(["consec_rev_growth", *rev])


def test_nothing_on_screen_gives_the_canonical_order():
    assert merge_new_columns_right([], ["a", "b", "c"]) == ["a", "b", "c"]


# ======================================================================
# End to end — the real window, scan after scan
# ======================================================================

@pytest.fixture
def window(_qapp, tmp_parquets, monkeypatch):
    from trade_scanner_fh.gui import main_window as mw_mod
    monkeypatch.setattr(mw_mod, "PRESETS_DIR", tmp_parquets / "presets")
    (tmp_parquets / "presets").mkdir(exist_ok=True)
    monkeypatch.setattr(mw_mod.scan_history, "record_scan_results",
                        lambda *a, **k: {})
    w = mw_mod.MainWindow()
    yield w
    w.close()
    w.deleteLater()


def _frame(extra=(), n_q=0):
    row = {"symbol": "AAA", "close": 10.0, "pct_gain": 1.0,
           "gain_start_date": pd.Timestamp("2026-09-01")}
    row.update({k: 1.0 for k in extra})
    for k in range(1, n_q + 1):
        row.update(dict(zip(_block(k), (pd.Timestamp("2026-08-01"), 1.0,
                                        0.1, 5.0, 10.0))))
    return pd.DataFrame([row])


def _scan(window, df):
    from trade_scanner_fh.gui.workers import WorkerScanResult
    window._on_scan_done(WorkerScanResult(period_results={"1D": df},
                                          period_order=["1D"]))
    return window.results_table.current_column_order()


def test_scans_add_columns_on_the_right_and_keep_the_layout(window):
    # 1. First scan, never reordered: the canonical layout.
    first = _scan(window, _frame(["adr_pct", "consec_eps_beats"], n_q=2))
    assert first.index("adr_pct") < first.index("consec_eps_beats")
    assert first[-5:] == _block(2)

    # 2. STI and HV switched on (STI's panel row sits LEFT of ADR%), and a
    #    third quarter: STI / HV land at the right edge, Q-3 after Q-2.
    second = _scan(window, _frame(["adr_pct", "consec_eps_beats", "sti", "hv"],
                                  n_q=3))
    assert second == first + _block(3) + ["sti", "hv"]

    # 3. A scan with no results leaves the layout alone (it used to wipe a
    #    saved order down to the always-visible columns).
    _scan(window, _frame()[0:0])
    assert window._results_column_order == second

    # 4. ADR% switched off, a fourth quarter: ADR% drops out, Q-4 extends
    #    the run — in front of STI / HV, which were added after Q-3.
    fourth = _scan(window, _frame(["consec_eps_beats", "sti", "hv"], n_q=4))
    expected = [k for k in second if k != "adr_pct"]
    cut = expected.index(_block(3)[-1]) + 1
    assert fourth == expected[:cut] + _block(4) + expected[cut:]


def test_after_reset_the_next_new_column_still_goes_right(window):
    _scan(window, _frame(["adr_pct", "sti"]))
    window._reset_columns_to_default()
    shown = window.results_table.current_column_order()
    after = _scan(window, _frame(["adr_pct", "sti", "dist_high_pct"]))
    assert after == shown + ["dist_high_pct"]


def test_a_dragged_layout_keeps_its_order_and_takes_new_columns_right(window):
    shown = _scan(window, _frame(["adr_pct", "sti"]))
    dragged = ["sti"] + [k for k in shown if k != "sti"]   # STI to the front
    window._on_results_column_order_changed(dragged)
    after = _scan(window, _frame(["adr_pct", "sti", "hv"]))
    assert after == dragged + ["hv"]
