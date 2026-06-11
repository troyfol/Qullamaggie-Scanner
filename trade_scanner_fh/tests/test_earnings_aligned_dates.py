"""Tests for the earnings-aligned date highlight (`_earnings_aligned_dates`).

When an indicator date column (gap / surge / gain / etc.) matches one
of a ticker's earnings report dates, both cells render in blue.
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd

from trade_scanner_fh import config, scanner
from trade_scanner_fh.scanner import ScanParams


def _hist_row(ticker, period_str, report_str, eps_pct=5.0, rev_pct=5.0):
    return {
        "ticker": ticker,
        "period_ending": pd.Timestamp(period_str),
        "report_date": pd.Timestamp(report_str),
        "report_time": "Close",
        "estimated_eps": 2.0, "reported_eps": 2.1,
        "surprise_eps": 0.1, "surprise_eps_pct": eps_pct,
        "estimated_rev": 100.0, "reported_rev": 105.0,
        "surprise_rev": 5.0, "surprise_rev_pct": rev_pct,
        "source": "zacks",
        "updated_at": pd.Timestamp(datetime.now()),
    }


def _write_ohlcv_with_jump(symbol, end, jump_date, jump_pct=15.0):
    """Write a synthetic OHLCV parquet for `symbol` ending at `end`,
    with one big gap-up day at `jump_date` so the max-gap indicator
    has something to land on."""
    idx = pd.bdate_range(end=end, periods=200)
    base = 100.0
    closes = [base] * len(idx)
    opens = [base] * len(idx)
    # Find the bar matching jump_date
    target = pd.Timestamp(jump_date)
    for i, ts in enumerate(idx):
        if ts.normalize() == target.normalize() and i > 0:
            # Gap up: open >> prior close
            opens[i] = base * (1 + jump_pct / 100.0)
            closes[i] = base * (1 + jump_pct / 100.0)
            break
    df = pd.DataFrame({
        "Open": opens,
        "High": [o * 1.01 for o in opens],
        "Low": [o * 0.99 for o in opens],
        "Close": closes,
        "Volume": [1_000_000] * len(idx),
    }, index=idx)
    df.to_parquet(config.PARQUET_DIR / f"{symbol}.parquet")


# ──────────────────────────────────────────────────────────────────────
# Compute-time: indicator date that matches a report date is captured
# ──────────────────────────────────────────────────────────────────────

def test_max_gap_date_on_earnings_day_marks_aligned(fake_scan_cache):
    """A max-gap-up that lands on an earnings report date for that
    ticker must end up in `_earnings_aligned_dates`."""
    end = pd.Timestamp(date(2026, 4, 30))
    earnings_day = "2026-02-17"  # Tuesday — must be a weekday
    _write_ohlcv_with_jump("TKR", end, jump_date=earnings_day, jump_pct=15.0)
    pd.DataFrame([
        _hist_row("TKR", "2026-01-31", earnings_day, eps_pct=10.0),
    ]).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    p = ScanParams(
        start_date=date(2026, 1, 1), end_date=date(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        max_gap_enabled=True, max_gap_min_pct=10.0,
    )
    result = scanner.run_scan(["TKR"], p)
    assert not result.results_df.empty

    row = result.results_df.iloc[0]
    aligned = row.get("_earnings_aligned_dates")
    assert isinstance(aligned, list)
    assert earnings_day in aligned


def test_gain_start_date_is_excluded_from_alignment(fake_scan_cache):
    """`gain_start_date` is metadata (the first bar in the scan
    window), not a user-chosen indicator. Including it in the
    alignment-cols list produced false positives whenever the
    user's scan-window start happened to coincide with an earnings
    report. Build a scenario where gain_start_date == an earnings
    date, with NO other indicator matching, and assert no alignment
    fires."""
    # Pick a scan start that lands EXACTLY on an earnings day.
    earnings_day = "2025-11-17"  # Monday — earnings report
    end = pd.Timestamp(date(2026, 4, 30))
    _write_ohlcv_with_jump(
        "TKR", end,
        jump_date="2026-03-10",  # gap on a different day
        jump_pct=15.0,
    )
    pd.DataFrame([
        _hist_row("TKR", "2025-10-31", earnings_day),
        _hist_row("TKR", "2026-01-31", "2026-02-17"),
    ]).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    p = ScanParams(
        # start_date is set to the earnings day so gain_start_date will
        # equal an earnings report date.
        start_date=date(2025, 11, 17), end_date=date(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        # max_gap is on but the gap is NOT on an earnings day, so the
        # only "match" candidate would be gain_start_date — which
        # we've explicitly excluded from alignment.
        max_gap_enabled=True, max_gap_min_pct=10.0,
    )
    result = scanner.run_scan(["TKR"], p)
    if result.results_df.empty:
        return
    row = result.results_df.iloc[0]
    aligned = row.get("_earnings_aligned_dates")
    # gain_start_date matching an earnings day must NOT trigger
    # alignment — it's not a real "indicator landed on earnings"
    # signal. Either no key, NaN, or empty list.
    assert aligned is None or pd.isna(aligned) or aligned == []


def test_no_match_means_no_aligned_dates(fake_scan_cache):
    """When the indicator date doesn't match any earnings report date,
    `_earnings_aligned_dates` is absent / empty."""
    end = pd.Timestamp(date(2026, 4, 30))
    _write_ohlcv_with_jump("TKR", end, jump_date="2026-03-10", jump_pct=15.0)
    pd.DataFrame([
        # Earnings on a different day
        _hist_row("TKR", "2026-01-31", "2026-02-15"),
    ]).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    p = ScanParams(
        start_date=date(2026, 1, 1), end_date=date(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        max_gap_enabled=True, max_gap_min_pct=10.0,
    )
    result = scanner.run_scan(["TKR"], p)
    if result.results_df.empty:
        return  # no result = no row to check
    row = result.results_df.iloc[0]
    aligned = row.get("_earnings_aligned_dates")
    # Either column is absent (NaN) or it's an empty list
    assert aligned is None or pd.isna(aligned) or aligned == []


def test_match_on_older_report_date_still_flags(fake_scan_cache):
    """Indicator can match an OLDER report (not just the most recent)
    and still get flagged. This case has 2 reports; the gap lands on
    the older one."""
    end = pd.Timestamp(date(2026, 4, 30))
    older_earnings = "2025-11-17"  # Monday — older report (weekday)
    _write_ohlcv_with_jump("TKR", end, jump_date=older_earnings, jump_pct=15.0)
    pd.DataFrame([
        _hist_row("TKR", "2026-01-31", "2026-02-17"),  # most recent (weekday)
        _hist_row("TKR", "2025-10-31", older_earnings),  # older
    ]).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    p = ScanParams(
        start_date=date(2025, 9, 1), end_date=date(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        max_gap_enabled=True, max_gap_min_pct=10.0,
    )
    result = scanner.run_scan(["TKR"], p)
    assert not result.results_df.empty
    row = result.results_df.iloc[0]
    aligned = row.get("_earnings_aligned_dates")
    assert isinstance(aligned, list)
    assert older_earnings in aligned


# ──────────────────────────────────────────────────────────────────────
# Render-time: aligned date cell renders in blue
# ──────────────────────────────────────────────────────────────────────

def _palette_rgbs():
    """Set of (r, g, b) tuples for the alignment palette. Used by
    tests that need to assert "any palette color" rather than a
    specific legacy single-color match."""
    from trade_scanner_fh.gui.widgets import ResultsTable
    return {(c.red(), c.green(), c.blue())
            for c in ResultsTable._ALIGN_PALETTE}


def test_aligned_date_cell_renders_in_palette_color(_qapp):
    """ResultsTable.populate must color a date cell with one of the
    alignment-palette colors when the value is in
    `_earnings_aligned_dates` AND an earnings cell anchors the same
    date (post-2026-05 earnings-anchor gate)."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    same = pd.Timestamp("2026-02-15")
    df = pd.DataFrame([{
        "symbol": "TKR", "close": 100.0, "price": 100.0, "pct_gain": 5.0,
        "max_gap_pct": 12.5,
        "max_gap_date": same,
        # Earnings anchor (visible last_report_date) makes the color
        # match valid — without it the gate suppresses the color.
        "last_report_date": same,
        "_earnings_aligned_dates": ["2026-02-15"],
    }])
    table = ResultsTable()
    table.populate(df)

    headers = [c[0] for c in table.active_columns]
    idx = headers.index("Max Gap Date")
    item = table.model_src.item(0, idx)
    c = item.foreground().color()
    assert (c.red(), c.green(), c.blue()) in _palette_rgbs()


def test_non_aligned_date_cell_uses_default_color(_qapp):
    """A date cell whose value is NOT in the aligned set keeps
    the default foreground (not in the alignment palette)."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    df = pd.DataFrame([{
        "symbol": "TKR", "close": 100.0, "price": 100.0, "pct_gain": 5.0,
        "max_gap_pct": 12.5,
        "max_gap_date": pd.Timestamp("2026-03-01"),
        "_earnings_aligned_dates": ["2026-02-15"],  # different date
    }])
    table = ResultsTable()
    table.populate(df)

    headers = [c[0] for c in table.active_columns]
    idx = headers.index("Max Gap Date")
    item = table.model_src.item(0, idx)
    c = item.foreground().color()
    # Non-matched date must NOT pick up any palette color — that
    # was the regression the per-match randomizer is built to avoid.
    assert (c.red(), c.green(), c.blue()) not in _palette_rgbs()


def test_match_color_spreads_to_related_value_cell(_qapp):
    """Generalized match-color (2026-05): when an indicator date
    matches an earnings report date, the matching color spreads from
    the date cell to the related VALUE cell. The earnings-anchor gate
    requires a visible earnings cell — provided here via
    `last_report_date`."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    same = pd.Timestamp("2026-02-15")
    df = pd.DataFrame([{
        "symbol": "TKR", "close": 100.0, "price": 100.0, "pct_gain": 5.0,
        "max_gap_pct": 12.5,
        "max_gap_date": same,
        "last_report_date": same,
        "_earnings_aligned_dates": ["2026-02-15"],
    }])
    table = ResultsTable()
    table.populate(df)

    headers = [c[0] for c in table.active_columns]
    date_idx = headers.index("Max Gap Date")
    pct_idx = headers.index("Max Gap%")  # value column for max_gap

    date_color = table.model_src.item(0, date_idx).foreground().color()
    pct_color = table.model_src.item(0, pct_idx).foreground().color()

    palette_rgbs = {(c.red(), c.green(), c.blue())
                    for c in ResultsTable._ALIGN_PALETTE}
    # Both cells in the gap unit must come from the curated palette
    assert (date_color.red(), date_color.green(), date_color.blue()) in palette_rgbs
    assert (pct_color.red(), pct_color.green(), pct_color.blue()) in palette_rgbs
    # And they MUST match — the unit shares one color
    assert date_color == pct_color


def test_match_color_spreads_to_q_block_unit(_qapp):
    """When a non-earnings indicator date matches a q-i report date,
    the entire Q-i quarter block (date + reported + surp $ + surp %)
    shares the same color. This is the user's specific example: gap
    date matches q3 rev report date → gap value AND all q3 rev data
    share the color."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    earnings_day = pd.Timestamp("2024-08-15")
    df = pd.DataFrame([{
        "symbol": "TKR", "close": 100.0, "price": 100.0, "pct_gain": 5.0,
        # Max gap landed on the earnings day
        "max_gap_pct": 12.5,
        "max_gap_date": earnings_day,
        # Q3 rev block — populate to make q3 columns appear
        "consec_rev_beats": 3,
        "q1_report_date_rev": pd.Timestamp("2025-02-15"),
        "q1_reported_rev": 110.0,
        "q1_surprise_rev_dollar": 5.0,
        "q1_surprise_rev_pct": 5.0,
        "q2_report_date_rev": pd.Timestamp("2024-11-15"),
        "q2_reported_rev": 108.0,
        "q2_surprise_rev_dollar": 4.0,
        "q2_surprise_rev_pct": 4.0,
        "q3_report_date_rev": earnings_day,  # ← match!
        "q3_reported_rev": 105.0,
        "q3_surprise_rev_dollar": 3.0,
        "q3_surprise_rev_pct": 3.0,
        "_earnings_aligned_dates": ["2024-08-15"],
    }])
    table = ResultsTable()
    table.populate(df)
    headers = [c[0] for c in table.active_columns]

    # Anchor: gap date / value share color
    gap_date_idx = headers.index("Max Gap Date")
    gap_pct_idx = headers.index("Max Gap%")
    gap_date_color = table.model_src.item(0, gap_date_idx).foreground().color()
    gap_pct_color = table.model_src.item(0, gap_pct_idx).foreground().color()
    assert gap_date_color == gap_pct_color

    # Q-3 Rev quarter block: all 4 columns share the color
    q3_keys = [
        ("Q-3 Date", "q3_report_date_rev"),
        ("Q-3 Reported Rev", "q3_reported_rev"),
        ("Q-3 Surp Rev $", "q3_surprise_rev_dollar"),
        ("Q-3 Surp Rev %", "q3_surprise_rev_pct"),
    ]
    # Find the Q-3 Date column for the REV block (there might be two
    # "Q-3 Date" headers if both EPS and Rev blocks render — pick the
    # one whose key is q3_report_date_rev).
    q3_rev_indices = []
    for i, (header, key, _fmt) in enumerate(table.active_columns):
        if key in dict(q3_keys).values():
            q3_rev_indices.append((key, i))
    # Should find 4 columns
    assert len(q3_rev_indices) == 4
    q3_colors = [
        table.model_src.item(0, i).foreground().color()
        for _k, i in q3_rev_indices
    ]
    # All 4 q3 rev cells share the same color
    for c in q3_colors[1:]:
        assert c == q3_colors[0], "Q-3 rev block cells should share one color"
    # And they share the gap unit's color (same earnings-aligned date)
    assert q3_colors[0] == gap_date_color


def test_match_color_does_not_spread_to_unrelated_columns(_qapp):
    """Unrelated value columns (like Close, % Gain, the streak count)
    must NOT pick up a match color even when the row has alignments.
    Only cells whose anchor date matches get colored."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    df = pd.DataFrame([{
        "symbol": "TKR", "close": 100.0, "price": 100.0, "pct_gain": 5.0,
        "max_gap_pct": 12.5,
        "max_gap_date": pd.Timestamp("2026-02-15"),
        "_earnings_aligned_dates": ["2026-02-15"],
    }])
    table = ResultsTable()
    table.populate(df)
    headers = [c[0] for c in table.active_columns]

    palette_rgbs = {(c.red(), c.green(), c.blue())
                    for c in ResultsTable._ALIGN_PALETTE}
    # Close and % Gain are unrelated to max_gap_date — must NOT pick
    # up the alignment color.
    for header_label in ("Close", "% Gain"):
        idx = headers.index(header_label)
        c = table.model_src.item(0, idx).foreground().color()
        rgb = (c.red(), c.green(), c.blue())
        assert rgb not in palette_rgbs, (
            f"{header_label} should not pick up an alignment color "
            f"(got {rgb})"
        )


def test_fuzzy_match_default_tolerance_pairs_off_by_one_day(fake_scan_cache):
    """Default tolerance is ±1 day. A gap on 2026-02-16 must pair
    with an earnings report on 2026-02-17 — both dates should land
    in `_earnings_aligned_dates`, AND a canonical map should be
    emitted that points both dates at the SAME canonical iso so the
    widget can paint them in matching colors."""
    end = pd.Timestamp(date(2026, 4, 30))
    earnings_day = "2026-02-17"  # Tuesday
    gap_day = "2026-02-16"       # Monday — one day before
    _write_ohlcv_with_jump("TKR", end, jump_date=gap_day, jump_pct=15.0)
    pd.DataFrame([
        _hist_row("TKR", "2026-01-31", earnings_day, eps_pct=10.0),
    ]).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    p = ScanParams(
        start_date=date(2026, 1, 1), end_date=date(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        max_gap_enabled=True, max_gap_min_pct=10.0,
        # Default match_color_tolerance_days = 1
    )
    result = scanner.run_scan(["TKR"], p)
    assert not result.results_df.empty
    row = result.results_df.iloc[0]

    aligned = row.get("_earnings_aligned_dates")
    assert isinstance(aligned, list)
    # Both the indicator date AND the report date land in the list.
    assert gap_day in aligned
    assert earnings_day in aligned

    # And the canonical map collapses both to the same canonical
    # (the report date) so the widget colors them identically.
    canon = row.get("_earnings_aligned_canon")
    assert isinstance(canon, dict)
    assert canon.get(gap_day) == earnings_day
    assert canon.get(earnings_day) == earnings_day


def test_fuzzy_match_tolerance_zero_requires_exact_match(fake_scan_cache):
    """Setting `match_color_tolerance_days=0` restores the legacy
    exact-only behavior. The off-by-one case from the previous test
    must NOT match when tolerance is 0."""
    end = pd.Timestamp(date(2026, 4, 30))
    earnings_day = "2026-02-17"
    gap_day = "2026-02-16"
    _write_ohlcv_with_jump("TKR", end, jump_date=gap_day, jump_pct=15.0)
    pd.DataFrame([
        _hist_row("TKR", "2026-01-31", earnings_day),
    ]).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    p = ScanParams(
        start_date=date(2026, 1, 1), end_date=date(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        max_gap_enabled=True, max_gap_min_pct=10.0,
        match_color_tolerance_days=0,
    )
    result = scanner.run_scan(["TKR"], p)
    assert not result.results_df.empty
    row = result.results_df.iloc[0]
    aligned = row.get("_earnings_aligned_dates")
    # No match expected — either absent / NaN / empty list.
    assert aligned is None or pd.isna(aligned) or aligned == []


def test_fuzzy_match_outside_tolerance_no_match(fake_scan_cache):
    """A 3-day offset with default tolerance (±1) must not match."""
    end = pd.Timestamp(date(2026, 4, 30))
    earnings_day = "2026-02-17"
    gap_day = "2026-02-13"   # 4 days before — outside any reasonable fuzzy window
    _write_ohlcv_with_jump("TKR", end, jump_date=gap_day, jump_pct=15.0)
    pd.DataFrame([
        _hist_row("TKR", "2026-01-31", earnings_day),
    ]).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    p = ScanParams(
        start_date=date(2026, 1, 1), end_date=date(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        max_gap_enabled=True, max_gap_min_pct=10.0,
    )
    result = scanner.run_scan(["TKR"], p)
    assert not result.results_df.empty
    row = result.results_df.iloc[0]
    aligned = row.get("_earnings_aligned_dates")
    assert aligned is None or pd.isna(aligned) or aligned == []


def test_fuzzy_match_widens_with_higher_tolerance(fake_scan_cache):
    """A 3-day offset that fails at tolerance=1 must succeed at
    tolerance=3."""
    end = pd.Timestamp(date(2026, 4, 30))
    earnings_day = "2026-02-17"
    gap_day = "2026-02-20"   # +3 days
    _write_ohlcv_with_jump("TKR", end, jump_date=gap_day, jump_pct=15.0)
    pd.DataFrame([
        _hist_row("TKR", "2026-01-31", earnings_day),
    ]).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    base = dict(
        start_date=date(2026, 1, 1), end_date=date(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        max_gap_enabled=True, max_gap_min_pct=10.0,
    )
    r1 = scanner.run_scan(["TKR"], ScanParams(
        **base, match_color_tolerance_days=1,
    ))
    a1 = r1.results_df.iloc[0].get("_earnings_aligned_dates")
    assert a1 is None or pd.isna(a1) or a1 == []

    r3 = scanner.run_scan(["TKR"], ScanParams(
        **base, match_color_tolerance_days=3,
    ))
    a3 = r3.results_df.iloc[0].get("_earnings_aligned_dates")
    assert isinstance(a3, list)
    assert gap_day in a3 and earnings_day in a3


def test_exact_match_does_not_emit_canonical_map(fake_scan_cache):
    """The canonical map is emitted ONLY when fuzzy matching actually
    produced a non-identity mapping. Exact matches don't need it,
    and absence keeps the row payload lean for the common case."""
    end = pd.Timestamp(date(2026, 4, 30))
    earnings_day = "2026-02-17"
    _write_ohlcv_with_jump("TKR", end, jump_date=earnings_day, jump_pct=15.0)
    pd.DataFrame([
        _hist_row("TKR", "2026-01-31", earnings_day),
    ]).to_parquet(config.EARNINGS_HISTORY_PARQUET)

    p = ScanParams(
        start_date=date(2026, 1, 1), end_date=date(2026, 4, 30),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False,
        adr_enabled=False, min_price_enabled=False,
        avg_vol_enabled=False, dollar_vol_enabled=False,
        max_gap_enabled=True, max_gap_min_pct=10.0,
    )
    result = scanner.run_scan(["TKR"], p)
    row = result.results_df.iloc[0]
    aligned = row.get("_earnings_aligned_dates")
    assert isinstance(aligned, list) and earnings_day in aligned
    # No canonical map needed for an exact match.
    assert row.get("_earnings_aligned_canon") is None or pd.isna(
        row.get("_earnings_aligned_canon")
    )


def test_fuzzy_paired_cells_share_one_color(_qapp):
    """Widget integration: when the row carries both dates AND a
    canonical map, the indicator date cell and the matched report
    date cell render in THE SAME palette color (same canonical →
    same color seed)."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    indicator_date = pd.Timestamp("2026-02-16")  # off by 1
    report_date = pd.Timestamp("2026-02-17")
    df = pd.DataFrame([{
        "symbol": "TKR", "close": 100.0, "price": 100.0, "pct_gain": 5.0,
        "max_gap_pct": 12.5,
        "max_gap_date": indicator_date,
        "last_report_date": report_date,
        "_earnings_aligned_dates": ["2026-02-16", "2026-02-17"],
        "_earnings_aligned_canon": {
            "2026-02-16": "2026-02-17",
            "2026-02-17": "2026-02-17",
        },
    }])
    table = ResultsTable()
    table.populate(df)
    headers = [c[0] for c in table.active_columns]

    palette_rgbs = _palette_rgbs()
    gap_c = table.model_src.item(0, headers.index("Max Gap Date")).foreground().color()
    lrd_c = table.model_src.item(0, headers.index("Last Report Date")).foreground().color()
    assert (gap_c.red(), gap_c.green(), gap_c.blue()) in palette_rgbs
    assert (lrd_c.red(), lrd_c.green(), lrd_c.blue()) in palette_rgbs
    # Critical assertion: paired dates share ONE color.
    assert gap_c == lrd_c, (
        "Fuzzy-paired indicator + report dates must share one palette "
        "color via the canonical map."
    )


def test_widget_falls_back_when_no_canonical_map(_qapp):
    """Backward compat: a row with only `_earnings_aligned_dates` and
    no `_earnings_aligned_canon` falls back to the legacy one-color-
    per-iso behavior. (Exact-match results don't emit a canon map,
    so this path must keep working.)"""
    from trade_scanner_fh.gui.widgets import ResultsTable

    same = pd.Timestamp("2026-02-15")
    df = pd.DataFrame([{
        "symbol": "TKR", "close": 100.0, "price": 100.0, "pct_gain": 5.0,
        "max_gap_pct": 12.5,
        "max_gap_date": same,
        "last_report_date": same,
        "_earnings_aligned_dates": ["2026-02-15"],
        # No _earnings_aligned_canon — legacy path.
    }])
    table = ResultsTable()
    table.populate(df)
    headers = [c[0] for c in table.active_columns]
    gap_c = table.model_src.item(0, headers.index("Max Gap Date")).foreground().color()
    lrd_c = table.model_src.item(0, headers.index("Last Report Date")).foreground().color()
    assert gap_c == lrd_c  # same date → same color, no canon map needed


def test_match_color_tolerance_param_default_is_one():
    """Sanity: the public default is ±1 day."""
    p = ScanParams()
    assert p.match_color_tolerance_days == 1


# ──────────────────────────────────────────────────────────────────────
# Earnings-anchor gate (2026-05): a color match cannot exist without at
# least one earnings-source cell anchoring the canonical iso. Pairs of
# non-earnings cells (gap + surge etc.) sharing a color with no visible
# earnings link is misleading — the gate suppresses that case.
# ──────────────────────────────────────────────────────────────────────

def test_color_match_suppressed_without_visible_earnings_anchor(_qapp):
    """A row with only non-earnings indicator cells (gap, surge) whose
    dates align to earnings does NOT receive any palette colors —
    there's no visible earnings cell to anchor the match. Without this
    gate the user sees gap+surge colored together with no apparent
    earnings link."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    earnings_day = pd.Timestamp("2026-02-15")
    df = pd.DataFrame([{
        "symbol": "TKR", "close": 100.0, "price": 100.0, "pct_gain": 5.0,
        "max_gap_pct": 12.5,
        "max_gap_date": earnings_day,
        "surge_pct": 8.0, "surge_window": 3,
        "surge_start_date": earnings_day,
        # _earnings_aligned_dates IS populated (the data alignment is
        # real) but no earnings cell is rendered → no color group.
        "_earnings_aligned_dates": ["2026-02-15"],
    }])
    table = ResultsTable()
    table.populate(df)
    headers = [c[0] for c in table.active_columns]
    palette_rgbs = _palette_rgbs()

    # Neither the gap nor the surge cells should pick up a palette
    # color — gate suppressed the canonical group.
    for label in ("Max Gap Date", "Max Gap%", "Surge Start"):
        if label not in headers:
            continue
        c = table.model_src.item(0, headers.index(label)).foreground().color()
        rgb = (c.red(), c.green(), c.blue())
        assert rgb not in palette_rgbs, (
            f"{label} got a palette color {rgb} despite no visible "
            f"earnings anchor — gate failed"
        )


def test_color_match_re_enabled_when_earnings_cell_visible(_qapp):
    """Adding a visible earnings cell (last_report_date) re-enables the
    color match for the same canonical that was suppressed in the
    previous test. Both gap AND last_report_date now share the color."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    earnings_day = pd.Timestamp("2026-02-15")
    df = pd.DataFrame([{
        "symbol": "TKR", "close": 100.0, "price": 100.0, "pct_gain": 5.0,
        "max_gap_pct": 12.5,
        "max_gap_date": earnings_day,
        # Earnings anchor visible — re-enables the color group.
        "last_report_date": earnings_day,
        "_earnings_aligned_dates": ["2026-02-15"],
    }])
    table = ResultsTable()
    table.populate(df)
    headers = [c[0] for c in table.active_columns]

    gap_c = table.model_src.item(0, headers.index("Max Gap Date")).foreground().color()
    lrd_c = table.model_src.item(0, headers.index("Last Report Date")).foreground().color()
    palette_rgbs = _palette_rgbs()
    assert (gap_c.red(), gap_c.green(), gap_c.blue()) in palette_rgbs
    assert (lrd_c.red(), lrd_c.green(), lrd_c.blue()) in palette_rgbs
    assert gap_c == lrd_c


def test_color_match_isolates_groups_with_and_without_earnings_anchor(_qapp):
    """Two distinct canonicals in one row: one has an earnings anchor
    (visible q-1 date), the other only has non-earnings cells. The
    anchored group renders in a palette color; the unanchored group
    is suppressed even though its dates ARE in
    `_earnings_aligned_dates`."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    anchored_day = pd.Timestamp("2026-02-15")
    orphan_day = pd.Timestamp("2025-11-15")
    df = pd.DataFrame([{
        "symbol": "TKR", "close": 100.0, "price": 100.0, "pct_gain": 5.0,
        # Group A (anchored): max_gap_date + q-1 report date
        "max_gap_pct": 12.5,
        "max_gap_date": anchored_day,
        "consec_eps_beats": 1,
        "q1_report_date_eps": anchored_day,
        "q1_reported_eps": 2.0,
        "q1_surprise_eps_dollar": 0.1,
        "q1_surprise_eps_pct": 5.0,
        # Group B (orphan): min_gap_date alone — no earnings cell on
        # 2025-11-15 in the rendered output
        "max_neg_gap_pct": -8.0,
        "min_gap_date": orphan_day,
        "_earnings_aligned_dates": ["2026-02-15", "2025-11-15"],
    }])
    table = ResultsTable()
    table.populate(df)
    headers = [c[0] for c in table.active_columns]
    palette_rgbs = _palette_rgbs()

    # Group A: anchored → both gap and q-1 cells have a palette color
    max_gap_c = table.model_src.item(
        0, headers.index("Max Gap Date"),
    ).foreground().color()
    assert (max_gap_c.red(), max_gap_c.green(), max_gap_c.blue()) in palette_rgbs

    # Group B: orphan → min_gap cell stays default (not in palette)
    min_gap_c = table.model_src.item(
        0, headers.index("Min Gap Date"),
    ).foreground().color()
    assert (min_gap_c.red(), min_gap_c.green(), min_gap_c.blue()) not in palette_rgbs


def test_color_match_suppressed_for_pair_of_non_earnings_indicators(_qapp):
    """The exact bug-report scenario: two non-earnings indicator dates
    (gap + surge) both align to the SAME earnings report date but no
    earnings cell is in the rendered output. Pre-fix they shared a
    color (false visual pairing); post-fix they are both default."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    earnings_day = pd.Timestamp("2026-02-15")
    df = pd.DataFrame([{
        "symbol": "TKR", "close": 100.0, "price": 100.0, "pct_gain": 5.0,
        # Both indicator dates land on the same earnings event,
        # both within tolerance, both end up under the same canonical
        # iso (2026-02-15). With no earnings cell visible the gate
        # suppresses the color group entirely.
        "max_gap_pct": 12.5, "max_gap_date": earnings_day,
        "surge_pct": 8.0, "surge_window": 3,
        "surge_start_date": earnings_day,
        "_earnings_aligned_dates": ["2026-02-15"],
        "_earnings_aligned_canon": {"2026-02-15": "2026-02-15"},
    }])
    table = ResultsTable()
    table.populate(df)
    headers = [c[0] for c in table.active_columns]
    palette_rgbs = _palette_rgbs()

    for label in ("Max Gap Date", "Surge Start"):
        if label not in headers:
            continue
        c = table.model_src.item(0, headers.index(label)).foreground().color()
        rgb = (c.red(), c.green(), c.blue())
        assert rgb not in palette_rgbs, (
            f"{label} got palette color {rgb} despite no earnings "
            f"anchor — gap+surge should NOT pair without earnings link"
        )


def test_last_report_date_paired_color_with_indicator_match(_qapp):
    """When the most recent report date equals an indicator date,
    BOTH Last Report Date and the indicator column render in the SAME
    palette color — that's the visual signal "these dates are paired".
    Color is per-match randomized (not the legacy fixed blue), but
    same-date-in-same-row must always share a color."""
    from trade_scanner_fh.gui.widgets import ResultsTable

    same = pd.Timestamp("2026-02-15")
    df = pd.DataFrame([{
        "symbol": "TKR", "close": 100.0, "price": 100.0, "pct_gain": 5.0,
        "max_gap_pct": 12.5,
        "max_gap_date": same,
        "last_report_date": same,
        "_earnings_aligned_dates": ["2026-02-15"],
    }])
    table = ResultsTable()
    table.populate(df)

    headers = [c[0] for c in table.active_columns]
    gap_item = table.model_src.item(0, headers.index("Max Gap Date"))
    lrd_item = table.model_src.item(0, headers.index("Last Report Date"))
    gap_c = gap_item.foreground().color()
    lrd_c = lrd_item.foreground().color()
    palette_rgbs = _palette_rgbs()
    # Both pulled from the curated palette
    assert (gap_c.red(), gap_c.green(), gap_c.blue()) in palette_rgbs
    assert (lrd_c.red(), lrd_c.green(), lrd_c.blue()) in palette_rgbs
    # And they match each other (same date → same color in same row)
    assert gap_c == lrd_c
