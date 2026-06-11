"""Tests for scanner._build_filter_stages — Phase 5 O2."""
from datetime import date

import numpy as np
import pandas as pd
import pytest

from trade_scanner_fh import scanner
from trade_scanner_fh.scanner import ScanParams


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _computed_df(**cols) -> pd.DataFrame:
    """Build a computed-indicators DataFrame of 5 tickers with configurable
    per-column overrides. Any column not passed defaults to a sensible value."""
    defaults = {
        "symbol":           ["A", "B", "C", "D", "E"],
        "close":            [50.0, 100.0, 150.0, 200.0, 500.0],
        "price":            [50.0, 100.0, 150.0, 200.0, 500.0],
        "pct_gain":         [5.0, 15.0, 25.0, 35.0, 50.0],
        "avg_vol":          [100_000, 500_000, 1_000_000, 2_000_000, 5_000_000],
        "dollar_vol":       [5e6, 50e6, 150e6, 400e6, 2.5e9],
        "adr_pct":          [1.5, 3.0, 4.5, 6.0, 8.0],
        "sti":              [0.95, 1.05, 1.10, 1.20, 1.30],
        "dist_high_pct":    [20.0, 10.0, 5.0, 2.0, 0.5],
    }
    defaults.update(cols)
    return pd.DataFrame(defaults)


def _base_params() -> ScanParams:
    """ScanParams with all filters disabled so tests can enable one at a time."""
    return ScanParams(
        start_date=date(2026, 1, 1), end_date=date(2026, 3, 1),
        sma1_enabled=False, sma2_enabled=False, sti_enabled=False,
        dist_high_enabled=False, pct_gain_enabled=False, top_pct_enabled=False,
        consec_gaps_enabled=False, consec_gaps_down_enabled=False,
        current_gap_enabled=False, max_gap_enabled=False, max_neg_gap_enabled=False,
        surge_enabled=False, adr_enabled=False, atr_enabled=False,
        bbw_enabled=False, atr_ratio_enabled=False, vol_dryup_enabled=False,
        min_price_enabled=False, avg_vol_enabled=False, dollar_vol_enabled=False,
        rs_market_enabled=False, rs_nasdaq_enabled=False, rs_sector_enabled=False,
        days_since_earnings_enabled=False,
        days_until_earnings_enabled=False, days_until_max_enabled=False,
    )


# ----------------------------------------------------------------------
# Empty / all-disabled stages
# ----------------------------------------------------------------------

def test_no_filters_enabled_returns_empty_stage_list():
    stages = scanner._build_filter_stages(_base_params())
    assert stages == []


# ----------------------------------------------------------------------
# Each filter produces the correct boolean mask
# ----------------------------------------------------------------------

def test_min_price_filter_keeps_rows_at_or_above_floor():
    params = _base_params()
    params.min_price_enabled = True
    params.min_price_floor = 100.0

    stages = scanner._build_filter_stages(params)
    assert len(stages) == 1
    name, fn = stages[0]
    df = _computed_df()
    mask = fn(df).fillna(False)
    # prices: 50, 100, 150, 200, 500 → >=100 keeps B, C, D, E
    assert list(mask) == [False, True, True, True, True]


def test_avg_vol_filter():
    params = _base_params()
    params.avg_vol_enabled = True
    params.avg_vol_min = 1_000_000

    _, fn = scanner._build_filter_stages(params)[0]
    df = _computed_df()
    mask = fn(df).fillna(False)
    assert list(mask) == [False, False, True, True, True]


def test_pct_gain_filter():
    params = _base_params()
    params.pct_gain_enabled = True
    params.pct_gain_min = 20.0

    _, fn = scanner._build_filter_stages(params)[0]
    df = _computed_df()
    mask = fn(df).fillna(False)
    assert list(mask) == [False, False, True, True, True]


def test_dist_high_filter():
    params = _base_params()
    params.dist_high_enabled = True
    params.dist_high_max_pct = 5.0

    _, fn = scanner._build_filter_stages(params)[0]
    df = _computed_df()
    # dist_high: 20, 10, 5, 2, 0.5 → <= 5 keeps C, D, E
    mask = fn(df).fillna(False)
    assert list(mask) == [False, False, True, True, True]


def test_nan_values_fail_the_filter():
    """NaN in the filtered column must not pass the filter — the scanner
    downstream of _build_filter_stages does .fillna(False)."""
    params = _base_params()
    params.pct_gain_enabled = True
    params.pct_gain_min = 20.0

    _, fn = scanner._build_filter_stages(params)[0]
    df = _computed_df(pct_gain=[np.nan, 15.0, np.nan, 35.0, 50.0])
    mask = fn(df).fillna(False)
    # NaN rows fail, in-range rows pass
    assert list(mask) == [False, False, False, True, True]


# ----------------------------------------------------------------------
# Ordering — min_price runs before SMA filters so early stages drop
# penny stocks cheaply
# ----------------------------------------------------------------------

def test_stage_ordering_min_price_before_sma():
    params = _base_params()
    params.min_price_enabled = True
    params.sma1_enabled = True
    stage_names = [name for name, _ in scanner._build_filter_stages(params)]
    # Min Price must come before "Above N SMA"
    mp_idx = next(i for i, n in enumerate(stage_names) if "Min Price" in n)
    sma_idx = next(i for i, n in enumerate(stage_names) if "SMA" in n)
    assert mp_idx < sma_idx


# ----------------------------------------------------------------------
# Composite stage label includes the threshold so log output is informative
# ----------------------------------------------------------------------

def test_stage_name_contains_threshold_value():
    params = _base_params()
    params.pct_gain_enabled = True
    params.pct_gain_min = 42.5
    name, _ = scanner._build_filter_stages(params)[0]
    assert "42.5" in name


# ----------------------------------------------------------------------
# RS-sector filter passes NaN rows (missing-sector tolerance)
# ----------------------------------------------------------------------

def test_rs_sector_filter_is_lenient_to_missing_data():
    """The sector ETF lookup can legitimately fail for a ticker (unmapped),
    producing NaN. The filter must still pass those rows (they aren't
    evidence of underperformance) so scans with RS-sector enabled don't
    zero out unmapped tickers."""
    params = _base_params()
    params.rs_sector_enabled = True
    params.rs_sector_min = 1.0

    _, fn = scanner._build_filter_stages(params)[0]
    df = pd.DataFrame({
        "symbol":    ["A", "B", "C"],
        "rs_sector": [0.8, np.nan, 1.5],
    })
    mask = fn(df)
    # 0.8 fails, NaN passes (lenient), 1.5 passes
    assert list(mask) == [False, True, True]
