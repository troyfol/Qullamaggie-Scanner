"""
Scanner — Funnel-Style Filtering Pipeline
============================================
Computes all active indicators across the ticker universe for a
user-defined date window, filters stage by stage, and logs the
funnel counts at every step.

Public API
    run_scan(params: ScanParams, progress_cb=None) -> ScanResult
    ScanParams   — dataclass holding every tunable + on/off toggle
    ScanResult   — dataclass with results table, funnel log, errors
"""

import logging
import math
import time
import traceback
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Callable, Literal, Optional

import numpy as np
import pandas as pd

from . import config, indicators
from .data_engine import load_ohlcv

log = logging.getLogger("scanner.scan")

# ATR-based stop distance for the informational "ATR Stop" results
# column: stop = last Close − ATR_STOP_MULTIPLIER × ATR(atr_period).
# Display-only by design — no filter stage and no IndicatorPanel row.
ATR_STOP_MULTIPLIER = 2.0


# ============================================================================
# Sequenced-run period chunker
# ============================================================================

def chunk_periods(
    start_date: date, end_date: date, chunk_n: int,
    unit: Literal["days", "weeks", "months"],
) -> list[tuple[date, date]]:
    """Split [start_date, end_date] into chunks of `chunk_n` `unit`s,
    starting from end_date and walking BACKWARDS. The last (oldest) chunk
    is allowed to be smaller than chunk_n if the range doesn't divide
    evenly — caller behavior choice (b).

    Returns a list of (chunk_start, chunk_end) date pairs in newest-first
    order. Both endpoints are inclusive.

    Example:
        chunk_periods(date(2025,1,1), date(2025,12,31), 2, "months")
        → [(2025-11-01, 2025-12-31),
           (2025-09-01, 2025-10-31),
           (2025-07-01, 2025-08-31),
           (2025-05-01, 2025-06-30),
           (2025-03-01, 2025-04-30),
           (2025-01-01, 2025-02-28)]
    """
    if start_date > end_date:
        return []
    if chunk_n <= 0:
        raise ValueError("chunk_n must be positive")
    if unit not in ("days", "weeks", "months"):
        raise ValueError(f"unsupported unit: {unit!r}")

    chunks: list[tuple[date, date]] = []
    current_end = end_date

    while current_end >= start_date:
        if unit == "days":
            tentative_start = current_end - timedelta(days=chunk_n - 1)
        elif unit == "weeks":
            tentative_start = current_end - timedelta(days=chunk_n * 7 - 1)
        else:  # months — calendar-aware
            target_year = current_end.year
            target_month = current_end.month - (chunk_n - 1)
            while target_month < 1:
                target_month += 12
                target_year -= 1
            tentative_start = date(target_year, target_month, 1)

        chunk_start = max(tentative_start, start_date)
        chunks.append((chunk_start, current_end))
        if chunk_start <= start_date:
            break
        current_end = chunk_start - timedelta(days=1)

    return chunks


# ============================================================================
# Scan parameters — every toggle + threshold in one place
# ============================================================================

@dataclass
class ScanParams:
    """All tunable scanner parameters with sensible defaults."""

    # Date window. end_date defaults to "today" (dynamic, per-instance) so
    # a direct ScanParams() never anchors to a frozen stale date; the GUI
    # always sets it explicitly from the End picker.
    start_date: date = date(2025, 1, 1)
    end_date: date = field(default_factory=date.today)

    # --- Display-only semantics (Phase 8 §8.5) ---
    # Each filter has a paired `*_display_only` flag. When True, the
    # indicator is computed (column appears in results) but the
    # threshold filter is NOT applied (every ticker passes that stage).
    # `_compute_ticker` tests `enabled OR display_only`;
    # `_build_filter_stages` tests `enabled AND NOT display_only`.
    # `top_pct` and `min_price` deliberately have no display-only
    # variant — top_pct has no per-ticker value to surface, and
    # min_price's value is just `price`, which is always shown.

    # --- Trend Filters ---
    # #1a  Price above SMA (first SMA)
    sma1_enabled: bool = True
    sma1_display_only: bool = False
    sma1_period: int = 200

    # #1b  Price above SMA (second SMA)
    sma2_enabled: bool = True
    sma2_display_only: bool = False
    sma2_period: int = 50

    # #2   Stockbee Trend Intensity
    sti_enabled: bool = True
    sti_display_only: bool = False
    sti_short_lb: int = 7
    sti_long_lb: int = 65
    sti_threshold: float = 1.05

    # #3   Distance from period high
    dist_high_enabled: bool = True
    dist_high_display_only: bool = False
    dist_high_max_pct: float = 5.0  # within 5% of high

    # --- Momentum / Prior Move ---
    # #4   % Gain over period
    pct_gain_enabled: bool = True
    pct_gain_display_only: bool = False
    pct_gain_min: float = 20.0  # at least 20% gain

    # #5   Top X percentile (of #4 gains)
    top_pct_enabled: bool = False
    top_pct_cutoff: float = 10.0  # top 10%

    # #6   Consecutive gaps
    consec_gaps_enabled: bool = False
    consec_gaps_display_only: bool = False
    consec_gaps_min: int = 2

    # #7   Current gap %
    current_gap_enabled: bool = False
    current_gap_display_only: bool = False
    current_gap_min_pct: float = 2.0

    # #6b  Consecutive gap-downs
    consec_gaps_down_enabled: bool = False
    consec_gaps_down_display_only: bool = False
    consec_gaps_down_min: int = 2

    # #7c  Max Positive Gap (largest overnight gap in window)
    max_gap_enabled: bool = False
    max_gap_display_only: bool = False
    max_gap_min_pct: float = 5.0

    # #7d  Max Negative Gap (largest overnight gap-down in window)
    max_neg_gap_enabled: bool = False
    max_neg_gap_display_only: bool = False
    max_neg_gap_min_pct: float = -5.0

    # #7b  Surge Detection — peak N-day % gain anywhere in the window
    surge_enabled: bool = False
    surge_display_only: bool = False
    surge_min_pct: float = 40.0
    surge_days: int = 7
    # `surge_mode` selects the surge-finding algorithm:
    #   "trend"     — Trend-continuous (default for episodic pivots).
    #                 Scans the WHOLE window for the longest sustained
    #                 rally ending at the global peak that never pulled
    #                 back more than `surge_max_drawdown_pct` from any
    #                 intermediate running high. `surge_days` is unused.
    #   "close"     — Legacy fixed-N-day close-to-close peak.
    #   "high_low"  — Legacy fixed-N-day low-to-high (intraday-aware).
    # `surge_use_high_low` is kept for preset back-compat: when True
    # and `surge_mode` is left at the new default, the loader treats
    # the preset as `mode="high_low"` (see IndicatorPanel.from_dict).
    surge_mode: str = "trend"
    surge_use_high_low: bool = False  # legacy — see comment above
    # Trend-continuous: max allowed drawdown (in %) from any running
    # peak between the rally's start and its global peak. 25% is a
    # generous default that rides NVDA-class moves while still
    # rejecting names whose "rally" was just a short-term mean revert.
    surge_max_drawdown_pct: float = 25.0
    # Ignition mode (mode == "ignition"): builds on top of trend-
    # continuous. After the rally bounds are found, the start is
    # re-anchored to the IGNITION BAR — first day inside the rally
    # whose day-over-day close gain >= `ignition_min_pct` AND whose
    # volume >= `ignition_vol_mult` × median of the prior 20 bars'
    # volume. If no such bar exists, falls back to the trend start.
    # The two new knobs are unused outside ignition mode.
    surge_ignition_vol_mult: float = 2.0
    surge_ignition_min_pct: float = 5.0

    # #8   ADR% (momentum — looking for expansion)
    adr_enabled: bool = True
    adr_display_only: bool = False
    adr_lookback: int = 14
    adr_min_pct: float = 3.0  # at least 3% ADR = momentum

    # #8b  ATR (Average True Range — absolute dollar value)
    atr_enabled: bool = False
    atr_display_only: bool = False
    atr_period: int = 14
    atr_min: float = 0.50
    atr_max: float = 999.0

    # --- Volatility Contraction ---

    # #9   Bollinger Band Width
    bbw_enabled: bool = False
    bbw_display_only: bool = False
    bbw_period: int = 20
    bbw_num_std: float = 2.0
    bbw_max: float = 0.10

    # #10  ATR Ratio
    atr_ratio_enabled: bool = False
    atr_ratio_display_only: bool = False
    atr_short: int = 5
    atr_long: int = 50
    atr_max_ratio: float = 0.75

    # --- Volume / Liquidity ---
    # #11  Volume dry-up ratio
    vol_dryup_enabled: bool = False
    vol_dryup_display_only: bool = False
    vol_dryup_recent: int = 10
    vol_dryup_prior: int = 20
    vol_dryup_max_ratio: float = 0.70

    # #12  Minimum price
    min_price_enabled: bool = True
    min_price_floor: float = 10.0

    # #13  Average volume (raw shares)
    avg_vol_enabled: bool = True
    avg_vol_display_only: bool = False
    avg_vol_lookback: int = 20
    avg_vol_min: float = 200_000

    # #14  Dollar volume
    dollar_vol_enabled: bool = True
    dollar_vol_display_only: bool = False
    dollar_vol_lookback: int = 20
    dollar_vol_min: float = 5_000_000

    # #14b Relative Volume (RVOL) — last bar's volume / mean volume of
    # the prior `rvol_lookback` bars (excluding the last bar). Defaults
    # OFF so legacy presets (which lack these keys) load unchanged.
    # NaN (insufficient history / zero base) fails the filter via the
    # standard `>=` comparison.
    rvol_enabled: bool = False
    rvol_display_only: bool = False
    rvol_lookback: int = 20
    rvol_min: float = 1.5

    # --- Relative Strength ---
    # #15  RS vs S&P 500 (SPY)
    rs_market_enabled: bool = False
    rs_market_display_only: bool = False
    rs_market_lookback: int = 20
    rs_market_min: float = 1.0

    # #15b RS vs NASDAQ (ONEQ)
    rs_nasdaq_enabled: bool = False
    rs_nasdaq_display_only: bool = False
    rs_nasdaq_lookback: int = 20
    rs_nasdaq_min: float = 1.0

    # #16  RS vs Sector ETF
    rs_sector_enabled: bool = False
    rs_sector_display_only: bool = False
    rs_sector_lookback: int = 20
    rs_sector_min: float = 1.0

    # --- Global Earnings Mode (replaces per-filter include_no_data) ---
    # Two independent global flags govern NaN-handling for the earnings
    # funnel filters. Split because dates and data are conceptually
    # distinct coverage signals:
    #
    #   earnings_dates_only=True  → days_since/days_until/days_until_max
    #                                filters require a value (NaN fails)
    #                                UNLESS the ticker has earnings DATA,
    #                                because data implies a date.
    #   earnings_data_only=True   → reported_eps / surprise_eps_$ /
    #                                surprise_eps_% / reported_rev /
    #                                surprise_rev_$ / surprise_rev_% +
    #                                consec beats filters require a
    #                                value (NaN fails).
    #
    # When False (defaults): NaN passes the corresponding filters cleanly
    # so non-earnings filters can still operate on tickers Zacks doesn't
    # cover. Replaced the prior 9 per-filter `*_include_no_data` flags
    # (and the unified `earnings_only_mode` that briefly replaced them)
    # because users wanted to require dates without requiring full data.
    #
    # Invariant: dates filter ⊇ data filter (anything with data has a
    # date, so the dates filter must never drop a ticker that has any
    # earnings data value).
    earnings_dates_only: bool = False
    earnings_data_only: bool = False

    # --- Match-color anchoring (ResultsTable highlight) ---
    # Tolerance in CALENDAR DAYS for matching an indicator date against
    # an earnings report date. 0 = exact match (legacy). 1 = the date
    # may be off by one day in either direction — covers common timing
    # mismatches (after-hours report vs next-day price reaction, half-
    # day weekend offset, calendar provider rounding). Range 0–7. Used
    # only for the visual color-pairing on the ResultsTable; does NOT
    # affect any funnel filter or earnings stat. Persisted per-user via
    # QSettings; the GUI exposes it under Settings → Color Match
    # Tolerance….
    match_color_tolerance_days: int = 1

    # --- Earnings Filters ---
    # #17  Days since last earnings
    days_since_earnings_enabled: bool = False
    days_since_earnings_display_only: bool = False
    days_since_min: int = 0
    days_since_max: int = 90

    # #18  Days until next earnings (min — exclude tickers reporting too soon)
    days_until_earnings_enabled: bool = False
    days_until_earnings_display_only: bool = False
    days_until_min: int = 5

    # #19  Days until next earnings max (ceiling — only show tickers reporting within N days)
    days_until_max_enabled: bool = False
    days_until_max_display_only: bool = False
    days_until_max: int = 0

    # --- Per-quarter Earnings Filters (Phase 7 §7.1) ---
    # Six minimum-threshold filters on the most-recent quarter that
    # reported on or before each period's end_date. All off by default.
    # When the corresponding Consecutive Beats filter is enabled, these
    # are bypassed (still computed for §8 output, but filter is dropped).
    # NaN-handling for these is driven by the global
    # `earnings_data_only` flag above, NOT a per-filter
    # `include_no_data` (which was removed because the choice is
    # intrinsically about the TICKER's earnings coverage, not the
    # filter).

    # #20  Reported EPS (min)
    reported_eps_enabled: bool = False
    reported_eps_display_only: bool = False
    reported_eps_min: float = 0.0

    # #21  Surprise EPS $ (min)
    surprise_eps_dollar_enabled: bool = False
    surprise_eps_dollar_display_only: bool = False
    surprise_eps_dollar_min: float = 0.0

    # #22  Surprise EPS % (min)
    surprise_eps_pct_enabled: bool = False
    surprise_eps_pct_display_only: bool = False
    surprise_eps_pct_min: float = 0.0

    # #23  Reported Revenue (min, $M)
    reported_rev_enabled: bool = False
    reported_rev_display_only: bool = False
    reported_rev_min: float = 0.0

    # #24  Surprise Revenue $ (min, $M)
    surprise_rev_dollar_enabled: bool = False
    surprise_rev_dollar_display_only: bool = False
    surprise_rev_dollar_min: float = 0.0

    # #25  Surprise Revenue % (min)
    surprise_rev_pct_enabled: bool = False
    surprise_rev_pct_display_only: bool = False
    surprise_rev_pct_min: float = 0.0

    # #26  YoY EPS % (min). Computed at fill-finalize time and stored
    # on every earnings_history row that has a same-quarter-prior-year
    # counterpart. NaN when prior year missing.
    yoy_eps_pct_enabled: bool = False
    yoy_eps_pct_display_only: bool = False
    yoy_eps_pct_min: float = 0.0

    # #27  YoY Revenue % (min)
    yoy_rev_pct_enabled: bool = False
    yoy_rev_pct_display_only: bool = False
    yoy_rev_pct_min: float = 0.0

    # --- Consecutive Beats (Phase 7 §7.3) ---
    # When enabled, a quarter counts as a beat iff
    # surprise_{metric}_pct > threshold_pct (strict >). A miss / NaN /
    # missing quarter (>135-day cadence gap) breaks the streak.

    # #26  Consecutive EPS Beats (min)
    # `_quarter_cap`: optional ceiling on how many Q-i columns the
    # scanner populates / the table renders for the EPS side. 0 = no
    # cap (use the full MAX_BEATS_QUARTERS=20 ceiling). e.g. 4 limits
    # to Q-1..Q-4 even if the ticker has 20 quarters of history.
    consec_eps_beats_enabled: bool = False
    consec_eps_beats_display_only: bool = False
    consec_eps_beats_min: int = 3
    consec_eps_beats_threshold_pct: float = 0.0
    consec_eps_beats_quarter_cap: int = 0

    # #27  Consecutive Revenue Beats (min)
    consec_rev_beats_enabled: bool = False
    consec_rev_beats_display_only: bool = False
    consec_rev_beats_min: int = 3
    consec_rev_beats_threshold_pct: float = 0.0
    consec_rev_beats_quarter_cap: int = 0


# ============================================================================
# Scan result
# ============================================================================

@dataclass
class FunnelStage:
    name: str
    passed: int
    total_before: int


@dataclass
class ScanResult:
    params: ScanParams
    results_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    funnel: list[FunnelStage] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)
    elapsed_sec: float = 0.0

    def funnel_summary(self) -> str:
        parts = []
        for stage in self.funnel:
            parts.append(f"{stage.name}: {stage.passed}")
        return " -> ".join(parts)


# ============================================================================
# Internal: compute all indicator values for one ticker
# ============================================================================

def _compute_ticker(
    symbol: str,
    params: ScanParams,
    benchmark_data: dict[str, pd.DataFrame] | None = None,
    sector_lookup: dict[str, str] | None = None,
    earnings_lookup: dict[str, tuple] | None = None,
    earnings_history_lookup: dict[str, pd.DataFrame] | None = None,
) -> Optional[dict]:
    """
    Load OHLCV, slice to the date window, compute all indicators.
    Returns a dict of computed values or None if data is unusable.
    Raises on unexpected errors (caught by the caller).
    """
    df = load_ohlcv(symbol)
    if df is None or df.empty:
        return None

    # R10: copy before tz-mutation so we never modify the cached DataFrame
    if df.index.tz is not None:
        df = df.copy()
        df.index = df.index.tz_localize(None)

    # Slice to date window
    start_ts = pd.Timestamp(params.start_date)
    end_ts = pd.Timestamp(params.end_date)
    window = df.loc[start_ts:end_ts]

    if window.empty or len(window) < 2:
        return None

    # Lookback-dependent indicators (SMA, STI, etc.) need history before the
    # window, so use the full df up to end_date for those.
    full_to_end = df.loc[:end_ts]

    # Always compute these — required regardless of filter toggles:
    #   symbol            identity
    #   close / price     min-price filter (#12) + results display
    #   pct_gain          multi-timeframe dedupe sort + top-percentile filter (#5)
    #   gain_start_date   results display (date of the first close used for pct_gain)
    close_val = window["Close"].iloc[-1]
    pct, gain_start = indicators.pct_gain_over_period(window)
    row = {
        "symbol": symbol,
        "close": close_val,
        "price": close_val,
        "pct_gain": pct,
        "gain_start_date": gain_start,
    }

    # Phase 3 I2 + Phase 8 §8.5: every indicator below is gated on
    # `_enabled OR _display_only`. Display-only computes the value
    # (so it shows in results) but `_build_filter_stages` skips the
    # threshold filter for it. A scan with no filters at all still
    # skips every unused computation.

    if params.sma1_enabled or params.sma1_display_only:
        sma1 = indicators.price_above_sma(full_to_end, period=params.sma1_period)
        row[f"sma{params.sma1_period}"] = sma1["sma_value"]

    if params.sma2_enabled or params.sma2_display_only:
        sma2 = indicators.price_above_sma(full_to_end, period=params.sma2_period)
        row[f"sma{params.sma2_period}"] = sma2["sma_value"]

    if params.sti_enabled or params.sti_display_only:
        row["sti"] = indicators.stockbee_trend_intensity(
            full_to_end, short_lb=params.sti_short_lb, long_lb=params.sti_long_lb
        )

    if params.dist_high_enabled or params.dist_high_display_only:
        row["dist_high_pct"] = indicators.distance_from_period_high(window)

    if params.consec_gaps_enabled or params.consec_gaps_display_only:
        cnt, streak_start = indicators.consecutive_gaps(window)
        row["consec_gaps"] = cnt
        row["up_gap_start_date"] = streak_start

    if params.consec_gaps_down_enabled or params.consec_gaps_down_display_only:
        cnt, streak_start = indicators.consecutive_gaps_down(window)
        row["consec_gaps_down"] = cnt
        row["down_gap_start_date"] = streak_start

    if params.current_gap_enabled or params.current_gap_display_only:
        row["current_gap_pct"] = indicators.current_gap_pct(window)

    if params.max_gap_enabled or params.max_gap_display_only:
        max_pct, max_dt = indicators.max_positive_gap(window)
        row["max_gap_pct"] = max_pct
        row["max_gap_date"] = max_dt

    if params.max_neg_gap_enabled or params.max_neg_gap_display_only:
        min_pct, min_dt = indicators.max_negative_gap(window)
        row["max_neg_gap_pct"] = min_pct
        row["min_gap_date"] = min_dt

    if params.surge_enabled or params.surge_display_only:
        # Mode dispatch:
        #   "trend"     — full-window trend-continuous (default)
        #   "close"     — fixed-N-day close-to-close peak
        #   "high_low"  — fixed-N-day low-to-high
        # `surge_use_high_low=True` from a legacy preset is honored as
        # equivalent to mode="high_low" — IndicatorPanel.from_dict
        # promotes it to surge_mode at load time, so by the time we
        # reach here `surge_mode` is authoritative.
        if params.surge_mode == "trend":
            surge_pct, surge_start, surge_end = indicators.surge_trend_continuous(
                window, max_drawdown_pct=params.surge_max_drawdown_pct,
            )
        elif params.surge_mode == "ignition":
            surge_pct, surge_start, surge_end = indicators.surge_ignition(
                window,
                max_drawdown_pct=params.surge_max_drawdown_pct,
                vol_mult=params.surge_ignition_vol_mult,
                min_pct=params.surge_ignition_min_pct,
            )
        elif params.surge_mode == "high_low":
            surge_pct, surge_start, surge_end = indicators.surge_detection(
                window, surge_pct=params.surge_min_pct,
                surge_days=params.surge_days,
                use_high_low=True,
            )
        else:  # "close" (or unknown — fall back to legacy default)
            surge_pct, surge_start, surge_end = indicators.surge_detection(
                window, surge_pct=params.surge_min_pct,
                surge_days=params.surge_days,
                use_high_low=False,
            )
        row["surge_pct"] = surge_pct
        row["surge_start_date"] = surge_start
        row["surge_end_date"] = surge_end
        row["surge_window"] = (
            f"{surge_start.strftime('%m/%d')}–{surge_end.strftime('%m/%d')}"
            if surge_start and surge_end else ""
        )

    if params.adr_enabled or params.adr_display_only:
        row["adr_pct"] = indicators.adr_pct(full_to_end, lookback=params.adr_lookback)

    if params.atr_enabled or params.atr_display_only:
        atr_v = indicators.atr_value(full_to_end, period=params.atr_period)
        row["atr"] = atr_v
        # ATR Stop — informational column only (no filter, no panel
        # row): suggested stop level = last Close − 2.0 × ATR. Reuses
        # the single atr_value computation above (compute once, reuse).
        # NaN-safe: NaN ATR or NaN close → NaN stop (renders blank).
        if (atr_v is not None and np.isfinite(atr_v)
                and close_val is not None and np.isfinite(close_val)):
            row["atr_stop"] = float(close_val) - ATR_STOP_MULTIPLIER * float(atr_v)
        else:
            row["atr_stop"] = np.nan

    if params.bbw_enabled or params.bbw_display_only:
        row["bbw"] = indicators.bollinger_band_width(
            full_to_end, period=params.bbw_period, num_std=params.bbw_num_std
        )

    if params.atr_ratio_enabled or params.atr_ratio_display_only:
        row["atr_ratio"] = indicators.atr_ratio(
            full_to_end, short_period=params.atr_short, long_period=params.atr_long
        )

    if params.vol_dryup_enabled or params.vol_dryup_display_only:
        row["vol_dryup"] = indicators.volume_dryup_ratio(
            full_to_end, recent_n=params.vol_dryup_recent,
            prior_n=params.vol_dryup_prior,
        )

    if params.avg_vol_enabled or params.avg_vol_display_only:
        row["avg_vol"] = indicators.avg_volume(
            full_to_end, lookback=params.avg_vol_lookback
        )

    if params.dollar_vol_enabled or params.dollar_vol_display_only:
        row["dollar_vol"] = indicators.avg_dollar_volume(
            full_to_end, lookback=params.dollar_vol_lookback
        )

    # #14b RVOL — computed on full_to_end so the "last bar" is the scan
    # End-date bar (and the prior-volume base can reach back before the
    # window start, matching the other lookback indicators).
    if params.rvol_enabled or params.rvol_display_only:
        row["rvol"] = indicators.relative_volume(
            full_to_end, lookback=params.rvol_lookback
        )

    if (params.rs_market_enabled or params.rs_market_display_only) \
            and benchmark_data and "SPY" in benchmark_data:
        row["rs_market"] = indicators.relative_strength_ratio(
            full_to_end, benchmark_data["SPY"],
            lookback=params.rs_market_lookback,
        )

    if (params.rs_nasdaq_enabled or params.rs_nasdaq_display_only) \
            and benchmark_data and "ONEQ" in benchmark_data:
        row["rs_nasdaq"] = indicators.relative_strength_ratio(
            full_to_end, benchmark_data["ONEQ"],
            lookback=params.rs_nasdaq_lookback,
        )

    if (params.rs_sector_enabled or params.rs_sector_display_only) \
            and sector_lookup and benchmark_data:
        etf = sector_lookup.get(symbol)
        if etf and etf in benchmark_data:
            row["rs_sector"] = indicators.relative_strength_ratio(
                full_to_end, benchmark_data[etf],
                lookback=params.rs_sector_lookback,
            )

    if (params.days_since_earnings_enabled
            or params.days_since_earnings_display_only
            or params.days_until_earnings_enabled
            or params.days_until_earnings_display_only
            or params.days_until_max_enabled
            or params.days_until_max_display_only) and earnings_lookup:
        last_e, next_e = earnings_lookup.get(symbol, (None, None))

        # Handle "next_earnings is now in the past" edge case
        if next_e is not None and next_e <= end_ts:
            if last_e is None or next_e > last_e:
                last_e = next_e
            next_e = None

        if last_e is not None:
            row["days_since_er"] = (end_ts - last_e).days
        if next_e is not None:
            row["days_until_er"] = (next_e - end_ts).days

    # --- Per-quarter Zacks earnings (Phase 7 §7.1 + §7.3) ---
    # Audit H2: always populate the seven single-quarter columns when
    # earnings_history is loaded — independent of which Phase 7 filters
    # are enabled. Per spec §9.2 step 2 a vanilla scan with no earnings
    # filters still surfaces these columns at the rightmost end. The
    # Phase 8 q*_* / consec_*_beats columns remain gated on the beats
    # filters because they're only meaningful for the multi-quarter
    # display.
    if earnings_history_lookup:
        ticker_hist = earnings_history_lookup.get(symbol)
        if ticker_hist is not None and not ticker_hist.empty:
            # Cross-check indicator dates against this ticker's full
            # earnings report history. A match means "the price action
            # in that column happened on an earnings day" — both ends
            # render in blue (see ResultsTable.populate). Captured
            # before the past-slice filter so we look across the full
            # 5-year window, not just before end_ts.
            report_dates_all = {
                pd.Timestamp(d).normalize()
                for d in ticker_hist["report_date"]
                if d is not None and not pd.isna(d)
            }
            # Indicator date columns considered for earnings-alignment.
            # Excludes `gain_start_date` — that's always populated (it's
            # the first bar in the scan window, derived from the user's
            # date range, not a user-chosen signal). Including it
            # produced false-positive links between earnings columns
            # whenever the scan-window start happened to coincide with
            # an earnings report. The genuine "I picked this indicator
            # and it landed on an earnings date" signal lives in the
            # gap / surge / streak columns below — they're only
            # populated when the matching filter or display-only is on.
            indicator_date_cols = (
                "up_gap_start_date",
                "down_gap_start_date", "max_gap_date",
                "min_gap_date", "surge_start_date", "surge_end_date",
            )
            aligned_dates: set[pd.Timestamp] = set()
            # Canonical map for fuzzy matching: when an indicator date
            # X matches a report date Y within `match_color_tolerance_days`
            # but X != Y, both cells need to share ONE color so the
            # visual pairing reads correctly. The canonical map
            # collapses each matched date to a single key (always the
            # report date — stable when multiple indicators all match
            # the same report). Stored as `_earnings_aligned_canon` on
            # the row and consumed by `ResultsTable.populate`.
            canonical_map: dict[str, str] = {}
            tolerance = max(0, int(params.match_color_tolerance_days))
            sorted_reports = sorted(report_dates_all)
            for col_key in indicator_date_cols:
                v = row.get(col_key)
                if v is None or pd.isna(v):
                    continue
                v_norm = pd.Timestamp(v).normalize()
                # Find the closest report date within tolerance. Tie-
                # break by EARLIEST report so multiple indicators that
                # all sit between two reports collapse to the same
                # canonical (deterministic pairing on screen).
                best_report = None
                best_dist = None
                for r in sorted_reports:
                    d = abs((v_norm - r).days)
                    if d > tolerance:
                        continue
                    if best_dist is None or d < best_dist:
                        best_dist = d
                        best_report = r
                if best_report is None:
                    continue
                aligned_dates.add(v_norm)
                aligned_dates.add(best_report)
                canon_iso = best_report.date().isoformat()
                canonical_map[v_norm.date().isoformat()] = canon_iso
                canonical_map[canon_iso] = canon_iso

            # Most-recent quarter where report_date <= end_ts (per §8.1).
            # Audit M4: order by report_date DESC everywhere — table Q-1
            # and the streak counter both use this same ordering.
            past = ticker_hist.loc[ticker_hist["report_date"] <= end_ts]
            if not past.empty:
                past_desc = past.sort_values("report_date", ascending=False)
                # Prefer REAL announcement rows for ALL earnings columns:
                # skip finnhub calendar-proxy rows (report_date_proxy=True,
                # stamped to a calendar quarter-end ~30d off the real date)
                # so a proxy can't (a) out-sort the real row and show a
                # shifted Last Report Date / EPS, nor (b) duplicate-count a
                # quarter in the beats streak / qK_* block. Keep-if-orphan:
                # fall back to the full slice only when the ticker has no
                # non-proxy row at all. `past_pref` drives the
                # single-quarter pick (mr) AND the multi-quarter beats
                # block below, so all earnings columns stay consistent.
                if "report_date_proxy" in past_desc.columns:
                    _real = past_desc.loc[
                        ~past_desc["report_date_proxy"].fillna(False).astype(bool)
                    ]
                    past_pref = _real if not _real.empty else past_desc
                else:
                    past_pref = past_desc
                mr = past_pref.iloc[0]
                # Per-column gating (Option B). Each individual
                # earnings stat is populated only when its specific
                # filter or display-only is on. Prior behavior (audit
                # H2) populated all 6 + last_report_date whenever
                # earnings_history.parquet existed, which produced
                # "always-on context" columns the user couldn't turn
                # off. Now Off / Filter / Display Only are
                # consistent across all earnings columns: Off → not
                # in output, Filter → in output and gated, Display
                # Only → in output, no gating, red-on-fail coloring.
                if (params.reported_eps_enabled
                        or params.reported_eps_display_only):
                    row["reported_eps"] = mr.get("reported_eps")
                if (params.surprise_eps_dollar_enabled
                        or params.surprise_eps_dollar_display_only):
                    row["surprise_eps_dollar"] = mr.get("surprise_eps")
                if (params.surprise_eps_pct_enabled
                        or params.surprise_eps_pct_display_only):
                    row["surprise_eps_pct"] = mr.get("surprise_eps_pct")
                if (params.reported_rev_enabled
                        or params.reported_rev_display_only):
                    row["reported_rev"] = mr.get("reported_rev")
                if (params.surprise_rev_dollar_enabled
                        or params.surprise_rev_dollar_display_only):
                    row["surprise_rev_dollar"] = mr.get("surprise_rev")
                if (params.surprise_rev_pct_enabled
                        or params.surprise_rev_pct_display_only):
                    row["surprise_rev_pct"] = mr.get("surprise_rev_pct")
                if (params.yoy_eps_pct_enabled
                        or params.yoy_eps_pct_display_only):
                    row["yoy_eps_pct"] = mr.get("yoy_eps_pct")
                if (params.yoy_rev_pct_enabled
                        or params.yoy_rev_pct_display_only):
                    row["yoy_rev_pct"] = mr.get("yoy_rev_pct")
                # last_report_date: include ONLY when at least one
                # individual earnings stat is active AND no beat is
                # active. When beats is on, the Q-1 Date column shows
                # the same date — last_report_date would be pure
                # redundancy.
                _individual_active = (
                    params.reported_eps_enabled
                    or params.reported_eps_display_only
                    or params.surprise_eps_dollar_enabled
                    or params.surprise_eps_dollar_display_only
                    or params.surprise_eps_pct_enabled
                    or params.surprise_eps_pct_display_only
                    or params.reported_rev_enabled
                    or params.reported_rev_display_only
                    or params.surprise_rev_dollar_enabled
                    or params.surprise_rev_dollar_display_only
                    or params.surprise_rev_pct_enabled
                    or params.surprise_rev_pct_display_only
                    or params.yoy_eps_pct_enabled
                    or params.yoy_eps_pct_display_only
                    or params.yoy_rev_pct_enabled
                    or params.yoy_rev_pct_display_only
                )
                _beats_active = (
                    params.consec_eps_beats_enabled
                    or params.consec_eps_beats_display_only
                    or params.consec_rev_beats_enabled
                    or params.consec_rev_beats_display_only
                )
                if _individual_active and not _beats_active:
                    row["last_report_date"] = mr.get("report_date")

                # Beats streaks computed only on the past slice — future
                # quarters relative to end_ts must not contribute, so
                # historical replays produce point-in-time-correct counts.
                # Phase 8 §8.3: when beats are active, also stash up to
                # MAX_BEATS_QUARTERS most-recent quarters per metric as
                # `qK_*` flat columns. The table model uses these to
                # render the multi-quarter wide-format display.
                # `consec_*_beats_quarter_cap` (default 0 = no cap)
                # narrows the per-side population to the requested
                # quarter count — useful when the user only cares about
                # the last 4 quarters and doesn't want 20 columns of
                # noise. Cap is independent per side so EPS and Rev
                # can use different limits.
                MAX_BEATS_QUARTERS = 20
                _eps_cap = params.consec_eps_beats_quarter_cap
                _eps_n = MAX_BEATS_QUARTERS if _eps_cap <= 0 else min(_eps_cap, MAX_BEATS_QUARTERS)
                _rev_cap = params.consec_rev_beats_quarter_cap
                _rev_n = MAX_BEATS_QUARTERS if _rev_cap <= 0 else min(_rev_cap, MAX_BEATS_QUARTERS)
                if params.consec_eps_beats_enabled or params.consec_eps_beats_display_only:
                    from .earnings_history import compute_consecutive_beats
                    row["consec_eps_beats"] = compute_consecutive_beats(
                        past_pref, "eps", params.consec_eps_beats_threshold_pct,
                    )
                    for k, (_, q) in enumerate(
                        past_pref.head(_eps_n).iterrows(), 1
                    ):
                        # Per-block date key so the EPS-streak and
                        # Rev-streak green-highlight logic can color
                        # each block's date cells independently. Same
                        # underlying value (a quarterly report covers
                        # both EPS and Rev) but rendered twice when
                        # both blocks are visible — that gives clean
                        # within-block visual scanning.
                        row[f"q{k}_report_date_eps"] = q.get("report_date")
                        row[f"q{k}_reported_eps"] = q.get("reported_eps")
                        row[f"q{k}_surprise_eps_dollar"] = q.get("surprise_eps")
                        row[f"q{k}_surprise_eps_pct"] = q.get("surprise_eps_pct")
                        row[f"q{k}_yoy_eps_pct"] = q.get("yoy_eps_pct")
                if params.consec_rev_beats_enabled or params.consec_rev_beats_display_only:
                    from .earnings_history import compute_consecutive_beats
                    row["consec_rev_beats"] = compute_consecutive_beats(
                        past_pref, "rev", params.consec_rev_beats_threshold_pct,
                    )
                    for k, (_, q) in enumerate(
                        past_pref.head(_rev_n).iterrows(), 1
                    ):
                        row[f"q{k}_report_date_rev"] = q.get("report_date")
                        row[f"q{k}_reported_rev"] = q.get("reported_rev")
                        row[f"q{k}_surprise_rev_dollar"] = q.get("surprise_rev")
                        row[f"q{k}_surprise_rev_pct"] = q.get("surprise_rev_pct")
                        row[f"q{k}_yoy_rev_pct"] = q.get("yoy_rev_pct")

            # Earnings-aligned date highlight: stash the matched dates
            # as ISO date strings (YYYY-MM-DD). The table model reads
            # this list to color any date cell whose value falls in the
            # set blue. The leading underscore marks this as
            # table-internal — RESULT_COLUMNS doesn't surface it as a
            # visible column. Compared against the FULL 5-year history
            # (report_dates_all above), not just past_desc — older
            # earnings still get flagged when an indicator date matches.
            if aligned_dates:
                row["_earnings_aligned_dates"] = sorted(
                    d.date().isoformat() for d in aligned_dates
                )
                # Only emit the canonical map when fuzzy matching
                # actually happened (some matched indicator date is
                # NOT identical to its canonical). Saves widget work
                # in the common exact-match case AND keeps the row
                # payload lean for legacy-shaped consumers.
                if any(k != v for k, v in canonical_map.items()):
                    row["_earnings_aligned_canon"] = canonical_map

    # Phase 8 §8.5 (revised): compute per-cell pass/fail for every
    # filter currently in display-only mode. The display-only filter
    # stages are SKIPPED in the funnel (`_build_filter_stages`) so
    # tickers aren't dropped, but the table still wants to mark cells
    # that would have failed in red. Stash the result for the widget.
    fails = _compute_display_only_fails(params, row)
    if fails:
        row["_display_only_fails"] = fails

    return row


def _compute_display_only_fails(
    params: "ScanParams", row: dict,
) -> dict[str, bool]:
    """For every filter currently in display-only mode, evaluate
    whether `row`'s value WOULD have failed the filter. Returns a
    `{column_key: True}` dict for cells that would have been filtered
    out. Cells the filter would have admitted (or whose data is
    absent) are NOT in the dict — the widget treats absence as
    "render in default color." Mirrors `_build_filter_stages` for the
    subset of filters that are gated on `*_display_only`."""
    fails: dict[str, bool] = {}

    def _flag_min(do_attr_prefix: str, col: str, thresh_attr: str):
        """`col >= threshold` → fail when col < threshold."""
        if not getattr(params, f"{do_attr_prefix}_display_only", False):
            return
        v = row.get(col)
        if v is None or (isinstance(v, float) and math.isnan(v)) or pd.isna(v):
            return
        try:
            if float(v) < float(getattr(params, thresh_attr)):
                fails[col] = True
        except (TypeError, ValueError):
            pass

    def _flag_max(do_attr_prefix: str, col: str, thresh_attr: str):
        """`col <= threshold` → fail when col > threshold."""
        if not getattr(params, f"{do_attr_prefix}_display_only", False):
            return
        v = row.get(col)
        if v is None or (isinstance(v, float) and math.isnan(v)) or pd.isna(v):
            return
        try:
            if float(v) > float(getattr(params, thresh_attr)):
                fails[col] = True
        except (TypeError, ValueError):
            pass

    # Min-threshold filters (col >= threshold).
    _flag_min("avg_vol", "avg_vol", "avg_vol_min")
    _flag_min("dollar_vol", "dollar_vol", "dollar_vol_min")
    _flag_min("sti", "sti", "sti_threshold")
    _flag_min("pct_gain", "pct_gain", "pct_gain_min")
    _flag_min("consec_gaps", "consec_gaps", "consec_gaps_min")
    _flag_min("consec_gaps_down", "consec_gaps_down", "consec_gaps_down_min")
    _flag_min("current_gap", "current_gap_pct", "current_gap_min_pct")
    _flag_min("max_gap", "max_gap_pct", "max_gap_min_pct")
    _flag_min("surge", "surge_pct", "surge_min_pct")
    _flag_min("adr", "adr_pct", "adr_min_pct")
    _flag_min("rvol", "rvol", "rvol_min")
    _flag_min("rs_market", "rs_market", "rs_market_min")
    _flag_min("rs_nasdaq", "rs_nasdaq", "rs_nasdaq_min")
    _flag_min("rs_sector", "rs_sector", "rs_sector_min")
    _flag_min("reported_eps", "reported_eps", "reported_eps_min")
    _flag_min("surprise_eps_dollar", "surprise_eps_dollar", "surprise_eps_dollar_min")
    _flag_min("surprise_eps_pct", "surprise_eps_pct", "surprise_eps_pct_min")
    _flag_min("reported_rev", "reported_rev", "reported_rev_min")
    _flag_min("surprise_rev_dollar", "surprise_rev_dollar", "surprise_rev_dollar_min")
    _flag_min("surprise_rev_pct", "surprise_rev_pct", "surprise_rev_pct_min")
    _flag_min("yoy_eps_pct", "yoy_eps_pct", "yoy_eps_pct_min")
    _flag_min("yoy_rev_pct", "yoy_rev_pct", "yoy_rev_pct_min")
    _flag_min("consec_eps_beats", "consec_eps_beats", "consec_eps_beats_min")
    _flag_min("consec_rev_beats", "consec_rev_beats", "consec_rev_beats_min")

    # Max-threshold filters (col <= threshold).
    _flag_max("dist_high", "dist_high_pct", "dist_high_max_pct")
    _flag_max("max_neg_gap", "max_neg_gap_pct", "max_neg_gap_min_pct")
    _flag_max("bbw", "bbw", "bbw_max")
    _flag_max("atr_ratio", "atr_ratio", "atr_max_ratio")
    _flag_max("vol_dryup", "vol_dryup", "vol_dryup_max_ratio")

    # SMA filters: close > sma{period}. Column key varies with period.
    if params.sma1_display_only:
        sma_col = f"sma{params.sma1_period}"
        sma_v = row.get(sma_col)
        close_v = row.get("close")
        if (sma_v is not None and not pd.isna(sma_v)
                and close_v is not None and not pd.isna(close_v)
                and close_v <= sma_v):
            fails[sma_col] = True
    if params.sma2_display_only:
        sma_col = f"sma{params.sma2_period}"
        sma_v = row.get(sma_col)
        close_v = row.get("close")
        if (sma_v is not None and not pd.isna(sma_v)
                and close_v is not None and not pd.isna(close_v)
                and close_v <= sma_v):
            fails[sma_col] = True

    # ATR: range filter atr_min <= atr <= atr_max.
    if params.atr_display_only:
        v = row.get("atr")
        if v is not None and not pd.isna(v):
            if not (params.atr_min <= float(v) <= params.atr_max):
                fails["atr"] = True

    # NaN-handling for the earnings filters below uses the two global
    # toggles `earnings_dates_only` (gates days_since/until columns) and
    # `earnings_data_only` (gates the 6 per-quarter Zacks columns).
    # True ⇒ NaN → fail (red); False ⇒ NaN → pass (no flag).
    # Dates-vs-data invariant: a row with any earnings-data value is
    # considered to "have a date" too (data implies date), so the dates
    # check skips the NaN-fail flag for those rows.
    _dates_only = params.earnings_dates_only
    _data_only = params.earnings_data_only

    _data_cols_for_invariant = (
        "reported_eps", "surprise_eps_dollar", "surprise_eps_pct",
        "reported_rev", "surprise_rev_dollar", "surprise_rev_pct",
    )
    _row_has_any_data = any(
        (row.get(c) is not None) and (not pd.isna(row.get(c)))
        for c in _data_cols_for_invariant
    )

    # Days Since ER: range.
    if params.days_since_earnings_display_only:
        v = row.get("days_since_er")
        if v is None or pd.isna(v):
            if _dates_only and not _row_has_any_data:
                fails["days_since_er"] = True
        else:
            if not (params.days_since_min <= float(v) <= params.days_since_max):
                fails["days_since_er"] = True

    # Days Until ER (min): >= days_until_min.
    if params.days_until_earnings_display_only:
        v = row.get("days_until_er")
        if v is None or pd.isna(v):
            if _dates_only and not _row_has_any_data:
                fails["days_until_er"] = True
        else:
            if float(v) < params.days_until_min:
                fails["days_until_er"] = True

    # Days Until ER (max): 0 <= days <= days_until_max.
    # If both min and max display-only flags target this column, we
    # OR the failures together — either condition failing flags red.
    if params.days_until_max_display_only:
        v = row.get("days_until_er")
        if v is None or pd.isna(v):
            if _dates_only and not _row_has_any_data:
                fails["days_until_er"] = True
        else:
            if not (0 <= float(v) <= params.days_until_max):
                fails["days_until_er"] = True

    # Per-quarter earnings (#20-#25): NaN → red iff earnings_data_only.
    # Mirrors the funnel-filter semantics so the red-on-fail signal
    # is consistent with what the filter would actually have done.
    if _data_only:
        for do_attr_prefix, col in (
            ("reported_eps", "reported_eps"),
            ("surprise_eps_dollar", "surprise_eps_dollar"),
            ("surprise_eps_pct", "surprise_eps_pct"),
            ("reported_rev", "reported_rev"),
            ("surprise_rev_dollar", "surprise_rev_dollar"),
            ("surprise_rev_pct", "surprise_rev_pct"),
        ):
            if not getattr(params, f"{do_attr_prefix}_display_only", False):
                continue
            v = row.get(col)
            if v is None or pd.isna(v):
                fails[col] = True

    return fails


# ============================================================================
# Funnel filter stages
# ============================================================================

def _build_filter_stages(params: ScanParams) -> list[tuple[str, Callable]]:
    """
    Build an ordered list of (stage_name, filter_func) pairs.
    Each filter_func takes a DataFrame of computed values and returns
    a boolean mask.  Only enabled filters are included.
    """
    stages = []

    # #12 Min price — run early to drop pennystocks fast
    if params.min_price_enabled:
        stages.append((
            f"Min Price (${params.min_price_floor:.0f})",
            lambda df, p=params: df["price"] >= p.min_price_floor,
        ))

    # #13 Avg volume
    if params.avg_vol_enabled and not params.avg_vol_display_only:
        stages.append((
            f"Avg Volume >= {params.avg_vol_min:,.0f}",
            lambda df, p=params: df["avg_vol"] >= p.avg_vol_min,
        ))

    # #14 Dollar volume
    if params.dollar_vol_enabled and not params.dollar_vol_display_only:
        stages.append((
            f"Dollar Vol >= ${params.dollar_vol_min:,.0f}",
            lambda df, p=params: df["dollar_vol"] >= p.dollar_vol_min,
        ))

    # #1a SMA (first)
    if params.sma1_enabled and not params.sma1_display_only:
        col = f"sma{params.sma1_period}"
        stages.append((
            f"Above {params.sma1_period} SMA",
            lambda df, c=col: df["close"] > df[c],
        ))

    # #1b SMA (second)
    if params.sma2_enabled and not params.sma2_display_only:
        col = f"sma{params.sma2_period}"
        stages.append((
            f"Above {params.sma2_period} SMA",
            lambda df, c=col: df["close"] > df[c],
        ))

    # #2 Stockbee TI
    if params.sti_enabled and not params.sti_display_only:
        stages.append((
            f"Stockbee TI >= {params.sti_threshold}",
            lambda df, p=params: df["sti"] >= p.sti_threshold,
        ))

    # #3 Distance from high
    if params.dist_high_enabled and not params.dist_high_display_only:
        stages.append((
            f"Within {params.dist_high_max_pct}% of High",
            lambda df, p=params: df["dist_high_pct"] <= p.dist_high_max_pct,
        ))

    # #4 % Gain
    if params.pct_gain_enabled and not params.pct_gain_display_only:
        stages.append((
            f"% Gain >= {params.pct_gain_min}%",
            lambda df, p=params: df["pct_gain"] >= p.pct_gain_min,
        ))

    # #5 Top percentile (applied AFTER #4, universe-wide)
    # Handled separately in run_scan because it needs the full distribution

    # #6 Consecutive gaps
    if params.consec_gaps_enabled and not params.consec_gaps_display_only:
        stages.append((
            f"Consec Gaps >= {params.consec_gaps_min}",
            lambda df, p=params: df["consec_gaps"] >= p.consec_gaps_min,
        ))

    # #6b Consecutive gap-downs
    if params.consec_gaps_down_enabled and not params.consec_gaps_down_display_only:
        stages.append((
            f"Consec Gaps Down >= {params.consec_gaps_down_min}",
            lambda df, p=params: df["consec_gaps_down"] >= p.consec_gaps_down_min,
        ))

    # #7 Current gap %
    if params.current_gap_enabled and not params.current_gap_display_only:
        stages.append((
            f"Current Gap >= {params.current_gap_min_pct}%",
            lambda df, p=params: df["current_gap_pct"] >= p.current_gap_min_pct,
        ))

    # #7c Max Positive Gap
    if params.max_gap_enabled and not params.max_gap_display_only:
        stages.append((
            f"Max Gap >= {params.max_gap_min_pct}%",
            lambda df, p=params: df["max_gap_pct"] >= p.max_gap_min_pct,
        ))

    # #7d Max Negative Gap
    if params.max_neg_gap_enabled and not params.max_neg_gap_display_only:
        stages.append((
            f"Max Neg Gap <= {params.max_neg_gap_min_pct}%",
            lambda df, p=params: df["max_neg_gap_pct"] <= p.max_neg_gap_min_pct,
        ))

    # #7b Surge Detection
    if params.surge_enabled and not params.surge_display_only:
        if params.surge_mode == "trend":
            label = (
                f"Surge >= {params.surge_min_pct}% "
                f"(trend-continuous, max DD {params.surge_max_drawdown_pct}%)"
            )
        elif params.surge_mode == "ignition":
            label = (
                f"Surge >= {params.surge_min_pct}% "
                f"(ignition, max DD {params.surge_max_drawdown_pct}%, "
                f"vol >= {params.surge_ignition_vol_mult}x, "
                f"day >= {params.surge_ignition_min_pct}%)"
            )
        elif params.surge_mode == "high_low":
            label = f"Surge >= {params.surge_min_pct}% in {params.surge_days}d (H/L)"
        else:
            label = f"Surge >= {params.surge_min_pct}% in {params.surge_days}d (C/C)"
        stages.append((
            label,
            lambda df, p=params: df["surge_pct"] >= p.surge_min_pct,
        ))

    # #8 ADR% (momentum — minimum)
    if params.adr_enabled and not params.adr_display_only:
        stages.append((
            f"ADR% >= {params.adr_min_pct}%",
            lambda df, p=params: df["adr_pct"] >= p.adr_min_pct,
        ))

    # #8b ATR (absolute dollar range)
    if params.atr_enabled and not params.atr_display_only:
        stages.append((
            f"ATR ${params.atr_min:.2f}–${params.atr_max:.2f}",
            lambda df, p=params: (df["atr"] >= p.atr_min) & (df["atr"] <= p.atr_max),
        ))

    # #9 BBW
    if params.bbw_enabled and not params.bbw_display_only:
        stages.append((
            f"BBW <= {params.bbw_max}",
            lambda df, p=params: df["bbw"] <= p.bbw_max,
        ))

    # #10 ATR ratio
    if params.atr_ratio_enabled and not params.atr_ratio_display_only:
        stages.append((
            f"ATR Ratio <= {params.atr_max_ratio}",
            lambda df, p=params: df["atr_ratio"] <= p.atr_max_ratio,
        ))

    # #11 Volume dry-up
    if params.vol_dryup_enabled and not params.vol_dryup_display_only:
        stages.append((
            f"Vol Dry-Up <= {params.vol_dryup_max_ratio}",
            lambda df, p=params: df["vol_dryup"] <= p.vol_dryup_max_ratio,
        ))

    # #14b RVOL — NaN fails via the >= comparison (standard convention).
    if params.rvol_enabled and not params.rvol_display_only:
        stages.append((
            f"RVOL >= {params.rvol_min:.1f}",
            lambda df, p=params: df["rvol"] >= p.rvol_min,
        ))

    # #15 RS vs S&P 500. Fail CLOSED when the rs_market column is absent — that
    # happens only when the SPY benchmark failed to load so NO ticker got an
    # rs_market value. An explicitly-enabled gate with no data must drop rows
    # (a loud, visible empty result) rather than let the funnel's per-stage
    # try/except swallow the KeyError and silently pass EVERY ticker as if the
    # gate were applied. (RS-Sector below intentionally relaxes NaN; do not
    # copy that here — these gates fail NaN via the comparison.)
    if params.rs_market_enabled and not params.rs_market_display_only:
        stages.append((
            f"RS S&P >= {params.rs_market_min}",
            lambda df, p=params: (
                df["rs_market"] >= p.rs_market_min
                if "rs_market" in df.columns
                else pd.Series(False, index=df.index)
            ),
        ))

    # #15b RS vs NASDAQ — same fail-closed guard as #15 (ONEQ benchmark).
    if params.rs_nasdaq_enabled and not params.rs_nasdaq_display_only:
        stages.append((
            f"RS NASDAQ >= {params.rs_nasdaq_min}",
            lambda df, p=params: (
                df["rs_nasdaq"] >= p.rs_nasdaq_min
                if "rs_nasdaq" in df.columns
                else pd.Series(False, index=df.index)
            ),
        ))

    # #16 RS vs Sector (missing data passes)
    if params.rs_sector_enabled and not params.rs_sector_display_only:
        stages.append((
            f"RS Sector >= {params.rs_sector_min}",
            lambda df, p=params: (df["rs_sector"] >= p.rs_sector_min) | df["rs_sector"].isna(),
        ))

    # NaN-handling for the 9 earnings filters below is driven by TWO
    # global flags: `earnings_dates_only` for the days_since/days_until
    # date-derived filters, and `earnings_data_only` for the 6
    # per-quarter Zacks data filters + consec-beats. False (default) =
    # NaN passes cleanly so non-earnings filters can still operate on
    # tickers Zacks doesn't cover. True = NaN fails.
    #
    # Dates-vs-data invariant: a ticker with any earnings DATA value
    # is treated as having a date too (data implies date), so the
    # dates filter never drops a row that the data filter would keep.
    # `_data_present_mask(df)` returns a Series[bool] flagging rows
    # with at least one non-NaN value across the 6 most-recent-quarter
    # Zacks columns. Used to relax NaN-fail for the dates filters.
    _dates_nan_passes = not params.earnings_dates_only
    _data_nan_passes = not params.earnings_data_only

    _DATA_COVERAGE_COLS = (
        "reported_eps", "surprise_eps_dollar", "surprise_eps_pct",
        "reported_rev", "surprise_rev_dollar", "surprise_rev_pct",
        "yoy_eps_pct", "yoy_rev_pct",
    )

    def _data_present_mask(df):
        present = [c for c in _DATA_COVERAGE_COLS if c in df.columns]
        if not present:
            return pd.Series(False, index=df.index)
        return df[present].notna().any(axis=1)

    # #17 Days since last earnings
    if params.days_since_earnings_enabled and not params.days_since_earnings_display_only:
        stages.append((
            f"Days Since ER {params.days_since_min}–{params.days_since_max}",
            lambda df, p=params, n=_dates_nan_passes: (
                (df["days_since_er"] >= p.days_since_min)
                & (df["days_since_er"] <= p.days_since_max)
            ) | (df["days_since_er"].isna() & (n | _data_present_mask(df))),
        ))

    # #18 Days until next earnings (min)
    if params.days_until_earnings_enabled and not params.days_until_earnings_display_only:
        stages.append((
            f"Days Until ER >= {params.days_until_min}",
            lambda df, p=params, n=_dates_nan_passes: (
                df["days_until_er"] >= p.days_until_min
            ) | (df["days_until_er"].isna() & (n | _data_present_mask(df))),
        ))

    # #19 Days until next earnings (max — only tickers reporting within N days)
    if params.days_until_max_enabled and not params.days_until_max_display_only:
        stages.append((
            f"Days Until ER <= {params.days_until_max}",
            lambda df, p=params, n=_dates_nan_passes: (
                (df["days_until_er"] >= 0) & (df["days_until_er"] <= p.days_until_max)
            ) | (df["days_until_er"].isna() & (n | _data_present_mask(df))),
        ))

    # --- Per-quarter Zacks earnings filters (Phase 7 §7.1) ---
    # Each filter is bypassed when the corresponding Consecutive Beats
    # filter is active AS A FILTER (§7.2 grey-out semantics — the
    # beats streak alone determines pass/fail; individual quarter
    # thresholds are advisory output only). Phase 8 §8.5 relaxation:
    # if consec_*_beats is in display-only mode, the streak isn't a
    # filter at all, so the individual filters DO apply. Likewise, the
    # individual filter is skipped when its own display_only is on.
    eps_beats_is_active_filter = (
        params.consec_eps_beats_enabled and not params.consec_eps_beats_display_only
    )
    rev_beats_is_active_filter = (
        params.consec_rev_beats_enabled and not params.consec_rev_beats_display_only
    )

    def _min_filter(col: str, threshold: float, nan_passes: bool):
        """Build a 'col >= threshold OR (NaN AND nan_passes)' mask fn.
        `nan_passes` comes from the global `earnings_data_only` flag —
        False means NaN tickers fail (earnings-required), True means
        NaN tickers pass (earnings-optional)."""
        def mask(df, c=col, t=threshold, n=nan_passes):
            if c not in df.columns:
                # No column means no Zacks data anywhere — pass all
                # when nan_passes is True, fail all when False.
                return pd.Series([n] * len(df), index=df.index)
            v = df[c]
            return (v >= t) | (v.isna() & n)
        return mask

    # #20  Reported EPS (min)
    if (params.reported_eps_enabled and not params.reported_eps_display_only
            and not eps_beats_is_active_filter):
        stages.append((
            f"Reported EPS >= {params.reported_eps_min:g}",
            _min_filter("reported_eps", params.reported_eps_min, _data_nan_passes),
        ))

    # #21  Surprise EPS $ (min)
    if (params.surprise_eps_dollar_enabled and not params.surprise_eps_dollar_display_only
            and not eps_beats_is_active_filter):
        stages.append((
            f"Surprise EPS $ >= {params.surprise_eps_dollar_min:g}",
            _min_filter("surprise_eps_dollar", params.surprise_eps_dollar_min, _data_nan_passes),
        ))

    # #22  Surprise EPS % (min)
    if (params.surprise_eps_pct_enabled and not params.surprise_eps_pct_display_only
            and not eps_beats_is_active_filter):
        stages.append((
            f"Surprise EPS % >= {params.surprise_eps_pct_min:g}",
            _min_filter("surprise_eps_pct", params.surprise_eps_pct_min, _data_nan_passes),
        ))

    # #23  Reported Revenue (min)
    if (params.reported_rev_enabled and not params.reported_rev_display_only
            and not rev_beats_is_active_filter):
        stages.append((
            f"Reported Rev >= {params.reported_rev_min:g}",
            _min_filter("reported_rev", params.reported_rev_min, _data_nan_passes),
        ))

    # #24  Surprise Revenue $ (min)
    if (params.surprise_rev_dollar_enabled and not params.surprise_rev_dollar_display_only
            and not rev_beats_is_active_filter):
        stages.append((
            f"Surprise Rev $ >= {params.surprise_rev_dollar_min:g}",
            _min_filter("surprise_rev_dollar", params.surprise_rev_dollar_min, _data_nan_passes),
        ))

    # #25  Surprise Revenue % (min)
    if (params.surprise_rev_pct_enabled and not params.surprise_rev_pct_display_only
            and not rev_beats_is_active_filter):
        stages.append((
            f"Surprise Rev % >= {params.surprise_rev_pct_min:g}",
            _min_filter("surprise_rev_pct", params.surprise_rev_pct_min, _data_nan_passes),
        ))

    # #26  YoY EPS % (min). Same NaN-passes contract as the surprise
    # filters: when earnings_data_only is on, NaN fails; otherwise NaN
    # passes (no prior-year data is not the ticker's "fault").
    if (params.yoy_eps_pct_enabled and not params.yoy_eps_pct_display_only
            and not eps_beats_is_active_filter):
        stages.append((
            f"YoY EPS % >= {params.yoy_eps_pct_min:g}",
            _min_filter("yoy_eps_pct", params.yoy_eps_pct_min, _data_nan_passes),
        ))

    # #27  YoY Revenue % (min)
    if (params.yoy_rev_pct_enabled and not params.yoy_rev_pct_display_only
            and not rev_beats_is_active_filter):
        stages.append((
            f"YoY Rev % >= {params.yoy_rev_pct_min:g}",
            _min_filter("yoy_rev_pct", params.yoy_rev_pct_min, _data_nan_passes),
        ))

    # --- Consecutive Beats filters (Phase 7 §7.3) ---
    # When active as a filter, individual EPS/Rev filters above are
    # skipped — the streak count is the sole gate. NaN streak (no
    # Zacks history at all) does NOT pass; the user's intent here is
    # "tickers with ≥N consecutive beats", which has no meaningful
    # "include_no_data" behavior. Phase 8 §8.5: when display-only,
    # the streak is computed and shown but no filter is appended.

    if eps_beats_is_active_filter:
        stages.append((
            f"Consec EPS Beats >= {params.consec_eps_beats_min} "
            f"(>{params.consec_eps_beats_threshold_pct:g}%)",
            lambda df, p=params: (
                df["consec_eps_beats"] >= p.consec_eps_beats_min
                if "consec_eps_beats" in df.columns
                else pd.Series([False] * len(df), index=df.index)
            ),
        ))

    if rev_beats_is_active_filter:
        stages.append((
            f"Consec Rev Beats >= {params.consec_rev_beats_min} "
            f"(>{params.consec_rev_beats_threshold_pct:g}%)",
            lambda df, p=params: (
                df["consec_rev_beats"] >= p.consec_rev_beats_min
                if "consec_rev_beats" in df.columns
                else pd.Series([False] * len(df), index=df.index)
            ),
        ))

    return stages


# ============================================================================
# Main scan entry point
# ============================================================================

def run_scan(
    symbols: list[str],
    params: ScanParams,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
    cancel_token: Optional[Callable[[], bool]] = None,
) -> ScanResult:
    """
    Run the full scan pipeline:
      1. Compute all indicator values for every symbol
      2. Apply funnel filters stage by stage
      3. Return ScanResult with results table, funnel log, errors

    progress_cb(done, total, symbol) is called after each ticker computation.
    cancel_token() is polled in the per-ticker compute loop; returning True
    interrupts the scan mid-flight and returns partial results (Phase 4 R2).
    """
    t0 = time.time()
    result = ScanResult(params=params)

    log.info("=" * 60)
    log.info("Scan started: %d tickers, window %s to %s",
             len(symbols), params.start_date, params.end_date)
    log.info("=" * 60)

    # ── Pre-load benchmark OHLCV (SPY + sector ETFs) ──
    # Phase 2 I3: tz-strip + end-date slice done ONCE here, not per-ticker.
    # Copy before mutating so a future LRU cache (Phase 3 I11) stays intact.
    end_ts = pd.Timestamp(params.end_date)
    benchmark_data: dict[str, pd.DataFrame] = {}
    if params.rs_market_enabled or params.rs_nasdaq_enabled or params.rs_sector_enabled:
        for ref in config.REFERENCE_TICKERS:
            ref_df = load_ohlcv(ref)
            if ref_df is None:
                continue
            if ref_df.index.tz is not None:
                ref_df = ref_df.copy()
                ref_df.index = ref_df.index.tz_localize(None)
            benchmark_data[ref] = ref_df.loc[:end_ts]
        log.info("Loaded %d/%d benchmark tickers for RS",
                 len(benchmark_data), len(config.REFERENCE_TICKERS))

    # ── Pre-load sector map and earnings cache as O(1) lookup dicts ──
    # Phase 2 I4: dict lookup replaces per-ticker DataFrame.loc filtering
    sector_lookup: dict[str, str] = {}
    if params.rs_sector_enabled:
        from .sector_map import load_sector_map
        sector_map_df = load_sector_map()
        if sector_map_df is None or sector_map_df.empty:
            log.warning("No sector_map.parquet — RS Sector will pass all")
        else:
            sector_lookup = {
                str(t): str(e)
                for t, e in zip(
                    sector_map_df["ticker"],
                    sector_map_df["sector_etf"].fillna(""),
                )
                if e
            }

    # ── Pre-load earnings_history per-ticker as O(1) lookup ──
    # Audit H2: always load earnings_history when the parquet exists, not
    # just when a Phase 7 filter is on. Per spec §9.2 step 2 the seven
    # single-quarter columns must surface even on a filterless scan.
    # When the parquet is missing the lookup stays empty and per-quarter
    # values come back NaN — no log warning since this is the expected
    # state on a fresh install before the first Zacks fill runs.
    earnings_history_lookup: dict[str, pd.DataFrame] = {}
    from .earnings_history import (
        load_earnings_history, dedupe_history, compute_yoy_columns,
    )
    history_df = load_earnings_history()
    if history_df is not None and not history_df.empty:
        # Per-(ticker, period_ending) dedup is a READ-side responsibility
        # — the on-disk parquet legitimately carries multiple source rows
        # per quarter (zacks + finnhub). Without this, each fiscal quarter
        # appeared twice in the per-ticker slice, so the most-recent-
        # quarter columns and the Q-i beats display picked up the wrong
        # source row and shifted by a quarter. `dedupe_history` collapses
        # each slot to its highest-priority source; `backfill_estimates=
        # True` inherits the estimate/surprise fields the winner lacks
        # from the same-slot lower-priority row (same adjusted basis).
        history_df = dedupe_history(history_df, backfill_estimates=True)
        # Recompute YoY on the deduped frame so the EPS YoY is derived
        # from the reported_eps actually displayed (one winner per slot)
        # rather than the pre-dedup multi-source value stored on disk.
        history_df = compute_yoy_columns(history_df)
        # Audit M4: sort by report_date DESC so per-ticker slices line
        # up with what `compute_consecutive_beats` and the table's Q-i
        # column ordering both use.
        history_df = history_df.sort_values(
            ["ticker", "report_date"], ascending=[True, False],
        )
        for tk, sub in history_df.groupby("ticker", sort=False):
            earnings_history_lookup[str(tk)] = sub.reset_index(drop=True)
        log.info("Loaded earnings_history for %d tickers",
                 len(earnings_history_lookup))

    # Audit BUG-7 fix: load earnings_dates.parquet whenever any of the
    # three earnings-date filters is on as EITHER an active filter OR
    # display-only. Without the `_display_only` flags here, a user who
    # turned on display-only-only mode for these filters would get
    # empty `days_since_er` / `days_until_er` columns — the per-ticker
    # block at `_compute_ticker` is gated by `... and earnings_lookup`,
    # so an empty lookup means the values never compute.
    earnings_lookup: dict[str, tuple] = {}
    if (params.days_since_earnings_enabled
            or params.days_since_earnings_display_only
            or params.days_until_earnings_enabled
            or params.days_until_earnings_display_only
            or params.days_until_max_enabled
            or params.days_until_max_display_only):
        from .earnings_cache import load_earnings_cache
        earnings_df = load_earnings_cache()
        if earnings_df is None or earnings_df.empty:
            log.warning("No earnings_dates.parquet — earnings filters will pass all")
        else:
            for t, last_e, next_e in zip(
                earnings_df["ticker"],
                earnings_df["last_earnings"],
                earnings_df["next_earnings"],
            ):
                le = None if pd.isna(last_e) else pd.Timestamp(last_e)
                ne = None if pd.isna(next_e) else pd.Timestamp(next_e)
                earnings_lookup[str(t)] = (le, ne)

    # ── Phase A: Compute indicators for all tickers ──
    rows = []
    cancelled_at: Optional[int] = None
    for i, sym in enumerate(symbols, 1):
        # Phase 4 R2: check cancel_token every iteration so the GUI Stop
        # button can interrupt a long scan mid-flight (previously Stop only
        # worked between timeframes in a multi-timeframe scan).
        if cancel_token and cancel_token():
            cancelled_at = i
            log.info("Scan cancelled after %d/%d tickers", i - 1, len(symbols))
            break
        try:
            row = _compute_ticker(
                sym, params, benchmark_data, sector_lookup, earnings_lookup,
                earnings_history_lookup,
            )
            if row is not None:
                rows.append(row)
        except Exception as exc:
            result.errors.append({
                "symbol": sym,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            })
            log.debug("Error computing %s: %s", sym, exc)

        if progress_cb and (i % 200 == 0 or i == len(symbols)):
            progress_cb(i, len(symbols), sym)

    if not rows:
        log.warning("No tickers produced computable results.")
        result.elapsed_sec = time.time() - t0
        return result

    computed = pd.DataFrame(rows)
    # Exclude reference/benchmark tickers from results
    ref_set = set(config.REFERENCE_TICKERS)
    computed = computed[~computed["symbol"].isin(ref_set)].reset_index(drop=True)
    total_computed = len(computed)
    log.info("Computed indicators for %d tickers (%d errors, %d no data)",
             total_computed, len(result.errors),
             len(symbols) - total_computed - len(result.errors))

    result.funnel.append(FunnelStage("Universe (computed)", total_computed, len(symbols)))

    # ── Phase B: Funnel filter stages ──
    stages = _build_filter_stages(params)

    current = computed.copy()
    for stage_name, filter_fn in stages:
        before = len(current)
        try:
            mask = filter_fn(current)
            # NaN values should fail the filter
            mask = mask.fillna(False)
            current = current[mask].copy()
        except Exception as exc:
            log.warning("Filter '%s' raised error: %s — skipping", stage_name, exc)
            continue
        result.funnel.append(FunnelStage(stage_name, len(current), before))
        log.info("  %s: %d -> %d", stage_name, before, len(current))

    # ── #5 Top percentile (universe-wide, applied after other filters) ──
    if params.top_pct_enabled and "pct_gain" in current.columns and len(current) > 0:
        before = len(current)
        cleaned = computed["pct_gain"].dropna().values  # FULL universe distribution
        if cleaned.size == 0:
            # No finite pct_gain anywhere (e.g. every ticker lacked lookback) —
            # np.nanpercentile([]) is undefined and would silently empty the
            # results. Skip the stage and say so rather than zeroing the scan.
            log.warning("Top-percentile stage skipped — no finite pct_gain "
                        "values in the universe distribution.")
        else:
            cutoff_value = np.nanpercentile(cleaned, 100 - params.top_pct_cutoff)
            current = current[current["pct_gain"] >= cutoff_value].copy()
            result.funnel.append(
                FunnelStage(f"Top {params.top_pct_cutoff}% Gain", len(current), before)
            )
            log.info("  Top %.0f%% Gain (cutoff=%.1f%%): %d -> %d",
                     params.top_pct_cutoff, cutoff_value, before, len(current))

    # ── Final ──
    result.funnel.append(FunnelStage("Final", len(current), len(current)))
    result.results_df = current.reset_index(drop=True)
    result.elapsed_sec = time.time() - t0

    log.info("=" * 60)
    log.info("Scan complete: %d results in %.1fs", len(current), result.elapsed_sec)
    log.info("Funnel: %s", result.funnel_summary())
    log.info("Errors: %d tickers", len(result.errors))
    log.info("=" * 60)

    return result
