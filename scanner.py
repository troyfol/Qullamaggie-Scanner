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
import time
import traceback
from dataclasses import dataclass, field
from datetime import date
from typing import Callable, Optional

import numpy as np
import pandas as pd

from . import config, indicators
from .data_engine import load_ohlcv

log = logging.getLogger("scanner.scan")


# ============================================================================
# Scan parameters — every toggle + threshold in one place
# ============================================================================

@dataclass
class ScanParams:
    """All tunable scanner parameters with sensible defaults."""

    # Date window
    start_date: date = date(2025, 1, 1)
    end_date: date = date(2026, 3, 4)

    # --- Trend Filters ---
    # #1a  Price above SMA (first SMA)
    sma1_enabled: bool = True
    sma1_period: int = 200

    # #1b  Price above SMA (second SMA)
    sma2_enabled: bool = True
    sma2_period: int = 50

    # #2   Stockbee Trend Intensity
    sti_enabled: bool = True
    sti_short_lb: int = 7
    sti_long_lb: int = 65
    sti_threshold: float = 1.05

    # #3   Distance from period high
    dist_high_enabled: bool = True
    dist_high_max_pct: float = 5.0  # within 5% of high

    # --- Momentum / Prior Move ---
    # #4   % Gain over period
    pct_gain_enabled: bool = True
    pct_gain_min: float = 20.0  # at least 20% gain

    # #5   Top X percentile (of #4 gains)
    top_pct_enabled: bool = False
    top_pct_cutoff: float = 10.0  # top 10%

    # #6   Consecutive gaps
    consec_gaps_enabled: bool = False
    consec_gaps_min: int = 2

    # #7   Current gap %
    current_gap_enabled: bool = False
    current_gap_min_pct: float = 2.0

    # #8   ADR% (momentum — looking for expansion)
    adr_enabled: bool = True
    adr_lookback: int = 14
    adr_min_pct: float = 3.0  # at least 3% ADR = momentum

    # --- Volatility Contraction ---

    # #9   Bollinger Band Width
    bbw_enabled: bool = False
    bbw_period: int = 20
    bbw_num_std: float = 2.0
    bbw_max: float = 0.10

    # #10  ATR Ratio
    atr_ratio_enabled: bool = False
    atr_short: int = 5
    atr_long: int = 50
    atr_max_ratio: float = 0.75

    # --- Volume / Liquidity ---
    # #11  Volume dry-up ratio
    vol_dryup_enabled: bool = False
    vol_dryup_recent: int = 10
    vol_dryup_prior: int = 20
    vol_dryup_max_ratio: float = 0.70

    # #12  Minimum price
    min_price_enabled: bool = True
    min_price_floor: float = 10.0

    # #13  Average volume (raw shares)
    avg_vol_enabled: bool = True
    avg_vol_lookback: int = 20
    avg_vol_min: float = 200_000

    # #14  Dollar volume
    dollar_vol_enabled: bool = True
    dollar_vol_lookback: int = 20
    dollar_vol_min: float = 5_000_000


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

def _compute_ticker(symbol: str, params: ScanParams) -> Optional[dict]:
    """
    Load OHLCV, slice to the date window, compute all indicators.
    Returns a dict of computed values or None if data is unusable.
    Raises on unexpected errors (caught by the caller).
    """
    df = load_ohlcv(symbol)
    if df is None or df.empty:
        return None

    # Ensure index is tz-naive for comparison
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)

    # Slice to date window
    start_ts = pd.Timestamp(params.start_date)
    end_ts = pd.Timestamp(params.end_date)
    window = df.loc[start_ts:end_ts]

    if window.empty or len(window) < 2:
        return None

    # We also need lookback data before the window for SMAs etc.
    # Use the full df up to end_date for lookback-dependent indicators
    full_to_end = df.loc[:end_ts]

    row = {"symbol": symbol}

    # --- #1a SMA (first) ---
    sma1 = indicators.price_above_sma(full_to_end, period=params.sma1_period)
    row["close"] = sma1["close"]
    row[f"sma{params.sma1_period}"] = sma1["sma_value"]

    # --- #1b SMA (second) ---
    sma2 = indicators.price_above_sma(full_to_end, period=params.sma2_period)
    row[f"sma{params.sma2_period}"] = sma2["sma_value"]

    # --- #2 Stockbee TI ---
    row["sti"] = indicators.stockbee_trend_intensity(
        full_to_end, short_lb=params.sti_short_lb, long_lb=params.sti_long_lb
    )

    # --- #3 Distance from high ---
    row["dist_high_pct"] = indicators.distance_from_period_high(window)

    # --- #4 % Gain ---
    row["pct_gain"] = indicators.pct_gain_over_period(window)

    # --- #6 Consecutive gaps ---
    row["consec_gaps"] = indicators.consecutive_gaps(window)

    # --- #7 Current gap % ---
    row["current_gap_pct"] = indicators.current_gap_pct(window)

    # --- #8 ADR% ---
    row["adr_pct"] = indicators.adr_pct(full_to_end, lookback=params.adr_lookback)

    # --- #9 BBW ---
    row["bbw"] = indicators.bollinger_band_width(
        full_to_end, period=params.bbw_period, num_std=params.bbw_num_std
    )

    # --- #10 ATR ratio ---
    row["atr_ratio"] = indicators.atr_ratio(
        full_to_end, short_period=params.atr_short, long_period=params.atr_long
    )

    # --- #11 Volume dry-up ---
    row["vol_dryup"] = indicators.volume_dryup_ratio(
        full_to_end, recent_n=params.vol_dryup_recent, prior_n=params.vol_dryup_prior
    )

    # --- #12 Min price ---
    row["price"] = indicators.min_price(window)

    # --- #13 Avg volume ---
    row["avg_vol"] = indicators.avg_volume(full_to_end, lookback=params.avg_vol_lookback)

    # --- #14 Dollar volume ---
    row["dollar_vol"] = indicators.avg_dollar_volume(
        full_to_end, lookback=params.dollar_vol_lookback
    )

    return row


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
    if params.avg_vol_enabled:
        stages.append((
            f"Avg Volume >= {params.avg_vol_min:,.0f}",
            lambda df, p=params: df["avg_vol"] >= p.avg_vol_min,
        ))

    # #14 Dollar volume
    if params.dollar_vol_enabled:
        stages.append((
            f"Dollar Vol >= ${params.dollar_vol_min:,.0f}",
            lambda df, p=params: df["dollar_vol"] >= p.dollar_vol_min,
        ))

    # #1a SMA (first)
    if params.sma1_enabled:
        col = f"sma{params.sma1_period}"
        stages.append((
            f"Above {params.sma1_period} SMA",
            lambda df, c=col: df["close"] > df[c],
        ))

    # #1b SMA (second)
    if params.sma2_enabled:
        col = f"sma{params.sma2_period}"
        stages.append((
            f"Above {params.sma2_period} SMA",
            lambda df, c=col: df["close"] > df[c],
        ))

    # #2 Stockbee TI
    if params.sti_enabled:
        stages.append((
            f"Stockbee TI >= {params.sti_threshold}",
            lambda df, p=params: df["sti"] >= p.sti_threshold,
        ))

    # #3 Distance from high
    if params.dist_high_enabled:
        stages.append((
            f"Within {params.dist_high_max_pct}% of High",
            lambda df, p=params: df["dist_high_pct"] <= p.dist_high_max_pct,
        ))

    # #4 % Gain
    if params.pct_gain_enabled:
        stages.append((
            f"% Gain >= {params.pct_gain_min}%",
            lambda df, p=params: df["pct_gain"] >= p.pct_gain_min,
        ))

    # #5 Top percentile (applied AFTER #4, universe-wide)
    # Handled separately in run_scan because it needs the full distribution

    # #6 Consecutive gaps
    if params.consec_gaps_enabled:
        stages.append((
            f"Consec Gaps >= {params.consec_gaps_min}",
            lambda df, p=params: df["consec_gaps"] >= p.consec_gaps_min,
        ))

    # #7 Current gap %
    if params.current_gap_enabled:
        stages.append((
            f"Current Gap >= {params.current_gap_min_pct}%",
            lambda df, p=params: df["current_gap_pct"] >= p.current_gap_min_pct,
        ))

    # #8 ADR% (momentum — minimum)
    if params.adr_enabled:
        stages.append((
            f"ADR% >= {params.adr_min_pct}%",
            lambda df, p=params: df["adr_pct"] >= p.adr_min_pct,
        ))

    # #9 BBW
    if params.bbw_enabled:
        stages.append((
            f"BBW <= {params.bbw_max}",
            lambda df, p=params: df["bbw"] <= p.bbw_max,
        ))

    # #10 ATR ratio
    if params.atr_ratio_enabled:
        stages.append((
            f"ATR Ratio <= {params.atr_max_ratio}",
            lambda df, p=params: df["atr_ratio"] <= p.atr_max_ratio,
        ))

    # #11 Volume dry-up
    if params.vol_dryup_enabled:
        stages.append((
            f"Vol Dry-Up <= {params.vol_dryup_max_ratio}",
            lambda df, p=params: df["vol_dryup"] <= p.vol_dryup_max_ratio,
        ))

    return stages


# ============================================================================
# Main scan entry point
# ============================================================================

def run_scan(
    symbols: list[str],
    params: ScanParams,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
) -> ScanResult:
    """
    Run the full scan pipeline:
      1. Compute all indicator values for every symbol
      2. Apply funnel filters stage by stage
      3. Return ScanResult with results table, funnel log, errors

    progress_cb(done, total, symbol) is called after each ticker computation.
    """
    t0 = time.time()
    result = ScanResult(params=params)

    log.info("=" * 60)
    log.info("Scan started: %d tickers, window %s to %s",
             len(symbols), params.start_date, params.end_date)
    log.info("=" * 60)

    # ── Phase A: Compute indicators for all tickers ──
    rows = []
    for i, sym in enumerate(symbols, 1):
        try:
            row = _compute_ticker(sym, params)
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
        cutoff_value = np.nanpercentile(
            computed["pct_gain"].dropna().values,  # use FULL universe distribution
            100 - params.top_pct_cutoff,
        )
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
