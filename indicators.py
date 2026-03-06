"""
Indicator Library — 14 Modular Technical / Momentum Indicators
================================================================
Every indicator is a standalone function:
    func(df, **params) -> scalar value

All operate on a pandas DataFrame slice (Date-indexed, OHLCV columns)
representing the user-selected date window.

Indicators return numeric values.  The *scanner* module handles the
pass/fail threshold logic so indicators stay pure computations.
"""

import numpy as np
import pandas as pd


# ============================================================================
# Trend Filters
# ============================================================================

def price_above_sma(df: pd.DataFrame, *, period: int = 200) -> dict:
    """
    #1  Price above X-day SMA.
    Returns dict with 'sma_value' and 'close' so the scanner can compare.
    Uses the full df to compute the SMA (needs lookback), then evaluates
    at the last bar.
    """
    if len(df) < period:
        return {"close": np.nan, "sma_value": np.nan}
    sma = df["Close"].rolling(period).mean()
    return {"close": df["Close"].iloc[-1], "sma_value": sma.iloc[-1]}


def stockbee_trend_intensity(
    df: pd.DataFrame, *, short_lb: int = 7, long_lb: int = 65
) -> float:
    """
    #2  Stockbee Trend Intensity = SMA(short) / SMA(long).
    Returns the ratio (e.g. 1.05 = short-term 5% above intermediate).
    """
    if len(df) < max(short_lb, long_lb):
        return np.nan
    short_avg = df["Close"].iloc[-short_lb:].mean()
    long_avg = df["Close"].iloc[-long_lb:].mean()
    if long_avg == 0:
        return np.nan
    return short_avg / long_avg


def distance_from_period_high(df: pd.DataFrame) -> float:
    """
    #3  Distance from period high (%).
    = (period_high - current_close) / period_high * 100
    0% = at the high.  5% = 5% below the high.
    """
    if df.empty:
        return np.nan
    period_high = df["High"].max()
    close = df["Close"].iloc[-1]
    if period_high == 0:
        return np.nan
    return (period_high - close) / period_high * 100.0


# ============================================================================
# Momentum / Prior Move
# ============================================================================

def pct_gain_over_period(df: pd.DataFrame) -> float:
    """
    #4  % gain from first close to last close in the window.
    """
    if len(df) < 2:
        return np.nan
    start_close = df["Close"].iloc[0]
    end_close = df["Close"].iloc[-1]
    if start_close == 0:
        return np.nan
    return (end_close - start_close) / start_close * 100.0


# NOTE: #5 (Top X Percentile) is handled at the scanner level because it
# requires the universe-wide distribution of #4 values.  It is NOT an
# individual-ticker indicator.


def consecutive_gaps(df: pd.DataFrame) -> int:
    """
    #6  Number of consecutive gap-up days (Open > prior Close), counted
    backward from the last bar.
    """
    if len(df) < 2:
        return 0
    opens = df["Open"].values
    closes = df["Close"].values
    count = 0
    for i in range(len(df) - 1, 0, -1):
        if opens[i] > closes[i - 1]:
            count += 1
        else:
            break
    return count


def current_gap_pct(df: pd.DataFrame) -> float:
    """
    #7  Current gap % = (today's Open - yesterday's Close) / yesterday's Close * 100.
    "Today" = last bar in the window.
    """
    if len(df) < 2:
        return np.nan
    today_open = df["Open"].iloc[-1]
    yest_close = df["Close"].iloc[-2]
    if yest_close == 0:
        return np.nan
    return (today_open - yest_close) / yest_close * 100.0


# ============================================================================
# Volatility Contraction
# ============================================================================

def adr_pct(df: pd.DataFrame, *, lookback: int = 14) -> float:
    """
    #8  ADR% — Average Daily Range excluding gaps.
    = mean((High - Low) / Close) * 100 over last N bars.
    """
    tail = df.iloc[-lookback:] if len(df) >= lookback else df
    if tail.empty:
        return np.nan
    ranges = (tail["High"] - tail["Low"]) / tail["Close"] * 100.0
    return ranges.mean()


def bollinger_band_width(
    df: pd.DataFrame, *, period: int = 20, num_std: float = 2.0
) -> float:
    """
    #9  Bollinger Band Width = (Upper - Lower) / Middle.
    Middle = SMA(period), Upper/Lower = Middle +/- num_std * StdDev.
    """
    if len(df) < period:
        return np.nan
    close = df["Close"]
    middle = close.rolling(period).mean().iloc[-1]
    std = close.rolling(period).std().iloc[-1]
    if middle == 0 or np.isnan(std):
        return np.nan
    upper = middle + num_std * std
    lower = middle - num_std * std
    return (upper - lower) / middle


def atr_ratio(
    df: pd.DataFrame, *, short_period: int = 5, long_period: int = 50
) -> float:
    """
    #10  ATR Ratio = ATR(short) / ATR(long).
    ATR uses true range: max(H-L, |H-prevC|, |L-prevC|).
    """
    if len(df) < long_period + 1:
        return np.nan

    high = df["High"].values
    low = df["Low"].values
    close = df["Close"].values

    # True range series (skip index 0 — no prev close)
    tr = np.maximum(
        high[1:] - low[1:],
        np.maximum(
            np.abs(high[1:] - close[:-1]),
            np.abs(low[1:] - close[:-1]),
        ),
    )

    if len(tr) < long_period:
        return np.nan

    atr_short = tr[-short_period:].mean()
    atr_long = tr[-long_period:].mean()
    if atr_long == 0:
        return np.nan
    return atr_short / atr_long


# ============================================================================
# Volume / Liquidity
# ============================================================================

def volume_dryup_ratio(
    df: pd.DataFrame, *, recent_n: int = 10, prior_n: int = 20
) -> float:
    """
    #11  Volume Dry-Up Ratio = avg_vol(recent N) / avg_vol(prior M).
    < 1.0 means volume is declining (consolidation).
    """
    needed = recent_n + prior_n
    if len(df) < needed:
        return np.nan
    recent_vol = df["Volume"].iloc[-recent_n:].mean()
    prior_vol = df["Volume"].iloc[-(recent_n + prior_n) : -recent_n].mean()
    if prior_vol == 0:
        return np.nan
    return recent_vol / prior_vol


def min_price(df: pd.DataFrame) -> float:
    """
    #12  Current close price (last bar in window).
    """
    if df.empty:
        return np.nan
    return df["Close"].iloc[-1]


def avg_volume(df: pd.DataFrame, *, lookback: int = 20) -> float:
    """
    #13  Average daily volume over last N bars.
    """
    tail = df.iloc[-lookback:] if len(df) >= lookback else df
    if tail.empty:
        return np.nan
    return tail["Volume"].mean()


def avg_dollar_volume(df: pd.DataFrame, *, lookback: int = 20) -> float:
    """
    #14  Average daily dollar volume = mean(Close * Volume) over last N bars.
    """
    tail = df.iloc[-lookback:] if len(df) >= lookback else df
    if tail.empty:
        return np.nan
    return (tail["Close"] * tail["Volume"]).mean()
