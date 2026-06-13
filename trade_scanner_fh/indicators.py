"""
Indicator Library — 23 Modular Technical / Momentum Indicators
================================================================
Every indicator is a standalone function:
    func(df, **params) -> scalar value

All operate on a pandas DataFrame slice (Date-indexed, OHLCV columns)
representing the user-selected date window.

Indicators return numeric values.  The *scanner* module handles the
pass/fail threshold logic so indicators stay pure computations.
"""

from datetime import date as _date

import numpy as np
import pandas as pd


def _to_date(ts):
    """Convert a pd.Timestamp / datetime / date / None to a plain date."""
    if ts is None:
        return None
    if hasattr(ts, "date"):
        try:
            return ts.date()
        except TypeError:
            return ts
    return ts


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


def relative_strength_ratio(
    stock_df: pd.DataFrame, bench_df: pd.DataFrame, *, lookback: int = 20
) -> float:
    """
    Relative strength ratio = stock return / benchmark return over N days.
    A value of 1.5 means the stock gained 50% more than the benchmark.
    Capped at 10.0 to prevent outlier distortion in sorting.
    """
    if len(stock_df) < lookback or len(bench_df) < lookback:
        return np.nan
    stock_close = stock_df["Close"].iloc[-lookback:]
    bench_close = bench_df["Close"].iloc[-lookback:]
    if stock_close.iloc[0] == 0 or bench_close.iloc[0] == 0:
        return np.nan
    stock_ret = stock_close.iloc[-1] / stock_close.iloc[0]
    bench_ret = bench_close.iloc[-1] / bench_close.iloc[0]
    if abs(bench_ret) < 1e-10:
        return np.nan
    ratio = stock_ret / bench_ret
    return min(ratio, 10.0)


# ============================================================================
# Momentum / Prior Move
# ============================================================================

def pct_gain_over_period(df: pd.DataFrame) -> tuple[float, "_date | None"]:
    """
    #4  % gain from first close to last close in the window.

    Returns (pct_gain, start_date) where start_date is the date of the
    first bar actually present in the window — useful when a ticker has
    less history than the requested scan range (e.g. recent IPO).
    """
    if len(df) < 2:
        return np.nan, None
    start_close = df["Close"].iloc[0]
    end_close = df["Close"].iloc[-1]
    if start_close == 0:
        return np.nan, None
    pct = (end_close - start_close) / start_close * 100.0
    return pct, _to_date(df.index[0])


# NOTE: #5 (Top X Percentile) is handled at the scanner level because it
# requires the universe-wide distribution of #4 values.  It is NOT an
# individual-ticker indicator.


def _trailing_true_count(mask: np.ndarray) -> int:
    """Return the length of the trailing True run in a boolean array.
    Vectorized helper shared by consecutive_gaps up/down."""
    if mask.size == 0 or not mask[-1]:
        return 0
    reversed_false = ~mask[::-1]
    if not reversed_false.any():
        return int(mask.size)
    return int(np.argmax(reversed_false))


def consecutive_gaps(df: pd.DataFrame) -> tuple[int, "_date | None"]:
    """
    #6  Number of consecutive gap-up days (Open > prior Close), counted
    backward from the last bar.

    Returns (count, streak_start_date) where streak_start_date is the date
    of the chronologically earliest gap in the trailing streak. None when
    count == 0.
    """
    if len(df) < 2:
        return 0, None
    opens = df["Open"].values
    closes = df["Close"].values
    gap_up = opens[1:] > closes[:-1]
    count = _trailing_true_count(gap_up)
    if count == 0:
        return 0, None
    # The trailing streak of length N covers bars [n-N, n-1] (the gap days).
    # Start of streak = df.index[n - count].
    return count, _to_date(df.index[len(df) - count])


def surge_trend_continuous(
    df: pd.DataFrame, *, max_drawdown_pct: float = 25.0,
) -> tuple:
    """
    #7e Trend-continuous surge — the start-to-peak rally that hasn't
    suffered an intermediate close-from-running-peak drawdown greater
    than `max_drawdown_pct` (in percent units, e.g. 25.0 = 25%).

    Designed for studying episodic pivots: catalyst-driven breakouts
    that *continue* without giving back the bulk of the move. Unlike
    the fixed-window N-day surge variants, this scans the **entire**
    `df` window and reports the longest sustained rally ending at
    the global peak.

    Algorithm — O(N) single forward pass:
      1. Find the global peak close (peak_idx) in df.
      2. Walk forward from index 0 to peak_idx, maintaining a running
         max close from the current candidate-start.
      3. When the running drawdown exceeds `max_drawdown_pct`, the
         prior trend is "broken" — reset the candidate-start to the
         current bar.
      4. At peak_idx, the surviving candidate-start is the earliest
         bar from which the rally to peak hasn't pulled back more
         than the threshold from any intermediate high.

    Returns (peak_pct, start_date, end_date), or (NaN, None, None)
    when the global peak is the first bar (no rally exists).
    """
    if len(df) < 2:
        return np.nan, None, None
    closes = df["Close"].values
    if not np.isfinite(closes).any():
        return np.nan, None, None

    peak_idx = int(np.nanargmax(closes))
    if peak_idx == 0:
        return np.nan, None, None  # peak is bar 0 — no rally to measure

    threshold = float(max_drawdown_pct) / 100.0
    last_valid_start = 0
    running_peak = closes[0]

    for j in range(1, peak_idx + 1):
        c = closes[j]
        if not np.isfinite(c):
            continue
        if c > running_peak:
            running_peak = c
        # Drawdown from current running peak — always >= 0
        if running_peak > 0:
            dd = (running_peak - c) / running_peak
        else:
            dd = 0.0
        if dd > threshold:
            # Prior trend broken — reset candidate start to this bar
            last_valid_start = j
            running_peak = c

    if last_valid_start == peak_idx:
        # Resets ran all the way to the peak — no sustained rally
        return np.nan, None, None

    start_close = closes[last_valid_start]
    if start_close <= 0:
        return np.nan, None, None
    peak_pct = (closes[peak_idx] / start_close - 1.0) * 100.0

    s_date = df.index[last_valid_start]
    e_date = df.index[peak_idx]
    s = s_date.date() if hasattr(s_date, "date") else s_date
    e = e_date.date() if hasattr(e_date, "date") else e_date
    return float(peak_pct), s, e


def surge_ignition(
    df: pd.DataFrame, *,
    max_drawdown_pct: float = 25.0,
    vol_mult: float = 2.0,
    min_pct: float = 5.0,
    vol_lookback: int = 20,
) -> tuple:
    """#7f Ignition surge — builds on top of `surge_trend_continuous`.

    First runs the trend-continuous detector to find the rally bounds
    (start_date → peak_date) using the same drawdown gating, then
    re-anchors the START to the IGNITION BAR — the first bar inside
    that rally where:

      * day-over-day close % gain  >=  `min_pct`, AND
      * day's volume               >=  `vol_mult` × median volume of
                                       the prior `vol_lookback` bars
                                       (looking back from the current
                                       bar, not the rally start)

    Use case: the user wants to see the catalyst bar (high-volume
    conviction up day) as the start, not the lowest-bar-before-the-
    move that the pure trend-continuous detector picks. The reported
    `peak_pct` is recomputed from the ignition close to the peak —
    so the % reflects the post-ignition gain, NOT the full trend-
    continuous run-up. The end_date is unchanged (still the peak).

    Fallback: if no qualifying ignition bar is found inside the
    rally, returns the trend-continuous result unchanged. This keeps
    ignition mode non-strict — the same rallies pass; only the start
    label moves when a clear catalyst exists. To be strict, the user
    can tighten `vol_mult` / `min_pct` until rallies without an
    ignition fail their other filters.

    Returns (peak_pct, start_date, end_date) just like the other
    surge detectors. NaN/None when no rally exists.
    """
    base = surge_trend_continuous(df, max_drawdown_pct=max_drawdown_pct)
    base_pct, base_start, base_end = base
    if base_start is None or base_end is None:
        return base

    # Resolve the rally bounds back to integer indices in df.
    try:
        idx_dates = [
            (d.date() if hasattr(d, "date") else d) for d in df.index
        ]
        start_idx = idx_dates.index(base_start)
        end_idx = idx_dates.index(base_end)
    except ValueError:
        return base  # date not in index — defensive, shouldn't happen

    if end_idx <= start_idx:
        return base

    closes = df["Close"].values
    volumes = df["Volume"].values if "Volume" in df.columns else None
    if volumes is None:
        return base  # no volume data — can't detect ignition, fall back

    threshold_pct = float(min_pct) / 100.0
    lookback = max(1, int(vol_lookback))
    multiplier = max(0.0, float(vol_mult))

    # Walk forward from the trend start (inclusive of start_idx + 1
    # so we have a prior close for the % gain comparison) to peak.
    # The start bar itself is excluded as a candidate because the
    # algorithm needs a prior close to compute day-over-day %.
    for j in range(start_idx + 1, end_idx + 1):
        prev_c = closes[j - 1]
        c = closes[j]
        v = volumes[j]
        if not (np.isfinite(prev_c) and np.isfinite(c) and np.isfinite(v)):
            continue
        if prev_c <= 0:
            continue
        pct_gain = (c - prev_c) / prev_c
        if pct_gain < threshold_pct:
            continue

        # Volume check — median of the prior `lookback` bars'
        # volumes (looking back from j, NOT including j itself).
        vol_window_start = max(0, j - lookback)
        prior_vols = volumes[vol_window_start:j]
        # Drop any NaNs / non-finite from the prior window.
        prior_vols = prior_vols[np.isfinite(prior_vols)]
        if prior_vols.size == 0:
            continue
        median_vol = float(np.median(prior_vols))
        if median_vol <= 0:
            continue
        if v < multiplier * median_vol:
            continue

        # All criteria met — this is the ignition bar.
        ig_close = closes[j]
        if ig_close <= 0:
            continue
        ig_peak_pct = (closes[end_idx] / ig_close - 1.0) * 100.0
        ig_date = df.index[j]
        ig_date = ig_date.date() if hasattr(ig_date, "date") else ig_date
        return float(ig_peak_pct), ig_date, base_end

    # No qualifying ignition bar inside the rally — fall back to the
    # trend-continuous result so the rally still surfaces.
    return base


def surge_detection(
    df: pd.DataFrame, *, surge_pct: float = 40.0, surge_days: int = 7,
    use_high_low: bool = False,
) -> tuple:
    """
    #7b  Surge Detection — find the peak N-day % gain within the window.

    Option A (default, use_high_low=False):
        Close-to-close:  max of  close[i] / close[i-N] - 1  across all bars.

    Option B (use_high_low=True):
        For each bar's Low, find the max High in the next N bars (forward).
        This enforces temporal order (low before high).

    Returns (peak_pct, start_date, end_date).
    peak_pct: e.g. 45.2 for a 45.2% move.  Dates are date objects or None.
    """
    if len(df) < surge_days + 1:
        return np.nan, None, None

    if use_high_low:
        # Option B: for each bar i, find max High in bars [i..i+N-1], then
        # compute gain from Low[i] to that max High (enforces low before high).
        # Phase 7 I8: vectorized via sliding-window max instead of Python loop.
        lows = df["Low"].values
        highs = df["High"].values
        n = len(df)

        # Pad highs with -inf so the forward window [i, i+surge_days] is valid
        # for every i in [0, n-1); argmax ignores -inf padding.
        pad = surge_days - 1
        padded = np.concatenate([highs, np.full(pad, -np.inf)])
        from numpy.lib.stride_tricks import sliding_window_view
        windows = sliding_window_view(padded, window_shape=surge_days)[:n - 1]
        max_highs = windows.max(axis=1)
        argmax_offsets = windows.argmax(axis=1)
        valid_lows = lows[:n - 1]

        with np.errstate(divide="ignore", invalid="ignore"):
            pcts = np.where(
                valid_lows > 0,
                (max_highs / valid_lows - 1.0) * 100.0,
                -np.inf,
            )
        best_idx = int(np.argmax(pcts))
        peak = float(pcts[best_idx])
        peak_start_pos = best_idx
        peak_end_pos = best_idx + int(argmax_offsets[best_idx])
    else:
        # Option A: close-to-close N-day return
        surge_series = df["Close"].pct_change(periods=surge_days) * 100.0
        peak = surge_series.max()
        if np.isnan(peak):
            return np.nan, None, None
        peak_idx = surge_series.idxmax()
        peak_end_pos = df.index.get_loc(peak_idx)
        peak_start_pos = peak_end_pos - surge_days

    # Finiteness guard: the high_low path emits -inf as the sentinel when every
    # window low is non-positive (best_idx then points at a bogus bar). Treat
    # any non-finite peak as "no surge" so -inf can't leak into the filter /
    # display (np.isnan alone missed -inf).
    if not np.isfinite(peak):
        return np.nan, None, None

    s_date = df.index[peak_start_pos]
    e_date = df.index[peak_end_pos]
    s = s_date.date() if hasattr(s_date, "date") else s_date
    e = e_date.date() if hasattr(e_date, "date") else e_date

    return (peak if not np.isnan(peak) else np.nan), s, e


def consecutive_gaps_down(df: pd.DataFrame) -> tuple[int, "_date | None"]:
    """
    #6b  Number of consecutive gap-down days (Open < prior Close), counted
    backward from the last bar.

    Returns (count, streak_start_date) — see consecutive_gaps for details.
    """
    if len(df) < 2:
        return 0, None
    opens = df["Open"].values
    closes = df["Close"].values
    gap_down = opens[1:] < closes[:-1]
    count = _trailing_true_count(gap_down)
    if count == 0:
        return 0, None
    return count, _to_date(df.index[len(df) - count])


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


def _gap_series(df: pd.DataFrame):
    """Return (gap_pcts, gap_dates) for every bar i in [1, n) — the gap %
    measured from prev close to today's open, paired with today's date.
    Bars where prev close is 0 yield NaN gaps."""
    opens = df["Open"].values[1:]
    prev_closes = df["Close"].values[:-1]
    dates = df.index[1:]
    with np.errstate(divide="ignore", invalid="ignore"):
        gaps = np.where(
            prev_closes != 0,
            (opens - prev_closes) / prev_closes * 100.0,
            np.nan,
        )
    return gaps, dates


def max_positive_gap(df: pd.DataFrame) -> tuple[float, "_date | None"]:
    """
    #7c  Largest positive overnight gap % in the window.
    Gap = (Open[i] - Close[i-1]) / Close[i-1] * 100.
    Returns (max_gap_pct, gap_date), or (NaN, None) if < 2 bars / no
    positive gaps exist. gap_date is the open date that opened the gap.
    """
    if len(df) < 2:
        return np.nan, None
    gaps, dates = _gap_series(df)
    pos = np.where(gaps > 0, gaps, np.nan)
    if not np.isfinite(pos).any():
        return np.nan, None
    best_idx = int(np.nanargmax(pos))
    return float(gaps[best_idx]), _to_date(dates[best_idx])


def max_negative_gap(df: pd.DataFrame) -> tuple[float, "_date | None"]:
    """
    #7d  Largest negative overnight gap % in the window.
    Gap = (Open[i] - Close[i-1]) / Close[i-1] * 100.
    Returns (min_gap_pct, gap_date), or (NaN, None) if < 2 bars / no
    negative gaps exist.
    """
    if len(df) < 2:
        return np.nan, None
    gaps, dates = _gap_series(df)
    neg = np.where(gaps < 0, gaps, np.nan)
    if not np.isfinite(neg).any():
        return np.nan, None
    worst_idx = int(np.nanargmin(neg))
    return float(gaps[worst_idx]), _to_date(dates[worst_idx])


# ============================================================================
# Volatility Contraction
# ============================================================================

def atr_value(df: pd.DataFrame, *, period: int = 14) -> float:
    """
    Average True Range (ATR) — absolute dollar value.
    ATR = SMA of True Range over *period* bars.
    True Range = max(H-L, |H-prevC|, |L-prevC|).
    """
    if len(df) < period + 1:
        return np.nan

    high = df["High"].values
    low = df["Low"].values
    close = df["Close"].values

    tr = np.maximum(
        high[1:] - low[1:],
        np.maximum(
            np.abs(high[1:] - close[:-1]),
            np.abs(low[1:] - close[:-1]),
        ),
    )
    if len(tr) < period:
        return np.nan
    return tr[-period:].mean()


def adr_pct(df: pd.DataFrame, *, lookback: int = 20) -> float:
    """
    #8  ADR% — Average Daily Range, classic ratio form (2026-06).
    = mean(100 * (High / Low - 1)) over the trailing `lookback` bars
    ending at the last bar.

    Formula change history: previously mean((High - Low) / Close) * 100
    with default lookback 14. The ratio form measures the bar's range
    against its own Low (the convention popularized for momentum
    screens) and the default lookback moved to 20.
    """
    tail = df.iloc[-lookback:] if len(df) >= lookback else df
    if tail.empty:
        return np.nan
    # Guard against a zero/negative Low (bad bar) producing inf/NaN that
    # would propagate into the filter (inf >= threshold reads as a false pass)
    # and render as "inf%". Mask those bars out before averaging, consistent
    # with the sibling indicators' zero-denominator guards.
    low = tail["Low"]
    ratios = (tail["High"] / low.where(low > 0) - 1.0) * 100.0
    return ratios.mean()


def adr_dollar(df: pd.DataFrame, *, lookback: int = 20) -> float:
    """
    #8c  $ADR — Average Daily Range expressed in dollars at the latest
    close.
    = (adr_pct / 100) * last Close, where adr_pct is the masked ratio
    mean above. Deriving from the SAME adr_pct computation (same
    lookback, same zero/negative-Low masking) guarantees ADR% and $ADR
    always agree.

    NaN when: df is empty / missing High, Low or Close columns, ADR%
    itself is NaN (no usable bars), or the last Close is NaN /
    non-positive (bad bar).
    """
    if df is None or len(df) == 0:
        return np.nan
    if not {"High", "Low", "Close"}.issubset(df.columns):
        return np.nan
    pct = adr_pct(df, lookback=lookback)
    if pct is None or not np.isfinite(pct):
        return np.nan
    last_close = df["Close"].iloc[-1]
    if last_close is None or not np.isfinite(last_close) or last_close <= 0:
        return np.nan
    return (float(pct) / 100.0) * float(last_close)


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


def relative_volume(df: pd.DataFrame, *, lookback: int = 20) -> float:
    """
    #14b  RVOL — Relative Volume of the LAST bar.
    = Volume[last] / mean(Volume of the prior `lookback` bars,
      EXCLUDING the last bar).

    A value of 1.0 means the last bar traded exactly its trailing
    average volume; 2.0 means double, 0.5 means half. NaN when:
      * fewer than `lookback` + 1 bars (need a full prior window plus
        the bar being measured), or
      * the prior-window mean is zero / non-finite (e.g. a halted name
        with all-zero volume — a ratio against 0 is meaningless), or
      * the last bar's volume is itself NaN.

    NOTE: deliberately separate from `surge_ignition`'s internal
    volume gate — that one is a *median*-based multiple evaluated at
    each candidate bar inside a rally. RVOL is a simple *mean*-based
    snapshot of the latest bar only; keep the two independent.
    """
    if "Volume" not in df.columns or len(df) < lookback + 1:
        return np.nan
    last_vol = df["Volume"].iloc[-1]
    base = df["Volume"].iloc[-(lookback + 1):-1].mean()
    if not np.isfinite(base) or base <= 0:
        return np.nan
    if last_vol is None or not np.isfinite(last_vol):
        return np.nan
    return float(last_vol) / float(base)
