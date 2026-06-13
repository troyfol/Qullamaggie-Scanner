"""Tests for indicators.py — Phase 5 O2 coverage for every pure indicator."""
import numpy as np
import pandas as pd
import pytest

from trade_scanner_fh import indicators


# ======================================================================
# Helpers — synthetic OHLCV builders
# ======================================================================

def _ohlcv(closes, opens=None, highs=None, lows=None, volumes=None):
    """Build a business-day-indexed OHLCV frame from a Close series and
    optional override columns. Defaults synthesize Open=Close, High=Close+1,
    Low=Close-1, Volume=1_000_000."""
    n = len(closes)
    idx = pd.date_range("2026-01-01", periods=n, freq="B")
    return pd.DataFrame({
        "Open":   opens   if opens   is not None else list(closes),
        "High":   highs   if highs   is not None else [c + 1.0 for c in closes],
        "Low":    lows    if lows    is not None else [c - 1.0 for c in closes],
        "Close":  list(closes),
        "Volume": volumes if volumes is not None else [1_000_000] * n,
    }, index=idx)


# ======================================================================
# #1a/1b — price_above_sma
# ======================================================================

def test_price_above_sma_basic():
    df = _ohlcv([100.0 + i for i in range(10)])
    result = indicators.price_above_sma(df, period=5)
    # SMA of last 5 closes [105..109] = 107.0
    assert result["close"] == 109.0
    assert result["sma_value"] == pytest.approx(107.0)


def test_price_above_sma_insufficient_data_returns_nan():
    df = _ohlcv([100.0, 101.0, 102.0])
    result = indicators.price_above_sma(df, period=10)
    assert np.isnan(result["close"])
    assert np.isnan(result["sma_value"])


# ======================================================================
# #2 — stockbee_trend_intensity
# ======================================================================

def test_stockbee_trend_intensity_basic():
    df = _ohlcv([100.0] * 60 + [110.0] * 10)  # flat then rally
    result = indicators.stockbee_trend_intensity(df, short_lb=5, long_lb=60)
    # short avg ~110, long avg = mix of 100/110
    assert result > 1.0


def test_stockbee_trend_intensity_insufficient_data():
    df = _ohlcv([100.0] * 5)
    assert np.isnan(
        indicators.stockbee_trend_intensity(df, short_lb=7, long_lb=65)
    )


def test_stockbee_trend_intensity_zero_long_avg_returns_nan():
    df = _ohlcv([0.0] * 70)
    assert np.isnan(
        indicators.stockbee_trend_intensity(df, short_lb=5, long_lb=60)
    )


# ======================================================================
# #3 — distance_from_period_high
# ======================================================================

def test_distance_from_period_high_basic():
    # High series max = 110; Close[-1] = 104.5 (104+0.5 wrapped); 5 away
    closes = list(range(100, 110))  # 100..109
    highs = [c + 1.0 for c in closes]  # 101..110 → max 110
    df = _ohlcv(closes, highs=highs)
    # Close[-1]=109, period_high=110 → (110-109)/110*100 ≈ 0.909
    assert indicators.distance_from_period_high(df) == pytest.approx(
        (110 - 109) / 110 * 100
    )


def test_distance_from_period_high_empty_returns_nan():
    df = _ohlcv([])
    assert np.isnan(indicators.distance_from_period_high(df))


def test_distance_from_period_high_zero_high_returns_nan():
    df = _ohlcv([0.0, 0.0, 0.0], highs=[0.0, 0.0, 0.0])
    assert np.isnan(indicators.distance_from_period_high(df))


# ======================================================================
# RS — relative_strength_ratio
# ======================================================================

def test_relative_strength_ratio_stock_outperforms():
    stock = _ohlcv([100.0, 105, 110, 115, 120])     # +20%
    bench = _ohlcv([100.0, 102, 104, 106, 108])     # +8%
    rs = indicators.relative_strength_ratio(stock, bench, lookback=5)
    # 1.20 / 1.08 ≈ 1.111
    assert rs == pytest.approx(1.20 / 1.08, rel=1e-3)


def test_relative_strength_ratio_capped_at_10():
    # stock ret = 20x, bench ret = 1.01x → ratio ≈ 19.8 → clamped to 10.0
    stock = _ohlcv([100.0, 2000.0])
    bench = _ohlcv([100.0, 101.0])
    rs = indicators.relative_strength_ratio(stock, bench, lookback=2)
    assert rs == 10.0


def test_relative_strength_ratio_insufficient_data():
    stock = _ohlcv([100.0, 110])
    bench = _ohlcv([100.0, 110])
    assert np.isnan(indicators.relative_strength_ratio(
        stock, bench, lookback=20))


# ======================================================================
# #4 — pct_gain_over_period
# ======================================================================

def test_pct_gain_over_period_basic():
    df = _ohlcv([100.0, 110, 120, 150])
    pct, start = indicators.pct_gain_over_period(df)
    # (150-100)/100 * 100 = 50.0
    assert pct == pytest.approx(50.0)
    assert start == df.index[0].date()


def test_pct_gain_over_period_negative():
    df = _ohlcv([100.0, 90, 80])
    pct, start = indicators.pct_gain_over_period(df)
    assert pct == pytest.approx(-20.0)
    assert start == df.index[0].date()


def test_pct_gain_over_period_insufficient_data():
    df = _ohlcv([100.0])
    pct, start = indicators.pct_gain_over_period(df)
    assert np.isnan(pct)
    assert start is None


def test_pct_gain_over_period_zero_start_returns_nan():
    df = _ohlcv([0.0, 50.0])
    pct, start = indicators.pct_gain_over_period(df)
    assert np.isnan(pct)
    assert start is None


# ======================================================================
# #6 / #6b — consecutive_gaps up/down
# ======================================================================

def test_consecutive_gaps_counts_backward_from_last():
    # Last 3 days are gap-ups; one before is a gap-down
    closes = [100.0, 101, 102, 103, 104]
    opens =  [100.0, 100, 105, 106, 107]  # Open[2]=105 > Close[1]=101 gap-up, etc.
    df = _ohlcv(closes, opens=opens)
    # Walking backward: Open[4]=107 > Close[3]=103 ✓ count=1
    #                   Open[3]=106 > Close[2]=102 ✓ count=2
    #                   Open[2]=105 > Close[1]=101 ✓ count=3
    #                   Open[1]=100 > Close[0]=100? 100>100 is False → break
    cnt, start = indicators.consecutive_gaps(df)
    assert cnt == 3
    # Streak start = bar n - count = bar 5 - 3 = bar 2 (Open[2]=105 gap)
    assert start == df.index[2].date()


def test_consecutive_gaps_no_gaps_is_zero():
    # Open[i] == prev Close[i-1] → not strictly greater → no gap-up counted
    closes = [100.0, 101, 102, 103]
    opens =  [100.0, 100, 101, 102]  # each Open[i] equals Close[i-1]
    df = _ohlcv(closes, opens=opens)
    assert indicators.consecutive_gaps(df) == (0, None)


def test_consecutive_gaps_single_bar_is_zero():
    df = _ohlcv([100.0])
    assert indicators.consecutive_gaps(df) == (0, None)


def test_consecutive_gaps_full_run_uninterrupted():
    """Every bar is a gap-up → count equals len-1 (all possible gaps),
    and streak start is bar 1 (the earliest bar where a gap can be measured)."""
    closes = [100.0, 101, 102, 103, 104, 105]
    opens =  [100.0, 105, 106, 107, 108, 109]  # every Open > prev Close
    df = _ohlcv(closes, opens=opens)
    cnt, start = indicators.consecutive_gaps(df)
    assert cnt == 5
    # Streak covers bars [1..5]; start = df.index[6 - 5] = df.index[1]
    assert start == df.index[1].date()


def test_consecutive_gaps_down_counts_backward():
    closes = [100.0, 101, 102, 103, 104]
    opens =  [100.0, 100, 100, 101, 102]  # Open[4]=102 < Close[3]=103 gap-down
    df = _ohlcv(closes, opens=opens)
    # Walking backward: Open[4]=102 < Close[3]=103 ✓ count=1
    #                   Open[3]=101 < Close[2]=102 ✓ count=2
    #                   Open[2]=100 < Close[1]=101 ✓ count=3
    #                   Open[1]=100 < Close[0]=100? no (equal) → break
    cnt, start = indicators.consecutive_gaps_down(df)
    assert cnt == 3
    assert start == df.index[2].date()


# ======================================================================
# #7 — current_gap_pct
# ======================================================================

def test_current_gap_pct_basic():
    closes = [100.0, 101]
    opens  = [100.0, 105]  # today open = 105, yesterday close = 100
    df = _ohlcv(closes, opens=opens)
    # (105-100)/100 * 100 = 5.0
    assert indicators.current_gap_pct(df) == pytest.approx(5.0)


def test_current_gap_pct_insufficient_data():
    df = _ohlcv([100.0])
    assert np.isnan(indicators.current_gap_pct(df))


def test_current_gap_pct_zero_prev_close_returns_nan():
    closes = [0.0, 10]
    opens  = [0.0, 5]
    df = _ohlcv(closes, opens=opens)
    assert np.isnan(indicators.current_gap_pct(df))


# ======================================================================
# #7c / #7d — max_positive_gap / max_negative_gap
# ======================================================================

def test_max_positive_gap_picks_largest_overnight_up():
    closes = [100.0, 101, 102, 103]
    opens  = [100.0, 102, 108, 99]  # gaps: +1, +6, -3
    df = _ohlcv(closes, opens=opens)
    # Largest positive = (108-101)/101*100 ≈ 6.93 on bar 2
    pct, dt = indicators.max_positive_gap(df)
    assert pct == pytest.approx((108 - 101) / 101 * 100)
    assert dt == df.index[2].date()


def test_max_positive_gap_no_ups_returns_nan():
    closes = [100.0, 101, 102]
    opens  = [100.0, 90, 95]  # all gap-downs
    df = _ohlcv(closes, opens=opens)
    pct, dt = indicators.max_positive_gap(df)
    assert np.isnan(pct)
    assert dt is None


def test_max_negative_gap_picks_largest_overnight_down():
    closes = [100.0, 101, 102, 103]
    opens  = [100.0, 102, 95, 105]  # gaps: +1, -5.9%, +3%
    df = _ohlcv(closes, opens=opens)
    # Largest negative = (95-101)/101*100 ≈ -5.94 on bar 2
    pct, dt = indicators.max_negative_gap(df)
    assert pct == pytest.approx((95 - 101) / 101 * 100)
    assert dt == df.index[2].date()


def test_max_negative_gap_no_downs_returns_nan():
    closes = [100.0, 101, 102]
    opens  = [100.0, 103, 105]
    df = _ohlcv(closes, opens=opens)
    pct, dt = indicators.max_negative_gap(df)
    assert np.isnan(pct)
    assert dt is None


# ======================================================================
# #7b — surge_detection (close-to-close and high-to-low modes)
# ======================================================================

def test_surge_detection_close_to_close_finds_peak_window():
    # A surge of +40% over days 4→9 (5-day window)
    closes = [100.0, 100, 100, 100, 140, 140, 140, 140, 140, 140]
    df = _ohlcv(closes)
    pct, s_date, e_date = indicators.surge_detection(
        df, surge_days=4, use_high_low=False
    )
    # peak close-to-close 4-day return: day4/day0=140/100=1.4 → 40%
    assert pct == pytest.approx(40.0, abs=0.01)
    assert s_date is not None and e_date is not None


def test_surge_detection_insufficient_data():
    df = _ohlcv([100.0, 101, 102])  # 3 bars, surge_days=7 needs 8
    pct, s, e = indicators.surge_detection(df, surge_days=7)
    assert np.isnan(pct)
    assert s is None and e is None


def test_surge_detection_high_low_vectorized_matches_reference():
    """On a larger fixture, verify the vectorized Option B output matches a
    hand-computed reference. Lows are kept well above any reachable high
    except at bar 5, so bar 5 is unambiguously the winning start — pins the
    selection logic, not just the pct math."""
    n = 20
    closes = [150.0] * n
    opens  = [150.0] * n
    highs  = [151.0] * n
    # Lows are high everywhere except bar 5 (the intended winning start)
    lows   = [149.0] * n
    lows[5]  = 100.0
    highs[7] = 200.0
    df = _ohlcv(closes, opens=opens, highs=highs, lows=lows)

    pct, s, e = indicators.surge_detection(df, surge_days=5, use_high_low=True)
    # Bar 5 Low=100, window is bars [5..9]; max High = highs[7] = 200 → 100%
    assert pct == pytest.approx(100.0, abs=0.01)
    assert s == df.index[5].date()
    assert e == df.index[7].date()


def test_surge_detection_high_low_mode():
    # Low=100 bar 0; High=150 bar 3 → 50% surge within a 4-bar window
    closes = [100.0, 100, 100, 140, 100, 100, 100, 100]
    highs  = [101.0, 102, 110, 150, 100, 100, 100, 100]
    lows   = [100.0, 100, 100, 120, 100, 100, 100, 100]
    df = _ohlcv(closes, highs=highs, lows=lows)
    pct, s, e = indicators.surge_detection(
        df, surge_days=4, use_high_low=True
    )
    # Bar 0 Low=100 → max High in bars 0..3 = 150 → (150/100-1)*100 = 50
    assert pct == pytest.approx(50.0, abs=0.01)


# ======================================================================
# ATR family
# ======================================================================

def test_atr_value_basic():
    # Hand-constructed: H-L=2 for all bars, prev-close moves small
    closes = [100.0] * 20
    highs  = [101.0] * 20
    lows   = [99.0]  * 20
    df = _ohlcv(closes, highs=highs, lows=lows)
    # TR = max(H-L, |H-prevC|, |L-prevC|) = max(2, 1, 1) = 2 for each
    # ATR over last 14 = 2.0
    assert indicators.atr_value(df, period=14) == pytest.approx(2.0)


def test_atr_value_insufficient_data():
    df = _ohlcv([100.0] * 5)
    assert np.isnan(indicators.atr_value(df, period=14))


def test_atr_ratio_basic():
    # Same constant-TR data → ratio = 1.0
    closes = [100.0] * 60
    highs  = [101.0] * 60
    lows   = [99.0]  * 60
    df = _ohlcv(closes, highs=highs, lows=lows)
    assert indicators.atr_ratio(df, short_period=5, long_period=50) == pytest.approx(1.0)


def test_atr_ratio_insufficient_data():
    df = _ohlcv([100.0] * 10)
    assert np.isnan(indicators.atr_ratio(df, short_period=5, long_period=50))


# ======================================================================
# ADR% / BBW
# ======================================================================

def test_adr_pct_basic():
    # Classic ratio form (2026-06): 100*(H/L - 1) = 100*(101/99 - 1)
    # = 200/99 ≈ 2.020202% for every bar → ADR% = 200/99.
    # (The retired mean((H-L)/C) form gave exactly 2.0 here — the tight
    # tolerance pins the new formula and fails on a regression.)
    closes = [100.0] * 20
    highs  = [101.0] * 20
    lows   = [99.0]  * 20
    df = _ohlcv(closes, highs=highs, lows=lows)
    assert indicators.adr_pct(df, lookback=14) == pytest.approx(200.0 / 99.0)


def test_adr_pct_empty_tail_returns_nan():
    df = _ohlcv([])
    assert np.isnan(indicators.adr_pct(df, lookback=14))


def test_adr_pct_default_lookback_is_20():
    # 25 bars: the oldest 5 have a 100% H/L range; the trailing 20 have
    # exactly 3% (H=103, L=100). The default lookback must cover ONLY
    # the trailing 20 bars → exactly 3.0. A 14-bar default would also
    # give 3.0, but a 21+-bar window would pull in a 100% bar
    # ((20*3 + 100)/21 ≈ 7.6) — combined with the explicit-lookback
    # window test in test_adr_dollar_stops.py this pins default=20.
    highs = [200.0] * 5 + [103.0] * 20
    lows  = [100.0] * 25
    df = _ohlcv([101.0] * 25, highs=highs, lows=lows)
    assert indicators.adr_pct(df) == pytest.approx(3.0)
    assert indicators.adr_pct(df, lookback=21) != pytest.approx(3.0)


def test_bollinger_band_width_nonzero_for_volatile():
    # Mix of 100s and 105s → nonzero std
    closes = [100.0 if i % 2 == 0 else 105.0 for i in range(30)]
    df = _ohlcv(closes)
    w = indicators.bollinger_band_width(df, period=20, num_std=2.0)
    assert w > 0.0


def test_bollinger_band_width_insufficient_data():
    df = _ohlcv([100.0, 101])
    assert np.isnan(indicators.bollinger_band_width(df, period=20))


def test_bollinger_band_width_zero_middle_returns_nan():
    df = _ohlcv([0.0] * 25)
    assert np.isnan(indicators.bollinger_band_width(df, period=20))


# ======================================================================
# Volume / Liquidity
# ======================================================================

def test_volume_dryup_ratio_basic():
    # recent 10 volume avg = 500k, prior 20 avg = 1M → ratio 0.5
    vols = [1_000_000] * 20 + [500_000] * 10
    df = _ohlcv([100.0] * 30, volumes=vols)
    assert indicators.volume_dryup_ratio(df, recent_n=10, prior_n=20) == pytest.approx(0.5)


def test_volume_dryup_ratio_insufficient_data():
    df = _ohlcv([100.0] * 5, volumes=[1_000_000] * 5)
    assert np.isnan(indicators.volume_dryup_ratio(df, recent_n=10, prior_n=20))


def test_volume_dryup_ratio_zero_prior_returns_nan():
    vols = [0] * 20 + [500_000] * 10
    df = _ohlcv([100.0] * 30, volumes=vols)
    assert np.isnan(indicators.volume_dryup_ratio(df, recent_n=10, prior_n=20))


def test_min_price_returns_last_close():
    df = _ohlcv([100.0, 110, 120, 99.5])
    assert indicators.min_price(df) == 99.5


def test_min_price_empty_returns_nan():
    df = _ohlcv([])
    assert np.isnan(indicators.min_price(df))


def test_avg_volume_basic():
    vols = [100_000] * 20
    df = _ohlcv([100.0] * 20, volumes=vols)
    assert indicators.avg_volume(df, lookback=20) == 100_000


def test_avg_volume_partial_lookback_falls_back_to_available():
    # Only 5 bars; lookback=20 → returns mean of all 5
    vols = [200_000] * 5
    df = _ohlcv([100.0] * 5, volumes=vols)
    assert indicators.avg_volume(df, lookback=20) == 200_000


def test_avg_dollar_volume_basic():
    # All closes=100, volumes=1M → dollar vol = 100M
    vols = [1_000_000] * 20
    df = _ohlcv([100.0] * 20, volumes=vols)
    assert indicators.avg_dollar_volume(df, lookback=20) == pytest.approx(100_000_000.0)


def test_avg_dollar_volume_empty_returns_nan():
    df = _ohlcv([])
    assert np.isnan(indicators.avg_dollar_volume(df, lookback=20))
