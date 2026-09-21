"""v7.0.0 chunks A + B: price z-score and the realized-volatility set.

The numeric tests check each estimator against an explicitly hand-written
computation rather than a frozen expected value, so a refactor that changes
the maths fails loudly instead of quietly agreeing with a stale constant.

`test_max_trailing_bars_*` guards the split-seam interaction: these lookbacks
are far longer than any classic indicator's, and an unconditional floor would
quarantine years of seams for users who never enabled a volatility filter.
"""

import datetime as dt
import os

import numpy as np
import pandas as pd
import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt6.QtWidgets import QApplication  # noqa: E402

from trade_scanner_fh import indicators as I  # noqa: E402
from trade_scanner_fh import scanner as sc  # noqa: E402
from trade_scanner_fh.gui import widgets as W  # noqa: E402


NEW_ROWS = ("hv", "yz", "atr_pct", "hv_rank", "hv_pct", "price_zscore")


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance() or QApplication([])
    yield app


@pytest.fixture(scope="module")
def bars():
    """Three years of deterministic synthetic daily bars."""
    rng = np.random.default_rng(42)
    n = 756
    idx = pd.bdate_range("2023-01-02", periods=n)
    close = 100 * np.exp(np.cumsum(rng.normal(0.0004, 0.02, n)))
    openp = np.r_[close[0], close[:-1]] * (1 + rng.normal(0, 0.004, n))
    hi = close * (1 + np.abs(rng.normal(0, 0.008, n)))
    lo = close * (1 - np.abs(rng.normal(0, 0.008, n)))
    return pd.DataFrame({
        "Open": openp,
        "High": np.maximum(hi, np.maximum(openp, close)),
        "Low": np.minimum(lo, np.minimum(openp, close)),
        "Close": close,
        "Volume": rng.integers(1e5, 1e7, n),
    }, index=idx)


# ----------------------------------------------------------------------
# A — price z-score
# ----------------------------------------------------------------------

def test_zscore_matches_an_explicit_computation(bars):
    window = bars.loc["2025-01-01":]
    end = window.index[-1]
    sl = bars.loc[end - pd.DateOffset(years=1):end]
    expected = (window["Close"].iloc[-1] - sl["Close"].mean()) / sl["Close"].std()
    got = I.price_zscore(bars, window, period_key="1y").z
    assert got == pytest.approx(float(expected), abs=1e-12)


def test_zscore_uses_sample_stdev_not_population(bars):
    """ddof=1. With ~260 bars the two differ in the 4th decimal, which is
    enough to flip a boundary comparison."""
    window = bars.loc["2025-01-01":]
    end = window.index[-1]
    sl = bars.loc[end - pd.DateOffset(years=1):end]["Close"]
    population = (window["Close"].iloc[-1] - sl.mean()) / sl.std(ddof=0)
    got = I.price_zscore(bars, window, period_key="1y").z
    assert got != pytest.approx(float(population), abs=1e-9)


def test_zscore_period_p_uses_the_scan_window_only(bars):
    window = bars.loc["2025-06-01":]
    res = I.price_zscore(bars, window, period_key="p")
    assert res.bars_used == len(window)
    assert res.truncated is False


def test_zscore_periods_measure_back_from_scan_end_not_today(bars):
    """A backdated window must not read bars after its own end date."""
    window = bars.loc["2024-01-02":"2024-06-28"]
    res = I.price_zscore(bars, window, period_key="6m")
    end = window.index[-1]
    expected = bars.loc[end - pd.DateOffset(months=6):end]
    assert res.bars_used == len(expected)


def test_zscore_flags_truncation_when_cache_is_too_shallow(bars):
    """The frame holds 3 years; a 5Y comparison cannot be satisfied."""
    window = bars.loc["2025-01-01":]
    res = I.price_zscore(bars, window, period_key="5y")
    assert res.truncated is True
    assert res.first_available > res.requested_start
    assert np.isfinite(res.z)  # still computed over what exists


def test_zscore_does_not_flag_truncation_when_history_suffices(bars):
    window = bars.loc["2025-01-01":]
    assert I.price_zscore(bars, window, period_key="6m").truncated is False
    assert I.price_zscore(bars, window, period_key="1y").truncated is False


def test_zscore_below_min_bars_is_na(bars):
    window = bars.iloc[-5:]
    res = I.price_zscore(bars, window, period_key="p")
    assert np.isnan(res.z)
    assert res.bars_used == 5


def test_zscore_flat_series_is_na():
    idx = pd.bdate_range("2025-01-01", periods=60)
    flat = pd.DataFrame({"Close": [50.0] * 60}, index=idx)
    assert np.isnan(I.price_zscore(flat, flat, period_key="p").z)


def test_zscore_empty_window_is_na(bars):
    assert np.isnan(I.price_zscore(bars, bars.iloc[0:0], period_key="1y").z)


def test_zscore_sign_follows_position_relative_to_mean():
    idx = pd.bdate_range("2025-01-01", periods=80)
    rising = pd.DataFrame({"Close": np.linspace(10, 90, 80)}, index=idx)
    falling = pd.DataFrame({"Close": np.linspace(90, 10, 80)}, index=idx)
    assert I.price_zscore(rising, rising, period_key="p").z > 0
    assert I.price_zscore(falling, falling, period_key="p").z < 0


# ----------------------------------------------------------------------
# B — realized volatility
# ----------------------------------------------------------------------

def test_historical_volatility_matches_explicit_numpy(bars):
    lr = np.diff(np.log(bars["Close"].to_numpy()[-21:]))
    expected = np.std(lr, ddof=1) * np.sqrt(252) * 100
    assert I.historical_volatility(bars, lookback=20) == pytest.approx(
        float(expected), abs=1e-12)


def test_historical_volatility_needs_lookback_plus_one_closes(bars):
    """An n-bar window yields n-1 returns; reporting a 19-return figure as a
    20-day vol would understate the sample."""
    assert np.isnan(I.historical_volatility(bars.iloc[-20:], lookback=20))
    assert np.isfinite(I.historical_volatility(bars.iloc[-21:], lookback=20))


def test_annualization_scales_by_sqrt_252(bars):
    daily = I.historical_volatility(bars, lookback=20, annualize=False)
    annual = I.historical_volatility(bars, lookback=20, annualize=True)
    assert annual == pytest.approx(daily * np.sqrt(252), rel=1e-12)


def test_yang_zhang_is_positive_and_finite(bars):
    yz = I.yang_zhang_volatility(bars, lookback=20)
    assert np.isfinite(yz) and yz > 0


def test_yang_zhang_reacts_to_overnight_gaps():
    """The reason Yang-Zhang was chosen over Parkinson / Garman-Klass: those
    two see only the intraday range and are blind to a gap."""
    n = 60
    idx = pd.bdate_range("2025-01-01", periods=n)
    base = np.full(n, 100.0)
    calm = pd.DataFrame({"Open": base, "High": base * 1.005,
                         "Low": base * 0.995, "Close": base,
                         "Volume": 1e6}, index=idx)
    # Same intraday ranges, but every session opens 4% away from the last close.
    jump = calm.copy()
    gapped = base * (1 + 0.04 * ((-1.0) ** np.arange(n)))
    jump["Open"] = gapped
    jump["High"] = np.maximum(gapped, base) * 1.005
    jump["Low"] = np.minimum(gapped, base) * 0.995
    assert (I.yang_zhang_volatility(jump, lookback=20)
            > I.yang_zhang_volatility(calm, lookback=20))


def test_atr_pct_normalises_by_price(bars):
    atr = I.atr_value(bars, period=14)
    close = bars["Close"].iloc[-1]
    assert I.atr_pct(bars, period=14) == pytest.approx(
        float(atr / close * 100), abs=1e-12)


def test_atr_pct_is_price_level_independent():
    """A $8 name and a $400 name with the same proportional range must give
    the same ATR% - which is the whole point of the column."""
    n = 60
    idx = pd.bdate_range("2025-01-01", periods=n)
    def frame(scale):
        c = np.full(n, 100.0) * scale
        return pd.DataFrame({"Open": c, "High": c * 1.03, "Low": c * 0.97,
                             "Close": c, "Volume": 1e6}, index=idx)
    assert I.atr_pct(frame(1.0)) == pytest.approx(I.atr_pct(frame(50.0)))


def test_hv_rank_and_percentile_are_bounded(bars):
    for fn in (I.hv_rank, I.hv_percentile):
        v = fn(bars, lookback=20)
        assert np.isfinite(v) and 0.0 <= v <= 100.0


def test_hv_rank_is_100_at_a_new_high_and_0_at_a_new_low():
    n = 400
    idx = pd.bdate_range("2024-01-01", periods=n)
    rng = np.random.default_rng(7)
    calm = rng.normal(0, 0.002, n)
    spiky = calm.copy()
    spiky[-25:] = rng.normal(0, 0.08, 25)      # violent recent stretch
    up = pd.DataFrame(
        {"Close": 100 * np.exp(np.cumsum(spiky))}, index=idx)
    assert I.hv_rank(up, lookback=20) == pytest.approx(100.0, abs=1e-9)

    quiet = calm.copy()
    quiet[:-25] = rng.normal(0, 0.08, n - 25)  # calm only at the very end
    down = pd.DataFrame(
        {"Close": 100 * np.exp(np.cumsum(quiet))}, index=idx)
    # Not exactly 0: the calm stretch is 25 bars and the HV window is 20, so
    # several fully-calm windows exist and the minimum need not land on the
    # final bar. "Pinned to the bottom of its own range" is the claim.
    assert I.hv_rank(down, lookback=20) < 2.0


def test_hv_rank_is_na_on_perfectly_flat_history():
    """0 and 100 are opposite readings; picking either would be arbitrary.

    A constant price is the only series whose rolling HV is EXACTLY flat. An
    exponential ramp looks like it should qualify - constant log returns - but
    floating point leaves a tiny varying residue, so its HV history has a real
    range and a real rank. That is correct behaviour, not a bug, and the
    distinction is why this test uses a constant.
    """
    idx = pd.bdate_range("2024-01-01", periods=400)
    flat = pd.DataFrame({"Close": np.full(400, 100.0)}, index=idx)
    assert np.isnan(I.hv_rank(flat, lookback=20))


def test_volatility_functions_are_na_on_short_frames(bars):
    short = bars.iloc[-5:]
    for fn in (I.historical_volatility, I.yang_zhang_volatility,
               I.hv_rank, I.hv_percentile):
        assert np.isnan(fn(short))


# ----------------------------------------------------------------------
# Scanner wiring
# ----------------------------------------------------------------------

def test_new_params_default_off_and_inert():
    p = sc.ScanParams()
    for pfx in NEW_ROWS:
        assert getattr(p, f"{pfx}_enabled") is False
        assert getattr(p, f"{pfx}_display_only") is False
    labels = [n for n, _f in sc._build_filter_stages(p)]
    assert not [s for s in labels
                if any(t in s for t in ("HV", "Yang", "ATR%", "Price Z"))]


def test_enabled_filters_append_labelled_stages():
    p = sc.ScanParams(hv_enabled=True, hv_min=30.0, hv_max=90.0,
                      price_zscore_enabled=True, price_zscore_period="5y",
                      price_zscore_min=1.5, price_zscore_max=4.0)
    labels = [n for n, _f in sc._build_filter_stages(p)]
    assert "HV 30% to 90%" in labels
    assert "Price Z [5Y] 1.5sd to 4sd" in labels


def test_display_only_appends_no_stage():
    p = sc.ScanParams(hv_enabled=True, hv_display_only=True,
                      hv_min=30.0, hv_max=90.0)
    labels = [n for n, _f in sc._build_filter_stages(p)]
    assert not [s for s in labels if s.startswith("HV ")]


def test_range_stage_fails_nan_and_missing_columns():
    p = sc.ScanParams(hv_enabled=True, hv_min=10.0, hv_max=50.0)
    stage = dict(sc._build_filter_stages(p))["HV 10% to 50%"]
    df = pd.DataFrame({"hv": [25.0, np.nan, 5.0, 80.0]})
    assert list(stage(df)) == [True, False, False, False]
    # Column absent entirely -> everything fails rather than raising.
    assert not stage(pd.DataFrame({"symbol": ["A", "B"]})).any()


def test_max_trailing_bars_is_unchanged_when_new_filters_are_off():
    """An unconditional floor would widen the split-seam quarantine for every
    existing user."""
    assert sc.ScanParams().max_trailing_bars() < 272


def test_max_trailing_bars_grows_when_a_long_lookback_goes_live():
    assert sc.ScanParams(
        price_zscore_enabled=True,
        price_zscore_period="5y").max_trailing_bars() == 1260
    assert sc.ScanParams(hv_rank_enabled=True).max_trailing_bars() == 272
    # display-only still reads the data, so it must still widen the reach.
    assert sc.ScanParams(
        hv_rank_display_only=True,
        hv_rank_enabled=False).max_trailing_bars() == 272


def test_scan_result_carries_notes():
    r = sc.ScanResult(params=sc.ScanParams())
    assert r.notes == []
    r.notes.append("x")
    assert sc.ScanResult(params=sc.ScanParams()).notes == []  # not shared


# ----------------------------------------------------------------------
# Panel wiring
# ----------------------------------------------------------------------

def test_panel_rows_exist_default_off_and_support_display_only(qapp):
    panel = W.IndicatorPanel()
    for key in NEW_ROWS:
        assert key in panel.rows, key
        assert panel.rows[key].is_enabled() is False, key
        assert panel.rows[key].display_only is not None, key


def test_panel_defaults_build_inert_params(qapp):
    panel = W.IndicatorPanel()
    p = panel.build_scan_params(dt.date(2025, 1, 1), dt.date(2025, 6, 1))
    labels = [n for n, _f in sc._build_filter_stages(p)]
    assert not [s for s in labels
                if any(t in s for t in ("HV", "Yang", "ATR%", "Price Z"))]


def test_panel_values_reach_scan_params_and_survive_a_preset(qapp):
    panel = W.IndicatorPanel()
    panel.rows["hv"].set_enabled(True)
    panel.rows["hv"].set_value("min_hv", 30.0)
    panel.rows["price_zscore"].set_enabled(True)
    panel.rows["price_zscore"].set_value("period", "5y")
    panel.rows["price_zscore"].set_value("min_z", 1.5)

    p = panel.build_scan_params(dt.date(2025, 1, 1), dt.date(2025, 6, 1))
    assert p.hv_min == 30.0 and p.price_zscore_period == "5y"
    assert p.price_zscore_min == 1.5

    other = W.IndicatorPanel()
    other.from_dict(panel.to_dict())
    p2 = other.build_scan_params(dt.date(2025, 1, 1), dt.date(2025, 6, 1))
    assert p2.hv_min == 30.0 and p2.price_zscore_period == "5y"
    assert p2.price_zscore_min == 1.5


def test_new_columns_are_registered(qapp):
    keys = {k for _h, k, _f in W.RESULT_COLUMNS}
    for k in ("hv", "yz_vol", "atr_pct", "hv_rank", "hv_pct", "price_zscore"):
        assert k in keys, k


def test_price_z_column_renders_signed(qapp):
    fmt = {k: f for _h, k, f in W.RESULT_COLUMNS}["price_zscore"]
    assert fmt(2.1) == "+2.10"
    assert fmt(-0.5) == "-0.50"
