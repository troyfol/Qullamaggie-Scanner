"""Tests for the post-split-anchored momentum columns (item 1).

The originals must keep their exact meaning — these are additive columns
answering a different question, not a correction — so the headline assertion
here is that `pct_gain` and `rs_market` are byte-identical before and after.
"""
import numpy as np
import pandas as pd
import pytest

from trade_scanner_fh import config, data_engine, indicators, scanner
from trade_scanner_fh.data_engine import (
    SplitAnchor,
    find_split_anchors_in_frame,
    load_split_anchors,
    write_split_anchors,
)
from trade_scanner_fh.indicators import (
    anchor_after_split,
    pct_gain_over_period,
    pct_gain_post_split,
    relative_strength_post_split,
)


def _px(closes, splits=None, start="2024-01-02"):
    idx = pd.bdate_range(start, periods=len(closes))
    data = {"Close": closes, "Open": closes, "High": closes, "Low": closes,
            "Volume": [1_000] * len(closes)}
    if splits is not None:
        data["Stock Splits"] = splits
    return pd.DataFrame(data, index=idx)


# ----------------------------------------------------------------------
# Anchor discovery
# ----------------------------------------------------------------------

def test_anchor_is_the_last_qualifying_event():
    df = _px([1, 2, 3, 4, 5, 6], [0, 0.05, 0, 0.1, 0, 0])
    a = find_split_anchors_in_frame("T", df)
    assert a.last_ex_date == df.index[3]
    assert a.n_events == 2
    assert a.cum_factor == pytest.approx(0.005)


def test_spinoff_never_becomes_an_anchor():
    """Anchoring on HON's Solstice separation would discard years of good
    history on a mega-cap."""
    df = _px([1, 2, 3], [0, 0.9535, 0])
    assert find_split_anchors_in_frame("HON", df) is None


def test_no_split_yields_no_anchor_row():
    df = _px([1, 2, 3], [0, 0, 0])
    assert find_split_anchors_in_frame("T", df) is None


# ----------------------------------------------------------------------
# Window anchoring
# ----------------------------------------------------------------------

def test_anchor_excludes_the_ex_date_bar():
    """The ex-date bar is NOT a safe base: GTIC's 1-for-100 leaves it on the
    old basis and only steps the session after, so anchoring on it would take
    the base price from the pre-split regime."""
    df = _px([10, 10, 99, 1, 2, 3])
    sub, anchored = anchor_after_split(df, df.index[2])
    assert anchored is True
    assert len(sub) == 3
    assert sub["Close"].iloc[0] == 1          # the 99 is skipped


def test_no_anchor_returns_frame_unchanged():
    df = _px([1, 2, 3, 4])
    sub, anchored = anchor_after_split(df, None)
    assert anchored is False
    assert sub is df


def test_anchor_before_window_is_ignored():
    df = _px([1, 2, 3, 4])
    sub, anchored = anchor_after_split(df, pd.Timestamp("2020-01-01"))
    assert anchored is False


def test_anchor_on_the_windows_first_bar_is_inside_it():
    """The split happened ON the window's first bar, so it is inside the
    window — and that bar is exactly the base the anchor must skip."""
    df = _px([99, 1, 2, 3])
    sub, anchored = anchor_after_split(df, df.index[0])
    assert anchored is True
    assert list(sub["Close"]) == [1, 2, 3]


def test_anchor_after_the_window_is_not_inside_it():
    """A backdated scan can end before the split; nothing follows it."""
    df = _px([1, 2, 3, 4])
    assert anchor_after_split(df, df.index[-1] + pd.Timedelta(days=5))[1] is False


def test_too_few_post_anchor_bars_falls_back():
    df = _px([10, 10, 10, 1, 1])
    sub, anchored = anchor_after_split(df, df.index[4])
    assert anchored is False


def test_garbage_anchor_tolerated():
    df = _px([1, 2, 3, 4])
    assert anchor_after_split(df, "not-a-date")[1] is False
    assert anchor_after_split(df, pd.NaT)[1] is False


# ----------------------------------------------------------------------
# The measurement itself
# ----------------------------------------------------------------------

def test_post_split_gain_ignores_pre_split_collapse():
    """BESS's shape: a cliff at the split, then a large run. The un-anchored
    figure reads deeply negative; the anchored one reports the run."""
    df = _px([100, 100, 100, 99, 1, 2, 4])
    plain, _ = pct_gain_over_period(df)
    anchored, start = pct_gain_post_split(df, df.index[3])
    assert plain == pytest.approx(-96.0)
    # measured from the bar AFTER the ex-date: 1 -> 4
    assert anchored == pytest.approx(300.0)
    assert start == df.index[4].date()


def test_post_split_gain_is_na_without_a_split():
    """N/A, not the un-anchored figure: a number in every row hid which
    tickers actually had a split (the user's rule, 2026-10-18)."""
    df = _px([10, 11, 12, 13])
    pct, start = pct_gain_post_split(df, None)
    assert np.isnan(pct) and start is None


def test_post_split_gain_is_na_when_the_split_predates_the_window():
    df = _px([10, 11, 12, 13])
    pct, start = pct_gain_post_split(df, pd.Timestamp("2020-01-01"))
    assert np.isnan(pct) and start is None


def test_post_split_gain_is_na_with_too_few_bars_after_the_split():
    df = _px([10, 10, 10, 99, 1, 1])
    pct, start = pct_gain_post_split(df, df.index[4])
    assert np.isnan(pct) and start is None


def test_rs_post_split_uses_date_aligned_benchmark():
    """A thin ticker can be missing bars the benchmark has; slicing the
    benchmark positionally would compare two different periods."""
    stock = _px([100, 100, 99, 1, 2, 3, 4])
    bench = _px([50, 51, 52, 53, 54, 55, 56])
    # Drop a bar INSIDE the post-anchor range, so positional and date-based
    # slicing of the benchmark genuinely disagree.
    stock = stock.drop(stock.index[4])
    anchor = stock.index[2]                     # the ex-date bar (99)

    got = relative_strength_post_split(stock, bench, anchor, lookback=5)

    # Correct: stock 1 -> 4 over d3..d6, benchmark 53 -> 56 over the SAME dates.
    assert got == pytest.approx(4.0 / (56 / 53))
    # Positional slicing would have taken the benchmark's last 3 bars (54..56)
    # and produced a different, wrong answer.
    assert got != pytest.approx(4.0 / (56 / 54))


def test_rs_post_split_is_na_without_anchor():
    stock = _px([10, 11, 12, 13])
    bench = _px([50, 51, 52, 53])
    assert np.isnan(relative_strength_post_split(stock, bench, None, lookback=4))


def test_rs_post_split_is_na_when_the_split_predates_the_lookback():
    """The bug the user caught: handed the whole history, the P/S RS anchored
    on a split 250 bars back and measured ~249 bars against a 20-bar RS S&P.
    A split outside the RS lookback is not inside the window — N/A."""
    stock = _px(list(np.linspace(10, 40, 300)))
    bench = _px([100.0] * 300)
    assert np.isnan(relative_strength_post_split(
        stock, bench, stock.index[50], lookback=20))


def test_rs_post_split_inside_the_lookback_measures_from_the_split():
    stock = _px([100.0] * 290 + [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    bench = _px([100.0] * 300)
    anchor = stock.index[289]                   # ex-date bar, 11 bars from the end
    # 1 -> 10 off the post-split base (the 100s before it are ignored)
    assert relative_strength_post_split(
        stock, bench, anchor, lookback=20) == pytest.approx(10.0)
    # ...and a ratio under the cap is exact
    stock2 = _px([100.0] * 290 + [4, 4, 4, 4, 4, 5, 5, 5, 5, 6])
    assert relative_strength_post_split(
        stock2, bench, anchor, lookback=20) == pytest.approx(6 / 4)


def test_rs_post_split_is_na_on_short_history():
    """Mirrors `relative_strength_ratio`: fewer bars than the lookback is N/A."""
    stock = _px([100, 99, 1, 2, 3])
    bench = _px([50] * 5)
    assert np.isnan(relative_strength_post_split(
        stock, bench, stock.index[1], lookback=20))


def test_rs_post_split_keeps_the_upside_cap():
    stock = _px([100, 100, 99, 1, 50, 900])
    bench = _px([50, 50, 50, 50, 50, 50])
    assert relative_strength_post_split(
        stock, bench, stock.index[2], lookback=5) == 10.0


def test_rs_post_split_nan_on_empty_benchmark():
    stock = _px([100, 100, 99, 1, 2, 4])
    assert np.isnan(relative_strength_post_split(
        stock, pd.DataFrame(), stock.index[2], lookback=4))


# ----------------------------------------------------------------------
# Sidecar round trip
# ----------------------------------------------------------------------

def test_anchor_sidecar_round_trip(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "SPLIT_ANCHORS_PARQUET",
                        tmp_path / "split_anchors.parquet")
    rows = [
        SplitAnchor("BBB", pd.Timestamp("2025-02-03"), 0.05, 1),
        SplitAnchor("AAA", pd.Timestamp("2024-10-09"), 0.002, 2),
    ]
    assert write_split_anchors(rows) == 2
    got = load_split_anchors()
    assert got == {"AAA": pd.Timestamp("2024-10-09"),
                   "BBB": pd.Timestamp("2025-02-03")}


def test_missing_sidecar_is_empty_not_an_error(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "SPLIT_ANCHORS_PARQUET", tmp_path / "nope.parquet")
    assert load_split_anchors() == {}


def test_empty_sidecar_round_trips(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "SPLIT_ANCHORS_PARQUET", tmp_path / "a.parquet")
    assert write_split_anchors([]) == 0
    assert load_split_anchors() == {}


def test_combined_rebuild_writes_both_artifacts(tmp_path, monkeypatch):
    """One sweep, two artifacts — the whole point of combining them."""
    monkeypatch.setattr(config, "PARQUET_DIR", tmp_path)
    monkeypatch.setattr(config, "SPLIT_ANCHORS_PARQUET", tmp_path / "a.parquet")
    monkeypatch.setattr(config, "SPLIT_SEAM_SKIP_FILE", tmp_path / "s.txt")

    _px([10, 10, 0.5, 0.5], [0, 0, 0.05, 0]).to_parquet(tmp_path / "BAD.parquet")
    _px([10, 10, 9.8, 10.1], [0, 0, 0.05, 0]).to_parquet(tmp_path / "OK.parquet")
    _px([10, 11, 12, 13], [0, 0, 0, 0]).to_parquet(tmp_path / "PLAIN.parquet")

    summary = data_engine.rebuild_split_artifacts(["BAD", "OK", "PLAIN"])
    assert summary["scanned"] == 3
    assert summary["anchors"] == 2          # BAD + OK have splits, PLAIN doesn't
    assert summary["seam_tickers"] == 1     # only BAD is discontinuous
    assert load_split_anchors().keys() == {"BAD", "OK"}
    assert data_engine.load_split_seam_skip() == frozenset({"BAD"})


# ----------------------------------------------------------------------
# The originals must not move
# ----------------------------------------------------------------------

def test_existing_columns_unchanged_by_the_feature(monkeypatch, tmp_path):
    """`pct_gain` and `rs_market` must be identical with and without anchors,
    or every saved preset silently changes meaning."""
    monkeypatch.setattr(config, "PARQUET_DIR", tmp_path)
    df = _px([100, 100, 100, 99, 1, 2, 3, 5], [0, 0, 0, 0.05, 0, 0, 0, 0])
    df.to_parquet(tmp_path / "SPLIT.parquet")
    data_engine.clear_ohlcv_cache()

    params = scanner.ScanParams(
        start_date=str(df.index[0].date()), end_date=str(df.index[-1].date()))
    bench = {"SPY": _px([50] * len(df))}

    plain = scanner._compute_ticker("SPLIT", params, bench, None, None, None, None)
    anchored = scanner._compute_ticker(
        "SPLIT", params, bench, None, None, None, {"SPLIT": df.index[3]})

    assert plain["pct_gain"] == anchored["pct_gain"]
    assert plain["gain_start_date"] == anchored["gain_start_date"]
    # ...while the new column carries a value exactly where a split exists
    assert anchored["pct_gain_post_split"] != anchored["pct_gain"]
    assert anchored["pct_gain_post_split"] == pytest.approx(400.0)  # 1 -> 5
    assert np.isnan(plain["pct_gain_post_split"])
    assert plain["post_split_start_date"] is None
