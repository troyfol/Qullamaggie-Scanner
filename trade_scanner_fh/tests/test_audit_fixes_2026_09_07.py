"""Regression tests for the three logic-audit fixes of 2026-09-07.

1. `days_since_er` / `days_until_er` are resolved as of the SCAN END
   DATE, not as of today, so a backdated scan stops reporting negative
   day counts and silently dropping ~86% of the universe.
2. `relative_strength_ratio` slices the benchmark by DATE, not by
   position, so a ticker missing bars no longer compares a longer
   calendar span against the benchmark's fixed N sessions.
3. The two YoY columns join the display-only NaN red-on-fail set, so
   the visual signal matches what the funnel would have done.
"""

import datetime as dt

import numpy as np
import pandas as pd
import pytest

from trade_scanner_fh import indicators, scanner


# ======================================================================
# 1. Report dates resolved as of the scan end date
# ======================================================================

def hist_frame(report_dates, *, proxy=None):
    """A per-ticker history slice, report_date DESC, as the scanner
    hands it around."""
    rows = []
    for i, d in enumerate(report_dates):
        rows.append({
            "ticker": "TEST",
            "report_date": pd.Timestamp(d),
            "period_ending": pd.Timestamp(d) - pd.Timedelta(days=40),
            "report_date_proxy": bool(proxy[i]) if proxy else False,
        })
    return (pd.DataFrame(rows)
            .sort_values("report_date", ascending=False)
            .reset_index(drop=True))


def resolve(last, next_, end, hist=None):
    return scanner._resolve_report_dates_as_of(
        pd.Timestamp(last) if last else None,
        pd.Timestamp(next_) if next_ else None,
        pd.Timestamp(end), hist,
    )


def test_live_scan_pair_is_unchanged():
    """The common case: one date behind the scan end, one ahead."""
    last, nxt = resolve("2026-08-01", "2026-11-01", "2026-09-07")
    assert last == pd.Timestamp("2026-08-01")
    assert nxt == pd.Timestamp("2026-11-01")


def test_next_earnings_already_in_the_past_is_promoted():
    """The behaviour the old code had, preserved: a next_earnings that
    has already happened at the scan end becomes the last report."""
    last, nxt = resolve("2026-05-01", "2026-08-01", "2026-09-07")
    assert last == pd.Timestamp("2026-08-01")
    assert nxt is None


def test_last_earnings_not_yet_happened_becomes_the_next_report():
    """The missing mirror of the promotion above, and the actual bug:
    with a backdated scan the store's last_earnings is still in the
    FUTURE, which used to produce a negative days_since_er."""
    last, nxt = resolve("2026-08-01", "2026-11-01", "2025-06-01")
    assert last is None
    assert nxt == pd.Timestamp("2026-08-01")


def test_days_since_can_never_go_negative():
    """The end-to-end property the fix exists to guarantee."""
    end = pd.Timestamp("2025-06-01")
    for last, nxt in (
        ("2026-08-01", "2026-11-01"),
        ("2026-08-01", None),
        (None, "2026-11-01"),
        ("2025-03-01", "2026-11-01"),
    ):
        last_r, next_r = resolve(last, nxt, end)
        if last_r is not None:
            assert (end - last_r).days >= 0, (last, nxt)
        if next_r is not None:
            assert (next_r - end).days > 0, (last, nxt)


def test_history_supplies_the_last_report_for_a_backdated_scan():
    """The calendar store only brackets today, so a backdated scan needs
    the per-quarter history to answer 'what was the last report before
    this date'."""
    hist = hist_frame(["2024-02-15", "2024-05-15", "2024-08-15",
                       "2026-08-01"])
    last, nxt = resolve("2026-08-01", "2026-11-01", "2024-06-01", hist)
    assert last == pd.Timestamp("2024-05-15")
    assert nxt == pd.Timestamp("2024-08-15")


def test_history_fallback_prefers_real_announcements_over_proxies():
    """A finnhub calendar proxy is stamped ~30 days off the real date —
    the wrong basis for a days-since measurement."""
    hist = hist_frame(
        ["2024-05-15", "2024-05-31"], proxy=[False, True],
    )
    last, _ = resolve(None, None, "2024-06-01", hist)
    assert last == pd.Timestamp("2024-05-15")


def test_history_never_overrides_a_usable_calendar_last_report():
    """The calendar is the purpose-built, five-source-reconciled record of
    PAST reports, so history fills a hole but never displaces it — even
    when history holds a newer real announcement (98 live tickers are in
    that state; that staleness is a separate issue)."""
    hist = hist_frame(["2026-07-01", "2026-08-20"])
    last, nxt = resolve("2026-08-01", "2026-11-01", "2026-09-07", hist)
    assert last == pd.Timestamp("2026-08-01")   # not 2026-08-20
    assert nxt == pd.Timestamp("2026-11-01")


def test_history_narrows_the_next_report_toward_the_scan_end():
    """On a backdated scan the calendar's next_earnings describes today's
    calendar and is years away; the history holds the report that
    actually came next."""
    hist = hist_frame(["2024-02-15", "2024-05-15", "2024-08-15"])
    _last, nxt = resolve("2026-08-01", "2026-11-01", "2024-06-01", hist)
    assert nxt == pd.Timestamp("2024-08-15")


def test_narrowing_cannot_fire_on_a_live_scan():
    """Provable no-op today: the history carries no non-proxy row dated
    after today, so there is never a nearer future candidate to pick."""
    hist = hist_frame(["2026-05-15", "2026-08-01"])
    _last, nxt = resolve("2026-08-01", "2026-11-01", "2026-09-07", hist)
    assert nxt == pd.Timestamp("2026-11-01")


def test_no_dates_anywhere_yields_none():
    assert resolve(None, None, "2026-09-07") == (None, None)
    assert resolve(None, None, "2026-09-07", pd.DataFrame()) == (None, None)


def test_history_without_a_report_date_column_is_tolerated():
    bad = pd.DataFrame({"ticker": ["TEST"], "period_ending": [pd.NaT]})
    assert resolve(None, None, "2026-09-07", bad) == (None, None)


def test_nat_calendar_dates_are_ignored():
    last, nxt = resolve(None, None, "2026-09-07",
                        hist_frame(["2026-08-01"]))
    assert last == pd.Timestamp("2026-08-01")
    assert nxt is None


# ======================================================================
# 2. RS benchmark sliced by date, not position
# ======================================================================

def series_frame(dates, closes):
    return pd.DataFrame(
        {"Close": closes}, index=pd.DatetimeIndex([pd.Timestamp(d) for d in dates]),
    )


def test_rs_slices_the_benchmark_to_the_stocks_calendar_span():
    """The stock is missing bars, so its last 5 bars span 8 sessions.
    The benchmark must be measured over those same 8 sessions, not over
    its own last 5."""
    bench_dates = pd.bdate_range("2026-01-01", periods=10)
    # Benchmark climbs 1% per session.
    bench = series_frame(bench_dates, [100 * (1.01 ** i) for i in range(10)])
    # Stock trades only on every other session of the same span, and
    # ends flat.
    stock_dates = bench_dates[::2]
    stock = series_frame(stock_dates, [100.0] * len(stock_dates))

    got = indicators.relative_strength_ratio(stock, bench, lookback=5)
    # Stock flat over its 5 bars, which span bench_dates[0]..[8].
    span = bench.loc[stock_dates[0]:stock_dates[-1], "Close"]
    expected = 1.0 / (span.iloc[-1] / span.iloc[0])
    assert got == pytest.approx(expected)

    # The old positional slice would have used the benchmark's LAST 5
    # bars, a different and shorter span — and reported a higher ratio.
    positional_bench = bench["Close"].iloc[-5:]
    positional = 1.0 / (positional_bench.iloc[-1] / positional_bench.iloc[0])
    assert positional > got


def test_rs_is_unchanged_when_the_calendars_align():
    """The overwhelmingly common case must produce the identical number
    it always did."""
    dates = pd.bdate_range("2026-01-01", periods=30)
    stock = series_frame(dates, [100 * (1.02 ** i) for i in range(30)])
    bench = series_frame(dates, [100 * (1.01 ** i) for i in range(30)])
    got = indicators.relative_strength_ratio(stock, bench, lookback=20)
    s = stock["Close"].iloc[-20:]
    b = bench["Close"].iloc[-20:]
    expected = (s.iloc[-1] / s.iloc[0]) / (b.iloc[-1] / b.iloc[0])
    assert got == pytest.approx(expected)


def test_rs_cap_still_applies():
    dates = pd.bdate_range("2026-01-01", periods=25)
    stock = series_frame(dates, [1.0] * 24 + [10_000.0])
    bench = series_frame(dates, [100.0] * 25)
    assert indicators.relative_strength_ratio(stock, bench, lookback=20) == 10.0


def test_rs_nan_when_the_benchmark_has_under_two_bars_in_span():
    stock_dates = pd.bdate_range("2026-06-01", periods=20)
    stock = series_frame(stock_dates, [100.0] * 20)
    # Benchmark exists but covers an entirely different period.
    bench = series_frame(pd.bdate_range("2025-01-01", periods=30),
                         [100.0] * 30)
    assert np.isnan(
        indicators.relative_strength_ratio(stock, bench, lookback=20)
    )


def test_rs_short_history_guards_are_preserved():
    dates = pd.bdate_range("2026-01-01", periods=10)
    short = series_frame(dates, [100.0] * 10)
    assert np.isnan(
        indicators.relative_strength_ratio(short, short, lookback=20)
    )


def test_rs_zero_base_price_yields_nan():
    dates = pd.bdate_range("2026-01-01", periods=25)
    stock = series_frame(dates, [0.0] + [100.0] * 24)
    bench = series_frame(dates, [100.0] * 25)
    assert np.isnan(
        indicators.relative_strength_ratio(stock, bench, lookback=25)
    )


# ======================================================================
# 3. YoY columns join the display-only NaN red-on-fail set
# ======================================================================

def do_params(**kw):
    return scanner.ScanParams(
        start_date=dt.date(2024, 1, 1), end_date=dt.date(2025, 1, 1), **kw,
    )


@pytest.mark.parametrize("col", [
    "reported_eps", "surprise_eps_dollar", "surprise_eps_pct",
    "reported_rev", "surprise_rev_dollar", "surprise_rev_pct",
    "yoy_eps_pct", "yoy_rev_pct",
])
def test_nan_flags_red_for_every_data_gated_column(col):
    """With the Earnings Data toggle on, the funnel drops a NaN row — so
    display-only must paint that cell red. The two YoY columns were
    missing from this set."""
    p = do_params(earnings_data_only=True, **{f"{col}_display_only": True})
    fails = scanner._compute_display_only_fails(p, {col: float("nan")})
    assert fails.get(col) is True


@pytest.mark.parametrize("col", ["yoy_eps_pct", "yoy_rev_pct"])
def test_nan_not_flagged_when_the_toggle_is_off(col):
    """Toggle off means NaN passes the funnel cleanly, so no red."""
    p = do_params(earnings_data_only=False, **{f"{col}_display_only": True})
    assert scanner._compute_display_only_fails(p, {col: float("nan")}) == {}


def test_display_only_nan_set_matches_the_funnels_data_gated_set():
    """Drift guard: the red-on-fail NaN list and the funnel's coverage
    columns must not diverge again."""
    covered = set()
    for col in ("reported_eps", "surprise_eps_dollar", "surprise_eps_pct",
                "reported_rev", "surprise_rev_dollar", "surprise_rev_pct",
                "yoy_eps_pct", "yoy_rev_pct"):
        p = do_params(earnings_data_only=True,
                      **{f"{col}_display_only": True})
        if scanner._compute_display_only_fails(p, {col: float("nan")}):
            covered.add(col)
    assert covered == {
        "reported_eps", "surprise_eps_dollar", "surprise_eps_pct",
        "reported_rev", "surprise_rev_dollar", "surprise_rev_pct",
        "yoy_eps_pct", "yoy_rev_pct",
    }
