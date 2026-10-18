"""RS Scan Window + the RS S&P P/S window rule (2026-10-18).

Each RS-vs row gains a `Scan Window` box: ticked, the row measures over the
scan's own Start → End range and its Days lookback is greyed out and unused.
RS S&P P/S follows its row's window — the lookback, or the scan window when
the box is ticked — and is N/A unless a split falls inside that window.
"""
import datetime as _dt
import os

import numpy as np
import pandas as pd
import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt6.QtWidgets import QApplication  # noqa: E402

from trade_scanner_fh import data_engine, indicators, scanner  # noqa: E402
from trade_scanner_fh.gui import widgets as W  # noqa: E402
from trade_scanner_fh.scanner import ScanParams  # noqa: E402

RS_PREFIXES = ("rs_market", "rs_nasdaq", "rs_sector")


def _px(closes, start="2025-01-02"):
    idx = pd.bdate_range(start, periods=len(closes))
    return pd.DataFrame({"Close": closes, "Open": closes, "High": closes,
                         "Low": closes, "Volume": [1_000] * len(closes)},
                        index=idx)


@pytest.fixture
def store(tmp_path, monkeypatch):
    """One 120-bar ticker whose window return and 20-bar return differ, and
    three benchmarks that are flat except for a known drift."""
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    data_engine.clear_ohlcv_cache()
    n = 120
    closes = list(np.linspace(50, 110, n))
    df = _px(closes)
    df.index.name = "Date"
    df.to_parquet(tmp_path / "TEST.parquet")
    data_engine.clear_ohlcv_cache()
    bench = _px(list(np.linspace(100, 112, n)))
    return df, {"SPY": bench, "ONEQ": bench, "XLK": bench}


def _params(df, start_pos, **kw):
    base = dict(start_date=df.index[start_pos].date(),
                end_date=df.index[-1].date())
    for p in RS_PREFIXES:
        base[f"{p}_display_only"] = True
    base.update(kw)
    return ScanParams(**base)


def _expected(df, bench, start, end):
    s = df.loc[start:end, "Close"]
    b = bench.loc[start:end, "Close"]
    return (s.iloc[-1] / s.iloc[0]) / (b.iloc[-1] / b.iloc[0])


# ----------------------------------------------------------------------
# Scanner: the span each RS row measures
# ----------------------------------------------------------------------

def test_lookback_mode_is_unchanged(store):
    df, bench = store
    params = _params(df, 60)
    row = scanner._compute_ticker("TEST", params, bench, {"TEST": "XLK"})
    full = df.loc[:df.index[-1]]
    for p, etf in (("rs_market", "SPY"), ("rs_nasdaq", "ONEQ"),
                   ("rs_sector", "XLK")):
        assert row[p] == pytest.approx(indicators.relative_strength_ratio(
            full, bench[etf], lookback=20))
        assert row[p] == pytest.approx(
            _expected(df, bench[etf], df.index[-20], df.index[-1]))


@pytest.mark.parametrize("prefix", RS_PREFIXES)
def test_scan_window_measures_start_to_end(store, prefix):
    df, bench = store
    params = _params(df, 60, **{f"{prefix}_scan_window": True})
    row = scanner._compute_ticker("TEST", params, bench, {"TEST": "XLK"})
    want = _expected(df, bench["SPY"], df.index[60], df.index[-1])
    assert row[prefix] == pytest.approx(want)
    # ...and only that row moved: the other two still read 20 bars.
    twenty = _expected(df, bench["SPY"], df.index[-20], df.index[-1])
    for other in RS_PREFIXES:
        if other != prefix:
            assert row[other] == pytest.approx(twenty)
    assert want != pytest.approx(twenty)


def test_scan_window_ignores_the_lookback(store):
    df, bench = store
    a = scanner._compute_ticker("TEST", _params(
        df, 60, rs_market_scan_window=True, rs_market_lookback=5), bench)
    b = scanner._compute_ticker("TEST", _params(
        df, 60, rs_market_scan_window=True, rs_market_lookback=200), bench)
    assert a["rs_market"] == b["rs_market"]


def test_scan_window_rs_is_pct_gain_over_the_benchmark(store):
    """Same base bar as % Gain: the window's first bar."""
    df, bench = store
    row = scanner._compute_ticker("TEST", _params(
        df, 60, rs_market_scan_window=True), bench)
    b = bench["SPY"].loc[df.index[60]:df.index[-1], "Close"]
    assert row["rs_market"] == pytest.approx(
        (1 + row["pct_gain"] / 100) / (b.iloc[-1] / b.iloc[0]))


def test_scan_window_params_default_off():
    p = ScanParams(start_date=_dt.date(2025, 1, 1), end_date=_dt.date(2025, 6, 1))
    for prefix in RS_PREFIXES:
        assert getattr(p, f"{prefix}_scan_window") is False


# ----------------------------------------------------------------------
# RS S&P P/S follows its row's window
# ----------------------------------------------------------------------

def _anchors(df, pos):
    return {"TEST": df.index[pos]}


def test_rs_ps_na_when_split_is_outside_the_lookback(store):
    df, bench = store
    row = scanner._compute_ticker("TEST", _params(df, 60), bench,
                                  None, None, None, _anchors(df, 70))
    assert np.isnan(row["rs_market_post_split"])     # 49 bars back, lookback 20


def test_rs_ps_inside_the_lookback(store):
    df, bench = store
    row = scanner._compute_ticker("TEST", _params(df, 60), bench,
                                  None, None, None, _anchors(df, 110))
    assert row["rs_market_post_split"] == pytest.approx(
        _expected(df, bench["SPY"], df.index[111], df.index[-1]))


def test_rs_ps_scan_window_reaches_a_split_the_lookback_cannot(store):
    df, bench = store
    row = scanner._compute_ticker("TEST", _params(
        df, 60, rs_market_scan_window=True), bench,
        None, None, None, _anchors(df, 70))
    assert row["rs_market_post_split"] == pytest.approx(
        _expected(df, bench["SPY"], df.index[71], df.index[-1]))


def test_rs_ps_scan_window_na_when_split_predates_the_scan(store):
    """A split inside the 20-bar lookback but before a SHORT scan window's
    Start is outside the scan window."""
    df, bench = store
    row = scanner._compute_ticker("TEST", _params(
        df, 112, rs_market_scan_window=True), bench,
        None, None, None, _anchors(df, 105))
    assert np.isnan(row["rs_market_post_split"])
    # The same split IS inside the default 20-bar lookback.
    row = scanner._compute_ticker("TEST", _params(df, 112), bench,
                                  None, None, None, _anchors(df, 105))
    assert not np.isnan(row["rs_market_post_split"])


def test_pct_gain_ps_follows_the_scan_window(store):
    df, bench = store
    inside = scanner._compute_ticker("TEST", _params(df, 60), bench,
                                     None, None, None, _anchors(df, 70))
    assert inside["post_split_start_date"] == df.index[71].date()
    outside = scanner._compute_ticker("TEST", _params(df, 60), bench,
                                      None, None, None, _anchors(df, 40))
    assert np.isnan(outside["pct_gain_post_split"])
    assert outside["post_split_start_date"] is None


# ----------------------------------------------------------------------
# Funnel label
# ----------------------------------------------------------------------

def test_funnel_label_names_the_scan_window():
    base = dict(start_date=_dt.date(2025, 1, 1), end_date=_dt.date(2025, 6, 1))
    for prefix in RS_PREFIXES:
        base[f"{prefix}_enabled"] = True
    labels = [s[0] for s in scanner._build_filter_stages(ScanParams(
        **base, rs_nasdaq_scan_window=True)) if s[0].startswith("RS ")]
    assert "RS S&P >= 1.0" in labels
    assert "RS NASDAQ >= 1.0 (scan window)" in labels
    assert "RS Sector >= 1.0" in labels


# ----------------------------------------------------------------------
# GUI: the box, the greyout, the params, presets
# ----------------------------------------------------------------------

@pytest.fixture(scope="module")
def qapp():
    yield QApplication.instance() or QApplication([])


@pytest.fixture
def panel(qapp):
    return W.IndicatorPanel()


def _greyed(spin) -> bool:
    return (not spin.isEnabled()
            and spin.styleSheet() == W.IndicatorRow._GREYED_INPUT_STYLE)


def test_every_rs_row_has_the_box_off_by_default(panel):
    for key in RS_PREFIXES:
        box = panel.rows[key].spinboxes["scan_window"]
        assert box.text() == "Scan Window"
        assert box.isChecked() is False
        assert panel.rows[key].spinboxes["lookback"].isEnabled()


@pytest.mark.parametrize("key", RS_PREFIXES)
def test_ticking_greys_days_and_unticking_restores_it(panel, key):
    row = panel.rows[key]
    row.spinboxes["scan_window"].setChecked(True)
    assert _greyed(row.spinboxes["lookback"])
    row.spinboxes["scan_window"].setChecked(False)
    days = row.spinboxes["lookback"]
    assert days.isEnabled() and days.styleSheet() == ""


@pytest.mark.parametrize("key", RS_PREFIXES)
def test_filter_and_display_only_toggles_keep_days_greyed(panel, key):
    """Both toggles strip every input's stylesheet; the greyout comes back."""
    row = panel.rows[key]
    row.spinboxes["scan_window"].setChecked(True)
    row.display_only.setChecked(True)
    assert _greyed(row.spinboxes["lookback"])
    row.toggle.setChecked(True)            # mutex: turns Display Only off
    assert _greyed(row.spinboxes["lookback"])
    row.toggle.setChecked(False)
    assert _greyed(row.spinboxes["lookback"])


def test_build_scan_params_carries_the_box(panel):
    panel.rows["rs_sector"].spinboxes["scan_window"].setChecked(True)
    p = panel.build_scan_params(_dt.date(2025, 1, 1), _dt.date(2025, 6, 1))
    assert p.rs_sector_scan_window is True
    assert p.rs_market_scan_window is False
    assert p.rs_nasdaq_scan_window is False


def test_preset_round_trip_restores_box_and_greyout(panel, qapp):
    panel.rows["rs_market"].spinboxes["scan_window"].setChecked(True)
    saved = panel.to_dict()
    assert saved["rs_market"]["scan_window"] is True
    other = W.IndicatorPanel()
    other.from_dict(saved)
    assert other.rows["rs_market"].spinboxes["scan_window"].isChecked()
    assert _greyed(other.rows["rs_market"].spinboxes["lookback"])
    assert other.build_scan_params(
        _dt.date(2025, 1, 1), _dt.date(2025, 6, 1)).rs_market_scan_window


def test_old_preset_without_the_key_loads_unticked(panel):
    """A preset saved before the box existed must not inherit the session's
    tick — the row's built-in default (off) applies."""
    old = panel.to_dict()
    for key in RS_PREFIXES:
        old[key].pop("scan_window")
    panel.rows["rs_nasdaq"].spinboxes["scan_window"].setChecked(True)
    panel.from_dict(old)
    for key in RS_PREFIXES:
        assert not panel.rows[key].spinboxes["scan_window"].isChecked()
        assert panel.rows[key].spinboxes["lookback"].isEnabled()


def test_preset_reasserting_the_same_tick_still_greys(panel):
    """`setChecked` emits nothing when the value is unchanged, so the load
    path must re-derive the greyout itself."""
    box = panel.rows["rs_market"].spinboxes["scan_window"]
    box.setChecked(True)
    days = panel.rows["rs_market"].spinboxes["lookback"]
    days.setEnabled(True)
    days.setStyleSheet("")                 # knocked out of sync
    panel.from_dict(panel.to_dict())
    assert _greyed(days)
