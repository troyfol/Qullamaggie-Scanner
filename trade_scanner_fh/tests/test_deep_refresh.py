"""Deep OHLCV refresh + null last-bar detection (2026-08-29).

Both features exist because of one incident. A Saturday refill (13:00-15:24 ET)
wrote a NaN Open/High/Low/Close bar dated 2026-08-28 for 12,450 of 14,747
cached tickers — 99.9% of every ticker that had a bar for that session — and
the run reported `0 errors`, because every download did succeed. What came back
was null.

Two properties made it unrecoverable from inside the app, and both are pinned
here:

* Nothing computed "what fraction of this refresh is unusable", so the failure
  was silent (`test_last_bar_health_*`, `test_report_*`).
* Staleness is judged by a file's last DATE, which was correct on every
  poisoned file — so Force OHLCV Refresh re-checked all 14,747 tickers and
  skipped 12,737 of them (`test_deep_refresh_never_consults_staleness`).
"""
from datetime import date

import pandas as pd
import pytest

from trade_scanner_fh import config, data_engine
from trade_scanner_fh.data_engine import (
    ScrapeResult,
    clear_ohlcv_cache,
    summarize_last_bar_health,
    trading_days_back,
)


def _sessions(dates) -> pd.DatetimeIndex:
    return pd.DatetimeIndex([pd.Timestamp(d) for d in dates]).normalize().sort_values()


# A real weekend straddle: Thu, Fri, then Monday. The weekend is simply absent
# from a session index, which is the whole reason the window is resolved
# against one instead of by subtracting calendar days.
_IDX = _sessions(["2026-08-26", "2026-08-27", "2026-08-28", "2026-08-31"])


def _seed_existing_parquet(tmp_path, symbol="AAPL", n=300):
    """Write an n-bar parquet standing in for an established cache."""
    idx = pd.date_range("2024-01-01", periods=n, freq="B")
    df = pd.DataFrame({
        "Open": [100.0] * n, "High": [101.0] * n, "Low": [99.0] * n,
        "Close": [100.0] * n, "Volume": [1_000_000] * n,
        "Stock Splits": [0.0] * n,
    }, index=idx)
    df.index.name = "Date"
    df.to_parquet(tmp_path / f"{symbol}.parquet")
    return df


# ----------------------------------------------------------------------
# trading_days_back — the window is counted in sessions, not calendar days
# ----------------------------------------------------------------------

def test_trading_days_back_counts_sessions_not_calendar_days():
    assert trading_days_back(1, sessions=_IDX) == date(2026, 8, 31)
    # 2 sessions back from Monday is FRIDAY, not Sunday — the property that
    # makes "1 day back" mean the most recent session all weekend long.
    assert trading_days_back(2, sessions=_IDX) == date(2026, 8, 28)
    assert trading_days_back(3, sessions=_IDX) == date(2026, 8, 27)


def test_trading_days_back_clamps_instead_of_failing():
    """Asking for more history than the reference holds widens the window to
    everything available; it must not raise and abort the refresh."""
    assert trading_days_back(500, sessions=_IDX) == date(2026, 8, 26)


def test_trading_days_back_rejects_nonsense_counts():
    with pytest.raises(ValueError):
        trading_days_back(0, sessions=_IDX)


def test_trading_days_back_returns_none_without_a_calendar(monkeypatch):
    """A fresh install has no reference ticker cached. The caller needs to be
    able to tell that apart from a real date and fall back."""
    monkeypatch.setattr(data_engine, "reference_sessions", lambda: None)
    assert trading_days_back(1) is None


# ----------------------------------------------------------------------
# summarize_last_bar_health — denominator choice is the whole game
# ----------------------------------------------------------------------

def test_last_bar_health_excludes_tickers_with_no_bar_on_that_session():
    """Thousands of cached names are delisted or illiquid and legitimately have
    no bar on any given day. Counting them as healthy would dilute the rate and
    mask the exact failure this is built to catch: here the true rate is 66.7%
    (fires at the 50% threshold), while the diluted rate would be 40% (silent).
    """
    sess = pd.Timestamp("2026-08-28")
    old = pd.Timestamp("2026-08-20")
    results = [
        ScrapeResult(symbol="A", last_bar_date=sess, last_bar_nan=True),
        ScrapeResult(symbol="B", last_bar_date=sess, last_bar_nan=True),
        ScrapeResult(symbol="C", last_bar_date=sess, last_bar_nan=False),
        ScrapeResult(symbol="D", last_bar_date=old, last_bar_nan=False),
        ScrapeResult(symbol="E", last_bar_date=old, last_bar_nan=False),
    ]
    health = summarize_last_bar_health(results)

    assert health.session == sess
    assert (health.null_count, health.total) == (2, 3)
    assert health.pct == pytest.approx(200 / 3, abs=0.01)
    assert health.pct >= config.OHLCV_NAN_LAST_BAR_WARN_PCT
    # The diluted denominator that would have stayed quiet:
    assert (2 / 5) * 100 < config.OHLCV_NAN_LAST_BAR_WARN_PCT


def test_last_bar_health_is_inert_on_empty_or_dateless_results():
    assert summarize_last_bar_health([]).total == 0
    assert summarize_last_bar_health([ScrapeResult(symbol="A")]).total == 0


def test_incident_ratio_fires_and_an_ordinary_session_does_not():
    """99.9% (the 2026-08-28 measurement) must warn; a handful of halted names
    on a normal session must not."""
    sess = pd.Timestamp("2026-08-28")

    def _batch(n_null, n_ok):
        return (
            [ScrapeResult(symbol=f"N{i}", last_bar_date=sess, last_bar_nan=True)
             for i in range(n_null)]
            + [ScrapeResult(symbol=f"K{i}", last_bar_date=sess, last_bar_nan=False)
               for i in range(n_ok)]
        )

    incident = summarize_last_bar_health(_batch(1245, 1))
    assert incident.pct > 99.0
    assert incident.pct >= config.OHLCV_NAN_LAST_BAR_WARN_PCT

    ordinary = summarize_last_bar_health(_batch(3, 1200))
    assert ordinary.pct < config.OHLCV_NAN_LAST_BAR_WARN_PCT


# ----------------------------------------------------------------------
# download_one(force_start=...) — must merge, must never truncate
# ----------------------------------------------------------------------

def test_force_start_requests_the_window_but_keeps_full_history(tmp_path, monkeypatch):
    """THE guard test for this feature.

    A provider handed a narrow window returns only that window. If a forced
    pull ever reached download_one's `combined = new_df` replace path, the
    ticker's parquet would become a one-row file — five years of history gone,
    times the whole universe. The forced window must always MERGE.
    """
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    clear_ohlcv_cache()
    seeded = _seed_existing_parquet(tmp_path, "AAPL", n=300)

    captured = {}

    def fake_download(symbol, start, end):
        captured["start"] = start
        # Only the requested bar comes back — the realistic narrow response.
        idx = seeded.index[-1:]
        return pd.DataFrame({
            "Open": [111.0], "High": [112.0], "Low": [110.0],
            "Close": [111.5], "Volume": [2_000_000], "Stock Splits": [0.0],
        }, index=idx)

    monkeypatch.setattr(data_engine, "_download_raw", fake_download)
    res = data_engine.download_one(
        "AAPL", force_start=date(2026, 8, 28), overwrite=True,
    )

    assert res.status == "ok"
    assert captured["start"] == "2026-08-28", "forced window must drive the request"
    final = pd.read_parquet(tmp_path / "AAPL.parquet")
    assert len(final) == 300, "history must survive a narrow forced pull"
    assert final["Close"].iloc[-1] == 111.5, "the requested bar must be updated"


def test_force_start_ignores_how_current_the_cached_file_looks(tmp_path, monkeypatch):
    """The staleness check would skip this file — its last date is current.
    A forced pull must re-request it anyway; that is the entire point."""
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    clear_ohlcv_cache()
    seeded = _seed_existing_parquet(tmp_path, "AAPL", n=50)

    calls = []

    def fake_download(symbol, start, end):
        calls.append(start)
        idx = seeded.index[-1:]
        return pd.DataFrame({
            "Open": [100.0], "High": [101.0], "Low": [99.0],
            "Close": [100.0], "Volume": [1_000_000], "Stock Splits": [0.0],
        }, index=idx)

    monkeypatch.setattr(data_engine, "_download_raw", fake_download)
    data_engine.download_one("AAPL", force_start=date(2024, 1, 1))
    assert calls == ["2024-01-01"]


# ----------------------------------------------------------------------
# overwrite — the conflict guard resolves in favour of the cache by default
# ----------------------------------------------------------------------

def _conflicting_setup(tmp_path, monkeypatch):
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    clear_ohlcv_cache()
    seeded = _seed_existing_parquet(tmp_path, "AAPL", n=50)

    def fake_download(symbol, start, end):
        # Close disagrees by ~90%, far beyond PRICE_JUMP_PCT.
        idx = seeded.index[-1:]
        return pd.DataFrame({
            "Open": [10.0], "High": [10.5], "Low": [9.5],
            "Close": [10.0], "Volume": [1_000_000], "Stock Splits": [0.0],
        }, index=idx)

    monkeypatch.setattr(data_engine, "_download_raw", fake_download)


def test_overwrite_replaces_a_bar_the_guard_would_have_kept(tmp_path, monkeypatch):
    _conflicting_setup(tmp_path, monkeypatch)
    res = data_engine.download_one(
        "AAPL", force_start=date(2024, 1, 1), overwrite=True,
    )
    final = pd.read_parquet(tmp_path / "AAPL.parquet")
    assert final["Close"].iloc[-1] == 10.0, "explicit repair must win"
    assert res.bars_overwritten == 1, "and must be reported, not silent"


def test_without_overwrite_the_cache_still_wins(tmp_path, monkeypatch):
    """Regression guard: the default path must keep protecting settled bars
    from provisional re-sends (audit 2026-08-12 INT-11 / 2026-08-16 F5)."""
    _conflicting_setup(tmp_path, monkeypatch)
    res = data_engine.download_one("AAPL", force_start=date(2024, 1, 1))
    final = pd.read_parquet(tmp_path / "AAPL.parquet")
    assert final["Close"].iloc[-1] == 100.0, "cached bar must survive"
    assert res.bars_overwritten == 0


# ----------------------------------------------------------------------
# last_bar_nan — the flag the end-of-run warning is built on
# ----------------------------------------------------------------------

def test_null_priced_bar_is_flagged(tmp_path, monkeypatch):
    """Reproduces the 2026-08-28 signature exactly: all four price fields null,
    Volume and Stock Splits populated. That is what `auto_adjust=True` produces
    from a null adjusted close, and what no existing check rejected."""
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    clear_ohlcv_cache()
    seeded = _seed_existing_parquet(tmp_path, "AAPL", n=50)
    new_day = seeded.index[-1] + pd.Timedelta(days=1)

    def fake_download(symbol, start, end):
        return pd.DataFrame({
            "Open": [float("nan")], "High": [float("nan")],
            "Low": [float("nan")], "Close": [float("nan")],
            "Volume": [38_500_185], "Stock Splits": [0.0],
        }, index=pd.DatetimeIndex([new_day], name="Date"))

    monkeypatch.setattr(data_engine, "_download_raw", fake_download)
    res = data_engine.download_one("AAPL")

    assert res.status == "ok", "the download itself succeeds — that was the trap"
    assert res.last_bar_nan is True
    assert res.last_bar_date == pd.Timestamp(new_day).normalize()


def test_healthy_bar_is_not_flagged(tmp_path, monkeypatch):
    monkeypatch.setattr(data_engine.config, "PARQUET_DIR", tmp_path)
    clear_ohlcv_cache()
    seeded = _seed_existing_parquet(tmp_path, "AAPL", n=50)
    new_day = seeded.index[-1] + pd.Timedelta(days=1)

    def fake_download(symbol, start, end):
        return pd.DataFrame({
            "Open": [100.0], "High": [101.0], "Low": [99.0], "Close": [100.5],
            "Volume": [1_000_000], "Stock Splits": [0.0],
        }, index=pd.DatetimeIndex([new_day], name="Date"))

    monkeypatch.setattr(data_engine, "_download_raw", fake_download)
    res = data_engine.download_one("AAPL")
    assert res.last_bar_nan is False
    assert res.last_bar_date == pd.Timestamp(new_day).normalize()


# ----------------------------------------------------------------------
# UpdateWorker deep-refresh mode
# ----------------------------------------------------------------------

def test_deep_refresh_never_consults_staleness(_qapp, monkeypatch):
    """The failure that made 2026-08-28 unrecoverable was staleness passing on
    every poisoned file. Deep refresh must not ask the question at all."""
    from trade_scanner_fh.gui import workers as workers_mod

    def _boom(*a, **k):
        raise AssertionError("deep refresh must not run the staleness check")

    monkeypatch.setattr(workers_mod, "cached_spans", _boom)
    monkeypatch.setattr(workers_mod, "_last_cached_date", _boom)
    monkeypatch.setattr(workers_mod, "reference_sessions", lambda: _IDX)

    worker = workers_mod.UpdateWorker(
        ["AAA", "BBB", "CCC"], force_days_back=2, overwrite=True,
    )
    seen = {}

    def _fake_pass(to_update, *, force_start=None):
        seen["symbols"] = list(to_update)
        seen["force_start"] = force_start
        return (3, 0)

    monkeypatch.setattr(worker, "_download_pass", _fake_pass)
    monkeypatch.setattr(worker, "_finish", lambda updated, errors: None)

    worker._do_update()

    assert seen["symbols"] == ["AAA", "BBB", "CCC"], "every symbol, no filtering"
    # 2 sessions back from the 08-31 Monday is the 08-28 Friday whose bars broke.
    assert seen["force_start"] == date(2026, 8, 28)


def test_normal_update_passes_no_forced_window(_qapp, monkeypatch):
    """Regression guard: without force_days_back the worker must take the
    ordinary staleness path and forward force_start=None."""
    from trade_scanner_fh.gui import workers as workers_mod

    monkeypatch.setattr(workers_mod, "cached_spans", lambda syms: {})
    monkeypatch.setattr(workers_mod, "_last_cached_date", lambda s: None)

    worker = workers_mod.UpdateWorker(["AAA"])
    seen = {}

    def _fake_pass(to_update, *, force_start=None):
        seen["force_start"] = force_start
        seen["symbols"] = list(to_update)
        return (1, 0)

    monkeypatch.setattr(worker, "_download_pass", _fake_pass)
    monkeypatch.setattr(worker, "_finish", lambda updated, errors: None)

    worker._do_update()
    assert seen["force_start"] is None
    assert seen["symbols"] == ["AAA"]     # classed "missing", so still updated


def test_report_warns_above_threshold_and_stays_quiet_below(_qapp, monkeypatch):
    from trade_scanner_fh.gui import workers as workers_mod

    sess = pd.Timestamp("2026-08-28")

    def _emitted(n_null, n_ok):
        worker = workers_mod.UpdateWorker(["X"])
        worker._results = (
            [ScrapeResult(symbol=f"N{i}", last_bar_date=sess, last_bar_nan=True)
             for i in range(n_null)]
            + [ScrapeResult(symbol=f"K{i}", last_bar_date=sess, last_bar_nan=False)
               for i in range(n_ok)]
        )
        lines = []
        worker.log_msg.connect(lines.append)
        worker._report_last_bar_health()
        return lines

    loud = _emitted(99, 1)
    assert any("SUSPECT OHLCV REFRESH" in ln for ln in loud)
    assert any("99.0%" in ln for ln in loud)

    assert _emitted(2, 98) == [], "an ordinary session must not warn"


def test_report_survives_malformed_results(_qapp):
    """A reporting helper must never be able to fail an update that otherwise
    succeeded."""
    from trade_scanner_fh.gui import workers as workers_mod

    worker = workers_mod.UpdateWorker(["X"])
    worker._results = [object()]        # no last_bar_date attribute at all
    worker._report_last_bar_health()    # must not raise


def test_deep_refresh_dialog_builds_and_cancels_cleanly(_qapp, monkeypatch):
    """Exercises the real dialog body — widget construction plus the live
    window-preview closure — which a compile check cannot reach. Cancelling
    must leave no worker behind."""
    from PyQt6.QtWidgets import QDialog, QWidget
    from trade_scanner_fh.gui import main_window as mw

    monkeypatch.setattr(mw, "cached_symbols", lambda: {"AAA", "BBB"})
    monkeypatch.setattr(mw, "reference_sessions", lambda: _IDX)
    monkeypatch.setattr(
        QDialog, "exec", lambda self: QDialog.DialogCode.Rejected,
    )

    class _Host(QWidget):
        _update_worker = None

        def _start_worker(self, *a, **k):      # must never be reached
            raise AssertionError("cancelled dialog must not start a worker")

    host = _Host()
    mw.MainWindow._deep_ohlcv_refresh_dialog(host)


def test_deep_refresh_dialog_refuses_when_nothing_is_cached(_qapp, monkeypatch):
    from PyQt6.QtWidgets import QMessageBox, QWidget
    from trade_scanner_fh.gui import main_window as mw

    monkeypatch.setattr(mw, "cached_symbols", lambda: set())
    warned = []
    monkeypatch.setattr(
        QMessageBox, "warning",
        lambda *a, **k: warned.append(a) or QMessageBox.StandardButton.Ok,
    )

    class _Host(QWidget):
        _update_worker = None

    mw.MainWindow._deep_ohlcv_refresh_dialog(_Host())
    assert warned, "an empty cache must be reported, not silently accepted"
