"""v8.0.1 — OHLCV bar health: gaps B and D from the 2026-09-27 review.

D. A re-sent bar with a NULL Close replaced a cached bar with real prices.
   `_reject_conflicting_bars` measures disagreement as a % of the cached Close;
   a null on either side made that null, read as "no conflict", and the
   keep-last merge adopted it — with overwrite off AND on (proved on a scratch
   file: a cached Close of 100.0 came back NaN). The mirror case must keep
   working: a NULL cached bar losing to a real re-send is how the refetch
   overlap repairs a null session on the next update.

B. The null-bar warning judged only the NEWEST session of each run, once, in
   one log line. Nothing confirmed a flagged session was later repaired, nor
   noticed when it was not. Flagged sessions are now kept with the list of
   tickers whose bar was null; every later run reports those bars; the session
   clears only when they carry prices. Per TICKER, because the 2026-09-24 nulls
   were clustered by sweep order (A-O clean, Q-Z 64-80% null): a run stopped
   after the first letters would otherwise measure ~0% and clear it.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest

from trade_scanner_fh import config, data_engine
from trade_scanner_fh.data_engine import (
    LastBarHealth, ScrapeResult, SessionCheck, _keep_priced_cached_bars,
    _watched_bar_states, load_suspect_sessions, save_suspect_sessions,
    suspect_session_resolved, update_suspect_sessions,
)

TZ = "America/New_York"
S24 = pd.Timestamp("2026-09-24")
NOW = datetime(2026, 9, 25, 18, 0, tzinfo=timezone.utc)


def _frame(dates, *, close=100.0, volume=1_000_000.0, tz=TZ, nulls=()):
    idx = pd.DatetimeIndex(pd.to_datetime(dates), name="Date")
    if tz:
        idx = idx.tz_localize(tz)
    n = len(idx)
    df = pd.DataFrame({
        "Open": [close] * n, "High": [close + 1] * n, "Low": [close - 1] * n,
        "Close": [close] * n, "Volume": [volume] * n,
        "Stock Splits": [0.0] * n, "Dividends": [0.0] * n,
    }, index=idx)
    for d in nulls:
        ts = pd.Timestamp(d).tz_localize(tz) if tz else pd.Timestamp(d)
        df.loc[ts, ["Open", "High", "Low", "Close"]] = np.nan
    return df


@pytest.fixture
def store(tmp_path, monkeypatch):
    """A cached ticker in the real on-disk shape (tz-aware ET index)."""
    monkeypatch.setattr(config, "PARQUET_DIR", tmp_path)
    data_engine.clear_ohlcv_cache()

    def seed(symbol="T", *, dates=None, nulls=(), close=100.0, tz=TZ):
        dates = dates or list(pd.bdate_range("2026-08-03", "2026-09-24"))
        df = _frame(dates, close=close, nulls=nulls, tz=tz)
        df[data_engine._STORED_COLUMNS].to_parquet(tmp_path / f"{symbol}.parquet")
        return df
    return seed


def _serve(monkeypatch, *, close=103.0, nulls=(), through="2026-09-25",
           tz=TZ):
    """Stub the provider: settled bars from the requested start, `nulls`
    served with null prices (the auto_adjust signature)."""
    calls = []

    def fake(symbol, start, end):
        calls.append((symbol, start))
        dates = pd.bdate_range(start, through)
        return _frame(list(dates), close=close, nulls=nulls, tz=tz)
    monkeypatch.setattr(data_engine, "_download_raw", fake)
    return calls


def _read(tmp_path, symbol="T"):
    df = pd.read_parquet(tmp_path / f"{symbol}.parquet")
    idx = df.index.tz_convert(None) if df.index.tz is not None else df.index
    df.index = idx.normalize()
    return df


# ======================================================================
# D — a null re-send never replaces real cached prices
# ======================================================================

@pytest.mark.parametrize("overwrite", [False, True])
def test_null_resend_never_replaces_a_priced_cached_bar(
        store, monkeypatch, tmp_path, overwrite):
    """The bug itself. Before v8.0.1 the cached 09-23 Close of 100.0 became
    NaN in both modes."""
    store()
    _serve(monkeypatch, nulls=["2026-09-23"])
    kw = {"overwrite": True, "force_start": pd.Timestamp("2026-09-20").date()} \
        if overwrite else {}
    res = data_engine.download_one("T", **kw)
    assert res.status == "ok"
    bar = _read(tmp_path).loc[pd.Timestamp("2026-09-23")]
    assert bar["Close"] == 100.0 and bar["Volume"] == 1_000_000.0, \
        "the cached bar is kept whole"
    assert res.null_bars_refused == 1


@pytest.mark.parametrize("overwrite", [False, True])
def test_a_null_cached_bar_is_still_repaired_by_a_priced_resend(
        store, monkeypatch, tmp_path, overwrite):
    """The other half of the same comparison, and the property the ordinary
    update's refetch overlap relies on to heal a null session."""
    store(nulls=["2026-09-24"])
    _serve(monkeypatch, close=103.0)
    kw = {"overwrite": True, "force_start": pd.Timestamp("2026-09-20").date()} \
        if overwrite else {}
    res = data_engine.download_one("T", **kw)
    assert _read(tmp_path).loc[S24, "Close"] == 103.0
    assert res.null_bars_refused == 0


def test_a_null_bar_on_a_new_date_is_still_written(store, monkeypatch,
                                                   tmp_path):
    """Refusing it would hide the incident: the newest-session health check
    is what has to see it."""
    store()
    _serve(monkeypatch, nulls=["2026-09-25"])
    res = data_engine.download_one("T")
    assert np.isnan(_read(tmp_path).loc[pd.Timestamp("2026-09-25"), "Close"])
    assert res.last_bar_nan is True
    assert res.null_bars_refused == 0


def test_a_fully_null_resend_leaves_the_file_intact(store, monkeypatch,
                                                    tmp_path):
    """Every overlap bar null (a wholesale bad response): nothing cached is
    lost and the file does not shrink."""
    before = store()
    _serve(monkeypatch, through="2026-09-24",
           nulls=list(pd.bdate_range("2026-09-01", "2026-09-24")))
    res = data_engine.download_one("T")
    after = _read(tmp_path)
    assert len(after) == len(before)
    assert after["Close"].notna().all()
    assert res.null_bars_refused >= 4


def test_overwrite_count_excludes_refused_bars(store, monkeypatch):
    store()
    _serve(monkeypatch, nulls=["2026-09-22", "2026-09-23"])
    res = data_engine.download_one(
        "T", overwrite=True, force_start=pd.Timestamp("2026-09-21").date())
    # 09-21..09-24 overlap = 4 bars, 2 of them refused.
    assert res.null_bars_refused == 2
    assert res.bars_overwritten == 2


def test_guard_survives_duplicate_cached_dates_and_missing_columns():
    idx = pd.DatetimeIndex(["2026-09-23", "2026-09-23", "2026-09-24"])
    old = pd.DataFrame({"Close": [100.0, 101.0, np.nan]}, index=idx)
    new = pd.DataFrame({"Close": [np.nan, 99.0]},
                       index=pd.DatetimeIndex(["2026-09-23", "2026-09-24"]))
    kept, n = _keep_priced_cached_bars("X", old, new)
    assert n == 1 and list(kept.index) == [pd.Timestamp("2026-09-24")]
    # No Close column anywhere: passes straight through, never raises.
    same, n = _keep_priced_cached_bars("X", old[[]], new)
    assert n == 0 and same is new


# ======================================================================
# B — watched bars reported by download_one
# ======================================================================

def test_watched_bar_states_reads_the_tz_aware_store_shape():
    df = _frame(["2026-09-23", "2026-09-24"], nulls=["2026-09-24"])
    st = _watched_bar_states(df, (pd.Timestamp("2026-09-23"), S24,
                                  pd.Timestamp("2026-09-25")))
    assert st == {pd.Timestamp("2026-09-23"): False, S24: True,
                  pd.Timestamp("2026-09-25"): None}
    assert _watched_bar_states(df, ()) == {}


def test_download_one_reports_the_watched_session_after_the_merge(
        store, monkeypatch):
    store(nulls=["2026-09-24"])
    _serve(monkeypatch)
    res = data_engine.download_one("T", watch_dates=(S24,))
    assert res.watched_bars == {S24: False}, "repaired by the overlap"


def test_download_many_forwards_watch_dates(monkeypatch):
    seen = []

    def fake(sym, *, force_start=None, overwrite=False, watch_dates=()):
        seen.append(watch_dates)
        return ScrapeResult(symbol=sym)
    monkeypatch.setattr(data_engine, "download_one", fake)
    data_engine.download_many(["A", "B"], min_interval_sec=0,
                              watch_dates=(S24,))
    assert seen == [(S24,), (S24,)]


# ======================================================================
# B — the flagged-session list (pure)
# ======================================================================

def _health(nulls, total, *, burst=0.0, session=S24):
    return LastBarHealth(session, nulls, total, nulls / total * 100.0,
                         burst, "Q0" if burst else None)


def _res(sym, state, *, day=S24, status="ok", watched=True):
    """A written result whose bar on `day` is null / priced / absent."""
    r = ScrapeResult(symbol=sym, status=status)
    if watched:
        r.watched_bars = {day: {"null": True, "priced": False,
                                "absent": None}[state]}
    else:
        r.last_bar_date, r.last_bar_nan = day, state == "null"
    return r


def _flag(null_syms, ok_syms, *, burst=0.0):
    """Run 1: the warning fires on S24 (newest session: last_bar_* only)."""
    results = ([_res(s, "null", watched=False) for s in null_syms]
               + [_res(s, "priced", watched=False) for s in ok_syms])
    return update_suspect_sessions(
        {}, results, _health(len(null_syms), len(results), burst=burst),
        flagged_by="launch update", now=NOW)


# The 09-24 shape: clean A-O, then the bad tail of the alphabet.
BAD = [f"Q{i:04d}" for i in range(2677)]
GOOD = [f"A{i:04d}" for i in range(12437 - 2677)]


def test_a_suspect_run_starts_a_watch_with_the_null_tickers():
    sessions, events = _flag(BAD, GOOD)
    e = sessions["2026-09-24"]
    assert e["still_null"] == sorted(BAD)
    assert (e["null_at_flag"], e["tickers_on_session"]) == (2677, 12437)
    assert e["flagged_by"] == "launch update"
    assert [ev.kind for ev in events] == ["flagged"]
    assert events[0].remaining == 2677


def test_an_ordinary_run_flags_nothing():
    results = [_res(f"T{i}", "priced", watched=False) for i in range(500)]
    sessions, events = update_suspect_sessions(
        {}, results, _health(1, 500), now=NOW)
    assert sessions == {} and events == []


def test_a_partial_run_over_clean_letters_does_not_clear_the_session():
    """THE trap: a run stopped after A-C re-reads 1,000 priced bars and zero
    flagged ones. It must learn nothing about 09-24 and say nothing."""
    sessions, _ = _flag(BAD, GOOD)
    after, events = update_suspect_sessions(
        sessions, [_res(s, "priced") for s in GOOD[:1000]], None, now=NOW)
    assert after["2026-09-24"]["still_null"] == sorted(BAD)
    assert events == []


def test_a_partial_repair_is_reported_and_stays_flagged():
    sessions, _ = _flag(BAD, GOOD)
    after, events = update_suspect_sessions(
        sessions, [_res(s, "priced") for s in BAD[:1500]], None, now=NOW)
    (ev,) = events
    assert (ev.kind, ev.rechecked, ev.repaired, ev.remaining) == \
        ("rechecked", 1500, 1500, 1177)
    assert not ev.resolved
    assert len(after["2026-09-24"]["still_null"]) == 1177
    assert after["2026-09-24"]["last_checked_at"]


def test_the_09_24_repair_resolves_and_drops_the_session():
    """All but one repaired (the real deep refresh left 1): at noise level."""
    sessions, _ = _flag(BAD, GOOD)
    results = ([_res(s, "priced") for s in BAD[:-1]]
               + [_res(BAD[-1], "null")])
    after, events = update_suspect_sessions(sessions, results, None, now=NOW)
    (ev,) = events
    assert ev.resolved and ev.repaired == 2676 and ev.still_null == 1
    assert "2026-09-24" not in after


def test_bars_still_null_keep_the_session_flagged():
    sessions, _ = _flag(BAD, GOOD)
    after, events = update_suspect_sessions(
        sessions, [_res(s, "null") for s in BAD], None, now=NOW)
    (ev,) = events
    assert ev.still_null == 2677 and ev.repaired == 0 and not ev.resolved
    assert len(after["2026-09-24"]["still_null"]) == 2677


def test_a_burst_only_flag_needs_most_of_its_bars_repaired():
    """The burst rule can fire on 100 nulls in a 12k session (0.8%) — below
    the 1% line from the start. The share rule stops that clearing it with
    nothing repaired."""
    bad = [f"Q{i:03d}" for i in range(100)]
    sessions, _ = _flag(bad, [f"A{i:05d}" for i in range(11900)], burst=25.0)
    half, ev = update_suspect_sessions(
        sessions, [_res(s, "priced") for s in bad[:50]], None, now=NOW)
    assert not ev[0].resolved, "50 still null: under 1% but not repaired"
    done, ev = update_suspect_sessions(
        half, [_res(s, "priced") for s in bad[50:91]], None, now=NOW)
    assert ev[0].remaining == 9 and ev[0].resolved


def test_newly_null_and_vanished_bars_are_counted():
    sessions, _ = _flag(BAD[:600], GOOD[:9400])       # 6%: flagged
    results = ([_res(BAD[0], "absent"), _res(GOOD[0], "null")]
               + [_res(s, "priced") for s in BAD[1:600]])
    after, (ev,) = update_suspect_sessions(sessions, results, None, now=NOW)
    assert (ev.no_bar, ev.newly_null, ev.repaired) == (1, 1, 599)
    assert ev.remaining == 1 and ev.resolved      # 1 of 10,000, 1 of 600


def test_unwritten_results_say_nothing():
    sessions, _ = _flag(BAD, GOOD)
    results = [_res(s, "priced", status="error") for s in BAD]
    after, events = update_suspect_sessions(sessions, results, None, now=NOW)
    assert events == [] and len(after["2026-09-24"]["still_null"]) == 2677


def test_a_session_flagged_again_keeps_its_first_record():
    sessions, _ = _flag(BAD, GOOD)
    again, events = update_suspect_sessions(
        sessions, [_res(s, "null", watched=False) for s in BAD[:600]],
        _health(600, 1000), flagged_by="Force OHLCV Refresh", now=NOW)
    e = again["2026-09-24"]
    assert e["null_at_flag"] == 2677 and e["flagged_by"] == "launch update"
    assert [ev.kind for ev in events] == ["rechecked"]


def test_resolution_boundaries():
    e = {"still_null": ["X"] * 124, "tickers_on_session": 12437,
         "null_at_flag": 2677}
    assert suspect_session_resolved(e)            # 0.997% < 1%
    e["still_null"] = ["X"] * 125
    assert not suspect_session_resolved(e)        # 1.005%
    assert suspect_session_resolved(
        {"still_null": [], "tickers_on_session": 1, "null_at_flag": 1})


# ======================================================================
# B — the file
# ======================================================================

def test_file_round_trip_and_tolerance(tmp_path, monkeypatch):
    path = tmp_path / "s.json"
    monkeypatch.setattr(data_engine, "_suspect_sessions_path", lambda: path)
    assert load_suspect_sessions() == {}            # no file
    sessions, _ = _flag(BAD[:20], GOOD[:200])       # 9.1% of 220: flagged
    assert list(sessions) == ["2026-09-24"]
    assert save_suspect_sessions(sessions)
    assert load_suspect_sessions() == sessions
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["sessions"]["not-a-date"] = {"still_null": ["A"]}
    raw["sessions"]["2026-09-25"] = "garbage"
    path.write_text(json.dumps(raw), encoding="utf-8")
    assert list(load_suspect_sessions()) == ["2026-09-24"]
    path.write_text("{ not json", encoding="utf-8")
    assert load_suspect_sessions() == {}


def test_describe_names_the_counts():
    sessions, _ = _flag(BAD, GOOD)
    txt = data_engine.describe_suspect_session(
        "2026-09-24", sessions["2026-09-24"])
    assert "2,677 of 12,437" in txt and "21.5%" in txt
    assert "launch update" in txt and "not re-checked yet" in txt


# ======================================================================
# B — UpdateWorker
# ======================================================================

@pytest.fixture
def quiet_finish(monkeypatch):
    """`_finish` without its side trips: no anomaly CSV, no gap rebuilds."""
    from trade_scanner_fh.gui import workers as W
    monkeypatch.setattr(W, "write_anomaly_report", lambda results: 0)
    monkeypatch.setattr(config, "OHLCV_GAP_CHECK_ENABLED", False)
    return W


def _lines(worker):
    out = []
    worker.log_msg.connect(out.append)
    return out


def test_worker_flags_then_confirms_the_repair(_qapp, quiet_finish):
    W = quiet_finish
    w1 = W.UpdateWorker(["X"], run_label="launch update")
    w1._results = ([_res(s, "null", watched=False) for s in BAD]
                   + [_res(s, "priced", watched=False) for s in GOOD])
    lines1 = _lines(w1)
    w1._finish(12437, 0)
    assert any("now watching 2026-09-24" in ln for ln in lines1)
    assert any("re-checks these bars" in ln for ln in lines1), "warning says so"
    assert len(load_suspect_sessions()["2026-09-24"]["still_null"]) == 2677

    w2 = W.UpdateWorker(["X"])
    w2._results = [_res(s, "priced") for s in BAD]
    lines2 = _lines(w2)
    w2._finish(2677, 0)
    assert any("2026-09-24 re-checked" in ln and "RESOLVED" in ln
               for ln in lines2), lines2
    assert load_suspect_sessions() == {}


def test_worker_reports_refused_bars_in_the_summary(_qapp, quiet_finish):
    w = quiet_finish.UpdateWorker(["X"])
    a, b = ScrapeResult("A"), ScrapeResult("B")
    a.null_bars_refused, b.null_bars_refused = 3, 2
    w._results = [a, b]
    lines = _lines(w)
    w._finish(2, 0)
    assert any("5 null re-sent bar(s) refused across 2 ticker(s)" in ln
               for ln in lines), lines


def test_worker_survives_a_broken_list(_qapp, quiet_finish, monkeypatch):
    W = quiet_finish
    monkeypatch.setattr(W, "load_suspect_sessions",
                        lambda: (_ for _ in ()).throw(OSError("locked")))
    w = W.UpdateWorker(["X"])
    w._results = [ScrapeResult("A")]
    w._finish(1, 0)                                   # must not raise


def test_download_pass_sends_watched_days_including_the_probe(
        _qapp, quiet_finish, monkeypatch):
    """Both download paths carry the watch: the batch AND the single-ticker
    rate-limit probe (whose result is now kept with the rest)."""
    W = quiet_finish
    sessions, _ = _flag(BAD[:20], GOOD[:200])
    save_suspect_sessions(sessions)
    seen = {}

    def many(batch, **kw):
        seen["many"] = kw.get("watch_dates")
        return [ScrapeResult(s, status="error") for s in batch]

    def one(sym, **kw):
        seen["one"] = kw.get("watch_dates")
        return ScrapeResult(sym, status="ok")
    monkeypatch.setattr(W, "download_many", many)
    monkeypatch.setattr(W, "download_one", one)
    w = W.UpdateWorker([f"T{i:03d}" for i in range(201)], backoff_threshold=1,
                       backoff_wait=0, max_retries=1)
    w._download_pass(list(w.symbols))
    assert seen["many"] == (S24,) and seen["one"] == (S24,)
    assert any(r.symbol == "T200" and r.status == "ok" for r in w._results)


def test_format_lines():
    from trade_scanner_fh.gui.workers import format_session_check
    ev = SessionCheck("2026-09-24", "rechecked", rechecked=10, repaired=7,
                      still_null=3, no_bar=0, newly_null=2, remaining=900,
                      tickers_on_session=12437, null_at_flag=2677,
                      pct_at_flag=21.5, resolved=False)
    ln = format_session_check(ev)
    assert "7 now have prices, 3 still null; 2 newly null" in ln
    assert "900 of 12,437 (7.2%)" in ln and "still suspect" in ln
    assert "RESOLVED" in format_session_check(ev._replace(resolved=True))


# ======================================================================
# B — the window: launch reminder, status label, dismiss
# ======================================================================

@pytest.fixture
def window(_qapp, tmp_parquets, monkeypatch):
    from trade_scanner_fh.gui import main_window as mw_mod
    monkeypatch.setattr(mw_mod, "PRESETS_DIR", tmp_parquets / "presets")
    (tmp_parquets / "presets").mkdir(exist_ok=True)
    w = mw_mod.MainWindow()
    yield w
    w.close()
    w.deleteLater()


def test_launch_reminder_and_label(window):
    sessions, _ = _flag(BAD, GOOD)
    save_suspect_sessions(sessions)
    lines = []
    window.log_panel.write_line = lines.append
    window._report_suspect_sessions()
    assert any("OHLCV health reminder" in ln and "2026-09-24" in ln
               for ln in lines)
    window._update_label.setText("OHLCV: 14 cached, current")
    window._update_label.setStyleSheet("color: #4caf50;")
    window._mark_update_label_suspects()
    assert window._update_label.text().endswith("⚠ 09-24 suspect")
    assert "#ff9800" in window._update_label.styleSheet()
    assert "2,677 of 12,437" in window._update_label.toolTip()
    window._mark_update_label_suspects()          # idempotent
    assert window._update_label.text().count("⚠") == 1


def test_dismiss_clears_the_list_and_restores_the_label(window, monkeypatch):
    sessions, _ = _flag(BAD, GOOD)
    save_suspect_sessions(sessions)
    window._update_label.setText("OHLCV: 14 cached, current")
    window._update_label.setStyleSheet("color: #4caf50;")
    window._mark_update_label_suspects()

    monkeypatch.setattr(window, "_confirm_dismiss_health", lambda text: False)
    window._dismiss_ohlcv_health_warnings()
    assert load_suspect_sessions(), "declined: nothing changes"

    asked = []
    monkeypatch.setattr(window, "_confirm_dismiss_health",
                        lambda text: asked.append(text) or True)
    window._dismiss_ohlcv_health_warnings()
    assert "2,677 of 12,437" in asked[0]
    assert load_suspect_sessions() == {}
    assert window._update_label.text() == "OHLCV: 14 cached, current"
    assert window._update_label.styleSheet() == "color: #4caf50;"


def test_dismiss_refuses_while_an_update_runs(window, monkeypatch):
    from trade_scanner_fh.gui import main_window as mw_mod
    sessions, _ = _flag(BAD, GOOD)
    save_suspect_sessions(sessions)

    class _Running:
        def isRunning(self):
            return True
    window._update_worker = _Running()
    monkeypatch.setattr(mw_mod.QMessageBox, "information",
                        lambda *a, **k: None)
    monkeypatch.setattr(window, "_confirm_dismiss_health",
                        lambda text: pytest.fail("must not ask"))
    window._dismiss_ohlcv_health_warnings()
    window._update_worker = None
    assert load_suspect_sessions()
