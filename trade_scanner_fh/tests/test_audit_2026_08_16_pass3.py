"""Audit 2026-08-16 — Pass 3 regression tests: F3, F4, F7.

F3 is three independent failures that compose into one universe-wide
truncation. F4 restores a priority chain that was being decided by fill order.
F7 closes the gap between what the atomic writers do and what they claim.
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest

from trade_scanner_fh import config
from trade_scanner_fh import earnings_cache as ec
from trade_scanner_fh import earnings_reconcile as er
from trade_scanner_fh import ticker_universe as tu


# ══════════════════════════════════════════════════════════════════════
# F3 — universe truncation
# ══════════════════════════════════════════════════════════════════════

def test_empty_batch_response_raises_instead_of_failing_everything(
        monkeypatch):
    """What rate limiting actually looks like: a 200-shaped empty frame, not
    an exception. The retry + per-ticker fallback were keyed on
    `except Exception`, so for the MOST LIKELY failure mode they never ran and
    every symbol in the batch was marked failed — then deleted."""
    monkeypatch.setattr(tu.yf, "download", lambda *a, **k: pd.DataFrame())

    with pytest.raises(tu.EmptyValidationBatch):
        tu._run_validation_batch(["AAPL", "MSFT", "TSLA"])


def test_empty_batch_falls_through_to_the_per_ticker_probe(monkeypatch):
    """And the raise must land in the existing fallback, not escape."""
    monkeypatch.setattr(tu.yf, "download", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr(tu.time, "sleep", lambda *_: None)
    probed = []

    def _probe(sym):
        probed.append(sym)
        return sym != "DEAD"

    monkeypatch.setattr(tu, "_probe_single_ticker", _probe)

    valid, failed = tu._validate_via_yfinance(["AAPL", "DEAD"])

    assert probed == ["AAPL", "DEAD"], "the per-ticker fallback never ran"
    assert valid == {"AAPL"} and failed == {"DEAD"}


def test_unreadable_universe_aborts_rather_than_revalidating_everything(
        tmp_path, monkeypatch):
    """A locked/partial/renamed-column read used to `except Exception: pass`,
    silently flipping the refresh into full-revalidation mode."""
    csv = tmp_path / "universe.csv"
    csv.write_text("this is not a csv\x00\x01", encoding="utf-8")
    monkeypatch.setattr(pd, "read_csv", _boom)

    with pytest.raises(RuntimeError, match="could not be read"):
        tu._load_previous_universe(csv)


def _boom(*a, **k):
    raise OSError(32, "The process cannot access the file")


def test_missing_universe_is_a_legitimate_first_run(tmp_path):
    assert tu._load_previous_universe(tmp_path / "nope.csv") == set()


def _seed_universe(csv: Path, n: int) -> None:
    pd.DataFrame({"symbol": [f"T{i:04d}" for i in range(n)]}).to_csv(
        csv, index=False)


def _stub_sources(monkeypatch, n_ftp: int):
    """Only the FTP source returns anything; GitHub and SEC are down."""
    monkeypatch.setattr(tu, "_fetch_nasdaq_ftp", lambda: pd.DataFrame({
        "symbol_raw": [f"T{i:04d}" for i in range(n_ftp)],
        "name": [""] * n_ftp, "exchange": [""] * n_ftp,
        "market_category": [""] * n_ftp, "etf": [False] * n_ftp,
        "source": ["ftp"] * n_ftp,
    }))
    monkeypatch.setattr(tu, "_fetch_github_tickers", lambda: set())
    monkeypatch.setattr(tu, "_fetch_sec_edgar", lambda: set())
    monkeypatch.setattr(tu, "_log_failed_tickers", lambda failed: None)


def test_universe_shrink_floor_blocks_a_truncated_write(tmp_path, monkeypatch):
    """universe.csv feeds every scan and every fill's work list. The codebase
    already caps an absurd third-party dataset (FINANCEDATABASE_MAX_ROWS); the
    universe, which matters more, had no floor at all.

    Scenario: the symbol sources degrade — NASDAQ returns a partial file and
    GitHub/SEC are unreachable. No validation failure is even needed; the
    shrunken merge would simply be written over a good universe.
    """
    csv = tmp_path / "universe.csv"
    monkeypatch.setattr(config, "TICKER_CSV", csv)
    _seed_universe(csv, 1000)
    _stub_sources(monkeypatch, n_ftp=100)

    with pytest.raises(RuntimeError, match="Refusing to write"):
        tu.refresh_universe(force=True, skip_validation=True)

    # The previous universe must be exactly as it was.
    assert len(pd.read_csv(csv)) == 1000


def test_normal_churn_is_allowed_through(tmp_path, monkeypatch):
    """Delistings and new listings move the count a few percent — the floor
    must not fire on a healthy refresh."""
    csv = tmp_path / "universe.csv"
    monkeypatch.setattr(config, "TICKER_CSV", csv)
    _seed_universe(csv, 1000)
    _stub_sources(monkeypatch, n_ftp=960)

    out = tu.refresh_universe(force=True, skip_validation=True)

    assert len(out) == 960
    assert len(pd.read_csv(csv)) == 960


def test_first_ever_run_has_no_baseline_to_compare(tmp_path, monkeypatch):
    """No previous file means nothing to protect — the floor must not block
    a legitimate first build."""
    csv = tmp_path / "universe.csv"
    monkeypatch.setattr(config, "TICKER_CSV", csv)
    _stub_sources(monkeypatch, n_ftp=50)

    out = tu.refresh_universe(force=True, skip_validation=True)

    assert len(out) == 50


# ══════════════════════════════════════════════════════════════════════
# F4 — per-source date store
# ══════════════════════════════════════════════════════════════════════

def _rows(ticker, last=None, nxt=None):
    return [{
        "ticker": ticker,
        "last_earnings": pd.Timestamp(last) if last else pd.NaT,
        "next_earnings": pd.Timestamp(nxt) if nxt else pd.NaT,
        "updated_at": pd.Timestamp("2026-08-16"),
    }]


def test_sources_no_longer_overwrite_each_other(tmp_parquets):
    """The failure: one row per ticker meant nasdaq and yahoo shared a record,
    so whichever fill ran LAST won and the reconciler could only read that
    collapsed row back."""
    ec.record_source_dates(_rows("AAPL", last="2026-08-04"), "nasdaq")
    ec.record_source_dates(_rows("AAPL", last="2026-08-05"), "yahoo")

    assert ec.source_dates_lookup("nasdaq")["AAPL"][0] == pd.Timestamp("2026-08-04")
    assert ec.source_dates_lookup("yahoo")["AAPL"][0] == pd.Timestamp("2026-08-05")


def test_priority_chain_now_actually_applies(tmp_parquets):
    """nasdaq outranks yahoo. Before F4 this was decided by fill order, so
    running yahoo second made yahoo win."""
    ec.record_source_dates(_rows("AAPL", last="2026-08-04"), "nasdaq")
    ec.record_source_dates(_rows("AAPL", last="2026-08-05"), "yahoo")  # later

    er.reconcile_earnings_dates(
        ["AAPL"], today=pd.Timestamp("2026-08-16"), history_df=None)

    row = ec.load_earnings_cache().iloc[0]
    assert row["last_earnings"] == pd.Timestamp("2026-08-04"), (
        "yahoo's later write beat nasdaq's higher priority"
    )
    assert row["last_source"] == "nasdaq"


def test_a_null_date_does_not_erase_a_known_one(tmp_parquets):
    """finviz writes a forward date with last=NaT; NaT means 'this fill didn't
    learn it', never 'it was cleared'."""
    ec.record_source_dates(
        _rows("AAPL", last="2026-08-04", nxt="2026-11-01"), "nasdaq")
    ec.record_source_dates(_rows("AAPL", nxt="2026-11-02"), "nasdaq")

    last, nxt = ec.source_dates_lookup("nasdaq")["AAPL"]
    assert last == pd.Timestamp("2026-08-04")
    assert nxt == pd.Timestamp("2026-11-02")


def test_per_source_store_round_trips(tmp_parquets):
    ec.record_source_dates(_rows("AAPL", last="2026-08-04"), "nasdaq")
    df = ec.load_source_dates()
    assert list(df.columns)[:5] == ec.SOURCE_COLUMNS
    assert len(df) == 1


def test_absent_store_falls_back_to_the_legacy_path(tmp_parquets):
    """A pre-F4 data directory must keep working unchanged."""
    assert ec.load_source_dates() is None
    ec.save_earnings_cache(pd.DataFrame([{
        "ticker": "AAPL", "last_earnings": pd.Timestamp("2026-08-04"),
        "next_earnings": pd.NaT, "updated_at": pd.Timestamp("2026-08-01"),
        "source": "nasdaq", "last_source": pd.NA, "next_source": pd.NA,
    }]))

    er.reconcile_earnings_dates(
        ["AAPL"], today=pd.Timestamp("2026-08-16"), history_df=None)

    row = ec.load_earnings_cache().iloc[0]
    assert row["last_earnings"] == pd.Timestamp("2026-08-04")


def test_fallback_is_per_source_during_a_transition(tmp_parquets):
    """Mid-transition the new store knows about yahoo but not nasdaq. Gating
    on the store as a WHOLE would discard nasdaq's legacy dates until its next
    sweep."""
    ec.save_earnings_cache(pd.DataFrame([{
        "ticker": "AAPL", "last_earnings": pd.Timestamp("2026-08-04"),
        "next_earnings": pd.NaT, "updated_at": pd.Timestamp("2026-08-01"),
        "source": "nasdaq", "last_source": "nasdaq", "next_source": pd.NA,
    }]))
    ec.record_source_dates(_rows("ZZZZ", last="2026-01-01"), "yahoo")

    er.reconcile_earnings_dates(
        ["AAPL"], today=pd.Timestamp("2026-08-16"), history_df=None)

    row = ec.load_earnings_cache()
    aapl = row.loc[row.ticker == "AAPL"].iloc[0]
    assert aapl["last_earnings"] == pd.Timestamp("2026-08-04"), (
        "nasdaq's legacy date was dropped because yahoo populated the store"
    )


# ══════════════════════════════════════════════════════════════════════
# S1 (partial) — the zacks fill finally has a resume checkpoint
# ══════════════════════════════════════════════════════════════════════

def _zacks_row(period="2025-12-01", report="2026-01-29"):
    return {
        "period_ending": pd.Timestamp(period),
        "report_date": pd.Timestamp(report),
        "report_time": "Close",
        "estimated_eps": 1.0, "reported_eps": 1.1,
        "surprise_eps": 0.1, "surprise_eps_pct": 10.0,
        "estimated_rev": None, "reported_rev": None,
        "surprise_rev": None, "surprise_rev_pct": None,
    }


class _Session:
    """Records which symbols were actually fetched."""

    def __init__(self):
        self.seen: list[str] = []
        self.last_failure_kind = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def fetch(self, symbol, years=5):
        self.seen.append(symbol)
        return [_zacks_row()]


def _run_zacks(monkeypatch, tickers, **kw):
    from trade_scanner_fh import earnings_history as eh
    sess = _Session()
    monkeypatch.setattr(eh, "ZacksSession", lambda *a, **k: sess)
    monkeypatch.setattr(eh.time, "sleep", lambda *_: None)
    eh.bulk_fill_zacks(tickers, blacklist=set(), delay_sec=0, **kw)
    return sess


def test_zacks_bulk_resumes_instead_of_restarting(tmp_parquets, monkeypatch):
    """S1's concrete gap: zacks was the ONE fill with no checkpoint, so a
    killed bulk restarted from zero — ~6.5 hours at the default pacing."""
    first = _run_zacks(monkeypatch, ["A", "B", "C"],
                       resume_from_checkpoint=False)
    assert first.seen == ["A", "B", "C"]

    # Simulate a kill mid-run: re-plant the checkpoint the run would have had.
    from trade_scanner_fh import fill_framework as ff
    ff.save_checkpoint(
        config.ZACKS_BULK_CHECKPOINT,
        ff.Checkpoint(run_id="r1",
                      started_at=pd.Timestamp.now().isoformat(
                          timespec="seconds"),
                      completed=["A", "B"]),
        __import__("logging").getLogger("t"))

    second = _run_zacks(monkeypatch, ["A", "B", "C"])

    assert second.seen == ["C"], "already-completed tickers were re-fetched"


def test_zacks_checkpoint_cleared_after_a_clean_run(tmp_parquets, monkeypatch):
    """A finished run must not leave a checkpoint that makes the NEXT bulk a
    silent no-op."""
    _run_zacks(monkeypatch, ["A", "B"])
    assert not config.ZACKS_BULK_CHECKPOINT.exists()


def test_zacks_checkpoint_survives_a_stop(tmp_parquets, monkeypatch):
    """A stopped run is exactly the one worth resuming."""
    from trade_scanner_fh import earnings_history as eh
    sess = _Session()
    monkeypatch.setattr(eh, "ZacksSession", lambda *a, **k: sess)
    monkeypatch.setattr(eh.time, "sleep", lambda *_: None)
    stop = [False]

    def _cb(done, total):
        if done >= 1:
            stop[0] = True

    eh.bulk_fill_zacks(["A", "B", "C"], blacklist=set(), delay_sec=0,
                       progress_cb=_cb, stop_flag=stop)

    assert config.ZACKS_BULK_CHECKPOINT.exists()


def test_a_stale_zacks_checkpoint_is_ignored(tmp_parquets, monkeypatch):
    """Reuses the generic staleness rejection — an abandoned run must not
    make the next bulk skip most of the universe."""
    from trade_scanner_fh import fill_framework as ff
    import logging
    old = (pd.Timestamp.now() - pd.Timedelta(days=5)).isoformat(
        timespec="seconds")
    ff.save_checkpoint(
        config.ZACKS_BULK_CHECKPOINT,
        ff.Checkpoint(run_id="old", started_at=old, completed=["A", "B"]),
        logging.getLogger("t"))

    sess = _run_zacks(monkeypatch, ["A", "B", "C"])

    assert sess.seen == ["A", "B", "C"], "a stale checkpoint was trusted"


# ══════════════════════════════════════════════════════════════════════
# Revenue-only quarters are kept (2026-08-16 policy decision)
# ══════════════════════════════════════════════════════════════════════

def test_finnhub_keeps_a_revenue_only_quarter():
    from trade_scanner_fh import finnhub_fill
    row = finnhub_fill._record_to_history_dict(
        {"period": "2025-12-31", "year": 2025, "quarter": 4,
         "actual": None, "estimate": None,
         "revenueActual": 1000.0, "revenueEstimate": 950.0},
        queried_symbol="AAPL", calendar_lookup={},
        cutoff=pd.Timestamp("2020-01-01"),
        now=pd.Timestamp("2026-08-16").to_pydatetime())
    assert row is not None
    assert row["reported_rev"] == 1000.0
    assert row["reported_eps"] is None


def test_finnhub_still_rejects_a_scheduled_unreported_quarter():
    """INT-7's actual target: a placeholder with NO figures at all. It claimed
    a slot and then suppressed the gap fill that would have replaced it."""
    from trade_scanner_fh import finnhub_fill
    assert finnhub_fill._record_to_history_dict(
        {"period": "2026-12-31", "year": 2026, "quarter": 4,
         "actual": None, "estimate": 1.0,
         "revenueActual": None, "revenueEstimate": 950.0},
        queried_symbol="AAPL", calendar_lookup={},
        cutoff=pd.Timestamp("2020-01-01"),
        now=pd.Timestamp("2026-08-16").to_pydatetime()) is None


def test_revenue_only_rows_do_not_suppress_their_own_repair(tmp_parquets):
    """The property that makes keeping them safe. `find_smart_refresh_candidates`
    keys coverage on reported_eps, so a revenue-only row is NOT counted as a
    captured quarter and the ticker still queues for a fill."""
    from trade_scanner_fh import earnings_history as eh
    rev_only = {
        "ticker": "AAPL", "period_ending": pd.Timestamp("2025-12-01"),
        "report_date": pd.Timestamp("2026-01-29"), "report_time": "Close",
        "estimated_eps": None, "reported_eps": None,
        "surprise_eps": None, "surprise_eps_pct": None,
        "estimated_rev": None, "reported_rev": 1000.0,
        "surprise_rev": None, "surprise_rev_pct": None,
        "source": "zacks", "updated_at": pd.Timestamp("2026-08-16"),
        "report_date_proxy": False,
    }
    eh.save_earnings_history(pd.DataFrame([rev_only]), sort=True)

    candidates = eh.find_smart_refresh_candidates(
        ["AAPL"], blacklist=set(),
        today=pd.Timestamp("2026-08-16"),
        history_df=eh.load_earnings_history(),
        dates_df=None,
    )
    assert "AAPL" in candidates, (
        "a revenue-only row was treated as coverage and blocked its own fill"
    )


def test_integrity_check_14_agrees_with_ingest(tmp_parquets):
    """The contradiction this settles: ingest dropped revenue-only rows while
    verify_integrity went out of its way to preserve them. A row with revenue
    is not a placeholder on either side now."""
    from trade_scanner_fh import earnings_history as eh
    df = pd.DataFrame([{
        "ticker": "AAPL", "period_ending": pd.Timestamp("2025-12-01"),
        "report_date": pd.Timestamp("2026-01-29"), "report_time": "Close",
        "estimated_eps": None, "reported_eps": None,
        "surprise_eps": None, "surprise_eps_pct": None,
        "estimated_rev": None, "reported_rev": 1000.0,
        "surprise_rev": None, "surprise_rev_pct": None,
        "source": "zacks", "updated_at": pd.Timestamp("2026-08-16"),
        "report_date_proxy": False,
    }])
    checks = {f.check for f in eh.verify_integrity(history_df=df)}
    assert "placeholder_no_actual" not in checks


# ══════════════════════════════════════════════════════════════════════
# F7 — atomic writes are fsync'd
# ══════════════════════════════════════════════════════════════════════

def test_all_three_writers_fsync_before_the_rename(tmp_path, monkeypatch):
    """The docstrings claimed a crash mid-write cannot corrupt the target.
    True for a PROCESS crash; not for power loss, where NTFS can make the
    rename durable before the data pages and leave an EMPTY file under the
    real filename."""
    synced: list[int] = []
    real_fsync = os.fsync
    monkeypatch.setattr(
        os, "fsync", lambda fd: (synced.append(fd), real_fsync(fd))[1])

    config.atomic_write_text(tmp_path / "a.txt", "hello")
    config.atomic_write_parquet(
        pd.DataFrame({"x": [1]}), tmp_path / "b.parquet", engine="pyarrow",
        index=False)
    config.atomic_write_csv(
        pd.DataFrame({"x": [1]}), tmp_path / "c.csv", index=False)

    assert len(synced) == 3, "one of the writers still isn't fsync'ing"


def test_writers_still_produce_correct_content(tmp_path):
    config.atomic_write_text(tmp_path / "a.txt", "hello\nworld\n")
    assert (tmp_path / "a.txt").read_text(encoding="utf-8") == "hello\nworld\n"

    df = pd.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    config.atomic_write_parquet(df, tmp_path / "b.parquet",
                                engine="pyarrow", index=False)
    pd.testing.assert_frame_equal(pd.read_parquet(tmp_path / "b.parquet"), df)

    config.atomic_write_csv(df, tmp_path / "c.csv", index=False)
    pd.testing.assert_frame_equal(pd.read_csv(tmp_path / "c.csv"), df)


def test_csv_has_no_blank_lines_between_rows(tmp_path):
    """Writing through a text handle without newline='' would re-translate
    pandas' "\\n" to "\\r\\n" on Windows and double every line ending."""
    config.atomic_write_csv(
        pd.DataFrame({"x": [1, 2, 3]}), tmp_path / "c.csv", index=False)
    raw = (tmp_path / "c.csv").read_bytes()
    assert b"\r\r\n" not in raw and b"\n\n" not in raw
    assert len(pd.read_csv(tmp_path / "c.csv")) == 3


def test_a_failed_write_leaves_no_residue(tmp_path):
    class _Bad:
        def to_parquet(self, *a, **k):
            raise ValueError("nope")

    with pytest.raises(ValueError):
        config.atomic_write_parquet(_Bad(), tmp_path / "x.parquet")

    assert not list(tmp_path.glob(".*")), "temp residue left behind"
    assert not (tmp_path / "x.parquet").exists()
