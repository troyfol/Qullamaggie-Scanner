"""Tests for the parse-failure spike alarm (scraper resilience, B2).

A cluster of parse_error classifications means the source changed its
page layout — a parser break on OUR side, not N bad tickers. The fill
loops (shared fill_framework loop + the Zacks loop in earnings_history)
must HALT loudly once the parse fraction of the run spikes, and must
NEVER blacklist the affected tickers. All network is mocked.
"""
from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from trade_scanner_fh import config
from trade_scanner_fh import earnings_history as eh
from trade_scanner_fh import (
    fill_framework, finnhub_client, finnhub_fill, finviz_client, finviz_fill,
)


# ──────────────────────────────────────────────────────────────────────
# Config constants
# ──────────────────────────────────────────────────────────────────────

def test_spike_constants_documented_values():
    """Pin the documented defaults: 25-attempt arming sample, 40% trip."""
    assert config.PARSE_SPIKE_MIN_SAMPLE == 25
    assert config.PARSE_SPIKE_FAIL_PCT == 40.0


def test_parse_failure_kind_matches_all_sources():
    """Every source's parse sentinel shares the one value the framework
    loop keys on."""
    from trade_scanner_fh.zacks_scraper import FAIL_PARSE_ERROR
    assert fill_framework.PARSE_FAILURE_KIND == finviz_client.FAIL_PARSE
    assert fill_framework.PARSE_FAILURE_KIND == finnhub_client.FAIL_PARSE
    assert fill_framework.PARSE_FAILURE_KIND == FAIL_PARSE_ERROR


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_world(tmp_path, monkeypatch):
    """Hermetic parquet/raw/checkpoint tree (mirrors test_finviz_fill)."""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "earnings_history.parquet")
    monkeypatch.setattr(config, "EARNINGS_PARQUET",
                        tmp_path / "earnings_dates.parquet")
    monkeypatch.setattr(config, "FINVIZ_BULK_CHECKPOINT",
                        tmp_path / ".finviz_bulk_checkpoint.json")
    monkeypatch.setattr(config, "FINNHUB_BULK_CHECKPOINT",
                        tmp_path / ".finnhub_bulk_checkpoint.json")
    raw_root = tmp_path / "earnings_raw"
    monkeypatch.setattr(config, "RAW_EARNINGS_DIR", raw_root)
    raw_root.mkdir()
    for src in config.RAW_SOURCES:
        (raw_root / src).mkdir()
    finviz_client._set_failure(None)
    finnhub_client._set_failure(None)
    yield tmp_path


@pytest.fixture
def spike_thresholds(monkeypatch):
    """Lower the alarm thresholds so tests need only ~10 tickers, and
    raise the consecutive-block limits sky-high so the block/backoff
    machinery can't fire first (the alarm is what's under test here)."""
    monkeypatch.setattr(config, "PARSE_SPIKE_MIN_SAMPLE", 10)
    monkeypatch.setattr(config, "PARSE_SPIKE_FAIL_PCT", 40.0)
    monkeypatch.setattr(config, "FINVIZ_CONSEC_BLOCK_LIMIT", 999)
    monkeypatch.setattr(config, "FINNHUB_CONSEC_BLOCK_LIMIT", 999)


# ──────────────────────────────────────────────────────────────────────
# Shared fill_framework loop (exercised via the finviz fill)
# ──────────────────────────────────────────────────────────────────────

def test_finviz_loop_halts_on_parse_spike(tmp_world, spike_thresholds,
                                          monkeypatch, caplog):
    """Every fetch parse-fails → the run halts at the min-sample line
    with a loud log line, the affected tickers are NOT blacklisted, and
    the checkpoint survives for a post-fix resume."""
    fetched: list[str] = []
    empty_seen: list[str] = []
    failed: list[tuple[str, str]] = []

    def fake_fetch(sym, **kwargs):
        fetched.append(sym)
        return finviz_fill._FetchResult(failure=finviz_client.FAIL_PARSE)

    monkeypatch.setattr(finviz_fill, "_fetch_one_ticker", fake_fetch)

    with caplog.at_level(logging.ERROR, logger="scanner.finviz_fill"):
        filled, errors = finviz_fill.bulk_fill_finviz(
            [f"T{i}" for i in range(50)], blacklist=set(),
            on_empty_identified=empty_seen.append,
            failed_cb=lambda s, k: failed.append((s, k)),
            resume_from_checkpoint=False,
        )

    # Halted exactly at the (lowered) min-sample line, far short of 50.
    assert len(fetched) == 10
    assert (filled, errors) == (0, 10)
    # Loud halt line.
    assert "PARSE-FAILURE SPIKE" in caplog.text
    # A parser break must never poison the blacklist.
    assert empty_seen == []
    assert failed and all(k == finviz_client.FAIL_PARSE for _, k in failed)
    # The checkpoint is kept so the run can resume once the parser is fixed.
    assert finviz_fill._load_checkpoint() is not None


def test_finviz_loop_no_halt_below_threshold(tmp_world, spike_thresholds,
                                             monkeypatch):
    """3 parse failures out of 20 attempts (15%) stays under the 40% trip
    line — the loop processes the whole list and clears its checkpoint."""
    fetched: list[str] = []
    parse_idx = {4, 9, 14}

    def fake_fetch(sym, **kwargs):
        fetched.append(sym)
        if int(sym[1:]) in parse_idx:
            return finviz_fill._FetchResult(failure=finviz_client.FAIL_PARSE)
        return finviz_fill._FetchResult()   # success, no rows in window

    monkeypatch.setattr(finviz_fill, "_fetch_one_ticker", fake_fetch)
    filled, errors = finviz_fill.bulk_fill_finviz(
        [f"T{i}" for i in range(20)], blacklist=set(),
        resume_from_checkpoint=False,
    )
    assert len(fetched) == 20               # ran to completion
    assert (filled, errors) == (0, 3)
    assert finviz_fill._load_checkpoint() is None  # clean finish clears it


def test_finviz_alarm_armed_only_after_min_sample(tmp_world, spike_thresholds,
                                                  monkeypatch):
    """Fewer total attempts than PARSE_SPIKE_MIN_SAMPLE never trip the
    alarm, even at a 100% parse-failure rate."""
    fetched: list[str] = []

    def fake_fetch(sym, **kwargs):
        fetched.append(sym)
        return finviz_fill._FetchResult(failure=finviz_client.FAIL_PARSE)

    monkeypatch.setattr(finviz_fill, "_fetch_one_ticker", fake_fetch)
    filled, errors = finviz_fill.bulk_fill_finviz(
        [f"T{i}" for i in range(9)], blacklist=set(),   # 9 < min sample 10
        resume_from_checkpoint=False,
    )
    assert len(fetched) == 9                # all attempted, no early halt
    assert (filled, errors) == (0, 9)


def test_finnhub_loop_halts_on_parse_spike(tmp_world, spike_thresholds,
                                           monkeypatch):
    """The same alarm guards the finnhub side of the shared loop."""
    fetched: list[str] = []
    empty_seen: list[str] = []

    def fake_fetch(sym, **kwargs):
        fetched.append(sym)
        return finnhub_fill._FetchResult(failure=finnhub_client.FAIL_PARSE)

    monkeypatch.setattr(finnhub_fill, "_fetch_one_ticker", fake_fetch)
    filled, errors = finnhub_fill.bulk_fill_finnhub(
        [f"T{i}" for i in range(40)], blacklist=set(),
        on_etf_identified=empty_seen.append,
        resume_from_checkpoint=False,
    )
    assert len(fetched) == 10
    assert (filled, errors) == (0, 10)
    assert empty_seen == []                 # no blacklist poisoning


# ──────────────────────────────────────────────────────────────────────
# Zacks loop (_fill_via_zacks in earnings_history)
# ──────────────────────────────────────────────────────────────────────

class _KindSession:
    """ZacksSession stand-in: every fetch fails with the kind returned by
    `kind_for(symbol)`, recording the call order in `fetched`."""

    def __init__(self, kind_for):
        self._kind_for = kind_for
        self.fetched: list[str] = []
        self.last_failure_kind = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass

    def fetch(self, symbol, years=5):
        self.fetched.append(symbol)
        self.last_failure_kind = self._kind_for(symbol)
        return None


def test_zacks_loop_halts_on_parse_spike(tmp_parquets, spike_thresholds,
                                         caplog):
    """Every fetch is a parse_error → the Zacks loop halts at the
    min-sample line, classifying the failures parse_error — NOT the
    not_found bucket the GUI auto-adds to the Zacks skip list."""
    from trade_scanner_fh.zacks_scraper import FAIL_PARSE_ERROR
    fake = _KindSession(lambda s: FAIL_PARSE_ERROR)
    failed: list[tuple[str, str]] = []

    with patch.object(eh, "ZacksSession", return_value=fake), \
         patch.object(eh.time, "sleep", lambda *_: None), \
         caplog.at_level(logging.ERROR, logger=eh.log.name):
        filled, errors = eh.bulk_fill_zacks(
            [f"T{i}" for i in range(50)], blacklist=set(), delay_sec=0,
            failed_cb=lambda s, k: failed.append((s, k)),
        )

    assert len(fake.fetched) == 10          # halted at the min-sample line
    assert (filled, errors) == (0, 10)
    assert "PARSE-FAILURE SPIKE" in caplog.text
    # parse_error keeps these tickers out of the auto-blacklisted bucket.
    assert failed and all(k == FAIL_PARSE_ERROR for _, k in failed)


def test_zacks_not_found_does_not_trip_alarm(tmp_parquets, spike_thresholds):
    """Coverage gaps (not_found) never count toward the parse fraction —
    a long tail of small-caps Zacks doesn't cover can't halt the run."""
    from trade_scanner_fh.zacks_scraper import FAIL_NOT_FOUND
    fake = _KindSession(lambda s: FAIL_NOT_FOUND)

    with patch.object(eh, "ZacksSession", return_value=fake), \
         patch.object(eh.time, "sleep", lambda *_: None):
        filled, errors = eh.bulk_fill_zacks(
            [f"T{i}" for i in range(20)], blacklist=set(), delay_sec=0,
        )

    assert len(fake.fetched) == 20          # ran to completion
    assert (filled, errors) == (0, 20)


def test_zacks_parse_below_threshold_no_halt(tmp_parquets, spike_thresholds):
    """Parse failures under the 40% line don't halt the Zacks loop."""
    from trade_scanner_fh.zacks_scraper import FAIL_NOT_FOUND, FAIL_PARSE_ERROR
    parse_syms = {"T4", "T9", "T14"}        # 3 of 20 = 15%
    fake = _KindSession(
        lambda s: FAIL_PARSE_ERROR if s in parse_syms else FAIL_NOT_FOUND)

    with patch.object(eh, "ZacksSession", return_value=fake), \
         patch.object(eh.time, "sleep", lambda *_: None):
        filled, errors = eh.bulk_fill_zacks(
            [f"T{i}" for i in range(20)], blacklist=set(), delay_sec=0,
        )

    assert len(fake.fetched) == 20
    assert (filled, errors) == (0, 20)
