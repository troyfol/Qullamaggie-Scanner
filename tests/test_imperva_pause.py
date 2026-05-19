"""Tests for the Phase 6 Imperva auto-pause callback wiring inside
`_fill_via_zacks`. Validates the consecutive-error detection, the
retry-current-ticker semantics, and the stop path."""
from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from trade_scanner_fh import earnings_history as eh


@pytest.fixture
def tmp_parquets(tmp_path, monkeypatch):
    monkeypatch.setattr(eh.config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(eh.config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "earnings_history.parquet")
    monkeypatch.setattr(eh.config, "EARNINGS_PARQUET",
                        tmp_path / "earnings_dates.parquet")
    return tmp_path


def _good_row():
    """Single quarter dict matching what the scraper returns."""
    return {
        "period_ending": pd.Timestamp("2026-01-31"),
        "report_date": pd.Timestamp("2026-02-15"),
        "report_time": "Close",
        "estimated_eps": 2.0, "reported_eps": 2.1,
        "surprise_eps": 0.1, "surprise_eps_pct": 5.0,
        "estimated_rev": 100.0, "reported_rev": 105.0,
        "surprise_rev": 5.0, "surprise_rev_pct": 5.0,
    }


@contextmanager
def _patched_session(fetch_seq, *, all_blocked: bool = True):
    """Patch ZacksSession so .fetch() returns successive elements of
    `fetch_seq`. Each element is either a list[dict] (success) or None
    (failure). Sleep is also patched so tests don't actually wait.

    Audit M1: the auto-pause counter only advances on `last_failure_kind
    == FAIL_BLOCKED`. By default these tests treat all None returns as
    Imperva blocks (`all_blocked=True`) — that's what the original tests
    assumed. Pass `all_blocked=False` to simulate "ticker not on Zacks"
    failures, which should NOT trigger the auto-pause."""
    from trade_scanner_fh.zacks_scraper import (
        FAIL_BLOCKED, FAIL_NOT_FOUND,
    )
    session = MagicMock()

    seq_iter = iter(fetch_seq)

    def _fetch(*args, **kwargs):
        result = next(seq_iter)
        if result is None or (hasattr(result, "__len__") and len(result) == 0):
            session.last_failure_kind = (
                FAIL_BLOCKED if all_blocked else FAIL_NOT_FOUND
            )
        else:
            session.last_failure_kind = None
        return result

    session.fetch.side_effect = _fetch

    @contextmanager
    def fake_ctx(*a, **kw):
        yield session

    with patch.object(eh, "ZacksSession", fake_ctx), \
         patch.object(eh.time, "sleep"):
        yield session


# ──────────────────────────────────────────────────────────────────────
# Callback fires after N consecutive failures
# ──────────────────────────────────────────────────────────────────────

def test_callback_fires_after_consecutive_errors(tmp_parquets):
    """5 consecutive Nones → callback invoked once."""
    fetch_seq = [None, None, None, None, None]
    callback = MagicMock(return_value="stop")

    with _patched_session(fetch_seq):
        filled, errors = eh._fill_via_zacks(
            ["A", "B", "C", "D", "E"], blacklist=set(),
            consec_error_limit=5,
            on_block_callback=callback,
            delay_sec=0,
        )

    callback.assert_called_once()
    args, _ = callback.call_args
    assert args[0] == 5  # consec count


def test_callback_does_not_fire_below_limit(tmp_parquets):
    """4 consecutive errors with limit=5 → callback not called."""
    fetch_seq = [None, None, None, None]
    callback = MagicMock(return_value="continue")

    with _patched_session(fetch_seq):
        eh._fill_via_zacks(
            ["A", "B", "C", "D"], blacklist=set(),
            consec_error_limit=5,
            on_block_callback=callback,
            delay_sec=0,
        )

    callback.assert_not_called()


def test_success_resets_consecutive_counter(tmp_parquets):
    """fail, fail, success, fail, fail → no callback (success reset the run)."""
    fetch_seq = [None, None, [_good_row()], None, None]
    callback = MagicMock(return_value="stop")

    with _patched_session(fetch_seq):
        eh._fill_via_zacks(
            ["A", "B", "C", "D", "E"], blacklist=set(),
            consec_error_limit=5,
            on_block_callback=callback,
            delay_sec=0,
        )

    callback.assert_not_called()


# ──────────────────────────────────────────────────────────────────────
# Stop semantics
# ──────────────────────────────────────────────────────────────────────

def test_callback_stop_breaks_loop(tmp_parquets):
    """Callback returning 'stop' must end the loop immediately — no
    further fetch() calls after the block."""
    # 5 fails → block → user stops; tickers F/G should never be fetched.
    fetch_seq = [None] * 5
    callback = MagicMock(return_value="stop")

    with _patched_session(fetch_seq) as session:
        filled, errors = eh._fill_via_zacks(
            ["A", "B", "C", "D", "E", "F", "G"], blacklist=set(),
            consec_error_limit=5,
            on_block_callback=callback,
            delay_sec=0,
        )

    assert filled == 0
    # 5 fetch calls before stop — not 7
    assert session.fetch.call_count == 5


# ──────────────────────────────────────────────────────────────────────
# Continue semantics — retries the current ticker
# ──────────────────────────────────────────────────────────────────────

def test_callback_continue_rewinds_to_block_window_start(tmp_parquets):
    """After 5 fails, 'continue' rewinds the cursor to the FIRST ticker
    in the failure window — every ticker that failed during the block
    gets retried, not just the most recent one."""
    # Sequence: A,B,C,D,E all fail → block → callback returns "continue"
    # → rewind to i=0 → retry A,B,C,D,E all succeed.
    fetch_seq = [None, None, None, None, None,
                 [_good_row()], [_good_row()], [_good_row()],
                 [_good_row()], [_good_row()]]
    callback = MagicMock(return_value="continue")

    with _patched_session(fetch_seq) as session:
        filled, errors = eh._fill_via_zacks(
            ["A", "B", "C", "D", "E"], blacklist=set(),
            consec_error_limit=5,
            on_block_callback=callback,
            delay_sec=0,
        )

    callback.assert_called_once()
    # 10 total fetches: 5 fails + 5 retries
    assert session.fetch.call_count == 10
    # First retry hits "A" — the start of the failure window.
    sixth_call_sym = session.fetch.call_args_list[5].args[0]
    assert sixth_call_sym == "A"
    # Last call is "E" because the rewind walked all the way back through.
    assert session.fetch.call_args_list[-1].args[0] == "E"
    assert filled == 5


def test_callback_continue_rolls_back_entire_window(tmp_parquets):
    """Errors counter rolls back the WHOLE consecutive-failure window
    when the user resumes — so a successful replay doesn't show the
    block-time errors as real misses."""
    # 5 fails (A–E) → block → continue → 5 retries all succeed.
    # Final state: filled=5, errors=0 (the 5 block-time errors were
    # rolled back, none of the retries failed).
    fetch_seq = [None, None, None, None, None,
                 [_good_row()], [_good_row()], [_good_row()],
                 [_good_row()], [_good_row()]]
    callback = MagicMock(return_value="continue")

    with _patched_session(fetch_seq):
        filled, errors = eh._fill_via_zacks(
            ["A", "B", "C", "D", "E"], blacklist=set(),
            consec_error_limit=5,
            on_block_callback=callback,
            delay_sec=0,
        )

    assert filled == 5
    assert errors == 0


def test_callback_continue_partial_window_rewind(tmp_parquets):
    """When a successful ticker precedes a block, only the post-success
    failure run gets rewound — the success itself is not retried."""
    # Sequence: A succeeds, B–F all fail (5 in a row → block).
    # 'continue' → rewind to B (first of the window), NOT to A.
    # Retries: B,C,D,E,F all succeed.
    fetch_seq = [
        [_good_row()],          # A: success
        None, None, None, None, None,  # B–F: 5 fails
        [_good_row()], [_good_row()], [_good_row()],
        [_good_row()], [_good_row()],  # B–F retried, all succeed
    ]
    callback = MagicMock(return_value="continue")

    with _patched_session(fetch_seq) as session:
        filled, errors = eh._fill_via_zacks(
            ["A", "B", "C", "D", "E", "F"], blacklist=set(),
            consec_error_limit=5,
            on_block_callback=callback,
            delay_sec=0,
        )

    assert filled == 6
    assert errors == 0
    # 11 fetches: A (1) + B–F fail (5) + B–F retry (5).
    assert session.fetch.call_count == 11
    # The 7th fetch is the retry of B (rewind landed on B, not A).
    seventh_call_sym = session.fetch.call_args_list[6].args[0]
    assert seventh_call_sym == "B"


# ──────────────────────────────────────────────────────────────────────
# No callback → original behavior preserved
# ──────────────────────────────────────────────────────────────────────

def test_no_callback_runs_to_completion(tmp_parquets):
    """When on_block_callback is None, all-fail runs still complete
    normally without any pause."""
    fetch_seq = [None] * 7

    with _patched_session(fetch_seq) as session:
        filled, errors = eh._fill_via_zacks(
            ["A", "B", "C", "D", "E", "F", "G"], blacklist=set(),
            consec_error_limit=5,
            on_block_callback=None,
            delay_sec=0,
        )

    assert filled == 0
    assert errors == 7
    assert session.fetch.call_count == 7
