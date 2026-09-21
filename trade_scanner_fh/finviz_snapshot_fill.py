"""Paced universe sweep for the finviz snapshot attributes.

The second of the two producers feeding `finviz_snapshot`. The first is the
free scavenge riding on the earnings fill, which refreshes whatever earnings
touches; this one covers the rest of the universe on its own cadence.

**It is deliberately NOT chained into the market-open auto-update.** At the
default 4.0s pacing a ~9,750-ticker in-scope universe is ~10.8 hours. Blocking
the Nasdaq calendar step behind that would stall the morning chain every day,
so the sweep runs weekly-by-staleness or on demand, never as a prerequisite.

Skip-list policy, which differs from every other source here: a ticker is
auto-added ONLY when finviz answers with its definitive 404 + ``Ticker "X" not
found`` page, or when the OHLCV blacklist already excludes it. A 429, a 403 or
a challenge page means we were BLOCKED, which says nothing about coverage —
treating those as "uncovered" is how a skip list silently fills with perfectly
good tickers during a throttle.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Callable, Optional
from urllib.parse import quote

import pandas as pd
from curl_cffi import requests as creq

from . import config, finviz_client, finviz_snapshot

log = logging.getLogger("scanner.finviz_snapshot_fill")

# Outcome codes, mirroring finviz_client's vocabulary so the two read alike.
OK = "ok"
NOT_FOUND = "not_found"          # definitive: safe to skip-list
BLOCKED = "blocked"              # 429 / 403 / challenge — never skip-list
NETWORK = "network"
EMPTY = "empty_grid"             # 200 but no snapshot grid — layout suspect


class SweepStopped(Exception):
    """Raised internally when the caller's cancel callback goes True."""


def _pace(last_at: float) -> float:
    """Sleep out the remainder of this request's interval; return the new mark.

    Its own knob rather than the earnings limiter: the two jobs hit different
    pages with very different weights, and slowing the sweep must not also
    slow the earnings fill.
    """
    interval = config.FINVIZ_SNAPSHOT_MIN_INTERVAL_SEC
    jitter = config.FINVIZ_SNAPSHOT_JITTER_SEC
    if jitter:
        interval += random.uniform(-jitter, jitter)
    interval = max(0.5, interval)
    wait = last_at + interval - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    return time.monotonic()


def fetch_one(symbol: str, *, timeout: float = 25.0) -> tuple:
    """Fetch and parse one ticker's snapshot.

    Returns ``(status, row_or_None)``. The row is already stamped with symbol
    and fetched_at, ready for `finviz_snapshot.queue_row`.
    """
    sym = config.url_safe_ticker(symbol)
    if not sym:
        log.warning("finviz snapshot: refusing implausible symbol %r", symbol)
        return NETWORK, None

    url = config.FINVIZ_SNAPSHOT_URL.format(sym=quote(sym, safe=""))
    try:
        r = creq.get(url, impersonate="chrome",
                     headers=finviz_client._HEADERS, timeout=timeout)
    except Exception as exc:
        log.debug("finviz snapshot network error for %s: %s", sym, exc)
        return NETWORK, None

    body = r.text or ""
    if r.status_code == 404:
        # The ONLY response that may skip-list a ticker, and only because the
        # body confirms it. A bare 404 is treated as a network blip.
        if finviz_snapshot.is_ticker_not_found(r.status_code, body):
            return NOT_FOUND, None
        return NETWORK, None
    if r.status_code in (403, 429) or 500 <= r.status_code < 600:
        return BLOCKED, None
    if r.status_code != 200:
        return NETWORK, None
    if len(body.encode("utf-8", "ignore")) > config.FINVIZ_MAX_RESPONSE_BYTES:
        return NETWORK, None

    data = finviz_snapshot.parse_snapshot(body)
    if not data:
        # 200 with no grid. Could be a challenge page that returns 200, or a
        # finviz redesign. Either way it is NOT evidence of non-coverage, so
        # it never skip-lists.
        return EMPTY, None

    data[finviz_snapshot.SYMBOL_COL] = sym.upper()
    data[finviz_snapshot.FETCHED_COL] = pd.Timestamp.now(tz="UTC")
    return OK, data


def run_sweep(
    symbols: list,
    *,
    skip: Optional[set] = None,
    on_not_found: Optional[Callable] = None,
    progress: Optional[Callable] = None,
    should_stop: Optional[Callable] = None,
    max_consecutive_blocks: Optional[int] = None,
) -> dict:
    """Sweep `symbols`, writing snapshots and reporting a summary dict.

    `skip` is the combined skip set the caller assembles — the snapshot skip
    list UNION the OHLCV blacklist, per the rule that anything OHLCV gave up
    on is not worth a finviz request either.

    `on_not_found(sym)` is invoked only for the definitive 404, and is how the
    caller grows the skip list.

    Consecutive BLOCKED responses back off and eventually abort the run: once
    finviz is throttling, continuing to hammer it makes the block longer and
    risks the earnings fill too.
    """
    skip = {s.upper() for s in (skip or set())}
    if max_consecutive_blocks is None:
        max_consecutive_blocks = config.FINVIZ_CONSEC_BLOCK_LIMIT

    todo = [s.upper().strip() for s in symbols if s and str(s).strip()]
    todo = [s for s in todo if s not in skip]

    summary = {"requested": len(todo), "ok": 0, "not_found": 0,
               "blocked": 0, "network": 0, "empty_grid": 0,
               "written": 0, "stopped": False, "aborted": False}
    if not todo:
        return summary

    consecutive_blocks = 0
    block_events = 0
    pause = config.FINVIZ_INITIAL_BLOCK_PAUSE_SEC
    last_at = 0.0
    try:
        for i, sym in enumerate(todo, 1):
            if should_stop is not None and should_stop():
                summary["stopped"] = True
                break
            last_at = _pace(last_at)
            status, row = fetch_one(sym)

            if status == OK:
                consecutive_blocks = 0
                summary["ok"] += 1
                summary["written"] += finviz_snapshot.queue_row(row)
            elif status == NOT_FOUND:
                # Definitive non-coverage. Does NOT count toward the block
                # streak — a cluster of dead tickers must not look like a
                # throttle and trip the backoff.
                consecutive_blocks = 0
                summary["not_found"] += 1
                if on_not_found is not None:
                    try:
                        on_not_found(sym)
                    except Exception as exc:
                        log.debug("on_not_found(%s) raised: %s", sym, exc)
            elif status == BLOCKED:
                summary["blocked"] += 1
                consecutive_blocks += 1
                if consecutive_blocks >= max_consecutive_blocks:
                    block_events += 1
                    if block_events > config.FINVIZ_MAX_BLOCKS_PER_RUN:
                        # Finviz is throttling persistently. Continuing makes
                        # the block longer and puts the earnings fill — which
                        # shares the same origin — at risk. Abort and let the
                        # next scheduled sweep resume from staleness.
                        log.warning(
                            "finviz snapshot sweep: %d block events — "
                            "aborting this run.", block_events)
                        summary["aborted"] = True
                        raise SweepStopped
                    log.warning(
                        "finviz snapshot sweep: %d consecutive blocks — "
                        "pausing %ds (block event %d/%d)",
                        consecutive_blocks, pause, block_events,
                        config.FINVIZ_MAX_BLOCKS_PER_RUN)
                    # Flush before sleeping so a stop during the pause keeps
                    # everything fetched so far.
                    summary["written"] += finviz_snapshot.flush_queue()
                    slept = 0
                    while slept < pause:
                        if should_stop is not None and should_stop():
                            summary["stopped"] = True
                            raise SweepStopped
                        time.sleep(min(2, pause - slept))
                        slept += 2
                    consecutive_blocks = 0
                    pause = min(pause * 2,
                                config.FINVIZ_MAX_BLOCK_PAUSE_SEC)
            elif status == EMPTY:
                summary["empty_grid"] += 1
                consecutive_blocks = 0
            else:
                summary["network"] += 1

            if progress is not None and (i % 25 == 0 or i == len(todo)):
                try:
                    progress(i, len(todo), dict(summary))
                except Exception:
                    pass
    except SweepStopped:
        pass
    finally:
        # Always flush: a stopped or crashed sweep must keep the requests it
        # already spent.
        summary["written"] += finviz_snapshot.flush_queue()
    return summary
