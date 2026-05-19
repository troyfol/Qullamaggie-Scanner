"""nasdaq_fill.py — bulk earnings_dates.parquet fill via the
finance-calendars Nasdaq API. Phase 3 source separation: extracted
out of earnings_cache.py so each upstream owns its own fill module.

Writes ONLY ``earnings_dates.parquet`` (last/next dates). Never
writes per-quarter rows — that's the Zacks/Finnhub responsibility.
Source stamp on every consumer row is ``"nasdaq"``.

Pacing: one HTTP call per calendar day in the [today−days_back,
today+days_forward] window, weekdays only. Default 90/90 yields
~131 calls at 1s pacing → ~2 minutes.
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd

from . import config
from . import earnings_cache as ec
from . import earnings_raw

log = logging.getLogger("scanner.nasdaq_fill")


def bulk_fill_nasdaq(
    universe_symbols: list[str],
    blacklist: set[str],
    *,
    progress_cb=None,
    stop_flag: Optional[list[bool]] = None,
    days_back: int = 90,
    days_forward: int = 90,
    delay: float = 1.0,
) -> tuple[int, int]:
    """Populate earnings_dates.parquet by sweeping the Nasdaq earnings
    calendar one day at a time via the finance-calendars package.

    Cross-references results against ``universe_symbols`` so only known
    tickers are stored. Much faster than per-ticker yfinance calls.

    Returns ``(filled_count, error_count)``.
    """
    from finance_calendars.finance_calendars import get_earnings_by_date

    universe_set = set(universe_symbols) - blacklist
    today = date.today()
    start = today - timedelta(days=days_back)
    end = today + timedelta(days=days_forward)

    # Build list of weekdays to scrape
    dates: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # Mon–Fri
            dates.append(current)
        current += timedelta(days=1)

    total = len(dates)
    log.info("Nasdaq bulk fill: scraping %d calendar days", total)

    # Collect (ticker → [dates]) plus raw-layer audit rows.
    ticker_dates: dict[str, list[date]] = {}
    raw_rows: list[dict] = []
    errors = 0
    run_id = earnings_raw.new_run_id()

    for i, day in enumerate(dates):
        if stop_flag and stop_flag[0]:
            log.info("Nasdaq bulk fill stopped at day %d/%d", i, total)
            break

        try:
            df = get_earnings_by_date(day)
            if df is not None and not df.empty:
                # finance-calendars indexes by 'symbol'.
                for sym in df.index:
                    sym_upper = str(sym).upper()
                    if sym_upper in universe_set:
                        ticker_dates.setdefault(sym_upper, []).append(day)
                        raw_rows.append({
                            "ticker": sym_upper,
                            "calendar_date": pd.Timestamp(day),
                        })
        except Exception as exc:
            log.debug("Calendar fetch failed for %s: %s", day.isoformat(), exc)
            errors += 1

        if progress_cb:
            progress_cb(i + 1, total)

        time.sleep(delay)

    # Persist raw capture before collapsing — replay-friendly audit trail.
    if raw_rows:
        try:
            earnings_raw.append_nasdaq_rows(raw_rows, run_id)
        except Exception as exc:
            log.warning("Nasdaq raw-layer write failed: %s", exc)

    # Convert collected (ticker → [dates]) into last/next consumer rows.
    now = datetime.now()
    new_rows: list[dict] = []
    for ticker, er_dates in ticker_dates.items():
        er_sorted = sorted(er_dates)
        past = [d for d in er_sorted if d <= today]
        future = [d for d in er_sorted if d > today]
        last_e = pd.Timestamp(past[-1]) if past else pd.NaT
        next_e = pd.Timestamp(future[0]) if future else pd.NaT
        new_rows.append({
            "ticker": ticker,
            "last_earnings": last_e,
            "next_earnings": next_e,
            "updated_at": now,
            "source": "nasdaq",
        })

    existing = ec.load_earnings_cache()
    ec._merge_and_save(new_rows, existing)

    # Reconcile so the freshly-written nasdaq-source rows get folded
    # into the priority chain alongside any existing Zacks/Finnhub
    # history. Without this, later Yahoo (or Yahoo spot) fills could
    # overwrite the nasdaq rows by-ticker before any reconciler had a
    # chance to consolidate them — see audit-fix note in PR.
    if new_rows:
        try:
            from . import earnings_reconcile  # lazy: cycle-safe
            earnings_reconcile.reconcile_earnings_dates(
                affected_tickers=[r["ticker"] for r in new_rows]
            )
        except Exception as exc:
            log.warning("Reconcile after Nasdaq bulk failed: %s", exc)

    filled = len(new_rows)
    log.info("Nasdaq bulk fill: %d tickers updated, %d day-fetch errors",
             filled, errors)
    return filled, errors
