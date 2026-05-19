"""yahoo_fill.py — earnings_dates.parquet fill via yfinance only.

Phase 3 source separation: extracted out of earnings_cache.py and the
Finnhub fallback dropped (Phase 2's dedicated finnhub_fill module is
now the official Finnhub path; Yahoo no longer chains into it).

Writes ONLY ``earnings_dates.parquet`` (last/next dates). Source stamp
on every consumer row is ``"yahoo"``.

Public API:
    targeted_fill_yahoo(gap_tickers, blacklist, ...) -> (filled, errors)
    spot_fill_yahoo(symbol, blacklist) -> (count, status)
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Optional

import pandas as pd

from . import earnings_cache as ec
from . import earnings_raw

log = logging.getLogger("scanner.yahoo_fill")


# ──────────────────────────────────────────────────────────────────────
# Per-ticker fetcher — yfinance only
# ──────────────────────────────────────────────────────────────────────

def _fetch_one(
    symbol: str, today_ts: pd.Timestamp, now: datetime,
) -> tuple[dict | None, dict | None]:
    """Pull yfinance .earnings_dates for one ticker. Returns
    ``(consumer_row, raw_row)`` — either may be None.

    Phase 3 contract: yfinance ONLY. The previous Finnhub-first chain
    has been removed; Finnhub now has its own dedicated fill module
    (finnhub_fill.py). A Yahoo fill on its own is what runs when the
    user explicitly chooses Yahoo from the menu.
    """
    import yfinance as yf
    try:
        ed = yf.Ticker(symbol).earnings_dates
    except Exception as exc:
        log.debug("yfinance .earnings_dates raised for %s: %s", symbol, exc)
        return None, None
    if ed is None or ed.empty:
        return None, None

    dates_ts = ed.index
    if dates_ts.tz is not None:
        dates_ts = dates_ts.tz_localize(None)

    past = sorted([d for d in dates_ts if d <= today_ts])
    future = sorted([d for d in dates_ts if d > today_ts])

    last_e = past[-1] if past else pd.NaT
    next_e = future[0] if future else pd.NaT

    row = {
        "ticker": symbol,
        "last_earnings": last_e,
        "next_earnings": next_e,
        "updated_at": now,
        "source": "yahoo",
    }
    # Raw capture: full date list semicolon-joined as ISO strings.
    all_iso = ";".join(
        pd.Timestamp(d).strftime("%Y-%m-%d") for d in sorted(dates_ts)
    )
    raw_row = {"ticker": symbol, "all_dates_returned": all_iso}
    return row, raw_row


# ──────────────────────────────────────────────────────────────────────
# Targeted (gap) fill
# ──────────────────────────────────────────────────────────────────────

def targeted_fill_yahoo(
    gap_tickers: list[str],
    blacklist: set[str],
    *,
    progress_cb=None,
    stop_flag: Optional[list[bool]] = None,
    delay: float = 0.5,
    flush_every: int = 50,
) -> tuple[int, int]:
    """Fill earnings dates for the provided ticker list via yfinance.
    One HTTP call per ticker.

    Persists progress incrementally every ``flush_every`` successful
    fills so a long run that gets interrupted does not lose its work.

    Returns ``(filled_count, error_count)``.
    """
    tickers = [t for t in gap_tickers if t and t not in blacklist]
    if not tickers:
        log.info("Yahoo targeted fill: no tickers to process")
        return 0, 0

    log.info("Yahoo targeted fill: %d tickers to process", len(tickers))

    now = datetime.now()
    today_ts = pd.Timestamp(now.date())
    pending: list[dict] = []
    raw_pending: list[dict] = []
    run_id = earnings_raw.new_run_id()
    filled = 0
    errors = 0
    total = len(tickers)

    def _flush_raw():
        if not raw_pending:
            return
        try:
            earnings_raw.append_yahoo_rows(raw_pending, run_id)
        except Exception as exc:
            log.warning("Yahoo raw-layer write failed: %s", exc)
        raw_pending.clear()

    for i, sym in enumerate(tickers):
        if stop_flag and stop_flag[0]:
            log.info("Yahoo targeted fill stopped at %d/%d", i, total)
            break

        row, raw_row = _fetch_one(sym, today_ts, now)
        if row is not None:
            pending.append(row)
            filled += 1
            if raw_row is not None:
                raw_pending.append(raw_row)
        else:
            errors += 1

        if progress_cb:
            progress_cb(i + 1, total)

        if len(pending) >= flush_every:
            ec._merge_and_save(pending, ec.load_earnings_cache())
            _flush_raw()
            log.info(
                "Yahoo targeted: flushed %d rows (%d/%d processed, "
                "%d filled, %d errors so far)",
                len(pending), i + 1, total, filled, errors,
            )
            pending = []

        if (i + 1) % 200 == 0:
            log.info(
                "Yahoo targeted: %d/%d processed (%d filled, %d errors)",
                i + 1, total, filled, errors,
            )

        time.sleep(delay)

    if pending:
        ec._merge_and_save(pending, ec.load_earnings_cache())
    _flush_raw()

    # Reconcile only the tickers we actually touched so the Yahoo rows
    # don't sit in dates_df waiting for a stray Cross-Check before they
    # get consolidated against existing Zacks/Finnhub history (if any).
    affected = [t for t in tickers]
    if affected:
        try:
            from . import earnings_reconcile  # lazy: cycle-safe
            earnings_reconcile.reconcile_earnings_dates(
                affected_tickers=affected
            )
        except Exception as exc:
            log.warning("Reconcile after Yahoo targeted fill failed: %s", exc)

    log.info("Yahoo targeted fill: %d filled, %d errors/no-data",
             filled, errors)
    return filled, errors


# ──────────────────────────────────────────────────────────────────────
# Spot (single-ticker) fill
# ──────────────────────────────────────────────────────────────────────

def spot_fill_yahoo(symbol: str, blacklist: set[str]) -> tuple[int, str]:
    """Fetch one ticker on demand. Returns ``(count, status)`` where
    status ∈ {"ok", "blacklisted", "no_data", "invalid"}. ``count`` is
    1 on success (one row written) else 0."""
    sym = (symbol or "").upper().strip()
    if not sym:
        return 0, "invalid"
    if sym in blacklist:
        return 0, "blacklisted"

    now = datetime.now()
    today_ts = pd.Timestamp(now.date())
    row, raw_row = _fetch_one(sym, today_ts, now)
    if row is None:
        return 0, "no_data"

    ec._merge_and_save([row], ec.load_earnings_cache())
    if raw_row is not None:
        try:
            earnings_raw.append_yahoo_rows([raw_row], earnings_raw.new_run_id())
        except Exception as exc:
            log.warning("Yahoo raw-layer write failed for %s: %s", sym, exc)

    # Reconcile the single ticker so the spot-written yahoo row gets
    # consolidated against any existing Zacks/Finnhub history.
    try:
        from . import earnings_reconcile  # lazy: cycle-safe
        earnings_reconcile.reconcile_earnings_dates(affected_tickers=[sym])
    except Exception as exc:
        log.warning("Reconcile after Yahoo spot fill failed: %s", exc)

    return 1, "ok"
