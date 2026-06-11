"""
Earnings date cache data layer.

Stores last/next earnings dates per ticker.
Storage: scanner_data/earnings_dates.parquet

Phase 3 of Finnhub augmentation: this module is now schema/IO only.
The bulk (Nasdaq) and targeted (yfinance) fill code has moved to
``nasdaq_fill.py`` and ``yahoo_fill.py`` respectively, and the legacy
"Finnhub-via-/calendar/earnings then fall back to yfinance" branch
has been deleted — Finnhub now has its own dedicated deep-history
module (``finnhub_fill.py``).

Schema (Phase 1 added the ``source`` column):

    ticker          string
    last_earnings   datetime64[ns]
    next_earnings   datetime64[ns]
    updated_at      datetime64[ns]
    source          string         "zacks_derived" | "finnhub_derived" |
                                   "nasdaq" | "yahoo" | "zacks+yahoo_aug"
                                   etc. — set by writer.

Source values stamped by the active fill paths:

  * nasdaq_fill.bulk_fill_nasdaq        → "nasdaq"
  * yahoo_fill.targeted_fill_yahoo      → "yahoo"
  * yahoo_fill.spot_fill_yahoo          → "yahoo"
  * earnings_reconcile.reconcile_earnings_dates → "zacks_derived" /
        "zacks+yahoo_aug" / source-passthrough
"""

import logging
import threading
import time

import pandas as pd

from . import config

log = logging.getLogger("scanner.earnings")

# Canonical column order for earnings_dates.parquet.
COLUMNS: list[str] = ["ticker", "last_earnings", "next_earnings",
                      "updated_at", "source"]

# Serializes the load→modify→save cycle on earnings_dates.parquet. Mirrors
# earnings_history.HISTORY_WRITE_LOCK: the launch-time smart refresh runs the
# finviz + zacks fill workers concurrently and both write this cache (finviz
# via _merge_and_save, the reconciler via save_earnings_cache), so without a
# shared lock the second writer's read-modify-write silently drops the first's
# rows. Re-entrant so a single thread re-entering (e.g. a flush nested in a
# reconcile) won't self-deadlock. Import this object — never make a second one.
DATES_WRITE_LOCK = threading.RLock()


# ---------------------------------------------------------------------------
# Load / Save
# ---------------------------------------------------------------------------

def load_earnings_cache() -> pd.DataFrame | None:
    """Read earnings_dates.parquet.  Returns None if file does not exist.

    For backward compat, rows missing the ``source`` column (legacy
    files written before Phase 1) are stamped ``"legacy"``.
    """
    path = config.EARNINGS_PARQUET
    if not path.exists():
        return None
    # Retry once on a read failure: the common cause on Windows is a transient
    # sharing-violation while another thread's os.replace swaps the file in. A
    # genuinely corrupt file still falls through to None after the retry.
    df = None
    last_exc = None
    for attempt in range(2):
        try:
            df = pd.read_parquet(path)
            break
        except Exception as exc:  # noqa: BLE001 - logged after the retry
            last_exc = exc
            if attempt == 0:
                time.sleep(0.2)
    if df is None:
        log.warning("Failed to read earnings_dates.parquet: %s", last_exc)
        return None

    # Ensure datetime columns are tz-naive. Guard the dtype first — a column
    # stored as object/all-null (torn write, legacy/empty file) has no `.dt`
    # accessor, and an unguarded `.dt.tz` would raise and (via the old broad
    # except) blank the whole cache. Mirrors load_earnings_history's guard.
    for col in ("last_earnings", "next_earnings", "updated_at"):
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                if df[col].dt.tz is not None:
                    df[col] = df[col].dt.tz_localize(None)
            except (AttributeError, TypeError):
                pass
    if "source" not in df.columns:
        df["source"] = "legacy"
    else:
        df["source"] = df["source"].fillna("legacy")
    return df


def save_earnings_cache(df: pd.DataFrame) -> None:
    """Write earnings_dates.parquet (atomic temp-file rename)."""
    config.atomic_write_parquet(
        df, config.EARNINGS_PARQUET, engine="pyarrow", index=False,
    )


def get_earnings_dates(
    ticker: str, earnings_df: pd.DataFrame
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    """Return (last_earnings, next_earnings) for a ticker, or (None, None)."""
    if earnings_df is None or earnings_df.empty:
        return None, None
    match = earnings_df.loc[earnings_df["ticker"] == ticker]
    if match.empty:
        return None, None
    row = match.iloc[0]
    last_e = row.get("last_earnings")
    next_e = row.get("next_earnings")
    last_e = None if pd.isna(last_e) else pd.Timestamp(last_e)
    next_e = None if pd.isna(next_e) else pd.Timestamp(next_e)
    return last_e, next_e


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _merge_and_save(new_rows: list[dict], existing: pd.DataFrame | None):
    """Merge new earnings rows with existing cache and save."""
    if not new_rows:
        return
    new_df = pd.DataFrame(new_rows)
    # Ensure datetime columns
    for col in ("last_earnings", "next_earnings", "updated_at"):
        if col in new_df.columns:
            new_df[col] = pd.to_datetime(new_df[col], errors="coerce")
    # Backfill source if any caller forgot — should not happen post-Phase 1,
    # but defends against tests/fixtures built before the column existed.
    if "source" not in new_df.columns:
        new_df["source"] = "unknown"

    with DATES_WRITE_LOCK:
        # Prefer the freshest on-disk snapshot (read inside the lock) over the
        # caller's possibly-stale `existing`, so a concurrent writer's rows are
        # not lost. Fall back to `existing` only when no file is present yet
        # (first write / in-memory test paths).
        if config.EARNINGS_PARQUET.exists():
            base = load_earnings_cache()
            if base is None:
                # File exists but is unreadable even after the retry — refuse
                # to overwrite it with only the new rows (that would truncate
                # the whole cache). Defer; the next flush retries.
                log.error(
                    "earnings_dates.parquet unreadable — deferring merge of "
                    "%d new rows to avoid truncating the cache", len(new_df),
                )
                return
        else:
            base = existing

        if base is not None and not base.empty:
            combined = pd.concat([base, new_df])
            combined = combined.drop_duplicates(subset=["ticker"], keep="last")
        else:
            combined = new_df
        save_earnings_cache(combined)


# Fill orchestration moved out of this module in Phase 3:
#   * Nasdaq calendar bulk → ``nasdaq_fill.bulk_fill_nasdaq``
#   * Yahoo (yfinance) targeted/spot → ``yahoo_fill.targeted_fill_yahoo``
#                                       / ``yahoo_fill.spot_fill_yahoo``
#   * Finnhub deep history → ``finnhub_fill.bulk_fill_finnhub`` /
#                            ``gap_fill_finnhub`` / ``spot_fill_finnhub``
# This module is now schema/IO only.
