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

import pandas as pd

from . import config

log = logging.getLogger("scanner.earnings")

# Canonical column order for earnings_dates.parquet.
COLUMNS: list[str] = ["ticker", "last_earnings", "next_earnings",
                      "updated_at", "source"]


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
    try:
        df = pd.read_parquet(path)
        # Ensure datetime columns are tz-naive
        for col in ("last_earnings", "next_earnings", "updated_at"):
            if col in df.columns and df[col].dt.tz is not None:
                df[col] = df[col].dt.tz_localize(None)
        if "source" not in df.columns:
            df["source"] = "legacy"
        else:
            df["source"] = df["source"].fillna("legacy")
        return df
    except Exception as exc:
        log.warning("Failed to read earnings_dates.parquet: %s", exc)
        return None


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

    if existing is not None and not existing.empty:
        combined = pd.concat([existing, new_df])
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
