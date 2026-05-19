"""earnings_reconcile.py — multi-source unifier for earnings_dates.parquet.

Phase 4 of the Finnhub augmentation: rebuilt around an explicit per-
position priority chain so all four upstream sources (Zacks/Finnhub
history + Nasdaq/Yahoo dates) feed cleanly into the canonical
last/next pair.

Per-position priority (first non-null wins):

    last_earnings:  zacks history > nasdaq dates > yahoo dates
                    > finnhub history
    next_earnings:  zacks history (real announcement) > nasdaq dates
                    > yahoo dates > finnhub history (real announcement,
                    NOT period_ending proxy)

Finnhub is demoted to last on every position. The semantic gap between
Zacks adjusted EPS and Finnhub GAAP EPS makes Finnhub data unreliable
when ANY other source covers the ticker — better to use a date from a
date-only source than a date from a row whose accompanying numeric
fields would silently disagree with the prevailing consensus.

`> today` filter is applied consistently to next_earnings so a stale
date from any source is cleared (Phase 1 stale-Yahoo bug fix
generalized to all sources).

Source label on the consolidated row reflects WHICH source produced
which position:

    same source for last+next  →  "{src}_derived"  (e.g. "zacks_derived")
    different sources          →  "{last_src}+{next_src}_aug"
                                  (e.g. "zacks+yahoo_aug",
                                   "finnhub+nasdaq_aug")
    only last                  →  "{src}_derived"
    only next                  →  "{src}_derived"
    neither                    →  ticker skipped

Public API:
    reconcile_earnings_dates(affected_tickers=None) -> (z, y, aug)
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import pandas as pd

from . import config  # noqa: F401  (re-exported for tests' monkeypatching)
from . import earnings_cache as ec
from . import earnings_history as eh

log = logging.getLogger("scanner.earnings_reconcile")


# ──────────────────────────────────────────────────────────────────────
# Per-source extraction helpers
# ──────────────────────────────────────────────────────────────────────

# Sources eligible for the last_earnings position, in priority order.
# Finnhub is last — only used when no other source has any data.
_LAST_PRIORITY = ("zacks", "nasdaq", "yahoo", "finnhub")
# Sources eligible for the next_earnings position, in priority order.
# Same chain — Finnhub future rows must NOT be proxy=True.
_NEXT_PRIORITY = ("zacks", "nasdaq", "yahoo", "finnhub")

# When the source label collapses to "{src}_derived" / "{src}":
_SAME_SRC_LABEL = {
    "zacks": "zacks_derived",
    "finnhub": "finnhub_derived",
    "nasdaq": "nasdaq",
    "yahoo": "yahoo",
}


def _extract_history_lookups(
    history_df: Optional[pd.DataFrame],
    source: str,
    today: pd.Timestamp,
) -> dict[str, tuple[pd.Timestamp, pd.Timestamp]]:
    """Return per-ticker ``(last, next)`` from history rows for a single
    source. Last = max(report_date ≤ today). Next = min(report_date >
    today) excluding Finnhub's proxy rows (period_ending stand-ins are
    NOT real announcement dates and must not be promoted to
    next_earnings)."""
    out: dict[str, tuple[pd.Timestamp, pd.Timestamp]] = {}
    if history_df is None or history_df.empty:
        return out
    if "source" not in history_df.columns:
        return out
    sub = history_df.loc[history_df["source"] == source]
    if sub.empty:
        return out

    for ticker, grp in sub.groupby("ticker"):
        rd = pd.to_datetime(grp["report_date"], errors="coerce")
        past = rd.loc[rd <= today]
        future = rd.loc[rd > today]
        # Finnhub: drop proxy rows from the future-set so a period_end
        # placeholder doesn't get promoted to next_earnings.
        if source == "finnhub" and "report_date_proxy" in grp.columns:
            future_idx = future.index
            mask = ~grp.loc[future_idx, "report_date_proxy"].fillna(False).astype(bool)
            future = future.loc[mask]
        last_ts = past.max() if not past.empty else pd.NaT
        next_ts = future.min() if not future.empty else pd.NaT
        out[str(ticker)] = (last_ts, next_ts)
    return out


def _extract_dates_lookup(
    dates_df: Optional[pd.DataFrame],
    source: str,
) -> dict[str, tuple[pd.Timestamp, pd.Timestamp]]:
    """Return per-ticker ``(last, next)`` from dates rows for a single
    source. ``source="yahoo"`` ALSO collects rows whose source is
    "legacy" / "unknown" / any prior reconciler output that isn't
    "nasdaq" — pre-Phase-3 fills lacked precise source tags but were
    overwhelmingly Yahoo-derived in practice."""
    out: dict[str, tuple[pd.Timestamp, pd.Timestamp]] = {}
    if dates_df is None or dates_df.empty:
        return out
    if "source" not in dates_df.columns:
        return out

    if source == "yahoo":
        # Conservative bucket: rows whose source is "yahoo", "legacy",
        # or a prior reconcile output that included yahoo as a
        # contributor (e.g. "yahoo+nasdaq_aug", "zacks+yahoo_aug",
        # "finnhub+yahoo_aug"). The yahoo+ label is sufficient evidence
        # that yahoo's raw fill supplied at least one position on this
        # row, so the row's last/next values are usable yahoo data.
        # Prior reconcile outputs WITHOUT yahoo (zacks_derived,
        # finnhub_derived, nasdaq) are excluded — their data came from
        # other sources and must re-derive through their own chain step
        # rather than being laundered through the yahoo step.
        src_str = dates_df["source"].fillna("legacy").astype(str).str.lower()
        mask = (
            (src_str == "yahoo")
            | (src_str == "legacy")
            | (src_str == "unknown")
            | src_str.str.contains("yahoo", na=False)
        )
        sub = dates_df.loc[mask]
    else:
        sub = dates_df.loc[dates_df["source"] == source]

    if sub.empty:
        return out

    # Collapse duplicates by ticker — keep the most recently-updated row.
    if "updated_at" in sub.columns:
        sub = sub.sort_values("updated_at", ascending=False)
    sub = sub.drop_duplicates(subset=["ticker"], keep="first")

    for _, row in sub.iterrows():
        t = str(row["ticker"])
        last = row.get("last_earnings")
        nxt = row.get("next_earnings")
        out[t] = (last, nxt)
    return out


# ──────────────────────────────────────────────────────────────────────
# Priority-chain pickers
# ──────────────────────────────────────────────────────────────────────

def _pick_last(
    ticker: str,
    lookups: dict[str, dict[str, tuple[pd.Timestamp, pd.Timestamp]]],
) -> tuple[Optional[pd.Timestamp], Optional[str]]:
    """Walk _LAST_PRIORITY and return the first non-null last_earnings.
    ``lookups`` is keyed by source name (one of the priority entries)."""
    for src in _LAST_PRIORITY:
        entry = lookups.get(src, {}).get(ticker)
        if entry is None:
            continue
        last_ts = entry[0]
        if pd.notna(last_ts):
            return pd.Timestamp(last_ts), src
    return None, None


def _pick_next(
    ticker: str,
    today: pd.Timestamp,
    lookups: dict[str, dict[str, tuple[pd.Timestamp, pd.Timestamp]]],
) -> tuple[Optional[pd.Timestamp], Optional[str]]:
    """Walk _NEXT_PRIORITY and return the first non-null next_earnings
    that is strictly > today (Phase 1 stale-date fix, generalized)."""
    for src in _NEXT_PRIORITY:
        entry = lookups.get(src, {}).get(ticker)
        if entry is None:
            continue
        next_ts = entry[1]
        if pd.notna(next_ts) and pd.Timestamp(next_ts) > today:
            return pd.Timestamp(next_ts), src
    return None, None


def _source_label(last_src: Optional[str], next_src: Optional[str]) -> str:
    """Compound source label — backwards-compatible with Phase 1 outputs:
    ``"zacks_derived"``, ``"zacks+yahoo_aug"``, etc.

    * Same source for both → ``"{src}_derived"`` (or just ``"nasdaq"``
      / ``"yahoo"`` for date-only sources, matching Phase 1 conventions).
    * Different sources → ``"{last_src}+{next_src}_aug"``.
    * Only one position filled → ``_SAME_SRC_LABEL[src]``.
    * Neither → ``"unknown"`` (caller should skip the row).
    """
    if last_src and next_src:
        if last_src == next_src:
            return _SAME_SRC_LABEL.get(last_src, last_src)
        return f"{last_src}+{next_src}_aug"
    if last_src:
        return _SAME_SRC_LABEL.get(last_src, last_src)
    if next_src:
        return _SAME_SRC_LABEL.get(next_src, next_src)
    return "unknown"


# Sources whose data lives in earnings_history.parquet. Used by the
# (z, y, aug) classifier below to preserve Phase 1 counter semantics.
_HISTORY_SOURCES = {"zacks", "finnhub"}


def _classify(last_src: Optional[str], next_src: Optional[str]) -> str:
    """Return one of ``"z"`` / ``"y"`` / ``"aug"`` for the
    backward-compat counters returned by reconcile_earnings_dates.

    Phase 1 semantics preserved + extended:
      * "z" — every position that's filled comes from a history source
        (zacks or finnhub). No mixing with dates_df sources.
      * "y" — no history source involved at all. Pure dates passthrough
        (nasdaq, yahoo, or legacy).
      * "aug" — mixed: at least one position from history, the other
        from dates. The Phase 1 "zacks+yahoo_aug" case lives here.
    """
    last_h = last_src in _HISTORY_SOURCES
    next_h = next_src in _HISTORY_SOURCES
    last_present = last_src is not None
    next_present = next_src is not None

    if last_h and (next_h or not next_present):
        return "z"
    if next_h and not last_present:
        # next from history, last empty (rare edge case)
        return "z"
    if last_h or next_h:
        # one position from history, the other from dates
        return "aug"
    # Neither position from history — pure dates passthrough.
    return "y"


# ──────────────────────────────────────────────────────────────────────
# reconcile_earnings_dates
# ──────────────────────────────────────────────────────────────────────

def reconcile_earnings_dates(
    affected_tickers: Optional[list[str]] = None,
    *,
    today: Optional[pd.Timestamp] = None,
    history_df: Optional[pd.DataFrame] = None,
    dates_df: Optional[pd.DataFrame] = None,
) -> tuple[int, int, int]:
    """Rebuild earnings_dates.parquet using the Phase 4 priority chain
    (see module docstring).

    Args:
        affected_tickers: when given, only these tickers are
            recomputed (their existing rows are replaced; all other
            rows are preserved). When None, every ticker known to any
            source is considered.
        today: override reference date for past/future classification.
            Defaults to ``pd.Timestamp.today().normalize()``.
        history_df / dates_df: optional in-memory overrides used by
            tests + the Phase 3 shim to avoid round-tripping through
            disk. None → load from disk.

    Returns:
        ``(z_count, y_count, aug_count)`` — backward-compat counters.
        See ``_classify`` for the exact semantics.
    """
    if history_df is None:
        history_df = eh.load_earnings_history()
    if dates_df is None:
        dates_df = ec.load_earnings_cache()

    if today is None:
        today = pd.Timestamp.today().normalize()
    now = pd.Timestamp(datetime.now())

    # Per-source raw-data lookups
    lookups = {
        "zacks":   _extract_history_lookups(history_df, "zacks", today),
        "finnhub": _extract_history_lookups(history_df, "finnhub", today),
        "nasdaq":  _extract_dates_lookup(dates_df, "nasdaq"),
        "yahoo":   _extract_dates_lookup(dates_df, "yahoo"),
    }

    # Candidates: every ticker present in any lookup.
    all_known = (set(lookups["zacks"]) | set(lookups["finnhub"])
                 | set(lookups["nasdaq"]) | set(lookups["yahoo"]))
    if affected_tickers is None:
        candidates = all_known
    else:
        candidates = set(affected_tickers)

    z_count = 0
    y_count = 0
    aug_count = 0
    new_rows: list[dict] = []

    for t in sorted(candidates):
        last_ts, last_src = _pick_last(t, lookups)
        next_ts, next_src = _pick_next(t, today, lookups)
        if last_ts is None and next_ts is None:
            # No usable data anywhere for this ticker — skip rather than
            # write a NaT/NaT row.
            continue

        new_rows.append({
            "ticker": t,
            "last_earnings": last_ts if last_ts is not None else pd.NaT,
            "next_earnings": next_ts if next_ts is not None else pd.NaT,
            "updated_at": now,
            "source": _source_label(last_src, next_src),
        })

        kind = _classify(last_src, next_src)
        if kind == "z":
            z_count += 1
        elif kind == "y":
            y_count += 1
        else:
            aug_count += 1

    if not new_rows:
        log.debug("reconcile: no candidates with data — earnings_dates untouched")
        return z_count, y_count, aug_count

    new_df = pd.DataFrame(new_rows)

    # Merge: rows for processed tickers replace any existing entries;
    # rows for tickers OUTSIDE the candidate set are preserved as-is.
    if dates_df is not None and not dates_df.empty:
        keep = dates_df.loc[~dates_df["ticker"].isin(new_df["ticker"])]
        combined = pd.concat([keep, new_df], ignore_index=True)
    else:
        combined = new_df

    ec.save_earnings_cache(combined)
    log.info(
        "Reconciled earnings_dates.parquet: z=%d, y=%d, aug=%d",
        z_count, y_count, aug_count,
    )
    return z_count, y_count, aug_count


# Phase 6.5: `fill_yahoo_gaps` was removed. After Phase 4/5.5 every
# fill auto-reconciles, so calling targeted_fill_yahoo directly with a
# pre-computed gap list (the path Phase 3 already exposed via the
# "Targeted Fill Earnings Dates (Yahoo)" menu action) is functionally
# equivalent without the triple-reconcile this function used to chain.
