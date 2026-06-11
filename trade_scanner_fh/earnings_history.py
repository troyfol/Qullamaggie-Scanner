"""Per-quarter earnings history data layer (Zacks-fork addition).

Stores **full quarterly history** (EPS + revenue actuals + estimates +
surprises) for every ticker the user has fetched. Mirrors the
earnings_cache.py pattern but with a much wider per-row schema.

Source reporting basis
----------------------
All EPS/revenue sources that write earnings_history.parquet quote the
*adjusted / non-GAAP* (Street) basis, verified empirically against the
live parquet: finviz ``epsActual`` matches Zacks ~98% to the penny, and
Zacks vs Finnhub agree to the penny at the median. The GAAP source (SEC
EDGAR) was removed 2026-05-31 — GAAP figures aren't useful for this
scanner's trading use case — so no basis mixing exists in the parquet.

    Source   | Writes EPS/Rev into     | Basis              | Estimates/surprise?
    ---------|-------------------------|--------------------|--------------------
    finviz   | earnings_history.parquet| adjusted / non-GAAP| yes
    zacks    | earnings_history.parquet| adjusted / non-GAAP| yes
    finnhub  | earnings_history.parquet| adjusted / non-GAAP| yes
    nasdaq   | earnings_dates.parquet  | n/a (dates only)   | no
    yahoo    | earnings_dates.parquet  | n/a (dates only)   | no

Storage:  scanner_data/earnings_history.parquet
Schema (per TINYEARNINGS_FORK.md §3.1):

    ticker             string
    period_ending      datetime64[ns]   fiscal-quarter end
    report_date        datetime64[ns]   announcement date (market-mover)
    report_time        string           Open / Close / Market / Unknown
    estimated_eps      float64
    reported_eps       float64
    surprise_eps       float64          reported − estimated ($)
    surprise_eps_pct   float64          surprise as percent (5.34 = "5.34%")
    estimated_rev      float64          revenue (millions $ as Zacks reports)
    reported_rev       float64
    surprise_rev       float64
    surprise_rev_pct   float64
    source             string           "zacks" | "yahoo" — set by writer
    updated_at         datetime64[ns]   when this row was last (re)written

Sort on save: (ticker ASC, period_ending DESC) so most-recent-quarter is
always first within a ticker's slice.

Public API (mirrors earnings_cache.py):
    load_earnings_history()        -> pd.DataFrame | None
    save_earnings_history(df)      -> None  (atomic)
    get_ticker_history(t, df)      -> pd.DataFrame  (sorted period_ending DESC)
    get_most_recent_quarter(t, df) -> pd.Series | None
    compute_consecutive_beats(...) -> int

Bulk + targeted fills via Zacks:
    bulk_fill_zacks(...)
    targeted_fill_zacks(...)

Both fills update earnings_dates.parquet at every flush so the existing
Days Since / Days Until ER filters stay in sync. Phase 4's
earnings_reconcile.py will supersede this Zacks-only reconcile with full
Zacks-primary + Yahoo-fallback logic.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from . import config
from . import earnings_raw
from .zacks_scraper import FAIL_BLOCKED, FAIL_PARSE_ERROR, ZacksSession

log = logging.getLogger("scanner.earnings_history")

# Process-wide lock that serializes the read-modify-write cycle on
# earnings_history.parquet. The on-disk write itself is already atomic
# (via config.atomic_write_parquet), but the load → merge → save cycle
# spans multiple operations: without this lock, the Zacks + Finnhub
# fill workers running concurrently can each load the same snapshot,
# each merge their pending rows, and the second writer overwrites the
# first writer's appended rows. The lock is re-entrant so a single
# worker doing multiple flushes (or a finalize-after-flush path) won't
# self-deadlock. Acquired by _flush_pending_to_disk here and by the
# matching flush helper in finnhub_fill.py.
HISTORY_WRITE_LOCK = threading.RLock()

# Canonical column order — used for both schema validation and the column
# layout of newly-built DataFrames.
#
# Phase 1 of Finnhub augmentation added `report_date_proxy` (bool):
# True when `report_date` is a fallback (period_ending used because the
# upstream didn't supply an announcement date). Always False for Zacks
# rows (Zacks gives real announcement dates). Will be True for Finnhub
# /stock/earnings rows that fall outside the calendar's announcement
# window in Phase 2.
COLUMNS: list[str] = [
    "ticker", "period_ending", "report_date", "report_time",
    "estimated_eps", "reported_eps", "surprise_eps", "surprise_eps_pct",
    "estimated_rev", "reported_rev", "surprise_rev", "surprise_rev_pct",
    "source", "updated_at", "report_date_proxy",
    # YoY %: computed at fill-finalize time from same-quarter-prior-year
    # row in this same parquet. NaN when the prior-year row is absent or
    # its denominator value is 0. See `compute_yoy_columns`.
    "yoy_eps_pct", "yoy_rev_pct",
]

# Quarterly cadence is ~90 days; allow up to 135 days between consecutive
# period_endings before treating the gap as a "missing quarter" that
# breaks a consecutive-beats streak (per spec §9.1).
_MAX_QUARTER_GAP_DAYS = 135


# EPS columns nulled together when a row's reported_eps is an implausible
# reverse-split artifact (see _implausible_eps_mask).
_EPS_FIELDS: tuple[str, ...] = (
    "estimated_eps", "reported_eps", "surprise_eps",
    "surprise_eps_pct", "yoy_eps_pct",
)


def _implausible_eps_mask(
    df: pd.DataFrame, *, price_by_ticker: Optional[dict] = None,
) -> pd.Series:
    """Boolean mask of rows whose ``reported_eps`` is a reverse-split
    adjustment artifact rather than a real per-share figure:

      * |reported_eps| > ``config.MAX_PLAUSIBLE_EPS`` (absolute cap — no real
        stock has a quarterly EPS this large), OR
      * when a current share price is supplied for the ticker,
        |reported_eps| > ``config.EPS_PRICE_IMPLAUSIBLE_MULT`` × price (a real
        stock's quarterly EPS is a small fraction of its price; a sub-$5
        nano-cap "earning" $600/share is impossible).

    ``price_by_ticker``: optional ``{ticker: current_close}``. Omit it (the
    write-time path, where price isn't available) to apply the absolute cap
    only.
    """
    if df is None or df.empty or "reported_eps" not in df.columns:
        return pd.Series(False, index=getattr(df, "index", pd.RangeIndex(0)))
    ae = pd.to_numeric(df["reported_eps"], errors="coerce").abs()
    mask = ae > config.MAX_PLAUSIBLE_EPS
    if price_by_ticker:
        price = pd.to_numeric(
            df["ticker"].astype(str).map(price_by_ticker), errors="coerce",
        )
        rel = price.notna() & (price > 0) & (
            ae > config.EPS_PRICE_IMPLAUSIBLE_MULT * price
        )
        mask = mask | rel
    return mask.fillna(False)


def _null_eps_fields(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """Return a copy of ``df`` with the EPS columns set to NaN on ``mask``
    rows. Revenue + dates are kept — only the per-share EPS is untrusted, so
    the row still carries a real report_date for the calendar."""
    import numpy as np
    out = df.copy()
    cols = [c for c in _EPS_FIELDS if c in out.columns]
    if cols and mask.any():
        out.loc[mask, cols] = np.nan
    return out


def sanitize_eps_artifacts(df: pd.DataFrame) -> pd.DataFrame:
    """INGEST-time, price-relative EPS artifact guard for the fill flush
    paths (finviz / zacks / finnhub). Nulls the EPS columns on freshly-
    fetched rows whose ``reported_eps`` is a reverse-split artifact —
    ``|eps|`` exceeding the absolute cap OR ``EPS_PRICE_IMPLAUSIBLE_MULT`` ×
    the ticker's current close.

    This is the WRITE-PATH counterpart to ``migrate_sanitize_absurd_eps``
    (the one-time historical migration): incremental fills would otherwise
    re-introduce nano-cap artifacts the absolute write-guard alone can't
    catch (the $20-$100k band). Loads OHLCV only for candidate tickers
    (|eps| > 20) in the supplied (small, freshly-fetched) ``df``, so a normal
    flush with no large-EPS rows does zero OHLCV reads.
    """
    if df is None or df.empty or "reported_eps" not in df.columns:
        return df
    ae = pd.to_numeric(df["reported_eps"], errors="coerce").abs()
    cand = sorted(set(df.loc[ae > 20, "ticker"].astype(str)))
    prices = _load_current_prices(cand) if cand else {}
    mask = _implausible_eps_mask(df, price_by_ticker=prices)
    return _null_eps_fields(df, mask) if mask.any() else df


# ──────────────────────────────────────────────────────────────────────
# Load / save
# ──────────────────────────────────────────────────────────────────────

def load_earnings_history() -> Optional[pd.DataFrame]:
    """Read earnings_history.parquet. Returns None if file missing.
    Datetime columns are normalized to tz-naive so consumer code can mix
    with `datetime.now()` without timezone hassles.

    Backward compat: rows missing the ``report_date_proxy`` column
    (legacy Zacks-only files written before Phase 1) get stamped False
    on read since Zacks always supplied real announcement dates.
    """
    path = config.EARNINGS_HISTORY_PARQUET
    if not path.exists():
        return None
    # Retry once on a read failure: the common cause on Windows is a transient
    # sharing-violation while another thread's os.replace swaps the file in. A
    # genuinely corrupt file still falls through to None after the retry — note
    # callers then treat None as "no history" and can re-queue a bulk fill, so
    # the retry is what prevents a momentary lock from looking like data loss.
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
        log.warning("Failed to read earnings_history.parquet: %s", last_exc)
        return None

    for col in ("period_ending", "report_date", "updated_at"):
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                if df[col].dt.tz is not None:
                    df[col] = df[col].dt.tz_localize(None)
            except (AttributeError, TypeError):
                pass
    if "report_date_proxy" not in df.columns:
        df["report_date_proxy"] = False
    else:
        # `.fillna(False).astype(bool)` triggers a Pandas 2.2 FutureWarning
        # about silent object→bool downcast. Build the bool series via a
        # mask that's explicit about the substitution and skips the
        # deprecated downcast path.
        proxy = df["report_date_proxy"]
        df["report_date_proxy"] = proxy.where(proxy.notna(), False).astype(bool)
    return df


def save_earnings_history(
    df: pd.DataFrame, *, sort: bool = True, dedup: Optional[bool] = None,
) -> None:
    """Atomically write earnings_history.parquet. Drops any rows missing
    both `ticker` and `period_ending` since they're not addressable by
    the lookup helpers.

    Args:
        df: rows to persist.
        sort: when True (default), sort `(ticker ASC, period_ending DESC)`
            before writing — guarantees the most-recent quarter for any
            ticker is the first row of its slice on disk. Bulk fills pass
            `sort=False` for per-flush writes (audit L8) and a single
            sorted save at end-of-fill keeps the on-disk layout canonical.
            Downstream consumers (`get_ticker_history`,
            `compute_consecutive_beats`, scanner lookup) all re-sort
            internally, so the on-disk order doesn't affect correctness.
        dedup: when True, apply ``dedupe_history`` (per-(ticker,
            period_ending) source-priority pick) before writing so the
            on-disk parquet has at most one row per fiscal quarter.
            Defaults to the value of ``sort`` — final / canonical writes
            dedup; per-flush writes don't (kept the same shape as the
            sort gate so a single ``sort=True`` flips both). Pass an
            explicit bool to override.
    """
    if df is None or df.empty:
        return
    if dedup is None:
        dedup = sort
    out = df.copy()
    # Coerce dtypes consistently
    for col in ("period_ending", "report_date", "updated_at"):
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce")
    # Drop rows that can't be addressed
    out = out.dropna(subset=["ticker", "period_ending"]).reset_index(drop=True)
    # Re-prune the rolling history cap on every canonical write. The fill
    # cutoff is evaluated once at fetch time, so a row fetched at the
    # boundary lingers as the daily-advancing cutoff overtakes it; re-pruning
    # here keeps the on-disk window a clean trailing EARNINGS_HISTORY_YEARS.
    if "period_ending" in out.columns and not out.empty:
        _cap_cutoff = pd.Timestamp.today().normalize() - pd.DateOffset(
            years=config.EARNINGS_HISTORY_YEARS,
        )
        out = out.loc[
            pd.to_datetime(out["period_ending"], errors="coerce") >= _cap_cutoff
        ].reset_index(drop=True)
    if dedup:
        out = dedupe_history(out)
    # Sanity guard: null EPS fields on rows whose reported_eps is an
    # impossible-magnitude reverse-split artifact. Absolute cap only here —
    # the write path has no share price; the eps-sanitize migration does the
    # precise price-relative pass. Cheap (a single abs comparison).
    bad_eps = _implausible_eps_mask(out)
    if bad_eps.any():
        out = _null_eps_fields(out, bad_eps)
    if sort:
        out = out.sort_values(["ticker", "period_ending"], ascending=[True, False])
    # Ensure the canonical column order even if caller passed extras
    keep = [c for c in COLUMNS if c in out.columns]
    out = out[keep + [c for c in out.columns if c not in keep]]
    # Low-cardinality string columns → category. `source` has 2 values and
    # `report_time` has ~3; category keeps them compact in memory and is the
    # canonical on-disk dtype. The fill path concats object-dtype rows (which
    # upcasts the column back to object), so coercing HERE — the single write
    # path — is what makes the category dtype persist across fills. pyarrow
    # round-trips it cleanly and all downstream ops (`==`, `.str.*`,
    # `.astype(str)`) tolerate category, so no reader needs to change.
    for col in ("source", "report_time"):
        if col in out.columns:
            out[col] = out[col].astype("category")
    config.atomic_write_parquet(
        out, config.EARNINGS_HISTORY_PARQUET, engine="pyarrow", index=False,
    )


# ──────────────────────────────────────────────────────────────────────
# Lookup helpers
# ──────────────────────────────────────────────────────────────────────

# Per-(ticker, period_ending) source priority. Lowest integer wins.
# Reorder via tuple position — adding a new source means adding it here.
# Public chain order (most authoritative first):
#   finviz — adjusted / non-GAAP EPS + real announcement dates/times;
#            matches Zacks ~98% to the penny with finer revenue precision
#   zacks  — adjusted / non-GAAP EPS, real announcement dates
#   finnhub — adjusted / non-GAAP EPS (matches Zacks), calendar-quarter normed
# (SEC EDGAR / GAAP source removed 2026-05-31; finviz added top-priority.)
_SOURCE_PRIORITY: dict[str, int] = {
    "finviz": 0,
    "zacks": 1,
    "finnhub": 2,
}
_SOURCE_PRIORITY_FALLBACK = 99


# Estimate / surprise columns a higher-priority winner row may be missing.
# When ``backfill_estimates=True`` these are filled onto the winner from
# the best available lower-priority same-slot row (always same adjusted
# basis now that EDGAR/GAAP is gone). The reported actuals and source
# label are never touched — only these estimate-derived fields inherit.
_ESTIMATE_BACKFILL_COLS: tuple[str, ...] = (
    "estimated_eps", "surprise_eps", "surprise_eps_pct",
    "estimated_rev", "surprise_rev", "surprise_rev_pct",
)


# Sources whose ``period_ending`` is the TRUE fiscal-quarter end (day-1 of
# the fiscal-end month). Finnhub is deliberately EXCLUDED: its
# ``/stock/earnings`` ``period`` is the calendar-quarter-end of the calendar
# quarter that *contains* the fiscal-quarter end, so its ``period_ending`` is
# calendar-normed and can disagree with the fiscal grid for non-calendar
# fiscal years. This is true for EVERY finnhub row regardless of
# ``report_date_proxy`` (the proxy flag only records whether a real
# announcement date was found — it does NOT track the period_ending norming).
_FISCAL_ACCURATE_SOURCES: frozenset = frozenset({"finviz", "zacks"})


def _calendar_dup_drop_mask(df: pd.DataFrame) -> pd.Series:
    """Boolean mask: True for rows to DROP as calendar-vs-fiscal phantom
    duplicates. A row from a calendar-normed source (any source NOT in
    ``_FISCAL_ACCURATE_SOURCES`` — currently only finnhub) is a phantom
    duplicate when a fiscal-accurate source (finviz/zacks) already holds a
    row for the same ``(ticker, CALENDAR quarter)``.

    Why this is needed (the calendar-vs-fiscal bug)
    ----------------------------------------------
    Finviz/Zacks store a quarter under its true fiscal-quarter end
    (``period_ending`` = day-1 of the fiscal-end month). Finnhub's
    ``/stock/earnings`` ``period`` is the calendar-quarter-end of the
    calendar quarter that *contains* that fiscal-end, normalized to day-1
    (Mar/Jun/Sep/Dec). For a non-calendar fiscal year (e.g. BBCP, FY ends
    Oct-31) the same earnings event therefore lands at two different
    ``period_ending`` values — finviz 2025-04-01 vs finnhub 2025-06-01 —
    so the per-(ticker, period_ending) dedup can't see they're the same
    event. But both *always* fall in the same CALENDAR quarter (Q2 2025
    here), because finnhub buckets the fiscal quarter into its containing
    calendar quarter and the fiscal-end month is by definition inside that
    quarter. So a calendar-quarter key reunites them — for ALL finnhub
    rows, not just ``report_date_proxy=True`` ones (a finnhub row gets
    proxy=False whenever /calendar/earnings supplied a real announcement
    date, but its period_ending is still calendar-normed).

    Safety
    ------
    Only NON-fiscal-accurate rows (finnhub) are ever flagged, and only
    when a fiscal-accurate row covers the same calendar quarter. Finviz/
    Zacks rows are never dropped here, so a genuine stub / fiscal-year-
    change quarter — which would appear as TWO same-source fiscal-accurate
    rows in one calendar quarter — is always safe. A finnhub row with no
    fiscal-accurate cover in its calendar quarter (genuine gap-fill) is
    kept. Companies report one fiscal quarter per calendar quarter, so a
    finnhub row sharing a calendar quarter with a finviz/zacks row is the
    SAME event and is correctly collapsed to the higher-priority source.
    """
    if df is None or df.empty or "source" not in df.columns:
        return pd.Series(False, index=getattr(df, "index", pd.RangeIndex(0)))
    src = df["source"].astype(str).str.lower()
    accurate = src.isin(_FISCAL_ACCURATE_SOURCES)
    pe = pd.to_datetime(df["period_ending"], errors="coerce")
    valid = pe.notna()
    if not ((~accurate) & valid).any():
        return pd.Series(False, index=df.index)
    # (ticker, calendar quarter) key. Default "Q" freq is calendar
    # quarters (Q-DEC), which is exactly the bucketing we want.
    cal_q = (
        df["ticker"].astype(str) + "|" + pe.dt.to_period("Q").astype(str)
    )
    covered_keys = set(cal_q[valid & accurate])
    if not covered_keys:
        return pd.Series(False, index=df.index)
    return (~accurate) & valid & cal_q.isin(covered_keys)


def dedupe_history(
    history_df: Optional[pd.DataFrame],
    *,
    backfill_estimates: bool = False,
) -> pd.DataFrame:
    """Per-(ticker, period_ending) priority dedup: for each fiscal-
    quarter slot the highest-priority source wins. Priority order is
    defined by ``_SOURCE_PRIORITY`` (currently finviz > zacks > finnhub).
    Within the same source, the most-recently-updated row wins.

    Gap-fill semantics: a ticker can carry rows from multiple sources
    as long as each row covers a different period_ending. E.g., Zacks
    covers Q1-Q4 2025 + Q1 2026 and Finnhub additionally provides Q3
    2024 → both source sets survive because they fill different slots.
    For the same slot, the highest-priority source's row replaces the
    lower-priority ones. Every source writes the same adjusted /
    non-GAAP basis (the GAAP EDGAR source was removed 2026-05-31), so
    there is no cross-basis mixing to reconcile.

    ``backfill_estimates`` (read-side only — keep it False for the
    write-time canonical dedup so on-disk source rows stay pure): when a
    slot's winner lacks estimate / surprise values, inherit those
    specific columns from the highest-priority lower same-slot row that
    has them, before the loser is dropped. Same-basis throughout, so the
    reported / estimated / surprise triple stays internally consistent.

    Returns an empty COLUMNS-shaped frame on None / empty input.
    """
    if history_df is None or history_df.empty:
        return pd.DataFrame(columns=COLUMNS)
    df = history_df.copy()
    if "source" not in df.columns:
        # Pre-Phase-2 file with no source column — nothing to dedup on.
        return df.reset_index(drop=True)

    src_str = df["source"].astype(str).str.lower()
    df["_prio"] = src_str.map(_SOURCE_PRIORITY).fillna(_SOURCE_PRIORITY_FALLBACK)

    sort_cols = ["ticker", "period_ending", "_prio"]
    if "updated_at" in df.columns:
        sort_cols.append("updated_at")
        # ticker ASC, period_ending ASC, _prio ASC (lower wins so the
        # highest-priority source floats to the top of each group),
        # updated_at DESC so the most-recently-updated row beats older
        # writes from the same source. drop_duplicates(keep="first")
        # then picks the winner per (ticker, period_ending).
        ascending = [True, True, True, False]
    else:
        ascending = [True, True, True]

    df = df.sort_values(sort_cols, ascending=ascending, kind="stable")

    if backfill_estimates:
        # Within each (ticker, period_ending) group the rows are already
        # ordered winner-first (lowest _prio at the top). A backward fill
        # pulls each missing estimate/surprise value UP from the nearest
        # lower-priority row below it, so the winner (first row) inherits
        # the best available estimate data before we drop the losers.
        # All sources share the adjusted basis, so no basis correction is
        # needed on the reported actuals.
        present = [c for c in _ESTIMATE_BACKFILL_COLS if c in df.columns]
        if present:
            grp = df.groupby(["ticker", "period_ending"], sort=False)
            for col in present:
                df[col] = grp[col].bfill()

    df = (
        df.drop_duplicates(subset=["ticker", "period_ending"], keep="first")
          .drop(columns="_prio")
          .reset_index(drop=True)
    )

    # Cross-source calendar-quarter collapse: after the exact-slot dedup,
    # drop calendar-normed finnhub rows whose calendar quarter is already
    # covered by a fiscal-accurate (finviz/zacks) row. This removes the
    # calendar-vs-fiscal phantom duplicates that the (ticker,
    # period_ending) key can't catch. See ``_calendar_dup_drop_mask``.
    cal_dups = _calendar_dup_drop_mask(df)
    if cal_dups.any():
        df = df.loc[~cal_dups].reset_index(drop=True)

    keep = [c for c in COLUMNS if c in df.columns]
    extra = [c for c in df.columns if c not in keep]
    return df[keep + extra].reset_index(drop=True)


def get_ticker_history(ticker: str, history_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Return all quarters for one ticker, sorted period_ending DESC.
    Empty DataFrame if the ticker isn't present (or history_df is None).

    Phase 2: dedup by (ticker, period_ending) preferring Zacks > Finnhub
    so consec-beats / surprise-pct filters never see the same quarter
    twice."""
    if history_df is None or history_df.empty:
        return pd.DataFrame(columns=COLUMNS)
    sub = history_df.loc[history_df["ticker"] == ticker]
    if sub.empty:
        return sub
    sub = dedupe_history(sub)
    return sub.sort_values("period_ending", ascending=False).reset_index(drop=True)


def get_most_recent_quarter(
    ticker: str, history_df: Optional[pd.DataFrame],
) -> Optional[pd.Series]:
    """Return the most-recent quarter row for `ticker`, or None."""
    sub = get_ticker_history(ticker, history_df)
    if sub.empty:
        return None
    return sub.iloc[0]


def compute_yoy_columns(history_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Add / refresh `yoy_eps_pct` and `yoy_rev_pct` columns on every row.

    For each (ticker, period_ending) pair, locates the row whose
    period_ending is exactly 1 calendar year earlier (using the day-1
    normalized convention: e.g. 2026-03-01 → 2025-03-01). Then:

        yoy_eps_pct = (cur.reported_eps - prior.reported_eps)
                      / |prior.reported_eps| * 100
        yoy_rev_pct = (cur.reported_rev - prior.reported_rev)
                      / |prior.reported_rev| * 100

    The standard "% growth" formula handles negative-prior cases
    correctly: prior=-0.50, cur=+0.10 → yoy = (0.60)/0.50 = +120%
    (a positive improvement). NaN when prior row missing, prior value
    is NaN/zero, or current value is NaN.

    Idempotent — call once at fill-finalize time. New row construction
    in `_record_to_history_dict` / `_row_to_history_dict` does NOT need
    to set these columns; this helper fills them post-flush. Safe to
    invoke on the entire parquet (operates per-ticker via groupby).
    """
    if history_df is None or history_df.empty:
        return history_df
    df = history_df.copy()
    if "period_ending" not in df.columns or "ticker" not in df.columns:
        return df

    import numpy as np

    period_ts = pd.to_datetime(df["period_ending"], errors="coerce")

    # Vectorized prior-year self-join (audit H1 — replaces an O(n) Python
    # row loop over the whole ~138k-row parquet that ran on every scan setup
    # and every fill finalize). Semantics are preserved exactly:
    #   * prior-year period is `period - DateOffset(years=1)` per row (same
    #     leap-year / day-1 behavior as the old scalar subtraction);
    #   * when a (ticker, period) repeats, the LAST occurrence wins as the
    #     prior-year source (matches the old period_to_idx last-assignment);
    #   * non-numeric / NaN current-or-prior values, a missing prior row, and
    #     a prior magnitude below the MIN_YOY_* floor all yield NaN.
    has_eps = "reported_eps" in df.columns
    has_rev = "reported_rev" in df.columns
    cur_eps = (pd.to_numeric(df["reported_eps"], errors="coerce")
               if has_eps else pd.Series(np.nan, index=df.index))
    cur_rev = (pd.to_numeric(df["reported_rev"], errors="coerce")
               if has_rev else pd.Series(np.nan, index=df.index))

    # Prior side: one row per (ticker, period_ending), last-wins on dups.
    prior_tbl = pd.DataFrame({
        "ticker": df["ticker"].to_numpy(),
        "p": period_ts.to_numpy(),
        "prior_eps": cur_eps.to_numpy(),
        "prior_rev": cur_rev.to_numpy(),
    }).dropna(subset=["p"]).drop_duplicates(subset=["ticker", "p"], keep="last")

    # Current side: each row's prior-year join key. Rows with a NaT period get
    # a NaT key, which never matches (so they stay NaN — same as the old skip).
    cur = pd.DataFrame({
        "ticker": df["ticker"].to_numpy(),
        "prior_p": (period_ts - pd.DateOffset(years=1)).to_numpy(),
        "cur_eps": cur_eps.to_numpy(),
        "cur_rev": cur_rev.to_numpy(),
    })
    merged = cur.merge(
        prior_tbl, left_on=["ticker", "prior_p"], right_on=["ticker", "p"],
        how="left", sort=False,
    )
    # Left merge against a (ticker, p)-deduped table is 1:1 per left row and
    # preserves left order, so the result aligns positionally with df.
    merged.index = df.index

    def _yoy(cur_v, prior_v, min_base):
        ok = (cur_v.notna() & prior_v.notna() & (prior_v.abs() >= min_base))
        return pd.Series(
            np.where(ok, (cur_v - prior_v) / prior_v.abs() * 100.0, np.nan),
            index=df.index, dtype="float64",
        )

    df["yoy_eps_pct"] = _yoy(merged["cur_eps"], merged["prior_eps"],
                             config.MIN_YOY_EPS_BASE)
    df["yoy_rev_pct"] = _yoy(merged["cur_rev"], merged["prior_rev"],
                             config.MIN_YOY_REV_BASE)
    return df


def compute_consecutive_beats(
    ticker_history: Optional[pd.DataFrame],
    metric: str,
    threshold_pct: float,
) -> int:
    """Walk newest → oldest, counting quarters where surprise % strictly
    exceeds `threshold_pct`. A miss / NaN / missing quarter (>135-day gap
    between consecutive `period_ending`s) breaks the streak immediately.

    Iteration order is by `report_date DESC` to match the table's
    Q-1..Q-N column display — Q-1 is whichever quarter announced most
    recently. But the cadence-gap test uses `period_ending` (the
    fiscal-quarter end), NOT `report_date`. Reasoning:

      - The "missing quarter" semantic is about the underlying business
        cycle: did the company actually deliver a quarter of results?
      - `period_ending` is rigid (always at quarter boundaries); a
        > 135-day gap there means a quarter was genuinely skipped.
      - `report_date` slides around (announcement timing varies with
        audit/filing calendars). A late announcement can produce a
        report_date gap > 135 days even when the underlying quarters
        happened on schedule — that would falsely break a real streak.

    Concretely: a ticker that beats every quarter for 5 quarters but
    delays Q3's announcement by 6 weeks would have `report_date` gaps
    of ~88 / ~88 / 132 / 175 / 88 days. The 175-day report_date gap
    would falsely truncate the streak under the prior implementation.
    Under period_ending the gaps are all ~91 days, no false break.

    Args:
        ticker_history: a slice of earnings_history.parquet for one ticker
            (or None / empty → returns 0).
        metric: "eps" or "rev" — selects which surprise column to use.
        threshold_pct: surprise must be > this. Strict >, so threshold=0
            means only positive surprises count.

    Returns:
        Length of the trailing-most beat streak. 0 when most recent
        quarter is a miss / NaN / threshold-tied.
    """
    if ticker_history is None or ticker_history.empty:
        return 0
    surp_col = f"surprise_{metric}_pct"
    if surp_col not in ticker_history.columns:
        return 0

    df = ticker_history.sort_values("report_date", ascending=False).reset_index(drop=True)
    n = len(df)
    if n == 0:
        return 0

    # Vectorized per-row "is this a beat?" test (audit L7). NaN surprises
    # become False so they break the streak rather than poisoning it.
    surp = pd.to_numeric(df[surp_col], errors="coerce")
    is_beat = (surp > threshold_pct).fillna(False)

    # Cadence gap: positive number of days between row i and row i+1
    # under report_date DESC. `diff(-1)` computes df[i] - df[i+1] —
    # positive for the natural "newer minus older" ordering. We use
    # `period_ending` (fiscal-quarter end), not `report_date`, because
    # the missing-quarter semantic is about the business cycle (see
    # docstring above for the late-filing failure mode under
    # report_date).
    if "period_ending" in df.columns:
        peds = pd.to_datetime(df["period_ending"], errors="coerce")
    else:
        # Backwards-compat: legacy histories without period_ending fall
        # back to report_date. Logged once per ticker if it happens.
        log.debug(
            "compute_consecutive_beats: history missing period_ending — "
            "falling back to report_date for cadence."
        )
        peds = pd.to_datetime(df["report_date"], errors="coerce")
    if n > 1:
        gaps = peds.diff(-1).dt.days  # length n; last entry is NaN
        # cadence_ok_at[i] is True for i=0 (no prior) or if the gap from
        # row i-1 to row i is within tolerance. Build by aligning gaps[:-1].
        cadence_ok = pd.Series([True] * n)
        cadence_ok.iloc[1:] = (gaps.iloc[:-1].values <= _MAX_QUARTER_GAP_DAYS)
        # NaN period_ending at row i → break (cadence_ok at that row is False)
        cadence_ok &= peds.notna().values
    else:
        cadence_ok = pd.Series([True])

    ok = is_beat & cadence_ok
    if ok.all():
        return n
    # First False in `ok` = first row that breaks the streak. The streak
    # is everything strictly before that row, so its index equals the
    # streak length.
    first_break = int((~ok).idxmax())
    return first_break


# ──────────────────────────────────────────────────────────────────────
# Phase 6.5 diagnostics — pure read-side helpers
# ──────────────────────────────────────────────────────────────────────

def coverage_report(
    universe_symbols: list[str],
    blacklist: set[str],
    *,
    history_df: Optional[pd.DataFrame] = None,
) -> dict:
    """Partition the universe by which earnings sources cover each ticker.

    Under the gap-fill source policy, a single ticker can carry rows
    from multiple sources (finviz/zacks/finnhub) covering different
    fiscal-quarter slots. The report tracks per-source coverage (with
    overlap) plus the canonical "no coverage from anywhere" gap.

    Returns a dict shaped:

        {
          "total_universe":   int,
          "blacklisted":      int,
          "in_scope":         int,
          # Per-source coverage — sets may overlap
          "zacks":            {"count": int, "tickers": list[str]},
          "finnhub":          {"count": int, "tickers": list[str]},
          # Tickers with no rows from ANY source
          "no_coverage":      {"count": int, "tickers": list[str]},
          # Most-recent period_ending per source
          "most_recent_zacks_quarter":   pd.Timestamp | None,
          "most_recent_finnhub_quarter": pd.Timestamp | None,
          # Back-compat (Zacks/Finnhub 2-source partition)
          "zacks_only":       {"count": int, "tickers": list[str]},
          "finnhub_only":     {"count": int, "tickers": list[str]},
          "both":             {"count": int, "tickers": list[str]},
          "neither":          {"count": int, "tickers": list[str]},  # alias of no_coverage
        }
    """
    if history_df is None:
        history_df = load_earnings_history()

    in_scope = [t for t in universe_symbols if t and t not in blacklist]
    blacklisted = [t for t in universe_symbols if t and t in blacklist]

    per_source: dict[str, set[str]] = {
        "finviz": set(), "zacks": set(), "finnhub": set(),
    }
    last_per_source: dict[str, Optional[pd.Timestamp]] = {
        "finviz": None, "zacks": None, "finnhub": None,
    }

    if history_df is not None and not history_df.empty:
        srcs = history_df["source"].astype(str).fillna("").str.lower()
        for src_key in per_source.keys():
            mask = srcs.str.contains(src_key, na=False)
            if not mask.any():
                continue
            sub = history_df.loc[mask]
            per_source[src_key] |= set(sub["ticker"].astype(str).unique())
            periods = pd.to_datetime(sub["period_ending"], errors="coerce")
            latest = periods.max()
            if pd.notna(latest):
                last_per_source[src_key] = latest

    in_scope_set = set(in_scope)
    for k in per_source:
        per_source[k] &= in_scope_set
    have_finviz = per_source["finviz"]
    have_zacks = per_source["zacks"]
    have_finnhub = per_source["finnhub"]
    no_coverage = in_scope_set - have_finviz - have_zacks - have_finnhub

    # Back-compat 2-source partition (Zacks vs Finnhub).
    both_zf = have_zacks & have_finnhub
    zacks_only = have_zacks - have_finnhub
    finnhub_only = have_finnhub - have_zacks

    def _bucket(s: set[str]) -> dict:
        return {"count": len(s), "tickers": sorted(s)}

    return {
        "total_universe":  len(universe_symbols),
        "blacklisted":     len(blacklisted),
        "in_scope":        len(in_scope),
        "finviz":          _bucket(have_finviz),
        "zacks":           _bucket(have_zacks),
        "finnhub":         _bucket(have_finnhub),
        "no_coverage":     _bucket(no_coverage),
        "most_recent_finviz_quarter":  last_per_source["finviz"],
        "most_recent_zacks_quarter":   last_per_source["zacks"],
        "most_recent_finnhub_quarter": last_per_source["finnhub"],
        # Back-compat
        "zacks_only":      _bucket(zacks_only),
        "finnhub_only":    _bucket(finnhub_only),
        "both":            _bucket(both_zf),
        "neither":         _bucket(no_coverage),
    }


# Diagnostic findings dataclass used by verify_integrity.
@dataclass
class IntegrityFinding:
    check: str           # short identifier, e.g. "duplicate_pk"
    severity: str        # "error" | "warning"
    affected_rows: int   # how many rows the check flagged
    sample: list[dict]   # first ~5 offending rows (column subset, JSON-safe)
    auto_fixable: bool   # whether fix_integrity_issues can resolve it
    description: str     # human-readable explanation


# Subset of columns to capture in samples — keeps the dialog readable
# and keeps sample rows JSON-serializable for "Save report".
_SAMPLE_COLS = ("ticker", "period_ending", "report_date", "source",
                "report_date_proxy", "updated_at")
_REQUIRED_COLS = ("ticker", "period_ending", "report_date", "source",
                  "estimated_eps", "reported_eps", "updated_at")


def _sample_rows(sub: pd.DataFrame, n: int = 5) -> list[dict]:
    """Pull `n` rows (subset of columns) for an integrity finding's
    sample list. Tolerates missing sample columns."""
    cols = [c for c in _SAMPLE_COLS if c in sub.columns]
    if not cols or sub.empty:
        return []
    out: list[dict] = []
    for _, row in sub.head(n).iterrows():
        rec = {}
        for c in cols:
            v = row[c]
            if isinstance(v, pd.Timestamp):
                rec[c] = v.isoformat() if pd.notna(v) else None
            elif pd.isna(v):
                rec[c] = None
            else:
                rec[c] = v if isinstance(v, (str, int, float, bool)) else str(v)
        out.append(rec)
    return out


def verify_integrity(
    history_df: Optional[pd.DataFrame] = None,
) -> list[IntegrityFinding]:
    """Walk earnings_history.parquet checking for known integrity
    issues. Empty list = clean. Each finding has an ``auto_fixable``
    flag — see ``fix_integrity_issues`` for the corresponding repairs.

    Checks (in run order):
      1. duplicate_pk        - same (ticker, period_ending, source)
                                appears > 1× → drop_duplicates(keep='last')
      2. orphan_ticker       - ticker is null/empty → drop
      3. orphan_period       - period_ending is NaT → drop
      4. null_source         - source column null → stamp 'legacy'
      5. proxy_dtype_drift   - report_date_proxy missing or non-bool
                                → coerce to bool
      6. rev_column_dtype    - revenue columns stored as object dtype
                                (typically because every value is None)
                                → astype(float)
      7. schema_missing_cols - REQUIRED column absent → NOT auto-fixable
      8. period_predates_cap - period_ending older than the configured
                                EARNINGS_HISTORY_YEARS cap → drop
                                (warning only — silent fix)
    """
    findings: list[IntegrityFinding] = []

    if history_df is None:
        history_df = load_earnings_history()
    if history_df is None or history_df.empty:
        return findings

    # 1 — duplicate (ticker, period_ending, source) PK violations
    if {"ticker", "period_ending", "source"}.issubset(history_df.columns):
        dup_mask = history_df.duplicated(
            subset=["ticker", "period_ending", "source"], keep=False,
        )
        if dup_mask.any():
            findings.append(IntegrityFinding(
                check="duplicate_pk",
                severity="error",
                affected_rows=int(dup_mask.sum()),
                sample=_sample_rows(history_df.loc[dup_mask]),
                auto_fixable=True,
                description=(
                    "Duplicate (ticker, period_ending, source) triplets "
                    "— soft-PK violation. Auto-fix drops duplicates "
                    "keeping the most-recently-updated copy."
                ),
            ))

    # 2 — orphan ticker
    if "ticker" in history_df.columns:
        orph = history_df["ticker"].isna() | (
            history_df["ticker"].astype(str).str.strip() == ""
        )
        if orph.any():
            findings.append(IntegrityFinding(
                check="orphan_ticker",
                severity="error",
                affected_rows=int(orph.sum()),
                sample=_sample_rows(history_df.loc[orph]),
                auto_fixable=True,
                description=(
                    "Rows with null/empty ticker. Auto-fix drops them."
                ),
            ))

    # 3 — orphan period_ending
    if "period_ending" in history_df.columns:
        orph_p = history_df["period_ending"].isna()
        if orph_p.any():
            findings.append(IntegrityFinding(
                check="orphan_period",
                severity="error",
                affected_rows=int(orph_p.sum()),
                sample=_sample_rows(history_df.loc[orph_p]),
                auto_fixable=True,
                description=(
                    "Rows with NaT period_ending. Auto-fix drops them."
                ),
            ))

    # 4 — null source
    if "source" in history_df.columns:
        null_s = history_df["source"].isna() | (
            history_df["source"].astype(str).str.strip() == ""
        )
        if null_s.any():
            findings.append(IntegrityFinding(
                check="null_source",
                severity="warning",
                affected_rows=int(null_s.sum()),
                sample=_sample_rows(history_df.loc[null_s]),
                auto_fixable=True,
                description=(
                    "Rows with null/empty source column. Auto-fix stamps "
                    "them as 'legacy'."
                ),
            ))

    # 5 — proxy dtype drift
    if "report_date_proxy" in history_df.columns:
        proxy_col = history_df["report_date_proxy"]
        # Column is fine if every non-null is a real bool.
        non_null = proxy_col.dropna()
        if not non_null.empty:
            non_bool = sum(1 for v in non_null
                           if not isinstance(v, (bool,)))
            if non_bool > 0:
                findings.append(IntegrityFinding(
                    check="proxy_dtype_drift",
                    severity="warning",
                    affected_rows=int(non_bool),
                    sample=[],
                    auto_fixable=True,
                    description=(
                        "report_date_proxy column has non-bool values. "
                        "Auto-fix coerces to bool."
                    ),
                ))
        # Pure-null proxy column is fine — load_earnings_history fills it.

    # 6 — revenue column object dtype (real-world drift seen on a
    # Finnhub-only fill where every revenue value was None).
    rev_cols = ("estimated_rev", "reported_rev", "surprise_rev",
                "surprise_rev_pct")
    rev_drift_cols = [
        c for c in rev_cols
        if c in history_df.columns
        and history_df[c].dtype == object
    ]
    if rev_drift_cols:
        findings.append(IntegrityFinding(
            check="rev_column_dtype",
            severity="warning",
            affected_rows=len(rev_drift_cols),  # count of columns affected
            sample=[{"column": c} for c in rev_drift_cols],
            auto_fixable=True,
            description=(
                f"Revenue columns stored as object dtype: "
                f"{', '.join(rev_drift_cols)}. Usually means the column "
                f"is all-None and pandas didn't coerce to float64. "
                f"Auto-fix runs pd.to_numeric(errors='coerce')."
            ),
        ))

    # 7 — required columns missing
    missing_req = [c for c in _REQUIRED_COLS if c not in history_df.columns]
    if missing_req:
        findings.append(IntegrityFinding(
            check="schema_missing_cols",
            severity="error",
            affected_rows=len(missing_req),
            sample=[{"column": c} for c in missing_req],
            auto_fixable=False,
            description=(
                f"Required columns absent: {', '.join(missing_req)}. "
                f"Not auto-fixable — implies a non-standard writer "
                f"touched the parquet."
            ),
        ))

    # 8 — period older than 5y cap
    if "period_ending" in history_df.columns:
        cutoff = pd.Timestamp.today().normalize() - pd.DateOffset(
            years=config.EARNINGS_HISTORY_YEARS,
        )
        too_old = (
            pd.to_datetime(history_df["period_ending"], errors="coerce")
            < cutoff
        )
        too_old = too_old.fillna(False)
        if too_old.any():
            findings.append(IntegrityFinding(
                check="period_predates_cap",
                severity="warning",
                affected_rows=int(too_old.sum()),
                sample=_sample_rows(history_df.loc[too_old]),
                auto_fixable=True,
                description=(
                    f"Rows with period_ending older than the "
                    f"{config.EARNINGS_HISTORY_YEARS}-year cap — "
                    f"shouldn't have been written by current fills. "
                    f"Auto-fix drops them."
                ),
            ))

    # 9 — same-period cross-source overlap (dedup not applied)
    # Under the gap-fill policy, tickers MAY carry rows from multiple
    # sources as long as each row covers a different period_ending
    # slot. What's still a violation is two rows for the SAME
    # (ticker, period_ending) coming from different sources — that
    # means write-time dedup didn't run (or was applied with sort=False
    # and the canonical save was skipped). Auto-fix re-runs
    # dedupe_history, which picks the highest-priority source per slot.
    if (
        "source" in history_df.columns
        and "ticker" in history_df.columns
        and "period_ending" in history_df.columns
    ):
        slot_mask = history_df.duplicated(
            subset=["ticker", "period_ending"], keep=False,
        )
        if slot_mask.any():
            # Refine to rows where the duplicates come from different
            # sources — same-source duplicate PKs are already covered by
            # check #1 (duplicate_pk).
            slots = history_df.loc[slot_mask].groupby(
                ["ticker", "period_ending"], dropna=False,
            )["source"].nunique()
            offending_keys = slots[slots > 1].index
            if len(offending_keys) > 0:
                key_pairs = set(offending_keys.tolist())
                row_mask = pd.Series(
                    [(t, p) in key_pairs
                     for t, p in zip(history_df["ticker"],
                                     history_df["period_ending"])],
                    index=history_df.index,
                )
                findings.append(IntegrityFinding(
                    check="cross_source_slot_overlap",
                    severity="warning",
                    affected_rows=int(row_mask.sum()),
                    sample=_sample_rows(history_df.loc[row_mask]),
                    auto_fixable=True,
                    description=(
                        "Two or more sources hold rows for the same "
                        "(ticker, period_ending). Auto-fix re-runs the "
                        "per-slot source-priority dedup "
                        "(finviz > zacks > finnhub) and writes the "
                        "winners back."
                    ),
                ))

    # 10 — calendar-vs-fiscal phantom duplicates. Finnhub stores a quarter
    # under its containing CALENDAR quarter (calendar-normed period_ending,
    # for every finnhub row regardless of report_date_proxy), so the same
    # event lands at a different period_ending than the finviz/zacks
    # fiscal-end row and the per-slot dedup (check #9) can't see it. Flag
    # finnhub rows whose calendar quarter is already covered by a
    # fiscal-accurate (finviz/zacks) row. Auto-fix re-runs dedupe_history,
    # which drops them.
    cal_dups = _calendar_dup_drop_mask(history_df)
    if cal_dups.any():
        findings.append(IntegrityFinding(
            check="calendar_quarter_overlap",
            severity="warning",
            affected_rows=int(cal_dups.sum()),
            sample=_sample_rows(history_df.loc[cal_dups]),
            auto_fixable=True,
            description=(
                "Calendar-normed finnhub rows duplicate a finviz/zacks "
                "row in the same calendar quarter under a shifted "
                "period_ending (non-calendar fiscal year). Auto-fix drops "
                "the finnhub rows; the fiscal-accurate row is kept."
            ),
        ))

    return findings


def fix_integrity_issues(
    history_df: pd.DataFrame,
    findings: list[IntegrityFinding],
) -> tuple[pd.DataFrame, list[str]]:
    """Apply auto-fixes from a verify_integrity() result. Returns the
    fixed DataFrame plus a list of human-readable messages describing
    what was done. Non-fixable findings are reported but skipped."""
    df = history_df.copy()
    msgs: list[str] = []

    findings_by_check = {f.check: f for f in findings if f.auto_fixable}

    if "duplicate_pk" in findings_by_check:
        before = len(df)
        df = df.sort_values(
            by="updated_at" if "updated_at" in df.columns else "period_ending",
            na_position="first",
        )
        df = df.drop_duplicates(
            subset=["ticker", "period_ending", "source"], keep="last",
        )
        msgs.append(f"duplicate_pk: dropped {before - len(df)} duplicate rows")

    if "orphan_ticker" in findings_by_check:
        before = len(df)
        df = df.loc[df["ticker"].notna()
                    & (df["ticker"].astype(str).str.strip() != "")]
        msgs.append(f"orphan_ticker: dropped {before - len(df)} rows")

    if "orphan_period" in findings_by_check:
        before = len(df)
        df = df.loc[df["period_ending"].notna()]
        msgs.append(f"orphan_period: dropped {before - len(df)} rows")

    if "null_source" in findings_by_check:
        mask = df["source"].isna() | (df["source"].astype(str).str.strip() == "")
        df.loc[mask, "source"] = "legacy"
        msgs.append(f"null_source: stamped {mask.sum()} rows as 'legacy'")

    if "proxy_dtype_drift" in findings_by_check:
        col = df["report_date_proxy"]
        df["report_date_proxy"] = col.where(col.notna(), False).astype(bool)
        msgs.append("proxy_dtype_drift: coerced report_date_proxy to bool")

    if "rev_column_dtype" in findings_by_check:
        coerced = []
        for c in ("estimated_rev", "reported_rev",
                  "surprise_rev", "surprise_rev_pct"):
            if c in df.columns and df[c].dtype == object:
                df[c] = pd.to_numeric(df[c], errors="coerce")
                coerced.append(c)
        msgs.append(
            f"rev_column_dtype: coerced {', '.join(coerced)} to float"
        )

    if "period_predates_cap" in findings_by_check:
        before = len(df)
        cutoff = pd.Timestamp.today().normalize() - pd.DateOffset(
            years=config.EARNINGS_HISTORY_YEARS,
        )
        df = df.loc[
            pd.to_datetime(df["period_ending"], errors="coerce") >= cutoff
        ]
        msgs.append(f"period_predates_cap: dropped {before - len(df)} rows")

    # Both overlap checks are resolved by re-running dedupe_history (which
    # applies per-slot priority dedup AND the calendar-quarter collapse).
    # Run it once if either fired and report each separately.
    if ("cross_source_slot_overlap" in findings_by_check
            or "calendar_quarter_overlap" in findings_by_check):
        before = len(df)
        df = dedupe_history(df)
        dropped = before - len(df)
        if "cross_source_slot_overlap" in findings_by_check:
            msgs.append(
                f"cross_source_slot_overlap: per-slot priority dedup + "
                f"calendar-quarter collapse dropped {dropped} "
                f"redundant rows"
            )
        if "calendar_quarter_overlap" in findings_by_check:
            msgs.append(
                f"calendar_quarter_overlap: dropped calendar-vs-fiscal "
                f"phantom finnhub rows (re-dedup removed {dropped} rows "
                f"total)"
            )

    # Surface non-fixable findings so the caller can show them in the UI.
    for f in findings:
        if not f.auto_fixable:
            msgs.append(
                f"{f.check}: NOT auto-fixable — {f.description} "
                f"({f.affected_rows} rows/cols affected)"
            )

    return df.reset_index(drop=True), msgs


# ──────────────────────────────────────────────────────────────────────
# Earnings-dates reconciliation (Zacks-only — Phase 4 will supersede)
# ──────────────────────────────────────────────────────────────────────

def _update_earnings_dates_for_tickers(
    tickers: list[str], history_df: pd.DataFrame, *, today: Optional[pd.Timestamp] = None,
) -> None:
    """Backward-compat shim for the Phase 3 helper.

    Phase 4 added the full Zacks-primary + Yahoo-augmentation reconciler
    (earnings_reconcile.reconcile_earnings_dates). This thin shim
    delegates to it, passing the caller's in-memory history_df through
    so the reconciler doesn't need to round-trip through disk for callers
    that haven't saved yet (notably Phase 3 tests).
    """
    # Lazy import: earnings_reconcile imports this module, so a top-level
    # import would create a cycle.
    from . import earnings_reconcile
    earnings_reconcile.reconcile_earnings_dates(
        affected_tickers=list(tickers),
        today=today,
        history_df=history_df,
    )


# ──────────────────────────────────────────────────────────────────────
# Bulk + targeted fills via Zacks
# ──────────────────────────────────────────────────────────────────────

def _row_to_history_dict(row: dict, ticker: str, source: str, now: datetime) -> dict:
    """Convert one zacks_scraper row dict into an earnings_history row.

    Zacks always supplies real announcement dates so ``report_date_proxy``
    is False here. Finnhub-fed rows (Phase 2) will set this True when
    falling back to period_ending.
    """
    out = {
        "ticker": ticker,
        "source": source,
        "updated_at": now,
        "report_date_proxy": False,
    }
    for col in (
        "period_ending", "report_date", "report_time",
        "estimated_eps", "reported_eps", "surprise_eps", "surprise_eps_pct",
        "estimated_rev", "reported_rev", "surprise_rev", "surprise_rev_pct",
    ):
        out[col] = row.get(col)
    return out


def _flush_pending_to_disk(
    pending: dict[str, list[dict]],
    affected_tickers_total: list[str],
    *,
    is_final: bool = False,
    source: str = "zacks",
) -> None:
    """Merge `pending` (ticker → list of row dicts) into the on-disk
    earnings_history.parquet — replacing only the **(ticker, source)**
    rows for tickers in `pending`. Phase 2 changed this from "replace
    by ticker" so Zacks and Finnhub rows for the same ticker can
    coexist on disk.

    Per audit H3, the per-flush reconcile of `earnings_dates.parquet`
    has been dropped. The ZacksFillWorker now runs a single reconcile
    after `_fill_via_zacks` exits (see `_finalize_fill`). Per-flush
    sorting is also skipped (audit L8); a single sorted save fires at
    end-of-fill via `is_final=True` here.

    Args:
        pending: ticker → list of row dicts to merge.
        affected_tickers_total: running list of every ticker touched
            across the fill (kept for caller bookkeeping; not used here).
        is_final: when True, sorts the on-disk parquet canonically.
        source: "zacks" (default — Zacks fill is the only caller in this
            module). The Finnhub fill in finnhub_fill.py has its own
            flush helper; both honor the (ticker, source) soft-PK.
    """
    if not pending:
        return

    # Serialize the load → merge → save cycle across all fill workers
    # (Zacks / Finnhub). Without this, two concurrent flushes
    # each load the same snapshot, each append their rows, and the
    # second writer wipes the first writer's appended rows. The lock
    # is re-entrant so callers that wrap multiple flush calls in their
    # own critical section won't self-deadlock.
    with HISTORY_WRITE_LOCK:
        existing = load_earnings_history()
        new_rows: list[dict] = []
        for rows in pending.values():
            new_rows.extend(rows)
        new_df = pd.DataFrame(new_rows, columns=COLUMNS)
        # Ingest-time price-relative EPS artifact guard (reverse-split
        # nano-caps). Catches the $20-$100k band the absolute write-guard
        # misses, at the moment rows arrive.
        new_df = sanitize_eps_artifacts(new_df)

        if existing is not None and not existing.empty:
            # Drop only the (ticker, source) rows we're replacing. Other-
            # source rows for these tickers stay. Other tickers stay.
            # Phase 6.5 fix: key the replacement on new_df["ticker"] (the
            # actual values being written) rather than pending.keys() (the
            # queried symbol). They normally agree for Zacks, but using
            # the row's own ticker is the robust invariant.
            new_tickers = set(new_df["ticker"].dropna().astype(str).unique())
            mask_replace = (
                existing["ticker"].astype(str).isin(new_tickers)
                & (existing["source"] == source)
            )
            keep = existing.loc[~mask_replace]
            combined = pd.concat([keep, new_df], ignore_index=True)
        else:
            combined = new_df

        save_earnings_history(combined, sort=is_final)


def _finalize_fill(affected_tickers: list[str]) -> None:
    """End-of-fill cleanup (audit H3 + L8):
      1. Re-load + re-save with sort=True so the on-disk parquet is
         canonical (ticker ASC, period_ending DESC).
      2. Run a single reconcile_earnings_dates against every ticker
         touched during the fill.

    Skipping in-loop reconciles trades freshness during the fill (the
    Days-Since/Days-Until ER filters can be up to one fill stale while
    a multi-hour run is in flight) for orders-of-magnitude less I/O.
    """
    if affected_tickers:
        # Serialize the read→recompute→write against concurrent fills — the
        # matching per-source finalizers (finviz_fill / finnhub_fill) take the
        # same re-entrant lock so no finalize clobbers another worker's rows.
        with HISTORY_WRITE_LOCK:
            existing = load_earnings_history()
            if existing is not None and not existing.empty:
                # Refresh YoY columns across the WHOLE parquet (cheap — pure
                # in-memory groupby) so any newly-arrived prior-year row
                # back-fills its current-year counterpart's yoy_*_pct.
                existing = compute_yoy_columns(existing)
                save_earnings_history(existing, sort=True)
        from . import earnings_reconcile  # lazy: cycle-safe
        earnings_reconcile.reconcile_earnings_dates(
            affected_tickers=list(set(affected_tickers))
        )


def _fill_via_zacks(
    tickers: list[str], blacklist: set[str],
    *,
    progress_cb=None,
    stop_flag: Optional[list[bool]] = None,
    delay_sec: float = 1.5,
    flush_every: int = 25,
    years: Optional[int] = None,
    label: str = "Zacks fill",
    consec_error_limit: int = 5,
    on_block_callback=None,
    failed_cb=None,
) -> tuple[int, int]:
    """Common loop body for both bulk_fill_zacks and targeted_fill_zacks.
    Walks `tickers`, fetches each via a single shared ZacksSession,
    flushes to disk every `flush_every` successful pulls so a long run
    that's interrupted doesn't lose progress.

    Imperva auto-pause: when `consec_error_limit` consecutive failures
    occur with `on_block_callback` configured, invokes the callback —
    which is expected to block the worker thread until the user either
    refreshes cookies (returns "continue") or aborts (returns "stop").
    The callable receives `(consec_count, session)`. When it returns
    "continue", the loop *rewinds to the first ticker in the failure
    window* and retries the entire window — every ticker that failed
    during the block almost certainly failed for the block, not for
    its own sake. "stop" exits the loop cleanly.
    """
    if years is None:
        # Resolved at CALL time (None sentinel, not a def-time default) so a
        # live Settings → Advanced… change to the earnings-history depth
        # applies without a restart.
        years = config.EARNINGS_HISTORY_YEARS
    work = [t for t in tickers if t not in blacklist]
    if not work:
        log.info("%s: no tickers to process", label)
        return 0, 0

    # Audit M5: a non-positive consec_error_limit would fire the block
    # callback every iteration. Clamp defensively so a bad caller can't
    # turn the loop into a modal storm.
    consec_error_limit = max(1, int(consec_error_limit))

    log.info("%s: %d tickers to process", label, len(work))

    # Cap consumer rows on period_ending, same as finviz/finnhub, so the
    # per-(ticker, period_ending) dedup sees an identical date window across
    # sources (the Zacks scraper bounds by `years` on the report date, which
    # is ~one quarter off the period_ending edge). Raw capture below stays
    # full for replay.
    cutoff = pd.Timestamp.today().normalize() - pd.DateOffset(years=years)

    pending: dict[str, list[dict]] = {}
    raw_pending: list[dict] = []
    run_id = earnings_raw.new_run_id()
    affected_total: list[str] = []
    filled = 0
    errors = 0
    consec_errors = 0
    total = len(work)
    # Parse-failure spike alarm (B2): fetch attempts vs. parse_error
    # classifications across the run — see the halt check in the loop.
    spike_attempts = 0
    spike_parse_fails = 0

    def _flush_raw():
        if not raw_pending:
            return
        try:
            earnings_raw.append_zacks_rows(raw_pending, run_id)
        except Exception as exc:
            log.warning("Zacks raw-layer write failed: %s", exc)
        raw_pending.clear()

    with ZacksSession() as session:
        i = 0
        while i < total:
            if stop_flag and stop_flag[0]:
                log.info("%s: stopped at %d/%d", label, i, total)
                break

            sym = work[i]
            try:
                rows = session.fetch(sym, years=years)
            except Exception as exc:
                log.debug("[%s] unexpected exception: %s", sym, exc)
                rows = None
            spike_attempts += 1

            if rows is None or len(rows) == 0:
                errors += 1
                if rows is None and session.last_failure_kind == FAIL_PARSE_ERROR:
                    spike_parse_fails += 1
                # Audit M1: only confirmed Imperva blocks advance the
                # auto-pause counter. "ticker not on Zacks" / parse
                # errors / network glitches reset it so a long alphabetical
                # tail of small-caps Zacks doesn't cover can't falsely
                # pop the cookie-refresh dialog.
                if session.last_failure_kind == FAIL_BLOCKED:
                    consec_errors += 1
                else:
                    consec_errors = 0
                # Surface the failure + its classification so callers
                # can show a per-ticker breakdown at end of run (ETFs
                # vs Imperva blocks vs network errors). `last_failure_kind`
                # may be None for unexpected exceptions; we tag those
                # as "unknown" so the caller can still see them.
                if failed_cb is not None:
                    kind = session.last_failure_kind or "unknown"
                    try:
                        failed_cb(sym, kind)
                    except Exception:
                        pass  # never let failed_cb crash the fill
            else:
                # Audit M2: stamp `updated_at` at fetch time, not at
                # run start. A multi-hour bulk fill otherwise marks
                # every row with the run-start timestamp, which the
                # smart-refresh staleness rules later misinterpret.
                fetch_now = datetime.now()
                hist_rows = [
                    _row_to_history_dict(r, sym, "zacks", fetch_now) for r in rows
                ]
                # Drop rows whose period_ending predates the history cap so
                # the on-disk window matches finviz/finnhub (period_ending
                # based). NaT period_ending is kept here — save_earnings_history
                # drops it later, and the failure mode for an unparseable date
                # shouldn't be a silent extra drop in this path.
                pending[sym] = [
                    h for h in hist_rows
                    if pd.isna(pd.to_datetime(h.get("period_ending"),
                                              errors="coerce"))
                    or pd.to_datetime(h["period_ending"]) >= cutoff
                ]
                # Raw capture: original Zacks row dicts plus ticker (FULL,
                # uncapped — preserves replay depth). earnings_raw stamps
                # fetched_at + run_id automatically.
                for r in rows:
                    raw_pending.append({"ticker": sym, **r})
                affected_total.append(sym)
                filled += 1
                consec_errors = 0

            if progress_cb:
                progress_cb(i + 1, total)

            # Parse-failure spike alarm (B2): a high fraction of
            # parse_error classifications means Zacks changed the page
            # format (a parser break on OUR side), not that N tickers
            # went bad. Halt loudly instead of churning the rest of the
            # run. parse_error tickers land in their own failed_cb
            # bucket — NOT the not_found bucket the GUI auto-blacklists
            # — so a parser break can never poison the skip list.
            # Thresholds read at call time so overrides apply mid-run.
            if (spike_attempts >= config.PARSE_SPIKE_MIN_SAMPLE
                    and spike_parse_fails * 100.0
                    >= config.PARSE_SPIKE_FAIL_PCT * spike_attempts):
                log.error(
                    "%s: PARSE-FAILURE SPIKE — %d of %d fetches (%.0f%%) "
                    "were parse errors; HALTING the run (Zacks page format "
                    "has likely changed; affected tickers were NOT "
                    "blacklisted)",
                    label, spike_parse_fails, spike_attempts,
                    spike_parse_fails * 100.0 / spike_attempts,
                )
                break

            if len(pending) >= flush_every:
                # Audit L8: skip the per-flush sort; one final sorted
                # save fires in `_finalize_fill` after the loop exits.
                _flush_pending_to_disk(pending, affected_total)
                _flush_raw()
                log.info(
                    "%s: flushed %d ticker(s) (%d/%d processed, "
                    "%d filled, %d errors so far)",
                    label, len(pending), i + 1, total, filled, errors,
                )
                pending = {}

            if (i + 1) % 200 == 0:
                log.info("%s: %d/%d processed (%d filled, %d errors)",
                         label, i + 1, total, filled, errors)

            # Imperva block heuristic: N misses in a row probably means a
            # cookie/IP-reputation block, not N delisted tickers. Pause
            # for cookie refresh before chewing through the rest.
            if (on_block_callback is not None
                    and consec_errors >= consec_error_limit):
                log.warning(
                    "%s: %d consecutive failures — invoking block callback",
                    label, consec_errors,
                )
                decision = on_block_callback(consec_errors, session)
                if decision == "stop":
                    log.info("%s: block callback returned 'stop' at %d/%d",
                             label, i + 1, total)
                    break
                # "continue" → rewind to the first ticker in the
                # consecutive-failure window. Every ticker in that
                # window almost certainly failed for the block (not for
                # its own sake), so they all need a retry now that
                # cookies are fresh.
                rewind = consec_errors
                errors = max(0, errors - rewind)
                consec_errors = 0
                i = max(0, i - (rewind - 1))
                log.info("%s: rewinding %d ticker(s) to retry block window (i=%d)",
                         label, rewind, i)
                if progress_cb:
                    progress_cb(i, total)
                # Audit M3: keep the standard pacing delay BEFORE the
                # retry attempt so we don't hammer Zacks back-to-back
                # under fresh cookies and re-trigger the same block.
                time.sleep(delay_sec)
                continue  # re-enter loop at rewound i (no increment)

            time.sleep(delay_sec)
            i += 1

    if pending:
        _flush_pending_to_disk(pending, affected_total)
    _flush_raw()

    # Audit H3 + L8: single sorted save + single reconcile at end of fill,
    # rather than per-flush. The Days-Since / Days-Until ER filters may
    # see slightly stale data while a fill is mid-run; the daily auto
    # refresh recovers it next launch.
    _finalize_fill(affected_total)

    log.info("%s done: %d filled, %d errors", label, filled, errors)
    return filled, errors


def bulk_fill_zacks(
    universe_symbols: list[str], blacklist: set[str],
    *,
    progress_cb=None,
    stop_flag: Optional[list[bool]] = None,
    delay_sec: float = 1.5,
    flush_every: int = 25,
    years: Optional[int] = None,  # None → config.EARNINGS_HISTORY_YEARS at call time
    on_block_callback=None,
    consec_error_limit: int = 5,
    failed_cb=None,
) -> tuple[int, int]:
    """Iterate every ticker in the universe and pull `years` of earnings
    history from Zacks. Returns (filled, errors). Long-running — at the
    1.5s default pacing this is ~6.5 hours for a 15k-ticker universe.
    Use the per-flush save so an interrupted run doesn't lose the
    quarters it already pulled.

    `on_block_callback`: optional Imperva auto-pause hook (see
    `_fill_via_zacks` for semantics).
    `failed_cb`: optional callable `(symbol: str, kind: str)` invoked
    once per failed ticker. `kind` is one of the FAIL_* sentinels
    (blocked / not_found / http_error / parse_error / unknown) so the
    caller can break down the failure list by cause."""
    return _fill_via_zacks(
        universe_symbols, blacklist,
        progress_cb=progress_cb, stop_flag=stop_flag,
        delay_sec=delay_sec, flush_every=flush_every, years=years,
        label="Zacks bulk fill",
        consec_error_limit=consec_error_limit,
        on_block_callback=on_block_callback,
        failed_cb=failed_cb,
    )


def targeted_fill_zacks(
    gap_tickers: list[str], blacklist: set[str],
    *,
    progress_cb=None,
    stop_flag: Optional[list[bool]] = None,
    delay_sec: float = 1.5,
    flush_every: int = 25,
    years: Optional[int] = None,  # None → config.EARNINGS_HISTORY_YEARS at call time
    on_block_callback=None,
    consec_error_limit: int = 5,
    failed_cb=None,
) -> tuple[int, int]:
    """Iterate only the provided gap_tickers list. The caller computes
    gaps as `universe ∩ (not blacklist)` minus the unique tickers in
    earnings_history.parquet.

    `failed_cb`: optional `(symbol, kind)` callback for per-ticker
    failure classification — see bulk_fill_zacks for details."""
    return _fill_via_zacks(
        gap_tickers, blacklist,
        progress_cb=progress_cb, stop_flag=stop_flag,
        delay_sec=delay_sec, flush_every=flush_every, years=years,
        label="Zacks targeted fill",
        consec_error_limit=consec_error_limit,
        on_block_callback=on_block_callback,
        failed_cb=failed_cb,
    )


def find_gap_tickers(
    universe_symbols: list[str], blacklist: set[str],
) -> list[str]:
    """Return tickers in `universe ∩ (not blacklist)` that have NO rows
    in earnings_history.parquet. Helper for the targeted-fill menu
    handler."""
    have: set[str] = set()
    df = load_earnings_history()
    if df is not None and not df.empty:
        have = set(df["ticker"].astype(str).unique())
    return [t for t in universe_symbols if t not in blacklist and t not in have]


# ──────────────────────────────────────────────────────────────────────
# Daily smart-refresh candidate selection (Phase 5 §5.1)
# ──────────────────────────────────────────────────────────────────────

def find_smart_refresh_candidates(
    universe_symbols: list[str],
    blacklist: set[str],
    *,
    today: Optional[pd.Timestamp] = None,
    history_df: Optional[pd.DataFrame] = None,
    dates_df: Optional[pd.DataFrame] = None,
) -> list[str]:
    """Return the subset of `universe_symbols` that look "earnings stale"
    — likely to have a reported quarter we haven't captured yet — and so
    should be re-pulled. Source-neutral: the same candidate list drives
    the finviz / zacks / finnhub refresh (a per-ticker decision; whichever
    source lands the quarter resolves it via the priority dedup).

    A ticker is a candidate iff ANY of:

      A. It has no rows in earnings_history.parquet at all (gap fill).
      B. The earnings calendar's `last_earnings` (most-recent PAST report
         date) is NEWER than the most-recent report we've actually
         captured — i.e., a quarter was reported after our latest stored
         report, so we're behind.
      C. The ticker has no `last_earnings` in the calendar at all (nothing
         to reason about) AND our most-recent captured report is more than
         EARNINGS_REFRESH_NOCAL_STALE_DAYS (default 90) days old.

    Re-poll guard (applies to B and C, not A): a ticker whose last fetch
    (`updated_at`) is within EARNINGS_REFRESH_RECHECK_GUARD_DAYS (default
    5) days is NOT re-queued even if it still looks stale. This bounds the
    daily re-poll on names the calendar says reported but no source
    actually carries yet — without it, Rule B would loop on them forever.

    Uncaptured-fresh bypass: the guard is SKIPPED for a Rule-B ticker whose
    `last_earnings` is within EARNINGS_REFRESH_UNCAPTURED_FRESH_DAYS
    (default 21) days — a recently-reported quarter we haven't captured.
    Sources often publish the actual a day or two after the announcement,
    so without the bypass a ticker fetched in that gap would sit uncaptured
    for a business week. The fresh window caps it: past that, a still-
    uncaptured report (likely uncoverable) falls back to the guarded cadence
    rather than churning every launch.

    Why `last_earnings` and not `next_earnings`: the reconcile clears any
    past `next_earnings` (the `> today` filter), so a stored next date is
    always in the future and can never read as "already happened."
    `last_earnings` is the past-event signal and is ~99% populated.

    "Captured" excludes future-dated rows (`report_date > today`) — e.g.
    Finnhub forward placeholders — so a proxy row can't mask staleness.

    Blacklisted tickers are always excluded.

    Args:
        universe_symbols: full ticker universe (typically all_syms).
        blacklist: set of tickers to skip outright.
        today: reference date for "stale" calcs. None → today's date.
        history_df / dates_df: optional in-memory overrides; mainly used
            by tests to avoid round-tripping through disk.

    Returns:
        Sorted list of candidate ticker symbols.
    """
    if today is None:
        today = pd.Timestamp.today().normalize()
    if history_df is None:
        history_df = load_earnings_history()
    if dates_df is None:
        # Lazy import to avoid pulling earnings_cache at module load
        from . import earnings_cache as ec
        dates_df = ec.load_earnings_cache()

    # Per-ticker: latest *real* captured report (future rows excluded) and
    # latest fetch time (for the re-poll guard).
    latest_capture: dict[str, pd.Timestamp] = {}
    latest_fetch: dict[str, pd.Timestamp] = {}
    have_history: set[str] = set()
    if history_df is not None and not history_df.empty:
        h = history_df
        have_history = set(h["ticker"].astype(str).unique())
        rd = pd.to_datetime(h["report_date"], errors="coerce")
        past = h.loc[rd <= today]
        if not past.empty:
            latest_capture = (
                past.groupby("ticker")["report_date"].max().to_dict()
            )
        if "updated_at" in h.columns:
            latest_fetch = (
                h.groupby("ticker")["updated_at"].max().to_dict()
            )

    last_earn: dict[str, pd.Timestamp] = {}
    if dates_df is not None and not dates_df.empty and "last_earnings" in dates_df.columns:
        # Audit L2: dict(zip(...)) is ~10× faster than iterrows().
        last_earn = {
            str(t): le
            for t, le in zip(dates_df["ticker"], dates_df["last_earnings"])
            if isinstance(t, str) and t
        }

    guard_cut = today - pd.Timedelta(days=config.EARNINGS_REFRESH_RECHECK_GUARD_DAYS)
    nocal_cut = today - pd.Timedelta(days=config.EARNINGS_REFRESH_NOCAL_STALE_DAYS)
    fresh_cut = today - pd.Timedelta(
        days=config.EARNINGS_REFRESH_UNCAPTURED_FRESH_DAYS
    )

    candidates: list[str] = []
    for t in universe_symbols:
        if not isinstance(t, str) or not t or t in blacklist:
            continue

        # Rule A — no earnings history at all
        if t not in have_history:
            candidates.append(t)
            continue

        captured = latest_capture.get(t)
        cap_known = captured is not None and not pd.isna(captured)

        le = last_earn.get(t)
        le_known = le is not None and not pd.isna(le)

        # Uncaptured-fresh: the calendar shows a report newer than anything
        # we've captured AND that report is recent (within the fresh window).
        # Sources often publish the actual a day or two after the
        # announcement, so for these we BYPASS the re-poll guard and retry
        # every launch until the actual lands — capped at the fresh window so
        # a permanently-uncoverable name (calendar date no source carries)
        # falls back to the guarded cadence instead of churning forever.
        uncaptured_fresh = (
            le_known
            and (not cap_known or le > captured)
            and le >= fresh_cut
        )

        # Re-poll guard — fetched too recently to expect anything new.
        # Skipped for an uncaptured-fresh report (see above).
        if not uncaptured_fresh:
            fetched = latest_fetch.get(t)
            if fetched is not None and not pd.isna(fetched) and fetched >= guard_cut:
                continue

        # Rule B — calendar reports a quarter newer than anything we hold.
        if le_known:
            if not cap_known or le > captured:
                candidates.append(t)
            continue

        # Rule C — no calendar event; re-check on a fixed quarterly cadence.
        if not cap_known or captured < nocal_cut:
            candidates.append(t)

    return sorted(set(candidates))


# ──────────────────────────────────────────────────────────────────────
# One-time gap-fill-dedup migration
# ──────────────────────────────────────────────────────────────────────

# Sentinel filename marking that the on-disk earnings_history.parquet
# has been re-deduped under the gap-fill policy. Resolved at call time
# (not import time) so tests can monkeypatch config.DATA_DIR.
_GAP_FILL_MIGRATION_FLAG_NAME = ".gap_fill_dedup_v1.done"


def _migration_flag_path() -> Path:
    return config.DATA_DIR / _GAP_FILL_MIGRATION_FLAG_NAME


def migrate_to_gap_fill_dedup(*, force: bool = False) -> tuple[int, int]:
    """Apply the gap-fill per-(ticker, period_ending) priority dedup to
    the on-disk parquet exactly once. Returns ``(rows_before, rows_after)``.

    Background: prior to the gap-fill rewrite, ``dedupe_history`` ran
    READ-SIDE in ``get_ticker_history`` and applied the binary
    ticker-level rule (drop all Finnhub rows when any Zacks row was
    present). Writes preserved both sources verbatim on disk. After the
    rewrite, dedup happens at WRITE time so the on-disk parquet is
    canonical — but the existing file still carries the pre-rewrite
    overlap. This migration runs once, re-dedups the on-disk file,
    and stamps a flag so subsequent launches no-op.

    Dropped rows are NOT gone forever — the ``earnings_raw/`` audit
    layer preserves every fetched row per source, and the next per-
    source fill replays cleanly.

    ``force=True`` bypasses the flag (useful for tests + the GUI's
    "Verify Integrity → Auto-fix" path).
    """
    flag_path = _migration_flag_path()
    if not force and flag_path.exists():
        return (0, 0)
    df = load_earnings_history()
    if df is None or df.empty:
        try:
            config.atomic_write_text(flag_path, "ok\n")
        except OSError as exc:
            log.debug("migration flag write failed: %s", exc)
        return (0, 0)

    before = len(df)
    cleaned = dedupe_history(df)
    after = len(cleaned)
    if after < before:
        save_earnings_history(cleaned, sort=True, dedup=False)  # already deduped
        log.info(
            "gap_fill_dedup migration: dropped %d lower-priority rows "
            "(%d → %d). Originals preserved in earnings_raw/.",
            before - after, before, after,
        )
    try:
        config.atomic_write_text(flag_path, "ok\n")
    except OSError as exc:
        log.debug("migration flag write failed: %s", exc)
    return (before, after)


# Sentinel marking that the on-disk parquet has had the calendar-vs-fiscal
# duplicate cleanup applied. Separate flag from the gap-fill dedup above so
# existing installs (whose gap-fill flag is already set) still run this
# cleanup exactly once. v2 re-keys the collapse from report_date_proxy to
# source (finnhub-vs-fiscal-accurate), catching non-proxy finnhub rows that
# the v1 proxy-only pass missed; bumping the version re-runs it once more.
_CAL_DEDUP_FLAG_NAME = ".calendar_dedup_v2.done"


def _calendar_migration_flag_path() -> Path:
    return config.DATA_DIR / _CAL_DEDUP_FLAG_NAME


def migrate_calendar_dedup(*, force: bool = False) -> tuple[int, int]:
    """Drop calendar-normed finnhub rows that duplicate a fiscal-accurate
    (finviz/zacks) row in the same CALENDAR quarter, exactly once. Returns
    ``(rows_before, rows_after)``.

    Background: finnhub stores a non-calendar fiscal quarter under the
    calendar-quarter end, while finviz/zacks store it under the true
    fiscal-quarter end. The same earnings event therefore lands at two
    different ``period_ending`` values, so the per-(ticker, period_ending)
    dedup never collapsed them and the on-disk parquet accumulated phantom
    duplicate quarters (e.g. BBCP showed 8 quarters for the last year
    instead of 4). This holds for EVERY finnhub row, not just
    ``report_date_proxy=True`` ones. ``dedupe_history`` now drops the
    covered finnhub rows (see ``_calendar_dup_drop_mask``); this migration
    applies that cleanup to the existing on-disk file one time.

    Dropped rows are NOT lost — the ``earnings_raw/`` audit layer keeps
    every fetched finnhub record, and a future fill replays cleanly.

    ``force=True`` bypasses the flag (used by tests + the GUI's "Verify
    Integrity → Auto-fix" path).
    """
    flag_path = _calendar_migration_flag_path()
    if not force and flag_path.exists():
        return (0, 0)
    df = load_earnings_history()
    if df is None or df.empty:
        try:
            config.atomic_write_text(flag_path, "ok\n")
        except OSError as exc:
            log.debug("calendar_dedup flag write failed: %s", exc)
        return (0, 0)

    before = len(df)
    cleaned = dedupe_history(df)
    after = len(cleaned)
    if after < before:
        # Row set changed → refresh YoY so any row whose prior-year match
        # was a now-dropped duplicate is recomputed against what remains.
        cleaned = compute_yoy_columns(cleaned)
        save_earnings_history(cleaned, sort=True, dedup=False)  # already deduped
        log.info(
            "calendar_dedup migration: dropped %d calendar-vs-fiscal "
            "phantom rows (%d → %d). Originals preserved in earnings_raw/.",
            before - after, before, after,
        )
    try:
        config.atomic_write_text(flag_path, "ok\n")
    except OSError as exc:
        log.debug("calendar_dedup flag write failed: %s", exc)
    return (before, after)


# Sentinel marking that the on-disk parquet has been backfilled with the
# deeper finviz history already sitting in the raw layer (recovered when the
# cap was raised 5y → 10y). Network-free; reuses the raw audit layer.
_FINVIZ_BACKFILL_FLAG_NAME = ".finviz_backfill_v1.done"


def _finviz_backfill_flag_path() -> Path:
    return config.DATA_DIR / _FINVIZ_BACKFILL_FLAG_NAME


def migrate_backfill_finviz_history_from_raw(*, force: bool = False) -> tuple[int, int]:
    """One-time backfill of deeper finviz history from the raw audit layer
    into the consumer parquet, exactly once. Returns ``(rows_before,
    rows_after)``.

    Background: ``EARNINGS_HISTORY_YEARS`` was raised 5 → 10, but the
    on-disk consumer parquet was written under the old 5y cap so its finviz
    history is truncated on the old end. The finviz raw layer
    (``earnings_raw/finviz/``) preserves the FULL fetched history (~10y+),
    so we can recover the extra quarters WITHOUT re-scraping by replaying
    the raw rows through the exact production converter
    (``finviz_fill._record_to_history_dict``) at the new cutoff.

    Merge is add-and-dedup (not replace): existing rows are never dropped
    (so any consumer row whose raw file was pruned survives), the recovered
    older quarters are added, and ``dedupe_history`` collapses overlaps
    (finviz wins per slot; calendar-vs-fiscal collapse still applies).
    ``updated_at`` is preserved from each raw row's ``fetched_at`` so the
    smart-refresh staleness logic isn't reset by the backfill.

    ``force=True`` bypasses the flag (tests / manual re-run).
    """
    flag_path = _finviz_backfill_flag_path()
    if not force and flag_path.exists():
        return (0, 0)

    from . import earnings_raw, finviz_fill  # lazy: finviz_fill imports us

    existing = load_earnings_history()
    before = 0 if existing is None else len(existing)

    try:
        raw = earnings_raw.read_raw(config.RAW_SOURCE_FINVIZ)
    except Exception as exc:
        log.warning("finviz_backfill: raw read failed: %s", exc)
        raw = None
    if raw is None or raw.empty:
        try:
            config.atomic_write_text(flag_path, "ok\n")
        except OSError as exc:
            log.debug("finviz_backfill flag write failed: %s", exc)
        return (before, before)

    # Latest fetch wins per (symbol, fiscal_end_date) so a re-fetched
    # quarter uses its newest values. NB: column name must be a valid
    # identifier (no leading underscore) so DataFrame.itertuples exposes it
    # as an attribute rather than a positional rename.
    raw = raw.copy()
    raw["fa_ts"] = pd.to_datetime(raw.get("fetched_at"), errors="coerce")
    raw = raw.sort_values("fa_ts").drop_duplicates(
        subset=["symbol", "fiscal_end_date"], keep="last",
    )

    cutoff = pd.Timestamp.today().normalize() - pd.DateOffset(
        years=config.EARNINGS_HISTORY_YEARS,
    )
    now = datetime.now()

    def _nn(v):
        # The raw parquet stores missing values as NaN/NaT, but
        # _record_to_history_dict (built for the finviz API shape) tests
        # ``is None`` — and ``NaN is None`` is False, so a NaN epsActual
        # would slip past its forward-estimate filter and write a
        # reported_eps=NaN row. Convert NaN/NaT → None so the converter
        # filters estimate-only rows exactly like the live fetch path.
        try:
            return None if pd.isna(v) else v
        except (TypeError, ValueError):
            return v

    rebuilt: list[dict] = []
    for r in raw.itertuples(index=False):
        entry = {
            "epsActual": _nn(getattr(r, "eps_actual", None)),
            "epsEstimate": _nn(getattr(r, "eps_estimate", None)),
            "salesActual": _nn(getattr(r, "sales_actual", None)),
            "salesEstimate": _nn(getattr(r, "sales_estimate", None)),
            "earningsDate": _nn(getattr(r, "earnings_date", None)),
            "fiscalEndDate": _nn(getattr(r, "fiscal_end_date", None)),
        }
        hd = finviz_fill._record_to_history_dict(
            entry, queried_symbol=str(getattr(r, "symbol", "") or ""),
            cutoff=cutoff, now=now,
        )
        if hd is None:
            continue
        fa = getattr(r, "fa_ts", None)
        if fa is not None and pd.notna(fa):
            hd["updated_at"] = fa  # preserve real fetch time (staleness)
        rebuilt.append(hd)

    if not rebuilt:
        try:
            config.atomic_write_text(flag_path, "ok\n")
        except OSError as exc:
            log.debug("finviz_backfill flag write failed: %s", exc)
        return (before, before)

    new_df = pd.DataFrame(rebuilt, columns=COLUMNS)
    if existing is not None and not existing.empty:
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df
    combined = dedupe_history(combined)
    combined = compute_yoy_columns(combined)
    after = len(combined)
    save_earnings_history(combined, sort=True, dedup=False)  # already deduped
    log.info(
        "finviz_backfill migration: %d → %d rows (+%d recovered from raw "
        "at the %dy cap).",
        before, after, after - before, config.EARNINGS_HISTORY_YEARS,
    )
    try:
        config.atomic_write_text(flag_path, "ok\n")
    except OSError as exc:
        log.debug("finviz_backfill flag write failed: %s", exc)
    return (before, after)


# Sentinel marking that the on-disk parquet has had the price-relative EPS
# artifact sanitization applied once.
_EPS_SANITIZE_FLAG_NAME = ".eps_sanitize_v1.done"


def _eps_sanitize_flag_path() -> Path:
    return config.DATA_DIR / _EPS_SANITIZE_FLAG_NAME


def _load_current_prices(tickers) -> dict:
    """Return ``{ticker: latest close}`` from the per-ticker OHLCV cache
    (``config.PARQUET_DIR``) for the given tickers. Missing / unreadable /
    empty parquets are omitted. Reads only the Close column for speed."""
    prices: dict = {}
    for t in tickers:
        path = config.PARQUET_DIR / f"{t}.parquet"
        if not path.exists():
            continue
        try:
            d = pd.read_parquet(path, columns=["Close"])
        except Exception:
            try:
                d = pd.read_parquet(path)
            except Exception:
                continue
        if "Close" not in d.columns or d.empty:
            continue
        close = pd.to_numeric(d["Close"], errors="coerce").dropna()
        if not close.empty:
            prices[str(t)] = float(close.iloc[-1])
    return prices


def migrate_sanitize_absurd_eps(*, force: bool = False) -> tuple[int, int]:
    """Null EPS fields on rows whose ``reported_eps`` is a reverse-split
    adjustment artifact, once (price-relative). Returns
    ``(rows_nulled, candidate_tickers_priced)``.

    Background: heavily-reverse-split nano-caps store nonsensical per-share
    EPS (observed up to ~-4e11/share) from both finviz and zacks — split-
    adjustment artifacts, not a source-correctness issue. The absolute write
    guard in ``save_earnings_history`` catches the impossible-magnitude ones,
    but the precise filter is price-relative (``_implausible_eps_mask`` with
    the current close): an |EPS| far exceeding the share price can't be real.
    This migration loads the current close for candidate tickers (any
    |reported_eps| > a low pre-screen, to bound OHLCV reads) and nulls the
    EPS columns on artifact rows. Revenue + dates are kept.

    Dropped EPS values are recoverable from ``earnings_raw/`` if the policy
    changes. ``force=True`` bypasses the flag (tests / manual re-run).
    """
    flag_path = _eps_sanitize_flag_path()
    if not force and flag_path.exists():
        return (0, 0)
    df = load_earnings_history()
    if df is None or df.empty:
        try:
            config.atomic_write_text(flag_path, "ok\n")
        except OSError as exc:
            log.debug("eps_sanitize flag write failed: %s", exc)
        return (0, 0)

    # Pre-screen candidate tickers (any |reported_eps| above a low bar) so we
    # only read OHLCV for names that could possibly be artifacts. Legit EPS
    # rarely exceeds ~$20/share except high-priced stocks, which the price-
    # relative rule then spares. The absolute cap still fires on any row
    # regardless of whether its ticker got priced.
    ae = pd.to_numeric(df["reported_eps"], errors="coerce").abs()
    cand_tickers = sorted(set(df.loc[ae > 20, "ticker"].astype(str)))
    prices = _load_current_prices(cand_tickers)

    mask = _implausible_eps_mask(df, price_by_ticker=prices)
    n = int(mask.sum())
    if n:
        df = _null_eps_fields(df, mask)
        df = compute_yoy_columns(df)
        save_earnings_history(df, sort=True, dedup=False)
        log.info(
            "eps_sanitize migration: nulled EPS on %d reverse-split artifact "
            "row(s) across %d candidate ticker(s).", n, len(cand_tickers),
        )
    try:
        config.atomic_write_text(flag_path, "ok\n")
    except OSError as exc:
        log.debug("eps_sanitize flag write failed: %s", exc)
    return (n, len(cand_tickers))
