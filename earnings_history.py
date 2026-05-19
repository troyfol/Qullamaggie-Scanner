"""Per-quarter earnings history data layer (Zacks-fork addition).

Stores **full quarterly history** (EPS + revenue actuals + estimates +
surprises) for every ticker the user has fetched. Mirrors the
earnings_cache.py pattern but with a much wider per-row schema.

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
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd

from . import config
from . import earnings_raw
from .zacks_scraper import FAIL_BLOCKED, ZacksSession

log = logging.getLogger("scanner.earnings_history")

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
    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        log.warning("Failed to read earnings_history.parquet: %s", exc)
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


def save_earnings_history(df: pd.DataFrame, *, sort: bool = True) -> None:
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
    """
    if df is None or df.empty:
        return
    out = df.copy()
    # Coerce dtypes consistently
    for col in ("period_ending", "report_date", "updated_at"):
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce")
    # Drop rows that can't be addressed
    out = out.dropna(subset=["ticker", "period_ending"]).reset_index(drop=True)
    if sort:
        out = out.sort_values(["ticker", "period_ending"], ascending=[True, False])
    # Ensure the canonical column order even if caller passed extras
    keep = [c for c in COLUMNS if c in out.columns]
    out = out[keep + [c for c in out.columns if c not in keep]]
    config.atomic_write_parquet(
        out, config.EARNINGS_HISTORY_PARQUET, engine="pyarrow", index=False,
    )


# ──────────────────────────────────────────────────────────────────────
# Lookup helpers
# ──────────────────────────────────────────────────────────────────────

# Retained for callers that still import these — dedupe_history no
# longer uses them after the binary-policy rewrite, but external test
# fixtures and downstream code may reference them.
_SOURCE_PRIORITY = {"zacks": 0, "finnhub": 1}
_SOURCE_PRIORITY_FALLBACK = 99


def dedupe_history(history_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Binary source policy: per ticker, if Zacks supplies any row,
    drop ALL Finnhub rows for that ticker. After that, drop exact
    (ticker, period_ending) duplicates within whatever survives —
    keeping the most-recently-fetched row.

    Replaces the prior field-level coalesce (Phase 6.5). The semantic
    gap between Zacks adjusted EPS and Finnhub GAAP EPS makes per-field
    merging unsafe — you'd silently mix two definitions of "EPS" in a
    single row. Binary policy preserves source consistency at the cost
    of dropping Finnhub data on overlap (acceptable: Zacks is the
    authoritative source whenever it covers a ticker).

    Returns an empty COLUMNS-shaped frame on None / empty input.
    """
    if history_df is None or history_df.empty:
        return pd.DataFrame(columns=COLUMNS)
    df = history_df.copy()
    if "source" not in df.columns:
        # Pre-Phase-2 file with no source column — nothing to dedup on.
        return df.reset_index(drop=True)

    src = df["source"].astype(str)
    zacks_tickers = set(df.loc[src.str.contains("zacks", na=False), "ticker"].unique())
    if zacks_tickers:
        drop_mask = (
            src.str.contains("finnhub", na=False)
            & df["ticker"].isin(zacks_tickers)
        )
        df = df.loc[~drop_mask].copy()

    sort_cols = ["ticker", "period_ending"]
    if "updated_at" in df.columns:
        sort_cols.append("updated_at")
    df = (
        df.sort_values(sort_cols)
          .drop_duplicates(subset=["ticker", "period_ending"], keep="last")
          .reset_index(drop=True)
    )

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

    period_ts = pd.to_datetime(df["period_ending"], errors="coerce")

    import numpy as np
    yoy_eps = pd.Series(np.nan, index=df.index, dtype="float64")
    yoy_rev = pd.Series(np.nan, index=df.index, dtype="float64")

    # Per-ticker pass: build a period_ending → row-index map (most-recent
    # row when dups exist) then look up each row's prior-year counterpart.
    for ticker, idx in df.groupby("ticker", sort=False).groups.items():
        sub_periods = period_ts.loc[idx]
        # If multiple rows share the same period_ending (cross-source dup
        # pre-binary-policy or transient state), prefer the latest
        # `updated_at`. A simple stable last-wins is fine for our purposes.
        period_to_idx: dict[pd.Timestamp, int] = {}
        for i in idx:
            p = sub_periods.loc[i]
            if pd.isna(p):
                continue
            period_to_idx[p] = i  # last wins

        for i in idx:
            cur_p = sub_periods.loc[i]
            if pd.isna(cur_p):
                continue
            prior_p = cur_p - pd.DateOffset(years=1)
            prior_i = period_to_idx.get(prior_p)
            if prior_i is None:
                continue
            for src_col, dst_series in (
                ("reported_eps", yoy_eps),
                ("reported_rev", yoy_rev),
            ):
                if src_col not in df.columns:
                    continue
                cur_v = df.at[i, src_col]
                prior_v = df.at[prior_i, src_col]
                if pd.isna(cur_v) or pd.isna(prior_v):
                    continue
                try:
                    cur_f = float(cur_v)
                    prior_f = float(prior_v)
                except (TypeError, ValueError):
                    continue
                if prior_f == 0.0:
                    continue
                dst_series.at[i] = (cur_f - prior_f) / abs(prior_f) * 100.0

    df["yoy_eps_pct"] = pd.to_numeric(yoy_eps, errors="coerce")
    df["yoy_rev_pct"] = pd.to_numeric(yoy_rev, errors="coerce")
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

    Returns a dict shaped:

        {
          "total_universe":   int,
          "blacklisted":      int,
          "in_scope":         int,
          "zacks_only":       {"count": int, "tickers": list[str]},
          "finnhub_only":     {"count": int, "tickers": list[str]},
          "both":             {"count": int, "tickers": list[str]},
          "neither":          {"count": int, "tickers": list[str]},
          "most_recent_zacks_quarter":   pd.Timestamp | None,
          "most_recent_finnhub_quarter": pd.Timestamp | None,
        }

    Under the binary source policy (post-rewrite), every (ticker,
    source) is mutually exclusive — `both.count` should always be 0
    in practice. The substring matching on `source` is retained so the
    function remains correct against any historical files that still
    carry merged-source labels.
    """
    if history_df is None:
        history_df = load_earnings_history()

    in_scope = [t for t in universe_symbols if t and t not in blacklist]
    blacklisted = [t for t in universe_symbols if t and t in blacklist]

    have_zacks: set[str] = set()
    have_finnhub: set[str] = set()
    last_zacks: Optional[pd.Timestamp] = None
    last_finnhub: Optional[pd.Timestamp] = None

    if history_df is not None and not history_df.empty:
        # Per-row: figure out which source(s) contributed.
        srcs = history_df["source"].astype(str).fillna("")
        for src_label in srcs.unique():
            mask = srcs == src_label
            sub = history_df.loc[mask]
            tickers = set(sub["ticker"].astype(str).unique())
            if "zacks" in src_label:
                have_zacks |= tickers
                periods = pd.to_datetime(sub["period_ending"], errors="coerce")
                latest = periods.max()
                if pd.notna(latest):
                    last_zacks = (latest if last_zacks is None
                                  else max(last_zacks, latest))
            if "finnhub" in src_label:
                have_finnhub |= tickers
                periods = pd.to_datetime(sub["period_ending"], errors="coerce")
                latest = periods.max()
                if pd.notna(latest):
                    last_finnhub = (latest if last_finnhub is None
                                    else max(last_finnhub, latest))

    in_scope_set = set(in_scope)
    have_zacks &= in_scope_set
    have_finnhub &= in_scope_set
    both = have_zacks & have_finnhub
    zacks_only = have_zacks - have_finnhub
    finnhub_only = have_finnhub - have_zacks
    neither = in_scope_set - have_zacks - have_finnhub

    return {
        "total_universe":  len(universe_symbols),
        "blacklisted":     len(blacklisted),
        "in_scope":        len(in_scope),
        "zacks_only":      {"count": len(zacks_only),
                            "tickers": sorted(zacks_only)},
        "finnhub_only":    {"count": len(finnhub_only),
                            "tickers": sorted(finnhub_only)},
        "both":            {"count": len(both), "tickers": sorted(both)},
        "neither":         {"count": len(neither), "tickers": sorted(neither)},
        "most_recent_zacks_quarter":   last_zacks,
        "most_recent_finnhub_quarter": last_finnhub,
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

    # 9 — cross-source period overlap (binary policy violation)
    if (
        "source" in history_df.columns
        and "ticker" in history_df.columns
        and "period_ending" in history_df.columns
    ):
        src = history_df["source"].astype(str)
        zk_tickers = set(history_df.loc[src.str.contains("zacks", na=False), "ticker"].unique())
        violation_mask = (
            src.str.contains("finnhub", na=False)
            & history_df["ticker"].isin(zk_tickers)
        )
        if violation_mask.any():
            findings.append(IntegrityFinding(
                check="cross_source_period_overlap",
                severity="warning",
                affected_rows=int(violation_mask.sum()),
                sample=_sample_rows(history_df.loc[violation_mask]),
                auto_fixable=True,
                description=(
                    "Binary source policy violation: Finnhub rows exist "
                    "for tickers that also have Zacks coverage. Auto-fix "
                    "drops the Finnhub rows (Zacks is the authoritative "
                    "source whenever it covers a ticker)."
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

    if "cross_source_period_overlap" in findings_by_check:
        before = len(df)
        src = df["source"].astype(str)
        zk_tickers = set(df.loc[src.str.contains("zacks", na=False), "ticker"].unique())
        drop_mask = (
            src.str.contains("finnhub", na=False)
            & df["ticker"].isin(zk_tickers)
        )
        df = df.loc[~drop_mask]
        msgs.append(
            f"cross_source_period_overlap: dropped {before - len(df)} "
            f"finnhub rows on zacks-covered tickers"
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

    existing = load_earnings_history()
    new_rows: list[dict] = []
    for rows in pending.values():
        new_rows.extend(rows)
    new_df = pd.DataFrame(new_rows, columns=COLUMNS)

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
    years: int = config.EARNINGS_HISTORY_YEARS,
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
    work = [t for t in tickers if t not in blacklist]
    if not work:
        log.info("%s: no tickers to process", label)
        return 0, 0

    # Audit M5: a non-positive consec_error_limit would fire the block
    # callback every iteration. Clamp defensively so a bad caller can't
    # turn the loop into a modal storm.
    consec_error_limit = max(1, int(consec_error_limit))

    log.info("%s: %d tickers to process", label, len(work))

    pending: dict[str, list[dict]] = {}
    raw_pending: list[dict] = []
    run_id = earnings_raw.new_run_id()
    affected_total: list[str] = []
    filled = 0
    errors = 0
    consec_errors = 0
    total = len(work)

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

            if rows is None or len(rows) == 0:
                errors += 1
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
                pending[sym] = [
                    _row_to_history_dict(r, sym, "zacks", fetch_now) for r in rows
                ]
                # Raw capture: original Zacks row dicts plus ticker.
                # earnings_raw stamps fetched_at + run_id automatically.
                for r in rows:
                    raw_pending.append({"ticker": sym, **r})
                affected_total.append(sym)
                filled += 1
                consec_errors = 0

            if progress_cb:
                progress_cb(i + 1, total)

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
    years: int = config.EARNINGS_HISTORY_YEARS,
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
    years: int = config.EARNINGS_HISTORY_YEARS,
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
    """Return the subset of `universe_symbols` that should be re-pulled
    from Zacks during the daily smart refresh, per spec §5.1.

    A ticker is a candidate iff ANY of:

      A. It has no rows in earnings_history.parquet at all (gap fill).
      B. Its most-recent `report_date` is more than ZACKS_REFRESH_STALE_DAYS
         (default 95) days old AND its `next_earnings` from
         earnings_dates.parquet is on or before today + ZACKS_REFRESH_NEXT_RECENT_DAYS
         days (i.e., already happened or imminent — likely produced a new
         quarter we should capture).
      C. Its most-recent `report_date` is more than
         ZACKS_REFRESH_LONG_STALE_DAYS (default 100) days old AND
         `next_earnings` is missing entirely (no forward calendar to
         reason about — re-check on a long cadence).

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

    latest_report: dict[str, pd.Timestamp] = {}
    if history_df is not None and not history_df.empty:
        grouped = history_df.groupby("ticker")["report_date"].max()
        latest_report = grouped.to_dict()

    next_e: dict[str, pd.Timestamp] = {}
    if dates_df is not None and not dates_df.empty and "next_earnings" in dates_df.columns:
        # Audit L2: dict(zip(...)) is ~10× faster than iterrows() on a
        # 15k-row dates parquet at no readability cost.
        next_e = {
            str(t): ne
            for t, ne in zip(dates_df["ticker"], dates_df["next_earnings"])
            if isinstance(t, str) and t
        }

    stale_cut = today - pd.Timedelta(days=config.ZACKS_REFRESH_STALE_DAYS)
    long_stale_cut = today - pd.Timedelta(days=config.ZACKS_REFRESH_LONG_STALE_DAYS)
    next_recent_cut = today + pd.Timedelta(days=config.ZACKS_REFRESH_NEXT_RECENT_DAYS)

    candidates: list[str] = []
    for t in universe_symbols:
        if not isinstance(t, str) or not t or t in blacklist:
            continue

        last = latest_report.get(t)
        # Rule A — no Zacks history at all
        if last is None or pd.isna(last):
            candidates.append(t)
            continue

        ne = next_e.get(t)
        ne_known = ne is not None and not pd.isna(ne)

        # Rule B — recently-reported (>95d since last AND next_e <= today + 7d)
        if last < stale_cut and ne_known and ne <= next_recent_cut:
            candidates.append(t)
            continue

        # Rule C — long-stale and no forward calendar
        if last < long_stale_cut and not ne_known:
            candidates.append(t)
            continue

    return sorted(set(candidates))
