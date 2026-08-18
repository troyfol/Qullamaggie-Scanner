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
    find_cross_source_disagreements(df, ...) -> pd.DataFrame  (report-only)
    report_cross_source_disagreements(df)    -> pd.DataFrame  (+ CSV + log)

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

import numpy as np
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
    # Provenance of the two REPORTED actuals (audit 2026-08-16, F13).
    # `source` remains the row's winning source and is still the
    # (ticker, source) flush key; these say where `reported_eps` and
    # `reported_rev` actually came from, which can now differ from each other
    # and from `source`. NA when the row has no such value.
    #
    # The estimate / surprise columns need no provenance column of their own:
    # they are finviz-only by construction (see `strip_foreign_estimates`), so
    # a non-null value there is always finviz.
    #
    # DELIBERATELY APPENDED at the end rather than placed next to `source`:
    # the Morning Scanner repo reads this parquet, and appending leaves every
    # existing column at its existing position.
    "eps_source", "rev_source",
    # Why a row's EPS looked implausible, "" when it did not. Replaces the old
    # behaviour of NULLING those rows — see `flag_eps_basis`. Appended at the
    # end for the same Morning Scanner reason as the two above.
    "eps_flag",
]

# Reported actuals mix freely across sources, per column. All three quote the
# same adjusted / non-GAAP basis (the GAAP EDGAR source was removed
# 2026-05-31), so a quarter may legitimately carry a zacks `reported_eps`
# alongside a finnhub `reported_rev`. Each value is taken from the
# highest-priority row in the slot that actually has it.
#
# The estimate-derived columns are in this list too, but they can only ever
# yield finviz values by the time it runs — `strip_foreign_estimates` has
# already nulled everyone else's.
_MERGEABLE_VALUE_COLS: tuple[str, ...] = (
    "estimated_eps", "reported_eps", "surprise_eps", "surprise_eps_pct",
    "estimated_rev", "reported_rev", "surprise_rev", "surprise_rev_pct",
    "yoy_eps_pct", "yoy_rev_pct",
)

# (provenance column, the reported value it describes).
_REPORTED_SOURCES: tuple[tuple[str, str], ...] = (
    ("eps_source", "reported_eps"),
    ("rev_source", "reported_rev"),
)

# The estimate and the surprise are ONE source's opinion and are inextricably
# paired: the surprise is literally `actual − estimate`, and the surprise
# percentage divides by that same estimate. Mixing providers across the pair
# would state a relationship nobody computed, so the whole estimate-derived
# cluster comes from a single source.
#
# finviz is the choice: it is the top-priority source, it derives its surprise
# from its own actual and estimate, and it covers ~99% of slots. The other
# sources' estimate/surprise values are still SCRAPED — still preserved
# verbatim in the earnings_raw audit layer, and still compared by the
# cross-source disagreement report — they are simply never persisted to the
# consumer parquet. See `strip_foreign_estimates`.
#
# (This is the same six-column set the pre-F13 code called
# `_ESTIMATE_BACKFILL_COLS`; the grouping was already recognised, it just
# wasn't pinned to one provider.)
ESTIMATE_SOURCE = "finviz"
_ESTIMATE_COLUMNS: tuple[str, ...] = (
    "estimated_eps", "surprise_eps", "surprise_eps_pct",
    "estimated_rev", "surprise_rev", "surprise_rev_pct",
)

# Quarterly cadence is ~90 days; allow up to 135 days between consecutive
# period_endings before treating the gap as a "missing quarter" that
# breaks a consecutive-beats streak (per spec §9.1).
_MAX_QUARTER_GAP_DAYS = 135


# EPS columns that move together when a row's reported_eps is implausible.
_EPS_FIELDS: tuple[str, ...] = (
    "estimated_eps", "reported_eps", "surprise_eps",
    "surprise_eps_pct", "yoy_eps_pct",
)

# ── EPS plausibility flags ─────────────────────────────────────────────
#
# WHY THESE ARE FLAGS AND NOT DELETIONS
#
# A heavily reverse-split nano-cap stores an enormous per-share EPS, and that
# value is ARITHMETICALLY CORRECT: it is the as-reported figure restated onto
# the current share basis. ABTC's 2019 quarter reads -7800 because -$0.26 was
# restated through a cumulative 30,000x of subsequent reverse splits, and the
# cached price for that same quarter reads $96,000 — the identical factor.
# Nulling those rows destroys real data, and de-adjusting them back to the
# as-reported basis would BREAK YoY, which is only comparable because every
# quarter is restated onto one common basis.
#
# THE BASIS BUG THIS REPLACES
#
# The previous guard compared a restated EPS against `_load_current_prices` —
# TODAY's close. EPS at factor F(t) against a price at factor 1 is not a
# comparison at all, and it fails toward destroying data: ABTC scored
# 7800 / 7.12 = 1096x and was judged an artifact.
#
# Because EPS and price carry the SAME factor, |eps| / price is basis-INVARIANT
# when both are taken at the same date. Comparing against the close on the
# row's own period_ending therefore needs no magnitude threshold to work, and
# it spares the two populations a size cut-off cannot tell apart:
#   * ABTC 2019   -7800 vs a $96,000 restated close -> 0.08x, sane
#   * BRK-A       ~9,000 EPS on a ~$700,000 share   -> 0.01x, sane
# while still catching a genuine artifact (a $600 EPS on a $5 stock).
EPS_FLAG_NONE = ""
EPS_FLAG_PRICE = "price"      # |eps| >> the close on this row's period_ending
EPS_FLAG_ABS = "abs"          # beyond MAX_PLAUSIBLE_EPS and unverifiable

# OHLCV covers OHLCV_HISTORY_YEARS; earnings run far deeper, so most rows have
# no contemporaneous close. Those are left UNFLAGGED rather than judged against
# a price from the wrong era — "cannot verify" is not "implausible".


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

    ``price_by_ticker``: optional ``{ticker: close}``. Omit it to apply the
    absolute cap only.

    Retained as the boolean view over ``flag_eps_basis`` for callers that only
    need "is this row suspect". NOTE that a flagged row is no longer emptied —
    see the module note above ``EPS_FLAG_NONE``.
    """
    if df is None or df.empty or "reported_eps" not in df.columns:
        return pd.Series(False, index=getattr(df, "index", pd.RangeIndex(0)))
    prices = None
    if price_by_ticker:
        prices = pd.to_numeric(
            df["ticker"].astype(str).map(price_by_ticker), errors="coerce",
        )
    return flag_eps_basis(df, prices=prices) != EPS_FLAG_NONE


def _period_prices(df: pd.DataFrame) -> pd.Series:
    """Close on each row's ``period_ending``, aligned to ``df.index``.

    NaN where the ticker has no cached OHLCV or the period predates it. One
    parquet read per candidate TICKER, not per row.

    ``Series.asof`` takes the last close at or before the period end, which is
    the price basis in force for that quarter — and, crucially, one carrying the
    same cumulative split factor as the EPS it will be compared against.
    """
    empty = pd.Series(np.nan, index=df.index, dtype="float64")
    if df.empty or "ticker" not in df.columns or "period_ending" not in df.columns:
        return empty
    ends = pd.to_datetime(df["period_ending"], errors="coerce")
    if getattr(ends.dtype, "tz", None) is not None:
        ends = ends.dt.tz_localize(None)
    out = empty.copy()
    for ticker, idx in df.groupby(df["ticker"].astype(str)).groups.items():
        path = config.PARQUET_DIR / f"{ticker}.parquet"
        if not path.exists():
            continue
        try:
            close = pd.read_parquet(path, columns=["Close"])["Close"]
        except Exception:
            continue
        if close.empty:
            continue
        if getattr(close.index, "tz", None) is not None:
            close.index = close.index.tz_localize(None)
        # A cache file with a non-datetime index cannot be asked "what was the
        # price on this date". Leave those rows unpriced rather than raising —
        # this runs on the ingest path, and one odd parquet must not take a
        # whole flush down with it.
        if not isinstance(close.index, pd.DatetimeIndex):
            continue
        close = close.sort_index()
        want = ends.loc[idx].dropna()
        if want.empty:
            continue
        got = close.asof(pd.DatetimeIndex(want.to_numpy()))
        out.loc[want.index] = np.asarray(got, dtype="float64")
    return out


def flag_eps_basis(
    df: pd.DataFrame, *, prices: Optional[pd.Series] = None,
) -> pd.Series:
    """Per-row EPS plausibility flag: ``""``, ``"price"`` or ``"abs"``.

    ``prices`` is a per-row close aligned to ``df.index`` (from
    ``_period_prices``). Rows without one are judged by the absolute cap alone,
    and rows within it are left unflagged — see the note above on why "cannot
    verify" must not read as "implausible".
    """
    if df is None or df.empty or "reported_eps" not in df.columns:
        idx = getattr(df, "index", pd.RangeIndex(0))
        return pd.Series(EPS_FLAG_NONE, index=idx, dtype="object")

    ae = pd.to_numeric(df["reported_eps"], errors="coerce").abs()
    flags = pd.Series(EPS_FLAG_NONE, index=df.index, dtype="object")

    if prices is not None:
        px = pd.to_numeric(prices, errors="coerce")
        usable = px.notna() & (px > 0) & ae.notna()
        # Basis-invariant: both sides carry the same cumulative split factor.
        flags[usable & (ae > config.EPS_PRICE_IMPLAUSIBLE_MULT * px)] = \
            EPS_FLAG_PRICE
    else:
        usable = pd.Series(False, index=df.index)

    # Absolute cap only where the precise test could not run. A priced row has
    # already been judged on the basis-invariant comparison, and overriding
    # that with a magnitude rule would reintroduce exactly the false positives
    # this replaces (BRK-A's real thousands-per-share among them).
    flags[~usable & ae.notna() & (ae > config.MAX_PLAUSIBLE_EPS)] = EPS_FLAG_ABS
    return flags


def apply_eps_flags(
    df: pd.DataFrame, *, prices: Optional[pd.Series] = None,
) -> pd.DataFrame:
    """Return a copy of ``df`` with ``eps_flag`` set. Values are NEVER altered:
    revenue, dates and every EPS field are left exactly as supplied."""
    if df is None or df.empty:
        return df
    out = df.copy()
    out["eps_flag"] = flag_eps_basis(out, prices=prices)
    return out


def sanitize_eps_artifacts(df: pd.DataFrame) -> pd.DataFrame:
    """INGEST-time EPS plausibility STAMP for the fill flush paths
    (finviz / zacks / finnhub). Sets ``eps_flag``; changes no value.

    Compares each row against the close on its OWN ``period_ending`` rather
    than today's, so the EPS and the price carry the same cumulative split
    factor and the ratio means something. Reads OHLCV only for candidate
    tickers (|eps| > 20), so an ordinary flush with no large-EPS rows does zero
    parquet reads.

    Name kept for its callers; it no longer sanitizes in the destructive sense.
    """
    if df is None or df.empty or "reported_eps" not in df.columns:
        return df
    ae = pd.to_numeric(df["reported_eps"], errors="coerce").abs()
    cand = ae > 20
    prices = _period_prices(df.loc[cand]) if cand.any() else None
    if prices is not None:
        full = pd.Series(np.nan, index=df.index, dtype="float64")
        full.loc[prices.index] = prices
        prices = full
    return apply_eps_flags(df, prices=prices)


# ──────────────────────────────────────────────────────────────────────
# Load / save
# ──────────────────────────────────────────────────────────────────────

# Read-retry budget for the history parquet. Audit 2026-08-12 (INT-1): 2
# attempts 0.2 s apart could not ride out an antivirus scan of an 8 MB file.
_READ_ATTEMPTS = 3
_READ_BACKOFF_SEC = 0.4

# Set by load_earnings_history(): True when the file EXISTS but could not be
# read. Read via history_read_failed() immediately after a load. Not a lock —
# every writer already holds HISTORY_WRITE_LOCK across its load+save pair, so
# the flag cannot be clobbered between the two by another writer.
_LAST_READ_FAILED = False


def history_read_failed() -> bool:
    """True if the most recent ``load_earnings_history()`` failed to READ an
    existing file (as opposed to finding no file at all).

    Audit 2026-08-12 (INT-1): guards the flush paths against replacing the whole
    store with a partial buffer after a transient read error. Mirrors the guard
    that already existed in ``earnings_cache._merge_and_save``.
    """
    return _LAST_READ_FAILED


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
    # Retry on a read failure: the common cause on Windows is a transient
    # sharing-violation while another thread's os.replace swaps the file in, or
    # an antivirus / backup / cloud-sync agent holding the 8 MB file.
    #
    # Audit 2026-08-12 (INT-1, CRITICAL): returning None here is ambiguous —
    # it previously meant BOTH "no store yet" and "store exists but I could not
    # read it", and the flush helpers treated the latter as the former and
    # overwrote the whole store with the current flush buffer (measured: 100%
    # of a 4,003-row store destroyed by one simulated sharing violation).
    # ``history_read_failed()`` now lets callers tell the two apart, and the
    # retry budget is wider (3 attempts, growing backoff) because 0.4 s total
    # was not enough to ride out a real AV scan.
    global _LAST_READ_FAILED
    df = None
    last_exc = None
    for attempt in range(_READ_ATTEMPTS):
        try:
            df = pd.read_parquet(path)
            break
        except Exception as exc:  # noqa: BLE001 - logged after the retries
            last_exc = exc
            if attempt < _READ_ATTEMPTS - 1:
                time.sleep(_READ_BACKOFF_SEC * (attempt + 1))
    if df is None:
        _LAST_READ_FAILED = True
        log.error(
            "Failed to read earnings_history.parquet after %d attempts: %s "
            "(callers MUST NOT treat this as an empty store)",
            _READ_ATTEMPTS, last_exc,
        )
        return None
    _LAST_READ_FAILED = False

    for col in ("period_ending", "report_date", "updated_at"):
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                if df[col].dt.tz is not None:
                    df[col] = df[col].dt.tz_localize(None)
            except (AttributeError, TypeError):
                pass
    # Audit 2026-08-16 (F13): back-fill the per-group provenance from `source`
    # on any row written before the columns existed — a pre-F13 row never took
    # part in a cross-source merge, so it owns both of its metric groups.
    df = ensure_group_sources(df)
    # Rows written before `eps_flag` existed are simply unjudged, which is "" —
    # NOT "implausible". Every consumer therefore sees a real column whether or
    # not the store predates it.
    if "eps_flag" not in df.columns:
        df["eps_flag"] = EPS_FLAG_NONE
    else:
        flag = df["eps_flag"].astype("object")
        df["eps_flag"] = flag.where(flag.notna(), EPS_FLAG_NONE)
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
            explicit bool to override. Canonical (dedup=True) saves also
            refresh the report-only cross-source disagreement CSV just
            before deduping (see ``report_cross_source_disagreements``).
    """
    if df is None:
        return
    if df.empty:
        # Audit 2026-08-12 (INT-17): an empty frame used to be an
        # unconditional no-op, so a legitimate repair that empties the store
        # (an integrity auto-fix that drops every row, a prune that removes
        # the last ticker) could not be PERSISTED — the next load returned the
        # stale pre-repair file and the fix appeared to have been ignored.
        # Writing an empty store is only allowed when one already exists;
        # `None` still means "nothing to do", and a first-ever save of an
        # empty frame still creates no file.
        if not config.EARNINGS_HISTORY_PARQUET.exists():
            return
        log.warning(
            "Persisting an EMPTY earnings_history.parquet — every row was "
            "removed by the caller (repair or prune), not by a read failure"
        )
        empty = pd.DataFrame(columns=COLUMNS)
        with HISTORY_WRITE_LOCK:
            _rotate_history_backup()
            config.atomic_write_parquet(
                empty, config.EARNINGS_HISTORY_PARQUET,
                engine="pyarrow", index=False,
            )
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
    # Audit 2026-08-16: normalise numeric dtypes before anything compares or
    # concatenates them, so an all-None batch can't cement an object column.
    out = coerce_value_dtypes(out)
    # Audit 2026-08-16 (F13): materialise the per-group provenance before dedup
    # so the merge has something to stamp and legacy / freshly-built rows carry
    # it. Runs on EVERY save, canonical or not, so the columns are never absent
    # from the on-disk schema once this build has written it.
    out = ensure_group_sources(out)
    # Re-prune the rolling history cap on every canonical write. The fill
    # cutoff is evaluated once at fetch time, so a row fetched at the
    # boundary lingers as the daily-advancing cutoff overtakes it; re-pruning
    # here keeps the on-disk window a clean trailing EARNINGS_HISTORY_YEARS.
    #
    # Audit 2026-08-12 (EFF-7): now actually gated on `sort`, matching what
    # this comment always claimed. It previously ran on EVERY per-flush write
    # too — ~600 extra full-frame date coercions and filters per universe fill,
    # for a boundary case that only matters on the canonical write. Rows that
    # age out mid-fill are pruned by the sorted save at _finalize_fill.
    if sort and "period_ending" in out.columns and not out.empty:
        _cap_cutoff = pd.Timestamp.today().normalize() - pd.DateOffset(
            years=config.EARNINGS_HISTORY_YEARS,
        )
        out = out.loc[
            pd.to_datetime(out["period_ending"], errors="coerce") >= _cap_cutoff
        ].reset_index(drop=True)
    if dedup:
        # F4 — report-only cross-source disagreement scan. Runs HERE
        # because the canonical (dedup=True) save is the single choke
        # point every fill's end-of-run write passes through (zacks'
        # _finalize_fill and fill_framework's flush/finalize all land
        # here), and it's the last moment the per-slot loser rows still
        # exist — dedupe_history drops them on the next line. Per-flush
        # saves (dedup=False) skip it. Never allowed to break the
        # parquet save (e.g. the CSV is locked open in Excel).
        try:
            report_cross_source_disagreements(out)
        except Exception as exc:  # noqa: BLE001 — diagnostics must not block saves
            log.warning("Cross-source disagreement report failed: %s", exc)
    # Audit 2026-08-16 (F14): estimate-derived columns are finviz-only.
    # Ordering is load-bearing — AFTER the disagreement report, which
    # legitimately compares every source's surprise figures and is the last
    # consumer of the foreign ones, and BEFORE the merge, so a back-fill can
    # only ever pull finviz values into those columns.
    out = strip_foreign_estimates(out)
    if dedup:
        # Audit 2026-08-16 (F13): merge values onto the slot winner BEFORE the
        # losers are dropped. This is the only moment the loser rows still
        # exist, which is precisely why the read-side-only version could never
        # fire.
        out = dedupe_history(out, merge_sources=True)
    # Sanity STAMP: mark rows whose reported_eps is beyond any verifiable
    # magnitude. Absolute cap only here, and deliberately so — this path runs on
    # every flush, and pricing each row would put an OHLCV read on it. The
    # precise, basis-invariant comparison belongs to the ingest guard
    # (`sanitize_eps_artifacts`) and the whole-store pass
    # (`migrate_sanitize_absurd_eps`), both of which price by period_ending.
    #
    # Rows arriving with a flag already set keep it: this only ever ADDS the
    # absolute-cap verdict to rows nobody has judged yet, so a precise "price"
    # verdict from ingest is never overwritten by the coarse rule here.
    stamped = flag_eps_basis(out)
    if "eps_flag" in out.columns:
        prior = out["eps_flag"].astype("object").fillna(EPS_FLAG_NONE)
        out["eps_flag"] = prior.where(prior != EPS_FLAG_NONE, stamped)
    else:
        out["eps_flag"] = stamped
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
    for col in ("source", "report_time", "eps_source", "rev_source",
                "eps_flag"):
        if col in out.columns:
            out[col] = out[col].astype("category")
    # Audit 2026-08-12 (INT-6): snapshot the current file before a CANONICAL
    # write. There was no automatic backup anywhere in the project — the .bak_*
    # files in scanner_data/ were all made by hand — so any write-path defect
    # was unrecoverable. Canonical (sorted) saves happen a handful of times per
    # fill, not per flush, so the cost is a few 8 MB copies rather than 600.
    if sort:
        _rotate_history_backup()
    config.atomic_write_parquet(
        out, config.EARNINGS_HISTORY_PARQUET, engine="pyarrow", index=False,
    )


_BACKUP_SUFFIX = ".autobak"


def history_backup_paths() -> list[Path]:
    """Existing rolling backups, newest first."""
    d = config.EARNINGS_HISTORY_PARQUET.parent
    stem = config.EARNINGS_HISTORY_PARQUET.name
    found = sorted(
        d.glob(f"{stem}{_BACKUP_SUFFIX}*"),
        key=lambda p: p.name,
    )
    return list(reversed(found))


def _rotate_history_backup() -> None:
    """Copy the live history parquet aside, keeping the newest N snapshots.

    Audit 2026-08-12 (INT-6). Named ``<file>.autobak<N>`` so the rotation is
    obvious on disk and distinct from the hand-made ``.bak_*`` files. Never
    raises: a backup failure must not block the save it protects — losing a
    snapshot is strictly better than refusing to persist new data.
    """
    src = config.EARNINGS_HISTORY_PARQUET
    if not src.exists():
        return
    keep = max(1, int(getattr(config, "HISTORY_BACKUP_COUNT", 3)))
    try:
        import shutil

        # Audit 2026-08-16 (F9): space the snapshots by TIME, not by write.
        # Canonical saves cluster — three sources finalize per smart refresh,
        # plus the launch migrations — so a purely per-write rotation could burn
        # all `keep` slots in one session and leave three snapshots of the same
        # afternoon. Skipping is safe: it PRESERVES the existing (older, and
        # therefore more useful) snapshot rather than overwriting it.
        min_gap_h = float(getattr(
            config, "HISTORY_BACKUP_MIN_INTERVAL_HOURS", 0) or 0)
        if min_gap_h > 0:
            newest = src.with_name(f"{src.name}{_BACKUP_SUFFIX}1")
            try:
                if newest.exists():
                    age_h = (
                        time.time() - newest.stat().st_mtime
                    ) / 3600.0
                    if age_h < min_gap_h:
                        log.debug(
                            "History backup skipped — newest snapshot is only "
                            "%.1f h old (min gap %.1f h)", age_h, min_gap_h,
                        )
                        return
            except OSError:
                pass      # can't stat — fall through and take the snapshot

        # Shift older snapshots down: autobak2 → autobak3, autobak1 → autobak2.
        for n in range(keep - 1, 0, -1):
            older = src.with_name(f"{src.name}{_BACKUP_SUFFIX}{n}")
            newer = src.with_name(f"{src.name}{_BACKUP_SUFFIX}{n + 1}")
            if older.exists():
                if newer.exists():
                    newer.unlink()
                older.rename(newer)
        shutil.copy2(src, src.with_name(f"{src.name}{_BACKUP_SUFFIX}1"))
        # Drop anything beyond the retention window (e.g. after keep shrank).
        for p in config.EARNINGS_HISTORY_PARQUET.parent.glob(
            f"{src.name}{_BACKUP_SUFFIX}*"
        ):
            tail = p.name.rsplit(_BACKUP_SUFFIX, 1)[-1]
            if tail.isdigit() and int(tail) > keep:
                p.unlink()
    except Exception as exc:  # noqa: BLE001 — never block the save
        log.warning("History backup rotation failed (continuing): %s", exc)


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


def coerce_value_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Force the numeric value columns to a real numeric dtype.

    Fill rows are built as dicts, so a column whose every value is ``None`` for
    this batch — a finnhub-only fill has no revenue at all, ever — arrives as
    OBJECT dtype. Concatenating that onto the float64 column already on disk is
    what pandas 2.2 warns about ("concatenation with empty or all-NA entries is
    deprecated ... this will no longer exclude all-NA columns when determining
    the result dtypes"), and in pandas 3 it resolves to an object column on
    disk. That is the exact drift ``verify_integrity`` check #6 was written to
    report — "typically because every value is None" — so this fixes the cause
    rather than waiting to detect the symptom.

    Only object-dtype columns are touched, so a well-formed frame pays nothing.
    """
    for col in _MERGEABLE_VALUE_COLS:
        if col in df.columns and df[col].dtype == object:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def strip_foreign_estimates(df: pd.DataFrame) -> pd.DataFrame:
    """Null every estimate-derived column on rows whose ``source`` is not
    ``ESTIMATE_SOURCE`` (audit 2026-08-16, F14).

    The estimate and its surprise are one provider's opinion and are
    inextricably paired, so the whole cluster comes from a single source and
    the columns mean the same thing on every row of the store. Other sources
    keep supplying these figures — they stay in the ``earnings_raw`` audit
    layer and are still visible to the cross-source disagreement report — they
    just never reach the consumer parquet.

    Applied on EVERY save, canonical or not, so a single-source zacks row can't
    smuggle one in either. Foreign values already on disk are cleaned by the
    first save that touches them, so no migration is needed.

    Mutates and returns ``df`` — callers already hold a copy.
    """
    import numpy as np

    if "source" not in df.columns or df.empty:
        return df
    cols = [c for c in _ESTIMATE_COLUMNS if c in df.columns]
    if not cols:
        return df
    foreign = df["source"].astype(str).str.lower() != ESTIMATE_SOURCE
    if foreign.any():
        df.loc[foreign, cols] = np.nan
    return df


def ensure_group_sources(df: pd.DataFrame) -> pd.DataFrame:
    """Materialise ``eps_source`` / ``rev_source`` — the origin of this row's
    ``reported_eps`` / ``reported_rev``.

    Defaults to ``source``: a row that never took part in a cross-source merge
    got its reported values from its own source. Rows written before the
    columns existed are covered by the same rule. A row with no reported value
    gets NA rather than a source, because there is no value to attribute.

    Always leaves the columns as plain object dtype. They are coerced to
    ``category`` once, at the single write path in ``save_earnings_history`` —
    doing it here instead would make every later assignment a categorical
    set-item, and a value outside the existing categories raises
    (``TypeError: Cannot setitem on a Categorical with a new category``). That
    is not hypothetical: it is exactly what a cross-source merge does.

    Mutates and returns ``df`` — callers already hold a copy.
    """
    if "source" not in df.columns:
        return df
    base = df["source"].astype(str)
    for col, anchor in _REPORTED_SOURCES:
        if col not in df.columns:
            cur = pd.Series(pd.NA, index=df.index, dtype="object")
        else:
            cur = df[col].astype("object")
        # Plain masked assignment rather than `.where`: on object dtype the
        # latter emits the Pandas 2.2 "Downcasting behavior in ... 'where'"
        # FutureWarning, which this codebase already sidesteps elsewhere (see
        # the report_date_proxy handling in load_earnings_history).
        blank = cur.isna() | (cur.astype(str).str.strip() == "")
        if blank.any():
            cur = cur.copy()
            cur[blank] = base[blank]
        if anchor in df.columns:
            absent = pd.to_numeric(df[anchor], errors="coerce").isna()
            if absent.any():
                cur = cur.copy()
                cur[absent] = pd.NA
        df[col] = cur
    return df


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


def _merge_slot_values(df: pd.DataFrame) -> pd.DataFrame:
    """Fill each slot's winner from the best available same-slot value, one
    COLUMN at a time (audit 2026-08-16, F13/F14).

    ``df`` must already be sorted winner-first within each
    ``(ticker, period_ending)`` group and carry a RangeIndex.

    Reported actuals mix freely, per column. Every source quotes the same
    adjusted / non-GAAP basis, so a quarter may legitimately end up with a
    zacks ``reported_eps``, a finnhub ``reported_rev`` and finviz estimate /
    surprise figures. Each value is taken from the highest-priority row in the
    slot that has it — which is just a within-slot back-fill, because the rows
    are already in priority order.

    The estimate-derived columns are the exception: they fill only from a
    finviz donor, so one source's estimate can never cross onto another row.
    A row's OWN estimate is left alone here — removing a legacy non-finviz
    estimate is ``strip_foreign_estimates``' job at save time, and doing it
    here would also change what a scan displays relative to what is on disk.

    ``eps_source`` / ``rev_source`` are computed from the PRE-merge nulls, so
    they name the row that actually supplied each reported value.
    """
    keys = ["ticker", "period_ending"]
    if not set(keys).issubset(df.columns) or df.empty:
        return df

    # Provenance first — from the pre-merge nulls, before the back-fill blurs
    # which row held what.
    #
    # The donor contributes its own `<x>_source`, NOT its `source`. Those differ
    # precisely when the donor already carries a value inherited from a third
    # source, which is the steady state after the first merge: a row can sit on
    # disk as source=finviz / rev_source=zacks, and re-deriving from `source`
    # would relabel zacks' revenue as finviz on the very next save. Every save
    # re-runs this, so that mislabelling would compound silently.
    df = ensure_group_sources(df)
    for src_col, anchor in _REPORTED_SOURCES:
        if anchor not in df.columns or "source" not in df.columns:
            continue
        has = pd.to_numeric(df[anchor], errors="coerce").notna()
        if not has.any():
            df[src_col] = pd.NA
            continue
        donors = (
            df.loc[has, keys + [src_col]]
              .groupby(keys, sort=False).head(1)
              .set_index(keys)
        )
        want = pd.MultiIndex.from_frame(df[keys])
        df[src_col] = donors[src_col].astype("object").reindex(want).to_numpy()

    # Values — highest-priority non-null per column within the slot.
    est_cols = [c for c in _ESTIMATE_COLUMNS if c in df.columns]
    plain_cols = [c for c in _MERGEABLE_VALUE_COLS
                  if c in df.columns and c not in est_cols]

    if plain_cols:
        grp = df.groupby(keys, sort=False)
        for c in plain_cols:
            df[c] = grp[c].bfill()

    if est_cols and "source" in df.columns:
        # The estimate-derived columns fill only from a finviz DONOR. Stripping
        # the whole frame here instead would be wrong in two ways: it is a
        # write-time concern (save_earnings_history owns it), and this function
        # also runs on the READ path, where nulling a legacy row's own zacks
        # estimate would silently change what a scan shows versus what is
        # actually on disk. Masking the donor pool gets the guarantee that
        # matters — a foreign estimate can never CROSS onto another row, where
        # the save-time strip would no longer recognise it as foreign because
        # the receiving row's `source` is finviz.
        import numpy as np  # noqa: F401 - Series.where uses NaN by default

        finviz_only = df["source"].astype(str).str.lower() == ESTIMATE_SOURCE
        visible = pd.DataFrame(
            {c: df[c].where(finviz_only) for c in est_cols}, index=df.index,
        )
        donated = visible.groupby([df[k] for k in keys], sort=False).bfill()
        for c in est_cols:
            gap = df[c].isna()
            if gap.any():
                df.loc[gap, c] = donated.loc[gap, c]
    return df


def dedupe_history(
    history_df: Optional[pd.DataFrame],
    *,
    merge_sources: bool = False,
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

    ``merge_sources``: before the losing rows are dropped, fill each
    value column on the slot winner from the highest-priority same-slot
    row that has it, and stamp ``eps_source`` / ``rev_source`` with where
    the reported actuals came from. See ``_merge_slot_values``.

    Audit 2026-08-16 (F13): this is now ON for the write-time canonical
    dedup too. It used to be read-side only, which made it dead code —
    the canonical save deleted the loser rows before any reader could
    inherit from them (measured: 0 of 215,469 on-disk slots carried more
    than one source). The cost was 1,562 finviz quarters showing an EPS
    and a blank revenue, 1,069 of which had revenue sitting unused in a
    zacks row that the dedup was about to discard.

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
    df = df.reset_index(drop=True)

    if merge_sources:
        df = _merge_slot_values(df)

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


# ──────────────────────────────────────────────────────────────────────
# Cross-source EPS disagreement flagging (F4 — report-only)
# ──────────────────────────────────────────────────────────────────────

# Column order of the find_cross_source_disagreements result / the CSV.
DISAGREEMENT_COLUMNS: list[str] = [
    "ticker", "period_ending", "source_a", "source_b",
    "eps_a", "eps_b", "surprise_a", "surprise_b",
    "delta_eps", "delta_surprise_pp",
]


def _disagreements_csv_path() -> Path:
    """scanner_data/earnings_disagreements.csv — resolved at call time
    (mirrors the migration-flag path helpers) so test fixtures that
    monkeypatch ``config.DATA_DIR`` redirect the report file too."""
    return config.DATA_DIR / config.EARNINGS_DISAGREEMENTS_CSV_NAME


def find_cross_source_disagreements(
    df: Optional[pd.DataFrame],
    eps_abs_tol: Optional[float] = None,
    surprise_pp_tol: Optional[float] = None,
) -> pd.DataFrame:
    """Report-only scan for (ticker, period_ending) slots where two
    sources materially disagree — the cases ``dedupe_history`` resolves
    silently by keeping the priority winner.

    A cross-source pair is flagged when BOTH rows have a non-null
    ``reported_eps`` and ``|Δeps| > eps_abs_tol`` (default
    ``config.EPS_DISAGREEMENT_ABS_TOL``), OR both have a non-null
    ``surprise_eps_pct`` and the values differ by more than
    ``surprise_pp_tol`` percentage points (default
    ``config.SURPRISE_DISAGREEMENT_PP_TOL``). Strict ``>`` on both axes,
    so a delta exactly at tolerance passes. Slots with rows from a
    single source are never flagged (same-source duplicates collapse to
    the most-recently-updated copy first, matching the dedup's
    same-source winner).

    Returns a frame with columns ``DISAGREEMENT_COLUMNS``. ``source_a``
    is the dedup-priority winner of the pair (the row ``dedupe_history``
    keeps) so each row reads "kept a / dropped b". ``delta_eps`` /
    ``delta_surprise_pp`` are absolute differences; NaN when either side
    is null (the pair was flagged on the other axis).

    Purely diagnostic: never mutates ``df`` and has NO effect on dedup
    outcomes. Vectorized via a self-merge on (ticker, period_ending)
    restricted to contested slots — no Python row loops, safe on the
    full ~138k-row parquet.
    """
    empty = pd.DataFrame(columns=DISAGREEMENT_COLUMNS)
    if df is None or df.empty:
        return empty
    if not {"ticker", "period_ending", "source"}.issubset(df.columns):
        return empty
    if eps_abs_tol is None:
        eps_abs_tol = config.EPS_DISAGREEMENT_ABS_TOL
    if surprise_pp_tol is None:
        surprise_pp_tol = config.SURPRISE_DISAGREEMENT_PP_TOL

    def _num(col: str) -> pd.Series:
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce")
        return pd.Series(float("nan"), index=df.index)

    sub = pd.DataFrame({
        "ticker": df["ticker"].astype(str),
        "period_ending": pd.to_datetime(df["period_ending"], errors="coerce"),
        "source": df["source"].astype(str).str.lower(),
        "eps": _num("reported_eps"),
        "surprise": _num("surprise_eps_pct"),
        "updated_at": (pd.to_datetime(df["updated_at"], errors="coerce")
                       if "updated_at" in df.columns
                       else pd.Series(pd.NaT, index=df.index)),
    })
    sub = sub.dropna(subset=["period_ending"])

    # One row per (ticker, period_ending, source) — most recently updated
    # copy wins, same as dedupe_history's within-source pick.
    sub = (
        sub.sort_values("updated_at", na_position="first", kind="stable")
           .drop_duplicates(subset=["ticker", "period_ending", "source"],
                            keep="last")
           .drop(columns="updated_at")
    )

    # Only slots holding ≥2 (distinct-source) rows can produce a pair —
    # pre-filtering keeps the self-merge near-free on an already-deduped
    # frame where virtually every slot is single-row.
    contested = sub.duplicated(subset=["ticker", "period_ending"], keep=False)
    sub = sub.loc[contested]
    if sub.empty:
        return empty

    # Self-merge → every ordered same-slot pair; keep each unordered
    # CROSS-source pair exactly once, with `a` = dedup-priority winner
    # (string-order tiebreak for sources outside _SOURCE_PRIORITY).
    # Same-source pairs (incl. self-pairs) fail both conditions.
    m = sub.merge(sub, on=["ticker", "period_ending"],
                  suffixes=("_a", "_b"), sort=False)
    prio_a = m["source_a"].map(_SOURCE_PRIORITY).fillna(_SOURCE_PRIORITY_FALLBACK)
    prio_b = m["source_b"].map(_SOURCE_PRIORITY).fillna(_SOURCE_PRIORITY_FALLBACK)
    m = m.loc[(prio_a < prio_b)
              | ((prio_a == prio_b) & (m["source_a"] < m["source_b"]))]
    if m.empty:
        return empty

    m = m.copy()
    m["delta_eps"] = (m["eps_a"] - m["eps_b"]).abs()
    m["delta_surprise_pp"] = (m["surprise_a"] - m["surprise_b"]).abs()
    eps_bad = (m["eps_a"].notna() & m["eps_b"].notna()
               & (m["delta_eps"] > float(eps_abs_tol)))
    sur_bad = (m["surprise_a"].notna() & m["surprise_b"].notna()
               & (m["delta_surprise_pp"] > float(surprise_pp_tol)))
    out = m.loc[eps_bad | sur_bad, DISAGREEMENT_COLUMNS]
    if out.empty:
        return empty
    return (
        out.sort_values(["ticker", "period_ending", "source_a", "source_b"],
                        ascending=[True, False, True, True], kind="stable")
           .reset_index(drop=True)
    )


def _comparable_slot_count(df: Optional[pd.DataFrame]) -> int:
    """Number of ``(ticker, period_ending)`` slots carrying more than one
    SOURCE — the only slots ``find_cross_source_disagreements`` can evaluate.

    Zero means the scan had nothing to compare, which is emphatically NOT the
    same as "compared everything and found it consistent". See
    ``report_cross_source_disagreements`` for why the difference matters.
    """
    if df is None or df.empty:
        return 0
    if not {"ticker", "period_ending", "source"}.issubset(df.columns):
        return 0
    pe = pd.to_datetime(df["period_ending"], errors="coerce")
    ok = pe.notna()
    if not ok.any():
        return 0
    key = pd.DataFrame({
        "ticker": df.loc[ok, "ticker"].astype(str),
        "period_ending": pe.loc[ok],
        "source": df.loc[ok, "source"].astype(str).str.lower(),
    }).drop_duplicates()
    per_slot = key.groupby(["ticker", "period_ending"], sort=False)["source"].size()
    return int((per_slot > 1).sum())


def report_cross_source_disagreements(
    history_df: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """Run the disagreement scan and persist the result to
    scanner_data/earnings_disagreements.csv. Logs loudly when any
    disagreement is found; silent when clean. Returns the report frame.

    The CSV is rewritten ONLY when the frame actually held cross-source slots
    to evaluate. Without that gate the report destroyed itself: this runs on
    every canonical save, immediately BEFORE ``dedupe_history`` collapses each
    slot to one source — so the save that finds the disagreements also removes
    the evidence, and the very next canonical save (whose frame comes off the
    now-deduped parquet) sees zero comparable slots and overwrote the file with
    a bare header. Observed 2026-08-13: 701 real finviz-vs-zacks findings
    written at 19:59 were gone by 20:01, two minutes later, when the next fill
    finalized. The findings were only recoverable because a rolling .autobak
    still held the pre-dedup frame.

    A frame with comparable slots and no disagreements still clears the file —
    that is a genuine "previously reported, now resolved" signal, and the
    self-clearing behaviour it provides is the reason the gate keys on
    comparability rather than simply refusing to write an empty report.
    """
    rep = find_cross_source_disagreements(history_df)
    comparable = _comparable_slot_count(history_df)
    if comparable == 0:
        log.debug(
            "cross-source disagreement scan skipped — no multi-source slots "
            "to compare; leaving %s as-is",
            config.EARNINGS_DISAGREEMENTS_CSV_NAME,
        )
        return rep
    config.atomic_write_csv(rep, _disagreements_csv_path(), index=False)
    if len(rep):
        log.warning(
            "%d cross-source EPS disagreements across %d comparable slot(s) "
            "— see %s",
            len(rep), comparable, config.EARNINGS_DISAGREEMENTS_CSV_NAME,
        )
    return rep


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
        out = pd.Series(
            np.where(ok, (cur_v - prior_v) / prior_v.abs() * 100.0, np.nan),
            index=df.index, dtype="float64",
        )
        # Audit 2026-08-12 (INT-16): the min_base floor bounds the DIVISOR but
        # not the RATIO — a base just above the floor still produced five-figure
        # percentages (live max 120,240%). Null anything beyond the sanity bound
        # rather than persisting a meaningless number that then drives filters.
        return out.where(out.abs() <= config.YOY_SANITY_MAX_PCT)

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
      9. cross_source_slot_overlap - same (ticker, period_ending) carried
                                by >1 source → report-only (dedup picks
                                the priority winner at save time)
     10. calendar_quarter_overlap - finnhub fiscal row collides with a
                                fiscal-accurate source's calendar quarter
                                → report-only (dropped by dedupe_history)
     11. report_before_period_end - report_date < period_ending
                                (impossible) → null report_date, set proxy
     12. future_report_date  - report_date in the future on a row that
                                already has an actual → null report_date,
                                set proxy
     13. absurd_yoy          - |yoy_*_pct| beyond YOY_SANITY_MAX_PCT
                                → null the offending YoY column(s)
     14. placeholder_no_actual - past report_date with no reported EPS
                                → drop the row so the gap fill re-queues it
     15. missing_quarter    - >135-day hole between consecutive quarters
                                inside the recent window → NOT auto-fixable
                                (re-fetch required); this is F1's detector

    Checks 11-14 were added by the 2026-08-12 audit (INT-6 / INT-16): the live
    store passed all ten original checks while holding 3,500 placeholder rows
    and 52 chronologically impossible dates.
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

    # ── Checks 11-14, added by the 2026-08-12 audit (INT-6 / INT-16) ──────
    # verify_integrity reported the live 148,218-row store as perfectly clean
    # while it held 3,500 placeholder rows, 29 chronologically impossible dates,
    # 23 future report dates and 28 absurd YoY ratios. None of those classes was
    # checked, so the one tool that could surface drift said "healthy".
    _rd = (pd.to_datetime(history_df["report_date"], errors="coerce")
           if "report_date" in history_df.columns else None)
    _pe = (pd.to_datetime(history_df["period_ending"], errors="coerce")
           if "period_ending" in history_df.columns else None)

    # 11 — report_date earlier than period_ending (a company cannot report a
    # quarter before that quarter has ended).
    if _rd is not None and _pe is not None:
        impossible = _rd.notna() & _pe.notna() & (_rd < _pe)
        if impossible.any():
            findings.append(IntegrityFinding(
                check="report_before_period_end",
                severity="error",
                affected_rows=int(impossible.sum()),
                sample=_sample_rows(history_df.loc[impossible]),
                auto_fixable=True,
                description=(
                    "report_date precedes period_ending — chronologically "
                    "impossible, so the announcement date is wrong. Auto-fix "
                    "nulls report_date and marks the row as a proxy date; "
                    "period_ending, EPS and revenue are preserved."
                ),
            ))

    # 12 — report_date in the future while the row carries an actual.
    if _rd is not None:
        today = pd.Timestamp.today().normalize()
        future = _rd.notna() & (_rd > today)
        if "reported_eps" in history_df.columns:
            future &= pd.to_numeric(
                history_df["reported_eps"], errors="coerce",
            ).notna()
        if future.any():
            findings.append(IntegrityFinding(
                check="future_report_date",
                severity="warning",
                affected_rows=int(future.sum()),
                sample=_sample_rows(history_df.loc[future]),
                auto_fixable=True,
                description=(
                    "report_date is in the future on a row that already has a "
                    "reported EPS. Auto-fix nulls report_date and marks the "
                    "row as a proxy date; the figures are preserved."
                ),
            ))

    # 13 — absurd YoY ratios. The MIN_YOY_*_BASE floors bound the DIVISOR but
    # not the resulting ratio, so a tiny base still yields five-figure
    # percentages (live max: 120,240%).
    yoy_cols = [c for c in ("yoy_eps_pct", "yoy_rev_pct")
                if c in history_df.columns]
    if yoy_cols:
        absurd = pd.Series(False, index=history_df.index)
        for c in yoy_cols:
            absurd |= (pd.to_numeric(history_df[c], errors="coerce").abs()
                       > config.YOY_SANITY_MAX_PCT)
        if absurd.any():
            findings.append(IntegrityFinding(
                check="absurd_yoy",
                severity="warning",
                affected_rows=int(absurd.sum()),
                sample=_sample_rows(history_df.loc[absurd]),
                auto_fixable=True,
                description=(
                    f"YoY percentage beyond ±{config.YOY_SANITY_MAX_PCT:,.0f}% "
                    "— a near-zero prior-year base, not real growth. Auto-fix "
                    "nulls the offending YoY column(s); the underlying EPS and "
                    "revenue are preserved."
                ),
            ))

    # 14 — placeholder rows: a PAST report_date carrying NO data at all. These
    # occupy their (ticker, period_ending) slot and, before INT-7, suppressed
    # the gap fill that would have replaced them.
    #
    # A row with revenue but no EPS is NOT a placeholder — it is a partially
    # captured quarter holding genuine data, and dropping it would destroy that
    # revenue history. On the live store 3,142 of the 3,500 EPS-null rows carry
    # real revenue, so keying this on EPS alone made the "auto-fix" a 3,142-row
    # data-loss event. The audit's own fix sketch called for this exclusion
    # ("revenue-only rows … should be excluded from the sweep"); requiring BOTH
    # to be null is what makes the auto-fix safe to offer.
    if _rd is not None and "reported_eps" in history_df.columns:
        today = pd.Timestamp.today().normalize()
        placeholder = _placeholder_mask(history_df, _rd, today)
        if placeholder.any():
            findings.append(IntegrityFinding(
                check="placeholder_no_actual",
                severity="warning",
                affected_rows=int(placeholder.sum()),
                sample=_sample_rows(history_df.loc[placeholder]),
                auto_fixable=True,
                description=(
                    "Row has a past report_date but NO reported EPS and NO "
                    "reported revenue — a scheduled-quarter placeholder that "
                    "should have been replaced by an actual. Auto-fix drops "
                    "these rows so the gap fill re-queues the ticker; the raw "
                    "audit layer keeps the original response. Rows carrying "
                    "revenue are left alone, EPS-null or not."
                ),
            ))

    # 15 — missing quarters inside a ticker's own recent history.
    #
    # Audit 2026-08-16 (F6): none of the fourteen checks above asked whether a
    # ticker's quarters are CONTIGUOUS, so the F1 truncation — which removes
    # quarters one ticker at a time — could run for months without leaving a
    # trace any tool would surface. This is F1's detector.
    #
    # Report-only by design: a missing quarter cannot be repaired by rewriting
    # what is already here. The fix is to re-fill the affected tickers, which
    # the gap/targeted fills already do.
    gap_finding = _quarter_gap_finding(history_df)
    if gap_finding is not None:
        findings.append(gap_finding)

    return findings


def _quarter_gap_finding(
    history_df: pd.DataFrame, *, years: Optional[int] = None,
) -> Optional[IntegrityFinding]:
    """Flag tickers with a >135-day hole between consecutive quarters inside
    the recent window. Returns None when clean.

    Restricted to the last ``EARNINGS_GAP_CHECK_YEARS`` because legitimate gaps
    dominate the deep history and would bury the signal: measured store-wide,
    2,700 gap events across 1,262 of 4,858 tickers, most of them IPOs, dark
    periods, fiscal-year changes, or the retention cap leaving one very old
    quarter followed by nothing. Inside a 3-year window a hole bracketed by
    real quarters on BOTH sides is a much stronger indication that something
    was lost.
    """
    if history_df is None or history_df.empty:
        return None
    if not {"ticker", "period_ending"}.issubset(history_df.columns):
        return None

    span = years if years is not None else config.EARNINGS_GAP_CHECK_YEARS
    cutoff = pd.Timestamp.today().normalize() - pd.DateOffset(years=span)
    pe = pd.to_datetime(history_df["period_ending"], errors="coerce")
    recent = pd.DataFrame({
        "ticker": history_df["ticker"].astype(str),
        "period_ending": pe,
    }).dropna()
    recent = recent.loc[recent["period_ending"] >= cutoff]
    if recent.empty:
        return None

    recent = (recent.drop_duplicates()
                    .sort_values(["ticker", "period_ending"], kind="stable"))
    gap_days = recent.groupby("ticker", sort=False)["period_ending"].diff().dt.days
    holed = recent.loc[gap_days > _MAX_QUARTER_GAP_DAYS].copy()
    if holed.empty:
        return None

    holed["gap_days"] = gap_days.loc[holed.index].astype(int)
    tickers = sorted(holed["ticker"].unique())
    sample = [
        {"ticker": r.ticker,
         "period_ending": r.period_ending.isoformat(),
         "gap_days": int(r.gap_days)}
        for r in holed.nlargest(5, "gap_days").itertuples(index=False)
    ]
    return IntegrityFinding(
        check="missing_quarter",
        severity="warning",
        affected_rows=int(len(holed)),
        sample=sample,
        auto_fixable=False,
        description=(
            f"{len(tickers)} ticker(s) have a >{_MAX_QUARTER_GAP_DAYS}-day hole "
            f"between consecutive quarters inside the last {span} year(s) — a "
            f"quarter that should be there is missing. NOT auto-fixable: the "
            f"data has to be re-fetched, so run a targeted/gap fill for these "
            f"tickers. Some gaps are legitimate (a company that went dark, a "
            f"fiscal-year change), so treat this as a list to investigate "
            f"rather than a defect count."
        ),
    )


def _placeholder_mask(
    df: pd.DataFrame, report_dates: pd.Series, today: pd.Timestamp,
) -> pd.Series:
    """Rows with a past report_date and no usable data in EITHER metric.

    Single source of truth shared by ``verify_integrity`` check 14 and its
    fixer, so the rows reported can never diverge from the rows deleted.
    """
    mask = (
        report_dates.notna() & (report_dates <= today)
        & pd.to_numeric(df["reported_eps"], errors="coerce").isna()
    )
    if "reported_rev" in df.columns:
        mask &= pd.to_numeric(df["reported_rev"], errors="coerce").isna()
    return mask


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

    # ── Fixers for checks 11-14 (2026-08-12 audit) ────────────────────────
    # The date repairs null report_date and flag the row as a proxy rather than
    # dropping it: period_ending, EPS and revenue are still good, and
    # report_date_proxy is the codebase's existing signal for "this
    # announcement date is not trustworthy" (the reconciler already excludes
    # proxy rows from next_earnings).
    _rd_fix = pd.to_datetime(df["report_date"], errors="coerce") \
        if "report_date" in df.columns else None
    _pe_fix = pd.to_datetime(df["period_ending"], errors="coerce") \
        if "period_ending" in df.columns else None

    if "report_before_period_end" in findings_by_check and _rd_fix is not None:
        mask = _rd_fix.notna() & _pe_fix.notna() & (_rd_fix < _pe_fix)
        df.loc[mask, "report_date"] = pd.NaT
        if "report_date_proxy" in df.columns:
            df.loc[mask, "report_date_proxy"] = True
        msgs.append(
            f"report_before_period_end: nulled report_date on "
            f"{int(mask.sum())} impossible row(s) and flagged them as proxy"
        )

    if "future_report_date" in findings_by_check and _rd_fix is not None:
        today = pd.Timestamp.today().normalize()
        mask = _rd_fix.notna() & (_rd_fix > today)
        if "reported_eps" in df.columns:
            mask &= pd.to_numeric(df["reported_eps"], errors="coerce").notna()
        df.loc[mask, "report_date"] = pd.NaT
        if "report_date_proxy" in df.columns:
            df.loc[mask, "report_date_proxy"] = True
        msgs.append(
            f"future_report_date: nulled report_date on {int(mask.sum())} "
            f"row(s) dated in the future and flagged them as proxy"
        )

    if "absurd_yoy" in findings_by_check:
        nulled = 0
        for c in ("yoy_eps_pct", "yoy_rev_pct"):
            if c not in df.columns:
                continue
            m = (pd.to_numeric(df[c], errors="coerce").abs()
                 > config.YOY_SANITY_MAX_PCT)
            nulled += int(m.sum())
            df.loc[m, c] = float("nan")
        msgs.append(f"absurd_yoy: nulled {nulled} out-of-range YoY value(s)")

    if "placeholder_no_actual" in findings_by_check and _rd_fix is not None:
        today = pd.Timestamp.today().normalize()
        before = len(df)
        # Same mask the check used — a row with revenue is NOT a placeholder
        # and must survive. Keying this on EPS alone would have dropped 3,142
        # rows of real revenue history on the live store.
        #
        # `_rd_fix` is deliberately the PRE-FIX snapshot of report_date, taken
        # before the two nullers above ran. verify_integrity counted against
        # those same original dates, so using them here is what guarantees the
        # number reported equals the number deleted — the property the user is
        # consenting to when they accept the fix.
        df = df.loc[~_placeholder_mask(df, _rd_fix, today)]
        msgs.append(
            f"placeholder_no_actual: dropped {before - len(df)} "
            f"no-data row(s); the gap fill will re-queue those tickers"
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


def rows_superseded_by(
    existing: Optional[pd.DataFrame],
    new_df: Optional[pd.DataFrame],
    source: str,
) -> pd.Series:
    """Boolean mask over ``existing``: which stored rows this fill response
    is entitled to replace.

    Audit 2026-08-16 (F1, CRITICAL). Both flush paths used to drop EVERY
    ``(ticker, source)`` row and write whatever the current response returned,
    with no comparison against what was already stored. A 200-OK-but-SHORT
    response — a partially-rendered finviz page, a CDN-cached stub, a trimmed
    finnhub array, a zacks page carrying fewer table rows — therefore truncated
    that ticker's history permanently and silently. The INT-1 guard does not
    cover this: it refuses to write when the store is UNREADABLE, which is a
    different failure. Nothing compared row counts, nothing checked coverage,
    and `verify_integrity` had no regression check, so this could run for months
    without leaving a trace.

    The rule
    --------
    **A response is authoritative over the span it covers, and only that span.**
    Stored rows whose ``period_ending`` is strictly OLDER than the response's
    oldest quarter are retained; everything from that quarter forward is
    replaced.

    That single rule handles both cases without branching:

    * a full response (reaches at least as far back as the store) supersedes
      everything, so a genuine RESTATEMENT that withdraws a quarter still
      removes it — the behaviour the old replace-everything semantics existed
      to provide, and the reason a naive merge-by-key would be wrong;
    * a short response supersedes only the recent window it actually covers,
      so the older quarters survive.

    The rolling ``EARNINGS_HISTORY_YEARS`` cap still governs the retained rows:
    ``save_earnings_history`` re-prunes on every canonical write, so this can
    never grow the store past its configured window.

    Fallbacks, both conservative:
      * no ``period_ending`` column on either side → the historical
        replace-by-``(ticker, source)`` behaviour, unchanged;
      * a ticker whose response carries no usable ``period_ending`` at all →
        supersede only EXACT ``(ticker, period_ending)`` matches, so an
        unparseable response can't delete anything it didn't name.
    """
    import numpy as np

    idx = getattr(existing, "index", pd.RangeIndex(0))
    empty = pd.Series(False, index=idx)
    if existing is None or existing.empty or new_df is None or new_df.empty:
        return empty
    if not {"ticker", "source"}.issubset(existing.columns):
        return empty

    # Scope first, convert second. `existing` is the WHOLE store (215k rows on
    # the live tree) and this runs once per flush, so the string/datetime
    # coercions are confined to the in-scope subset — at most this flush's
    # tickers. `source` is category dtype on disk and `.isin` is hash-based, so
    # neither comparison materialises a 215k-element string array.
    inc_tickers = set(new_df["ticker"].dropna().astype(str).unique())
    if not inc_tickers:
        return empty
    in_scope = (
        existing["ticker"].isin(inc_tickers) & (existing["source"] == source)
    )
    in_scope = np.asarray(in_scope, dtype=bool)
    scope_pos = np.flatnonzero(in_scope)
    if scope_pos.size == 0:
        return empty

    if ("period_ending" not in existing.columns
            or "period_ending" not in new_df.columns):
        return pd.Series(in_scope, index=idx)

    inc_t = new_df["ticker"].astype(str)
    inc_pe = pd.to_datetime(new_df["period_ending"], errors="coerce")
    # min() skips NaT, so a ticker whose rows are all unparseable yields NaT.
    oldest = {
        t: v for t, v in inc_pe.groupby(inc_t).min().items() if pd.notna(v)
    }

    sub = existing.iloc[scope_pos]
    sub_t = sub["ticker"].astype(str).to_numpy()
    sub_pe = pd.to_datetime(sub["period_ending"], errors="coerce").to_numpy()
    nat = np.datetime64("NaT")
    thr = np.array(
        [oldest.get(t, nat) for t in sub_t], dtype="datetime64[ns]",
    )

    sup = np.zeros(scope_pos.size, dtype=bool)
    has_thr = ~np.isnat(thr)
    comparable = has_thr & ~np.isnat(sub_pe)
    sup[comparable] = sub_pe[comparable] >= thr[comparable]

    if (~has_thr).any():
        pairs = set(zip(inc_t.to_numpy(), inc_pe.to_numpy()))
        for j in np.flatnonzero(~has_thr):
            sup[j] = (sub_t[j], sub_pe[j]) in pairs

    retained = int(scope_pos.size - sup.sum())
    if retained:
        # This is F1 firing. Loud on purpose: under the old semantics these
        # rows were deleted, so a steady stream here means a source is
        # regularly serving less than it used to and wants investigating.
        shrunk = sorted({
            str(t) for t, keep in zip(sub_t, ~sup) if keep
        })
        log.warning(
            "%s: response covered less history than the store for %d "
            "ticker(s) — retaining %d older row(s) that the pre-F1 "
            "replace-everything write would have deleted (%s%s)",
            source, len(shrunk), retained, ", ".join(shrunk[:8]),
            ", …" if len(shrunk) > 8 else "",
        )

    out = np.zeros(len(existing), dtype=bool)
    out[scope_pos] = sup
    return pd.Series(out, index=idx)


def _slot_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Normalised ``(ticker, period_ending)`` key frame, index-aligned to
    ``df``. Fill rows arrive as dicts and can carry an object-dtype
    ``period_ending``, so both sides are coerced before they are matched."""
    return pd.DataFrame(
        {
            "ticker": df["ticker"].astype(str),
            "period_ending": pd.to_datetime(df["period_ending"],
                                            errors="coerce"),
        },
        index=df.index,
    )


def carry_forward_foreign_values(
    existing: Optional[pd.DataFrame],
    new_df: Optional[pd.DataFrame],
    source: str,
    superseded: Optional[pd.Series],
) -> pd.DataFrame:
    """Preserve reported values a superseded row was holding on ANOTHER
    source's behalf (audit 2026-08-16, F15).

    The problem this closes
    -----------------------
    ``source`` does two jobs, and after F13 they disagree. It names the row's
    winning source AND it is the ``(ticker, source)`` key the flush supersedes
    on — but a merged row's values may belong to a different source, recorded
    in ``eps_source`` / ``rev_source``. So a row can sit on disk as
    ``source=finviz`` while carrying zacks revenue, and the next finviz fill
    supersedes it by ``source`` and takes the zacks revenue with it. The donor
    zacks row is long gone: the merge that created the value deleted it.

    Measured before the fix: finviz fill (EPS only) → zacks fill (revenue
    merged in, ``reported_rev=1000``) → finviz fill again → ``reported_rev``
    back to NaN. Since finviz is the primary and most frequently refreshed
    source, every merged value would have eroded on its next refresh.

    The rule
    --------
    A fill may only retract what it OWNS. When a superseded row holds a
    reported value whose ``*_source`` is some other source, and the incoming
    response has no value of its own for that slot, the value and its
    provenance are carried onto the incoming row.

    The two cases this deliberately does NOT cover:

    * the incoming response HAS its own value — the priority chain applies
      normally and the incoming (higher-priority) value wins;
    * the superseded row's ``*_source`` equals this fill's source — the source
      is retracting its own figure, which is a legitimate correction.
    """
    if (existing is None or new_df is None or new_df.empty
            or superseded is None):
        return new_df
    if not bool(superseded.any()):
        return new_df
    keys = ["ticker", "period_ending"]
    if not (set(keys) | {"source"}).issubset(existing.columns):
        return new_df
    if not set(keys).issubset(new_df.columns):
        return new_df

    dying = existing.loc[superseded]
    if dying.empty:
        return new_df

    out = ensure_group_sources(new_df.copy())
    dying = ensure_group_sources(dying.copy())
    dying_keys = _slot_keys(dying)
    out_keys = _slot_keys(out)
    carried = 0

    for src_col, anchor in _REPORTED_SOURCES:
        if anchor not in dying.columns or anchor not in out.columns:
            continue
        owner = dying[src_col].astype("object")
        foreign = (
            pd.to_numeric(dying[anchor], errors="coerce").notna()
            & owner.notna()
            & (owner.astype(str) != str(source))
        )
        if not foreign.any():
            continue
        donors = (
            pd.concat([dying_keys, dying[[anchor, src_col]]], axis=1)
              .loc[foreign]
              .groupby(keys, sort=False).head(1)
              .set_index(keys)
        )
        gap = pd.to_numeric(out[anchor], errors="coerce").isna()
        if not gap.any():
            continue
        want = pd.MultiIndex.from_frame(out_keys.loc[gap])
        payload = donors.reindex(want)
        got = payload[anchor].notna().to_numpy()
        if not got.any():
            continue
        rows = out.index[gap][got]
        out.loc[rows, anchor] = payload[anchor].to_numpy()[got]
        out.loc[rows, src_col] = payload[src_col].to_numpy()[got]
        carried += int(got.sum())

    if carried:
        log.info(
            "%s: carried %d value(s) forward from superseded row(s) that were "
            "holding them on another source's behalf", source, carried,
        )
    return out


def _flush_pending_to_disk(
    pending: dict[str, list[dict]],
    affected_tickers_total: list[str],
    *,
    is_final: bool = False,
    source: str = "zacks",
) -> bool:
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

    Returns True when the merge reached disk (or there was nothing to write),
    False when it was DEFERRED because the store existed but was unreadable
    (audit 2026-08-12, INT-1 — callers must not advance the checkpoint).
    """
    if not pending:
        return True

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
        # Audit 2026-08-16: see fill_framework — coerce all-None value columns
        # off object dtype before they reach the concat.
        new_df = coerce_value_dtypes(new_df)

        # Audit 2026-08-12 (INT-1, CRITICAL): never mistake "could not read the
        # store" for "there is no store". Overwriting on a transient read error
        # destroyed the entire history in testing. Defer instead — `pending` is
        # not cleared during a run, so the next flush rewrites everything.
        if existing is None and history_read_failed():
            log.error(
                "earnings_history.parquet unreadable — deferring merge of %d "
                "row(s) for %d ticker(s) to avoid truncating the store; the "
                "next flush will retry",
                len(new_df), len(pending),
            )
            return False

        if existing is not None and not existing.empty:
            # Drop only the (ticker, source) rows this response supersedes.
            # Other-source rows for these tickers stay. Other tickers stay.
            #
            # Phase 6.5 fix: keyed on new_df["ticker"] (the actual values being
            # written) rather than pending.keys() (the queried symbol). They
            # normally agree for Zacks, but the row's own ticker is the robust
            # invariant.
            #
            # Audit 2026-08-16 (F1): "supersedes" is no longer "every row for
            # this (ticker, source)" — see rows_superseded_by. A short response
            # used to delete the quarters it simply didn't mention.
            mask_replace = rows_superseded_by(existing, new_df, source)
            # Audit 2026-08-16 (F15): a fill may only retract what it owns.
            new_df = carry_forward_foreign_values(
                existing, new_df, source, mask_replace)
            keep = existing.loc[~mask_replace]
            combined = pd.concat([keep, new_df], ignore_index=True)
        else:
            combined = new_df

        save_earnings_history(combined, sort=is_final)
        return True


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


def _has_any_actual(row: dict) -> bool:
    """True when a built row carries a reported EPS **or** a reported revenue.

    The ingest gate for every source (audit 2026-08-16). A quarter with only
    revenue is genuine reported data; a scheduled-but-unreported quarter has
    neither and is still rejected, which is what INT-7 was actually protecting
    against.
    """
    for col in ("reported_eps", "reported_rev"):
        val = row.get(col)
        if val is None:
            continue
        try:
            if not pd.isna(val):
                return True
        except (TypeError, ValueError):
            return True
    return False


def _save_zacks_checkpoint(run_id: str, completed: set) -> None:
    """Persist the zacks resume point. Never raises — a checkpoint failure
    must not end a 6.5-hour fill (audit 2026-08-16, S1)."""
    from . import fill_framework
    fill_framework.save_checkpoint(
        config.ZACKS_BULK_CHECKPOINT,
        fill_framework.Checkpoint(
            run_id=run_id,
            started_at=datetime.now().isoformat(timespec="seconds"),
            completed=sorted(completed),
        ),
        log,
    )


def _fill_via_zacks(
    tickers: list[str], blacklist: set[str],
    *,
    progress_cb=None,
    stop_flag: Optional[list[bool]] = None,
    delay_sec: float = 1.5,
    flush_every: int = config.FILL_FLUSH_EVERY,
    years: Optional[int] = None,
    label: str = "Zacks fill",
    consec_error_limit: int = 5,
    on_block_callback=None,
    failed_cb=None,
    resume_from_checkpoint: bool = False,
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
    from . import fill_framework
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

    # Audit 2026-08-16 (S1): resume support. This was the one fill with no
    # checkpoint at all — a killed bulk restarted from zero, and at the 1.5 s
    # default pacing a universe run is ~6.5 hours. Uses the same generic
    # helpers finviz/finnhub already share, including their staleness rejection
    # (a checkpoint older than CHECKPOINT_MAX_AGE_HOURS is ignored rather than
    # silently skipping most of the universe).
    completed: set[str] = set()
    if resume_from_checkpoint:
        cp = fill_framework.load_checkpoint(config.ZACKS_BULK_CHECKPOINT, log)
        if cp is not None and cp.completed:
            completed = set(cp.completed)
            run_id = cp.run_id or run_id
            log.info("%s: resuming run %s with %d ticker(s) already complete",
                     label, run_id, len(completed))

    # Seed with tickers completed in a PRIOR session so the end-of-fill
    # reconcile folds them in too — without this a kill+resume left session-1
    # tickers' history on disk but never reconciled their dates.
    affected_total: list[str] = list(completed)
    filled = 0
    errors = 0
    consec_errors = 0
    total = len(work)
    spike_halted = False
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
            if sym in completed:
                i += 1
                continue
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
                # Audit 2026-08-12 (INT-7): also require a reported EPS, as the
                # finviz builder already does. A zacks row for a
                # scheduled-but-unreported quarter otherwise claims its
                # (ticker, period_ending) slot with reported_eps=NaN and then
                # suppresses the gap fill that would replace it (the refresh
                # selector counts any row as coverage). The raw layer keeps the
                # original response, so nothing is lost for replay.
                # Audit 2026-08-16: require at least one ACTUAL rather than an
                # EPS specifically. Zacks' sales table runs deeper than its EPS
                # table (12 revenue-only quarters for AAPL), and that revenue
                # was being fetched and then discarded. INT-7's concern was
                # placeholder rows with NO data suppressing their own repair;
                # coverage is still keyed on `reported_eps` in
                # find_smart_refresh_candidates, so a revenue-only row cannot
                # do that.
                pending[sym] = [
                    h for h in hist_rows
                    if _has_any_actual(h)
                    and (pd.isna(pd.to_datetime(h.get("period_ending"),
                                                errors="coerce"))
                         or pd.to_datetime(h["period_ending"]) >= cutoff)
                ]
                # Raw capture: original Zacks row dicts plus ticker (FULL,
                # uncapped — preserves replay depth). earnings_raw stamps
                # fetched_at + run_id automatically.
                for r in rows:
                    raw_pending.append({"ticker": sym, **r})
                affected_total.append(sym)
                completed.add(sym)
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
                spike_halted = True
                break

            if len(pending) >= flush_every:
                # Audit L8: skip the per-flush sort; one final sorted
                # save fires in `_finalize_fill` after the loop exits.
                #
                # Audit 2026-08-12 (INT-1): only CLEAR `pending` when the flush
                # actually reached disk. A deferred flush (unreadable store)
                # followed by `pending = {}` would silently discard the rows —
                # keeping them means the next flush retries with everything.
                if _flush_pending_to_disk(pending, affected_total) is not False:
                    _flush_raw()
                    # Checkpoint LAST, so it can never claim a ticker whose
                    # rows aren't on disk yet — the same ordering contract
                    # run_fill_loop documents (S1).
                    _save_zacks_checkpoint(run_id, completed)
                    log.info(
                        "%s: flushed %d ticker(s) (%d/%d processed, "
                        "%d filled, %d errors so far)",
                        label, len(pending), i + 1, total, filled, errors,
                    )
                    pending = {}
                else:
                    log.warning(
                        "%s: retaining %d pending ticker(s) in memory — the "
                        "history write deferred and will be retried",
                        label, len(pending),
                    )

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
    _save_zacks_checkpoint(run_id, completed)

    # A clean end clears the checkpoint so the next bulk starts fresh instead
    # of resuming a finished run. A stop or a parse-spike halt KEEPS it — those
    # are exactly the runs worth resuming (S1, mirroring run_fill_loop).
    if not (stop_flag and stop_flag[0]) and not spike_halted:
        fill_framework.clear_checkpoint(config.ZACKS_BULK_CHECKPOINT, log)

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
    flush_every: int = config.FILL_FLUSH_EVERY,
    years: Optional[int] = None,  # None → config.EARNINGS_HISTORY_YEARS at call time
    on_block_callback=None,
    consec_error_limit: int = 5,
    failed_cb=None,
    resume_from_checkpoint: bool = True,
) -> tuple[int, int]:
    """Iterate every ticker in the universe and pull `years` of earnings
    history from Zacks. Returns (filled, errors). Long-running — at the
    1.5s default pacing this is ~6.5 hours for a 15k-ticker universe.
    Use the per-flush save so an interrupted run doesn't lose the
    quarters it already pulled.

    Resumes from `config.ZACKS_BULK_CHECKPOINT` by default (audit 2026-08-16,
    S1) — this was the only fill without one, so a kill meant restarting the
    whole 6.5 hours. Pass ``resume_from_checkpoint=False`` for a forced fresh
    start. A checkpoint older than a day is ignored, so an abandoned run can't
    silently skip most of the universe.

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
        resume_from_checkpoint=resume_from_checkpoint,
    )


def targeted_fill_zacks(
    gap_tickers: list[str], blacklist: set[str],
    *,
    progress_cb=None,
    stop_flag: Optional[list[bool]] = None,
    delay_sec: float = 1.5,
    flush_every: int = config.FILL_FLUSH_EVERY,
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
        # Audit 2026-08-12 (INT-7): coverage means a row with an ACTUAL, not
        # merely a row. Placeholder rows (reported_eps NaN) previously counted
        # toward both `have_history` (suppressing Rule A) and `latest_capture`
        # (making the quarter look captured, suppressing the uncaptured-fresh
        # retry) — so a placeholder blocked its own repair indefinitely.
        if "reported_eps" in h.columns:
            real = h.loc[pd.to_numeric(h["reported_eps"], errors="coerce").notna()]
        else:
            real = h
        have_history = set(real["ticker"].astype(str).unique())
        rd = pd.to_datetime(real["report_date"], errors="coerce")
        past = real.loc[rd <= today]
        if not past.empty:
            latest_capture = (
                past.groupby("ticker")["report_date"].max().to_dict()
            )
        # The re-poll guard still uses EVERY row's updated_at: we did fetch
        # those tickers recently, so the guard must keep pacing them even when
        # the fetch produced only placeholders. Otherwise a ticker that
        # genuinely has no actuals yet would be re-polled on every pass.
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
            log.warning("migration flag write failed: %s", exc)
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
        log.warning("migration flag write failed: %s", exc)
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
            log.warning("calendar_dedup flag write failed: %s", exc)
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
        log.warning("calendar_dedup flag write failed: %s", exc)
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
        # Audit 2026-08-12 (INT-9): do NOT stamp the sentinel here. Stamping on
        # the no-data path permanently consumed a ONE-SHOT migration whenever it
        # ran against an empty/unreadable data dir — which already happened: the
        # source tree carried all four sentinels and no history parquet at all.
        # Leaving the sentinel unset lets a later launch (with data present, or
        # with the raw layer repopulated) still do the work. Re-running when
        # there is genuinely nothing to do costs one cheap read.
        log.info(
            "finviz_backfill: no raw finviz captures available — leaving the "
            "migration unstamped so it can run once data is present"
        )
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
        # Audit 2026-08-12 (INT-9): raw captures existed but none converted, so
        # there was nothing to recover. Unlike the no-raw case above this IS a
        # real answer (the converter saw the data and rejected it), so stamping
        # is legitimate — a re-run would reach the same conclusion.
        try:
            config.atomic_write_text(flag_path, "ok\n")
        except OSError as exc:
            log.warning("finviz_backfill flag write failed: %s", exc)
        return (before, before)

    new_df = pd.DataFrame(rebuilt, columns=COLUMNS)
    # Stamp the replayed rows exactly as a live fill would.
    #
    # This path used to bypass the EPS guard entirely, and that is precisely how
    # ABTC's artifacts came back: the eps_sanitize pass cleaned the store, then
    # a later launch replayed these same raw rows straight past it, through a
    # save that applied only the absolute cap. Because both were one-shot
    # sentinel-gated migrations that stamp independently, a backfill deferred to
    # a later launch (it bails unstamped when no raw captures exist yet) landed
    # AFTER sanitize had permanently retired — and `updated_at` is carried from
    # the raw row, so the result looked like a row an earlier fill had blessed.
    new_df = sanitize_eps_artifacts(new_df)
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


# NOTE: a `_load_current_prices` helper used to live here, returning each
# ticker's LATEST close. It was the input to the old EPS guard and is the exact
# shape of the basis bug described above `EPS_FLAG_NONE` — a restated historical
# EPS judged against a present-day price. It is deliberately gone rather than
# left unused, so it cannot be reached for again. Use `_period_prices`.


def migrate_sanitize_absurd_eps(*, force: bool = False) -> tuple[int, int]:
    """Stamp ``eps_flag`` across the WHOLE store, price-relative. Returns
    ``(rows_flagged, candidate_tickers_priced)``.

    Each candidate row is compared against the close on its own
    ``period_ending``, so EPS and price carry the same cumulative split factor
    and the ratio is basis-invariant. Rows with no contemporaneous close fall
    back to the absolute cap alone; rows inside it are left unflagged.

    NO VALUE IS ALTERED. This previously nulled the EPS columns on every row it
    judged, which destroyed correct data — a heavily reverse-split nano-cap's
    enormous per-share EPS is the real as-reported figure restated onto the
    current share basis, and its cached price for the same quarter carries the
    identical factor. See the module note above ``EPS_FLAG_NONE``.

    RUNS EVERY LAUNCH — this is no longer a one-shot migration, and that is the
    structural fix for how ABTC's artifacts came back.

    Two sentinel-gated one-shot migrations, where one's output is the other's
    input, only stay correctly ordered WITHIN a single launch. The finviz
    backfill bails without stamping when no raw captures exist yet, so it can
    land on a later launch — after this pass has permanently retired. That is
    exactly what happened: `.eps_sanitize_v1.done` is stamped 19:23 and
    `.finviz_backfill_v1.done` 19:24, in the opposite order to the source, which
    can only mean two different launches.

    Making this recurring removes the hazard outright: whatever any other writer
    reintroduces, the next launch re-stamps it. That is only safe because
    flagging is idempotent and alters no value — it could never have been done
    while this nulled rows. Cost is one OHLCV read per candidate ticker (~537 on
    the current store), not per row.

    The sentinel is still written, as a last-run marker only. ``force`` is
    retained for callers and no longer gates anything.
    """
    flag_path = _eps_sanitize_flag_path()
    df = load_earnings_history()
    if df is None or df.empty:
        # Audit 2026-08-12 (INT-9): do NOT stamp on the no-data path — that
        # permanently consumes a one-shot migration against an empty or
        # unreadable data dir (which has already happened once in this project).
        log.info(
            "eps_sanitize: no history to sanitize — leaving the migration "
            "unstamped so it can run once data is present"
        )
        return (0, 0)

    # Pre-screen candidates (any |reported_eps| above a low bar) so OHLCV is
    # read only for names that could possibly be implausible. Legit EPS rarely
    # exceeds ~$20/share except high-priced stocks, which the price-relative
    # rule then spares.
    ae = pd.to_numeric(df["reported_eps"], errors="coerce").abs()
    cand = ae > 20
    cand_tickers = sorted(set(df.loc[cand, "ticker"].astype(str)))
    prices = pd.Series(np.nan, index=df.index, dtype="float64")
    if cand.any():
        got = _period_prices(df.loc[cand])
        prices.loc[got.index] = got

    before = (
        df["eps_flag"].astype("object").fillna(EPS_FLAG_NONE)
        if "eps_flag" in df.columns
        else pd.Series(EPS_FLAG_NONE, index=df.index, dtype="object")
    )
    df = apply_eps_flags(df, prices=prices)
    n = int((df["eps_flag"] != EPS_FLAG_NONE).sum())

    # Write only on an actual verdict change. Two reasons this matters now the
    # pass is recurring: an unchanged store must not rewrite 12 MB and rotate a
    # backup on every launch, and a row whose verdict CLEARS (its price moved)
    # has to be persisted too — keying the write on "anything is flagged" would
    # silently strand stale flags forever.
    changed = int((before.to_numpy() != df["eps_flag"].to_numpy()).sum())
    if changed:
        df = compute_yoy_columns(df)
        save_earnings_history(df, sort=True, dedup=False)
        log.info(
            "eps_flag pass: %d verdict(s) changed; %d row(s) now flagged "
            "across %d candidate ticker(s). No values were altered.",
            changed, n, len(cand_tickers),
        )
    else:
        log.debug("eps_flag pass: no verdict changed (%d flagged)", n)
    try:
        config.atomic_write_text(flag_path, "ok\n")
    except OSError as exc:
        log.warning("eps_sanitize flag write failed: %s", exc)
    return (n, len(cand_tickers))
