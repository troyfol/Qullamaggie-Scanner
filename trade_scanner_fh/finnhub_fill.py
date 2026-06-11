"""finnhub_fill.py — bulk / gap / spot fills against Finnhub.

The single source of writes for Finnhub-source rows in
``earnings_history.parquet``. Phase 2 of the Finnhub augmentation —
Phase 1 stood up the raw audit layer; Phase 2 plumbs Finnhub through
it as a first-class earnings-history source.

Per-ticker pull pattern
-----------------------
Each ticker requires TWO API calls:

  1. ``/stock/earnings`` — full quarterly history (EPS + revenue +
     surprises). Carries `period` (fiscal-quarter end) but NOT the
     announcement date.
  2. ``/calendar/earnings?symbol=...`` over the 5-year window —
     events with real `date` (announcement) plus `year`/`quarter` so
     we can join.

History rows whose (year, quarter) match a calendar event get the real
announcement date; rows without a match fall back to ``period_ending``
and are stamped ``report_date_proxy=True``.

Both calls go through a single rate-limiter (`finnhub_client._limiter`)
at ~1.15s pacing → ~52 req/min, comfortably under the 60/min free-tier
cap. Two calls per ticker × 15k universe ≈ 9.5 hours wall-clock for
a full bulk run.

Resilience
----------
* Per-flush checkpoint at ``config.FINNHUB_BULK_CHECKPOINT`` so a
  killed run resumes from where it left off rather than restarting.
* Per-ticker failure classification (empty / 429 / 5xx / network /
  auth) — empty responses route the ticker to the Finnhub blacklist
  and DO NOT count toward the block streak (ETFs don't mean Finnhub
  is blocking).
* After ``config.FINNHUB_CONSEC_BLOCK_LIMIT`` consecutive non-empty
  failures: pause (initial 60s, doubling per subsequent block within
  the run, capped at 5 min), verify the API key with a cheap probe,
  then rewind to the first ticker in the failure window and retry —
  every ticker that failed during the block almost certainly failed
  for the block, not for its own sake.
* After ``config.FINNHUB_MAX_BLOCKS_PER_RUN`` blocks within a single
  run: invoke the on-block callback so the user can stop / resume.

History cap
-----------
Hardcoded via ``config.EARNINGS_HISTORY_YEARS`` (10y as of 2026-06). Rows
whose ``period_ending`` is older than the cutoff are dropped before write.
The same cutoff is applied on the finviz + Zacks sides so the per-
(ticker, period_ending) dedup sees identical date ranges. The
``/calendar/earnings`` lookback is bounded SEPARATELY by
``config.FINNHUB_CALENDAR_LOOKBACK_YEARS`` (a single from→to call can't
span the full 10y safely); history rows older than that window keep
``report_date_proxy=True``.
"""
from __future__ import annotations

import logging
import sys
import time  # noqa: F401 — kept: tests patch sleep via ``finnhub_fill.time``
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd

from . import config
from . import earnings_raw
from . import fill_framework
from . import finnhub_client

log = logging.getLogger("scanner.finnhub_fill")


# ──────────────────────────────────────────────────────────────────────
# Schema mapping: /stock/earnings record → earnings_history row dict
# ──────────────────────────────────────────────────────────────────────

def _record_to_history_dict(
    record: dict,
    *,
    queried_symbol: str,
    calendar_lookup: dict[tuple[int, int], pd.Timestamp],
    cutoff: pd.Timestamp,
    now: datetime,
) -> Optional[dict]:
    """Translate one /stock/earnings record into the canonical
    earnings_history row schema. Applies the 5-year cap on
    ``period_ending``. Returns None if the record is too malformed
    or its period predates the cutoff.

    Phase 6.5 fix: ``queried_symbol`` is REQUIRED and is what gets
    stamped into the row's ``ticker`` field — NOT ``record["symbol"]``.
    Finnhub canonicalizes some queried symbols to a different
    response form (e.g. query="ENB" → response symbol="ENB.TO" for
    Enbridge's Toronto listing) and using the response form would
    break the per-(ticker, source) flush replacement logic, causing
    duplicate rows to accumulate across runs. Always use what the
    caller queried so pending.keys() and parquet ticker column stay
    aligned.

    `calendar_lookup` maps (year, quarter) → announcement Timestamp
    for the symbol being processed. Misses fall back to
    period_ending with ``report_date_proxy=True``.
    """
    queried = (queried_symbol or "").upper().strip()
    period_str = record.get("period")
    if not queried or not period_str:
        return None
    period_ts = pd.to_datetime(period_str, errors="coerce")
    if pd.isna(period_ts):
        return None
    if period_ts < cutoff:
        return None

    # Normalize period_ending to day-1 of its month (Zacks convention).
    # Finnhub returns true calendar quarter-end (e.g. 2026-03-31); Zacks
    # uses 2026-03-01. Without this normalization, dedup keyed on
    # (ticker, period_ending) treats the same fiscal quarter as two
    # distinct rows. report_date is preserved exactly as supplied.
    period_ending_normalized = period_ts.replace(day=1)

    # Sanity: log if Finnhub returned a different symbol than queried.
    # Doesn't affect correctness — we always use the queried form — but
    # surfaces canonicalization quirks for triage.
    response_sym = (record.get("symbol") or "").upper().strip()
    if response_sym and response_sym != queried:
        log.debug(
            "Finnhub canonicalized %s → %s; storing under queried form.",
            queried, response_sym,
        )

    sym = queried

    year = record.get("year")
    quarter = record.get("quarter")
    announcement = None
    if isinstance(year, int) and isinstance(quarter, int):
        announcement = calendar_lookup.get((year, quarter))

    if announcement is not None and pd.notna(announcement):
        report_ts = pd.Timestamp(announcement)
        proxy = False
    else:
        report_ts = period_ts
        proxy = True

    rev_actual = record.get("revenueActual")
    rev_estimate = record.get("revenueEstimate")
    surprise_rev = None
    surprise_rev_pct = None
    if rev_actual is not None and rev_estimate is not None:
        try:
            surprise_rev = float(rev_actual) - float(rev_estimate)
            denom = abs(float(rev_estimate))
            if denom > 0:
                surprise_rev_pct = surprise_rev / denom * 100.0
        except (TypeError, ValueError):
            surprise_rev = None
            surprise_rev_pct = None

    return {
        "ticker": sym,
        "period_ending": period_ending_normalized,
        "report_date": report_ts,
        "report_time": "Unknown",
        "estimated_eps": record.get("estimate"),
        "reported_eps": record.get("actual"),
        "surprise_eps": record.get("surprise"),
        "surprise_eps_pct": record.get("surprisePercent"),
        "estimated_rev": rev_estimate,
        "reported_rev": rev_actual,
        "surprise_rev": surprise_rev,
        "surprise_rev_pct": surprise_rev_pct,
        "source": "finnhub",
        "updated_at": now,
        "report_date_proxy": proxy,
    }


def _calendar_events_to_lookup(
    events: list[dict],
) -> dict[tuple[int, int], pd.Timestamp]:
    """Build (year, quarter) → announcement_date map from a list of
    /calendar/earnings events for ONE symbol. When duplicate
    (year, quarter) keys exist (rare; revisions), the later date wins.
    """
    out: dict[tuple[int, int], pd.Timestamp] = {}
    for evt in events or []:
        y = evt.get("year")
        q = evt.get("quarter")
        d = evt.get("date")
        if not isinstance(y, int) or not isinstance(q, int) or not d:
            continue
        ts = pd.to_datetime(d, errors="coerce")
        if pd.isna(ts):
            continue
        key = (y, q)
        prior = out.get(key)
        if prior is None or ts > prior:
            out[key] = ts
    return out


# ──────────────────────────────────────────────────────────────────────
# Per-ticker fetcher
# ──────────────────────────────────────────────────────────────────────

@dataclass
class _FetchResult:
    """Result of one full per-ticker fetch (history + calendar)."""
    rows: list[dict] = field(default_factory=list)        # consumer rows (history schema)
    raw_records: list[dict] = field(default_factory=list)  # verbatim Finnhub records for raw layer
    failure: Optional[str] = None                          # FAIL_* sentinel or None on success
    is_empty: bool = False                                  # True when /stock/earnings returned []


def _fetch_one_ticker(
    symbol: str,
    *,
    cutoff: pd.Timestamp,
    cal_start: date,
    cal_end: date,
    now: datetime,
) -> _FetchResult:
    """Pull both endpoints for one ticker and assemble consumer + raw
    rows. Never raises — failures are routed to FAIL_* on the result.

    Side effect: leaves ``finnhub_client.last_failure_kind()`` set to
    the kind of the LAST call made (calendar). Callers should rely on
    the result's ``failure`` field instead.
    """
    out = _FetchResult()

    history = finnhub_client.fetch_earnings_history(symbol)
    if history is None:
        out.failure = finnhub_client.last_failure_kind()
        return out
    if history == []:
        out.failure = finnhub_client.FAIL_EMPTY
        out.is_empty = True
        return out

    # Stash verbatim records for the raw layer BEFORE the schema
    # translation drops anything. Phase 6.5 fix: use the QUERIED symbol
    # for the raw layer's `symbol` field too, matching the consumer
    # parquet's ticker convention. Finnhub may canonicalize (e.g.
    # ENB → ENB.TO) but storing under what the caller asked for keeps
    # the raw layer joinable to the consumer parquet.
    #
    # Raw layer preserves EVERY record Finnhub returned, including
    # the multi-record-per-period rows that some non-calendar-fiscal
    # tickers come back with — see the dedup step below for how the
    # consumer parquet handles those.
    queried_sym_upper = symbol.upper().strip()
    for rec in history:
        if isinstance(rec, dict):
            raw = {
                "symbol": queried_sym_upper,
                "period": rec.get("period"),
                "year": rec.get("year"),
                "quarter": rec.get("quarter"),
                "actual": rec.get("actual"),
                "estimate": rec.get("estimate"),
                "surprise": rec.get("surprise"),
                "surprise_percent": rec.get("surprisePercent"),
                "revenue_actual": rec.get("revenueActual"),
                "revenue_estimate": rec.get("revenueEstimate"),
            }
            out.raw_records.append(raw)

    # Phase 6.5 fix #2: dedup history records by `period` BEFORE row
    # construction. Finnhub returns 2 records for the same period when
    # a ticker has a non-calendar fiscal year (e.g. AENT period=
    # 2025-09-30 returned as both year=2026/q=1 (fiscal-year view) AND
    # year=2025/q=3 (calendar-year view), with sometimes different EPS
    # values). We keep the higher (year, quarter) — that's the
    # as-reported fiscal-year view, which matches what the company
    # actually announced and what filters expect.
    seen: dict = {}
    for rec in history:
        if not isinstance(rec, dict):
            continue
        period = rec.get("period")
        if not period:
            continue
        key = period
        prior = seen.get(key)
        if prior is None:
            seen[key] = rec
        else:
            cur_yq = (rec.get("year") or 0, rec.get("quarter") or 0)
            prior_yq = (prior.get("year") or 0, prior.get("quarter") or 0)
            if cur_yq > prior_yq:
                seen[key] = rec
    deduped_history = list(seen.values())

    # Calendar lookup for accurate announcement dates. Failure here is
    # NON-fatal — we fall back to period_ending with proxy=True.
    events = finnhub_client.fetch_calendar_earnings_window(
        start=cal_start, end=cal_end, symbol=symbol,
    )
    cal_failure = finnhub_client.last_failure_kind()
    if events is None:
        # Calendar call failed, but history succeeded. Treat as a
        # soft failure: emit history rows with proxy=True.
        events = []
        if cal_failure not in (None, finnhub_client.FAIL_EMPTY):
            log.debug(
                "[%s] calendar lookup failed (%s) — falling back to "
                "period_ending as report_date proxy",
                symbol, cal_failure,
            )

    calendar_lookup = _calendar_events_to_lookup(events)

    # Iterate the DEDUPED history list (one record per period) for
    # row construction. Raw layer kept all records above for audit.
    for rec in deduped_history:
        row = _record_to_history_dict(
            rec,
            queried_symbol=symbol,
            calendar_lookup=calendar_lookup,
            cutoff=cutoff, now=now,
        )
        if row is not None:
            out.rows.append(row)

    return out


# ──────────────────────────────────────────────────────────────────────
# Checkpoint persistence (resumable bulk runs) — shared impl in
# fill_framework; thin wrappers keep the module-level names that tests
# (and run_fill_loop's call-time hook resolution) rely on.
# ──────────────────────────────────────────────────────────────────────

_Checkpoint = fill_framework.Checkpoint


def _save_checkpoint(cp: _Checkpoint) -> None:
    fill_framework.save_checkpoint(config.FINNHUB_BULK_CHECKPOINT, cp, log)


def _load_checkpoint() -> Optional[_Checkpoint]:
    return fill_framework.load_checkpoint(config.FINNHUB_BULK_CHECKPOINT, log)


def _clear_checkpoint() -> None:
    fill_framework.clear_checkpoint(config.FINNHUB_BULK_CHECKPOINT, log)


# ──────────────────────────────────────────────────────────────────────
# Persistence — write to earnings_history.parquet (per-source replace)
# ──────────────────────────────────────────────────────────────────────

def _flush_pending_to_disk(
    pending: dict[str, list[dict]],
    *,
    is_final: bool = False,
) -> None:
    """Merge ``pending`` (ticker → list of finnhub-source rows) into
    earnings_history.parquet. Replaces only the (ticker, source=finnhub)
    rows for those tickers — Zacks rows for the same ticker are
    preserved (Phase 2 (ticker, source) soft-PK).

    Phase 6.5 fix: replacement is keyed on ``new_df["ticker"]`` (the
    actual ticker value being written) NOT ``pending.keys()`` (the
    queried symbol). The two used to align by convention, but Finnhub's
    response symbol can differ from what was queried (e.g.
    ENB → ENB.TO canonicalization), causing the pending-key form to
    miss the existing-row form and pile duplicates run-over-run. Using
    the row's own ticker is correct regardless of how it was sourced.
    """
    fill_framework.flush_pending_to_disk(
        pending, source="finnhub", is_final=is_final,
    )


def _finalize_fill(affected_tickers: list[str]) -> None:
    """End-of-fill: re-sort the parquet canonically and run a single
    reconcile against affected tickers — same pattern as the Zacks
    fill in earnings_history.py."""
    fill_framework.finalize_fill(affected_tickers)


# ──────────────────────────────────────────────────────────────────────
# Inner loop (shared by bulk / gap / spot)
# ──────────────────────────────────────────────────────────────────────

def _fill_via_finnhub(
    tickers: list[str],
    blacklist: set[str],
    *,
    progress_cb=None,
    stop_flag: Optional[list[bool]] = None,
    flush_every: int = 25,
    label: str = "Finnhub fill",
    on_block_callback=None,
    on_etf_identified=None,
    failed_cb=None,
    delay_sec: float = 0.0,
    resume_from_checkpoint: bool = False,
) -> tuple[int, int]:
    """Common loop body for bulk_fill_finnhub / gap_fill_finnhub /
    spot_fill_finnhub — delegates to ``fill_framework.run_fill_loop``,
    with finnhub's FAIL_AUTH halt on the spec and its post-pause API-key
    probe riding the on_block_pause hook.

    Args:
        tickers: ordered list of symbols to process. Already filtered
            of blacklist/finnhub_blacklist by the caller; we re-apply
            the universe blacklist here defensively.
        blacklist: union of universe + finnhub blacklist (callers MUST
            pre-union; documented).
        on_block_callback: ``(consec_errors, blocks_so_far) -> "continue"|"stop"``.
            Invoked on each block trigger (≥ FINNHUB_CONSEC_BLOCK_LIMIT
            consecutive non-empty failures). Returning "stop" exits the
            loop cleanly.
        on_etf_identified: ``(symbol) -> None``. Invoked once per ticker
            whose /stock/earnings returned []. Caller adds to the
            Finnhub blacklist live.
        failed_cb: ``(symbol, kind) -> None``. Per-ticker failure
            classification for end-of-run breakdowns.
        resume_from_checkpoint: when True and a checkpoint matches the
            same ordered ticker list, skip already-completed tickers.

    Returns ``(filled_count, error_count)``. ``filled_count`` counts
    every ticker that produced at least one history row written to
    disk; ``error_count`` counts every ticker that failed (including
    empty-response identifications).
    """
    # History cap (drops old /stock/earnings rows) + calendar window
    # (announcement-date recovery). The calendar lookback is bounded
    # independently of the larger history cap so the single from→to call
    # can't blow up the /calendar/earnings endpoint; rows older than the
    # calendar window fall back to report_date_proxy=True.
    today_ts = pd.Timestamp.today().normalize()
    cutoff = today_ts - pd.DateOffset(years=config.EARNINGS_HISTORY_YEARS)
    today_d = today_ts.date()
    cal_start = today_d - timedelta(
        days=int(config.FINNHUB_CALENDAR_LOOKBACK_YEARS * 365.25),
    )
    cal_end = today_d + timedelta(days=90)

    def _probe_key_after_pause() -> bool:
        # Cheap key probe: if we get FAIL_AUTH the key is dead and
        # there's no point continuing.
        if not finnhub_client.verify_api_key():
            kind = finnhub_client.last_failure_kind()
            if kind == finnhub_client.FAIL_AUTH:
                log.error("%s: API key probe returned 401 — halting", label)
                return False
            log.warning("%s: key probe failed (%s) — proceeding to retry",
                        label, kind)
        return True

    spec = fill_framework.FillSpec(
        module=sys.modules[__name__],   # hooks resolved through here at call time
        log=log,
        config_prefix="FINNHUB",
        fail_empty=finnhub_client.FAIL_EMPTY,
        append_raw_rows=lambda rows, rid: earnings_raw.append_finnhub_rows(rows, rid),
        raw_label="Finnhub",
        # Bad / revoked key. Halt immediately — no point churning.
        halt_failure_kind=finnhub_client.FAIL_AUTH,
        halt_log_message="%s: Finnhub returned 401 — halting run",
    )
    return fill_framework.run_fill_loop(
        spec, tickers, blacklist,
        fetch_kwargs={"cutoff": cutoff, "cal_start": cal_start, "cal_end": cal_end},
        progress_cb=progress_cb, stop_flag=stop_flag,
        flush_every=flush_every, label=label,
        on_block_callback=on_block_callback,
        on_empty_identified=on_etf_identified,
        failed_cb=failed_cb,
        delay_sec=delay_sec,
        resume_from_checkpoint=resume_from_checkpoint,
        on_block_pause=_probe_key_after_pause,
    )


# ──────────────────────────────────────────────────────────────────────
# Public entrypoints
# ──────────────────────────────────────────────────────────────────────

def bulk_fill_finnhub(
    universe_symbols: list[str],
    blacklist: set[str],
    *,
    progress_cb=None,
    stop_flag: Optional[list[bool]] = None,
    # Bulk runs rewrite the WHOLE earnings_history parquet each flush, so a
    # small interval is O(n²) I/O over a 9-hour run. 100 (vs the gap/spot 25)
    # cuts full rewrites ~4× without hurting durability — the checkpoint +
    # raw layer still let an interrupted run resume by re-fetching only the
    # at-most-100 unflushed tickers.
    flush_every: int = 100,
    on_block_callback=None,
    on_etf_identified=None,
    failed_cb=None,
    resume_from_checkpoint: bool = True,
) -> tuple[int, int]:
    """Iterate every ticker in the universe and pull /stock/earnings +
    /calendar/earnings. Two API calls per ticker × ~52/min sustainable
    pacing ≈ 9.5 hrs for a 15k-ticker universe.

    `blacklist`: union of universe blacklist + Finnhub-specific
    blacklist (caller assembles).

    By default resumes from checkpoint if one exists for an unfinished
    run. Pass ``resume_from_checkpoint=False`` for a forced fresh start.
    """
    return _fill_via_finnhub(
        universe_symbols, blacklist,
        progress_cb=progress_cb, stop_flag=stop_flag,
        flush_every=flush_every,
        label="Finnhub bulk fill",
        on_block_callback=on_block_callback,
        on_etf_identified=on_etf_identified,
        failed_cb=failed_cb,
        resume_from_checkpoint=resume_from_checkpoint,
    )


def gap_fill_finnhub(
    gap_tickers: list[str],
    blacklist: set[str],
    *,
    progress_cb=None,
    stop_flag: Optional[list[bool]] = None,
    flush_every: int = 25,
    on_block_callback=None,
    on_etf_identified=None,
    failed_cb=None,
) -> tuple[int, int]:
    """Iterate only the provided gap_tickers list (universe minus
    tickers that already have any earnings_history rows). The caller
    computes gaps via ``find_finnhub_gap_tickers``."""
    return _fill_via_finnhub(
        gap_tickers, blacklist,
        progress_cb=progress_cb, stop_flag=stop_flag,
        flush_every=flush_every,
        label="Finnhub gap fill",
        on_block_callback=on_block_callback,
        on_etf_identified=on_etf_identified,
        failed_cb=failed_cb,
        # Gap fills are typically short — no resume.
        resume_from_checkpoint=False,
    )


def spot_fill_finnhub(
    symbol: str,
    blacklist: set[str],
    *,
    on_etf_identified=None,
) -> tuple[int, str]:
    """Fetch one ticker on demand. Returns ``(filled_count, status)``
    where ``status`` is one of: "ok", "empty", FAIL_*. Does not respect
    the resume checkpoint (single-ticker lookups are always fresh)."""
    sym = (symbol or "").upper().strip()
    if not sym:
        return 0, "invalid"
    if sym in blacklist:
        return 0, "blacklisted"

    today_ts = pd.Timestamp.today().normalize()
    cutoff = today_ts - pd.DateOffset(years=config.EARNINGS_HISTORY_YEARS)
    today_d = today_ts.date()
    cal_start = today_d - timedelta(
        days=int(config.FINNHUB_CALENDAR_LOOKBACK_YEARS * 365.25),
    )
    cal_end = today_d + timedelta(days=90)
    now = datetime.now()

    result = _fetch_one_ticker(
        sym, cutoff=cutoff, cal_start=cal_start, cal_end=cal_end, now=now,
    )
    if result.is_empty:
        if on_etf_identified is not None:
            try:
                on_etf_identified(sym)
            except Exception:
                pass
        return 0, "empty"
    if result.failure is not None:
        return 0, str(result.failure)
    if not result.rows:
        return 0, "no_rows_in_window"

    run_id = earnings_raw.new_run_id()
    pending = {sym: result.rows}
    _flush_pending_to_disk(pending, is_final=True)
    if result.raw_records:
        try:
            earnings_raw.append_finnhub_rows(result.raw_records, run_id)
        except Exception as exc:
            log.warning("Finnhub raw-layer write failed: %s", exc)
    _finalize_fill([sym])
    return len(result.rows), "ok"


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

def find_finnhub_gap_tickers(
    universe_symbols: list[str], blacklist: set[str],
) -> list[str]:
    """Return tickers in ``universe ∩ (not blacklist)`` whose
    ``source=finnhub`` row count in earnings_history.parquet is 0.
    Tickers covered only by Zacks ARE returned — gap fill is "fill in
    Finnhub-source coverage", not "tickers with no data anywhere".
    """
    return fill_framework.find_gap_tickers(
        universe_symbols, blacklist, source="finnhub",
    )
