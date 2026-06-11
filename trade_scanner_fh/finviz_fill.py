"""finviz_fill.py — bulk / gap / spot fills against finviz.

The single source of writes for ``source=finviz`` rows in
``earnings_history.parquet``. Finviz is the TOP-priority per-quarter
earnings source (finviz > zacks > finnhub) — adjusted / non-GAAP EPS
that matches Zacks ~98% to the penny, with finer revenue precision and
real announcement dates + times.

Per-ticker pull pattern
-----------------------
ONE scrape per ticker: ``finviz_client.fetch_earnings`` returns the
``earningsData`` array from the ``ty=ea`` page. We keep the rows that
have an actual (``epsActual`` + ``earningsDate``) — finviz also returns
forward analyst-estimate rows for upcoming quarters which we drop — and
map the adjusted fields into the canonical schema. Surprise is derived
(``actual − estimate``) like the other sources.

Resilience
----------
* Per-flush checkpoint at ``config.FINVIZ_BULK_CHECKPOINT`` for resume.
* Per-ticker failure classification: ``empty`` (no earningsData — ETF /
  fund / brand-new) routes to the finviz blacklist and does NOT count
  toward the block streak; real failures (``rate_limited`` / ``blocked``
  / ``server`` / ``network``) do.
* After ``config.FINVIZ_CONSEC_BLOCK_LIMIT`` consecutive real failures:
  pause with exponential backoff, then rewind to the first ticker in the
  failure window. After ``config.FINVIZ_MAX_BLOCKS_PER_RUN`` blocks,
  invoke the on-block callback (stop / continue).

Pacing
------
Deliberately slow (``config.FINVIZ_MIN_INTERVAL_SEC`` ± jitter, enforced
by ``finviz_client._limiter``) so a full bulk runs safely overnight
without tripping finviz's throttle. Callers MUST pre-union the universe
OHLCV blacklist + ETF/ADR auto-skip into ``blacklist`` so funds never
cost a request — the GUI's ``_combined_finviz_skip_set`` does this.

5-year cap
----------
Same ``config.EARNINGS_HISTORY_YEARS`` cutoff as the other sources so
dedup (finviz > zacks > finnhub) sees identical date ranges.
"""
from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import pandas as pd

from . import config
from . import earnings_raw
from . import finviz_client

log = logging.getLogger("scanner.finviz_fill")


# ──────────────────────────────────────────────────────────────────────
# Schema mapping: one earningsData entry → earnings_history row dict
# ──────────────────────────────────────────────────────────────────────

def _report_time_from_hour(hour: Optional[int]) -> str:
    """Map a finviz earningsDate hour to the report_time bucket.
    >=16:00 → after-market-close; <12:00 → before-market-open; else
    Unknown. Midnight (00:00) means finviz had no time → Unknown."""
    if hour is None:
        return "Unknown"
    if hour >= 16:
        return "Close"
    if 0 < hour < 12:
        return "Open"
    return "Unknown"


def _to_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    # Reject inf / nan so a malformed upstream value can't write a non-finite
    # EPS/revenue into the parquet (audit: parsers accepted inf).
    if not math.isfinite(f):
        return None
    return f


def _record_to_history_dict(
    entry: dict,
    *,
    queried_symbol: str,
    cutoff: pd.Timestamp,
    now: datetime,
) -> Optional[dict]:
    """Translate one finviz ``earningsData`` entry into the canonical
    earnings_history row. Returns None when the entry is a forward
    estimate (no ``epsActual`` / ``earningsDate``), malformed, or its
    period predates the 5-year cap.

    Uses the ADJUSTED fields (``epsActual`` / ``epsEstimate`` /
    ``salesActual`` / ``salesEstimate``); the GAAP ``*Reported*`` fields
    are ignored so the row stays on the same adjusted basis as Zacks /
    Finnhub.
    """
    sym = (queried_symbol or "").upper().strip()
    if not sym or not isinstance(entry, dict):
        return None

    eps_actual = _to_float(entry.get("epsActual"))
    earnings_date = entry.get("earningsDate")
    fiscal_end = entry.get("fiscalEndDate")
    # Past quarters only: must have an actual EPS and a real report date.
    if eps_actual is None or not earnings_date or not fiscal_end:
        return None

    period_ts = pd.to_datetime(fiscal_end, errors="coerce")
    if pd.isna(period_ts):
        return None
    # Normalize to day-1 of the fiscal-quarter-end month (cross-source key).
    period_ending = period_ts.replace(day=1)
    if period_ending < cutoff:
        return None

    report_dt = pd.to_datetime(earnings_date, errors="coerce")
    if pd.isna(report_dt):
        return None
    report_time = _report_time_from_hour(int(report_dt.hour))
    report_date = report_dt.normalize()

    eps_est = _to_float(entry.get("epsEstimate"))
    surprise_eps = None
    surprise_eps_pct = None
    if eps_actual is not None and eps_est is not None:
        surprise_eps = eps_actual - eps_est
        if abs(eps_est) > 0:
            surprise_eps_pct = surprise_eps / abs(eps_est) * 100.0

    sales_actual = _to_float(entry.get("salesActual"))
    sales_est = _to_float(entry.get("salesEstimate"))
    surprise_rev = None
    surprise_rev_pct = None
    if sales_actual is not None and sales_est is not None:
        surprise_rev = sales_actual - sales_est
        if abs(sales_est) > 0:
            surprise_rev_pct = surprise_rev / abs(sales_est) * 100.0

    return {
        "ticker": sym,
        "period_ending": period_ending,
        "report_date": report_date,
        "report_time": report_time,
        "estimated_eps": eps_est,
        "reported_eps": eps_actual,
        "surprise_eps": surprise_eps,
        "surprise_eps_pct": surprise_eps_pct,
        "estimated_rev": sales_est,
        "reported_rev": sales_actual,
        "surprise_rev": surprise_rev,
        "surprise_rev_pct": surprise_rev_pct,
        "source": "finviz",
        "updated_at": now,
        "report_date_proxy": False,  # finviz gives real announcement dates
    }


def _next_date_from_entries(entries, today: pd.Timestamp) -> Optional[pd.Timestamp]:
    """Nearest FUTURE ``earningsDate`` across the finviz earningsData
    entries — finviz's next scheduled report date. It rides on a
    forward/analyst-estimate row (no ``epsActual``) that
    ``_record_to_history_dict`` deliberately drops, so it would otherwise
    be lost. Returned so the fill can route it to earnings_dates as
    finviz's ``next_earnings`` contribution WITHOUT polluting the
    per-quarter history with a future NaN-actual row. ``None`` when finviz
    lists no upcoming date (its forward horizon is near-term only)."""
    best: Optional[pd.Timestamp] = None
    for e in (entries or []):
        if not isinstance(e, dict):
            continue
        ed = pd.to_datetime(e.get("earningsDate"), errors="coerce")
        if pd.isna(ed):
            continue
        ed = ed.normalize()
        if ed > today and (best is None or ed < best):
            best = ed
    return best


# ──────────────────────────────────────────────────────────────────────
# Per-ticker fetcher
# ──────────────────────────────────────────────────────────────────────

@dataclass
class _FetchResult:
    """Result of one per-ticker finviz scrape."""
    rows: list[dict] = field(default_factory=list)
    raw_records: list[dict] = field(default_factory=list)
    failure: Optional[str] = None
    is_empty: bool = False   # True when finviz has no earningsData for the ticker
    # Nearest future earningsDate (finviz's next scheduled report), routed
    # to earnings_dates as finviz's next_earnings. None when none upcoming.
    next_date: Optional[pd.Timestamp] = None


def _fetch_one_ticker(
    symbol: str,
    *,
    cutoff: pd.Timestamp,
    now: datetime,
) -> _FetchResult:
    """Scrape one ticker and assemble consumer + raw rows. Never raises."""
    out = _FetchResult()
    sym = (symbol or "").upper().strip()
    if not sym:
        out.failure = "invalid"
        return out

    data = finviz_client.fetch_earnings(sym)
    if data is None:
        kind = finviz_client.last_failure_kind()
        if kind == finviz_client.FAIL_EMPTY:
            out.is_empty = True
        out.failure = kind
        return out
    if not data:
        # 200 with an empty earningsData array — covered but no rows.
        out.is_empty = True
        out.failure = finviz_client.FAIL_EMPTY
        return out

    for entry in data:
        if not isinstance(entry, dict):
            continue
        # Raw audit row — verbatim finviz fields (both bases + dates).
        out.raw_records.append({
            "symbol": sym,
            "fiscal_period": entry.get("fiscalPeriod"),
            "fiscal_end_date": entry.get("fiscalEndDate"),
            "earnings_date": entry.get("earningsDate"),
            "eps_actual": entry.get("epsActual"),
            "eps_estimate": entry.get("epsEstimate"),
            "eps_reported_actual": entry.get("epsReportedActual"),
            "eps_reported_estimate": entry.get("epsReportedEstimate"),
            "sales_actual": entry.get("salesActual"),
            "sales_estimate": entry.get("salesEstimate"),
        })
        row = _record_to_history_dict(
            entry, queried_symbol=sym, cutoff=cutoff, now=now,
        )
        if row is not None:
            out.rows.append(row)

    # Defensive in-ticker dedup: one row per period_ending (finviz
    # shouldn't repeat a fiscal quarter, but keep the latest report_date
    # if it ever does).
    if out.rows:
        seen: dict = {}
        for r in out.rows:
            p = r["period_ending"]
            prior = seen.get(p)
            if prior is None or r["report_date"] >= prior["report_date"]:
                seen[p] = r
        out.rows = list(seen.values())

    # finviz's next scheduled date (from a dropped forward row) → routed
    # to earnings_dates by the fill, never into per-quarter history.
    out.next_date = _next_date_from_entries(data, pd.Timestamp(now).normalize())
    return out


# ──────────────────────────────────────────────────────────────────────
# Checkpoint persistence (resumable bulk runs)
# ──────────────────────────────────────────────────────────────────────

@dataclass
class _Checkpoint:
    run_id: str
    started_at: str
    completed: list[str]


def _save_checkpoint(cp: _Checkpoint) -> None:
    try:
        config.FINVIZ_BULK_CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
        config.atomic_write_text(
            config.FINVIZ_BULK_CHECKPOINT,
            json.dumps({
                "run_id": cp.run_id,
                "started_at": cp.started_at,
                "completed": cp.completed,
            }),
        )
    except OSError as exc:
        log.warning("Checkpoint write failed: %s", exc)


def _load_checkpoint() -> Optional[_Checkpoint]:
    path = config.FINVIZ_BULK_CHECKPOINT
    if not path.exists():
        return None
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
        return _Checkpoint(
            run_id=str(d.get("run_id", "")),
            started_at=str(d.get("started_at", "")),
            completed=[str(t) for t in (d.get("completed") or []) if t],
        )
    except (OSError, ValueError) as exc:
        log.warning("Checkpoint read failed: %s", exc)
        return None


def _clear_checkpoint() -> None:
    try:
        config.FINVIZ_BULK_CHECKPOINT.unlink(missing_ok=True)
    except OSError as exc:
        log.debug("Checkpoint clear failed: %s", exc)


# ──────────────────────────────────────────────────────────────────────
# Persistence — write to earnings_history.parquet (per-source replace)
# ──────────────────────────────────────────────────────────────────────

def _flush_pending_to_disk(
    pending: dict[str, list[dict]],
    *,
    is_final: bool = False,
) -> None:
    """Merge ``pending`` (ticker → finviz rows) into
    earnings_history.parquet, replacing only the (ticker, source=finviz)
    rows for those tickers. Zacks / Finnhub rows are preserved."""
    from . import earnings_history as eh

    if not pending:
        return

    with eh.HISTORY_WRITE_LOCK:
        existing = eh.load_earnings_history()
        new_rows: list[dict] = []
        for rows in pending.values():
            new_rows.extend(rows)
        new_df = pd.DataFrame(new_rows, columns=eh.COLUMNS)
        # Ingest-time price-relative EPS artifact guard (reverse-split
        # nano-caps): finviz eps_actual can carry pre-split values for
        # sub-$5 tickers; null them before they hit disk.
        new_df = eh.sanitize_eps_artifacts(new_df)

        if not new_df.empty and new_df.duplicated(
            subset=["ticker", "period_ending"], keep=False,
        ).any():
            new_df = new_df.drop_duplicates(
                subset=["ticker", "period_ending"], keep="last",
            ).reset_index(drop=True)

        if existing is not None and not existing.empty:
            new_tickers = set(new_df["ticker"].dropna().astype(str).unique())
            mask_replace = (
                existing["ticker"].astype(str).isin(new_tickers)
                & (existing["source"] == "finviz")
            )
            keep = existing.loc[~mask_replace]
            combined = pd.concat([keep, new_df], ignore_index=True)
        else:
            combined = new_df

        eh.save_earnings_history(combined, sort=is_final)


def _flush_next_dates_to_cache(
    next_pending: dict, now: datetime,
) -> bool:
    """Write finviz forward (next-earnings) dates into earnings_dates as
    ``source='finviz'`` rows (``last_earnings=NaT``, ``next_earnings=date``).
    The reconciler reads these as finviz's NEXT contribution while deriving
    finviz's LAST from the per-quarter history — so finviz's real scheduled
    next date augments nasdaq/yahoo coverage WITHOUT putting future
    NaN-actual rows in the per-quarter history.

    The caller MUST reconcile the affected tickers afterwards: a bare
    next-only row carries ``last=NaT`` until the reconcile rebuilds last
    from history. Mirrors ``nasdaq_fill``'s cache-write + reconcile.

    Returns True when there was nothing to write OR the write succeeded;
    False when the cache write raised — so the caller can KEEP the buffer
    and retry on the next flush rather than silently dropping the dates."""
    if not next_pending:
        return True
    from . import earnings_cache as ec
    rows = [{
        "ticker": t,
        "last_earnings": pd.NaT,
        "next_earnings": pd.Timestamp(d),
        "updated_at": now,
        "source": "finviz",
    } for t, d in next_pending.items()]
    try:
        existing = ec.load_earnings_cache()
        ec._merge_and_save(rows, existing)
        return True
    except Exception as exc:
        log.warning("Finviz next-date cache write failed: %s", exc)
        return False


def _finalize_fill(affected_tickers: list[str]) -> None:
    """End-of-fill: refresh YoY across the parquet + one reconcile."""
    if not affected_tickers:
        return
    from . import earnings_history as eh
    # Serialize the read→recompute→write against concurrent fills (the
    # launch-time smart refresh runs finviz + zacks workers at once). The
    # per-flush writes already take this lock; the finalize must too, or it
    # reads a stale snapshot and writes back over another worker's rows.
    with eh.HISTORY_WRITE_LOCK:
        existing = eh.load_earnings_history()
        if existing is not None and not existing.empty:
            existing = eh.compute_yoy_columns(existing)
            eh.save_earnings_history(existing, sort=True)
    from . import earnings_reconcile  # lazy: cycle-safe
    earnings_reconcile.reconcile_earnings_dates(
        affected_tickers=list(set(affected_tickers))
    )


# ──────────────────────────────────────────────────────────────────────
# Inner loop (shared by bulk / gap / spot)
# ──────────────────────────────────────────────────────────────────────

def _fill_via_finviz(
    tickers: list[str],
    blacklist: set[str],
    *,
    progress_cb=None,
    stop_flag: Optional[list[bool]] = None,
    flush_every: int = 25,
    label: str = "Finviz fill",
    on_block_callback=None,
    on_empty_identified=None,
    failed_cb=None,
    resume_from_checkpoint: bool = False,
) -> tuple[int, int]:
    """Common loop body for bulk / gap / spot finviz fills.

    Args:
        tickers: ordered symbols to process. The caller MUST pre-union
            the universe OHLCV blacklist + ETF/ADR auto-skip + finviz
            blacklist into ``blacklist``; we re-apply defensively.
        on_block_callback: ``(consec_errors, blocks_so_far) -> "continue"|"stop"``.
        on_empty_identified: ``(symbol) -> None`` per ticker finviz
            doesn't cover (no earningsData) — caller adds to the finviz
            blacklist live.
        failed_cb: ``(symbol, kind) -> None`` per-ticker classification.

    Returns ``(filled_count, error_count)``.
    """
    work = [t for t in tickers if t and t not in blacklist]
    if not work:
        log.info("%s: no tickers to process", label)
        return 0, 0

    run_id = earnings_raw.new_run_id()
    completed: set[str] = set()
    if resume_from_checkpoint:
        cp = _load_checkpoint()
        if cp is not None and cp.completed:
            completed = set(cp.completed)
            run_id = cp.run_id or run_id
            log.info("%s: resuming run %s with %d tickers already complete",
                     label, run_id, len(completed))

    log.info("%s: %d tickers to process (run_id=%s)", label, len(work), run_id)

    pending: dict[str, list[dict]] = {}
    raw_pending: list[dict] = []
    # Seed with prior-session completed tickers so a kill+resume bulk still
    # reconciles their dates at end-of-run (idempotent). See finnhub_fill.
    affected_total: list[str] = list(completed)
    next_pending: dict[str, pd.Timestamp] = {}  # finviz next dates buffered for the next durable flush
    next_tickers: set[str] = set()              # cumulative — every ticker that got a next date (for the end reconcile)
    filled = 0
    errors = 0
    consec_errors = 0
    blocks_so_far = 0
    total = len(work)

    today_ts = pd.Timestamp.today().normalize()
    cutoff = today_ts - pd.DateOffset(years=config.EARNINGS_HISTORY_YEARS)

    first_fail_idx: Optional[int] = None

    def _flush_raw():
        if not raw_pending:
            return
        try:
            earnings_raw.append_finviz_rows(raw_pending, run_id)
        except Exception as exc:
            log.warning("Finviz raw-layer write failed: %s", exc)
        raw_pending.clear()

    def _persist_progress():
        _flush_pending_to_disk(pending)
        _flush_raw()
        # Durably write finviz next dates BEFORE the checkpoint advances, so
        # a ticker is never recorded as `completed` while its next date
        # lives only in memory (a hard crash + resume would skip it →
        # silent loss of its next_earnings). Clear the buffer only on a
        # successful write so a failed flush retries on the next persist.
        if _flush_next_dates_to_cache(next_pending, datetime.now()):
            next_pending.clear()
        _save_checkpoint(_Checkpoint(
            run_id=run_id,
            started_at=datetime.now().isoformat(timespec="seconds"),
            completed=sorted(completed),
        ))

    i = 0
    while i < total:
        if stop_flag and stop_flag[0]:
            log.info("%s: stopped at %d/%d", label, i, total)
            break

        sym = work[i]
        if sym in completed:
            i += 1
            continue

        fetch_now = datetime.now()
        result = _fetch_one_ticker(sym, cutoff=cutoff, now=fetch_now)

        # Capture finviz's next scheduled date (rides on a dropped forward
        # row) for any successful fetch — buffered for the next durable
        # flush (in _persist_progress) and tracked cumulatively for the
        # end-of-run reconcile.
        if result.failure is None and result.next_date is not None:
            next_pending[sym] = result.next_date
            next_tickers.add(sym)

        if result.failure is None and result.rows:
            pending[sym] = result.rows
            for raw in result.raw_records:
                raw_pending.append(raw)
            affected_total.append(sym)
            completed.add(sym)
            filled += 1
            consec_errors = 0
            first_fail_idx = None
        elif result.failure is None and not result.rows:
            # 200 OK but every entry filtered (all forward / all >5y).
            completed.add(sym)
            consec_errors = 0
            first_fail_idx = None
        elif result.is_empty:
            # finviz doesn't cover the ticker — blacklist; NOT a block.
            errors += 1
            completed.add(sym)
            consec_errors = 0
            first_fail_idx = None
            if on_empty_identified is not None:
                try:
                    on_empty_identified(sym)
                except Exception:
                    pass
            if failed_cb is not None:
                try:
                    failed_cb(sym, finviz_client.FAIL_EMPTY)
                except Exception:
                    pass
        else:
            # Real failure (rate_limited / blocked / server / network /
            # parse / forbidden) — counts toward the block streak.
            errors += 1
            consec_errors += 1
            if first_fail_idx is None:
                first_fail_idx = i
            log.warning("%s: %s → fetch failure (kind=%s, consec=%d)",
                        label, sym, result.failure or "unknown", consec_errors)
            if failed_cb is not None:
                try:
                    failed_cb(sym, result.failure or "unknown")
                except Exception:
                    pass

        if progress_cb:
            progress_cb(i + 1, total)

        if len(pending) >= flush_every:
            _persist_progress()
            log.info(
                "%s: flushed %d ticker(s) (%d/%d processed, "
                "%d filled, %d errors so far)",
                label, len(pending), i + 1, total, filled, errors,
            )
            pending = {}

        if (i + 1) % 200 == 0:
            log.info("%s: %d/%d processed (%d filled, %d errors, %d blocks)",
                     label, i + 1, total, filled, errors, blocks_so_far)

        # Block trigger
        if consec_errors >= config.FINVIZ_CONSEC_BLOCK_LIMIT:
            blocks_so_far += 1
            pause_sec = min(
                config.FINVIZ_INITIAL_BLOCK_PAUSE_SEC * (2 ** (blocks_so_far - 1)),
                config.FINVIZ_MAX_BLOCK_PAUSE_SEC,
            )
            log.warning(
                "%s: %d consecutive failures (block #%d) — pausing %ds",
                label, consec_errors, blocks_so_far, pause_sec,
            )
            time.sleep(pause_sec)

            if blocks_so_far >= config.FINVIZ_MAX_BLOCKS_PER_RUN:
                if on_block_callback is not None:
                    decision = on_block_callback(consec_errors, blocks_so_far)
                    if decision == "stop":
                        log.info("%s: on_block_callback returned 'stop'", label)
                        break
                else:
                    log.warning(
                        "%s: hit %d blocks with no callback configured — halting",
                        label, blocks_so_far,
                    )
                    break

            # Rewind to the first ticker in the failure window and retry.
            rewind_to = first_fail_idx if first_fail_idx is not None else i
            rewind_count = i + 1 - rewind_to
            errors = max(0, errors - rewind_count)
            consec_errors = 0
            first_fail_idx = None
            i = rewind_to
            log.info("%s: rewinding %d ticker(s) to retry block window (i=%d)",
                     label, rewind_count, i)
            if progress_cb:
                progress_cb(i, total)
            continue

        i += 1

    _persist_progress()   # flushes remaining history + next dates + checkpoint
    _finalize_fill(affected_total)
    # finviz next dates for tickers that yielded no NEW history row still
    # need a reconcile to fold their next_earnings into the dates row
    # (_finalize_fill only reconciles history-affected tickers). Use the
    # cumulative next_tickers set — next_pending was drained by the flushes.
    next_only = sorted(next_tickers - set(affected_total))
    if next_only:
        try:
            from . import earnings_reconcile  # lazy: cycle-safe
            earnings_reconcile.reconcile_earnings_dates(affected_tickers=next_only)
        except Exception as exc:
            log.warning("Reconcile of finviz next-only tickers failed: %s", exc)

    if not (stop_flag and stop_flag[0]):
        _clear_checkpoint()

    log.info("%s done: %d filled, %d errors, %d blocks",
             label, filled, errors, blocks_so_far)
    return filled, errors


# ──────────────────────────────────────────────────────────────────────
# Public entrypoints
# ──────────────────────────────────────────────────────────────────────

def bulk_fill_finviz(
    universe_symbols: list[str],
    blacklist: set[str],
    *,
    progress_cb=None,
    stop_flag: Optional[list[bool]] = None,
    # See bulk_fill_finnhub: 100 (vs gap/spot 25) cuts the full-parquet
    # rewrite count ~4× over a multi-hour run; checkpoint/raw cover resume.
    flush_every: int = 100,
    on_block_callback=None,
    on_empty_identified=None,
    failed_cb=None,
    resume_from_checkpoint: bool = True,
) -> tuple[int, int]:
    """Iterate the universe and scrape finviz earnings for each ticker.
    One request per ticker at the deliberately-slow finviz pace — a
    ~10k-ticker (ex-ETF/ADR) universe runs ~11 hours, intended for an
    overnight run. Resumes from checkpoint by default.

    ``blacklist`` MUST already include the universe OHLCV blacklist +
    ETF/ADR auto-skip + finviz blacklist (the GUI assembles this)."""
    return _fill_via_finviz(
        universe_symbols, blacklist,
        progress_cb=progress_cb, stop_flag=stop_flag,
        flush_every=flush_every,
        label="Finviz bulk fill",
        on_block_callback=on_block_callback,
        on_empty_identified=on_empty_identified,
        failed_cb=failed_cb,
        resume_from_checkpoint=resume_from_checkpoint,
    )


def gap_fill_finviz(
    gap_tickers: list[str],
    blacklist: set[str],
    *,
    progress_cb=None,
    stop_flag: Optional[list[bool]] = None,
    flush_every: int = 25,
    on_block_callback=None,
    on_empty_identified=None,
    failed_cb=None,
) -> tuple[int, int]:
    """Iterate only ``gap_tickers`` (universe minus tickers already
    carrying any ``source=finviz`` row). Caller computes gaps via
    ``find_finviz_gap_tickers``."""
    return _fill_via_finviz(
        gap_tickers, blacklist,
        progress_cb=progress_cb, stop_flag=stop_flag,
        flush_every=flush_every,
        label="Finviz gap fill",
        on_block_callback=on_block_callback,
        on_empty_identified=on_empty_identified,
        failed_cb=failed_cb,
        resume_from_checkpoint=False,
    )


def spot_fill_finviz(
    symbol: str,
    blacklist: set[str],
    *,
    on_empty_identified=None,
) -> tuple[int, str]:
    """Fetch one ticker on demand. Returns ``(filled_count, status)``
    where status ∈ {"ok", "empty", "blacklisted", "invalid",
    "no_rows_in_window", FAIL_*}."""
    sym = (symbol or "").upper().strip()
    if not sym:
        return 0, "invalid"
    if sym in blacklist:
        return 0, "blacklisted"

    today_ts = pd.Timestamp.today().normalize()
    cutoff = today_ts - pd.DateOffset(years=config.EARNINGS_HISTORY_YEARS)
    now = datetime.now()

    result = _fetch_one_ticker(sym, cutoff=cutoff, now=now)
    if result.is_empty:
        if on_empty_identified is not None:
            try:
                on_empty_identified(sym)
            except Exception:
                pass
        return 0, "empty"
    if result.failure is not None:
        return 0, str(result.failure)
    # Capture finviz's next scheduled date (if any) regardless of whether
    # there were past rows to write — it routes to earnings_dates. Only
    # reconcile when the cache write actually succeeded.
    next_written = (
        result.next_date is not None
        and _flush_next_dates_to_cache({sym: result.next_date}, now)
    )
    if not result.rows:
        if next_written:
            try:
                from . import earnings_reconcile  # lazy: cycle-safe
                earnings_reconcile.reconcile_earnings_dates(affected_tickers=[sym])
            except Exception as exc:
                log.warning("Reconcile of finviz next-only spot failed: %s", exc)
        return 0, "no_rows_in_window"

    run_id = earnings_raw.new_run_id()
    _flush_pending_to_disk({sym: result.rows}, is_final=True)
    if result.raw_records:
        try:
            earnings_raw.append_finviz_rows(result.raw_records, run_id)
        except Exception as exc:
            log.warning("Finviz raw-layer write failed: %s", exc)
    _finalize_fill([sym])
    return len(result.rows), "ok"


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

def find_finviz_gap_tickers(
    universe_symbols: list[str], blacklist: set[str],
) -> list[str]:
    """Return tickers in ``universe ∩ (not blacklist)`` whose
    ``source=finviz`` row count in earnings_history.parquet is 0."""
    from . import earnings_history as eh
    have_finviz: set[str] = set()
    df = eh.load_earnings_history()
    if df is not None and not df.empty and "source" in df.columns:
        fv = df.loc[df["source"] == "finviz"]
        have_finviz = set(fv["ticker"].astype(str).unique())
    return [
        t for t in universe_symbols
        if t not in blacklist and t not in have_finviz
    ]
