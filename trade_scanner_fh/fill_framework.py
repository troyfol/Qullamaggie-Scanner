"""fill_framework.py — shared bulk / gap / spot fill orchestration.

Extracted from ``finviz_fill.py`` / ``finnhub_fill.py``, whose checkpoint
trio, per-source parquet flush, end-of-fill finalize, and resilient
fetch loop (per-N flush, consecutive-failure block backoff
``INITIAL * 2**(blocks-1)``, rewind-to-first-failure, resumable
checkpoint) were verbatim duplicates modulo the source string / config
prefix.

Hook resolution contract
------------------------
The per-source modules keep thin module-level wrappers under their
ORIGINAL private names (``_fetch_one_ticker``, ``_load_checkpoint``,
``_save_checkpoint``, ``_flush_pending_to_disk``, ...) because tests
monkeypatch them by module attribute. ``run_fill_loop`` therefore
resolves every hook THROUGH the fill module at call time
(``spec.module._fetch_one_ticker(...)``) — never bind a hook function
at definition/import time or monkeypatching breaks.

Genuinely source-specific behavior stays in the owning module and is
injected per run:

* finviz's next-date pipeline rides the ``after_fetch`` /
  ``persist_extra`` / ``after_finalize`` hooks;
* finnhub's FAIL_AUTH halt is the spec's ``halt_failure_kind`` and its
  ``verify_api_key`` probe is the ``on_block_pause`` hook;
* finnhub's ``delay_sec`` pacing is a plain loop parameter.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Callable, Optional

import pandas as pd

from . import config
from . import earnings_raw


# Failure-kind value every source's client uses for "page fetched but
# un-parseable" (finviz FAIL_PARSE, finnhub FAIL_PARSE, zacks
# FAIL_PARSE_ERROR are all this string). The parse-failure spike alarm
# in `run_fill_loop` keys on it.
PARSE_FAILURE_KIND = "parse_error"


# ──────────────────────────────────────────────────────────────────────
# Checkpoint persistence (resumable bulk runs)
# ──────────────────────────────────────────────────────────────────────

@dataclass
class Checkpoint:
    run_id: str
    started_at: str  # ISO datetime
    completed: list[str]


def save_checkpoint(path: Path, cp: Checkpoint, log: logging.Logger) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        config.atomic_write_text(
            path,
            json.dumps({
                "run_id": cp.run_id,
                "started_at": cp.started_at,
                "completed": cp.completed,
            }),
        )
    except OSError as exc:
        log.warning("Checkpoint write failed: %s", exc)


def load_checkpoint(path: Path, log: logging.Logger) -> Optional[Checkpoint]:
    if not path.exists():
        return None
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
        return Checkpoint(
            run_id=str(d.get("run_id", "")),
            started_at=str(d.get("started_at", "")),
            completed=[str(t) for t in (d.get("completed") or []) if t],
        )
    except (OSError, ValueError) as exc:
        log.warning("Checkpoint read failed: %s", exc)
        return None


def clear_checkpoint(path: Path, log: logging.Logger) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        log.debug("Checkpoint clear failed: %s", exc)


# ──────────────────────────────────────────────────────────────────────
# Persistence — write to earnings_history.parquet (per-source replace)
# ──────────────────────────────────────────────────────────────────────

def flush_pending_to_disk(
    pending: dict[str, list[dict]],
    *,
    source: str,
    is_final: bool = False,
) -> None:
    """Merge ``pending`` (ticker → list of ``source``-tagged rows) into
    earnings_history.parquet, replacing only the (ticker, source) rows
    for those tickers — other sources' rows for the same ticker are
    preserved ((ticker, source) soft-PK).

    Replacement is keyed on ``new_df["ticker"]`` (the actual ticker value
    being written) NOT ``pending.keys()`` (the queried symbol). The two
    used to align by convention, but Finnhub's response symbol can differ
    from what was queried (e.g. ENB → ENB.TO canonicalization), causing
    the pending-key form to miss the existing-row form and pile
    duplicates run-over-run. Using the row's own ticker is correct
    regardless of how it was sourced.
    """
    from . import earnings_history as eh

    if not pending:
        return

    # Same cross-worker R-M-W race fix as earnings_history's own
    # _flush_pending_to_disk: the on-disk save is atomic, but the
    # load → merge → save cycle is not. Acquire the shared lock so
    # concurrent fills serialize their merges instead of overwriting
    # each other's appended rows.
    with eh.HISTORY_WRITE_LOCK:
        existing = eh.load_earnings_history()
        new_rows: list[dict] = []
        for rows in pending.values():
            new_rows.extend(rows)
        new_df = pd.DataFrame(new_rows, columns=eh.COLUMNS)
        # Ingest-time price-relative EPS artifact guard (reverse-split
        # nano-caps): source EPS actuals can carry pre-split / total-NI
        # values for sub-$5 tickers; null them before they hit disk.
        new_df = eh.sanitize_eps_artifacts(new_df)

        # Defensive: if any caller violates the (ticker, period_ending)
        # invariant within a single batch, dedup here keeping last.
        # _fetch_one_ticker is supposed to handle this, but a belt-and-
        # suspenders check at the parquet boundary catches anything
        # that slips.
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
                & (existing["source"] == source)
            )
            keep = existing.loc[~mask_replace]
            combined = pd.concat([keep, new_df], ignore_index=True)
        else:
            combined = new_df

        eh.save_earnings_history(combined, sort=is_final)


def find_gap_tickers(
    universe_symbols: list[str], blacklist: set[str], *, source: str,
) -> list[str]:
    """Return tickers in ``universe ∩ (not blacklist)`` whose
    ``source=<source>`` row count in earnings_history.parquet is 0."""
    from . import earnings_history as eh
    have: set[str] = set()
    df = eh.load_earnings_history()
    if df is not None and not df.empty and "source" in df.columns:
        sub = df.loc[df["source"] == source]
        have = set(sub["ticker"].astype(str).unique())
    return [
        t for t in universe_symbols
        if t not in blacklist and t not in have
    ]


def finalize_fill(affected_tickers: list[str]) -> None:
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
            # Refresh YoY columns across the whole parquet so newly-fetched
            # rows back-fill their current-year counterparts' yoy_*_pct.
            existing = eh.compute_yoy_columns(existing)
            eh.save_earnings_history(existing, sort=True)
    from . import earnings_reconcile  # lazy: cycle-safe
    earnings_reconcile.reconcile_earnings_dates(
        affected_tickers=list(set(affected_tickers))
    )


# ──────────────────────────────────────────────────────────────────────
# The shared fetch loop (bulk / gap)
# ──────────────────────────────────────────────────────────────────────

@dataclass
class FillSpec:
    """Source-level knobs for ``run_fill_loop``.

    ``module`` is the fill module itself — every ``_fetch_one_ticker`` /
    ``_load_checkpoint`` / ``_save_checkpoint`` / ``_clear_checkpoint`` /
    ``_flush_pending_to_disk`` / ``_finalize_fill`` call is resolved
    through it at call time so test monkeypatches keep working.
    """
    module: ModuleType
    log: logging.Logger          # the fill module's logger (keeps attribution)
    config_prefix: str           # "FINVIZ" / "FINNHUB" → <prefix>_CONSEC_BLOCK_LIMIT etc.
    fail_empty: str              # the client's FAIL_EMPTY sentinel (for failed_cb)
    append_raw_rows: Callable[[list[dict], str], None]
    raw_label: str               # "Finviz" / "Finnhub" — raw-layer warning prefix
    halt_failure_kind: Optional[str] = None   # e.g. finnhub FAIL_AUTH — break immediately
    halt_log_message: Optional[str] = None    # log.error format (gets ``label``) on halt
    warn_on_fetch_failure: bool = False       # finviz logs each real per-ticker failure
    # Failure kinds that are a permanent per-TICKER coverage gap rather than
    # the source blocking us (e.g. finnhub FAIL_FORBIDDEN — 403, symbol not in
    # the account's plan). Handled like an empty response: routed to the source
    # skip list via ``on_empty_identified`` and explicitly NOT counted toward
    # the consecutive-block streak, so a handful of out-of-plan symbols can't
    # trigger a backoff pause or a rewind-and-retry of the failure window.
    skip_failure_kinds: tuple[str, ...] = ()


def run_fill_loop(
    spec: FillSpec,
    tickers: list[str],
    blacklist: set[str],
    *,
    fetch_kwargs: dict,
    progress_cb=None,
    stop_flag: Optional[list[bool]] = None,
    flush_every: int = 25,
    label: str,
    on_block_callback=None,
    on_empty_identified=None,
    failed_cb=None,
    delay_sec: float = 0.0,
    resume_from_checkpoint: bool = False,
    after_fetch=None,
    persist_extra=None,
    after_finalize=None,
    on_block_pause=None,
) -> tuple[int, int]:
    """Common loop body for the per-source bulk / gap fills.

    Args:
        tickers: ordered symbols to process. The caller MUST pre-union
            the source-appropriate blacklists; we re-apply defensively.
        fetch_kwargs: source-specific keyword args forwarded to the fill
            module's ``_fetch_one_ticker`` (cutoff / calendar window);
            ``now`` is supplied per call by the loop.
        on_block_callback: ``(consec_errors, blocks_so_far) -> "continue"|"stop"``.
        on_empty_identified: ``(symbol) -> None`` per ticker the source
            doesn't cover — caller adds to the source blacklist live.
        failed_cb: ``(symbol, kind) -> None`` per-ticker classification.
        delay_sec: extra per-ticker pacing sleep (finnhub).
        after_fetch: ``(symbol, result) -> None`` — invoked right after
            every fetch (finviz next-date capture).
        persist_extra: ``() -> None`` — invoked inside each durable
            persist AFTER history + raw but BEFORE the checkpoint
            advances (finviz next-date flush).
        after_finalize: ``(affected_total) -> None`` — invoked after the
            module's ``_finalize_fill`` (finviz next-only reconcile).
        on_block_pause: ``() -> bool`` — invoked after each block pause;
            returning False halts the run (finnhub key probe).

    Returns ``(filled_count, error_count)``. ``filled_count`` counts
    every ticker that produced at least one history row written to
    disk; ``error_count`` counts every ticker that failed (including
    empty-response identifications).
    """
    log = spec.log
    mod = spec.module

    def _cfg(suffix: str):
        # Read block constants through config at call time so tests can
        # override them mid-run, same as the pre-extraction loops.
        return getattr(config, f"{spec.config_prefix}_{suffix}")

    work = [t for t in tickers if t and t not in blacklist]
    if not work:
        log.info("%s: no tickers to process", label)
        return 0, 0

    # Resume from checkpoint if requested
    run_id = earnings_raw.new_run_id()
    completed: set[str] = set()
    if resume_from_checkpoint:
        cp = mod._load_checkpoint()
        if cp is not None and cp.completed:
            completed = set(cp.completed)
            run_id = cp.run_id or run_id
            log.info("%s: resuming run %s with %d tickers already complete",
                     label, run_id, len(completed))

    log.info("%s: %d tickers to process (run_id=%s)", label, len(work), run_id)

    pending: dict[str, list[dict]] = {}
    raw_pending: list[dict] = []
    # Seed with tickers completed in a PRIOR (interrupted) session so the
    # end-of-run reconcile folds them in too. Without this, a kill+resume bulk
    # left session-1 tickers' history on disk but never reconciled their dates
    # into earnings_dates.parquet (session 2 skips them via `completed` and
    # excluded them from affected_total). reconcile is idempotent, so re-listing
    # already-reconciled names is harmless.
    affected_total: list[str] = list(completed)
    filled = 0
    errors = 0
    consec_errors = 0
    blocks_so_far = 0
    total = len(work)

    # First-failed-index for the current consecutive-failure window —
    # used to rewind on block recovery so the failed window retries
    # under (potentially) fresh state.
    first_fail_idx: Optional[int] = None

    # Parse-failure spike alarm (B2): fetch attempts vs. parse_error
    # classifications across the run. A spiking fraction means the
    # source changed its page layout (parser break) — see the halt
    # check inside the loop. `spike_halted` keeps the checkpoint from
    # being cleared so the run can resume once the parser is fixed.
    spike_attempts = 0
    spike_parse_fails = 0
    spike_halted = False

    def _flush_raw():
        if not raw_pending:
            return
        try:
            spec.append_raw_rows(raw_pending, run_id)
        except Exception as exc:
            log.warning("%s raw-layer write failed: %s", spec.raw_label, exc)
        raw_pending.clear()

    def _persist_progress():
        # Save the consumer parquet, then the raw layer, then any source-
        # specific extras (e.g. finviz next dates — durably written BEFORE
        # the checkpoint advances so a ticker is never recorded as
        # `completed` while side data lives only in memory), then the
        # checkpoint (in that order so the checkpoint never claims
        # tickers whose history rows aren't on disk yet).
        mod._flush_pending_to_disk(pending)
        _flush_raw()
        if persist_extra is not None:
            persist_extra()
        mod._save_checkpoint(Checkpoint(
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
        result = mod._fetch_one_ticker(sym, now=fetch_now, **fetch_kwargs)

        spike_attempts += 1
        if result.failure == PARSE_FAILURE_KIND:
            spike_parse_fails += 1

        if after_fetch is not None:
            after_fetch(sym, result)

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
            # 200 OK but every record was filtered (forward / pre-cutoff).
            completed.add(sym)
            consec_errors = 0
            first_fail_idx = None
        elif result.is_empty:
            # Source doesn't cover the ticker (ETF / fund / IPO) —
            # blacklist via callback; does NOT count toward block streak.
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
                    failed_cb(sym, spec.fail_empty)
                except Exception:
                    pass
        elif (spec.halt_failure_kind is not None
              and result.failure == spec.halt_failure_kind):
            # Bad / revoked key. Halt immediately — no point churning.
            log.error(spec.halt_log_message, label)
            if failed_cb is not None:
                try:
                    failed_cb(sym, result.failure)
                except Exception:
                    pass
            errors += 1
            break
        elif result.failure in spec.skip_failure_kinds:
            # Permanent per-ticker coverage gap the source reports as a hard
            # failure but that will NEVER succeed on retry (e.g. finnhub 403 —
            # symbol not in the account's plan). Route it to the source skip
            # list like an empty response and reset the streak: it's a property
            # of the ticker, NOT the source blocking us, so it must not count
            # toward the consecutive-block limit, trigger a backoff pause, or
            # rewind-and-retry the window. The skip is persisted at run end via
            # the same on_empty_identified → skip-list path as ETF/uncovered.
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
                    failed_cb(sym, result.failure)
                except Exception:
                    pass
        else:
            # Real failure (rate_limited / blocked / server / network /
            # parse / forbidden) — counts toward the block streak.
            errors += 1
            consec_errors += 1
            if first_fail_idx is None:
                first_fail_idx = i
            if spec.warn_on_fetch_failure:
                log.warning("%s: %s → fetch failure (kind=%s, consec=%d)",
                            label, sym, result.failure or "unknown", consec_errors)
            if failed_cb is not None:
                try:
                    failed_cb(sym, result.failure or "unknown")
                except Exception:
                    pass

        if progress_cb:
            progress_cb(i + 1, total)

        # Parse-failure spike alarm (B2): a high fraction of parse_error
        # results means the source changed its page layout — a parser
        # break on OUR side, not N bad tickers. Halt loudly instead of
        # churning the rest of the universe. Parse failures never route
        # to on_empty_identified, so the affected tickers are NOT
        # blacklisted; the checkpoint survives (see spike_halted) so the
        # run can resume once the parser is fixed. Thresholds read at
        # call time so tests / Settings overrides apply mid-run.
        if (spike_attempts >= config.PARSE_SPIKE_MIN_SAMPLE
                and spike_parse_fails * 100.0
                >= config.PARSE_SPIKE_FAIL_PCT * spike_attempts):
            log.error(
                "%s: PARSE-FAILURE SPIKE — %d of %d fetches (%.0f%%) were "
                "parse errors; HALTING the run (the source's page format "
                "has likely changed; affected tickers were NOT blacklisted)",
                label, spike_parse_fails, spike_attempts,
                spike_parse_fails * 100.0 / spike_attempts,
            )
            spike_halted = True
            break

        # Per-N flush
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
        if consec_errors >= _cfg("CONSEC_BLOCK_LIMIT"):
            blocks_so_far += 1
            pause_sec = min(
                _cfg("INITIAL_BLOCK_PAUSE_SEC") * (2 ** (blocks_so_far - 1)),
                _cfg("MAX_BLOCK_PAUSE_SEC"),
            )
            log.warning(
                "%s: %d consecutive failures (block #%d) — pausing %ds",
                label, consec_errors, blocks_so_far, pause_sec,
            )
            time.sleep(pause_sec)

            # Source-specific post-pause probe (finnhub API-key check) —
            # False means the run can't usefully continue.
            if on_block_pause is not None and not on_block_pause():
                break

            if blocks_so_far >= _cfg("MAX_BLOCKS_PER_RUN"):
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

            # Rewind to first-in-failure-window. Every ticker in that
            # window almost certainly failed for the block, not for
            # its own sake; retry them all under fresh state.
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
            if delay_sec > 0:
                time.sleep(delay_sec)
            continue

        if delay_sec > 0:
            time.sleep(delay_sec)
        i += 1

    # Final flush (history + extras + checkpoint) + reconcile
    _persist_progress()
    mod._finalize_fill(affected_total)
    if after_finalize is not None:
        after_finalize(affected_total)

    # Successful end-of-run clears the checkpoint so the next bulk
    # starts fresh (instead of resuming a completed run). A spike halt
    # keeps it — the run should resume where it left off after the
    # parser is fixed.
    if not (stop_flag and stop_flag[0]) and not spike_halted:
        mod._clear_checkpoint()

    log.info("%s done: %d filled, %d errors, %d blocks",
             label, filled, errors, blocks_so_far)
    return filled, errors
