"""Append-only raw layer for earnings scrapes.

One parquet per fill *run* per source under
``scanner_data/earnings_raw/{source}/{run_id}.parquet``. Files are
written atomically (temp + rename). Each call to ``append_*_rows``
reads the run's existing file (if any), concatenates the new rows,
and atomically rewrites — so a kill mid-flush leaves the previous
flush intact.

This layer is for replay/audit only, NOT correctness — the consumer
parquets (``earnings_history.parquet``, ``earnings_dates.parquet``)
remain authoritative. The point is that the reconciler logic can be
re-run against frozen captures without re-hitting upstream APIs.

Files older than ``config.RAW_RETENTION_DAYS`` are pruned at app
startup via ``prune_old_raw``.

Per-source schemas:

  zacks:
    ticker, period_ending, report_date, report_time,
    estimated_eps, reported_eps, surprise_eps, surprise_eps_pct,
    estimated_rev, reported_rev, surprise_rev, surprise_rev_pct,
    fetched_at, run_id

  finnhub:
    symbol, period, year, quarter,
    actual, estimate, surprise, surprise_percent,
    revenue_actual, revenue_estimate,
    fetched_at, run_id

  nasdaq:
    ticker, calendar_date, fetched_at, run_id

  yahoo:
    ticker, all_dates_returned (str — semicolon-joined ISO dates),
    fetched_at, run_id
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from . import config

log = logging.getLogger("scanner.earnings_raw")


# Schema columns per source — used both for column ordering on write
# and for empty-frame init when the run file doesn't exist yet.
_ZACKS_COLS = [
    "ticker", "period_ending", "report_date", "report_time",
    "estimated_eps", "reported_eps", "surprise_eps", "surprise_eps_pct",
    "estimated_rev", "reported_rev", "surprise_rev", "surprise_rev_pct",
    "fetched_at", "run_id",
]
_FINNHUB_COLS = [
    "symbol", "period", "year", "quarter",
    "actual", "estimate", "surprise", "surprise_percent",
    "revenue_actual", "revenue_estimate",
    "fetched_at", "run_id",
]
_NASDAQ_COLS = ["ticker", "calendar_date", "fetched_at", "run_id"]
_YAHOO_COLS = ["ticker", "all_dates_returned", "fetched_at", "run_id"]

_SCHEMA_BY_SOURCE = {
    config.RAW_SOURCE_ZACKS: _ZACKS_COLS,
    config.RAW_SOURCE_FINNHUB: _FINNHUB_COLS,
    config.RAW_SOURCE_NASDAQ: _NASDAQ_COLS,
    config.RAW_SOURCE_YAHOO: _YAHOO_COLS,
}


# ──────────────────────────────────────────────────────────────────────
# Run id
# ──────────────────────────────────────────────────────────────────────

def new_run_id() -> str:
    """Generate a unique run id for a fill invocation. Format:
    ``{YYYYMMDDHHMMSS}_{8 hex chars}`` so directory listings sort by
    name in approximate creation order, independent of mtime.
    """
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{stamp}_{uuid.uuid4().hex[:8]}"


# ──────────────────────────────────────────────────────────────────────
# Path helpers
# ──────────────────────────────────────────────────────────────────────

def _source_dir(source: str) -> Path:
    if source not in _SCHEMA_BY_SOURCE:
        raise ValueError(f"unknown raw-layer source: {source!r}")
    return config.RAW_EARNINGS_DIR / source


def _run_file(source: str, run_id: str) -> Path:
    return _source_dir(source) / f"{run_id}.parquet"


# ──────────────────────────────────────────────────────────────────────
# Append
# ──────────────────────────────────────────────────────────────────────

def _append_rows(
    source: str,
    rows: list[dict],
    run_id: str,
    fetched_at: Optional[datetime] = None,
) -> int:
    """Common append path. Reads the run's file (if any), concats new
    rows, atomically rewrites. Stamps ``fetched_at`` and ``run_id`` on
    every row that doesn't already have them. Returns the count of new
    rows written (not the total file size).
    """
    if not rows:
        return 0
    cols = _SCHEMA_BY_SOURCE[source]
    fetched_at = fetched_at or datetime.now()

    enriched = []
    for r in rows:
        out = dict(r)
        out.setdefault("fetched_at", fetched_at)
        out.setdefault("run_id", run_id)
        enriched.append(out)

    new_df = pd.DataFrame(enriched)
    for c in cols:
        if c not in new_df.columns:
            new_df[c] = None
    new_df = new_df[cols]

    path = _run_file(source, run_id)
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        try:
            existing = pd.read_parquet(path)
            combined = pd.concat([existing, new_df], ignore_index=True)
        except Exception as exc:
            # Corrupted/truncated run file — overwrite rather than fail
            # the fill. Loss of audit data is acceptable; loss of fill
            # progress is not.
            log.warning(
                "raw[%s] read of %s failed (%s) — overwriting with new batch",
                source, path.name, exc,
            )
            combined = new_df
    else:
        combined = new_df

    config.atomic_write_parquet(combined, path, engine="pyarrow", index=False)
    return len(new_df)


def append_zacks_rows(
    rows: list[dict], run_id: str, fetched_at: Optional[datetime] = None,
) -> int:
    """Append Zacks-fetched per-quarter rows to the run's raw parquet."""
    return _append_rows(config.RAW_SOURCE_ZACKS, rows, run_id, fetched_at)


def append_finnhub_rows(
    rows: list[dict], run_id: str, fetched_at: Optional[datetime] = None,
) -> int:
    """Append Finnhub /stock/earnings rows verbatim. Phase 2 use; the
    function exists in Phase 1 so the raw layer is feature-complete
    before any source plumbs into it.
    """
    return _append_rows(config.RAW_SOURCE_FINNHUB, rows, run_id, fetched_at)


def append_nasdaq_rows(
    rows: list[dict], run_id: str, fetched_at: Optional[datetime] = None,
) -> int:
    """Append (ticker, calendar_date) pairs from finance-calendars
    Nasdaq sweeps. One row per (ticker, day) the calendar reported."""
    return _append_rows(config.RAW_SOURCE_NASDAQ, rows, run_id, fetched_at)


def append_yahoo_rows(
    rows: list[dict], run_id: str, fetched_at: Optional[datetime] = None,
) -> int:
    """Append yfinance .earnings_dates results. ``all_dates_returned``
    should be a semicolon-joined ISO-date string of every date the
    upstream returned (past + future) — the writer collapses to
    last/next; the raw record preserves the full list for replay.
    """
    return _append_rows(config.RAW_SOURCE_YAHOO, rows, run_id, fetched_at)


# ──────────────────────────────────────────────────────────────────────
# Read (replay / tests)
# ──────────────────────────────────────────────────────────────────────

def read_raw(
    source: str, *,
    run_id: Optional[str] = None,
    since: Optional[datetime] = None,
) -> pd.DataFrame:
    """Read raw rows for a source. Optionally restrict to one
    ``run_id`` or to files with mtime ≥ ``since``."""
    src_dir = _source_dir(source)
    if not src_dir.exists():
        return pd.DataFrame(columns=_SCHEMA_BY_SOURCE[source])

    files = sorted(src_dir.glob("*.parquet"))
    if run_id is not None:
        files = [f for f in files if f.stem == run_id]
    if since is not None:
        threshold = since.timestamp()
        files = [f for f in files if f.stat().st_mtime >= threshold]

    if not files:
        return pd.DataFrame(columns=_SCHEMA_BY_SOURCE[source])

    frames = []
    for f in files:
        try:
            frames.append(pd.read_parquet(f))
        except Exception as exc:
            log.warning("raw[%s] could not read %s: %s", source, f.name, exc)
    if not frames:
        return pd.DataFrame(columns=_SCHEMA_BY_SOURCE[source])
    return pd.concat(frames, ignore_index=True)


def list_run_ids(source: str) -> list[str]:
    """List run_ids on disk for a source, newest-first by mtime."""
    src_dir = _source_dir(source)
    if not src_dir.exists():
        return []
    files = sorted(
        src_dir.glob("*.parquet"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return [f.stem for f in files]


# ──────────────────────────────────────────────────────────────────────
# Pruning (called at startup)
# ──────────────────────────────────────────────────────────────────────

def prune_old_raw(
    *,
    retention_days: Optional[int] = None,
    now: Optional[datetime] = None,
) -> int:
    """Delete raw-layer files older than ``retention_days``. Walks
    every source dir; safe to call repeatedly. Returns count deleted.
    """
    days = retention_days if retention_days is not None else config.RAW_RETENTION_DAYS
    cut = (now or datetime.now()) - timedelta(days=days)
    cut_ts = cut.timestamp()
    deleted = 0
    for source in config.RAW_SOURCES:
        src_dir = _source_dir(source)
        if not src_dir.exists():
            continue
        for f in src_dir.glob("*.parquet"):
            try:
                if f.stat().st_mtime < cut_ts:
                    f.unlink()
                    deleted += 1
            except OSError as exc:
                log.debug("raw prune: failed to remove %s: %s", f, exc)
    if deleted:
        log.info("Pruned %d raw earnings file(s) older than %d days",
                 deleted, days)
    return deleted
