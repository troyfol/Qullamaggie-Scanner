"""
Watchlist diffing — persist each completed scan's per-period ticker set
and diff it against the previous run of the same (preset, period) key.

Feature F2 (2026-06): after every scan the GUI records, per period, the
set of tickers that passed all filters. The NEXT run of the same key
(preset name or "adhoc" for unsaved settings, plus the period label)
is diffed against that snapshot:

  NEW     — in the current run, absent from the prior run
  DROPPED — in the prior run, absent from the current run

("Returned after absence" is intentionally out of scope — only NEW vs
carryover is tracked, one level deep.)

Storage: ``scanner_data/scan_history.json`` —

    {
      "version": 1,
      "latest": {
        "<preset-or-adhoc>": {
          "<period-label>": {"timestamp": "...", "symbols": [...]}
        }
      },
      "summary": [
        {"timestamp": "...", "preset": "...", "period": "...", "count": N},
        ...
      ]
    }

Only the LATEST prior set per key is kept (single-level diff), plus a
bounded rolling ``summary`` list (newest last) for the future scheduler
to extend. Writes are atomic (``config.atomic_write_text``) and reads
are corrupt-file-safe: any unreadable / malformed file is treated as
"no prior run" rather than raising.

Growth bounds (2026-06): date-stamped one-off period labels (sequenced
chunks "2025-01-01 → 2025-02-01", "Custom <start> → <end>", and the
no-checkbox date-picker fallback "<start> → <end>") are never recorded
— a future run almost never reproduces the label, so each one would be
a permanent dead key plus a "no prior run" noise line. Additionally,
``latest`` entries older than ``LATEST_MAX_AGE_DAYS`` are pruned on
every record pass, so renamed/deleted presets can't leave orphaned
keys forever.

Zero GUI dependencies — the MainWindow glue calls
``record_scan_results`` and renders the returned ``ScanDiff`` objects
into the "Chg" column + log-panel lines.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from . import config

log = logging.getLogger("scanner.scan_history")

# Key used when the scan was run without a named preset selected.
ADHOC_KEY = "adhoc"

# Rolling summary bound — newest entries win. Generous enough for a
# scheduler appending several runs/day for months without churn.
SUMMARY_MAX_ENTRIES = 200

# Cap on how many DROPPED symbols are spelled out in the log line.
MAX_DROPPED_IN_LOG = 20

# 'latest' entries older than this are pruned on every record pass —
# bounds the store and clears keys orphaned by renamed/deleted presets.
# Generous enough that any preset run even monthly keeps its baseline.
LATEST_MAX_AGE_DAYS = 90

HISTORY_VERSION = 1


def is_one_off_period(label) -> bool:
    """True for date-stamped one-shot period labels that almost never
    recur and therefore must not become permanent ``latest`` keys:
    sequenced-run chunks (``"2025-01-01 → 2025-02-01"``), custom ranges
    (``"Custom <start> → <end>"``), and the no-checkbox date-picker
    fallback (``"<start> → <end>"``). The fixed timeframe labels
    (``1D``/``1W``/``1M``/...) never contain the arrow."""
    s = str(label)
    return s.startswith("Custom ") or "→" in s


def prune_latest(history: dict, now: Optional[datetime] = None) -> int:
    """Drop ``latest`` entries whose timestamp is older than
    ``LATEST_MAX_AGE_DAYS`` (or unparseable), and any preset key left
    empty afterwards. Mutates ``history`` in place; returns the number
    of (preset, period) entries removed. Shape-tolerant — malformed
    slots are treated as prunable rather than raising."""
    now = now if now is not None else datetime.now()
    cutoff = now - timedelta(days=LATEST_MAX_AGE_DAYS)
    latest = history.get("latest")
    if not isinstance(latest, dict):
        return 0
    removed = 0
    for preset in list(latest.keys()):
        per_preset = latest[preset]
        if not isinstance(per_preset, dict):
            del latest[preset]
            removed += 1
            continue
        for period in list(per_preset.keys()):
            entry = per_preset[period]
            ts_raw = entry.get("timestamp") if isinstance(entry, dict) else None
            stale = True
            if isinstance(ts_raw, str):
                try:
                    stale = datetime.fromisoformat(ts_raw) < cutoff
                except ValueError:
                    stale = True
            if stale:
                del per_preset[period]
                removed += 1
        if not per_preset:
            del latest[preset]
    if removed:
        log.info("scan history: pruned %d stale latest entr%s",
                 removed, "y" if removed == 1 else "ies")
    return removed


def history_path() -> Path:
    """Resolved lazily so tests that monkeypatch ``config.DATA_DIR``
    are honored (mirrors how other scanner_data files are addressed)."""
    return config.DATA_DIR / "scan_history.json"


def _empty_history() -> dict:
    return {"version": HISTORY_VERSION, "latest": {}, "summary": []}


@dataclass
class ScanDiff:
    """Diff of one (preset, period) run against the previous run of the
    same key. ``has_prior`` False means first-ever run for the key."""
    preset: str
    period: str
    new: list[str] = field(default_factory=list)
    dropped: list[str] = field(default_factory=list)
    has_prior: bool = False

    def log_line(self) -> str:
        """One log-panel line for this period's diff."""
        key = f"{self.preset}/{self.period}"
        if not self.has_prior:
            return f"vs last {key} run: no prior run"
        line = (
            f"vs last {key} run: "
            f"{len(self.new)} new, {len(self.dropped)} dropped"
        )
        if self.dropped:
            shown = self.dropped[:MAX_DROPPED_IN_LOG]
            extra = len(self.dropped) - len(shown)
            listing = ", ".join(shown)
            if extra > 0:
                listing += f", +{extra} more"
            line += f" (DROPPED: {listing})"
        return line


def load_history(path: Optional[Path] = None) -> dict:
    """Load the scan-history store. Missing, unreadable, or malformed
    files all yield a fresh empty structure (= "no prior run") — the
    diff feature must never block a scan from completing."""
    p = path if path is not None else history_path()
    try:
        raw = p.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return _empty_history()
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        log.warning("scan_history.json is corrupt — treating as no prior run")
        return _empty_history()
    if not isinstance(data, dict):
        log.warning("scan_history.json has wrong shape — treating as no prior run")
        return _empty_history()
    # Normalize the two payload slots defensively — a hand-edited or
    # partially-written file must degrade to "no prior", not crash.
    if not isinstance(data.get("latest"), dict):
        data["latest"] = {}
    if not isinstance(data.get("summary"), list):
        data["summary"] = []
    data["version"] = HISTORY_VERSION
    return data


def save_history(history: dict, path: Optional[Path] = None) -> None:
    """Persist atomically via ``config.atomic_write_text`` so a crash
    mid-write can never corrupt the store."""
    p = path if path is not None else history_path()
    content = json.dumps(history, indent=1, default=str)
    config.atomic_write_text(p, content)


def _prior_symbols(history: dict, preset: str, period: str) -> Optional[list[str]]:
    """Return the prior run's symbol list for (preset, period), or None
    when there is no usable prior entry. Shape-tolerant."""
    per_preset = history.get("latest", {}).get(preset)
    if not isinstance(per_preset, dict):
        return None
    entry = per_preset.get(period)
    if not isinstance(entry, dict):
        return None
    syms = entry.get("symbols")
    if not isinstance(syms, list):
        return None
    return [str(s) for s in syms]


def diff_and_record(
    history: dict,
    preset: str,
    period: str,
    symbols: list[str],
    timestamp: Optional[str] = None,
) -> ScanDiff:
    """Diff ``symbols`` against the stored prior run for (preset,
    period), then overwrite the stored latest set and append a summary
    entry (bounded). Mutates ``history`` in place; the caller persists
    via ``save_history`` once all periods are recorded."""
    ts = timestamp or datetime.now().isoformat(timespec="seconds")
    # Dedup while preserving scan-result order.
    cur: list[str] = []
    seen: set[str] = set()
    for s in symbols:
        s = str(s)
        if s and s not in seen:
            seen.add(s)
            cur.append(s)

    prior = _prior_symbols(history, preset, period)
    if prior is None:
        diff = ScanDiff(preset=preset, period=period, has_prior=False)
    else:
        prior_set = set(prior)
        cur_set = set(cur)
        diff = ScanDiff(
            preset=preset,
            period=period,
            new=[s for s in cur if s not in prior_set],
            dropped=[s for s in prior if s not in cur_set],
            has_prior=True,
        )

    latest = history.setdefault("latest", {})
    per_preset = latest.setdefault(preset, {})
    if not isinstance(per_preset, dict):  # shape repair
        per_preset = {}
        latest[preset] = per_preset
    per_preset[period] = {"timestamp": ts, "symbols": cur}

    summary = history.setdefault("summary", [])
    if not isinstance(summary, list):  # shape repair
        summary = []
        history["summary"] = summary
    summary.append({
        "timestamp": ts, "preset": preset, "period": period,
        "count": len(cur),
    })
    if len(summary) > SUMMARY_MAX_ENTRIES:
        del summary[: len(summary) - SUMMARY_MAX_ENTRIES]
    return diff


def record_scan_results(
    preset: str,
    period_symbols: dict[str, list[str]],
    path: Optional[Path] = None,
) -> dict[str, ScanDiff]:
    """Convenience wrapper for the GUI: load → prune stale entries →
    diff every period → save once (atomic). Returns
    {period_label: ScanDiff}. Periods are processed in dict insertion
    order (= the scan's period order).

    One-off date-stamped labels (see ``is_one_off_period``) are skipped
    entirely — not diffed, not recorded, no entry in the returned dict —
    so sequenced/custom runs neither pollute the store with dead keys
    nor emit "no prior run" noise lines. The file is only rewritten
    when something actually changed (a recorded period or a prune)."""
    history = load_history(path)
    pruned = prune_latest(history)
    ts = datetime.now().isoformat(timespec="seconds")
    diffs: dict[str, ScanDiff] = {}
    for period, symbols in period_symbols.items():
        if is_one_off_period(period):
            log.debug("scan history: skipping one-off period label %r",
                      period)
            continue
        diffs[period] = diff_and_record(
            history, preset, period, symbols, timestamp=ts,
        )
    if diffs or pruned:
        save_history(history, path)
    return diffs
