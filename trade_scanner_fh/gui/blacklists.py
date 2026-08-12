"""
Skip / blacklist persistence for the GUI (Step A2 extraction).

MainWindow manages four persisted ticker skip-lists, each a plain text
file under scanner_data/:

    universal OHLCV blacklist   blacklist.txt           "csv"   format
    Zacks-only skip list        zacks_blacklist.txt     "lines" format
    Finnhub-only skip list      finnhub_blacklist.txt   "lines" format
    finviz-only skip list       finviz_blacklist.txt    "lines" format

The eight load/save methods on MainWindow were near-identical
try/read/normalize boilerplate — BlacklistManager centralizes them.
MainWindow keeps thin delegate methods under the original names (GUI
menu wiring and tests reference them), so behavior is unchanged: same
file formats, same error handling, same log messages.

No Qt imports — the module stays importable headless.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from .. import config

# Same logger as main_window — these warnings flowed through the
# "scanner" hierarchy (and thus the GUI log-panel handler) before the
# extraction and must keep doing so. Don't switch to getLogger(__name__):
# that would fall outside the "scanner" root the handlers attach to.
log = logging.getLogger("scanner.gui")


def normalize_ticker(t: str) -> str:
    """Normalize Unicode minus/dash variants to ASCII hyphen."""
    return (t.strip().upper()
            .replace("\u2212", "-")   # minus sign
            .replace("\u2013", "-")   # en dash
            .replace("\u2014", "-"))  # em dash


class BlacklistManager:
    """Load/save one persisted ticker skip-list file.

    Formats (``fmt``):
      ``"csv"``   — single comma-joined line (the universal OHLCV
                    blacklist). Load splits on commas; save writes
                    ``", ".join(sorted(...))`` with no trailing
                    newline. No comment-line support.
      ``"lines"`` — one ticker per line for easy diffing (the
                    per-source skip lists). Load also tolerates commas
                    within a line and skips ``#`` comment lines; save
                    defensively strips embedded newlines/CRs from each
                    ticker (so a crafted upstream symbol — or a
                    clipboard-paste mishap in the manual editor dialog
                    — can't inject phantom entries on the next line)
                    and ends with a trailing newline.

    ``label`` is the human-readable name used in the load-failure
    warning so the log text matches the pre-extraction per-method
    messages exactly.

    **Entry metadata (audit 2026-08-12, INT-5).** ``"lines"`` files now store
    ``TICKER<TAB>YYYY-MM-DD<TAB>reason`` so an entry's age and cause are
    recoverable. Previously they held bare ticker strings — no date, no reason —
    which made a re-validation cadence *impossible to implement*, and combined
    with routing a bare HTTP 404 to the permanent list, one bad afternoon could
    exclude a ticker forever. The live lists had reached 10,246 / 10,048 / 6,394
    entries with no way to tell a real coverage gap from a transient miss.

    Backward compatible in both directions: a bare legacy line loads with an
    unknown date (treated as eligible for re-check), and ``load()`` still returns
    a plain ``set[str]`` so every existing caller is unaffected. Use
    ``load_entries()`` / ``save_entries()`` for the metadata-aware form.
    """

    def __init__(self, path: Path, *, fmt: str = "lines",
                 label: str = "skip list") -> None:
        self.path = Path(path)
        self.fmt = fmt
        self.label = label

    def load(self) -> set[str]:
        """Read the list from disk → normalized ``set[str]``. A missing
        file or any read error (locked/corrupt/unreadable) degrades to
        an empty set with a warning — the loaders run in MainWindow's
        __init__ before the window is shown, so an unguarded read error
        would abort launch with no GUI to report it."""
        if self.path.exists():
            try:
                return self._load_txt_set()
            except Exception as exc:
                log.warning("Failed to load %s: %s", self.label, exc)
        return set()

    def save(self, tickers: set[str]) -> None:
        """Persist ``tickers`` to disk (atomic write via temp + rename).

        Metadata for tickers already on disk is PRESERVED — only genuinely new
        entries get today's date — so a plain ``save()`` from legacy callers
        never destroys the added-on dates that drive re-validation.
        """
        self._save_txt_set(tickers)

    # ── Metadata-aware API (audit INT-5) ──────────────────────────────

    def load_entries(self) -> dict[str, tuple[Optional[date], str]]:
        """``{ticker: (added_on | None, reason)}``. ``None`` date = legacy
        entry with no recorded date, which re-validation treats as eligible."""
        out: dict[str, tuple[Optional[date], str]] = {}
        if not self.path.exists():
            return out
        try:
            text = self.path.read_text(encoding="utf-8")
        except Exception as exc:
            log.warning("Failed to load %s: %s", self.label, exc)
            return out
        if self.fmt == "csv":
            for t in text.strip().split(","):
                if t.strip():
                    out[normalize_ticker(t)] = (None, "")
            return out
        for line in text.splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            parts = line.split("\t")
            head = parts[0]
            added: Optional[date] = None
            reason = ""
            if len(parts) >= 2:
                try:
                    added = date.fromisoformat(parts[1].strip())
                except ValueError:
                    added = None
            if len(parts) >= 3:
                reason = parts[2].strip()
            # A legacy line may still be comma-packed.
            for t in head.split(","):
                if t.strip():
                    out[normalize_ticker(t)] = (added, reason)
        return out

    def save_entries(
        self, entries: dict[str, tuple[Optional[date], str]],
    ) -> None:
        """Persist ticker → (added_on, reason). ``csv`` format drops metadata
        (the universal blacklist is hand-edited and has no re-check rule)."""
        if self.fmt == "csv":
            config.atomic_write_text(
                self.path, ", ".join(sorted(entries)),
            )
            return
        today = date.today()
        lines = []
        for tk in sorted(entries):
            if not tk or not tk.strip():
                continue
            added, reason = entries[tk]
            clean = tk.replace("\n", "").replace("\r", "").replace("\t", "").strip()
            if not clean:
                continue
            stamp = (added or today).isoformat()
            safe_reason = (reason or "").replace("\n", " ").replace(
                "\r", " ").replace("\t", " ").strip()
            lines.append(f"{clean}\t{stamp}\t{safe_reason}")
        header = (
            "# Trade_Scanner_FH skip list — TICKER<TAB>ADDED_ON<TAB>REASON\n"
            "# ADDED_ON drives periodic re-validation; edit or delete freely.\n"
        )
        config.atomic_write_text(self.path, header + "\n".join(lines) + "\n")

    def stale_entries(self, max_age_days: int,
                      *, reasons: Optional[set[str]] = None) -> set[str]:
        """Tickers eligible for a re-check: older than ``max_age_days`` (or with
        no recorded date), optionally restricted to specific ``reasons``.

        Audit 2026-08-12 (INT-5): this is what the old bare-ticker format could
        not support, and why the skip lists were a one-way ratchet.
        """
        cutoff = date.today() - timedelta(days=max_age_days)
        out: set[str] = set()
        for tk, (added, reason) in self.load_entries().items():
            if reasons is not None and reason not in reasons:
                continue
            if added is None or added <= cutoff:
                out.add(tk)
        return out

    # ── Generic text-set helpers ───────────────────────────────────────

    def _load_txt_set(self) -> set[str]:
        """Parse the file per ``self.fmt`` into a normalized set."""
        if self.fmt == "csv":
            text = self.path.read_text(encoding="utf-8").strip()
            return {
                normalize_ticker(t)
                for t in text.split(",") if t.strip()
            }
        # "lines": tolerate the legacy bare/comma form AND the tab-delimited
        # metadata form, so an old file and a new one both load correctly.
        text = self.path.read_text(encoding="utf-8")
        out: set[str] = set()
        for line in text.splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            head = line.split("\t")[0]
            for t in head.split(","):
                if t.strip():
                    out.add(normalize_ticker(t))
        return out

    def _save_txt_set(self, tickers: set[str]) -> None:
        """Serialize ``tickers`` per ``self.fmt`` and atomically write.

        Preserves existing (added_on, reason) metadata for tickers already on
        disk so a legacy set-based save doesn't reset every entry's age.
        """
        if self.fmt == "csv":
            config.atomic_write_text(self.path, ", ".join(sorted(tickers)))
            return
        prior = self.load_entries()
        entries = {
            t: prior.get(t, (None, ""))
            for t in tickers if t and t.strip()
        }
        self.save_entries(entries)
