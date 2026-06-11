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
from pathlib import Path

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
        """Persist ``tickers`` to disk (atomic write via temp + rename)."""
        self._save_txt_set(tickers)

    # ── Generic text-set helpers ───────────────────────────────────────

    def _load_txt_set(self) -> set[str]:
        """Parse the file per ``self.fmt`` into a normalized set."""
        if self.fmt == "csv":
            text = self.path.read_text(encoding="utf-8").strip()
            return {
                normalize_ticker(t)
                for t in text.split(",") if t.strip()
            }
        text = self.path.read_text(encoding="utf-8")
        return {
            normalize_ticker(t)
            for line in text.splitlines()
            for t in line.split(",")
            if t.strip() and not t.lstrip().startswith("#")
        }

    def _save_txt_set(self, tickers: set[str]) -> None:
        """Serialize ``tickers`` per ``self.fmt`` and atomically write."""
        if self.fmt == "csv":
            config.atomic_write_text(self.path, ", ".join(sorted(tickers)))
            return
        cleaned = sorted(
            t.replace("\n", "").replace("\r", "").strip()
            for t in tickers if t and t.strip()
        )
        body = "\n".join(cleaned) + "\n"
        config.atomic_write_text(self.path, body)
