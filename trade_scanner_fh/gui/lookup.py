"""
Lookup mode (v8.0.1) — run the current filters over a typed list of tickers.

A lookup is an ordinary scan with the user's list in place of the cached
universe: the same OHLCV / earnings stores, the same filter panel, the same
timeframes, custom range or Sequenced Run. Two modes:

  * "as set"        — exactly the scan's filters; a ticker that fails any of
                      them is not shown.
  * "display-only"  — every switched-on filter becomes display-only, so every
                      ticker is shown and failing values are coloured.

The parameter adjustments live in `scanner.lookup_params`; `run_scan(lookup=
True)` records what happened to each ticker. This module holds the pieces the
window needs around that: matching the typed list (parsed by the shared
`ticker_input.parse_ticker_list`) to cached symbols, the input dialog (built to the Manual Input STW dialog's form
factor) and the per-ticker report written to the log after each run.
"""

from __future__ import annotations

from typing import Iterable

from PyQt6.QtWidgets import (
    QButtonGroup, QCheckBox, QDialog, QHBoxLayout, QLabel, QMessageBox,
    QPushButton, QRadioButton, QTextEdit, QVBoxLayout,
)

from .. import scanner as S
# v8.0.2: the parser is shared by every ticker dialog; re-exported here
# because the lookup path and its tests reach it as lookup.parse_ticker_list.
from .ticker_input import INPUT_PROMPT, parse_ticker_list  # noqa: F401

# Pre-lookup earnings refresh choices (v8.0.1). "all" is the three
# earnings-HISTORY sources — the same set as Data → Run Earnings Smart Refresh
# Now. The Nasdaq calendar (a whole-market date sweep that already runs
# daily) and Yahoo dates (a gap filler) are deliberately not part of it.
REFRESH_FINVIZ = "finviz"
REFRESH_ALL = "all"
REFRESH_SOURCES = {REFRESH_FINVIZ: ("finviz",),
                   REFRESH_ALL: ("finviz", "zacks", "finnhub")}


def resolve_tickers(tickers: Iterable[str], cached) -> list:
    """``[(typed, cached_symbol_or_None)]``. The store names share classes
    with a dash (``BRK-B``), so ``BRK.B`` and ``BRK/B`` resolve to it."""
    cached = set(cached)
    out = []
    for t in tickers:
        hit = next((c for c in (t, t.replace(".", "-"), t.replace("/", "-"))
                    if c in cached), None)
        out.append((t, hit))
    return out


def _names(symbols, limit: int = 40) -> str:
    symbols = list(symbols)
    more = len(symbols) - limit
    return ", ".join(symbols[:limit]) + (f" … +{more} more" if more > 0 else "")


def report_lines(*, display_only: bool, requested, period_order,
                 period_outcomes, view_split, notes=(),
                 intra_run_omit: bool = False) -> list:
    """The per-ticker report, one block per period.

    `requested` is `resolve_tickers` output. `view_split[label]` is
    ``(shown, hidden_by_you, hidden_by_view)`` — sets of passing tickers the
    table shows, that a row hide removes, and that a view toggle removes.
    """
    mode = "all filters display-only" if display_only else "filters as set"
    lines = [f"── Lookup report ({mode}) ──"]
    aliases = [f"{t} → {c}" for t, c in requested if c and c != t]
    if aliases:
        lines.append(f"  Matched as: {_names(aliases)}")
    missing = [t for t, c in requested if not c]
    if missing:
        lines.append(f"  Not in the OHLCV cache (skipped): {_names(missing)}"
                     f" — Data → Rebuild Tickers can fetch one.")
    for note in notes:
        lines.append(f"  Note: {note}")
    wanted = [c for _t, c in requested if c]
    claimed: dict = {}
    for label in period_order:
        outcomes = period_outcomes.get(label, {}) or {}
        shown, by_you, by_view = view_split.get(label, (set(), set(), set()))
        failed, other = [], {}
        earlier = []
        for sym in wanted:
            out = outcomes.get(sym)
            if out is None:
                if intra_run_omit and sym in claimed:
                    earlier.append(f"{sym} ({claimed[sym]})")
                else:
                    other.setdefault("not evaluated", []).append(sym)
            elif out.startswith(S.FAILED_PREFIX):
                failed.append(f"{sym} ({out[len(S.FAILED_PREFIX):]})")
            elif out.startswith(S.ERROR_PREFIX):
                other.setdefault("error", []).append(
                    f"{sym} ({out[len(S.ERROR_PREFIX):]})")
            elif out != S.OUTCOME_PASSED:
                other.setdefault(out, []).append(sym)
        shown_list = [s for s in wanted if s in shown]
        head = f"  [{label}] {len(shown_list)} of {len(wanted)} shown"
        lines.append(head + (f" — {_names(shown_list)}" if shown_list else ""))
        if failed:
            lines.append(f"      failed — {_names(failed)}")
        for what, syms in other.items():
            lines.append(f"      {what} — {_names(syms)}")
        if by_you:
            lines.append(f"      passed but hidden by you — "
                         f"{_names(s for s in wanted if s in by_you)}")
        if by_view:
            lines.append(f"      passed but hidden by a view toggle "
                         f"(Earnings Data / Dates / Color Match Only) — "
                         f"{_names(s for s in wanted if s in by_view)}")
        if earlier:
            lines.append(f"      already shown in an earlier period "
                         f"(Omit intra-run) — {_names(earlier)}")
        for sym, out in outcomes.items():
            if out == S.OUTCOME_PASSED:
                claimed.setdefault(sym, label)
    return lines


class LookupDialog(QDialog):
    """Ticker entry for Lookup — built to the Manual Input STW dialog's form
    factor (same width, margins, prompt and text box), plus the mode."""

    def __init__(self, parent=None, *, text: str = "",
                 display_only: bool = False, refresh=None):
        super().__init__(parent)
        self.setWindowTitle("Lookup — Run Current Filters on Tickers")
        self.setMinimumWidth(500)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(30, 20, 30, 20)

        prompt = QLabel(INPUT_PROMPT)
        prompt.setWordWrap(True)
        layout.addWidget(prompt)
        self.txt = QTextEdit()
        self.txt.setMinimumHeight(120)
        self.txt.setPlaceholderText("AAPL, MSFT, NVDA, TSLA, ...")
        self.txt.setPlainText(text)
        layout.addWidget(self.txt)

        self.rb_as_set = QRadioButton(
            "Apply filters as set — show only tickers that pass")
        self.rb_display_only = QRadioButton(
            "All filters display-only — show every ticker, failures colored")
        self._modes = QButtonGroup(self)
        for rb in (self.rb_as_set, self.rb_display_only):
            self._modes.addButton(rb)
            layout.addWidget(rb)
        (self.rb_display_only if display_only else self.rb_as_set).setChecked(True)

        # Optional earnings refresh before the lookup. Mutually exclusive but
        # both may be off, so a QButtonGroup (which forbids "none") won't do.
        self.chk_refresh_finviz = QCheckBox(
            "Refresh earnings from finviz first")
        self.chk_refresh_all = QCheckBox(
            "Refresh earnings from all sources first "
            "(finviz + zacks + finnhub)")
        for chk in (self.chk_refresh_finviz, self.chk_refresh_all):
            layout.addWidget(chk)
        self.chk_refresh_finviz.toggled.connect(
            lambda on: on and self.chk_refresh_all.setChecked(False))
        self.chk_refresh_all.toggled.connect(
            lambda on: on and self.chk_refresh_finviz.setChecked(False))
        if refresh == REFRESH_FINVIZ:
            self.chk_refresh_finviz.setChecked(True)
        elif refresh == REFRESH_ALL:
            self.chk_refresh_all.setChecked(True)

        hint = QLabel(
            "Uses the current filter panel, timeframes and Sequenced Run. "
            "Top X% is not applied in lookups; a report of every ticker is "
            "written to the log. A refresh runs first for the listed tickers "
            "(finviz is paced at about 4 s per ticker) and the lookup starts "
            "when it finishes.")
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #999; font-size: 11px;")
        layout.addWidget(hint)

        btn_row = QHBoxLayout()
        self.btn_go = QPushButton("  Lookup  ")
        self.btn_go.setStyleSheet(
            "QPushButton { background: #1565c0; color: white; font-size: 13px; "
            "font-weight: bold; padding: 8px 24px; border-radius: 4px; }"
        )
        btn_cancel = QPushButton("  Cancel  ")
        btn_cancel.setStyleSheet(
            "QPushButton { background: #555; color: white; font-size: 13px; "
            "padding: 8px 24px; border-radius: 4px; }"
        )
        btn_row.addWidget(self.btn_go)
        btn_row.addWidget(btn_cancel)
        btn_row.addStretch()
        layout.addLayout(btn_row)

        self.btn_go.clicked.connect(self._on_go)
        btn_cancel.clicked.connect(self.reject)

    def tickers(self) -> list:
        return parse_ticker_list(self.txt.toPlainText())

    def display_only(self) -> bool:
        return self.rb_display_only.isChecked()

    def refresh(self):
        """REFRESH_FINVIZ, REFRESH_ALL or None."""
        if self.chk_refresh_finviz.isChecked():
            return REFRESH_FINVIZ
        if self.chk_refresh_all.isChecked():
            return REFRESH_ALL
        return None

    def text(self) -> str:
        return self.txt.toPlainText()

    def _warn_empty(self):
        QMessageBox.warning(self, "No Tickers", "Enter at least one ticker.")

    def _on_go(self):
        if not self.tickers():
            self._warn_empty()
            return
        self.accept()
