"""
Multi-source earnings refresh orchestration — extracted from MainWindow.

Owns the 3-bar earnings progress panel (finviz / zacks / finnhub), the
``_pending_smart_refresh`` same-launch-capture chaining, the daily Nasdaq
calendar sweep kickoff, smart-refresh candidate trimming, and the
per-source fill-worker bringup / completion handlers.

Design notes (load-bearing for the test suite — do not "simplify"):

- The coordinator holds a plain back-reference to the window (``self.win``)
  and is NOT parented to it as a QObject: tests build bare
  ``MainWindow.__new__(MainWindow)`` shells whose C++ side is
  uninitialized, and passing one as a QObject parent would raise.
- Every cross-method orchestration call routes through the window's
  delegate (``self.win._earn_threads_active()``, not a direct
  ``self._earn_threads_active()``). Tests override these as instance
  attributes on the window (``w._earn_threads_active = lambda: False``,
  ``w._kick_off_smart_refresh = MagicMock()``), and routing through the
  window preserves exactly the pre-extraction dynamic-dispatch semantics.
- Window-owned collaborators (workers, blacklists, refs, log panel,
  status bar, QSettings) are likewise read/written via ``self.win`` so
  tests that seed them on bare window shells keep working.
- ``ZacksFillWorker`` / ``EarningsFillWorker`` are looked up through the
  ``main_window`` module namespace at call time because tests patch
  ``trade_scanner_fh.gui.main_window.ZacksFillWorker``.
"""

from __future__ import annotations

import logging
from datetime import datetime

from PyQt6.QtCore import QObject
from PyQt6.QtWidgets import (
    QFrame, QHBoxLayout, QLabel, QMessageBox, QProgressBar, QPushButton,
    QVBoxLayout,
)

from .. import config

# Same logger channel as main_window so the extracted log lines keep
# their historical "scanner.gui" tag in the panel / subsystem files.
log = logging.getLogger("scanner.gui")


class EarningsRefreshCoordinator(QObject):
    """Orchestrates the multi-source earnings refresh for MainWindow.

    MainWindow keeps every historical method name as a thin delegate onto
    this object, so signal wiring, menu actions, and tests are untouched.
    """

    # ── Earnings smart-refresh progress panel ───────────────────────────
    # Three concurrent fill sources (finviz / zacks / finnhub) each clobber
    # the single shared status bar, so the smart refresh had no usable
    # progress surface. This panel gives each source its own bar (hover for
    # per-source detail) plus a single Stop button that halts all three.

    _EARN_SOURCES = ("finviz", "zacks", "finnhub")
    _EARN_SRC_COLORS = {
        "finviz":  "#4a90d9",   # blue
        "zacks":   "#9b6cd6",   # purple
        "finnhub": "#3aa676",   # green
    }

    _NASDAQ_LAST_RUN_KEY = "menu/last_nasdaq_run_iso"

    def __init__(self, window):
        # No QObject parent: `window` may be a bare __new__ test shell
        # whose C++ side was never initialized (see module docstring).
        super().__init__()
        self.win = window
        # Same-launch capture: armed by _on_update_done when an earnings
        # smart refresh should run AFTER the daily Nasdaq sweep + reconcile
        # finish (so the candidate selector reads a fresh `last_earnings`).
        # _on_nasdaq_auto_refresh_done consumes it.
        self._pending_smart_refresh: bool = False

    def _build_earnings_progress_panel(self, main_layout) -> None:
        """Construct the (initially hidden) earnings-fill progress panel and
        append it to ``main_layout``. One labelled QProgressBar per source
        with a live tooltip, and a Stop-all button below."""
        self._earn_prog_bars: dict[str, QProgressBar] = {}
        # Per-source live state, used to build the hover tooltip.
        self._earn_prog_state: dict[str, dict] = {
            src: {"done": 0, "total": 0, "filled": None, "errors": None,
                  "status": "idle", "label": src.capitalize()}
            for src in self._EARN_SOURCES
        }

        self._earn_prog_panel = QFrame()
        self._earn_prog_panel.setStyleSheet(
            "QFrame { background: #2b2b2b; border-top: 1px solid #444; }"
        )
        outer = QVBoxLayout(self._earn_prog_panel)
        outer.setContentsMargins(8, 4, 8, 4)
        outer.setSpacing(3)

        title = QLabel("Earnings fills")
        title.setStyleSheet("color: #ccc; font-size: 10px; font-weight: bold;")
        outer.addWidget(title)

        bars_row = QHBoxLayout()
        bars_row.setSpacing(10)
        for src in self._EARN_SOURCES:
            col = QVBoxLayout()
            col.setSpacing(1)
            lbl = QLabel(src.capitalize())
            lbl.setStyleSheet("color: #bbb; font-size: 10px;")
            col.addWidget(lbl)

            bar = QProgressBar()
            bar.setRange(0, 100)
            bar.setValue(0)
            bar.setTextVisible(True)
            bar.setFormat("idle")
            bar.setFixedHeight(16)
            color = self._EARN_SRC_COLORS[src]
            bar.setStyleSheet(
                "QProgressBar { background: #1e1e1e; border: 1px solid #444; "
                "border-radius: 3px; color: #eee; font-size: 9px; "
                "text-align: center; }"
                f"QProgressBar::chunk {{ background: {color}; border-radius: 2px; }}"
            )
            bar.setToolTip(f"{src.capitalize()}: idle")
            col.addWidget(bar)
            self._earn_prog_bars[src] = bar
            bars_row.addLayout(col, 1)
        outer.addLayout(bars_row)

        btn_row = QHBoxLayout()
        btn_row.addStretch()
        self._earn_stop_btn = QPushButton("Stop Earnings Refresh")
        self._earn_stop_btn.setStyleSheet(
            "QPushButton { background: #803030; color: white; font-size: 10px; "
            "padding: 3px 12px; border-radius: 3px; }"
            "QPushButton:hover { background: #a04040; }"
            "QPushButton:disabled { background: #444; color: #888; }"
        )
        self._earn_stop_btn.clicked.connect(self.win._stop_all_earnings_fills)
        btn_row.addWidget(self._earn_stop_btn)
        outer.addLayout(btn_row)

        self._earn_prog_panel.setVisible(False)
        main_layout.addWidget(self._earn_prog_panel)

    def _earn_prog_tooltip_text(self, src: str) -> str:
        st = self._earn_prog_state[src]
        lines = [f"{st['label']} — {st['status']}"]
        if st["total"]:
            lines.append(f"Progress: {st['done']}/{st['total']}")
        if st["filled"] is not None:
            lines.append(f"Filled: {st['filled']}   Errors: {st['errors']}")
        return "\n".join(lines)

    def _earn_prog_set_idle(self, src: str) -> None:
        st = self._earn_prog_state[src]
        st.update(done=0, total=0, filled=None, errors=None,
                  status="idle", label=src.capitalize())
        bar = self._earn_prog_bars[src]
        bar.setRange(0, 100)
        bar.setValue(0)
        bar.setFormat("idle")
        bar.setToolTip(f"{src.capitalize()}: idle")

    def _earn_prog_begin(self, src: str, label: str) -> None:
        """Reveal the panel and mark ``src`` as queued/running. When no
        source is currently in flight (a fresh batch), every source is
        first reset to idle so stale 'done' results from a prior run don't
        linger on the other bars."""
        if src not in self._earn_prog_bars:
            return
        if not self.win._earn_state_active():
            for other in self._EARN_SOURCES:
                self.win._earn_prog_set_idle(other)
        st = self._earn_prog_state[src]
        st.update(done=0, total=0, filled=None, errors=None,
                  status="running", label=label)
        bar = self._earn_prog_bars[src]
        bar.setRange(0, 0)            # busy/indeterminate until first tick
        bar.setFormat("starting…")
        bar.setToolTip(self.win._earn_prog_tooltip_text(src))
        self._earn_prog_panel.setVisible(True)
        self._earn_stop_btn.setEnabled(True)
        self._earn_stop_btn.setText("Stop Earnings Refresh")

    def _earn_prog_tick(self, src: str, done: int, total: int) -> None:
        if src not in self._earn_prog_bars:
            return
        st = self._earn_prog_state[src]
        st.update(done=done, total=total, status="running")
        bar = self._earn_prog_bars[src]
        if total > 0:
            bar.setRange(0, total)
            bar.setValue(done)
            bar.setFormat(f"%p%  ({done}/{total})")
        else:
            bar.setRange(0, 0)
            bar.setFormat("working…")
        bar.setToolTip(self.win._earn_prog_tooltip_text(src))

    def _earn_prog_finish(self, src: str, filled: int, errors: int) -> None:
        if src not in self._earn_prog_bars:
            return
        st = self._earn_prog_state[src]
        st.update(filled=filled, errors=errors, status="done")
        bar = self._earn_prog_bars[src]
        total = st["total"] or 1
        bar.setRange(0, total)
        bar.setValue(total)
        bar.setFormat(f"done — {filled} filled, {errors} err")
        bar.setToolTip(self.win._earn_prog_tooltip_text(src))
        self.win._earn_prog_maybe_collapse()

    def _earn_state_active(self) -> bool:
        """True while any source's *tracked state* is still in flight.
        State-based (not QThread.isRunning) because the custom `finished`
        signal fires from inside the worker's run(), so the just-finished
        thread still reports isRunning()==True when collapse is evaluated."""
        return any(
            self._earn_prog_state[s]["status"] in ("running", "stopping")
            for s in self._EARN_SOURCES
        )

    def _earn_threads_active(self) -> bool:
        """True if any earnings worker thread is genuinely alive."""
        for w in (getattr(self.win, "_finviz_worker", None),
                  getattr(self.win, "_zacks_worker", None),
                  getattr(self.win, "_finnhub_worker", None)):
            if w is not None and w.isRunning():
                return True
        return False

    def _earn_prog_maybe_collapse(self) -> None:
        """When no source's state is still in flight, flip the Stop button
        to a Hide action and auto-collapse the panel after a grace period."""
        if self.win._earn_state_active():
            return
        self._earn_stop_btn.setEnabled(True)
        self._earn_stop_btn.setText("Hide")
        from PyQt6.QtCore import QTimer
        QTimer.singleShot(12000, self.win._earn_prog_autohide)

    def _earn_prog_autohide(self) -> None:
        if not self.win._earn_state_active():
            self._earn_prog_panel.setVisible(False)

    def _stop_all_earnings_fills(self) -> None:
        """Stop button: halt every running earnings fill, or hide the panel
        if all have already finished."""
        if not self.win._earn_threads_active():
            self._earn_prog_panel.setVisible(False)
            return
        stopped = []
        for name, w in (("finviz", getattr(self.win, "_finviz_worker", None)),
                        ("zacks", getattr(self.win, "_zacks_worker", None)),
                        ("finnhub", getattr(self.win, "_finnhub_worker", None))):
            if w is not None and w.isRunning():
                w.request_stop()
                stopped.append(name)
                self._earn_prog_state[name]["status"] = "stopping"
                bar = self._earn_prog_bars.get(name)
                if bar is not None:
                    bar.setFormat("stopping…")
                    bar.setToolTip(self.win._earn_prog_tooltip_text(name))
        if stopped:
            self.win.log_panel.write_line(
                f"Earnings refresh stop requested: {', '.join(stopped)}."
            )
            self._earn_stop_btn.setEnabled(False)

    # ── Smart-refresh chaining + candidate trimming ─────────────────────

    def _arm_or_run_smart_refresh(self, *, want_smart: bool) -> None:
        """Drive the earnings smart refresh with SAME-LAUNCH CAPTURE: arm
        ``_pending_smart_refresh`` and start the daily Nasdaq sweep; when the
        sweep actually starts, the smart refresh is deferred until the sweep
        + reconcile finish (chained from ``_on_nasdaq_auto_refresh_done``) so
        the candidate selector reads a freshly-updated ``last_earnings`` and
        catches a just-reported quarter THIS launch. When no sweep is due,
        run the smart refresh immediately.

        Shared by both launch paths — the OHLCV-update path
        (``_on_update_done``) and the OHLCV-current path — so a report the
        calendar discovers gets its actual captured regardless of OHLCV
        freshness. ``want_smart`` lets the caller suppress earnings (auto-
        refresh disabled, or a stopped/partial OHLCV update) while still
        letting the Nasdaq sweep run on its own cadence.
        """
        self._pending_smart_refresh = want_smart
        swept = self.win._maybe_run_nasdaq_refresh()
        if not swept:
            # No sweep started (not due / already running / no universe):
            # nothing to chain off, so run the smart refresh now.
            self._pending_smart_refresh = False
            if want_smart:
                self.win._kick_off_smart_refresh()

    def _maybe_run_nasdaq_refresh(self) -> bool:
        """Kick off the daily Nasdaq calendar sweep if enabled and due.
        Shared by the post-update path and the OHLCV-skip path so the
        calendar cadence isn't coupled to OHLCV freshness. Returns True
        iff a sweep was actually started (the caller chains the earnings
        smart refresh off a started sweep for same-launch capture)."""
        if self.win._nasdaq_auto_refresh_ref[0] and self.win._is_nasdaq_refresh_due():
            return self.win._kick_off_nasdaq_auto_refresh()
        return False

    def _trim_all_blocked_candidates(self, candidates: list[str]) -> list[str]:
        """Drop candidates that EVERY earnings source has permanently
        blacklisted (finnhub ∩ finviz ∩ zacks) — no source can refresh them,
        so they are pure dead weight. Without this, no-history names no source
        covers (warrants, preferreds, foreign OTC, SPACs) re-trip Rule A every
        launch: they inflate the flagged count ~20×, false-fire the bulk-run
        warning, and make a prompt promise far more work than the per-source
        workers (which skip every one of them) actually queue. `candidates`
        already excludes the OHLCV blacklist + ETF/ADR, so the raw per-source
        blacklist intersection is exactly "blocked everywhere". Returns the
        filtered list and logs how many were trimmed. Shared by the auto
        (launch) and manual ("Run Earnings Smart Refresh Now") paths."""
        all_blocked = (
            getattr(self.win, "_finnhub_blacklist", set())
            & getattr(self.win, "_finviz_blacklist", set())
            & getattr(self.win, "_zacks_blacklist", set())
        )
        if not all_blocked:
            return list(candidates)
        before_n = len(candidates)
        filtered = [t for t in candidates if t not in all_blocked]
        trimmed = before_n - len(filtered)
        if trimmed:
            self.win.log_panel.write_line(
                f"Earnings smart refresh: skipped {trimmed:,} no-history "
                f"ticker(s) all three sources have blacklisted "
                f"(uncoverable — warrants / preferreds / foreign OTC / SPACs)."
            )
        return filtered

    def _kick_off_smart_refresh(self) -> None:
        """Launch-time earnings smart refresh. Runs finviz + zacks
        CONCURRENTLY against one shared per-ticker candidate list
        (`find_smart_refresh_candidates`). Finnhub is EXCLUDED from this
        automatic cycle (config.FINNHUB_IN_AUTO_REFRESH=False; least-
        effective source, manual-only). Each source applies its own skip
        set; whichever lands a quarter wins the priority dedup.

        Only invoked from `_on_update_done`, so it inherits the OHLCV
        freshness gate (a skipped update never reaches here). A
        bulk-sized candidate set (> ZACKS_SMART_REFRESH_BULK_THRESHOLD)
        prompts Run / Skip / Disable before kicking anything off.
        """
        win = self.win
        if win._universe_df is None and not win._symbols:
            return
        universe_syms = win._get_universe_symbols()
        if not universe_syms:
            return

        # Exclude the OHLCV blacklist + ETF/ADR auto-skip from candidate
        # selection so Rule A (no-history) doesn't flood with funds the
        # earnings sources never cover. Per-source skip sets are applied
        # again at the worker level below.
        cand_skip = set(win._blacklist) | win._etf_adr_auto_skip_set()
        from ..earnings_history import find_smart_refresh_candidates
        candidates = find_smart_refresh_candidates(universe_syms, cand_skip)
        # Drop names every source has permanently blacklisted (see helper).
        candidates = win._trim_all_blocked_candidates(candidates)
        if not candidates:
            return

        threshold = config.ZACKS_SMART_REFRESH_BULK_THRESHOLD
        if len(candidates) > threshold:
            pct = len(candidates) * 100 // max(1, len(universe_syms))
            choice = QMessageBox.question(
                win,
                "Earnings Smart Refresh — Bulk-Sized Run Detected",
                (
                    f"The earnings smart refresh would queue "
                    f"<b>{len(candidates):,}</b> tickers ({pct}% of your "
                    f"universe) across finviz + zacks (finnhub is manual-only).\n\n"
                    f"A set this large usually means a first-time / recovery "
                    f"fill — functionally a multi-hour bulk scrape, not a "
                    f"daily top-up.\n\n"
                    f"<b>Yes</b> — run it now\n"
                    f"<b>No</b> — skip this launch (ask again next launch)\n"
                    f"<b>Cancel</b> — disable earnings auto-refresh for this "
                    f"session\n\nContinue?"
                ),
                QMessageBox.StandardButton.Yes
                | QMessageBox.StandardButton.No
                | QMessageBox.StandardButton.Cancel,
                QMessageBox.StandardButton.No,
            )
            if choice == QMessageBox.StandardButton.Cancel:
                win._earnings_auto_refresh_ref[0] = False
                win.log_panel.write_line(
                    "Earnings auto-refresh disabled for this session."
                )
                return
            if choice != QMessageBox.StandardButton.Yes:
                win.log_panel.write_line(
                    f"Earnings smart refresh skipped "
                    f"({len(candidates):,} candidates would have queued)."
                )
                return

        # Auto cycle: finviz + zacks only. Finnhub (least-effective source)
        # is manual-only — run it via the Finnhub bulk/gap/spot menu actions
        # or the manual "Run Earnings Smart Refresh Now".
        win._launch_smart_refresh_workers(
            candidates, due=True,
            include_finnhub=config.FINNHUB_IN_AUTO_REFRESH,
        )

    def _launch_smart_refresh_workers(
        self, candidates: list[str], *, due: bool = True,
        include_finnhub: bool = True,
    ) -> None:
        """Start the earnings sources concurrently against ``candidates`` in
        the smart-refresh ``targeted`` mode. Each guard avoids clobbering a
        manual fill of the same source already running. Shared by the
        launch-time auto path (`_kick_off_smart_refresh`) and the manual
        `Run Earnings Smart Refresh Now` menu action.

        ``include_finnhub``: the auto path passes False so Finnhub (the
        least-effective source) never runs on an automatic cycle — it's
        manual-only via the dedicated Finnhub bulk/gap/spot menu actions and
        this manual smart-refresh. finviz + zacks always run."""
        win = self.win
        srcs = "finviz + zacks" + (" + finnhub" if include_finnhub else "")
        word = "due" if due else "ticker(s)"
        win.log_panel.write_line(
            f"Earnings smart refresh: {len(candidates)} {word} — "
            f"refreshing {srcs} concurrently."
        )
        if not (win._finviz_worker and win._finviz_worker.isRunning()):
            win._start_finviz_worker(
                candidates, win._combined_finviz_skip_set(),
                mode="targeted", label="Smart refresh (finviz)",
            )
        if not (win._zacks_worker and win._zacks_worker.isRunning()):
            win._start_zacks_worker(
                candidates, mode="targeted", label="Smart refresh (zacks)",
            )
        if include_finnhub and not (
            win._finnhub_worker and win._finnhub_worker.isRunning()
        ):
            win._start_finnhub_worker(
                candidates, win._combined_finnhub_skip_set(),
                mode="targeted", label="Smart refresh (finnhub)",
            )

    def _run_earnings_smart_refresh_now(self) -> None:
        """Manual Data-menu trigger for the concurrent earnings smart
        refresh — the same process that normally follows an OHLCV update,
        invokable on demand. Useful when the OHLCV freshness gate skipped
        the update (cache already current) so the auto path never fired.

        If the staleness rules report nothing due, offers a small sample
        test run so the concurrent fill + progress panel are still visible.
        """
        win = self.win
        if win._universe_df is None and not win._symbols:
            QMessageBox.information(
                win, "Earnings Smart Refresh",
                "No universe is loaded yet — load tickers first.",
            )
            return
        universe_syms = win._get_universe_symbols()
        if not universe_syms:
            QMessageBox.information(
                win, "Earnings Smart Refresh",
                "The universe is empty — nothing to refresh.",
            )
            return

        # Don't double-launch if a refresh is already mid-flight.
        if win._earn_threads_active():
            QMessageBox.information(
                win, "Earnings Smart Refresh",
                "An earnings fill is already running — watch the progress "
                "panel above the status bar, or stop it first.",
            )
            return

        cand_skip = set(win._blacklist) | win._etf_adr_auto_skip_set()
        from ..earnings_history import find_smart_refresh_candidates
        candidates = find_smart_refresh_candidates(universe_syms, cand_skip)
        # Apply the same all-three-sources-blocked trim the auto path uses, so
        # the manual prompt's count reflects what will actually be fetched
        # (not thousands of permanently-uncoverable names every source skips).
        candidates = win._trim_all_blocked_candidates(candidates)

        if candidates:
            threshold = config.ZACKS_SMART_REFRESH_BULK_THRESHOLD
            if len(candidates) > threshold:
                pct = len(candidates) * 100 // max(1, len(universe_syms))
                choice = QMessageBox.question(
                    win, "Earnings Smart Refresh — Bulk-Sized Run",
                    (
                        f"<b>{len(candidates):,}</b> tickers ({pct}% of your "
                        f"universe) are due across finviz + zacks + finnhub — "
                        f"functionally a multi-hour bulk scrape.\n\n"
                        f"Run it now?"
                    ),
                    QMessageBox.StandardButton.Yes
                    | QMessageBox.StandardButton.No,
                    QMessageBox.StandardButton.No,
                )
                if choice != QMessageBox.StandardButton.Yes:
                    win.log_panel.write_line(
                        f"Manual earnings smart refresh skipped "
                        f"({len(candidates):,} candidates)."
                    )
                    return
            win.log_panel.write_line("Manual earnings smart refresh triggered.")
            win._launch_smart_refresh_workers(candidates, due=True)
            return

        # Nothing due — offer a sample test pass so the panel is visible.
        sample = [s for s in universe_syms if s not in cand_skip][:25]
        choice = QMessageBox.question(
            win, "Earnings Smart Refresh — Nothing Due",
            (
                "No tickers are currently due for an earnings refresh "
                "(everything is fresh per the staleness rules).\n\n"
                f"Run a TEST pass against the first <b>{len(sample)}</b> "
                "universe ticker(s) anyway, so you can watch the concurrent "
                "finviz + zacks + finnhub fill and the progress panel?"
            ),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.Yes,
        )
        if choice == QMessageBox.StandardButton.Yes and sample:
            win.log_panel.write_line(
                f"Manual earnings smart refresh (sample test): "
                f"{len(sample)} ticker(s)."
            )
            win._launch_smart_refresh_workers(sample, due=False)

    # ── Nasdaq daily auto-refresh (Phase 5 of Finnhub augmentation) ─────

    def _is_nasdaq_refresh_due(self) -> bool:
        """True iff the last recorded Nasdaq run is older than
        ``config.NASDAQ_REFRESH_DAYS`` (or never ran). Daily as of
        2026-06 so the calendar's `last_earnings` stays current for the
        earnings smart-refresh candidate selector."""
        try:
            last_iso = self.win._qsettings().value(self._NASDAQ_LAST_RUN_KEY)
        except Exception as exc:
            log.debug("Could not read last_nasdaq_run_iso: %s", exc)
            return True  # err on the side of refreshing
        if not last_iso:
            return True
        try:
            last = datetime.fromisoformat(str(last_iso))
        except ValueError:
            log.debug("Bad last_nasdaq_run_iso value: %r", last_iso)
            return True
        # Calendar-day cadence, NOT a rolling 24h gap: the first launch of
        # a new calendar day re-sweeps regardless of clock time. A 24h gap
        # on a completion-time stamp would skip a same-time-next-morning
        # launch (gap.days == 0 at ~23h58m), silently reintroducing the
        # stale-calendar miss this daily sweep exists to prevent.
        return (datetime.now().date() - last.date()).days >= config.NASDAQ_REFRESH_DAYS

    def _stamp_nasdaq_run_now(self) -> None:
        """Record the current time as the most recent Nasdaq run.
        Called from BOTH the auto-trigger path and the manual menu
        action so they share the daily counter."""
        try:
            self.win._qsettings().setValue(
                self._NASDAQ_LAST_RUN_KEY,
                datetime.now().isoformat(timespec="seconds"),
            )
        except Exception as exc:
            log.debug("Could not write last_nasdaq_run_iso: %s", exc)

    def _kick_off_nasdaq_auto_refresh(self) -> bool:
        """Phase 5 launch-time auto-trigger. Spawns the same
        EarningsFillWorker the manual menu does, then stamps the last-
        run timestamp on completion so the daily counter resets. Returns
        True iff the sweep worker was actually started (so the caller can
        chain the earnings smart refresh off its completion)."""
        win = self.win
        if win._earnings_worker and win._earnings_worker.isRunning():
            return False
        if win._universe_df is None and not win._symbols:
            return False
        syms = win._get_universe_symbols()
        if not syms:
            return False
        win.log_panel.write_line(
            "Nasdaq daily auto-refresh: launching calendar sweep..."
        )
        # Worker class resolved through the main_window module namespace —
        # tests patch `trade_scanner_fh.gui.main_window.EarningsFillWorker`.
        from . import main_window as _mw
        win._earnings_worker = _mw.EarningsFillWorker(
            syms, win._blacklist, mode="bulk",
        )
        win._earnings_worker.log_msg.connect(win.log_panel.write_line)
        win._earnings_worker.progress.connect(
            lambda d, t: win.status.showMessage(
                f"Nasdaq calendar (auto-daily): {d}/{t}"
            )
        )
        win._earnings_worker.finished.connect(
            win._on_nasdaq_auto_refresh_done
        )
        win._earnings_worker.start()
        return True

    def _on_nasdaq_auto_refresh_done(self, filled: int, errors: int):
        win = self.win
        win._stamp_nasdaq_run_now()
        win.status.showMessage(
            f"Nasdaq daily auto-refresh done: {filled} filled, "
            f"{errors} errors"
        )
        # Same-launch capture: the calendar's `last_earnings` is now
        # current, so run the deferred earnings smart refresh against the
        # fresh candidate list. Only fires when _on_update_done armed it
        # (the real OHLCV-update path); the OHLCV-skip path leaves the
        # flag False so a fresh cache still skips earnings.
        if self._pending_smart_refresh:
            self._pending_smart_refresh = False
            if win._earnings_auto_refresh_ref[0]:
                win._kick_off_smart_refresh()

    # ── Per-source fill-worker bringup + completion handlers ───────────

    def _start_finnhub_worker(self, symbols: list[str], skip: set[str],
                              *, mode: str, label: str):
        from .workers import FinnhubFillWorker
        win = self.win
        win._finnhub_worker = FinnhubFillWorker(symbols, skip, mode=mode)
        win._finnhub_worker.log_msg.connect(win.log_panel.write_line)
        win._finnhub_worker.progress.connect(
            lambda d, t, _l=label: win.status.showMessage(f"{_l}: {d}/{t}")
        )
        win._finnhub_worker.progress.connect(
            lambda d, t: win._earn_prog_tick("finnhub", d, t)
        )
        win._finnhub_worker.etf_identified.connect(
            win._on_finnhub_etf_identified
        )
        win._finnhub_worker.finished.connect(win._on_finnhub_done)
        win._earn_prog_begin("finnhub", label)
        win._finnhub_worker.start()

    def _on_finnhub_done(self, filled: int, errors: int):
        win = self.win
        # Persist any auto-added Finnhub skip-list entries collected via
        # the etf_identified signal.
        if win._auto_added_finnhub_skips > 0:
            try:
                win._save_finnhub_blacklist()
            except Exception as exc:
                log.warning("Could not save Finnhub skip list: %s", exc)
            win.log_panel.write_line(
                f"Finnhub fill: added {win._auto_added_finnhub_skips} "
                f"ETF / uncovered ticker(s) to Finnhub skip list "
                f"(now {len(win._finnhub_blacklist)} total)."
            )
            win._auto_added_finnhub_skips = 0
        win.log_panel.write_line(
            f"Finnhub fill done: {filled} filled, {errors} errors."
        )
        win.status.showMessage(
            f"Finnhub fill done: {filled} filled, {errors} errors"
        )
        win._earn_prog_finish("finnhub", filled, errors)

    def _start_finviz_worker(self, symbols: list[str], skip: set[str],
                             *, mode: str, label: str):
        from .workers import FinvizFillWorker
        win = self.win
        win._finviz_worker = FinvizFillWorker(symbols, skip, mode=mode)
        win._finviz_worker.log_msg.connect(win.log_panel.write_line)
        win._finviz_worker.progress.connect(
            lambda d, t, _l=label: win.status.showMessage(f"{_l}: {d}/{t}")
        )
        win._finviz_worker.progress.connect(
            lambda d, t: win._earn_prog_tick("finviz", d, t)
        )
        win._finviz_worker.empty_identified.connect(
            win._on_finviz_empty_identified
        )
        win._finviz_worker.finished.connect(win._on_finviz_done)
        win._earn_prog_begin("finviz", label)
        win._finviz_worker.start()

    def _on_finviz_done(self, filled: int, errors: int):
        win = self.win
        if win._auto_added_finviz_skips > 0:
            try:
                win._save_finviz_blacklist()
            except Exception as exc:
                log.warning("Could not save finviz skip list: %s", exc)
            win.log_panel.write_line(
                f"Finviz fill: added {win._auto_added_finviz_skips} "
                f"uncovered ticker(s) to finviz skip list "
                f"(now {len(win._finviz_blacklist)} total)."
            )
            win._auto_added_finviz_skips = 0
        win.log_panel.write_line(
            f"Finviz fill done: {filled} filled, {errors} errors."
        )
        win.status.showMessage(
            f"Finviz fill done: {filled} filled, {errors} errors"
        )
        win._earn_prog_finish("finviz", filled, errors)

    def _start_zacks_worker(self, symbols: list[str], *, mode: str, label: str):
        """Shared worker bringup for the bulk/targeted Zacks menu actions.
        Wires the progress/log/Imperva-block signals onto a fresh
        ZacksFillWorker."""
        win = self.win
        # Worker class resolved through the main_window module namespace —
        # tests patch `trade_scanner_fh.gui.main_window.ZacksFillWorker`.
        from . import main_window as _mw
        win._zacks_worker = _mw.ZacksFillWorker(
            symbols, win._zacks_skip_set(), mode=mode,
        )
        win._zacks_worker.log_msg.connect(win.log_panel.write_line)
        win._zacks_worker.progress.connect(
            lambda d, t, _l=label: win.status.showMessage(f"{_l}: {d}/{t}")
        )
        win._zacks_worker.progress.connect(
            lambda d, t: win._earn_prog_tick("zacks", d, t)
        )
        win._zacks_worker.finished.connect(win._on_zacks_done)
        win._zacks_worker.failure_breakdown.connect(win._on_zacks_failures)
        win._zacks_worker.imperva_block_detected.connect(
            win._on_imperva_block_detected
        )
        win._earn_prog_begin("zacks", label)
        win._zacks_worker.start()

    def _on_zacks_progress(self, done: int, total: int):
        if total <= 0:
            return
        self.win.status.showMessage(
            f"Zacks refresh: {done}/{total} ticker(s)..."
        )

    def _on_zacks_done(self, filled: int, errors: int, candidates: list):
        win = self.win
        n = len(candidates)
        if n == 0:
            win._earn_prog_finish("zacks", filled, errors)
            return  # candidate empty path already logged inside the worker
        win.log_panel.write_line(
            f"Zacks fill done: {n} tickers attempted, {filled} ok, "
            f"{errors} errors."
        )
        # Per-kind breakdown summary (failure_breakdown signal already
        # fired before this slot; the auto-add to skip list happened
        # there too, so _auto_added_zacks_skips reflects this run).
        breakdown = win._last_zacks_failures
        if breakdown:
            from ..zacks_scraper import (
                FAIL_BLOCKED, FAIL_NOT_FOUND,
                FAIL_HTTP_ERROR, FAIL_PARSE_ERROR,
            )
            n_block = len(breakdown.get(FAIL_BLOCKED, []))
            n_nf = len(breakdown.get(FAIL_NOT_FOUND, []))
            n_http = len(breakdown.get(FAIL_HTTP_ERROR, []))
            n_parse = len(breakdown.get(FAIL_PARSE_ERROR, []))
            n_unk = len(breakdown.get("unknown", []))

            added = int(getattr(win, "_auto_added_zacks_skips", 0))
            if n_nf:
                already = max(0, n_nf - added)
                win.log_panel.write_line(
                    f"  ↳ {n_nf} not on Zacks "
                    f"(auto-added {added} new to Zacks skip list, "
                    f"{already} were already on it)"
                )
            if n_block:
                win.log_panel.write_line(
                    f"  ↳ {n_block} Imperva block(s) "
                    "(auto-recovered with cookie refresh)"
                )
            if n_http:
                win.log_panel.write_line(
                    f"  ↳ {n_http} HTTP / network error(s) (transient)"
                )
            if n_parse:
                win.log_panel.write_line(
                    f"  ↳ {n_parse} parse error(s) (Zacks page format may have shifted)"
                )
            if n_unk:
                win.log_panel.write_line(
                    f"  ↳ {n_unk} unclassified failure(s)"
                )
            win.log_panel.write_line(
                "Tip: Data → Show Last Zacks Failures... for the per-ticker "
                "breakdown; Data → Edit Zacks Skip List... to review/edit."
            )
        win.status.showMessage(
            f"Ready: {len(win._symbols)} tickers with OHLCV data"
        )
        win._earn_prog_finish("zacks", filled, errors)

    def _on_zacks_failures(self, breakdown: dict):
        """Stash the worker's per-kind failure breakdown for the
        Show Last Zacks Failures menu action AND auto-add every
        FAIL_NOT_FOUND ticker to the Zacks-only skip list — they're
        definitively not on Zacks (Imperva blocks land in a different
        bucket), no point re-checking next run.

        Sets `_auto_added_zacks_skips` on the window so `_on_zacks_done`
        can report the count cleanly."""
        win = self.win
        win._last_zacks_failures = dict(breakdown or {})
        win._auto_added_zacks_skips = 0
        if not breakdown:
            return

        from ..zacks_scraper import FAIL_NOT_FOUND
        not_found = breakdown.get(FAIL_NOT_FOUND, []) or []
        new_skips = [
            win._normalize_ticker(t) for t in not_found
            if win._normalize_ticker(t) not in win._zacks_blacklist
        ]
        new_skips = [t for t in new_skips if t]  # drop blanks
        if not new_skips:
            return
        for t in new_skips:
            win._zacks_blacklist.add(t)
        try:
            win._save_zacks_blacklist()
            win._auto_added_zacks_skips = len(new_skips)
        except Exception as exc:
            win._log_error(
                "zacks-skip-list",
                f"Warning: Zacks skip list save failed: {exc}", exc,
            )
