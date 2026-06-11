"""
Trading Scanner — PyQt6 Main Window + main() entry point.

The rest of the old monolithic gui.py was split in Phase 6 O1:
  - theme.py     dark stylesheet
  - widgets.py   IndicatorPanel, ResultsTable, LogPanel, QtLogHandler, RESULT_COLUMNS
  - workers.py   the 7 QThread workers
  - dialogs.py   WatchlistDialog
This module now hosts the MainWindow class, the inline dialogs tightly
coupled to MainWindow state (backoff, blacklist, rebuild, coverage,
manual-input), and the main() launcher.
"""

from __future__ import annotations

import csv
import json
import logging
import sys
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq_reader

from PyQt6.QtCore import QDate, QEvent, Qt, pyqtSlot
from PyQt6.QtGui import QAction, QFont
from PyQt6.QtWidgets import (
    QApplication, QCheckBox, QComboBox, QDateEdit, QDialog,
    QDialogButtonBox, QDoubleSpinBox, QFileDialog, QFrame, QHBoxLayout,
    QInputDialog, QLabel, QLineEdit, QMainWindow, QMessageBox,
    QProgressBar, QPushButton, QSpinBox, QSplitter, QStatusBar,
    QTextEdit, QToolBar, QVBoxLayout, QWidget,
)

from .. import __version__, config, finnhub_client
from .. import hotkey as hotkey_mod
from ..data_engine import check_schema_version, load_ohlcv, rebuild_ticker
from ..hotkey import HotkeyConfig
from ..scanner import ScanResult, chunk_periods
from ..ticker_universe import load_universe
from ..tradestation import BridgeConfig
from .blacklists import BlacklistManager, normalize_ticker
from .columns import ColumnManager
from .dialogs import (
    ColumnsManagerDialog, ExcelExportDialog, SequencedRunDialog,
    WatchlistDialog,
)
from .earnings_coordinator import EarningsRefreshCoordinator
from .exports import ExportsController
from .hotkey_dialog import HotkeySettingsDialog
from .theme import DARK_STYLESHEET
from .widgets import (
    _fmt_date, IndicatorPanel, LogPanel, QtLogHandler, RESULT_COLUMNS,
    ResultsTable,
)
from .workers import (
    BridgeWorker, EarningsFillWorker, ScanWorker, SectorFillWorker,
    UniverseRefreshWorker, UniverseWorker, UpdateWorker, ZacksFillWorker,
)

log = logging.getLogger("scanner.gui")

PRESETS_DIR = config.DATA_DIR / "presets"
# Note: directory is created lazily in main() via config.ensure_dirs() +
# explicit mkdir below — NOT at import time — so merely importing this module
# has no filesystem side effects (aids tests and non-GUI usage).

# Phase 6 O11: preset schema version. Bump when a change would materially
# break old presets' meaning; loaders tolerate missing keys so minor
# additions don't require a version bump.
#
# v2 (2026-05): persists timeframe checkboxes (1D/1W/1M/3M/6M, custom
# range, sequenced run + its config), plus the indicator panel's per-row
# `display_only` state already lived under `indicators`. Old v1 presets
# that lack any of the new keys still load (loader uses .get() with
# defaults) but won't restore series/timeframe selections.
#
# v5 (2026-05): persists `column_order` (list of keys = manual user
# reorder) and `column_hidden` (list of hidden keys). Loading a v5+
# preset wipes the current results so the Columns dropdown shows the
# preset's saved column intent. Pre-v5 presets lack these fields and
# leave the existing column layout alone.
#
# v6 (2026-05): persists the two session-bar omission toggles
# (`omit_previously_scanned` ↔ chk_omit_seen, `omit_earlier_period_hits`
# ↔ chk_omit_intra_run). Previously these defaulted off on every launch
# regardless of preset; now they round-trip through save/load so a user
# can capture their preferred session-filter posture per preset. Missing
# keys load as False — pre-v6 presets behave like before.
PRESET_SCHEMA_VERSION = 6

# ============================================================================
# Main window
# ============================================================================

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"Trading Scanner v{__version__}")
        self.setMinimumSize(1280, 800)
        self.resize(1500, 900)

        self._worker: Optional[ScanWorker] = None
        self._update_worker: Optional[UpdateWorker] = None
        self._universe_worker: Optional[UniverseWorker] = None
        self._sector_worker: Optional[SectorFillWorker] = None
        self._earnings_worker: Optional[EarningsFillWorker] = None
        # Phase 5: Zacks daily smart-refresh worker — kicked off after
        # OHLCV update completes when ZACKS_AUTO_REFRESH_ENABLED is True.
        self._zacks_worker: Optional[ZacksFillWorker] = None
        # Phase 2 of Finnhub augmentation: deep-history fill worker.
        self._finnhub_worker = None
        # Finviz earnings fill worker (top-priority adjusted source).
        self._finviz_worker = None
        # FirefoxCookieWaitWorker that watches the user-launched Firefox
        # for close + reads cookies.sqlite. None when no browser session
        # is in flight. Lifecycle: created on
        # _refresh_zacks_cookies_action(), cleared in its `finished` slot.
        self._cookie_wait_worker = None
        # Cookie-browser monitor preference: (x, y, w, h) physical-pixel
        # geometry of the monitor Firefox should open on. None = let the
        # OS place it. Set/cleared via Settings → Set Cookie Browser
        # Monitor and persisted via QSettings across sessions.
        self._cookie_monitor_geom: Optional[tuple[int, int, int, int]] = None
        self._load_cookie_monitor_pref()
        # When Firefox is launched from inside the Imperva auto-pause
        # flow, set the worker that should be resumed once cookies land.
        # Cleared in the cookie-finished slot. Distinct from
        # _zacks_worker so we never leak the reference across runs.
        self._zacks_worker_awaiting_resume: Optional[ZacksFillWorker] = None
        # Last Zacks fill's per-kind failure breakdown, populated from
        # ZacksFillWorker.failure_breakdown signal at end of each run.
        # Surfaced via Data → Show Last Zacks Failures... so the user
        # can review what was missed and decide what to blacklist.
        self._last_zacks_failures: dict[str, list[str]] = {}
        # Mirror config flag in a mutable container so the menu toggle
        # picks up changes without restart. Same pattern as
        # _backoff_enabled_ref. Persisted across sessions via QSettings
        # — see `_load_menu_toggles_pref` (called below) and the
        # toggled-slot persistence in `_on_zacks_auto_refresh_toggled`.
        # Phase 5 of the Finnhub augmentation: this ref no longer drives
        # any auto-trigger — the toggle's been removed and the launch
        # sequence does NOT call _kick_off_zacks_smart_refresh anymore.
        # Kept as a structural attribute for back-compat with tests that
        # still construct it.
        self._zacks_auto_refresh_ref: list[bool] = [config.ZACKS_AUTO_REFRESH_ENABLED]
        # Earnings smart-refresh (all 3 sources) auto-trigger — fires from
        # _on_update_done after a real OHLCV update (so it inherits the
        # market-close freshness gate). Session flag: the bulk-size prompt's
        # "Disable" choice flips it off until the next launch.
        self._earnings_auto_refresh_ref: list[bool] = [True]
        # Nasdaq calendar auto-refresh ref (replaces the Zacks one as the
        # only surviving piece of launch-time automation; now DAILY).
        # Toggle in Data → Earnings dates section. QSettings-persisted.
        self._nasdaq_auto_refresh_ref: list[bool] = [config.NASDAQ_AUTO_REFRESH_ENABLED]
        # Same-launch capture: armed by _on_update_done when an earnings
        # smart refresh should run AFTER the daily Nasdaq sweep + reconcile
        # finish (so the candidate selector reads a fresh `last_earnings`).
        # _on_nasdaq_auto_refresh_done consumes it.
        self._pending_smart_refresh: bool = False
        # Cached "latest available data date" (newest OHLCV bar) used to
        # anchor the quick-range buttons' End to the last trading day.
        # Invalidated after each OHLCV update.
        self._latest_data_date_cache = None
        self._symbols: list[str] = []
        self._universe_df: Optional[pd.DataFrame] = None
        self._session_seen: set[str] = set()
        self._session_scan_count: int = 0
        # Phase 3 I5: cache {symbol: (mtime_ns, row_count)} to avoid reading
        # parquet metadata for every candidate on every IPO-mode scan click.
        self._ipo_row_count_cache: dict[str, tuple[int, int]] = {}

        # Bridge state — used by the Stop Sending button to support
        # Cancel / Resume / Restart after halting a watchlist send mid-run.
        self._bridge_symbols: Optional[list[str]] = None
        self._bridge_cfg: Optional[BridgeConfig] = None
        self._bridge_last_sent_idx: int = 0
        self._stop_sending_show_dialog: bool = False

        # Latest scan results, cached for the Excel export dialog so it can
        # auto-pre-select columns based on which have valid data right now.
        self._last_results_df: Optional[pd.DataFrame] = None
        # User-saved column order — list of column keys in their
        # preferred display order. Persists across timeframe switches
        # within a session; reset on app restart. Threaded into Excel
        # exports so saved sheets match the on-screen layout.
        self._results_column_order: list[str] = []

        # Sequenced Run state — populated by the Sequenced Run dialog
        # when the user ticks the toolbar checkbox.
        self._sequenced_cfg: Optional[tuple[date, date, int, str]] = None

        # ── Per-row HOTKEY ticker sender ──
        # Config (position / delay / cue / end-sequence) persists across
        # sessions via QSettings; the on/off toggle does NOT — defaults
        # to off on each launch so a stale screen position can't fire
        # surprise input. See `hotkey.py` and `gui/hotkey_dialog.py`.
        self._hotkey_cfg: HotkeyConfig = self._load_hotkey_pref()
        self._hotkey_enabled: bool = False

        # ── Menu toggle persistence ──
        # Auto-refresh Zacks at launch + Enable Rate-Limit Backoff
        # both persist across sessions via QSettings. Loaded BEFORE
        # `_build_ui` so the QAction's initial checked state matches
        # the saved value rather than the hardcoded config default.
        self._load_menu_toggles_pref()

        # ── Match-color tolerance (results-table date pairing) ──
        # Default 1 (±1 calendar day). Persists across sessions.
        # Adjustable via Settings → Color Match Tolerance….
        self._match_color_tolerance: int = self._load_match_color_tolerance_pref()

        self._build_ui()
        self._install_log_handler()
        self._startup()

    # ── UI construction ────────────────────────────────────────────────

    def _build_ui(self):
        # --- Toolbar ---
        toolbar = QToolBar("Main")
        toolbar.setMovable(False)
        self.addToolBar(toolbar)

        # Global earnings filter mode toggles — placed at the LEFT
        # edge of the toolbar so they sit ahead of every other control.
        # Replace the per-row "Include No Data" checkboxes from the
        # earnings indicator panel. Two flags because dates and data
        # are conceptually distinct coverage signals:
        #   "Earnings Dates" → days_since/until filters require a value
        #   "Earnings Data"  → 6 Zacks per-quarter filters require a value
        # Invariant: dates ⊇ data (anything with data has a date), so
        # the dates filter never drops a ticker that has earnings data.
        # Both default off → NaN passes the filter cleanly.
        self.chk_earnings_dates_only = QCheckBox("Earnings Dates")
        self.chk_earnings_dates_only.setChecked(False)
        self.chk_earnings_dates_only.setToolTip(
            "When checked, the days-since / days-until earnings filters "
            "require an actual calendar date — tickers without an "
            "earnings calendar entry are dropped (UNLESS they have "
            "earnings data, in which case the date is implied). "
            "Unchecked = NaN dates pass."
        )
        toolbar.addWidget(self.chk_earnings_dates_only)

        self.chk_earnings_data_only = QCheckBox("Earnings Data")
        self.chk_earnings_data_only.setChecked(False)
        self.chk_earnings_data_only.setToolTip(
            "When checked, the 6 per-quarter Zacks filters (reported "
            "EPS / Rev, surprise EPS / Rev $ + %) and consec beats "
            "filters require actual data — NaN tickers are dropped. "
            "Unchecked = NaN passes those filters so non-earnings "
            "filters can still operate on tickers Zacks doesn't cover."
        )
        toolbar.addWidget(self.chk_earnings_data_only)

        toolbar.addSeparator()

        # Date pickers with custom range checkbox
        self.chk_custom_range = QCheckBox()
        self.chk_custom_range.setToolTip("Include custom date range in multi-timeframe scan")
        toolbar.addWidget(self.chk_custom_range)
        toolbar.addWidget(QLabel("Start: "))
        self.date_start = QDateEdit()
        self.date_start.setCalendarPopup(True)
        self.date_start.setDate(QDate(2025, 1, 1))
        self.date_start.setDisplayFormat("yyyy-MM-dd")
        toolbar.addWidget(self.date_start)

        toolbar.addWidget(QLabel("  End: "))
        self.date_end = QDateEdit()
        self.date_end.setCalendarPopup(True)
        # Default End to the last completed trading day, not raw "today"
        # (which on a weekend/holiday has no bar). The quick-range buttons
        # re-anchor to the latest ACTUAL data date; this is just the launch
        # default so a fresh scan targets current data.
        try:
            _lc = config.last_market_close().date()
            self.date_end.setDate(QDate(_lc.year, _lc.month, _lc.day))
        except Exception:
            self.date_end.setDate(QDate.currentDate())
        self.date_end.setDisplayFormat("yyyy-MM-dd")
        toolbar.addWidget(self.date_end)

        # Quick date range buttons with multi-timeframe checkboxes
        toolbar.addWidget(QLabel("  "))
        self.tf_checks: dict[int, QCheckBox] = {}
        _qdr_style = (
            "QPushButton { background: #3a5f8a; color: #ddd; font-size: 11px; "
            "padding: 3px 8px; border-radius: 3px; border: 1px solid #4a7aaa; }"
            "QPushButton:hover { background: #4a7aaa; color: #fff; }"
        )
        for label, days in [("1D", 1), ("1W", 7), ("1M", 30), ("3M", 90), ("6M", 180)]:
            chk = QCheckBox()
            chk.setToolTip(f"Include {label} in multi-timeframe scan")
            self.tf_checks[days] = chk
            toolbar.addWidget(chk)
            btn = QPushButton(label)
            btn.setFixedWidth(36)
            btn.setStyleSheet(_qdr_style)
            btn.clicked.connect(lambda checked, d=days: self._set_quick_range(d))
            toolbar.addWidget(btn)

        # Sequenced Run — exclusive batch mode. When ticked, all other
        # timeframe checkboxes are unchecked + disabled and a config
        # dialog opens to configure the date range and chunk size.
        toolbar.addWidget(QLabel("  "))
        self.chk_sequenced_run = QCheckBox("Sequenced Run")
        self.chk_sequenced_run.setToolTip(
            "Walk a date range backwards in fixed chunks (e.g., 2-month "
            "blocks). Each chunk runs as its own scan; results are "
            "tagged with their Period and merged into one table."
        )
        self.chk_sequenced_run.toggled.connect(self._on_sequenced_run_toggled)
        toolbar.addWidget(self.chk_sequenced_run)

        toolbar.addSeparator()

        # Preset controls
        toolbar.addWidget(QLabel("  Preset: "))
        self.preset_combo = QComboBox()
        self.preset_combo.setMinimumWidth(160)
        self._refresh_preset_list()
        toolbar.addWidget(self.preset_combo)

        btn_load_preset = QPushButton("Load")
        btn_load_preset.clicked.connect(self._load_preset)
        toolbar.addWidget(btn_load_preset)

        btn_save_preset = QPushButton("Save")
        btn_save_preset.clicked.connect(self._save_preset)
        toolbar.addWidget(btn_save_preset)

        btn_delete_preset = QPushButton("Delete")
        btn_delete_preset.clicked.connect(self._delete_preset)
        toolbar.addWidget(btn_delete_preset)

        toolbar.addSeparator()

        # Universe filters
        toolbar.addWidget(QLabel("  Universe: "))

        self.chk_include_etf = QCheckBox("Include ETFs")
        self.chk_include_etf.setChecked(False)
        toolbar.addWidget(self.chk_include_etf)

        self.chk_include_adr = QCheckBox("Include ADRs")
        self.chk_include_adr.setChecked(True)
        toolbar.addWidget(self.chk_include_adr)

        toolbar.addSeparator()

        self.chk_ipo_mode = QCheckBox("IPO Mode")
        self.chk_ipo_mode.setChecked(False)
        self.chk_ipo_mode.stateChanged.connect(self._on_ipo_toggle)
        toolbar.addWidget(self.chk_ipo_mode)

        toolbar.addWidget(QLabel(" Max Days: "))
        self.spin_ipo_days = QSpinBox()
        self.spin_ipo_days.setRange(1, 252)
        self.spin_ipo_days.setValue(14)
        self.spin_ipo_days.setEnabled(False)
        self.spin_ipo_days.setMinimumWidth(70)
        toolbar.addWidget(self.spin_ipo_days)

        toolbar.addSeparator()

        # Columns dropdown — opens ColumnsManagerDialog showing the
        # active output columns. Reorder via drag (multi-select OK),
        # toggle visibility via checkbox. Mirrors `_results_column_order`
        # + `_deleted_column_keys` and persists into presets so a saved
        # preset restores both layout and visibility on load.
        self.btn_columns = QPushButton("Columns ▾")
        self.btn_columns.setToolTip(
            "Open the column manager: drag to reorder (top → bottom = "
            "leftmost → rightmost in the table), uncheck to hide. "
            "Reset to default available inside the popup or via the "
            "table header right-click. Saved with the active preset."
        )
        self.btn_columns.clicked.connect(self._open_columns_dialog)
        toolbar.addWidget(self.btn_columns)
        self._columns_dialog: Optional[ColumnsManagerDialog] = None

        # --- Menu bar ---
        menubar = self.menuBar()
        data_menu = menubar.addMenu("Data")

        act_refresh_universe = QAction("Force Universe Refresh", self)
        act_refresh_universe.setToolTip(
            "Re-download the full ticker universe from all sources "
            "(NASDAQ, NYSE, OTC). Does NOT delete existing price data."
        )
        act_refresh_universe.triggered.connect(self._force_universe_refresh)
        data_menu.addAction(act_refresh_universe)

        act_refresh_ohlcv = QAction("Force OHLCV Refresh", self)
        act_refresh_ohlcv.setToolTip(
            "Re-check all tickers for stale/missing OHLCV data and "
            "download updates. Uses the existing ticker universe."
        )
        act_refresh_ohlcv.triggered.connect(self._force_ohlcv_refresh)
        data_menu.addAction(act_refresh_ohlcv)

        act_missing_only = QAction("Download Missing Tickers Only", self)
        act_missing_only.setToolTip(
            "Only download price data for tickers that have NO parquet file. "
            "Skips tickers that already have any cached data (even if stale)."
        )
        act_missing_only.triggered.connect(self._refresh_missing_only)
        data_menu.addAction(act_missing_only)

        data_menu.addSeparator()

        act_stop_refresh = QAction("Stop OHLCV Refresh", self)
        act_stop_refresh.setToolTip(
            "Immediately stop the running OHLCV background update. "
            "Progress so far is kept."
        )
        act_stop_refresh.triggered.connect(self._stop_ohlcv_refresh)
        data_menu.addAction(act_stop_refresh)

        act_reset_session = QAction("Reset yfinance Session", self)
        act_reset_session.setToolTip(
            "Clear yfinance's internal cookie/crumb cache and create a fresh "
            "HTTP session. May help bypass rate-limit fingerprinting."
        )
        act_reset_session.triggered.connect(self._reset_yfinance_session)
        data_menu.addAction(act_reset_session)

        # Phase 4 R9: on-demand cache rebuild for tickers where cached prices
        # look wrong (most commonly due to an unreflected stock split).
        act_rebuild = QAction("Rebuild Tickers...", self)
        act_rebuild.setToolTip(
            "Delete cached parquet(s) for specific tickers and re-download "
            "from scratch. Use for tickers with stale split-adjusted prices."
        )
        act_rebuild.triggered.connect(self._rebuild_tickers_dialog)
        data_menu.addAction(act_rebuild)

        act_finnhub = QAction("Set Finnhub API Key...", self)
        act_finnhub.setToolTip(
            "Store or update the Finnhub API key. When set, targeted "
            "earnings and sector fills try Finnhub before falling back "
            "to yfinance. Stored securely in the OS credential manager."
        )
        act_finnhub.triggered.connect(self._set_finnhub_key_dialog)
        data_menu.addAction(act_finnhub)

        data_menu.addSeparator()

        # Backoff toggle — mutable list so running workers see changes
        # live. Initial value loaded from QSettings in __init__ via
        # `_load_menu_toggles_pref` (defaults to True on first run).
        self._backoff_threshold = 10
        self._backoff_wait = 30
        self._max_retries = 3

        self.act_backoff_toggle = QAction("Enable Rate-Limit Backoff", self)
        self.act_backoff_toggle.setCheckable(True)
        self.act_backoff_toggle.setChecked(self._backoff_enabled_ref[0])
        self.act_backoff_toggle.setToolTip(
            "When enabled, the updater pauses and retries after detecting "
            "many consecutive download failures (rate limiting). "
            "Takes effect immediately, even on a running update. "
            "Persisted across sessions."
        )
        self.act_backoff_toggle.toggled.connect(self._on_backoff_toggled)
        data_menu.addAction(self.act_backoff_toggle)

        act_backoff = QAction("Backoff Settings...", self)
        act_backoff.setToolTip("Configure rate-limit detection and retry behavior")
        act_backoff.triggered.connect(self._show_backoff_settings)
        data_menu.addAction(act_backoff)

        data_menu.addSeparator()

        # Ticker blacklist
        self._blacklist: set[str] = set()
        self._zacks_blacklist: set[str] = set()
        self._finnhub_blacklist: set[str] = set()
        self._finviz_blacklist: set[str] = set()
        # Counter populated by `_on_zacks_failures` so `_on_zacks_done`
        # can report "added X new to skip list" cleanly. Reset every run.
        self._auto_added_zacks_skips: int = 0
        self._auto_added_finnhub_skips: int = 0
        self._auto_added_finviz_skips: int = 0
        self._ohlcv_error_tickers: list[str] = []
        self._load_blacklist()
        self._load_zacks_blacklist()
        self._load_finnhub_blacklist()
        self._load_finviz_blacklist()

        # Ticker greylist — scan-only filter that does NOT exclude tickers
        # from OHLCV / sector / earnings updates.
        self._greylist: set[str] = set()
        self._load_greylist()

        act_blacklist = QAction("Ticker Blacklist...", self)
        act_blacklist.setToolTip(
            "Comma-separated list of tickers to always skip during "
            "OHLCV refresh (e.g. delisted or problematic symbols)."
        )
        act_blacklist.triggered.connect(self._show_blacklist_editor)
        data_menu.addAction(act_blacklist)

        act_greylist = QAction("Ticker Greylist...", self)
        act_greylist.setToolTip(
            "Tickers to OMIT FROM SCANS only — OHLCV / sector / earnings "
            "updates still run for these tickers so you can re-enable them "
            "instantly. Use to temporarily de-clutter scan output without "
            "stopping data refresh."
        )
        act_greylist.triggered.connect(self._show_greylist_editor)
        data_menu.addAction(act_greylist)

        # --- Sector Data ---
        data_menu.addSeparator()

        act_bulk_sector = QAction("Bulk Fill Sector Map", self)
        act_bulk_sector.setToolTip(
            "Populate sector map using FinanceDatabase package (fast, offline)."
        )
        act_bulk_sector.triggered.connect(self._bulk_fill_sectors)
        data_menu.addAction(act_bulk_sector)

        act_tgt_sector = QAction("Targeted Fill Sector Map", self)
        act_tgt_sector.setToolTip(
            "Fill sector gaps via yfinance .info calls (slow, one per ticker)."
        )
        act_tgt_sector.triggered.connect(self._targeted_fill_sectors)
        data_menu.addAction(act_tgt_sector)

        act_stop_sector = QAction("Stop Sector Fill", self)
        act_stop_sector.setToolTip("Stop a running sector fill operation.")
        act_stop_sector.triggered.connect(self._stop_sector_fill)
        data_menu.addAction(act_stop_sector)

        # --- Earnings (Zacks — primary, Phase 6 §6.1) -----------------
        data_menu.addSeparator()
        earn_zacks_label = QAction("— Earnings (Zacks — primary) —", self)
        earn_zacks_label.setEnabled(False)
        data_menu.addAction(earn_zacks_label)

        act_bulk_zacks = QAction("Bulk Fill Earnings (Zacks)", self)
        act_bulk_zacks.setToolTip(
            "Scrape full per-quarter EPS + revenue history from Zacks for the "
            "entire universe. Multi-hour run — use only on first setup or when "
            "doing a deliberate full refresh."
        )
        act_bulk_zacks.triggered.connect(self._bulk_fill_zacks)
        data_menu.addAction(act_bulk_zacks)

        act_tgt_zacks = QAction("Targeted Fill Earnings (Zacks)", self)
        act_tgt_zacks.setToolTip(
            "Re-pull earnings history from Zacks for tickers with no rows in "
            "earnings_history.parquet. Skips tickers already covered."
        )
        act_tgt_zacks.triggered.connect(self._targeted_fill_zacks)
        data_menu.addAction(act_tgt_zacks)

        act_stop_zacks = QAction("Stop Zacks Fill", self)
        act_stop_zacks.setToolTip("Stop a running Zacks fill operation.")
        act_stop_zacks.triggered.connect(self._stop_zacks_fill)
        data_menu.addAction(act_stop_zacks)

        act_zacks_cookies = QAction("Set Zacks Cookies...", self)
        act_zacks_cookies.setToolTip(
            "Paste a fresh `key=value; key=value; ...` cookie string from a "
            "logged-in browser session. Required to bypass Imperva when the "
            "default impersonated TLS handshake gets flagged."
        )
        act_zacks_cookies.triggered.connect(self._set_zacks_cookies_dialog)
        data_menu.addAction(act_zacks_cookies)

        act_zacks_open_cookie_browser = QAction(
            "Refresh Zacks Cookies (Open Browser)...", self,
        )
        act_zacks_open_cookie_browser.setToolTip(
            "Open Firefox at the persistent Zacks profile (kept under "
            "scanner_data/firefox_zacks_profile/). The browser lands on "
            "the AAPL earnings calendar; complete any CAPTCHA / login, "
            "then close Firefox. The scanner watches for the close and "
            "captures the cookies automatically. Use Settings → Set "
            "Cookie Browser Monitor to pin the browser to a specific "
            "monitor."
        )
        act_zacks_open_cookie_browser.triggered.connect(
            self._refresh_zacks_cookies_action
        )
        data_menu.addAction(act_zacks_open_cookie_browser)
        # Reference for use by the imperva auto-pause path so it can
        # disable the action while a cookie-wait is in flight.
        self.act_zacks_open_cookie_browser = act_zacks_open_cookie_browser


        act_zacks_show_failures = QAction(
            "Show Last Zacks Failures...", self,
        )
        act_zacks_show_failures.setToolTip(
            "Per-ticker breakdown of the most recent Zacks fill's "
            "failures, grouped by cause: Imperva blocks, tickers Zacks "
            "doesn't cover (auto-added to the Zacks skip list), HTTP "
            "errors, parse errors."
        )
        act_zacks_show_failures.triggered.connect(
            self._show_last_zacks_failures
        )
        data_menu.addAction(act_zacks_show_failures)

        act_zacks_skip_list = QAction("Edit Zacks Skip List...", self)
        act_zacks_skip_list.setToolTip(
            "View and edit the Zacks-only skip list — tickers known "
            "not to be on Zacks (ETFs / ADRs / delisted). Auto-populated "
            "by failed Zacks lookups and by the bulk-fill ETF/ADR "
            "pre-skip. Honored ONLY by Zacks fills; OHLCV / sector / "
            "scan / Finn+YH paths still see these tickers normally."
        )
        act_zacks_skip_list.triggered.connect(
            self._show_zacks_skip_list_editor
        )
        data_menu.addAction(act_zacks_skip_list)

        # --- Earnings (Finviz — top-priority adjusted source) ---------
        # Scrapes the finviz earnings tab (ty=ea). Adjusted/non-GAAP EPS
        # matching Zacks ~98%, with finer revenue precision + real dates.
        # Highest dedup priority (finviz > zacks > finnhub). Paced slow
        # for an overnight-safe bulk; ETFs/ADRs + the OHLCV blacklist are
        # pre-skipped so funds never cost a request.
        data_menu.addSeparator()
        earn_finviz_label = QAction("— Earnings (Finviz — top priority) —", self)
        earn_finviz_label.setEnabled(False)
        data_menu.addAction(earn_finviz_label)

        act_bulk_finviz = QAction("Bulk Fill Earnings (Finviz)", self)
        act_bulk_finviz.setToolTip(
            "Scrape finviz earnings for the entire universe (ex-ETF/ADR "
            "and ex-blacklist). One request per ticker at a deliberately "
            "slow pace — intended to run OVERNIGHT (~11 hrs for a ~10k "
            "universe). Resumable: a killed run picks up from the last "
            "checkpoint on the next launch."
        )
        act_bulk_finviz.triggered.connect(self._bulk_fill_finviz)
        data_menu.addAction(act_bulk_finviz)

        act_gap_finviz = QAction("Gap Fill Earnings (Finviz)", self)
        act_gap_finviz.setToolTip(
            "Run finviz against tickers that have NO finviz-source rows "
            "yet in earnings_history.parquet. Tickers Zacks / Finnhub "
            "already cover ARE included — finviz coverage is independent."
        )
        act_gap_finviz.triggered.connect(self._gap_fill_finviz)
        data_menu.addAction(act_gap_finviz)

        act_spot_finviz = QAction("Spot Fill Earnings (Finviz)...", self)
        act_spot_finviz.setToolTip(
            "Fetch one ticker's finviz earnings on demand and write its "
            "rows immediately."
        )
        act_spot_finviz.triggered.connect(self._spot_fill_finviz)
        data_menu.addAction(act_spot_finviz)

        act_stop_finviz = QAction("Stop Finviz Fill", self)
        act_stop_finviz.setToolTip("Stop a running finviz fill operation.")
        act_stop_finviz.triggered.connect(self._stop_finviz_fill)
        data_menu.addAction(act_stop_finviz)

        act_finviz_skip_list = QAction("Edit Finviz Skip List...", self)
        act_finviz_skip_list.setToolTip(
            "View and edit the finviz-only skip list. Auto-populated by "
            "tickers finviz doesn't cover (no earningsData — ETFs, funds, "
            "brand-new listings). Honored ONLY by finviz fills."
        )
        act_finviz_skip_list.triggered.connect(
            self._show_finviz_skip_list_editor
        )
        data_menu.addAction(act_finviz_skip_list)

        # --- Earnings (Finnhub — deep history, Phase 2) ---------------
        # First-class earnings-history source via /stock/earnings.
        # Coexists with Zacks rows in earnings_history.parquet via
        # the (ticker, source) soft-PK; read-side dedup prefers Zacks.
        data_menu.addSeparator()
        earn_finnhub_label = QAction("— Earnings (Finnhub — deep history) —", self)
        earn_finnhub_label.setEnabled(False)
        data_menu.addAction(earn_finnhub_label)

        act_bulk_finnhub = QAction("Bulk Fill Earnings (Finnhub)", self)
        act_bulk_finnhub.setToolTip(
            "Pull /stock/earnings + /calendar/earnings for the entire "
            "universe. Two API calls per ticker @ ~52/min — ~9.5 hours "
            "for a 15k universe. Resumable: a killed run picks up from "
            "the last checkpoint on the next launch."
        )
        act_bulk_finnhub.triggered.connect(self._bulk_fill_finnhub)
        data_menu.addAction(act_bulk_finnhub)

        act_gap_finnhub = QAction("Gap Fill Earnings (Finnhub)", self)
        act_gap_finnhub.setToolTip(
            "Run Finnhub against tickers that have NO Finnhub-source "
            "rows yet in earnings_history.parquet. Tickers Zacks already "
            "covers ARE included — Finnhub coverage is independent."
        )
        act_gap_finnhub.triggered.connect(self._gap_fill_finnhub)
        data_menu.addAction(act_gap_finnhub)

        act_spot_finnhub = QAction("Spot Fill Earnings (Finnhub)...", self)
        act_spot_finnhub.setToolTip(
            "Fetch one ticker on demand and write its rows immediately. "
            "Useful when a freshly-listed ticker isn't covered by the "
            "last bulk."
        )
        act_spot_finnhub.triggered.connect(self._spot_fill_finnhub)
        data_menu.addAction(act_spot_finnhub)

        act_stop_finnhub = QAction("Stop Finnhub Fill", self)
        act_stop_finnhub.setToolTip("Stop a running Finnhub fill operation.")
        act_stop_finnhub.triggered.connect(self._stop_finnhub_fill)
        data_menu.addAction(act_stop_finnhub)

        act_finnhub_skip_list = QAction("Edit Finnhub Skip List...", self)
        act_finnhub_skip_list.setToolTip(
            "View and edit the Finnhub-only skip list. Auto-populated "
            "by tickers whose /stock/earnings response is empty (ETFs, "
            "funds, recently-IPO'd, delisted). Honored ONLY by Finnhub "
            "fills."
        )
        act_finnhub_skip_list.triggered.connect(
            self._show_finnhub_skip_list_editor
        )
        data_menu.addAction(act_finnhub_skip_list)

        # (EDGAR earnings menu removed 2026-05-31 — GAAP source dropped.)

        # --- Earnings dates (Nasdaq calendar + Yahoo, Phase 3) -------
        # Phase 3 split: the legacy "Finn+YH" combined path is gone.
        # Bulk uses the Nasdaq finance-calendars sweep; targeted gap
        # fills use yfinance only. Finnhub now has its own dedicated
        # bulk/gap/spot menu items (see "Earnings (Finnhub …)" above).
        data_menu.addSeparator()
        earn_dates_label = QAction("— Earnings dates (Nasdaq + Yahoo) —", self)
        earn_dates_label.setEnabled(False)
        data_menu.addAction(earn_dates_label)

        act_bulk_earn = QAction("Bulk Fill Earnings Dates (Nasdaq)", self)
        act_bulk_earn.setToolTip(
            "Sweep the Nasdaq earnings calendar for ±90 days via "
            "finance-calendars. Provides last/next dates only. ~2 min "
            "for the full window."
        )
        act_bulk_earn.triggered.connect(self._bulk_fill_earnings)
        data_menu.addAction(act_bulk_earn)

        act_tgt_earn = QAction("Targeted Fill Earnings Dates (Yahoo)", self)
        act_tgt_earn.setToolTip(
            "Fill earnings-date gaps via yfinance .earnings_dates "
            "(one HTTP call per ticker). last/next dates only."
        )
        act_tgt_earn.triggered.connect(self._targeted_fill_earnings)
        data_menu.addAction(act_tgt_earn)

        act_spot_yahoo = QAction("Spot Fill Earnings Dates (Yahoo)...", self)
        act_spot_yahoo.setToolTip(
            "Fetch a single ticker's last/next earnings via yfinance "
            "and write immediately."
        )
        act_spot_yahoo.triggered.connect(self._spot_fill_yahoo)
        data_menu.addAction(act_spot_yahoo)

        act_stop_earn = QAction("Stop Earnings-Dates Fill", self)
        act_stop_earn.setToolTip(
            "Stop a running Nasdaq or Yahoo earnings-dates fill."
        )
        act_stop_earn.triggered.connect(self._stop_earnings_fill)
        data_menu.addAction(act_stop_earn)

        # Daily Nasdaq auto-refresh toggle (Phase 5 of Finnhub augment).
        # The ONLY surviving piece of launch-time earnings automation.
        # Default on. Run cadence + last-run timestamp tracked via
        # QSettings; manual "Bulk Fill Earnings Dates (Nasdaq)" also
        # stamps the timestamp so the daily counter resets either way.
        self.act_nasdaq_auto_refresh = QAction(
            "Auto-refresh Nasdaq calendar daily", self,
        )
        self.act_nasdaq_auto_refresh.setCheckable(True)
        self.act_nasdaq_auto_refresh.setChecked(self._nasdaq_auto_refresh_ref[0])
        self.act_nasdaq_auto_refresh.setToolTip(
            "When enabled, the launch sequence runs a Nasdaq calendar "
            "bulk fill once per day — keeps last/next earnings dates fresh "
            "so newly-reported quarters are detected and captured. Manual "
            "'Bulk Fill Earnings Dates (Nasdaq)' resets the timer."
        )
        self.act_nasdaq_auto_refresh.toggled.connect(
            self._on_nasdaq_auto_refresh_toggled
        )
        data_menu.addAction(self.act_nasdaq_auto_refresh)

        # Manual trigger for the concurrent earnings smart refresh — the
        # same finviz + zacks + finnhub process that normally follows an
        # OHLCV update. Lets the user run it on demand (e.g. when the OHLCV
        # freshness gate skipped the update so the auto path never fired).
        act_smart_refresh_now = QAction(
            "Run Earnings Smart Refresh Now", self,
        )
        act_smart_refresh_now.setToolTip(
            "Manually run the launch-time concurrent earnings smart refresh "
            "(finviz + zacks + finnhub) against the tickers due for a new "
            "quarter. Progress shows in the panel above the status bar. If "
            "nothing is due, offers a small sample test run."
        )
        act_smart_refresh_now.triggered.connect(
            self._run_earnings_smart_refresh_now
        )
        data_menu.addAction(act_smart_refresh_now)

        # --- Cross-Check & Reconcile -----------------------------------
        # Phase 6.5: removed "Fill Finn+YH Gaps" — its function was
        # functionally equivalent to "Targeted Fill Earnings Dates
        # (Yahoo)" after Phase 4/5.5 (every fill auto-reconciles), and
        # the underlying `fill_yahoo_gaps` function chained through a
        # triple-reconcile path. The full-universe reconcile below
        # remains useful for manual cleanup.
        data_menu.addSeparator()
        crosscheck_label = QAction("— Cross-Check & Reconcile —", self)
        crosscheck_label.setEnabled(False)
        data_menu.addAction(crosscheck_label)

        act_reconcile = QAction("Reconcile earnings_dates.parquet", self)
        act_reconcile.setToolTip(
            "Force a full universe-wide reconcile: re-derive last/next "
            "earnings from the demoted-finnhub priority chain "
            "(zacks > nasdaq > yahoo > finnhub). Finnhub only fills "
            "tickers no other source covers. No network calls."
        )
        act_reconcile.triggered.connect(self._reconcile_earnings_dates_action)
        data_menu.addAction(act_reconcile)

        # --- Diagnostics (Phase 6.5) -----------------------------------
        # Pure-read tools — no network, no parquet writes (except the
        # opt-in auto-fix on the integrity check).
        data_menu.addSeparator()
        diagnostics_label = QAction("— Diagnostics —", self)
        diagnostics_label.setEnabled(False)
        data_menu.addAction(diagnostics_label)

        act_coverage = QAction("Earnings Coverage Report...", self)
        act_coverage.setToolTip(
            "Show which tickers in the universe are covered by Zacks "
            "only / Finnhub only / both / neither, plus the most-recent "
            "reported quarter per source. Pure read — sub-second."
        )
        act_coverage.triggered.connect(self._show_earnings_coverage_report)
        data_menu.addAction(act_coverage)

        act_integrity = QAction(
            "Verify earnings_history Integrity...", self,
        )
        act_integrity.setToolTip(
            "Check earnings_history.parquet for soft-PK violations "
            "(duplicate ticker+period+source), orphan rows, schema "
            "drift, and out-of-cap rows. Optional 'Auto-fix all "
            "fixable' button applies repairs with confirmation."
        )
        act_integrity.triggered.connect(
            self._verify_earnings_history_integrity
        )
        data_menu.addAction(act_integrity)

        # Phase 5 of the Finnhub augmentation: the launch-time
        # "Auto-refresh Zacks" toggle has been removed. Zacks is now
        # entirely manual. The only surviving auto-trigger is the
        # Nasdaq daily check (toggle below).

        data_menu.addSeparator()

        act_coverage = QAction("Data Coverage Gaps...", self)
        act_coverage.setToolTip(
            "Show tickers missing OHLCV, sector, or earnings data."
        )
        act_coverage.triggered.connect(self._show_coverage_gaps)
        data_menu.addAction(act_coverage)

        # --- Settings menu (May 2026) ---
        # Currently holds only the cookie-browser monitor preference,
        # but kept as a separate menu so future preferences land in a
        # consistent location instead of bloating Data.
        settings_menu = menubar.addMenu("Settings")

        self.act_set_cookie_monitor = QAction(
            "Set Cookie Browser Monitor to Current Window", self,
        )
        self.act_set_cookie_monitor.setToolTip(
            "Pin the Firefox cookie-refresh browser to the monitor the "
            "scanner is currently on. Future Refresh Zacks Cookies "
            "launches (manual + Imperva auto-pause) open Firefox "
            "maximized on this monitor. Run again from a different "
            "monitor to update."
        )
        self.act_set_cookie_monitor.triggered.connect(
            self._set_cookie_browser_monitor_action
        )
        settings_menu.addAction(self.act_set_cookie_monitor)

        self.act_clear_cookie_monitor = QAction(
            "Clear Cookie Browser Monitor", self,
        )
        self.act_clear_cookie_monitor.setToolTip(
            "Forget the saved cookie-browser monitor preference. The "
            "next Refresh Zacks Cookies launch lets the OS place "
            "Firefox wherever it wants."
        )
        self.act_clear_cookie_monitor.triggered.connect(
            self._clear_cookie_browser_monitor_action
        )
        settings_menu.addAction(self.act_clear_cookie_monitor)

        settings_menu.addSeparator()

        # Per-row HOTKEY settings — opens a modal editor with the click
        # position, delay, send cue, and end-sequence controls plus a
        # Reset to Defaults button. The HOTKEY toggle button itself
        # lives in the bottom button bar (right of the Excel button).
        self.act_hotkey_settings = QAction("Hotkey Settings…", self)
        self.act_hotkey_settings.setToolTip(
            "Configure the per-row HOTKEY ticker sender: target click "
            "position, send cue (right-click / shift+left etc.), delay "
            "between click and typing, and end-of-sequence keystroke."
        )
        self.act_hotkey_settings.triggered.connect(self._open_hotkey_settings)
        settings_menu.addAction(self.act_hotkey_settings)

        # Color Match Tolerance — fuzzy-day window for pairing
        # indicator dates with earnings report dates in the results
        # table color highlights. Default ±1 day; range 0–7.
        # Persists across sessions; applies on the next scan.
        self.act_match_color_tolerance = QAction(
            "Color Match Tolerance…", self,
        )
        self.act_match_color_tolerance.setToolTip(
            "Set the tolerance (in calendar days) for matching an "
            "indicator date against an earnings report date in the "
            "results table color highlights. 0 = exact, 1 = ±1 day "
            "(default), up to 7. Visual only — does not affect any "
            "funnel filter or earnings statistic."
        )
        self.act_match_color_tolerance.triggered.connect(
            self._open_match_color_tolerance_settings
        )
        settings_menu.addAction(self.act_match_color_tolerance)

        settings_menu.addSeparator()

        # SEC EDGAR contact email — SEC's fair-access policy requires a real
        # contact email in the request User-Agent for the company_tickers.json
        # download (ticker-universe Source 4). Stored per-user in
        # scanner_data/sec_contact.txt; until set, the SEC source is skipped.
        self.act_sec_contact = QAction("Set SEC Contact Email…", self)
        self.act_sec_contact.setToolTip(
            "Set the contact email SEC EDGAR requires in the request "
            "User-Agent for the ticker-universe download. Until set, the "
            "SEC source is skipped (the other three universe sources still "
            "run). Stored locally in scanner_data/sec_contact.txt."
        )
        self.act_sec_contact.triggered.connect(self._set_sec_contact_email_dialog)
        settings_menu.addAction(self.act_sec_contact)

        settings_menu.addSeparator()

        # Advanced tunables — OHLCV/earnings history depths + the
        # reference/benchmark ticker list. Persisted per-user to
        # scanner_data/user_config.json (loaded at config import) and
        # applied to the live config module on OK — no restart needed.
        self.act_advanced_settings = QAction("Advanced…", self)
        self.act_advanced_settings.setToolTip(
            "Edit advanced tunables: OHLCV cache depth (years), earnings "
            "history depth (years), and the reference/benchmark ticker "
            "list used for RS calculations. Stored locally in "
            "scanner_data/user_config.json."
        )
        self.act_advanced_settings.triggered.connect(
            self._show_advanced_settings
        )
        settings_menu.addAction(self.act_advanced_settings)

        # --- Central layout ---
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(6, 6, 6, 6)

        # Top splitter: indicators | (results + log)
        top_splitter = QSplitter(Qt.Orientation.Horizontal)

        # Left: indicator panel
        self.indicator_panel = IndicatorPanel()
        top_splitter.addWidget(self.indicator_panel)

        # Phase 7 §7.2: when either Consecutive Beats filter is active,
        # Sequenced Run becomes ineligible (single-window scan required
        # for streak semantics). Multi-timeframe checkboxes stay
        # available because they all consolidate to today's date and
        # produce identical streak data.
        self.indicator_panel.beats_filter_toggled.connect(
            self._on_beats_filter_toggled
        )

        # Right: search bar + results table above, log below
        right_splitter = QSplitter(Qt.Orientation.Vertical)

        # Search bar above results
        results_container = QWidget()
        results_vbox = QVBoxLayout(results_container)
        results_vbox.setContentsMargins(0, 0, 0, 0)
        results_vbox.setSpacing(2)

        search_row = QHBoxLayout()
        search_row.addWidget(QLabel("Search:"))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Type to filter results by ticker...")
        self.search_input.setClearButtonEnabled(True)
        self.search_input.setMaximumWidth(300)
        self.search_input.textChanged.connect(self._on_search_changed)
        search_row.addWidget(self.search_input)

        # Timeframe selector — shown when a multi-timeframe or sequenced
        # scan produced multiple period DataFrames. Switching it re-paints
        # the results table and re-targets Send-to-Watchlist + Export +
        # Excel to the selected period.
        search_row.addWidget(QLabel("    Timeframe:"))
        self.combo_timeframe = QComboBox()
        self.combo_timeframe.setMinimumWidth(220)
        self.combo_timeframe.setEnabled(False)
        self.combo_timeframe.currentIndexChanged.connect(self._on_timeframe_changed)
        search_row.addWidget(self.combo_timeframe)

        # View-only filters that hide rows in the displayed table
        # without affecting the underlying scan results. Applied
        # POST-SCAN: toggling any re-paints the table with a filtered
        # slice of the active period's df. The same filter carries
        # through to Send-to-Watchlist and Excel export so what the
        # user sees is what gets exported.
        # Persistence: states are saved across timeframe switches
        # (the slot reads the current toggle state on each switch)
        # and across preset save/load (v4 schema).
        # Dates / Data are independent and parallel the scan-level
        # toolbar split. Invariant: a row passing the data view filter
        # also passes the dates view filter (data implies date).
        self.chk_view_earnings_dates_only = QCheckBox("Earnings Dates (view)")
        self.chk_view_earnings_dates_only.setToolTip(
            "View-only: hide tickers without ANY earnings touchpoint — "
            "neither a calendar date (last_report_date / next_earnings_"
            "date / days_since / days_to) NOR any earnings data value. "
            "Broader than 'Earnings Data (view)'. Doesn't affect scan "
            "results — just the displayed table, exports, and Send-to-"
            "Watchlist. Persists across timeframe switches."
        )
        self.chk_view_earnings_dates_only.toggled.connect(
            lambda _on: self._reapply_view_filters_for_active_period()
        )
        search_row.addWidget(self.chk_view_earnings_dates_only)

        self.chk_view_earnings_data_only = QCheckBox("Earnings Data (view)")
        self.chk_view_earnings_data_only.setToolTip(
            "View-only: hide tickers without any earnings data — must "
            "have at least one non-NaN value across the 6 most-recent-"
            "quarter Zacks columns or any q-beats column. Doesn't "
            "affect scan results. Persists across timeframe switches."
        )
        self.chk_view_earnings_data_only.toggled.connect(
            lambda _on: self._reapply_view_filters_for_active_period()
        )
        search_row.addWidget(self.chk_view_earnings_data_only)

        self.chk_view_color_match_only = QCheckBox("Color Match Only")
        self.chk_view_color_match_only.setToolTip(
            "View-only: hide tickers without at least one color-date "
            "match (where a non-earnings indicator date landed on an "
            "earnings report date). Doesn't affect scan results."
        )
        self.chk_view_color_match_only.toggled.connect(
            lambda _on: self._reapply_view_filters_for_active_period()
        )
        search_row.addWidget(self.chk_view_color_match_only)

        # Interleave EPS+Rev quarters: rearrange the per-quarter blocks
        # so all data for one quarter sits adjacent (Q-1 EPS+Rev, then
        # Q-2 EPS+Rev, ...). Only kicks in when both sides have data —
        # no behavior change when only one side is rendered. Lives next
        # to the view-filter checkboxes since it's a display-only
        # toggle that doesn't affect scan results.
        self.chk_view_interleave_quarters = QCheckBox("Interleave Q EPS+Rev")
        self.chk_view_interleave_quarters.setToolTip(
            "View-only: when BOTH consecutive EPS Beats and Rev Beats "
            "are populated, group each quarter's EPS and Rev columns "
            "side by side (Q-1 EPS+Rev together, then Q-2 EPS+Rev, "
            "etc.) instead of all EPS quarters first then all Rev. "
            "No-op when only one side has data. Doesn't affect scan "
            "results."
        )
        self.chk_view_interleave_quarters.toggled.connect(
            self._on_interleave_quarters_toggled
        )
        search_row.addWidget(self.chk_view_interleave_quarters)

        search_row.addStretch()
        results_vbox.addLayout(search_row)

        self.results_table = ResultsTable()
        # Persist user-defined column order across timeframe switches
        # and propagate to Excel export. The table fires this signal
        # whenever the user drags a column or uses the right-click
        # menu (Send to Front / Send to End / Reset).
        self.results_table.column_order_changed.connect(
            self._on_results_column_order_changed
        )
        self.results_table.rows_deletion_requested.connect(
            self._on_rows_deletion_requested
        )
        self.results_table.rows_paste_requested.connect(
            self._on_rows_paste_requested
        )
        self.results_table.columns_deletion_requested.connect(
            self._on_columns_deletion_requested
        )
        self.results_table.columns_reset_requested.connect(
            self._reset_columns_to_default
        )
        # Per-session set of column keys the user has hidden via the
        # header right-click menu. Hidden columns are dropped from
        # `_apply_view_filters` output → no impact on `_period_results`
        # so the underlying scan data is preserved. Reset on every
        # fresh scan (alongside the cut clipboard).
        self._deleted_column_keys: set[str] = set()
        # Per-row HOTKEY: intercept the configured cue.
        #   - Mouse cues (right-click, shift+left, ctrl+left, middle):
        #     filter on the viewport — that's where Qt routes mouse
        #     presses on the data area.
        #   - Keyboard cue (Enter on selected row): filter on the table
        #     itself — key events go to the focused widget, not the
        #     viewport. Lets the user navigate with arrow keys then
        #     press Enter to fire the send.
        # Filters are always installed; the cue check no-ops when
        # `_hotkey_enabled` is off so passthrough behavior is unchanged.
        self.results_table.viewport().installEventFilter(self)
        self.results_table.installEventFilter(self)
        results_vbox.addWidget(self.results_table, 1)

        right_splitter.addWidget(results_container)

        self.log_panel = LogPanel()
        self.log_panel.btn_blacklist_errors.clicked.connect(
            self._send_ohlcv_errors_to_blacklist
        )
        right_splitter.addWidget(self.log_panel)
        right_splitter.setSizes([500, 200])

        top_splitter.addWidget(right_splitter)
        top_splitter.setSizes([540, 900])

        main_layout.addWidget(top_splitter, 1)

        # --- Pre-transfer summary ---
        self.summary_label = QLabel("")
        self.summary_label.setStyleSheet(
            "font-size: 12px; padding: 4px; background: #2d2d2d; "
            "color: #e0e0e0; border-radius: 4px;"
        )
        main_layout.addWidget(self.summary_label)

        # --- Bottom button bar ---
        btn_bar = QHBoxLayout()

        self.btn_scan = QPushButton("  Run Scan  ")
        self.btn_scan.setStyleSheet(
            "QPushButton { background: #2e7d32; color: white; font-size: 14px; "
            "font-weight: bold; padding: 8px 24px; border-radius: 4px; }"
            "QPushButton:hover { background: #388e3c; }"
        )
        self.btn_scan.clicked.connect(self._run_scan)
        btn_bar.addWidget(self.btn_scan)

        self.btn_stop = QPushButton("  Stop  ")
        self.btn_stop.setEnabled(False)
        self.btn_stop.setStyleSheet(
            "QPushButton { background: #c62828; color: white; font-size: 14px; "
            "padding: 8px 16px; border-radius: 4px; }"
            "QPushButton:hover { background: #d32f2f; }"
        )
        self.btn_stop.clicked.connect(self._stop_scan)
        btn_bar.addWidget(self.btn_stop)

        self.btn_export = QPushButton("  Export  ")
        self.btn_export.setEnabled(False)
        self.btn_export.setStyleSheet(
            "QPushButton { background: #e65100; color: white; font-size: 14px; "
            "padding: 8px 16px; border-radius: 4px; }"
            "QPushButton:hover { background: #f57c00; }"
            "QPushButton:disabled { background: #555; color: #999; }"
        )
        self.btn_export.clicked.connect(self._export_results)
        btn_bar.addWidget(self.btn_export)

        # Excel button — opens ExcelExportDialog with column selector + News
        # toggle + format choice. Every column starts pre-checked; the user
        # uses Select-None / individual checkboxes to refine.
        self.btn_excel = QPushButton("  Excel  ")
        self.btn_excel.setEnabled(False)
        self.btn_excel.setStyleSheet(
            "QPushButton { background: #2e7d32; color: white; font-size: 14px; "
            "padding: 8px 16px; border-radius: 4px; }"
            "QPushButton:hover { background: #388e3c; }"
            "QPushButton:disabled { background: #555; color: #999; }"
        )
        self.btn_excel.clicked.connect(self._excel_export_dialog)
        btn_bar.addWidget(self.btn_excel)

        # HOTKEY toggle — flips the per-row ticker sender on/off. Grey
        # when off, hot pink with bright purple text when on. Settings
        # for click position / cue / delay / end-sequence live under
        # Settings → Hotkey Settings…. Toggle defaults to off on each
        # launch (see `_hotkey_enabled` in __init__).
        self.btn_hotkey = QPushButton("  HOTKEY  ")
        self.btn_hotkey.setCheckable(True)
        self.btn_hotkey.setToolTip(
            "Toggle the per-row hotkey ticker sender. When ON, the "
            "configured cue (right-click etc.) on a results-table row "
            "clicks the saved screen position, types that row's ticker, "
            "and presses the end key. Configure under Settings → Hotkey "
            "Settings…."
        )
        self._apply_hotkey_button_style(False)
        self.btn_hotkey.clicked.connect(self._toggle_hotkey)
        btn_bar.addWidget(self.btn_hotkey)

        btn_bar.addStretch()

        self.btn_send = QPushButton("  Send to Watchlist  ")
        self.btn_send.setEnabled(False)
        self.btn_send.setStyleSheet(
            "QPushButton { background: #1565c0; color: white; font-size: 14px; "
            "padding: 8px 24px; border-radius: 4px; }"
            "QPushButton:hover { background: #1976d2; }"
            "QPushButton:disabled { background: #555; color: #999; }"
        )
        self.btn_send.clicked.connect(self._send_to_watchlist)
        btn_bar.addWidget(self.btn_send)

        # Stop Sending — only enabled while a watchlist send is running.
        # Halts the bridge mid-list and offers Cancel / Resume / Restart.
        self.btn_stop_send = QPushButton("  Stop Sending  ")
        self.btn_stop_send.setEnabled(False)
        self.btn_stop_send.setStyleSheet(
            "QPushButton { background: #c62828; color: white; font-size: 14px; "
            "padding: 8px 16px; border-radius: 4px; }"
            "QPushButton:hover { background: #d32f2f; }"
            "QPushButton:disabled { background: #555; color: #999; }"
        )
        self.btn_stop_send.clicked.connect(self._stop_sending)
        btn_bar.addWidget(self.btn_stop_send)

        self.btn_manual = QPushButton("  Manual Input STW  ")
        self.btn_manual.setStyleSheet(
            "QPushButton { background: #6a1b9a; color: white; font-size: 14px; "
            "padding: 8px 24px; border-radius: 4px; }"
            "QPushButton:hover { background: #7b1fa2; }"
        )
        self.btn_manual.clicked.connect(self._manual_input_stw)
        btn_bar.addWidget(self.btn_manual)

        main_layout.addLayout(btn_bar)

        # --- Session controls ---
        session_bar = QHBoxLayout()
        self.chk_omit_seen = QCheckBox("Omit previously scanned tickers")
        self.chk_omit_seen.setStyleSheet("color: #e0e0e0; font-size: 12px;")
        session_bar.addWidget(self.chk_omit_seen)

        self.chk_omit_intra_run = QCheckBox("Omit earlier-period hits (this run)")
        self.chk_omit_intra_run.setStyleSheet("color: #e0e0e0; font-size: 12px;")
        self.chk_omit_intra_run.setToolTip(
            "Within a multi-timeframe or sequenced run, prevent tickers "
            "that passed an earlier period from appearing in any later "
            "period. Periods always execute shortest→longest (or "
            "newest-chunk-first for sequenced runs), so the most "
            "proximal timeframe gets first claim on each ticker."
        )
        session_bar.addWidget(self.chk_omit_intra_run)

        btn_reset_session = QPushButton("Reset Session")
        btn_reset_session.setStyleSheet(
            "QPushButton { background: #555; color: white; font-size: 11px; "
            "padding: 4px 12px; border-radius: 3px; }"
            "QPushButton:hover { background: #666; }"
        )
        btn_reset_session.clicked.connect(self._reset_session)
        session_bar.addWidget(btn_reset_session)

        self.session_label = QLabel("Session: 0 scans, 0 tickers seen")
        self.session_label.setStyleSheet("color: #aaa; font-size: 11px;")
        session_bar.addWidget(self.session_label)
        session_bar.addStretch()
        main_layout.addLayout(session_bar)

        # --- Earnings smart-refresh progress panel (above status bar) ---
        self._build_earnings_progress_panel(main_layout)

        # --- Status bar ---
        self.status = QStatusBar()
        self.setStatusBar(self.status)
        self.status.showMessage("Ready")

        # Persistent OHLCV update indicator (right side of status bar)
        self._update_label = QLabel("")
        self._update_label.setStyleSheet(
            "color: #aaa; font-size: 11px; padding: 0 8px;"
        )
        self.status.addPermanentWidget(self._update_label)

    # ── Earnings smart-refresh progress panel ───────────────────────────
    # The 3-bar progress panel and the wider multi-source earnings refresh
    # orchestration live in EarningsRefreshCoordinator
    # (earnings_coordinator.py). MainWindow keeps every historical method
    # name below as a thin delegate so signal wiring, menu actions, and
    # tests are untouched.

    _EARN_SOURCES = EarningsRefreshCoordinator._EARN_SOURCES
    _EARN_SRC_COLORS = EarningsRefreshCoordinator._EARN_SRC_COLORS

    @property
    def _earn_coord(self) -> EarningsRefreshCoordinator:
        """Lazily-created earnings-refresh coordinator. Looked up via
        ``__dict__`` (not getattr) because bare ``MainWindow.__new__``
        test shells raise RuntimeError when attribute lookup falls
        through to the uninitialized Qt C++ side."""
        coord = self.__dict__.get("_earn_coord_obj")
        if coord is None:
            coord = EarningsRefreshCoordinator(self)
            self.__dict__["_earn_coord_obj"] = coord
        return coord

    # Coordinator-owned state, re-exposed under the historical attribute
    # names (tests and intra-window code read these on the window).

    @property
    def _earn_prog_state(self) -> dict[str, dict]:
        return self._earn_coord._earn_prog_state

    @property
    def _earn_prog_bars(self) -> dict[str, QProgressBar]:
        return self._earn_coord._earn_prog_bars

    @property
    def _earn_prog_panel(self) -> QFrame:
        return self._earn_coord._earn_prog_panel

    @property
    def _earn_stop_btn(self) -> QPushButton:
        return self._earn_coord._earn_stop_btn

    def _build_earnings_progress_panel(self, main_layout) -> None:
        self._earn_coord._build_earnings_progress_panel(main_layout)

    def _earn_prog_tooltip_text(self, src: str) -> str:
        return self._earn_coord._earn_prog_tooltip_text(src)

    def _earn_prog_set_idle(self, src: str) -> None:
        self._earn_coord._earn_prog_set_idle(src)

    def _earn_prog_begin(self, src: str, label: str) -> None:
        self._earn_coord._earn_prog_begin(src, label)

    def _earn_prog_tick(self, src: str, done: int, total: int) -> None:
        self._earn_coord._earn_prog_tick(src, done, total)

    def _earn_prog_finish(self, src: str, filled: int, errors: int) -> None:
        self._earn_coord._earn_prog_finish(src, filled, errors)

    def _earn_state_active(self) -> bool:
        return self._earn_coord._earn_state_active()

    def _earn_threads_active(self) -> bool:
        return self._earn_coord._earn_threads_active()

    def _earn_prog_maybe_collapse(self) -> None:
        self._earn_coord._earn_prog_maybe_collapse()

    def _earn_prog_autohide(self) -> None:
        self._earn_coord._earn_prog_autohide()

    def _stop_all_earnings_fills(self) -> None:
        self._earn_coord._stop_all_earnings_fills()

    # ── Log handler ────────────────────────────────────────────────────

    def _install_log_handler(self):
        formatter = logging.Formatter(
            "%(asctime)s  %(name)-22s  %(levelname)-7s  %(message)s",
            datefmt="%H:%M:%S",
        )
        handler = QtLogHandler(self.log_panel.append_signal)
        handler.setFormatter(formatter)
        # `_log_error` writes its user-facing message to the panel
        # directly AND logs the full record (incl. traceback) at ERROR.
        # That record propagates up to this handler on the `scanner`
        # logger, so without a guard every `_log_error` site showed in
        # the panel TWICE — once verbatim, once as the formatted record
        # plus traceback. Records tagged `extra={"panel_skip": True}`
        # are dropped by this GUI handler only; propagation to every
        # other handler (subsystem files, console) is unaffected, so
        # the full record still lands everywhere else.
        handler.addFilter(
            lambda record: not getattr(record, "panel_skip", False)
        )
        logging.getLogger("scanner").addHandler(handler)
        logging.getLogger("scanner").setLevel(logging.INFO)
        # Track the GUI handler so closeEvent can detach it cleanly.
        # Without this, multiple-MainWindow test runs (or any
        # construct-then-discard pattern) leak handlers attached to
        # the global `scanner` logger; subsequent log emissions try
        # to push messages into a deleted Qt signal and the process
        # crashes with a Windows access violation. The crash isn't a
        # Python exception so try/except inside `QtLogHandler.emit`
        # can't catch it — only detaching at close time prevents the
        # dangling reference.
        self._gui_log_handler = handler

        # Phase 6 O10: per-subsystem log files alongside the main panel log,
        # so users can grep OHLCV download issues independently from scan
        # funnel output, universe refresh, or watchlist bridge activity.
        # The full session log lives in scan_{ts}.log (written by LogPanel)
        # — these are additive, one file per logger prefix.
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._subsystem_log_handlers: list[logging.Handler] = []
        for logger_name, filename in [
            ("scanner.data",         f"ohlcv_{ts}.log"),
            ("scanner.universe",     f"universe_{ts}.log"),
            ("scanner.tradestation", f"bridge_{ts}.log"),
        ]:
            try:
                fh = logging.FileHandler(
                    config.LOG_DIR / filename, encoding="utf-8"
                )
                fh.setFormatter(formatter)
                logging.getLogger(logger_name).addHandler(fh)
                self._subsystem_log_handlers.append(fh)
            except OSError as exc:
                log.warning("Could not create subsystem log %s: %s", filename, exc)

    # ── Shared error-reporting + worker-bringup helpers ───────────────

    def _log_error(self, category: str, msg: str,
                   exc: Exception | None = None) -> None:
        """Report an error to BOTH the rotating module log and the GUI
        log panel — consolidates the repeated ``log.error(...)`` +
        ``self.log_panel.write_line(...)`` pair so a failure can't show
        up in one channel while silently missing from the other.

        ``category`` prefixes the logger record (greppable subsystem
        tag); the panel shows ``msg`` verbatim so existing user-facing
        wording is preserved at refactored call sites. When ``exc`` is
        given the logger record carries the full traceback. The record
        is tagged ``panel_skip`` so the QtLogHandler on the parent
        ``scanner`` logger doesn't echo it back into the panel — the
        panel shows exactly ONE clean line per call. The panel write is
        guarded so reporting never raises from teardown / crash-handler
        paths."""
        log.error("%s: %s", category, msg, exc_info=exc,
                  extra={"panel_skip": True})
        try:
            self.log_panel.write_line(msg)
        except Exception as panel_exc:
            log.debug("log panel write failed (%s): %s", category, panel_exc)

    def _start_worker(self, worker, **connections):
        """Wire and start a QThread worker — consolidates the repeated
        assign → connect(log_msg / progress / finished / …) → start()
        block used by the worker launch sites.

        Each ``signal_name=slot`` kwarg is connected onto the worker.
        ``log_msg`` auto-connects to the log panel when the worker
        exposes it and the caller didn't supply an override. Returns
        the worker so call sites keep their one-line attribute
        assignment. Launch sites that fan one signal out to multiple
        slots keep their explicit wiring (kwargs can't repeat a key)."""
        if "log_msg" not in connections and hasattr(worker, "log_msg"):
            connections["log_msg"] = self.log_panel.write_line
        for signal_name, slot in connections.items():
            getattr(worker, signal_name).connect(slot)
        worker.start()
        return worker

    # ── Universe + Auto-update ────────────────────────────────────────

    def _startup(self):
        """Load universe and kick off OHLCV update. Handles first-run."""
        if not config.TICKER_CSV.exists():
            # First run -- download universe first, then update OHLCV
            self.status.showMessage("First run -- downloading ticker universe...")
            self.log_panel.write_line("First run detected. Downloading ticker universe...")
            self._universe_worker = self._start_worker(
                UniverseWorker(),
                finished=self._on_universe_downloaded,
            )
        else:
            # Refresh universe if stale (picks up new IPOs/delistings)
            # refresh_universe() has a built-in 7-day staleness guard
            self._universe_worker = self._start_worker(
                UniverseRefreshWorker(),
                finished=lambda _: self._load_universe_and_update(),
            )

    def _on_universe_downloaded(self, df):
        """Called when first-run universe download completes."""
        if df is None or df.empty:
            self.status.showMessage("Universe download failed. Check internet connection.")
            return
        self._load_universe_and_update()

    def _load_universe_and_update(self, force: bool = False):
        """Load universe from CSV, then start background OHLCV update.

        ``force=True`` (the explicit Force OHLCV Refresh menu action) bypasses
        the launch-time freshness gate so the update re-runs even when the
        cache looks current. The per-ticker staleness check inside UpdateWorker
        still applies, so force only re-checks — it won't redundantly re-fetch
        bars that are already up to date."""
        try:
            universe = load_universe()
            self._universe_df = universe
            all_syms = [
                s for s in universe["symbol"].tolist()
                if isinstance(s, str) and s.strip()
            ]
        except FileNotFoundError:
            self._symbols = []
            self.status.showMessage("No universe file found.")
            self.log_panel.write_line("ERROR: No universe CSV found.")
            return

        # Tickers with existing cached data are immediately scannable
        self._symbols = [
            s for s in all_syms
            if (config.PARQUET_DIR / f"{s}.parquet").exists()
        ]
        self.status.showMessage(
            f"Universe: {len(universe)} tickers, "
            f"{len(self._symbols)} with cached OHLCV -- checking for updates..."
        )
        self.log_panel.write_line(
            f"Universe: {len(universe)} total, {len(self._symbols)} with OHLCV data"
        )

        # Launch-time freshness gate: if a completed OHLCV update already
        # ran since the most recent market close, there's no new bar to
        # fetch — skip the update (and therefore the earnings refresh that
        # hangs off _on_update_done). Manual "Update Now" bypasses this.
        if not force and not self._is_ohlcv_due():
            self._update_label.setText(
                f"OHLCV: {len(self._symbols)} cached, current"
            )
            self._update_label.setStyleSheet(
                "color: #4caf50; font-size: 11px; padding: 0 8px;"
            )
            self.status.showMessage(
                f"Ready: {len(self._symbols)} tickers with OHLCV data "
                f"(cache current — no update needed)"
            )
            # Daily Nasdaq calendar sweep runs on its own cadence,
            # independent of OHLCV freshness. Chain the earnings smart
            # refresh off the sweep (same-launch capture) so a quarter the
            # calendar just discovered — `last_earnings` newer than anything
            # we've captured — gets its actual fetched THIS launch, even when
            # the OHLCV cache is current. Previously the smart refresh was
            # gated to the OHLCV-update path, so on OHLCV-fresh launches the
            # calendar learned a report happened but no source ever fetched
            # the actual (the report sat uncaptured indefinitely).
            self._arm_or_run_smart_refresh(
                want_smart=bool(self._earnings_auto_refresh_ref[0]),
            )
            return

        # Start background OHLCV update for all tickers
        self._update_label.setText("OHLCV update: starting...")
        self._update_label.setStyleSheet(
            "color: #4a90d9; font-size: 11px; padding: 0 8px;"
        )
        # Filter out blacklisted tickers
        update_syms = [s for s in all_syms if s not in self._blacklist]
        if len(update_syms) < len(all_syms):
            skipped = len(all_syms) - len(update_syms)
            self.log_panel.write_line(f"Blacklist: skipping {skipped} tickers")

        # Ensure reference/benchmark tickers are always updated
        ref_set = set(config.REFERENCE_TICKERS)
        existing_set = set(update_syms)
        for ref in config.REFERENCE_TICKERS:
            if ref not in existing_set and ref not in self._blacklist:
                update_syms.insert(0, ref)
        self.log_panel.write_line(
            f"Reference tickers: {len(ref_set)} benchmarks ensured in OHLCV cache"
        )

        self._update_worker = self._start_worker(
            UpdateWorker(
                update_syms,
                backoff_enabled_ref=self._backoff_enabled_ref,
                backoff_threshold=self._backoff_threshold,
                backoff_wait=self._backoff_wait,
                max_retries=self._max_retries,
            ),
            progress=self._on_update_progress,
            error_tickers=self._on_ohlcv_error_tickers,
            finished=self._on_update_done,
        )

    @pyqtSlot(int, int)
    def _on_update_progress(self, done: int, total: int):
        pct = done * 100 // total if total else 0
        self._update_label.setText(
            f"OHLCV update: {done}/{total} ({pct}%)"
        )
        self._update_label.setStyleSheet(
            "color: #4a90d9; font-size: 11px; padding: 0 8px;"
        )

    def _on_ohlcv_error_tickers(self, tickers: list):
        """Store failed OHLCV tickers and list them in the log."""
        self._ohlcv_error_tickers = list(tickers)
        if not tickers:
            self.log_panel.btn_blacklist_errors.setVisible(False)
            return
        self.log_panel.write_line(
            f"── Failed OHLCV tickers ({len(tickers)}) ──"
        )
        # Show them in rows of 10 for readability
        for i in range(0, len(tickers), 10):
            chunk = tickers[i:i + 10]
            self.log_panel.write_line("  " + ", ".join(chunk))
        self.log_panel.btn_blacklist_errors.setVisible(True)

    @pyqtSlot(int, int)
    def _on_update_done(self, updated: int, errors: int):
        # OHLCV just advanced — invalidate the latest-data-date cache FIRST,
        # before the universe reload (which can raise on a corrupt/locked
        # CSV). The recompute reads only the on-disk parquets, so the
        # quick-range buttons still re-anchor End to the freshest bar even
        # if the symbol-list reload below fails.
        self._latest_data_date_cache = None
        # Reload symbol list to pick up newly downloaded tickers
        try:
            universe = load_universe()
            self._universe_df = universe
            all_syms = [
                s for s in universe["symbol"].tolist()
                if isinstance(s, str) and s.strip()
            ]
            self._symbols = [
                s for s in all_syms
                if (config.PARQUET_DIR / f"{s}.parquet").exists()
            ]
        except FileNotFoundError as exc:
            log.debug(
                "universe reload after OHLCV update skipped "
                "(keeping prior symbol list): %s", exc,
            )

        if errors == 0:
            color = "#4caf50"
            status = f"OHLCV: {len(self._symbols)} cached, {updated} updated"
        else:
            color = "#ff9800"
            status = f"OHLCV: {len(self._symbols)} cached, {updated} updated, {errors} errors"
        self._update_label.setText(status)
        self._update_label.setStyleSheet(
            f"color: {color}; font-size: 11px; padding: 0 8px;"
        )
        self.status.showMessage(f"Ready: {len(self._symbols)} tickers with OHLCV data")

        # Record this as the last completed OHLCV update so the next launch
        # can skip if no market close has happened since — UNLESS the run
        # was stopped early (partial cache → must re-run next launch).
        stopped = bool(
            self._update_worker is not None
            and getattr(self._update_worker, "_stop", None) is not None
            and self._update_worker._stop.is_set()
        )
        if not stopped:
            self._stamp_ohlcv_run_now()

        # Daily Nasdaq calendar sweep (own cadence) + earnings smart
        # refresh, with same-launch capture (see _arm_or_run_smart_refresh).
        # A stopped/partial OHLCV update suppresses the smart refresh here
        # (want_smart=False); the OHLCV-current path arms it independently.
        self._arm_or_run_smart_refresh(
            want_smart=bool(self._earnings_auto_refresh_ref[0] and not stopped),
        )

    # Same-launch capture flag — owned by the coordinator, re-exposed
    # under the historical name (tests and __init__ assign it directly).

    @property
    def _pending_smart_refresh(self) -> bool:
        return self._earn_coord._pending_smart_refresh

    @_pending_smart_refresh.setter
    def _pending_smart_refresh(self, value: bool) -> None:
        self._earn_coord._pending_smart_refresh = value

    def _arm_or_run_smart_refresh(self, *, want_smart: bool) -> None:
        self._earn_coord._arm_or_run_smart_refresh(want_smart=want_smart)

    def _maybe_run_nasdaq_refresh(self) -> bool:
        return self._earn_coord._maybe_run_nasdaq_refresh()

    def _trim_all_blocked_candidates(self, candidates: list[str]) -> list[str]:
        return self._earn_coord._trim_all_blocked_candidates(candidates)

    def _kick_off_smart_refresh(self) -> None:
        self._earn_coord._kick_off_smart_refresh()

    def _launch_smart_refresh_workers(
        self, candidates: list[str], *, due: bool = True,
        include_finnhub: bool = True,
    ) -> None:
        self._earn_coord._launch_smart_refresh_workers(
            candidates, due=due, include_finnhub=include_finnhub,
        )

    def _run_earnings_smart_refresh_now(self) -> None:
        self._earn_coord._run_earnings_smart_refresh_now()

    # ── Zacks smart refresh ────────────────────────────────────────────

    def _kick_off_zacks_smart_refresh(self):
        """Phase 5: launch the daily Zacks targeted fill against tickers
        that look due for a new quarter. No-op when no universe is loaded
        or when an existing Zacks worker is already running.

        Bulk-fill threshold guard: pre-compute candidates synchronously
        and prompt the user before kicking off the worker if the
        candidate set looks like a first-time fill (>
        ZACKS_SMART_REFRESH_BULK_THRESHOLD, default 1000). Avoids the
        surprise of "daily refresh" silently turning into a 5-hour
        full-universe pull on a fresh install.
        """
        if self._zacks_worker and self._zacks_worker.isRunning():
            return
        if self._universe_df is None and not self._symbols:
            return
        # Use the broader universe (not just the symbols with OHLCV) so we
        # pick up gap-fill candidates that don't yet have a parquet —
        # Zacks doesn't depend on OHLCV cache.
        if self._universe_df is not None:
            universe_syms = [
                s for s in self._universe_df["symbol"].tolist()
                if isinstance(s, str) and s.strip()
            ]
        else:
            universe_syms = list(self._symbols)
        if not universe_syms:
            return

        # Pre-compute candidates so we can guard the threshold. The
        # compute is sub-second for any realistic universe.
        from ..earnings_history import find_smart_refresh_candidates
        candidates = find_smart_refresh_candidates(
            universe_syms, self._zacks_skip_set(),
        )
        threshold = config.ZACKS_SMART_REFRESH_BULK_THRESHOLD
        if len(candidates) > threshold:
            est_hours = len(candidates) * 1.5 / 3600.0  # 1.5s pacing default
            choice = QMessageBox.question(
                self,
                "Zacks Smart Refresh — Bulk-Sized Run Detected",
                (
                    f"The smart refresh would queue <b>{len(candidates):,}</b> "
                    f"tickers ({len(candidates) * 100 // len(universe_syms)}% "
                    f"of your universe).\n\n"
                    f"This usually means earnings_history.parquet is empty "
                    f"or just-installed — what's about to run is functionally "
                    f"a full bulk fill (~{est_hours:.1f} hours at the default "
                    f"1.5s pacing).\n\n"
                    f"<b>Yes</b> — run it now (same as Bulk Fill from the menu)\n"
                    f"<b>No</b> — skip this launch's smart refresh (toggle "
                    f"stays on; will ask again next launch)\n"
                    f"<b>Cancel</b> — skip AND turn off auto-refresh for this "
                    f"session (re-enable from Data menu)\n\n"
                    f"Continue?"
                ),
                QMessageBox.StandardButton.Yes
                | QMessageBox.StandardButton.No
                | QMessageBox.StandardButton.Cancel,
                QMessageBox.StandardButton.No,
            )
            if choice == QMessageBox.StandardButton.Cancel:
                self._zacks_auto_refresh_ref[0] = False
                if hasattr(self, "act_zacks_auto_refresh"):
                    self.act_zacks_auto_refresh.setChecked(False)
                self.log_panel.write_line(
                    "Zacks auto-refresh disabled for this session "
                    "(re-enable from Data menu)."
                )
                return
            if choice != QMessageBox.StandardButton.Yes:
                self.log_panel.write_line(
                    f"Zacks smart refresh skipped ({len(candidates):,} "
                    "candidates would have queued)."
                )
                return
            # User said Yes — proceed. Switch to mode="targeted" with
            # the explicit candidate list so we don't recompute (and so
            # the menu stop button reads correctly).
            self.log_panel.write_line(
                f"Zacks bulk-sized refresh: {len(candidates):,} ticker(s) "
                f"approved — kicking off."
            )
            self._zacks_worker = ZacksFillWorker(
                candidates, self._zacks_skip_set(), mode="targeted",
            )
        elif not candidates:
            # Nothing due — silent skip.
            return
        else:
            self.log_panel.write_line(
                f"Zacks smart refresh: {len(candidates)} candidate ticker(s) due."
            )
            self._zacks_worker = ZacksFillWorker(
                candidates, self._zacks_skip_set(), mode="targeted",
            )
        self._zacks_worker.log_msg.connect(self.log_panel.write_line)
        self._zacks_worker.progress.connect(self._on_zacks_progress)
        self._zacks_worker.finished.connect(self._on_zacks_done)
        self._zacks_worker.failure_breakdown.connect(self._on_zacks_failures)
        # Audit H1: the daily smart refresh must surface the cookie
        # refresh dialog if Imperva blocks mid-run, otherwise the worker
        # blocks indefinitely on its resume Event with no UX recovery.
        self._zacks_worker.imperva_block_detected.connect(
            self._on_imperva_block_detected
        )
        self._zacks_worker.start()

    @pyqtSlot(int, int)
    def _on_zacks_progress(self, done: int, total: int):
        self._earn_coord._on_zacks_progress(done, total)

    @pyqtSlot(int, int, list)
    def _on_zacks_done(self, filled: int, errors: int, candidates: list):
        self._earn_coord._on_zacks_done(filled, errors, candidates)

    @pyqtSlot(dict)
    def _on_zacks_failures(self, breakdown: dict):
        self._earn_coord._on_zacks_failures(breakdown)

    def _show_last_zacks_failures(self):
        """Menu action: per-kind ticker-failure breakdown for the most
        recent Zacks fill. Each section is a copy-friendly text block.
        Has a 'Send Not-Found to Blacklist' button so the user can
        prune ETFs / ADRs / delisted symbols from future fills with
        one click."""
        breakdown = dict(self._last_zacks_failures or {})
        if not breakdown:
            QMessageBox.information(
                self, "Zacks Failures",
                "No Zacks fill has run yet (or it had no failures). "
                "Try Data → Bulk Fill Earnings (Zacks) or wait for "
                "the daily smart refresh.",
            )
            return

        from ..zacks_scraper import (
            FAIL_BLOCKED, FAIL_NOT_FOUND, FAIL_HTTP_ERROR, FAIL_PARSE_ERROR,
        )
        # Display order + human-readable headings for each kind. Keys
        # not in this list still appear under "Unknown".
        kind_meta = [
            (FAIL_BLOCKED,
             "Imperva blocks — cookie refresh likely needed"),
            (FAIL_NOT_FOUND,
             "Not on Zacks — auto-added to Zacks skip list "
             "(edit via Data → Edit Zacks Skip List...)"),
            (FAIL_HTTP_ERROR,
             "HTTP / network errors — usually transient"),
            (FAIL_PARSE_ERROR,
             "Parse errors — Zacks page format may have changed"),
        ]
        listed_kinds = {k for k, _ in kind_meta}

        dlg = QDialog(self)
        dlg.setWindowTitle("Last Zacks Failures")
        dlg.setMinimumWidth(720)
        dlg.setMinimumHeight(540)
        layout = QVBoxLayout(dlg)
        total = sum(len(v) for v in breakdown.values())
        layout.addWidget(QLabel(
            f"<b>{total}</b> ticker(s) failed in the most recent Zacks "
            "fill, broken down by cause:"
        ))

        body = QTextEdit()
        body.setReadOnly(True)
        body.setFont(QFont("Consolas", 9))
        lines: list[str] = []
        for kind, header in kind_meta:
            tickers = breakdown.get(kind, [])
            if not tickers:
                continue
            lines.append(f"=== {header} ({len(tickers)}) ===")
            # 10 per row for readability
            for i in range(0, len(tickers), 10):
                lines.append("  " + ", ".join(sorted(tickers)[i:i + 10]))
            lines.append("")
        # Catch any kinds outside the canonical list (defensive)
        for kind, tickers in breakdown.items():
            if kind in listed_kinds or not tickers:
                continue
            lines.append(f"=== Unknown ({kind}, {len(tickers)}) ===")
            for i in range(0, len(tickers), 10):
                lines.append("  " + ", ".join(sorted(tickers)[i:i + 10]))
            lines.append("")
        body.setPlainText("\n".join(lines).rstrip())
        layout.addWidget(body)

        btns = QDialogButtonBox()
        btn_copy = btns.addButton(
            "Copy All to Clipboard",
            QDialogButtonBox.ButtonRole.ActionRole,
        )
        btn_copy.clicked.connect(
            lambda: QApplication.clipboard().setText(body.toPlainText())
        )

        # New: bulk-add every ticker in the breakdown to the Zacks skip
        # list. FAIL_NOT_FOUND is already auto-added by `_on_zacks_failures`,
        # but the user may want to skip the OTHER buckets too (Imperva
        # blocks, HTTP errors, parse errors) when those tickers are
        # consistently noisy. Skip-list format is one ticker per line,
        # sorted, normalized via `_normalize_ticker` — the helper
        # below handles dedup against the in-memory set so re-clicks
        # are no-ops.
        btn_skip = btns.addButton(
            "Send All Misses to Zacks Skip List",
            QDialogButtonBox.ButtonRole.ActionRole,
        )
        btn_skip.setToolTip(
            "Add every ticker in the failure breakdown to "
            "scanner_data/zacks_blacklist.txt, deduped against what's "
            "already on the list. Honored by every future Zacks fill "
            "(targeted / bulk / smart) — those tickers won't be "
            "re-tried until manually removed."
        )
        btn_skip.clicked.connect(
            lambda: self._send_zacks_misses_to_skip_list(breakdown, dlg)
        )

        close_btn = btns.addButton(QDialogButtonBox.StandardButton.Close)
        close_btn.clicked.connect(dlg.accept)
        layout.addWidget(btns)
        dlg.exec()

    def _send_zacks_misses_to_skip_list(self, breakdown: dict, parent_dlg) -> None:
        """Bulk-add every ticker in `breakdown` (across ALL failure
        kinds) to the Zacks skip list. Normalizes ticker text, dedupes
        against the in-memory set + against itself, persists via the
        atomic-write path, and reports the result to the user.

        Idempotent: re-clicking after a save is a no-op (everything is
        already on the list). Safe to call from the dialog's button
        slot — wraps every mutating step in try/except so a write
        failure surfaces as a dialog instead of silently dropping the
        change."""
        if not breakdown:
            QMessageBox.information(
                parent_dlg, "Nothing to Send",
                "No failure breakdown is available to send.",
            )
            return

        # Flatten all kinds → list of raw tickers
        all_misses: list[str] = []
        for tickers in breakdown.values():
            if tickers:
                all_misses.extend(tickers)

        # Normalize + dedupe within the batch first; then check against
        # the existing skip set. Counts let the user see what happened.
        seen_in_batch: set[str] = set()
        new_skips: list[str] = []
        already_on_list = 0
        for raw in all_misses:
            norm = self._normalize_ticker(raw)
            if not norm or norm in seen_in_batch:
                continue
            seen_in_batch.add(norm)
            if norm in self._zacks_blacklist:
                already_on_list += 1
            else:
                new_skips.append(norm)

        total_unique = len(seen_in_batch)

        if not new_skips:
            QMessageBox.information(
                parent_dlg, "Nothing New",
                f"All {total_unique} unique ticker(s) in this failure "
                f"batch are already on the Zacks skip list. Nothing added.",
            )
            return

        # Mutate + persist
        for t in new_skips:
            self._zacks_blacklist.add(t)
        try:
            self._save_zacks_blacklist()
        except Exception as exc:
            # Roll back the in-memory add so a later retry sees the
            # same state.
            for t in new_skips:
                self._zacks_blacklist.discard(t)
            log.error("Save zacks_blacklist failed: %s", exc, exc_info=True)
            QMessageBox.critical(
                parent_dlg, "Save Failed",
                f"Could not write zacks_blacklist.txt:\n{exc}\n\n"
                "No changes were saved; the in-memory skip list was "
                "rolled back so you can retry.",
            )
            return

        self.log_panel.write_line(
            f"Zacks skip list: added {len(new_skips)} new ticker(s) from "
            f"failure dialog ({already_on_list} already present, "
            f"{total_unique} total unique misses). "
            f"List size now {len(self._zacks_blacklist)}."
        )
        QMessageBox.information(
            parent_dlg, "Skip List Updated",
            f"Added <b>{len(new_skips)}</b> new ticker(s) to the Zacks "
            f"skip list.<br><br>"
            f"<b>{already_on_list}</b> were already on the list.<br>"
            f"<b>{total_unique}</b> total unique misses processed.<br><br>"
            f"List size: <b>{len(self._zacks_blacklist)}</b>",
        )

    # ── Force refresh actions ─────────────────────────────────────────────

    def _force_universe_refresh(self):
        """Menu action: force re-download the full ticker universe."""
        if self._universe_worker and self._universe_worker.isRunning():
            QMessageBox.information(self, "In Progress",
                                    "A universe refresh is already running.")
            return
        self.status.showMessage("Force refreshing ticker universe...")
        self.log_panel.write_line("Force universe refresh requested by user.")
        self._universe_worker = self._start_worker(
            UniverseWorker(),
            finished=self._on_universe_downloaded,
        )

    def _force_ohlcv_refresh(self):
        """Menu action: force re-run OHLCV staleness check and update."""
        if self._update_worker and self._update_worker.isRunning():
            QMessageBox.information(self, "In Progress",
                                    "An OHLCV update is already running.")
            return
        if not self._symbols:
            QMessageBox.warning(self, "No Universe",
                                "No ticker universe loaded yet. "
                                "Try a universe refresh first.")
            return
        self.status.showMessage("Force refreshing OHLCV data...")
        self.log_panel.write_line("Force OHLCV refresh requested by user.")
        self._load_universe_and_update(force=True)

    def _stop_ohlcv_refresh(self):
        """Menu action: stop the running OHLCV update."""
        if self._update_worker and self._update_worker.isRunning():
            self._update_worker.request_stop()
            self.log_panel.write_line("OHLCV refresh stop requested.")
            self._update_label.setText("OHLCV update: stopping...")
            self._update_label.setStyleSheet(
                "color: #ff9800; font-size: 11px; padding: 0 8px;"
            )
        else:
            self.log_panel.write_line("No OHLCV refresh is running.")

    def _set_finnhub_key_dialog(self):
        """Prompt for a Finnhub API key and store it in the OS credential
        manager via the keyring library. Reachable from Data → Set Finnhub
        API Key... and also auto-shown on first launch when no key is
        stored (see prompt_finnhub_key_if_missing)."""
        existing = finnhub_client.get_api_key()
        existing_blurb = (f" (currently set, ending …{existing[-4:]})"
                          if existing else " (not currently set)")
        text, ok = QInputDialog.getText(
            self,
            "Set Finnhub API Key",
            (
                "Enter your Finnhub API key" + existing_blurb + ".\n\n"
                "Get one free at https://finnhub.io/dashboard.\n"
                "Stored in the OS credential manager — no plaintext on disk.\n"
                "Leave blank to remove a stored key."
            ),
            QLineEdit.EchoMode.Password,
        )
        if not ok:
            return
        text = (text or "").strip()
        if not text:
            if existing and finnhub_client.clear_api_key():
                self.log_panel.write_line("Finnhub API key cleared.")
            return
        if finnhub_client.set_api_key(text):
            self.log_panel.write_line(
                f"Finnhub API key stored (ends …{text[-4:]}). "
                "Targeted fills will now try Finnhub first."
            )
        else:
            QMessageBox.warning(
                self, "Keyring Error",
                "Could not write the key to the OS credential manager.",
            )

    def _rebuild_tickers_dialog(self):
        """Phase 4 R9: prompt for comma-separated tickers, delete each one's
        parquet cache, and trigger a fresh download via the parallel
        download_many pipeline. Used when cached prices look wrong (most
        commonly from a stock split that pre-dated the cache)."""
        dlg = QDialog(self)
        dlg.setWindowTitle("Rebuild Tickers")
        dlg.setMinimumWidth(520)
        layout = QVBoxLayout(dlg)

        layout.addWidget(QLabel(
            "Enter tickers to rebuild (comma-separated). "
            "Their cached parquet will be deleted and re-downloaded."
        ))
        txt = QTextEdit()
        txt.setPlaceholderText("e.g. AAPL, TSLA, NVDA")
        txt.setMinimumHeight(100)
        layout.addWidget(txt)

        btn_row = QHBoxLayout()
        btn_ok = QPushButton("Rebuild")
        btn_ok.setStyleSheet(
            "QPushButton { background: #2e7d32; color: white; "
            "font-weight: bold; padding: 6px 18px; border-radius: 3px; }"
        )
        btn_cancel = QPushButton("Cancel")
        btn_row.addStretch()
        btn_row.addWidget(btn_ok)
        btn_row.addWidget(btn_cancel)
        layout.addLayout(btn_row)

        btn_ok.clicked.connect(dlg.accept)
        btn_cancel.clicked.connect(dlg.reject)

        if dlg.exec() != QDialog.DialogCode.Accepted:
            return

        raw = txt.toPlainText()
        syms = [t.strip().upper() for t in raw.replace("\n", ",").split(",")
                if t.strip()]
        if not syms:
            return

        self.log_panel.write_line(f"Rebuild: deleting + re-downloading {len(syms)} ticker(s)...")
        ok = 0
        errs = 0
        for s in syms:
            try:
                res = rebuild_ticker(s)
                if res.status == "ok":
                    ok += 1
                    self.log_panel.write_line(f"  {s}: rebuilt ({res.rows_received} rows)")
                else:
                    errs += 1
                    self.log_panel.write_line(f"  {s}: {res.status} — {res.error_msg}")
            except Exception as exc:
                errs += 1
                self.log_panel.write_line(f"  {s}: ERROR {exc}")
        self.status.showMessage(f"Rebuild complete: {ok} ok, {errs} errors")
        # Any IPO row-count entries for rebuilt tickers are now stale
        for s in syms:
            self._ipo_row_count_cache.pop(s, None)

    def _reset_yfinance_session(self):
        """Best-effort clear of yfinance's internal caches.

        Phase 4 R14: all cache clears use hasattr probes so yfinance version
        bumps don't crash the feature; we also count cleared attrs and warn
        if nothing matched (signal that yfinance's internal API changed).
        Tested against yfinance 0.2.x. If this feature becomes a no-op after
        a yfinance upgrade, this is the first thing to revisit.
        """
        import yfinance as yf
        cleared = 0
        try:
            # Module-level cookie/crumb caches
            for attr in ('_cache', '_CACHE'):
                for mod in (yf,
                            getattr(yf, 'data', None),
                            getattr(yf, 'utils', None)):
                    if mod is not None and hasattr(mod, attr):
                        try:
                            getattr(mod, attr).clear()
                            cleared += 1
                        except Exception as exc:
                            log.debug(
                                "yfinance %s.%s cache clear failed: %s",
                                getattr(mod, "__name__", mod), attr, exc,
                            )
            # Shared HTTP session singleton
            if hasattr(yf, 'shared') and hasattr(yf.shared, '_REQUESTS_SESSION'):
                yf.shared._REQUESTS_SESSION = None
                cleared += 1
            # Ticker-class caches
            if hasattr(yf, 'Ticker'):
                for attr in ('_cache', '_tz_cache'):
                    if hasattr(yf.Ticker, attr):
                        try:
                            getattr(yf.Ticker, attr).clear()
                            cleared += 1
                        except Exception as exc:
                            log.debug(
                                "yfinance Ticker.%s cache clear failed: %s",
                                attr, exc,
                            )

            if cleared == 0:
                msg = (f"yfinance session reset: no internal caches matched "
                       f"(yfinance={getattr(yf, '__version__', 'unknown')}). "
                       f"The reset code may need updating for this version.")
                self.log_panel.write_line(msg)
                log.warning(msg)
                self.status.showMessage("yfinance session reset — no-op")
            else:
                self.log_panel.write_line(
                    f"yfinance session reset: cleared {cleared} cache(s)."
                )
                self.status.showMessage("yfinance session reset")
        except Exception as exc:
            self._log_error(
                "yfinance-session", f"Session reset error: {exc}", exc,
            )

    def _refresh_missing_only(self):
        """Menu action: download OHLCV only for tickers with no cached data."""
        if self._update_worker and self._update_worker.isRunning():
            QMessageBox.information(self, "In Progress",
                                    "An OHLCV update is already running.")
            return
        try:
            universe = load_universe()
            all_syms = [
                s for s in universe["symbol"].tolist()
                if isinstance(s, str) and s.strip()
            ]
        except FileNotFoundError:
            QMessageBox.warning(self, "No Universe",
                                "No universe file found.")
            return

        # Only tickers with NO parquet file at all
        missing = [
            s for s in all_syms
            if not (config.PARQUET_DIR / f"{s}.parquet").exists()
            and s not in self._blacklist
        ]
        if not missing:
            self.log_panel.write_line("All tickers already have cached data.")
            self.status.showMessage("No missing tickers to download.")
            return

        self.log_panel.write_line(
            f"Downloading {len(missing)} tickers with no cached data..."
        )
        self._update_label.setText(f"Missing tickers: 0/{len(missing)}")
        self._update_label.setStyleSheet(
            "color: #4a90d9; font-size: 11px; padding: 0 8px;"
        )
        self._update_worker = self._start_worker(
            UpdateWorker(
                missing,
                backoff_enabled_ref=self._backoff_enabled_ref,
                backoff_threshold=self._backoff_threshold,
                backoff_wait=self._backoff_wait,
                max_retries=self._max_retries,
            ),
            progress=self._on_update_progress,
            error_tickers=self._on_ohlcv_error_tickers,
            finished=self._on_update_done,
        )

    def _show_backoff_settings(self):
        """Open a dialog to configure rate-limit backoff parameters."""
        dlg = QDialog(self)
        dlg.setWindowTitle("Rate-Limit Backoff Settings")
        dlg.setModal(True)
        dlg.setMinimumWidth(360)
        layout = QVBoxLayout(dlg)

        # Fail threshold
        row1 = QHBoxLayout()
        lbl1 = QLabel("Fail threshold:")
        lbl1.setToolTip(
            "Number of consecutive download failures before "
            "the updater pauses and backs off."
        )
        row1.addWidget(lbl1)
        spin_thresh = QSpinBox()
        spin_thresh.setRange(3, 100)
        spin_thresh.setValue(self._backoff_threshold)
        spin_thresh.setMinimumWidth(80)
        row1.addWidget(spin_thresh)
        layout.addLayout(row1)

        # Wait time
        row2 = QHBoxLayout()
        lbl2 = QLabel("Base wait time (sec):")
        lbl2.setToolTip(
            "Base wait time in seconds between retries. "
            "Multiplied by retry number (e.g. 30x1, 30x2, 30x3)."
        )
        row2.addWidget(lbl2)
        spin_wait = QSpinBox()
        spin_wait.setRange(5, 300)
        spin_wait.setValue(self._backoff_wait)
        spin_wait.setMinimumWidth(80)
        row2.addWidget(spin_wait)
        layout.addLayout(row2)

        # Max retries
        row3 = QHBoxLayout()
        lbl3 = QLabel("Max retries:")
        lbl3.setToolTip(
            "How many times to retry after each backoff before "
            "giving up and stopping the update."
        )
        row3.addWidget(lbl3)
        spin_retries = QSpinBox()
        spin_retries.setRange(1, 20)
        spin_retries.setValue(self._max_retries)
        spin_retries.setMinimumWidth(80)
        row3.addWidget(spin_retries)
        layout.addLayout(row3)

        # OK / Cancel
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_ok = QPushButton("OK")
        btn_ok.clicked.connect(dlg.accept)
        btn_row.addWidget(btn_ok)
        btn_cancel = QPushButton("Cancel")
        btn_cancel.clicked.connect(dlg.reject)
        btn_row.addWidget(btn_cancel)
        layout.addLayout(btn_row)

        if dlg.exec() == QDialog.DialogCode.Accepted:
            self._backoff_threshold = spin_thresh.value()
            self._backoff_wait = spin_wait.value()
            self._max_retries = spin_retries.value()
            self.log_panel.write_line(
                f"Backoff settings updated: threshold={self._backoff_threshold}, "
                f"wait={self._backoff_wait}s, retries={self._max_retries}"
            )

    # ── Ticker blacklist ────────────────────────────────────────────────
    #
    # Persistence lives in blacklists.BlacklistManager (Step A2 split).
    # The load/save methods below are thin delegates that keep their
    # original names — GUI menu wiring and tests reference them. Managers
    # are built per call (not cached in __init__) so the `_*_BLACKLIST_FILE`
    # path attrs are read at call time: tests shadow them on bare `__new__`
    # instances, exactly like the pre-extraction direct reads.

    _BLACKLIST_FILE = config.DATA_DIR / "blacklist.txt"
    # Zacks-only skip list: tickers known not to be on Zacks. Honored
    # by ZacksFillWorker (in addition to the universal blacklist) so
    # bulk / smart-refresh fills don't waste cycles on definitively-
    # uncovered symbols. Auto-populated by FAIL_NOT_FOUND results
    # during fills and by the bulk-fill ETF/ADR pre-skip pass.
    # Critically does NOT affect OHLCV / sector / scan / Yahoo paths —
    # those still see the ticker normally.
    _ZACKS_BLACKLIST_FILE = config.DATA_DIR / "zacks_blacklist.txt"

    @staticmethod
    def _normalize_ticker(t: str) -> str:
        """Normalize Unicode minus/dash variants to ASCII hyphen.
        Canonical implementation lives in blacklists.normalize_ticker
        (shared with BlacklistManager); kept as a staticmethod here
        because dozens of call sites + tests reference it."""
        return normalize_ticker(t)

    def _load_blacklist(self):
        """Load blacklisted tickers from file. Degrades to an empty set on a
        locked/corrupt/unreadable file (mirrors the Zacks/Finnhub/Finviz skip-
        list loaders) — this runs in __init__ before the window is shown, so
        an unguarded read error would abort launch with no GUI to report it."""
        self._blacklist = BlacklistManager(
            self._BLACKLIST_FILE, fmt="csv", label="blacklist",
        ).load()

    def _send_ohlcv_errors_to_blacklist(self):
        """Append OHLCV error tickers to the blacklist."""
        if not self._ohlcv_error_tickers:
            self.log_panel.write_line("No OHLCV error tickers to blacklist.")
            return
        added = []
        for t in self._ohlcv_error_tickers:
            norm = self._normalize_ticker(t)
            if norm and norm not in self._blacklist:
                self._blacklist.add(norm)
                added.append(norm)
        self._save_blacklist()
        self._ohlcv_error_tickers.clear()
        self.log_panel.btn_blacklist_errors.setVisible(False)
        self.log_panel.write_line(
            f"Blacklisted {len(added)} new tickers from OHLCV errors. "
            f"Total blacklist: {len(self._blacklist)}"
        )

    def _save_blacklist(self):
        """Persist blacklist to file (atomic write via temp + rename)."""
        BlacklistManager(
            self._BLACKLIST_FILE, fmt="csv", label="blacklist",
        ).save(self._blacklist)

    # ── Universe-derived auto-skip (ETFs + ADRs) ───────────────────────
    #
    # ETFs have no operating EPS / revenue. Zacks treats them as "not on
    # Zacks" misses and Finnhub returns empty on /stock/earnings for
    # them, so both sources classify them as coverage misses — but only
    # after burning one wasted HTTP request each.
    #
    # ADRs (American Depositary Receipts) wrap a foreign issuer whose
    # actual financials are reported abroad. Zacks' coverage of ADRs is
    # spotty; Finnhub treats most as empty.
    #
    # The universe.csv from NASDAQ FTP carries clean `etf` and `adr`
    # boolean columns. Pre-skipping these symbols from every bulk / gap
    # earnings fill cuts ~37% of the universe (5,800+ tickers on a
    # ~15.5k universe) — typically ~25 min of wasted requests per source.
    #
    # NOT persisted to any per-source skip-list file — recomputed
    # dynamically each fill so a universe refresh that adds/removes
    # ETF or ADR flags is picked up automatically. Spot fill bypasses
    # this skip (user explicitly typed the symbol).

    def _etf_adr_auto_skip_set(self) -> set[str]:
        """Return tickers flagged as ETF or ADR in the current universe.
        Empty set when no universe is loaded yet.

        Defensive on attribute access so callers (including tests that
        instantiate MainWindow via ``__new__`` to bypass init) don't
        trip on PyQt6's "super().__init__ was never called" guard,
        which raises RuntimeError on any attribute access against an
        uninitialized instance — including getattr."""
        try:
            df = self._universe_df
        except (AttributeError, RuntimeError):
            return set()
        if df is None or df.empty:
            return set()
        mask = pd.Series(False, index=df.index)
        if "etf" in df.columns:
            mask |= df["etf"].fillna(False).astype(bool)
        if "adr" in df.columns:
            mask |= df["adr"].fillna(False).astype(bool)
        if not mask.any():
            return set()
        syms = df.loc[mask, "symbol"]
        return {
            s.upper().strip() for s in syms
            if isinstance(s, str) and s.strip()
        }

    def _log_etf_adr_preskip(self, source_label: str, syms: list[str]) -> None:
        """Log the count of universe tickers that will be auto-skipped
        by the ETF/ADR pre-filter on this fill kickoff. Called from
        each bulk / gap fill entry point so the user sees the trim."""
        auto_skip = self._etf_adr_auto_skip_set()
        if not auto_skip:
            return
        n = sum(1 for s in syms if s in auto_skip)
        if n:
            self.log_panel.write_line(
                f"{source_label}: pre-skipping {n} ETF/ADR ticker(s) "
                f"flagged in universe.csv (these sources have no "
                f"useful data for funds or foreign-issuer ADRs)."
            )

    # ── Zacks-only skip list ───────────────────────────────────────────

    def _load_zacks_blacklist(self):
        """Load Zacks-only skip-list tickers from disk. Empty set if
        file missing — auto-populated as the user runs fills."""
        self._zacks_blacklist = BlacklistManager(
            self._ZACKS_BLACKLIST_FILE, label="Zacks skip list",
        ).load()

    def _save_zacks_blacklist(self):
        """Atomic write of zacks_blacklist.txt — one ticker per line for
        easy diffing / inspection.

        Defensively strips newlines/carriage-returns/whitespace from
        each ticker before joining so that a crafted upstream symbol
        (or a clipboard-paste mishap in the manual editor dialog)
        can't inject phantom entries on the next line.
        """
        BlacklistManager(
            self._ZACKS_BLACKLIST_FILE, label="Zacks skip list",
        ).save(self._zacks_blacklist)

    # ── Finnhub-only skip list (Phase 2 mirror of the Zacks pattern) ───
    #
    # Tickers Finnhub doesn't cover (`/stock/earnings` returned `[]`)
    # plus user-curated entries. Auto-populated by FAIL_EMPTY results
    # during fills. Like the Zacks list, this DOES NOT affect OHLCV /
    # sector / scan paths.

    _FINNHUB_BLACKLIST_FILE = config.FINNHUB_BLACKLIST_FILE

    def _load_finnhub_blacklist(self):
        """Load Finnhub-only skip-list tickers from disk."""
        self._finnhub_blacklist = BlacklistManager(
            self._FINNHUB_BLACKLIST_FILE, label="Finnhub skip list",
        ).load()

    def _save_finnhub_blacklist(self):
        """Atomic write of finnhub_blacklist.txt — one ticker per line.

        Defensive newline/whitespace strip per the same rationale as
        _save_zacks_blacklist.
        """
        BlacklistManager(
            self._FINNHUB_BLACKLIST_FILE, label="Finnhub skip list",
        ).save(self._finnhub_blacklist)

    def _combined_finnhub_skip_set(self) -> set[str]:
        """Combined skip set for Finnhub bulk / gap fills:

        - Universe blacklist (`self._blacklist`) — persisted into the
          Finnhub skip list so it shows up in the editor.
        - Finnhub-specific skip list (`self._finnhub_blacklist`).
        - ETF + ADR auto-skip from universe.csv flags — NOT persisted
          (re-derived per fill so universe refreshes flow through; spot
          fill bypasses).

        Computed at the start of each Finnhub fill so any tickers added
        to the universe blacklist since the last run are picked up
        automatically."""
        before = len(self._finnhub_blacklist)
        self._finnhub_blacklist |= self._blacklist
        if len(self._finnhub_blacklist) > before:
            self._save_finnhub_blacklist()
        return set(self._finnhub_blacklist) | self._etf_adr_auto_skip_set()

    def _on_finnhub_etf_identified(self, sym: str) -> None:
        """Callback wired into FinnhubFillWorker — adds tickers whose
        /stock/earnings response was [] to the Finnhub skip list."""
        norm = self._normalize_ticker(sym)
        if norm and norm not in self._finnhub_blacklist:
            self._finnhub_blacklist.add(norm)
            self._auto_added_finnhub_skips += 1

    def _zacks_skip_set(self) -> set[str]:
        """Combined skip set Zacks bulk / targeted fills should honor:

        - Universe blacklist (`self._blacklist`).
        - Zacks-only skip list (`self._zacks_blacklist`).
        - ETF + ADR auto-skip from universe.csv flags — pure read,
          NOT persisted.

        Centralized so all three ZacksFillWorker spawn sites stay
        consistent."""
        return (self._blacklist | self._zacks_blacklist
                | self._etf_adr_auto_skip_set())

    def _show_blacklist_editor(self):
        """Open a dialog to edit the ticker blacklist."""
        dlg = QDialog(self)
        dlg.setWindowTitle("Ticker Blacklist")
        dlg.setModal(True)
        dlg.setMinimumWidth(500)
        layout = QVBoxLayout(dlg)

        lbl = QLabel(
            "Enter tickers to skip during OHLCV refresh (comma-separated).\n"
            "These tickers will never be downloaded or updated."
        )
        lbl.setWordWrap(True)
        layout.addWidget(lbl)

        txt = QTextEdit()
        txt.setPlaceholderText("e.g. AAPL, MSFT, BADTICKER")
        txt.setPlainText(", ".join(sorted(self._blacklist)))
        txt.setMinimumHeight(200)
        layout.addWidget(txt)

        count_lbl = QLabel(f"Currently blacklisted: {len(self._blacklist)}")
        layout.addWidget(count_lbl)

        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_ok = QPushButton("OK")
        btn_ok.clicked.connect(dlg.accept)
        btn_row.addWidget(btn_ok)
        btn_cancel = QPushButton("Cancel")
        btn_cancel.clicked.connect(dlg.reject)
        btn_row.addWidget(btn_cancel)
        layout.addLayout(btn_row)

        if dlg.exec() == QDialog.DialogCode.Accepted:
            raw = txt.toPlainText().strip()
            self._blacklist = {
                self._normalize_ticker(t)
                for t in raw.split(",") if t.strip()
            }
            self._save_blacklist()
            self.log_panel.write_line(
                f"Blacklist updated: {len(self._blacklist)} tickers"
            )

    def _show_zacks_skip_list_editor(self):
        """Open a dialog to view/edit the Zacks-only skip list. One
        ticker per line for diffability — the file format itself uses
        line-per-ticker (vs the universal blacklist's CSV)."""
        dlg = QDialog(self)
        dlg.setWindowTitle("Zacks Skip List")
        dlg.setModal(True)
        dlg.setMinimumWidth(560)
        dlg.setMinimumHeight(480)
        layout = QVBoxLayout(dlg)

        lbl = QLabel(
            "Tickers in this list are <b>skipped only by Zacks earnings "
            "fills</b> (in addition to the universal blacklist). OHLCV "
            "downloads, sector fills, scans, and Finn+YH earnings all "
            "still see these tickers normally.\n\n"
            "Auto-populated by:\n"
            "  • Failed Zacks lookups classified as 'not on Zacks' "
            "(every fill)\n"
            "  • Bulk Fill Earnings (Zacks): pre-skip pass over "
            "universe.csv ETF/ADR flags\n\n"
            "One ticker per line."
        )
        lbl.setWordWrap(True)
        layout.addWidget(lbl)

        txt = QTextEdit()
        txt.setFont(QFont("Consolas", 9))
        txt.setPlaceholderText("e.g.\nAAPL\nQQQ\nSPY")
        txt.setPlainText("\n".join(sorted(self._zacks_blacklist)))
        layout.addWidget(txt)

        count_lbl = QLabel(
            f"Currently on Zacks skip list: {len(self._zacks_blacklist)}"
        )
        layout.addWidget(count_lbl)

        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_ok = QPushButton("OK")
        btn_ok.clicked.connect(dlg.accept)
        btn_row.addWidget(btn_ok)
        btn_cancel = QPushButton("Cancel")
        btn_cancel.clicked.connect(dlg.reject)
        btn_row.addWidget(btn_cancel)
        layout.addLayout(btn_row)

        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        raw = txt.toPlainText()
        new_set = {
            self._normalize_ticker(t)
            for line in raw.splitlines()
            for t in line.split(",")
            if t.strip()
        }
        new_set.discard("")
        if new_set != self._zacks_blacklist:
            self._zacks_blacklist = new_set
            try:
                self._save_zacks_blacklist()
                self.log_panel.write_line(
                    f"Zacks skip list updated: {len(new_set)} tickers."
                )
            except Exception as exc:
                QMessageBox.warning(
                    self, "Save Failed",
                    f"Could not save Zacks skip list: {exc}",
                )

    def _show_finnhub_skip_list_editor(self):
        """Open a dialog to view/edit the Finnhub-only skip list. Mirrors
        the Zacks skip-list editor — line-per-ticker for diffability."""
        dlg = QDialog(self)
        dlg.setWindowTitle("Finnhub Skip List")
        dlg.setModal(True)
        dlg.setMinimumWidth(560)
        dlg.setMinimumHeight(480)
        layout = QVBoxLayout(dlg)

        lbl = QLabel(
            "Tickers in this list are <b>skipped only by Finnhub earnings "
            "fills</b> (in addition to the universal blacklist). OHLCV "
            "downloads, sector fills, scans, and Zacks earnings all still "
            "see these tickers normally.\n\n"
            "Auto-populated by tickers whose <code>/stock/earnings</code> "
            "response is <code>[]</code> — Finnhub doesn't cover them "
            "(ETFs, funds, recently-IPO'd, delisted).\n\n"
            "One ticker per line."
        )
        lbl.setWordWrap(True)
        layout.addWidget(lbl)

        txt = QTextEdit()
        txt.setFont(QFont("Consolas", 9))
        txt.setPlaceholderText("e.g.\nQQQ\nSPY\nVTI")
        txt.setPlainText("\n".join(sorted(self._finnhub_blacklist)))
        layout.addWidget(txt)

        count_lbl = QLabel(
            f"Currently on Finnhub skip list: {len(self._finnhub_blacklist)}"
        )
        layout.addWidget(count_lbl)

        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_ok = QPushButton("OK")
        btn_ok.clicked.connect(dlg.accept)
        btn_row.addWidget(btn_ok)
        btn_cancel = QPushButton("Cancel")
        btn_cancel.clicked.connect(dlg.reject)
        btn_row.addWidget(btn_cancel)
        layout.addLayout(btn_row)

        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        raw = txt.toPlainText()
        new_set = {
            self._normalize_ticker(t)
            for line in raw.splitlines()
            for t in line.split(",")
            if t.strip()
        }
        new_set.discard("")
        if new_set != self._finnhub_blacklist:
            self._finnhub_blacklist = new_set
            try:
                self._save_finnhub_blacklist()
                self.log_panel.write_line(
                    f"Finnhub skip list updated: {len(new_set)} tickers."
                )
            except Exception as exc:
                QMessageBox.warning(
                    self, "Save Failed",
                    f"Could not save Finnhub skip list: {exc}",
                )

    # ── Finnhub menu handlers (Phase 2) ────────────────────────────────

    def _check_finnhub_key_or_warn(self) -> bool:
        """Pre-flight: confirm an API key is configured before kicking
        off any Finnhub fill. Returns True if OK to proceed."""
        from .. import finnhub_client
        if not finnhub_client.is_configured():
            QMessageBox.warning(
                self, "Finnhub API Key Required",
                "Finnhub fills need a valid API key.\n\n"
                "Use the first-launch prompt or "
                "settings → Set Finnhub API Key... to configure one.",
            )
            return False
        return True

    def _bulk_fill_finnhub(self):
        """Menu: full universe Finnhub deep-history pull. ~9.5 hrs."""
        if self._finnhub_worker and self._finnhub_worker.isRunning():
            self.log_panel.write_line("Finnhub fill already running.")
            return
        if not self._check_finnhub_key_or_warn():
            return

        # Universe blacklist gets unioned into the Finnhub skip list at
        # run start so any tickers the user added to the main blacklist
        # since the last run are honored. Idempotent.
        skip = self._combined_finnhub_skip_set()

        if QMessageBox.question(
            self, "Bulk Fill Earnings (Finnhub)",
            "This pulls 5 years of EPS + revenue + announcement-date "
            "history for every ticker in your universe that isn't on the "
            "Finnhub skip list.\n\n"
            "Two API calls per ticker @ ~52/min — estimated ~9 hours for "
            "a 15k-ticker universe.\n\n"
            "Progress is checkpointed every 25 tickers — a kill mid-run "
            "resumes from the last checkpoint on next launch.\n\n"
            "Continue?",
        ) != QMessageBox.StandardButton.Yes:
            return

        syms = self._get_universe_symbols()
        self._log_etf_adr_preskip("Finnhub bulk fill", syms)
        self._auto_added_finnhub_skips = 0
        self._start_finnhub_worker(syms, skip, mode="bulk",
                                   label="Finnhub bulk fill")

    def _gap_fill_finnhub(self):
        """Menu: Finnhub fill for tickers without any Finnhub-source
        rows in earnings_history.parquet."""
        if self._finnhub_worker and self._finnhub_worker.isRunning():
            self.log_panel.write_line("Finnhub fill already running.")
            return
        if not self._check_finnhub_key_or_warn():
            return

        from ..finnhub_fill import find_finnhub_gap_tickers
        skip = self._combined_finnhub_skip_set()
        syms = self._get_universe_symbols()
        self._log_etf_adr_preskip("Finnhub gap fill", syms)
        gaps = find_finnhub_gap_tickers(syms, skip)
        if not gaps:
            QMessageBox.information(
                self, "No Gaps",
                "Every ticker in the universe already has Finnhub "
                "history. Nothing to do.",
            )
            return
        self.log_panel.write_line(
            f"Finnhub gap fill: {len(gaps)} gap ticker(s) identified."
        )
        self._auto_added_finnhub_skips = 0
        self._start_finnhub_worker(gaps, skip, mode="gap",
                                   label="Finnhub gap fill")

    def _spot_fill_finnhub(self):
        """Menu: single-ticker Finnhub fetch via input dialog. Bypasses
        the ETF/ADR auto-skip (the user explicitly typed the symbol
        so we run it even if universe.csv flags it as a fund) — only
        the user-curated skip lists block a spot fill."""
        if self._finnhub_worker and self._finnhub_worker.isRunning():
            self.log_panel.write_line("Finnhub fill already running.")
            return
        if not self._check_finnhub_key_or_warn():
            return

        sym, ok = QInputDialog.getText(
            self, "Spot Fill Earnings (Finnhub)",
            "Ticker:",
        )
        if not ok or not sym.strip():
            return
        norm = self._normalize_ticker(sym)
        # User-only skip (universe blacklist + Finnhub-specific skip
        # list). Excludes the ETF/ADR auto-skip — see method docstring.
        user_only_skip = self._blacklist | self._finnhub_blacklist
        if norm in user_only_skip:
            QMessageBox.information(
                self, "Skipped",
                f"{norm} is on the Finnhub skip list — remove it first "
                "via Edit Finnhub Skip List...",
            )
            return
        # The fill itself still receives the user-only skip so it
        # doesn't get filtered out at the start of _fill_via_finnhub.
        skip = user_only_skip

        # Spot fills are short — run inline without a worker thread.
        # Wrap in a try/except so the GUI doesn't crash on unexpected
        # exceptions inside the fill.
        from .. import finnhub_fill
        self.log_panel.write_line(f"Finnhub spot fill: {norm}...")
        try:
            count, status = finnhub_fill.spot_fill_finnhub(
                norm, skip, on_etf_identified=self._on_finnhub_etf_identified,
            )
        except Exception as exc:
            self._log_error(
                "finnhub-spot-fill", f"Finnhub spot fill failed: {exc}", exc,
            )
            return

        if status == "ok":
            self.log_panel.write_line(
                f"Finnhub spot fill done: {norm} → {count} quarter(s) written."
            )
        elif status == "empty":
            if self._auto_added_finnhub_skips:
                self._save_finnhub_blacklist()
            self.log_panel.write_line(
                f"Finnhub returned empty for {norm} (likely an ETF / "
                "fund / uncovered) — added to Finnhub skip list."
            )
        else:
            self.log_panel.write_line(
                f"Finnhub spot fill: {norm} → status='{status}'."
            )

    def _stop_finnhub_fill(self):
        if self._finnhub_worker and self._finnhub_worker.isRunning():
            self._finnhub_worker.request_stop()
            self.log_panel.write_line("Finnhub fill stop requested.")
        else:
            self.log_panel.write_line("No Finnhub fill running.")

    def _start_finnhub_worker(self, symbols: list[str], skip: set[str],
                              *, mode: str, label: str):
        self._earn_coord._start_finnhub_worker(
            symbols, skip, mode=mode, label=label,
        )

    @pyqtSlot(int, int)
    def _on_finnhub_done(self, filled: int, errors: int):
        self._earn_coord._on_finnhub_done(filled, errors)

    # (EDGAR-only skip list + EDGAR fill handlers removed 2026-05-31
    #  — earnings EDGAR/GAAP source dropped.)

    # ── Finviz-only skip list + fill handlers (top-priority source) ──────

    _FINVIZ_BLACKLIST_FILE = config.FINVIZ_BLACKLIST_FILE

    def _load_finviz_blacklist(self):
        """Load finviz-only skip-list tickers from disk."""
        self._finviz_blacklist = BlacklistManager(
            self._FINVIZ_BLACKLIST_FILE, label="finviz skip list",
        ).load()

    def _save_finviz_blacklist(self):
        """Atomic write of finviz_blacklist.txt — one ticker per line."""
        BlacklistManager(
            self._FINVIZ_BLACKLIST_FILE, label="finviz skip list",
        ).save(self._finviz_blacklist)

    def _combined_finviz_skip_set(self) -> set[str]:
        """Combined skip set for finviz bulk / gap fills:

        - Universe blacklist (`self._blacklist`) — persisted into the
          finviz skip list so it shows up in the editor.
        - Finviz-specific skip list (`self._finviz_blacklist`).
        - ETF + ADR auto-skip from universe.csv flags — NOT persisted
          (re-derived per fill; spot fill bypasses).

        This is what keeps a finviz bulk from wasting requests on funds /
        ADRs and from re-fetching tickers the user OHLCV-blacklisted."""
        before = len(self._finviz_blacklist)
        self._finviz_blacklist |= self._blacklist
        if len(self._finviz_blacklist) > before:
            self._save_finviz_blacklist()
        return set(self._finviz_blacklist) | self._etf_adr_auto_skip_set()

    def _on_finviz_empty_identified(self, sym: str) -> None:
        """Callback wired into FinvizFillWorker — adds tickers finviz
        doesn't cover (no earningsData) to the finviz skip list."""
        norm = self._normalize_ticker(sym)
        if norm and norm not in self._finviz_blacklist:
            self._finviz_blacklist.add(norm)
            self._auto_added_finviz_skips += 1

    def _show_finviz_skip_list_editor(self):
        """View/edit the finviz-only skip list (line-per-ticker)."""
        dlg = QDialog(self)
        dlg.setWindowTitle("Finviz Skip List")
        dlg.setModal(True)
        dlg.setMinimumWidth(560)
        dlg.setMinimumHeight(480)
        layout = QVBoxLayout(dlg)

        lbl = QLabel(
            "Tickers in this list are <b>skipped only by finviz earnings "
            "fills</b> (in addition to the universal blacklist). OHLCV "
            "downloads, sector fills, scans, and other earnings sources "
            "all still see these tickers normally.\n\n"
            "Auto-populated by tickers finviz doesn't cover (no "
            "<code>earningsData</code> — ETFs, funds, brand-new listings)."
            "\n\nOne ticker per line."
        )
        lbl.setWordWrap(True)
        layout.addWidget(lbl)

        txt = QTextEdit()
        txt.setFont(QFont("Consolas", 9))
        txt.setPlaceholderText("e.g.\nQQQ\nSPY\nVTI")
        txt.setPlainText("\n".join(sorted(self._finviz_blacklist)))
        layout.addWidget(txt)

        count_lbl = QLabel(
            f"Currently on finviz skip list: {len(self._finviz_blacklist)}"
        )
        layout.addWidget(count_lbl)

        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_ok = QPushButton("OK")
        btn_ok.clicked.connect(dlg.accept)
        btn_row.addWidget(btn_ok)
        btn_cancel = QPushButton("Cancel")
        btn_cancel.clicked.connect(dlg.reject)
        btn_row.addWidget(btn_cancel)
        layout.addLayout(btn_row)

        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        raw = txt.toPlainText()
        new_set = {
            self._normalize_ticker(t)
            for line in raw.splitlines()
            for t in line.split(",")
            if t.strip()
        }
        new_set.discard("")
        if new_set != self._finviz_blacklist:
            self._finviz_blacklist = new_set
            try:
                self._save_finviz_blacklist()
                self.log_panel.write_line(
                    f"Finviz skip list updated: {len(new_set)} tickers."
                )
            except Exception as exc:
                QMessageBox.warning(
                    self, "Save Failed",
                    f"Could not save finviz skip list: {exc}",
                )

    def _bulk_fill_finviz(self):
        """Menu: full-universe finviz pull (overnight)."""
        if self._finviz_worker and self._finviz_worker.isRunning():
            self.log_panel.write_line("Finviz fill already running.")
            return
        skip = self._combined_finviz_skip_set()
        if QMessageBox.question(
            self, "Bulk Fill Earnings (Finviz)",
            "This scrapes finviz earnings for every ticker in your "
            "universe that isn't an ETF/ADR or on a skip list.\n\n"
            "One request per ticker at a deliberately SLOW pace to stay "
            "under finviz's rate limit — estimated ~11 hours for a ~10k "
            "universe. Intended to run overnight.\n\n"
            "Progress is checkpointed every 25 tickers — a kill mid-run "
            "resumes from the last checkpoint on next launch.\n\n"
            "Continue?",
        ) != QMessageBox.StandardButton.Yes:
            return
        syms = self._get_universe_symbols()
        self._log_etf_adr_preskip("Finviz bulk fill", syms)
        self._auto_added_finviz_skips = 0
        self._start_finviz_worker(syms, skip, mode="bulk",
                                  label="Finviz bulk fill")

    def _gap_fill_finviz(self):
        """Menu: finviz fill for tickers without any finviz-source rows."""
        if self._finviz_worker and self._finviz_worker.isRunning():
            self.log_panel.write_line("Finviz fill already running.")
            return
        from ..finviz_fill import find_finviz_gap_tickers
        skip = self._combined_finviz_skip_set()
        syms = self._get_universe_symbols()
        self._log_etf_adr_preskip("Finviz gap fill", syms)
        gaps = find_finviz_gap_tickers(syms, skip)
        if not gaps:
            QMessageBox.information(
                self, "No Gaps",
                "Every ticker in the universe already has finviz "
                "history. Nothing to do.",
            )
            return
        self.log_panel.write_line(
            f"Finviz gap fill: {len(gaps)} gap ticker(s) identified."
        )
        self._auto_added_finviz_skips = 0
        self._start_finviz_worker(gaps, skip, mode="gap",
                                  label="Finviz gap fill")

    def _spot_fill_finviz(self):
        """Menu: single-ticker finviz fetch. Bypasses ETF/ADR auto-skip
        (user typed the symbol) — only user-curated skip lists block it."""
        if self._finviz_worker and self._finviz_worker.isRunning():
            self.log_panel.write_line("Finviz fill already running.")
            return
        sym, ok = QInputDialog.getText(
            self, "Spot Fill Earnings (Finviz)", "Ticker:",
        )
        if not ok or not sym.strip():
            return
        norm = self._normalize_ticker(sym)
        user_only_skip = self._blacklist | self._finviz_blacklist
        if norm in user_only_skip:
            QMessageBox.information(
                self, "Skipped",
                f"{norm} is on the finviz skip list — remove it first "
                "via Edit Finviz Skip List...",
            )
            return
        from .. import finviz_fill
        self.log_panel.write_line(f"Finviz spot fill: {norm}...")
        try:
            count, status = finviz_fill.spot_fill_finviz(
                norm, user_only_skip,
                on_empty_identified=self._on_finviz_empty_identified,
            )
        except Exception as exc:
            self._log_error(
                "finviz-spot-fill", f"Finviz spot fill failed: {exc}", exc,
            )
            return

        if status == "ok":
            self.log_panel.write_line(
                f"Finviz spot fill done: {norm} → {count} quarter(s) written."
            )
        elif status == "empty":
            if self._auto_added_finviz_skips:
                self._save_finviz_blacklist()
            self.log_panel.write_line(
                f"Finviz has no earnings for {norm} (likely an ETF / "
                "fund / uncovered) — added to finviz skip list."
            )
        else:
            self.log_panel.write_line(
                f"Finviz spot fill: {norm} → status='{status}'."
            )

    def _stop_finviz_fill(self):
        if self._finviz_worker and self._finviz_worker.isRunning():
            self._finviz_worker.request_stop()
            self.log_panel.write_line("Finviz fill stop requested.")
        else:
            self.log_panel.write_line("No finviz fill running.")

    def _start_finviz_worker(self, symbols: list[str], skip: set[str],
                             *, mode: str, label: str):
        self._earn_coord._start_finviz_worker(
            symbols, skip, mode=mode, label=label,
        )

    @pyqtSlot(int, int)
    def _on_finviz_done(self, filled: int, errors: int):
        self._earn_coord._on_finviz_done(filled, errors)

    # ── Ticker greylist ─────────────────────────────────────────────────
    # Greylist is a SCAN-ONLY filter — tickers in this list are skipped by
    # _filtered_symbols but still receive OHLCV / sector / earnings updates.
    # Use it to temporarily de-clutter scan output without halting refresh.

    _GREYLIST_FILE = config.DATA_DIR / "greylist.txt"

    def _load_greylist(self):
        """Load grey-listed tickers from file. Degrades to an empty set on a
        locked/corrupt/unreadable file (mirrors the other skip-list loaders) —
        runs in __init__ before the window is shown, so an unguarded read error
        would otherwise abort launch with no GUI to surface it."""
        if self._GREYLIST_FILE.exists():
            try:
                text = self._GREYLIST_FILE.read_text(encoding="utf-8").strip()
                self._greylist = {
                    self._normalize_ticker(t)
                    for t in text.split(",") if t.strip()
                }
                return
            except Exception as exc:
                log.warning("Failed to load greylist: %s", exc)
        self._greylist = set()

    def _save_greylist(self):
        """Persist greylist to file (atomic write via temp + rename)."""
        config.atomic_write_text(
            self._GREYLIST_FILE,
            ", ".join(sorted(self._greylist)),
        )

    def _show_greylist_editor(self):
        """Open a dialog to edit the ticker greylist."""
        dlg = QDialog(self)
        dlg.setWindowTitle("Ticker Greylist")
        dlg.setModal(True)
        dlg.setMinimumWidth(500)
        layout = QVBoxLayout(dlg)

        lbl = QLabel(
            "Tickers to OMIT FROM SCANS only (comma-separated).\n\n"
            "Greylist does NOT affect OHLCV / sector / earnings updates — "
            "those still run normally so you can clear a ticker from the "
            "list and immediately scan it without re-downloading anything."
        )
        lbl.setWordWrap(True)
        layout.addWidget(lbl)

        txt = QTextEdit()
        txt.setPlaceholderText("e.g. AAPL, MSFT, NVDA")
        txt.setPlainText(", ".join(sorted(self._greylist)))
        txt.setMinimumHeight(200)
        layout.addWidget(txt)

        count_lbl = QLabel(f"Currently grey-listed: {len(self._greylist)}")
        layout.addWidget(count_lbl)

        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_ok = QPushButton("OK")
        btn_ok.clicked.connect(dlg.accept)
        btn_row.addWidget(btn_ok)
        btn_cancel = QPushButton("Cancel")
        btn_cancel.clicked.connect(dlg.reject)
        btn_row.addWidget(btn_cancel)
        layout.addLayout(btn_row)

        if dlg.exec() == QDialog.DialogCode.Accepted:
            raw = txt.toPlainText().strip()
            self._greylist = {
                self._normalize_ticker(t)
                for t in raw.split(",") if t.strip()
            }
            self._save_greylist()
            self.log_panel.write_line(
                f"Greylist updated: {len(self._greylist)} tickers (scan-only filter)"
            )

    # ── Sector / Earnings fill handlers ─────────────────────────────────

    def _get_universe_symbols(self) -> list[str]:
        """Return the universe symbol list for fill operations."""
        if self._universe_df is not None:
            return [
                s for s in self._universe_df["symbol"].tolist()
                if isinstance(s, str) and s.strip()
            ]
        return list(self._symbols)

    def _bulk_fill_sectors(self):
        if self._sector_worker and self._sector_worker.isRunning():
            self.log_panel.write_line("Sector fill already running.")
            return
        syms = self._get_universe_symbols()
        self._sector_worker = SectorFillWorker(syms, self._blacklist, mode="bulk")
        self._sector_worker.log_msg.connect(self.log_panel.write_line)
        self._sector_worker.progress.connect(
            lambda d, t: self.status.showMessage(f"Sector fill: {d}/{t}")
        )
        self._sector_worker.finished.connect(
            lambda f, e: self.status.showMessage(f"Sector fill done: {f} filled, {e} errors")
        )
        self._sector_worker.start()

    def _targeted_fill_sectors(self):
        if self._sector_worker and self._sector_worker.isRunning():
            self.log_panel.write_line("Sector fill already running.")
            return
        syms = self._get_universe_symbols()
        self._sector_worker = SectorFillWorker(syms, self._blacklist, mode="targeted")
        self._sector_worker.log_msg.connect(self.log_panel.write_line)
        self._sector_worker.progress.connect(
            lambda d, t: self.status.showMessage(f"Targeted sector fill: {d}/{t}")
        )
        self._sector_worker.finished.connect(
            lambda f, e: self.status.showMessage(f"Sector fill done: {f} filled, {e} errors")
        )
        self._sector_worker.start()

    def _stop_sector_fill(self):
        if self._sector_worker and self._sector_worker.isRunning():
            self._sector_worker.request_stop()
            self.log_panel.write_line("Sector fill stop requested.")
        else:
            self.log_panel.write_line("No sector fill running.")

    def _bulk_fill_earnings(self):
        if self._earnings_worker and self._earnings_worker.isRunning():
            self.log_panel.write_line("Earnings fill already running.")
            return
        syms = self._get_universe_symbols()
        self._earnings_worker = EarningsFillWorker(syms, self._blacklist, mode="bulk")
        self._earnings_worker.log_msg.connect(self.log_panel.write_line)
        self._earnings_worker.progress.connect(
            lambda d, t: self.status.showMessage(f"Earnings calendar: {d}/{t}")
        )

        def _on_done(f, e):
            # Phase 5: manual Nasdaq run resets the daily counter so
            # the next auto-trigger waits a full day regardless of
            # whether this run was triggered manually or automatically.
            self._stamp_nasdaq_run_now()
            self.status.showMessage(
                f"Earnings fill done: {f} filled, {e} errors"
            )

        self._earnings_worker.finished.connect(_on_done)
        self._earnings_worker.start()

    def _targeted_fill_earnings(self):
        if self._earnings_worker and self._earnings_worker.isRunning():
            self.log_panel.write_line("Earnings fill already running.")
            return
        syms = self._get_universe_symbols()
        self._earnings_worker = EarningsFillWorker(syms, self._blacklist, mode="targeted")
        self._earnings_worker.log_msg.connect(self.log_panel.write_line)
        self._earnings_worker.progress.connect(
            lambda d, t: self.status.showMessage(f"Targeted earnings fill: {d}/{t}")
        )
        self._earnings_worker.finished.connect(
            lambda f, e: self.status.showMessage(f"Earnings fill done: {f} filled, {e} errors")
        )
        self._earnings_worker.start()

    def _stop_earnings_fill(self):
        if self._earnings_worker and self._earnings_worker.isRunning():
            self._earnings_worker.request_stop()
            self.log_panel.write_line("Earnings fill stop requested.")
        else:
            self.log_panel.write_line("No earnings fill running.")

    def _spot_fill_yahoo(self):
        """Menu: single-ticker Yahoo (yfinance) earnings-date fetch.
        Runs inline — no worker thread for one ticker."""
        sym, ok = QInputDialog.getText(
            self, "Spot Fill Earnings Dates (Yahoo)",
            "Ticker:",
        )
        if not ok or not sym.strip():
            return
        norm = self._normalize_ticker(sym)
        if norm in self._blacklist:
            QMessageBox.information(
                self, "Skipped",
                f"{norm} is on the universal blacklist — remove it first.",
            )
            return
        from .. import yahoo_fill
        self.log_panel.write_line(f"Yahoo spot fill: {norm}...")
        try:
            count, status = yahoo_fill.spot_fill_yahoo(norm, self._blacklist)
        except Exception as exc:
            self._log_error(
                "yahoo-spot-fill", f"Yahoo spot fill failed: {exc}", exc,
            )
            return
        if status == "ok":
            self.log_panel.write_line(
                f"Yahoo spot fill done: {norm} → wrote {count} row."
            )
        else:
            self.log_panel.write_line(
                f"Yahoo spot fill: {norm} → status='{status}'."
            )

    # ── Zacks menu handlers (Phase 6) ─────────────────────────────────

    def _bulk_fill_zacks(self):
        """Menu: full universe Zacks earnings history pull. Multi-hour.

        Pre-skip pass: before kicking off the worker, walk universe.csv
        and add every flagged ETF / ADR to the Zacks-only skip list.
        Zacks doesn't cover those, so this saves 5,000+ tickers' worth
        of network calls (~2 hours at 1.5s pacing) right off the top.
        Only runs on bulk — smart-refresh is already small enough that
        we want to detect new ETF additions naturally.
        """
        if self._zacks_worker and self._zacks_worker.isRunning():
            self.log_panel.write_line("Zacks fill already running.")
            return
        from ..zacks_scraper import has_zacks_cookies
        if not has_zacks_cookies():
            QMessageBox.warning(
                self, "Zacks Cookies Required",
                "Zacks scraping requires a fresh browser cookie string to "
                "bypass Imperva.\n\nUse Data → Set Zacks Cookies... before "
                "running a Zacks fill.",
            )
            return

        # Pre-skip pass over universe.csv ETF/ADR flags. Adds them
        # to the persisted Zacks skip list (visible in the skip-list
        # editor). The dynamic _etf_adr_auto_skip_set in _zacks_skip_set
        # ALSO catches these every run, so this persistent pre-skip is
        # now belt-and-suspenders — kept because the editor surfacing
        # is useful for triage. Idempotent.
        pre_added = self._pre_skip_known_etfs_adrs()
        if pre_added:
            self.log_panel.write_line(
                f"Bulk fill prep: pre-added {pre_added} known ETF/ADR "
                f"ticker(s) to Zacks skip list "
                f"(Zacks skip list now: {len(self._zacks_blacklist)})."
            )
        # Dynamic-skip count log (covers BOTH the pre-added entries
        # above AND any ETF/ADR flags added after the last pre-skip
        # via a universe refresh).
        self._log_etf_adr_preskip("Zacks bulk fill",
                                  self._get_universe_symbols())

        if QMessageBox.question(
            self, "Bulk Fill Earnings (Zacks)",
            "This will pull 5 years of EPS + revenue history for every "
            "ticker in your universe that isn't already on the Zacks "
            "skip list.\n\n"
            "Estimated time at the default 1.5s pacing: 5–7 hours for "
            "~15,000 tickers (less after the ETF/ADR pre-skip pass).\n\n"
            "Continue?",
        ) != QMessageBox.StandardButton.Yes:
            return
        syms = self._get_universe_symbols()
        self._start_zacks_worker(syms, mode="bulk", label="Zacks bulk fill")

    def _pre_skip_known_etfs_adrs(self) -> int:
        """Walk universe.csv and add every row with `etf=True` or
        `adr=True` to the Zacks-only skip list. Returns the count of
        NEW additions (already-listed tickers don't count).

        Idempotent — safe to call before every bulk fill. Universe is
        re-downloaded weekly so new ETF launches will appear and get
        skipped on the next bulk run."""
        if self._universe_df is None or self._universe_df.empty:
            return 0
        df = self._universe_df
        if "etf" not in df.columns and "adr" not in df.columns:
            return 0

        candidates: set[str] = set()
        if "etf" in df.columns:
            mask_etf = df["etf"].fillna(False).astype(bool)
            candidates.update(
                self._normalize_ticker(s)
                for s in df.loc[mask_etf, "symbol"].astype(str)
                if isinstance(s, str) and s.strip()
            )
        if "adr" in df.columns:
            mask_adr = df["adr"].fillna(False).astype(bool)
            candidates.update(
                self._normalize_ticker(s)
                for s in df.loc[mask_adr, "symbol"].astype(str)
                if isinstance(s, str) and s.strip()
            )
        candidates.discard("")  # drop normalized blanks

        new = candidates - self._zacks_blacklist
        if not new:
            return 0
        self._zacks_blacklist |= new
        try:
            self._save_zacks_blacklist()
        except Exception as exc:
            log.warning("Pre-skip save failed: %s", exc)
        return len(new)

    def _targeted_fill_zacks(self):
        """Menu: pull Zacks history for tickers with no rows in
        earnings_history.parquet (gap fill)."""
        if self._zacks_worker and self._zacks_worker.isRunning():
            self.log_panel.write_line("Zacks fill already running.")
            return
        from ..zacks_scraper import has_zacks_cookies
        from ..earnings_history import find_gap_tickers
        if not has_zacks_cookies():
            QMessageBox.warning(
                self, "Zacks Cookies Required",
                "Zacks scraping requires a fresh browser cookie string. "
                "Use Data → Set Zacks Cookies... first.",
            )
            return
        syms = self._get_universe_symbols()
        self._log_etf_adr_preskip("Zacks targeted fill", syms)
        # Targeted fill honors the combined Zacks skip set (universe
        # blacklist + Zacks skip list + ETF/ADR auto-skip) so it
        # doesn't waste requests on funds or foreign-issuer ADRs.
        gaps = find_gap_tickers(syms, self._zacks_skip_set())
        if not gaps:
            QMessageBox.information(
                self, "No Gaps",
                "Every ticker in the universe already has Zacks history. "
                "Nothing to do.",
            )
            return
        self.log_panel.write_line(
            f"Zacks targeted fill: {len(gaps)} gap ticker(s) identified."
        )
        self._start_zacks_worker(gaps, mode="targeted",
                                 label="Zacks targeted fill")

    def _stop_zacks_fill(self):
        """Menu: stop a running Zacks fill operation."""
        if self._zacks_worker and self._zacks_worker.isRunning():
            self._zacks_worker.request_stop()
            self.log_panel.write_line("Zacks fill stop requested.")
        else:
            self.log_panel.write_line("No Zacks fill running.")

    def _start_zacks_worker(self, symbols: list[str], *, mode: str, label: str):
        self._earn_coord._start_zacks_worker(symbols, mode=mode, label=label)

    def _build_cookie_textedit(
        self, initial: str, *, hide_initially: bool,
    ) -> tuple["QTextEdit", "QPushButton"]:
        """Thin wrapper around the module-level helper. Kept as a method
        for back-compat with existing tests; the heavy lifting lives in
        `_build_cookie_textedit_widget` so the startup prompt can reuse
        it without instantiating MainWindow."""
        return _build_cookie_textedit_widget(initial, hide_initially=hide_initially)

    @pyqtSlot(int)
    def _on_imperva_block_detected(self, consec_count: int):
        """Cookie-refresh handler invoked when a Zacks fill hits N
        consecutive failures. Worker is paused waiting for our reply.

        New flow (May 2026 rewrite): launch Firefox at the persistent
        Zacks profile via the same path the menu action uses. The user
        does the actual JS-challenge / CAPTCHA / login work and closes
        Firefox when done — we read cookies.sqlite at that point and
        resume the fill. No timeouts, no headless probing, no auto-
        detection of monitor or window state.

        Stale-signal guard: `imperva_block_detected.emit()` is a queued
        cross-thread signal, while "Stop Zacks Fill" is a same-thread
        synchronous menu click. If Stop is clicked between emit and
        slot dispatch, the worker exits with `_block_decision="stop"`
        and there's no listener left for Resume / Cancel. Skip the
        handler in that case.
        """
        worker = self._zacks_worker
        if worker is None or not worker.isRunning():
            self.log_panel.write_line(
                f"Zacks: stale Imperva-block signal ({consec_count} "
                "failures) ignored — worker already stopped."
            )
            return
        self.log_panel.write_line(
            f"Zacks: {consec_count} consecutive failures — likely Imperva "
            "block."
        )

        # New imperva-block flow (May 2026): launch Firefox at the
        # persistent profile, kick off a FirefoxCookieWaitWorker that
        # reads cookies.sqlite when the user closes the browser, and
        # arm `_zacks_worker_awaiting_resume` so the cookie-finished
        # slot resumes / cancels the fill based on the scrape verdict.
        # The user does the actual cookie-acquisition work in Firefox
        # (CAPTCHA, login, refresh) and closes when done — we don't
        # try to detect "challenge complete" or apply timeouts.
        if (self._cookie_wait_worker is not None
                and self._cookie_wait_worker.isRunning()):
            self.log_panel.write_line(
                "Zacks: cookie refresh already in flight — close the "
                "open Firefox to capture, then the fill resumes."
            )
            self._zacks_worker_awaiting_resume = self._zacks_worker
            return

        self.log_panel.write_line(
            "── Zacks auto-pause: launching Firefox at persistent "
            "profile. Complete any CAPTCHA / login, then CLOSE Firefox "
            "to capture cookies and resume the fill. ──"
        )
        self._zacks_worker_awaiting_resume = self._zacks_worker
        self._launch_cookie_browser_and_wait()

    def _set_zacks_cookies_dialog(self):
        """Menu: paste a fresh cookie string for the Zacks scraper."""
        from ..zacks_scraper import get_zacks_cookies, set_zacks_cookies
        existing = get_zacks_cookies() or ""

        dlg = QDialog(self)
        dlg.setWindowTitle("Set Zacks Cookies")
        dlg.setMinimumWidth(640)
        layout = QVBoxLayout(dlg)
        layout.addWidget(QLabel(
            "Paste a `key=value; key=value; ...` cookie string copied from "
            "a logged-in zacks.com session (DevTools → Application → "
            "Cookies → copy as cookie header).\n\n"
            "Leave blank to clear stored cookies."
        ))
        # Audit M7: same Show/Hide pattern as the Imperva dialog.
        txt, show_btn = self._build_cookie_textedit(existing, hide_initially=True)
        toggle_row = QHBoxLayout()
        toggle_row.addWidget(QLabel("Cookies:"))
        toggle_row.addStretch()
        toggle_row.addWidget(show_btn)
        layout.addLayout(toggle_row)
        layout.addWidget(txt)
        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(dlg.accept)
        btns.rejected.connect(dlg.reject)
        layout.addWidget(btns)

        if dlg.exec() != QDialog.DialogCode.Accepted:
            return

        new = txt.toPlainText().strip()
        if set_zacks_cookies(new):
            if new:
                self.log_panel.write_line("Zacks cookies updated.")
            else:
                self.log_panel.write_line("Zacks cookies cleared.")
        else:
            QMessageBox.warning(
                self, "Save Failed",
                "Could not write the cookie file under scanner_data/.",
            )

    # ──────────────────────────────────────────────────────────────────
    # Cookie-browser monitor preference (Settings menu)
    # ──────────────────────────────────────────────────────────────────

    def _qsettings(self):
        """Return a QSettings handle keyed under the scanner's app id.
        Centralized so all preference reads/writes share the same key
        space."""
        from PyQt6.QtCore import QSettings
        return QSettings("trade_scanner_fh", "Trade_Scanner_FH")

    def _load_cookie_monitor_pref(self):
        """Read the saved cookie-browser monitor geometry from
        QSettings into `self._cookie_monitor_geom`. Called once at
        construction. Tolerates missing / malformed values."""
        try:
            s = self._qsettings()
            x = s.value("cookie_browser_monitor/x")
            y = s.value("cookie_browser_monitor/y")
            w = s.value("cookie_browser_monitor/w")
            h = s.value("cookie_browser_monitor/h")
            if None in (x, y, w, h):
                self._cookie_monitor_geom = None
                return
            self._cookie_monitor_geom = (int(x), int(y), int(w), int(h))
        except Exception as exc:
            log.debug("Could not load cookie-browser monitor pref: %s", exc)
            self._cookie_monitor_geom = None

    # ──────────────────────────────────────────────────────────────────
    # Per-row HOTKEY ticker sender — config persistence + toggle + cue
    # ──────────────────────────────────────────────────────────────────

    def _load_hotkey_pref(self) -> HotkeyConfig:
        """Read the saved HotkeyConfig from QSettings. Tolerates missing
        / malformed values by falling back to defaults via
        `HotkeyConfig.normalized()`."""
        try:
            s = self._qsettings()
            x = s.value("hotkey/click_x")
            y = s.value("hotkey/click_y")
            rx = s.value("hotkey/return_click_x")
            ry = s.value("hotkey/return_click_y")
            cfg = HotkeyConfig(
                click_x=int(x) if x is not None else None,
                click_y=int(y) if y is not None else None,
                delay_ms=int(s.value("hotkey/delay_ms", 200)),
                cue=str(s.value("hotkey/cue", hotkey_mod.CUE_RIGHT_CLICK)),
                end_sequence=str(s.value("hotkey/end_sequence",
                                          hotkey_mod.END_ENTER)),
                return_click_x=int(rx) if rx is not None else None,
                return_click_y=int(ry) if ry is not None else None,
            )
            return cfg.normalized()
        except Exception as exc:
            log.debug("Could not load hotkey pref: %s", exc)
            return HotkeyConfig()

    def _save_hotkey_pref(self, cfg: HotkeyConfig) -> None:
        """Persist the HotkeyConfig to QSettings."""
        s = self._qsettings()
        if cfg.click_x is None or cfg.click_y is None:
            s.remove("hotkey/click_x")
            s.remove("hotkey/click_y")
        else:
            s.setValue("hotkey/click_x", int(cfg.click_x))
            s.setValue("hotkey/click_y", int(cfg.click_y))
        s.setValue("hotkey/delay_ms", int(cfg.delay_ms))
        s.setValue("hotkey/cue", cfg.cue)
        s.setValue("hotkey/end_sequence", cfg.end_sequence)
        if cfg.return_click_x is None or cfg.return_click_y is None:
            s.remove("hotkey/return_click_x")
            s.remove("hotkey/return_click_y")
        else:
            s.setValue("hotkey/return_click_x", int(cfg.return_click_x))
            s.setValue("hotkey/return_click_y", int(cfg.return_click_y))

    def _apply_hotkey_button_style(self, on: bool) -> None:
        """Repaint the HOTKEY button to reflect its on/off state.
        Off = neutral grey; on = hot pink with bright purple bold text
        per the user's spec."""
        if on:
            self.btn_hotkey.setStyleSheet(
                "QPushButton { background: #ff1493; color: #c000ff; "
                "font-size: 14px; font-weight: bold; padding: 8px 16px; "
                "border-radius: 4px; }"
                "QPushButton:hover { background: #ff5bbf; }"
            )
        else:
            self.btn_hotkey.setStyleSheet(
                "QPushButton { background: #555; color: #ddd; "
                "font-size: 14px; padding: 8px 16px; border-radius: 4px; }"
                "QPushButton:hover { background: #666; }"
            )

    def _toggle_hotkey(self) -> None:
        """Flip the HOTKEY toggle. If turning on without a saved click
        position, prompt the user to open Hotkey Settings instead of
        silently no-op-ing the mouse cue."""
        # The button is checkable, so its checked state has already
        # flipped by the time this slot fires — read it as the request.
        wants_on = self.btn_hotkey.isChecked()
        if wants_on and not self._hotkey_cfg.has_position:
            self.btn_hotkey.setChecked(False)
            self._apply_hotkey_button_style(False)
            self._hotkey_enabled = False
            ans = QMessageBox.question(
                self, "Hotkey Settings Required",
                "No hotkey click position is set.\n\n"
                "Open Hotkey Settings now to configure the target "
                "position, send cue, delay, and end sequence?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            )
            if ans == QMessageBox.StandardButton.Yes:
                self._open_hotkey_settings()
                # If the user saved a position, auto-flip ON for them.
                if self._hotkey_cfg.has_position:
                    self.btn_hotkey.setChecked(True)
                    self._apply_hotkey_button_style(True)
                    self._hotkey_enabled = True
                    self.log_panel.write_line(
                        "Hotkey: ON (click position set, ready)."
                    )
            return

        self._hotkey_enabled = wants_on
        self._apply_hotkey_button_style(wants_on)
        if wants_on:
            self.log_panel.write_line(
                f"Hotkey: ON — cue {hotkey_mod.cue_label(self._hotkey_cfg.cue)} "
                f"on a row sends ticker to ({self._hotkey_cfg.click_x},"
                f"{self._hotkey_cfg.click_y})."
            )
        else:
            self.log_panel.write_line("Hotkey: OFF.")

    def _open_hotkey_settings(self) -> None:
        """Show the modal HotkeySettingsDialog. On Accept, persist the
        new config to QSettings and refresh `_hotkey_cfg`."""
        dlg = HotkeySettingsDialog(self._hotkey_cfg, self)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        new_cfg = dlg.result_config().normalized()
        self._hotkey_cfg = new_cfg
        self._save_hotkey_pref(new_cfg)
        pos = (f"({new_cfg.click_x},{new_cfg.click_y})"
               if new_cfg.has_position else "(no position)")
        ret = (f"({new_cfg.return_click_x},{new_cfg.return_click_y})"
               if new_cfg.has_return_position else "off")
        self.log_panel.write_line(
            f"Hotkey settings updated: pos={pos} "
            f"cue={hotkey_mod.cue_label(new_cfg.cue)} "
            f"delay={new_cfg.delay_ms}ms "
            f"end={hotkey_mod.end_sequence_label(new_cfg.end_sequence)} "
            f"return={ret}."
        )
        # If the toggle was on but the new config has no position, the
        # cue would silently no-op — flip it off for honesty.
        if self._hotkey_enabled and not new_cfg.has_position:
            self._hotkey_enabled = False
            self.btn_hotkey.setChecked(False)
            self._apply_hotkey_button_style(False)

    # ── Cue interception ──────────────────────────────────────────────

    def eventFilter(self, watched, event):
        """Intercept the configured cue on the results-table when
        HOTKEY is on. All other events pass through.

        Two cue families:
          * Mouse cues — filter sees MouseButtonPress on the viewport.
            We resolve the row under the cursor.
          * Keyboard cue (Enter) — filter sees KeyPress on the table
            itself. We resolve the currently-selected row.

        Returning True swallows the event so the cue does NOT also pop
        the normal table behavior (context menu, edit-on-Enter, etc.).
        Returning False lets Qt handle it as usual.
        """
        cue = self._hotkey_cfg.cue

        # Mouse-cue path: presses on the viewport
        if (
            self._hotkey_enabled
            and event.type() == QEvent.Type.MouseButtonPress
            and watched is self.results_table.viewport()
            and not hotkey_mod.is_keyboard_cue(cue)
        ):
            if self._cue_matches(event, cue):
                self._handle_hotkey_cue_mouse(event)
                return True

        # Keyboard-cue path: Enter on the table while a row is selected
        if (
            self._hotkey_enabled
            and event.type() == QEvent.Type.KeyPress
            and watched is self.results_table
            and hotkey_mod.is_keyboard_cue(cue)
        ):
            if self._key_matches(event, cue):
                self._handle_hotkey_cue_key()
                return True

        # Right-click cue: suppress the table's OWN context menu. The send
        # now waits for the user to release the button before moving the
        # cursor (see hotkey._wait_for_input_release), so the cursor lingers
        # over the table through the release — which would otherwise pop the
        # row context menu on top of the cue. Only the right-click cue
        # conflicts with the context menu; every other cue (and hotkey-off)
        # leaves it fully intact.
        if (
            self._hotkey_enabled
            and event.type() == QEvent.Type.ContextMenu
            and watched in (self.results_table, self.results_table.viewport())
            and cue == hotkey_mod.CUE_RIGHT_CLICK
        ):
            return True

        return super().eventFilter(watched, event)

    @staticmethod
    def _cue_matches(event, cue_id: str) -> bool:
        """Return True if `event` (a QMouseEvent) matches the given
        mouse cue id. Modifier-aware for Shift / Ctrl variants."""
        button = event.button()
        mods = event.modifiers()
        if cue_id == hotkey_mod.CUE_RIGHT_CLICK:
            return button == Qt.MouseButton.RightButton
        if cue_id == hotkey_mod.CUE_MIDDLE:
            return button == Qt.MouseButton.MiddleButton
        if cue_id == hotkey_mod.CUE_SHIFT_LEFT:
            return (button == Qt.MouseButton.LeftButton
                    and mods & Qt.KeyboardModifier.ShiftModifier)
        if cue_id == hotkey_mod.CUE_CTRL_LEFT:
            return (button == Qt.MouseButton.LeftButton
                    and mods & Qt.KeyboardModifier.ControlModifier)
        return False

    @staticmethod
    def _key_matches(event, cue_id: str) -> bool:
        """Return True if `event` (a QKeyEvent) matches the given
        keyboard cue id. Matches both main-keyboard Return and numpad
        Enter so either works equally for the user."""
        if cue_id == hotkey_mod.CUE_ENTER_KEY:
            return event.key() in (
                Qt.Key.Key_Return, Qt.Key.Key_Enter,
            )
        return False

    def _handle_hotkey_cue_mouse(self, event) -> None:
        """Mouse-cue handler: resolve the row under the cursor and
        dispatch the send sequence."""
        view = self.results_table
        index = view.indexAt(event.position().toPoint())
        if not index.isValid():
            self.log_panel.write_line(
                "Hotkey: no row under cursor — click on a results row."
            )
            return
        # Visual feedback — select the row the user targeted.
        view.selectRow(index.row())
        self._dispatch_hotkey_for_row(index.row())

    def _handle_hotkey_cue_key(self) -> None:
        """Keyboard-cue handler: resolve the currently-selected row and
        dispatch the send sequence. Pairs with arrow-key navigation."""
        view = self.results_table
        index = view.currentIndex()
        if not index.isValid():
            self.log_panel.write_line(
                "Hotkey: no row selected — use arrow keys to pick a row."
            )
            return
        self._dispatch_hotkey_for_row(index.row())

    def _dispatch_hotkey_for_row(self, row: int) -> None:
        """Extract the ticker from `row` of the results-table proxy
        model and dispatch the click → type → end-key sequence on a
        daemon thread so the GUI stays responsive."""
        view = self.results_table
        ticker_index = view.proxy.index(row, 0)
        ticker = ticker_index.data(Qt.ItemDataRole.DisplayRole)
        if not ticker:
            self.log_panel.write_line("Hotkey: row has no ticker.")
            return

        # Serialize hotkey sends: pyautogui input is a single shared stream and
        # send_ticker also save/restores process-global FAILSAFE/PAUSE, so two
        # overlapping cues would interleave clicks/keystrokes into one OS input
        # queue (garbled ticker entry) and corrupt the global save/restore.
        # Drop a cue that arrives while one is in flight — a dropped second cue
        # is the desired behavior. Also refuse while a bulk watchlist send runs.
        if getattr(self, "_hotkey_in_flight", False):
            self.log_panel.write_line("Hotkey: busy — cue ignored.")
            return
        bridge = getattr(self, "_bridge_worker", None)
        if bridge is not None and bridge.isRunning():
            self.log_panel.write_line(
                "Hotkey ignored — watchlist send in progress."
            )
            return

        cfg = self._hotkey_cfg
        log_writer = self.log_panel.write_line
        self._hotkey_in_flight = True

        def _runner():
            try:
                hotkey_mod.send_ticker(str(ticker), cfg, on_log=log_writer)
            except Exception as exc:
                log.exception("Hotkey send failed: %s", exc)
                log_writer(f"Hotkey: send failed — {exc}")
            finally:
                self._hotkey_in_flight = False

        import threading
        threading.Thread(
            target=_runner, name=f"hotkey-{ticker}", daemon=True,
        ).start()

    def _save_cookie_monitor_pref(self, geom):
        """Persist or clear the cookie-browser monitor preference."""
        s = self._qsettings()
        if geom is None:
            for k in ("x", "y", "w", "h"):
                s.remove(f"cookie_browser_monitor/{k}")
            return
        x, y, w, h = geom
        s.setValue("cookie_browser_monitor/x", int(x))
        s.setValue("cookie_browser_monitor/y", int(y))
        s.setValue("cookie_browser_monitor/w", int(w))
        s.setValue("cookie_browser_monitor/h", int(h))

    def _resolve_current_monitor_geometry(self):
        """Return the physical-pixel (x, y, w, h) of the monitor the
        main window is currently on. Used only when the user clicks
        Settings → Set Cookie Browser Monitor — never inline during a
        scan / auto-pause."""
        try:
            wh = self.windowHandle()
            screen = wh.screen() if wh is not None else None
            if screen is None:
                center = self.frameGeometry().center()
                screen = (
                    QApplication.screenAt(center)
                    or QApplication.primaryScreen()
                )
            if screen is None:
                return None
            geom = screen.geometry()
            ratio = float(screen.devicePixelRatio() or 1.0)
            return (
                int(geom.x() * ratio), int(geom.y() * ratio),
                int(geom.width() * ratio), int(geom.height() * ratio),
            )
        except Exception as exc:
            log.debug("Could not resolve current monitor geometry: %s", exc)
            return None

    def _set_cookie_browser_monitor_action(self):
        """Settings menu: pin the cookie browser to the monitor the
        scanner is currently on."""
        geom = self._resolve_current_monitor_geometry()
        if geom is None:
            QMessageBox.warning(
                self, "Could Not Detect Monitor",
                "The scanner couldn't resolve which monitor it's on. "
                "Try moving the window fully onto a single monitor and "
                "running this again.",
            )
            return
        self._cookie_monitor_geom = geom
        self._save_cookie_monitor_pref(geom)
        self.log_panel.write_line(
            f"Cookie browser monitor set: {geom[2]}×{geom[3]} @ "
            f"({geom[0]},{geom[1]}). Future Refresh Zacks Cookies "
            "launches will open Firefox maximized on this monitor."
        )

    def _clear_cookie_browser_monitor_action(self):
        """Settings menu: forget the saved monitor preference."""
        self._cookie_monitor_geom = None
        self._save_cookie_monitor_pref(None)
        self.log_panel.write_line(
            "Cookie browser monitor preference cleared. Firefox will "
            "open wherever the OS places it."
        )

    # ──────────────────────────────────────────────────────────────────
    # Cookie-refresh manual flow (May 2026 rewrite)
    # ──────────────────────────────────────────────────────────────────

    def _refresh_zacks_cookies_action(self):
        """Menu: open Firefox at the persistent Zacks profile and
        wait for the user to close it before scraping cookies. The
        Imperva auto-pause flow funnels into the same mechanism."""
        if (self._cookie_wait_worker is not None
                and self._cookie_wait_worker.isRunning()):
            QMessageBox.information(
                self, "Cookie Refresh In Progress",
                "A Firefox cookie-refresh is already in flight. Close "
                "the open Firefox window to capture the cookies, or "
                "use Settings to clear the cookie-wait if Firefox "
                "crashed silently.",
            )
            return
        self._launch_cookie_browser_and_wait()

    def _launch_cookie_browser_and_wait(self):
        """Spawn Firefox at the persistent profile, then attach a
        FirefoxCookieWaitWorker that reads cookies on close. Shared
        by the menu action and the Imperva auto-pause path. Both
        callers populate `_zacks_worker_awaiting_resume` if a fill
        worker should be resumed when cookies land (auto-pause sets
        it; manual menu action leaves it None)."""
        from ..zacks_scraper import (
            launch_firefox_for_zacks_cookies, get_zacks_cookies,
            _cookie_signature, get_zacks_profile_dir,
        )

        pre_cookies = get_zacks_cookies() or ""
        pre_sig = _cookie_signature(pre_cookies)

        if self._cookie_monitor_geom is None:
            self.log_panel.write_line(
                "── Refresh Zacks cookies: launching Firefox (no "
                "monitor preference set — Settings → Set Cookie "
                "Browser Monitor pins it). ──"
            )
        else:
            mg = self._cookie_monitor_geom
            self.log_panel.write_line(
                f"── Refresh Zacks cookies: launching Firefox on "
                f"monitor {mg[2]}×{mg[3]} @ ({mg[0]},{mg[1]}). ──"
            )
        QApplication.processEvents()

        def _progress(msg: str):
            try:
                self.log_panel.write_line(f"  {msg}")
                QApplication.processEvents()
            except Exception as exc:
                log.debug("cookie-refresh progress write failed: %s", exc)

        try:
            pid = launch_firefox_for_zacks_cookies(
                geometry=self._cookie_monitor_geom,
                progress_log=_progress,
            )
        except Exception as exc:
            self._log_error(
                "zacks-cookies",
                f"── Refresh Zacks cookies CRASHED at launch: {exc} ──", exc,
            )
            self._fallback_to_paste_dialog_or_cancel()
            return

        if pid is None:
            self.log_panel.write_line(
                "── Refresh Zacks cookies: Firefox failed to launch. "
                "Install Firefox at C:\\Program Files\\Mozilla Firefox\\ "
                "or use Data → Set Zacks Cookies... to paste manually. ──"
            )
            self._fallback_to_paste_dialog_or_cancel()
            return

        # Lock out the menu action while the wait is in flight so a
        # second click can't spawn an overlapping Firefox.
        if hasattr(self, "act_zacks_open_cookie_browser"):
            self.act_zacks_open_cookie_browser.setEnabled(False)

        from .workers import FirefoxCookieWaitWorker
        self._cookie_wait_worker = self._start_worker(
            FirefoxCookieWaitWorker(
                get_zacks_profile_dir(),
                pre_signature=pre_sig,
            ),
            finished=self._on_cookie_wait_done,
        )

    @pyqtSlot(bool, int, bool, str)
    def _on_cookie_wait_done(self, success: bool, n_cookies: int,
                              is_new: bool, pre_sig: str):
        """FirefoxCookieWaitWorker.finished slot. Resumes / cancels the
        paused Zacks fill (if any) based on the scrape verdict."""
        if hasattr(self, "act_zacks_open_cookie_browser"):
            self.act_zacks_open_cookie_browser.setEnabled(True)
        worker_to_resume = self._zacks_worker_awaiting_resume
        self._zacks_worker_awaiting_resume = None
        self._cookie_wait_worker = None

        if success and is_new:
            self.log_panel.write_line(
                f"── Cookie refresh OK — NEW session captured "
                f"({n_cookies} cookies). ──"
            )
            if (worker_to_resume is not None
                    and worker_to_resume.isRunning()):
                self.log_panel.write_line(
                    "Zacks fill is paused — resuming with new cookies."
                )
                worker_to_resume.resume_after_block()
            return

        if success and not is_new:
            self.log_panel.write_line(
                f"── Cookie refresh: cookies read OK but match prior "
                f"session ({n_cookies} cookies). Your existing cookies "
                "were still valid; nothing changed. ──"
            )
            # Treat same-as-before as still good for the auto-pause
            # path — the worker's session may simply not have been
            # using these cookies yet (e.g., they were just rotated by
            # the user out-of-band). Resume so the worker re-tries.
            if (worker_to_resume is not None
                    and worker_to_resume.isRunning()):
                worker_to_resume.resume_after_block()
            return

        # Failure: capture didn't yield Imperva tokens.
        self.log_panel.write_line(
            "── Cookie refresh FAILED — no Imperva tokens captured. "
            "Try again, or fall back to Data → Set Zacks Cookies... "
            "(manual paste). ──"
        )
        if worker_to_resume is not None and worker_to_resume.isRunning():
            self._fallback_to_paste_dialog_or_cancel(worker_to_resume)

    def _fallback_to_paste_dialog_or_cancel(self, worker_to_resume=None):
        """When the manual browser flow fails inside the imperva
        auto-pause path, give the user a last chance to paste cookies
        manually rather than just stopping the fill silently. Outside
        the auto-pause path, this is a no-op (we already logged the
        failure)."""
        if worker_to_resume is None:
            worker_to_resume = self._zacks_worker_awaiting_resume
            self._zacks_worker_awaiting_resume = None
        if worker_to_resume is None or not worker_to_resume.isRunning():
            return

        from ..zacks_scraper import get_zacks_cookies, set_zacks_cookies
        existing = get_zacks_cookies() or ""

        dlg = QDialog(self)
        dlg.setWindowTitle("Zacks Imperva Block — Paste Cookies")
        dlg.setMinimumWidth(640)
        layout = QVBoxLayout(dlg)
        layout.addWidget(QLabel(
            "The Firefox cookie-refresh didn't yield valid Imperva "
            "tokens.\n\nLast resort: open zacks.com in any browser, "
            "copy a fresh `key=value; key=value; ...` cookie string "
            "from DevTools, and paste it below. Click Stop to abort "
            "the fill."
        ))
        txt, show_btn = self._build_cookie_textedit(
            existing, hide_initially=True,
        )
        toggle_row = QHBoxLayout()
        toggle_row.addWidget(QLabel("Cookies:"))
        toggle_row.addStretch()
        toggle_row.addWidget(show_btn)
        layout.addLayout(toggle_row)
        layout.addWidget(txt)

        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel
        )
        btns.button(QDialogButtonBox.StandardButton.Ok).setText("Resume")
        btns.button(QDialogButtonBox.StandardButton.Cancel).setText("Stop fill")
        btns.accepted.connect(dlg.accept)
        btns.rejected.connect(dlg.reject)
        layout.addWidget(btns)

        result = dlg.exec()
        if result != QDialog.DialogCode.Accepted:
            self.log_panel.write_line("Zacks fill stopped by user.")
            worker_to_resume.cancel_after_block()
            return

        cookie_str = txt.toPlainText().strip()
        if not cookie_str:
            self.log_panel.write_line(
                "No cookies entered — stopping Zacks fill."
            )
            worker_to_resume.cancel_after_block()
            return

        if not set_zacks_cookies(cookie_str):
            QMessageBox.warning(
                self, "Cookie Save Failed",
                "Could not write the cookie file. The fill will stop.",
            )
            worker_to_resume.cancel_after_block()
            return

        self.log_panel.write_line("Cookies refreshed — resuming Zacks fill.")
        worker_to_resume.resume_after_block()

    # Phase 6.5: `_fill_yahoo_gaps_action` and its `_on_yahoo_gaps_done`
    # slot were removed — functionally equivalent to "Targeted Fill
    # Earnings Dates (Yahoo)" after Phase 4/5.5 (every fill auto-
    # reconciles). The menu item also disappears with this delete.

    def _reconcile_earnings_dates_action(self):
        """Menu: force a full universe-wide reconcile of earnings_dates."""
        from ..earnings_reconcile import reconcile_earnings_dates
        try:
            z, y, aug = reconcile_earnings_dates()
        except Exception as exc:
            QMessageBox.critical(
                self, "Reconcile Failed", f"Reconcile error: {exc}",
            )
            return
        msg = (
            f"Reconcile complete — history-sourced: {z}, "
            f"dates-passthrough: {y}, augmented: {aug}."
        )
        self.log_panel.write_line(msg)
        self.status.showMessage(msg)

    # ── Phase 6.5 Diagnostics ────────────────────────────────────────

    def _show_earnings_coverage_report(self):
        """Menu: Data → Earnings → Diagnostics → Earnings Coverage Report.
        Pure read — sub-second compute. Renders as a non-modal dialog
        with counts + per-row drill-down into the actual ticker lists."""
        from ..earnings_history import coverage_report
        syms = self._get_universe_symbols()
        if not syms:
            QMessageBox.information(
                self, "Coverage Report",
                "No universe loaded. Run Refresh Universe first.",
            )
            return
        try:
            rep = coverage_report(syms, self._blacklist)
        except Exception as exc:
            QMessageBox.critical(
                self, "Coverage Report Failed", f"{exc}",
            )
            return
        self._render_coverage_dialog(rep)

    def _render_coverage_dialog(self, rep: dict) -> None:
        """Render the coverage report dict as a read-only dialog."""
        dlg = QDialog(self)
        dlg.setWindowTitle("Earnings Coverage Report")
        dlg.setModal(False)
        dlg.setMinimumWidth(520)
        layout = QVBoxLayout(dlg)

        def _fmt_quarter(ts):
            if ts is None:
                return "—"
            try:
                return pd.Timestamp(ts).strftime("%Y-%m-%d")
            except Exception:
                return str(ts)

        in_scope = max(rep["in_scope"], 1)
        header = QLabel(
            f"<b>Universe:</b> {rep['total_universe']:,} tickers<br>"
            f"&nbsp;&nbsp;blacklisted: {rep['blacklisted']:,}<br>"
            f"&nbsp;&nbsp;in scope:    {rep['in_scope']:,}<br><br>"
            f"<b>Most recent quarter per source:</b><br>"
            f"&nbsp;&nbsp;Finviz:  {_fmt_quarter(rep.get('most_recent_finviz_quarter'))}<br>"
            f"&nbsp;&nbsp;Zacks:   {_fmt_quarter(rep['most_recent_zacks_quarter'])}<br>"
            f"&nbsp;&nbsp;Finnhub: {_fmt_quarter(rep['most_recent_finnhub_quarter'])}"
        )
        header.setTextFormat(Qt.TextFormat.RichText)
        layout.addWidget(header)

        def _make_row(label: str, bucket: dict, color: str):
            row = QHBoxLayout()
            pct = bucket["count"] / in_scope * 100
            lbl = QLabel(
                f"<span style='color:{color}'>● </span>"
                f"<b>{label}:</b> {bucket['count']:,} "
                f"({pct:.1f}% of in-scope)"
            )
            lbl.setTextFormat(Qt.TextFormat.RichText)
            row.addWidget(lbl)
            row.addStretch()
            if bucket["tickers"]:
                btn = QPushButton("Show tickers...")
                btn.clicked.connect(
                    lambda _, t=bucket["tickers"], lab=label:
                    self._show_ticker_list_dialog(lab, t)
                )
                row.addWidget(btn)
            return row

        # Per-source coverage (overlapping sets — sum > in_scope is normal).
        note = QLabel(
            "<i>Under gap-fill, the same ticker can carry rows from "
            "multiple sources covering different periods — the per-source "
            "totals overlap.</i>"
        )
        note.setTextFormat(Qt.TextFormat.RichText)
        note.setWordWrap(True)
        layout.addWidget(note)

        layout.addLayout(_make_row(
            "Finviz coverage",  rep.get("finviz", {"count": 0, "tickers": []}),                          "#4caf50"))
        layout.addLayout(_make_row(
            "Zacks coverage",   rep.get("zacks", rep.get("zacks_only", {"count": 0, "tickers": []})),   "#2196f3"))
        layout.addLayout(_make_row(
            "Finnhub coverage", rep.get("finnhub", rep.get("finnhub_only", {"count": 0, "tickers": []})), "#ff9800"))
        layout.addLayout(_make_row(
            "No coverage at all", rep.get("no_coverage", rep["neither"]),                              "#f44336"))

        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_close = QPushButton("Close")
        btn_close.clicked.connect(dlg.close)
        btn_row.addWidget(btn_close)
        layout.addLayout(btn_row)

        dlg.show()
        # Keep a ref so the non-modal dialog isn't GC'd immediately.
        self._coverage_dialog = dlg

    def _show_ticker_list_dialog(self, title: str, tickers: list[str]) -> None:
        """Secondary popup for the coverage-report drill-downs. Caps
        display at 1000 entries with a "(...)" marker — full list is
        always copyable from the textedit."""
        dlg = QDialog(self)
        dlg.setWindowTitle(f"Coverage: {title} ({len(tickers):,} tickers)")
        dlg.setModal(False)
        dlg.setMinimumWidth(440)
        dlg.setMinimumHeight(420)
        layout = QVBoxLayout(dlg)

        txt = QTextEdit()
        txt.setReadOnly(True)
        txt.setFont(QFont("Consolas", 9))
        if len(tickers) > 1000:
            shown = "\n".join(tickers[:1000])
            shown += f"\n... ({len(tickers) - 1000:,} more)"
        else:
            shown = "\n".join(tickers)
        txt.setPlainText(shown)
        layout.addWidget(txt)

        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_close = QPushButton("Close")
        btn_close.clicked.connect(dlg.close)
        btn_row.addWidget(btn_close)
        layout.addLayout(btn_row)

        dlg.show()

    def _verify_earnings_history_integrity(self):
        """Menu: Data → Earnings → Diagnostics → Verify Integrity.
        Walks earnings_history.parquet for known issues and renders a
        dialog with per-check results + an auto-fix button for the
        fixable ones."""
        from ..earnings_history import (
            load_earnings_history, verify_integrity, fix_integrity_issues,
            save_earnings_history,
        )
        df = load_earnings_history()
        if df is None or df.empty:
            QMessageBox.information(
                self, "Integrity Check",
                "earnings_history.parquet doesn't exist or is empty. "
                "Nothing to verify.",
            )
            return
        try:
            findings = verify_integrity(history_df=df)
        except Exception as exc:
            QMessageBox.critical(
                self, "Integrity Check Failed", f"{exc}",
            )
            return

        dlg = QDialog(self)
        dlg.setWindowTitle("earnings_history Integrity Check")
        dlg.setModal(False)
        dlg.setMinimumWidth(620)
        dlg.setMinimumHeight(420)
        layout = QVBoxLayout(dlg)

        if not findings:
            layout.addWidget(QLabel(
                f"<span style='color:#4caf50'>● PASS</span> — "
                f"{len(df):,} rows, no integrity findings."
            ))
        else:
            n_err = sum(1 for f in findings if f.severity == "error")
            n_warn = sum(1 for f in findings if f.severity == "warning")
            summary = QLabel(
                f"<b>{len(findings)} finding(s):</b> "
                f"{n_err} error, {n_warn} warning. "
                f"Total rows scanned: {len(df):,}."
            )
            summary.setTextFormat(Qt.TextFormat.RichText)
            layout.addWidget(summary)

            for f in findings:
                color = "#f44336" if f.severity == "error" else "#ff9800"
                row_lbl = QLabel(
                    f"<span style='color:{color}'>● </span>"
                    f"<b>{f.check}</b> ({f.severity}, "
                    f"{f.affected_rows} affected"
                    + (", auto-fixable" if f.auto_fixable
                       else ", NOT auto-fixable") + "):<br>"
                    f"&nbsp;&nbsp;{f.description}"
                )
                row_lbl.setTextFormat(Qt.TextFormat.RichText)
                row_lbl.setWordWrap(True)
                layout.addWidget(row_lbl)

        btn_row = QHBoxLayout()
        if any(f.auto_fixable for f in findings):
            btn_fix = QPushButton("Auto-fix all fixable")

            def _do_fix():
                # Refuse while a fill is in flight — its appends would race
                # this whole-file rewrite and one would clobber the other.
                if self._earn_threads_active():
                    QMessageBox.warning(
                        dlg, "Fill in progress",
                        "An earnings fill is currently running. Wait for it "
                        "to finish before auto-fixing the parquet.",
                    )
                    return
                if QMessageBox.question(
                    dlg, "Confirm auto-fix",
                    f"Apply auto-fix repairs to "
                    f"earnings_history.parquet? This rewrites the file.",
                ) != QMessageBox.StandardButton.Yes:
                    return
                from ..earnings_history import HISTORY_WRITE_LOCK
                # Re-read inside the write lock so the repair targets the
                # current on-disk state, not the snapshot captured when the
                # dialog opened. fix_integrity_issues derives its repairs from
                # the frame content (not stored row indices) and each repair is
                # idempotent, so driving them with the original `findings` is
                # safe against the freshly-read frame.
                with HISTORY_WRITE_LOCK:
                    df_now = load_earnings_history()
                    if df_now is None or df_now.empty:
                        QMessageBox.information(
                            self, "Auto-fix",
                            "earnings_history.parquet is now empty — "
                            "nothing to fix.",
                        )
                        return
                    fixed, msgs = fix_integrity_issues(df_now, findings)
                    save_earnings_history(fixed, sort=True)
                self.log_panel.write_line(
                    f"Integrity auto-fix applied "
                    f"({len(df_now) - len(fixed)} rows removed):"
                )
                for m in msgs:
                    self.log_panel.write_line(f"  {m}")
                dlg.close()
                QMessageBox.information(
                    self, "Auto-fix complete",
                    f"earnings_history.parquet rewritten. Net row "
                    f"change: {len(df_now):,} → {len(fixed):,}.",
                )

            btn_fix.clicked.connect(_do_fix)
            btn_row.addWidget(btn_fix)
        btn_row.addStretch()
        btn_close = QPushButton("Close")
        btn_close.clicked.connect(dlg.close)
        btn_row.addWidget(btn_close)
        layout.addLayout(btn_row)

        dlg.show()
        self._integrity_dialog = dlg

    def _on_zacks_auto_refresh_toggled(self, enabled: bool):
        """Menu: toggle whether the daily Zacks smart refresh runs at
        launch. Updates the in-memory ref so the next launch picks up
        the new value (config.py constant is unaffected — this is a
        runtime override) AND persists to QSettings so the choice
        survives an exe restart."""
        self._zacks_auto_refresh_ref[0] = enabled
        self._qsettings().setValue("menu/zacks_auto_refresh", bool(enabled))
        state = "ENABLED" if enabled else "DISABLED"
        self.log_panel.write_line(
            f"Zacks auto-refresh at launch: {state} (saved)."
        )

    def _on_backoff_toggled(self, enabled: bool):
        """Menu: toggle the rate-limit backoff. Live update of the
        mutable ref (running workers see the change instantly) AND
        persisted via QSettings so it survives across launches."""
        self._backoff_enabled_ref[0] = bool(enabled)
        self._qsettings().setValue("menu/backoff_enabled", bool(enabled))
        state = "ENABLED" if enabled else "DISABLED"
        self.log_panel.write_line(
            f"Rate-limit backoff: {state} (saved)."
        )

    # ──────────────────────────────────────────────────────────────────
    # Nasdaq daily auto-refresh (Phase 5 of Finnhub augmentation)
    # ──────────────────────────────────────────────────────────────────

    # Nasdaq-sweep cadence logic lives in EarningsRefreshCoordinator;
    # the key is aliased here because tests read it off the window.
    _NASDAQ_LAST_RUN_KEY = EarningsRefreshCoordinator._NASDAQ_LAST_RUN_KEY
    _OHLCV_LAST_RUN_KEY = "menu/last_ohlcv_run_iso"

    def _is_ohlcv_due(self) -> bool:
        """True iff the cached OHLCV needs a launch-time update — i.e. the
        last completed update ran BEFORE the most recent market close (or
        never ran). Logs the last-run / last-close it reasoned about so
        the decision is visible in the log panel.

        Errs on the side of updating: any read/parse problem returns True.
        """
        close = config.last_market_close()
        try:
            last_iso = self._qsettings().value(self._OHLCV_LAST_RUN_KEY)
        except Exception as exc:
            log.debug("Could not read last_ohlcv_run_iso: %s", exc)
            self.log_panel.write_line(
                "OHLCV: no record of a prior update — running."
            )
            return True
        if not last_iso:
            self.log_panel.write_line(
                "OHLCV: no record of a prior update — running."
            )
            return True
        try:
            last = datetime.fromisoformat(str(last_iso))
        except ValueError:
            log.debug("Bad last_ohlcv_run_iso value: %r", last_iso)
            return True

        # Compare instants. `last` is tz-aware (we stamp aware); `close` is
        # tz-aware ET. If `last` is somehow naive (older builds), assume it
        # is local wall-clock and compare against the close in local naive.
        try:
            if last.tzinfo is None:
                close_local = close.astimezone().replace(tzinfo=None)
                due = last < close_local
            else:
                due = last < close
        except (TypeError, ValueError):
            return True

        verb = "running" if due else "current — skipping update"
        self.log_panel.write_line(
            f"OHLCV: last completed update {last.isoformat(timespec='minutes')}; "
            f"last market close {close.isoformat(timespec='minutes')} → {verb}."
        )
        return due

    def _stamp_ohlcv_run_now(self) -> None:
        """Record the current time as the most recent COMPLETED OHLCV
        update. Stamped as a tz-aware ISO timestamp so the due-check can
        compare instants against the market-close time unambiguously."""
        try:
            self._qsettings().setValue(
                self._OHLCV_LAST_RUN_KEY,
                datetime.now().astimezone().isoformat(timespec="seconds"),
            )
        except Exception as exc:
            log.debug("Could not write last_ohlcv_run_iso: %s", exc)

    def _on_nasdaq_auto_refresh_toggled(self, enabled: bool):
        """Toggle the daily Nasdaq calendar auto-refresh."""
        self._nasdaq_auto_refresh_ref[0] = bool(enabled)
        self._qsettings().setValue(
            "menu/nasdaq_auto_refresh", bool(enabled),
        )
        state = "ENABLED" if enabled else "DISABLED"
        self.log_panel.write_line(
            f"Nasdaq daily auto-refresh: {state} (saved)."
        )

    def _is_nasdaq_refresh_due(self) -> bool:
        return self._earn_coord._is_nasdaq_refresh_due()

    def _stamp_nasdaq_run_now(self) -> None:
        self._earn_coord._stamp_nasdaq_run_now()

    def _kick_off_nasdaq_auto_refresh(self) -> bool:
        return self._earn_coord._kick_off_nasdaq_auto_refresh()

    @pyqtSlot(int, int)
    def _on_nasdaq_auto_refresh_done(self, filled: int, errors: int):
        self._earn_coord._on_nasdaq_auto_refresh_done(filled, errors)

    # ──────────────────────────────────────────────────────────────────
    # Match-color tolerance — settings menu + persistence
    # ──────────────────────────────────────────────────────────────────

    MATCH_COLOR_TOLERANCE_MIN = 0
    MATCH_COLOR_TOLERANCE_MAX = 7
    MATCH_COLOR_TOLERANCE_DEFAULT = 1

    def _load_match_color_tolerance_pref(self) -> int:
        """Read saved match-color tolerance from QSettings, clamped to
        [MIN, MAX]. Defaults to MATCH_COLOR_TOLERANCE_DEFAULT (1)."""
        try:
            v = self._qsettings().value("match_color/tolerance_days")
            if v is None:
                return self.MATCH_COLOR_TOLERANCE_DEFAULT
            n = int(v)
        except Exception:
            return self.MATCH_COLOR_TOLERANCE_DEFAULT
        return max(
            self.MATCH_COLOR_TOLERANCE_MIN,
            min(self.MATCH_COLOR_TOLERANCE_MAX, n),
        )

    def _save_match_color_tolerance_pref(self, n: int) -> None:
        """Persist the match-color tolerance to QSettings."""
        self._qsettings().setValue("match_color/tolerance_days", int(n))

    def _open_match_color_tolerance_settings(self) -> None:
        """Show a small modal dialog to edit the tolerance. The new
        value persists immediately and is applied to the NEXT scan
        (already-rendered results aren't re-coloured — they were
        computed with the prior tolerance)."""
        dlg = QDialog(self)
        dlg.setWindowTitle("Color Match Tolerance")
        dlg.setModal(True)
        dlg.setMinimumWidth(440)

        layout = QVBoxLayout(dlg)
        layout.setContentsMargins(20, 18, 20, 18)
        layout.setSpacing(12)

        intro = QLabel(
            "Tolerance in CALENDAR DAYS for matching an indicator "
            "date against an earnings report date in the results "
            "table color pairing.\n\n"
            "  0 = exact match only (legacy)\n"
            "  1 = ±1 day (default — covers after-hours report vs "
            "next-day price reaction)\n"
            "  2–7 = wider fuzzy windows for deliberately loose "
            "pairing\n\n"
            "Affects ONLY the visual color pairing on the results "
            "table — does not change any funnel filter, ticker count, "
            "or earnings statistic. Applied on the NEXT scan."
        )
        intro.setWordWrap(True)
        intro.setStyleSheet("color: #aaa; font-size: 11px;")
        layout.addWidget(intro)

        row = QHBoxLayout()
        row.addWidget(QLabel("Tolerance (days):"))
        spin = QSpinBox()
        spin.setRange(
            self.MATCH_COLOR_TOLERANCE_MIN,
            self.MATCH_COLOR_TOLERANCE_MAX,
        )
        spin.setValue(int(self._match_color_tolerance))
        spin.setMinimumWidth(80)
        row.addWidget(spin)
        row.addStretch()
        layout.addLayout(row)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(dlg.accept)
        buttons.rejected.connect(dlg.reject)
        layout.addWidget(buttons)

        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        new_val = int(spin.value())
        self._match_color_tolerance = new_val
        self._save_match_color_tolerance_pref(new_val)
        self.log_panel.write_line(
            f"Color match tolerance updated: ±{new_val} day(s) "
            "(applies to next scan)."
        )

    def _set_sec_contact_email_dialog(self) -> None:
        """Prompt for the SEC EDGAR contact email and persist it to
        scanner_data/sec_contact.txt. Reachable from Settings → Set SEC
        Contact Email…. SEC's fair-access policy requires a real contact
        email in the request User-Agent for the ticker-universe download;
        until one is set the SEC universe source is skipped."""
        current = config.get_sec_contact_email()
        is_set = config.sec_contact_is_configured()
        blurb = (
            f"Currently set to: {current}"
            if is_set
            else "Not currently set — the SEC universe source is skipped."
        )
        text, ok = QInputDialog.getText(
            self,
            "Set SEC Contact Email",
            (
                "Enter a contact email for SEC EDGAR requests.\n\n"
                "SEC's fair-access policy requires a real email in the "
                "request User-Agent for the ticker-universe download — a "
                "generic User-Agent is rejected with 403 Forbidden.\n\n"
                + blurb + "\n\n"
                "Stored locally in scanner_data/sec_contact.txt. "
                "Leave blank to clear."
            ),
            QLineEdit.EchoMode.Normal,
            current if is_set else "",
        )
        if not ok:
            return
        text = (text or "").strip()
        if not text:
            if config.set_sec_contact_email(""):
                self.log_panel.write_line(
                    "SEC contact email cleared — the SEC universe source "
                    "will be skipped on the next universe refresh."
                )
            return
        if "@" not in text:
            QMessageBox.warning(
                self, "Invalid Email",
                "That doesn't look like an email address (no '@'). "
                "Nothing was saved.",
            )
            return
        if config.set_sec_contact_email(text):
            self.log_panel.write_line(
                f"SEC contact email set to {text} — the SEC universe "
                "source is now enabled."
            )
        else:
            QMessageBox.warning(
                self, "Write Error",
                "Could not write scanner_data/sec_contact.txt.",
            )

    def _show_advanced_settings(self) -> None:
        """Settings → Advanced… — edit the user-configurable tunables:
        OHLCV cache depth, earnings-history depth, and the reference/
        benchmark ticker list. OK validates, persists via
        config.save_user_config() (scanner_data/user_config.json), and
        applies the values to the live config module immediately — no
        restart. Cancel changes nothing."""
        dlg = QDialog(self)
        dlg.setWindowTitle("Advanced Settings")
        dlg.setModal(True)
        dlg.setMinimumWidth(460)
        layout = QVBoxLayout(dlg)

        # OHLCV history depth
        row1 = QHBoxLayout()
        lbl1 = QLabel("OHLCV history depth (years):")
        lbl1.setToolTip(
            "How many years of daily bars a full OHLCV download pulls "
            "per ticker."
        )
        row1.addWidget(lbl1)
        spin_ohlcv = QSpinBox()
        spin_ohlcv.setRange(
            *config.USER_CONFIG_INT_RANGES["OHLCV_HISTORY_YEARS"]
        )
        spin_ohlcv.setValue(int(config.OHLCV_HISTORY_YEARS))
        spin_ohlcv.setMinimumWidth(80)
        row1.addWidget(spin_ohlcv)
        layout.addLayout(row1)

        # Depth changes never rewrite the existing per-ticker parquet
        # cache — only future full downloads use the new window.
        note = QLabel(
            "Note: the OHLCV depth applies to NEW downloads only — "
            "already-cached tickers keep their current depth until "
            "re-downloaded or rebuilt."
        )
        note.setWordWrap(True)
        note.setStyleSheet("color: #888888; font-size: 11px;")
        layout.addWidget(note)

        # Earnings history depth
        row2 = QHBoxLayout()
        lbl2 = QLabel("Earnings history depth (years):")
        lbl2.setToolTip(
            "Hard cap on per-quarter earnings history for all three "
            "fill sources (Finviz / Zacks / Finnhub). Rows older than "
            "this are pruned at save time."
        )
        row2.addWidget(lbl2)
        spin_earn = QSpinBox()
        spin_earn.setRange(
            *config.USER_CONFIG_INT_RANGES["EARNINGS_HISTORY_YEARS"]
        )
        spin_earn.setValue(int(config.EARNINGS_HISTORY_YEARS))
        spin_earn.setMinimumWidth(80)
        row2.addWidget(spin_earn)
        layout.addLayout(row2)

        # Reference / benchmark tickers
        lbl3 = QLabel(
            "Reference / benchmark tickers (comma-separated). Always "
            "kept in the OHLCV cache and used for the RS calculations; "
            "never appear in scan results."
        )
        lbl3.setWordWrap(True)
        layout.addWidget(lbl3)
        txt = QTextEdit()
        txt.setPlaceholderText("e.g. SPY, ONEQ, XLK")
        txt.setPlainText(", ".join(config.REFERENCE_TICKERS))
        txt.setMinimumHeight(80)
        layout.addWidget(txt)

        # OK / Cancel
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_ok = QPushButton("OK")
        btn_ok.clicked.connect(dlg.accept)
        btn_row.addWidget(btn_ok)
        btn_cancel = QPushButton("Cancel")
        btn_cancel.clicked.connect(dlg.reject)
        btn_row.addWidget(btn_cancel)
        layout.addLayout(btn_row)

        if dlg.exec() != QDialog.DialogCode.Accepted:
            return

        # Pre-validate the ticker list here so a typo warns BY NAME —
        # save_user_config() would drop the whole invalid list and
        # silently revert to the baked-in defaults.
        tickers = [
            t.strip().upper()
            for t in txt.toPlainText().replace("\n", ",").split(",")
            if t.strip()
        ]
        bad = [
            t for t in tickers if not config.PLAUSIBLE_TICKER_RE.match(t)
        ]
        if not tickers or bad:
            QMessageBox.warning(
                self, "Invalid Reference Tickers",
                (
                    "These entries don't look like tickers: "
                    f"{', '.join(bad)}. "
                    if bad
                    else "The reference ticker list cannot be empty. "
                )
                + "Nothing was saved.",
            )
            return

        if not config.save_user_config({
            "OHLCV_HISTORY_YEARS": spin_ohlcv.value(),
            "EARNINGS_HISTORY_YEARS": spin_earn.value(),
            "REFERENCE_TICKERS": tickers,
        }):
            QMessageBox.warning(
                self, "Write Error",
                "Could not write scanner_data/user_config.json.",
            )
            return
        self.log_panel.write_line(
            "Advanced settings saved: OHLCV depth="
            f"{config.OHLCV_HISTORY_YEARS}y, earnings depth="
            f"{config.EARNINGS_HISTORY_YEARS}y, "
            f"{len(config.REFERENCE_TICKERS)} reference ticker(s). "
            "OHLCV depth applies to new downloads only."
        )

    def _load_menu_toggles_pref(self) -> None:
        """Read persisted menu-toggle values from QSettings into the
        in-memory refs that drive the QAction.checked state. Called
        from __init__ BEFORE `_build_ui` so the menu paints with the
        saved state instead of the hardcoded config default.

        Tolerates missing keys (first-run / fresh install) by falling
        back to the same defaults the app used previously:
            - Zacks auto-refresh: `config.ZACKS_AUTO_REFRESH_ENABLED`
            - Backoff:            True
        """
        try:
            s = self._qsettings()
            zacks_v = s.value("menu/zacks_auto_refresh")
            backoff_v = s.value("menu/backoff_enabled")
            nasdaq_v = s.value("menu/nasdaq_auto_refresh")
            if zacks_v is not None:
                self._zacks_auto_refresh_ref[0] = self._coerce_qsettings_bool(
                    zacks_v, default=config.ZACKS_AUTO_REFRESH_ENABLED,
                )
            self._backoff_enabled_ref = [
                self._coerce_qsettings_bool(backoff_v, default=True)
                if backoff_v is not None else True
            ]
            if nasdaq_v is not None:
                self._nasdaq_auto_refresh_ref[0] = self._coerce_qsettings_bool(
                    nasdaq_v, default=config.NASDAQ_AUTO_REFRESH_ENABLED,
                )
        except Exception as exc:
            log.debug("Could not load menu-toggle prefs: %s", exc)
            # Fall back to the historical defaults so the menu still
            # works even if QSettings is corrupt.
            if not hasattr(self, "_backoff_enabled_ref"):
                self._backoff_enabled_ref = [True]
            if not hasattr(self, "_nasdaq_auto_refresh_ref"):
                self._nasdaq_auto_refresh_ref = [config.NASDAQ_AUTO_REFRESH_ENABLED]

    @staticmethod
    def _coerce_qsettings_bool(value, *, default: bool) -> bool:
        """QSettings on Windows round-trips bools as the strings
        'true' / 'false' (or sometimes the raw bool). Coerce both
        forms without crashing on unexpected types."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in ("true", "1", "yes", "on")
        if isinstance(value, (int, float)):
            return bool(value)
        return default

    def _show_coverage_gaps(self):
        """Show tickers missing OHLCV, sector, or earnings data."""
        # Two dots — sector_map / earnings_cache live in trade_scanner_fh/,
        # one level above this gui/ subpackage. Single-dot imports here
        # were a leftover from before the Phase 6 gui split and crashed
        # the dialog on click.
        from ..sector_map import load_sector_map
        from ..earnings_cache import load_earnings_cache

        try:
            universe = load_universe()
        except FileNotFoundError:
            QMessageBox.warning(self, "No Universe",
                                "No universe file found. Download universe first.")
            return

        all_syms = sorted(
            s for s in universe["symbol"].tolist()
            if isinstance(s, str) and s.strip()
        )

        # OHLCV gaps: no parquet file at all
        ohlcv_missing = [
            s for s in all_syms
            if not (config.PARQUET_DIR / f"{s}.parquet").exists()
        ]

        # Sector gaps
        sector_df = load_sector_map()
        if sector_df is not None and not sector_df.empty:
            mapped = set(sector_df["ticker"].tolist())
            sector_missing = [s for s in all_syms if s not in mapped]
        else:
            sector_missing = list(all_syms)

        # Earnings gaps
        earn_df = load_earnings_cache()
        if earn_df is not None and not earn_df.empty:
            mapped = set(earn_df["ticker"].tolist())
            earnings_missing = [s for s in all_syms if s not in mapped]
        else:
            earnings_missing = list(all_syms)

        # Build dialog
        dlg = QDialog(self)
        dlg.setWindowTitle("Data Coverage Gaps")
        dlg.resize(520, 500)
        layout = QVBoxLayout(dlg)

        summary = QLabel(
            f"Universe: {len(all_syms)} tickers\n"
            f"Missing OHLCV: {len(ohlcv_missing)}  |  "
            f"Missing Sector: {len(sector_missing)}  |  "
            f"Missing Earnings: {len(earnings_missing)}"
        )
        summary.setStyleSheet("font-weight: bold; padding: 4px;")
        layout.addWidget(summary)

        text = QTextEdit()
        text.setReadOnly(True)
        text.setFont(QFont("Consolas", 9))

        lines = []
        if ohlcv_missing:
            lines.append(f"=== MISSING OHLCV ({len(ohlcv_missing)}) ===")
            lines.append(", ".join(ohlcv_missing))
            lines.append("")
        if sector_missing:
            lines.append(f"=== MISSING SECTOR ({len(sector_missing)}) ===")
            lines.append(", ".join(sector_missing))
            lines.append("")
        if earnings_missing:
            lines.append(f"=== MISSING EARNINGS ({len(earnings_missing)}) ===")
            lines.append(", ".join(earnings_missing))
            lines.append("")
        if not lines:
            lines.append("All tickers have complete data coverage.")

        text.setPlainText("\n".join(lines))
        layout.addWidget(text)

        btn_close = QPushButton("Close")
        btn_close.clicked.connect(dlg.accept)
        layout.addWidget(btn_close)

        dlg.exec()

    # ── Universe filters ─────────────────────────────────────────────────

    def _on_ipo_toggle(self, state):
        self.spin_ipo_days.setEnabled(state == Qt.CheckState.Checked.value)

    def _filtered_symbols(self) -> list[str]:
        """Apply universe pre-filters (ETF, ADR, IPO, greylist) to
        self._symbols. Greylist is a scan-only filter — it does NOT affect
        OHLCV/sector/earnings refresh, only what reaches the scan pipeline."""
        ref_set = set(config.REFERENCE_TICKERS)
        syms = [s for s in self._symbols if s not in ref_set]

        # Greylist filter (scan-only)
        if self._greylist:
            before = len(syms)
            syms = [s for s in syms if s not in self._greylist]
            removed = before - len(syms)
            if removed:
                log.info("Greylist filter: omitted %d tickers from this scan",
                         removed)

        if self._universe_df is not None:
            df = self._universe_df

            # ETF filter
            if not self.chk_include_etf.isChecked():
                etf_syms = set(
                    df.loc[df["etf"].fillna(False).astype(bool), "symbol"]
                )
                syms = [s for s in syms if s not in etf_syms]

            # ADR filter
            if not self.chk_include_adr.isChecked():
                adr_syms = set(
                    df.loc[df["adr"].fillna(False).astype(bool), "symbol"]
                )
                syms = [s for s in syms if s not in adr_syms]

        # IPO mode: only keep tickers with <= N days of data.
        # Phase 3 I5: cache row counts keyed by mtime so only newly-updated
        # parquets trigger a metadata re-read on repeat IPO-mode scans.
        if self.chk_ipo_mode.isChecked():
            max_days = self.spin_ipo_days.value()
            ipo_syms = []
            for s in syms:
                rc = self._get_row_count_cached(s)
                if rc is not None and rc <= max_days:
                    ipo_syms.append(s)
            syms = ipo_syms

        return syms

    def _get_row_count_cached(self, symbol: str) -> Optional[int]:
        """Return the row count of a ticker's parquet file, using an
        mtime-keyed cache so unchanged parquets don't re-read metadata."""
        pq = config.PARQUET_DIR / f"{symbol}.parquet"
        try:
            st = pq.stat()
        except OSError:
            return None
        mtime_ns = st.st_mtime_ns
        cached = self._ipo_row_count_cache.get(symbol)
        if cached is not None and cached[0] == mtime_ns:
            return cached[1]
        try:
            row_count = pq_reader.read_metadata(str(pq)).num_rows
        except Exception:
            return None
        self._ipo_row_count_cache[symbol] = (mtime_ns, row_count)
        return row_count

    # ── Quick date range ─────────────────────────────────────────────────

    def _latest_data_date(self):
        """Most-recent available OHLCV bar date across the cache — the
        'latest trading day' the quick-range buttons + launch default
        anchor End to, so '1D' means the last trading day rather than a
        stale End field. Sampled (all actively-traded tickers share the
        trading calendar, so the max over a small strided sample is the
        latest available date) and cached; falls back to the market
        calendar when no OHLCV is cached yet. Cache is cleared after each
        OHLCV update (see _on_update_done)."""
        cached = getattr(self, "_latest_data_date_cache", None)
        if cached is not None:
            return cached
        best = None
        syms = getattr(self, "_symbols", None) or []
        if syms:
            # Strided sample so we hit liquid names (reliably up to date),
            # not just the alphabetically-first thin/delisted tickers.
            step = max(1, len(syms) // 30)
            for s in syms[::step][:30]:
                try:
                    df = load_ohlcv(s)
                except Exception:
                    continue
                if df is not None and len(df):
                    try:
                        d = pd.Timestamp(df.index.max()).date()
                    except Exception:
                        continue
                    if best is None or d > best:
                        best = d
        if best is None:
            try:
                best = config.last_market_close().date()
            except Exception:
                best = date.today()
        self._latest_data_date_cache = best
        return best

    def _window_start(self, end: date, days: int) -> date:
        """Start date for a `days`-calendar-day lookback ending on `end`,
        floored to the most recent trading session. A raw calendar-day start
        collapses to a single-bar window for short lookbacks on a Monday or
        post-holiday End (e.g. 1D: End=Mon → Sun 06-07; Fri 06-05 is excluded),
        which scanner's `len(window) < 2` guard rejects for EVERY ticker — an
        empty scan. Flooring to a real trading day (weekend- AND holiday-aware)
        guarantees the window spans >= 2 trading bars. No-op for 1W+ unless the
        start itself lands on a non-trading day."""
        return config.most_recent_trading_day(end - timedelta(days=days))

    def _set_quick_range(self, days: int):
        """Anchor the window to the latest available trading day: set End
        to the latest data date and Start to the trading session `days`
        calendar days back (see _window_start). So '1D' = the prior trading
        session — NOT the prior *calendar* day, which on a Monday End is a
        Sunday and would leave a single-bar window. The manual date pickers
        + Custom Range remain for explicit historical scans."""
        latest = self._latest_data_date()
        self.date_end.setDate(QDate(latest.year, latest.month, latest.day))
        start = self._window_start(latest, days)
        self.date_start.setDate(QDate(start.year, start.month, start.day))

    # ── Search / filter ─────────────────────────────────────────────────

    def _on_search_changed(self, text: str):
        """Filter results table by ticker symbol (case-insensitive)."""
        self.results_table.proxy.setFilterKeyColumn(0)  # Ticker column
        self.results_table.proxy.setFilterCaseSensitivity(
            Qt.CaseSensitivity.CaseInsensitive
        )
        self.results_table.proxy.setFilterFixedString(text)

    # ── Scan execution ─────────────────────────────────────────────────

    def _run_scan(self):
        if not self._symbols:
            QMessageBox.warning(self, "No Data",
                                "No cached OHLCV data yet. Wait for the background "
                                "update to finish, or check your internet connection.")
            return

        filtered = self._filtered_symbols()
        if not filtered:
            QMessageBox.warning(self, "No Tickers",
                                "No tickers remain after applying universe filters "
                                "(ETF/ADR/IPO). Adjust your filters and try again.")
            return

        # Session deduplication — exclude tickers seen in prior scans
        if self.chk_omit_seen.isChecked() and self._session_seen:
            before = len(filtered)
            filtered = [s for s in filtered if s not in self._session_seen]
            omitted = before - len(filtered)
            self.log_panel.write_line(
                f"Session filter: omitted {omitted} previously seen tickers "
                f"({len(filtered)} remaining)"
            )
            if not filtered:
                QMessageBox.information(
                    self, "All Omitted",
                    f"All {before} tickers were already seen this session.\n"
                    "Uncheck 'Omit previously scanned tickers' or click "
                    "'Reset Session' to scan them again.")
                return

        # Build labeled timeframe list — Sequenced Run takes priority and
        # produces backward-walking chunks; otherwise normal multi-timeframe
        # checkbox behavior applies. Each entry is (label, start, end).
        sequenced = (
            self.chk_sequenced_run.isChecked()
            and self._sequenced_cfg is not None
        )
        labeled_tfs: list[tuple[str, date, date]] = []
        if sequenced:
            seq_start, seq_end, chunk_n, unit = self._sequenced_cfg
            chunks = chunk_periods(seq_start, seq_end, chunk_n, unit)
            if not chunks:
                QMessageBox.warning(
                    self, "Sequenced Run",
                    "Sequenced Run produced no chunks. Check the date "
                    "range and chunk size, then try again.",
                )
                return
            for s, e in chunks:
                labeled_tfs.append((f"{s.isoformat()} → {e.isoformat()}", s, e))
        else:
            qd_end = self.date_end.date()
            end = date(qd_end.year(), qd_end.month(), qd_end.day())

            # Safety net: warn when the End date is behind the latest
            # available data. The scanner shows earnings AS-OF End
            # (report_date <= end_ts), so a stale End silently hides
            # freshly-reported quarters. Surfaces the trap for scans that
            # didn't go through a quick-range button (e.g. manual End).
            try:
                _latest = self._latest_data_date()
                if end < _latest:
                    self.log_panel.write_line(
                        f"⚠ End date {end} is behind the latest available "
                        f"data ({_latest}) — scanning AS-OF a past date, so "
                        f"recently-reported quarters are excluded. Click a "
                        f"timeframe button (1D/1W/…) to jump to the latest."
                    )
            except Exception as exc:
                # A failure here only skips the stale-End-date warning —
                # the scan itself still proceeds with the chosen End.
                log.warning(
                    "stale End-date pre-scan check failed "
                    "(warning skipped; scan proceeds): %s", exc,
                )

            # Force smallest→longest ordering so the most proximal
            # timeframe always runs (and displays) first. Sorting
            # explicitly guards against any future reordering of
            # tf_checks insertion. Custom range is always appended
            # last so its hits don't pre-empt the standard periods
            # when the intra-run omit filter is active.
            day_to_label = {1: "1D", 7: "1W", 30: "1M", 90: "3M", 180: "6M"}
            checked_days = sorted(
                days for days, chk in self.tf_checks.items() if chk.isChecked()
            )
            for days in checked_days:
                s = self._window_start(end, days)
                labeled_tfs.append((day_to_label.get(days, f"{days}D"), s, end))
            if self.chk_custom_range.isChecked():
                qd_start = self.date_start.date()
                s = date(qd_start.year(), qd_start.month(), qd_start.day())
                labeled_tfs.append((f"Custom {s} → {end}", s, end))

            # No checkboxes → single scan using date pickers (backward compatible)
            if not labeled_tfs:
                qd_start = self.date_start.date()
                s = date(qd_start.year(), qd_start.month(), qd_start.day())
                labeled_tfs.append((f"{s} → {end}", s, end))

        # Pass the two global earnings toolbar toggles into every
        # period's ScanParams. Replaces the prior per-filter
        # include_no_data flags. Dates / data are independent.
        earnings_dates_only = self.chk_earnings_dates_only.isChecked()
        earnings_data_only = self.chk_earnings_data_only.isChecked()
        params_list = [
            (label, self.indicator_panel.build_scan_params(
                s, e,
                earnings_dates_only=earnings_dates_only,
                earnings_data_only=earnings_data_only,
                match_color_tolerance_days=self._match_color_tolerance,
            ))
            for label, s, e in labeled_tfs
        ]

        tf_desc = ", ".join(label for label, _s, _e in labeled_tfs)
        prefix = "Sequenced Run: " if sequenced else ""
        self.log_panel.write_line(
            f"{prefix}Scanning {len(filtered)} tickers across "
            f"{len(labeled_tfs)} timeframe(s): {tf_desc} "
            f"(filtered from {len(self._symbols)} cached)"
        )

        self.btn_scan.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.btn_send.setEnabled(False)
        self.btn_export.setEnabled(False)
        self.btn_excel.setEnabled(False)
        self.summary_label.setText("")
        self.status.showMessage("Scanning...")

        self._worker = self._start_worker(
            ScanWorker(
                filtered, params_list,
                sequenced=sequenced,
                omit_intra_run=self.chk_omit_intra_run.isChecked(),
            ),
            progress=self._on_scan_progress,
            finished=self._on_scan_done,
        )

    def _stop_scan(self):
        if self._worker:
            self._worker.request_stop()
            self.log_panel.write_line("Stop requested...")

    def _reset_session(self):
        self._session_seen.clear()
        self._session_scan_count = 0
        self.session_label.setText("Session: 0 scans, 0 tickers seen")
        self.log_panel.write_line("Session reset — previously seen tickers cleared")

    @pyqtSlot(int, int, str)
    def _on_scan_progress(self, done: int, total: int, sym: str):
        pct = done * 100 // total if total else 0
        self.status.showMessage(f"Computing: {done}/{total} ({pct}%)")

    @pyqtSlot(object)
    def _on_scan_done(self, result):
        """Handle WorkerScanResult: stash per-period DataFrames, populate
        the timeframe dropdown, and display the first period in the table.

        Wrapped in an outer try/except: the slot runs on the main GUI
        thread, and an unhandled exception here propagates to Qt's
        slot dispatcher. Under PyInstaller windowed mode on Windows
        that aborts the whole exe rather than just logging the failure.
        Convert any escaped exception to a user-visible "scan crashed"
        summary so the GUI stays alive.
        """
        try:
            self._on_scan_done_impl(result)
        except Exception as exc:
            log.error("_on_scan_done crashed: %s", exc, exc_info=True)
            self.btn_scan.setEnabled(True)
            self.btn_stop.setEnabled(False)
            self._worker = None
            try:
                self.log_panel.write_line(
                    f"── Scan post-processing crashed: {exc} — "
                    "results may be partial. See log for traceback. ──"
                )
                import traceback as _tb
                self.log_panel.write_line(_tb.format_exc())
            except Exception as panel_exc:
                log.debug(
                    "crash-summary write to log panel failed: %s", panel_exc,
                )
            try:
                self.summary_label.setText("  Scan crashed — see log  ")
                self.summary_label.setStyleSheet(
                    "font-size: 13px; padding: 6px; background: #c0392b; "
                    "color: white; border-radius: 4px; font-weight: bold;"
                )
            except Exception as label_exc:
                log.debug(
                    "crash-summary label update failed: %s", label_exc,
                )

    def _on_scan_done_impl(self, result):
        """Original body of _on_scan_done — split out so the wrapping
        try/except in the slot can catch any escaped exception."""
        self.btn_scan.setEnabled(True)
        self.btn_stop.setEnabled(False)

        # Cache per-period results — used by the dropdown + Excel export
        self._period_results = dict(getattr(result, "period_results", {}))
        self._period_order = list(getattr(result, "period_order", []))
        # Fresh scan invalidates any pending cut/paste clipboard from
        # the prior result set — those symbols may not exist in the
        # new results.
        try:
            self.results_table.clear_cut_clipboard()
        except Exception as exc:
            log.debug("cut-clipboard clear after scan failed: %s", exc)
        # Reconcile the saved column order against the new canonical
        # set: NEW filter/display columns (variables added since last
        # scan) get prepended to the user's saved layout in indicator-
        # panel order; columns whose source variable was disabled drop
        # out. Empty saved order means canonical wins. Columns dialog
        # visibility (`_deleted_column_keys`) is preserved across scans
        # so users don't have to re-hide the same noisy columns each
        # run; right-click → Reset Column Order or the popup's Reset
        # to Default button restores everything.
        try:
            from .widgets import _build_dynamic_columns
            interleave = self.results_table.interleave_quarters
            primary_label = (
                self._period_order[0] if self._period_order else None
            )
            primary_df = (
                self._period_results.get(primary_label)
                if primary_label else None
            )
            cols_for_reconcile, _, _ = _build_dynamic_columns(
                primary_df, interleave_quarters=interleave,
            )
            self._reconcile_column_order_for_scan(
                [k for _h, k, _f in cols_for_reconcile]
            )
        except Exception as exc:
            log.debug("column-order reconcile after scan failed: %s", exc)

        # Populate the timeframe dropdown. Block signals so the populate
        # below isn't fired by the auto-emitted currentIndexChanged.
        self.combo_timeframe.blockSignals(True)
        self.combo_timeframe.clear()
        for label in self._period_order:
            df = self._period_results.get(label)
            n = 0 if df is None else len(df)
            self.combo_timeframe.addItem(f"{label}  —  {n} results", userData=label)
        self.combo_timeframe.setEnabled(len(self._period_order) > 1)
        self.combo_timeframe.blockSignals(False)

        # Activate the first period (most recent / highest priority)
        if self._period_order:
            self._active_period = self._period_order[0]
            raw = self._period_results.get(self._active_period, pd.DataFrame())
        else:
            self._active_period = None
            raw = pd.DataFrame()

        # Apply view filters so the post-scan table matches what
        # the user expects given the current view-filter toggle state.
        df = self._apply_view_filters(raw)

        # Re-apply the user's saved column order BEFORE populate so
        # the table can move sections after rebuilding the model.
        self.results_table.set_saved_column_order(self._results_column_order)
        self.results_table.populate(df)
        self._last_results_df = df
        # Sync the (possibly open) Columns dropdown so it reflects the
        # post-reconcile order + the surviving hidden set.
        self._sync_columns_dialog()

        n_pass = len(df) if df is not None else 0
        n_err = len(getattr(result, "errors", []))
        total_unique = (
            len(result.total_unique_symbols())
            if hasattr(result, "total_unique_symbols") else n_pass
        )

        self.btn_send.setEnabled(n_pass > 0)
        self.btn_export.setEnabled(n_pass > 0)
        self.btn_excel.setEnabled(n_pass > 0)

        # Pre-transfer summary
        if n_err == 0:
            if len(self._period_order) > 1:
                summary = (f"{n_pass} in {self._active_period} | "
                           f"{total_unique} unique across {len(self._period_order)} periods")
            else:
                summary = f"{n_pass} tickers passed all filters, 0 data warnings"
            color = "#4caf50"
        else:
            summary = f"{n_pass} tickers passed, {n_err} had errors -- review log"
            color = "#ff9800"

        self.summary_label.setText(f"  {summary}  ")
        self.summary_label.setStyleSheet(
            f"font-size: 13px; padding: 6px; background: {color}; "
            "color: white; border-radius: 4px; font-weight: bold;"
        )

        self.status.showMessage(
            f"Scan complete: {total_unique} unique results in "
            f"{getattr(result, 'elapsed_sec', 0):.1f}s"
        )

        self._worker = None

        # Track session history — union over ALL periods (so a ticker that
        # qualified in any period is counted once for session-dedup).
        if hasattr(result, "total_unique_symbols"):
            self._session_seen.update(result.total_unique_symbols())
        self._session_scan_count += 1
        self.session_label.setText(
            f"Session: {self._session_scan_count} scans, "
            f"{len(self._session_seen)} tickers seen"
        )

    # ── Export Results ────────────────────────────────────────────────

    @pyqtSlot(bool)
    def _on_beats_filter_toggled(self, any_active: bool):
        """Phase 7 §7.2: lock out Sequenced Run while Consecutive Beats
        is active (single-window scan required for streak semantics).

        Multi-timeframe checkboxes stay available — multi-timeframe
        windows all end on today's date so the streak data is identical
        across them, which is harmless if a bit redundant.

        When `any_active` flips on while Sequenced Run is already
        checked, force-untick it to keep state consistent."""
        if any_active:
            if self.chk_sequenced_run.isChecked():
                # Force-untick before disabling so the cleanup path in
                # _on_sequenced_run_toggled runs.
                self.chk_sequenced_run.setChecked(False)
            self.chk_sequenced_run.setEnabled(False)
            self.chk_sequenced_run.setToolTip(
                "Consecutive Beats requires a single-window scan. "
                "Uncheck Consecutive EPS Beats / Consecutive Rev Beats "
                "to re-enable Sequenced Run."
            )
        else:
            self.chk_sequenced_run.setEnabled(True)
            self.chk_sequenced_run.setToolTip(
                "Walk a date range backwards in fixed chunks (e.g., 2-month "
                "blocks). Each chunk runs as its own scan; results are "
                "tagged with their Period and merged into one table."
            )

    def _on_sequenced_run_toggled(self, checked: bool):
        """Sequenced Run is mutually exclusive with the per-timeframe
        checkboxes. When ticked, blank + disable the others; when unticked,
        re-enable them."""
        # Other timeframe checkboxes
        for chk in self.tf_checks.values():
            if checked:
                chk.setChecked(False)
            chk.setEnabled(not checked)
        if checked:
            self.chk_custom_range.setChecked(False)
        self.chk_custom_range.setEnabled(not checked)

        if checked:
            # Open the config dialog. If user cancels and we have no prior
            # config, untick the box so we don't run with stale state.
            ok = self._show_sequenced_run_config()
            if not ok and self._sequenced_cfg is None:
                # Block re-entry of the toggle handler while we revert
                self.chk_sequenced_run.blockSignals(True)
                self.chk_sequenced_run.setChecked(False)
                self.chk_sequenced_run.blockSignals(False)
                # Re-enable other widgets since we're effectively off
                for chk in self.tf_checks.values():
                    chk.setEnabled(True)
                self.chk_custom_range.setEnabled(True)
        else:
            self._sequenced_cfg = None
            self.log_panel.write_line("Sequenced Run disabled.")

    def _show_sequenced_run_config(self) -> bool:
        """Open the Sequenced Run config dialog, store the result on self.
        Returns True if accepted, False if cancelled."""
        # Sensible defaults: full prior calendar year, 2-month chunks
        today = date.today()
        default_end = (
            self._sequenced_cfg[1] if self._sequenced_cfg
            else date(today.year - 1, 12, 31)
        )
        default_start = (
            self._sequenced_cfg[0] if self._sequenced_cfg
            else date(today.year - 1, 1, 1)
        )
        default_n = self._sequenced_cfg[2] if self._sequenced_cfg else 2
        default_unit = self._sequenced_cfg[3] if self._sequenced_cfg else "months"

        dlg = SequencedRunDialog(
            default_start=default_start, default_end=default_end,
            default_n=default_n, default_unit=default_unit, parent=self,
        )
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return False

        cfg = dlg.get_config()
        if cfg[0] > cfg[1]:
            QMessageBox.warning(
                self, "Sequenced Run",
                "Start date must be on or before end date.",
            )
            return False

        self._sequenced_cfg = cfg
        chunks = chunk_periods(cfg[0], cfg[1], cfg[2], cfg[3])
        self.log_panel.write_line(
            f"Sequenced Run armed: {len(chunks)} chunks of "
            f"{cfg[2]} {cfg[3]} between {cfg[0]} and {cfg[1]}. "
            f"First: {chunks[0]} … last: {chunks[-1]}."
        )
        return True

    @pyqtSlot(int)
    def _on_timeframe_changed(self, idx: int):
        """User picked a different period from the dropdown — repaint the
        results table and re-target the Send/Export/Excel buttons to it.

        Re-entrancy guard: `ResultsTable.populate` yields to
        `QApplication.processEvents()` every 500 rows so the GUI stays
        responsive during big renders. That means a SECOND dropdown
        click fired during the first render's processEvents tick will
        re-enter this slot synchronously — its inner populate would
        call `setRowCount(0)` on the model the outer populate is still
        writing into, corrupting state and (with sequenced runs of
        100+ columns × thousands of rows) producing a stack-deep
        re-entry pile that registers as a hang to the user.

        Fix: drop any change that arrives while a switch is already
        in flight. After the in-flight render finishes, check whether
        the dropdown's CURRENT index differs from what we just
        rendered (the user may have clicked a different period during
        the render); if so, queue a follow-up via QTimer.singleShot
        so the latest selection wins WITHOUT synchronous recursion.
        """
        if getattr(self, "_timeframe_switch_in_flight", False):
            return
        if idx < 0:
            return
        label = self.combo_timeframe.itemData(idx)
        if not label or label not in self._period_results:
            return

        self._timeframe_switch_in_flight = True
        try:
            raw = self._period_results.get(label, pd.DataFrame())
            self._active_period = label
            # Apply view filters (Earnings Only / Color Match Only)
            # to the slice the user actually sees + exports. The
            # original (unfiltered) df stays in `_period_results` so
            # toggling a view-filter off restores all rows without
            # re-running the scan.
            df = self._apply_view_filters(raw)
            self._last_results_df = df
            # Saved column order applies across all periods; reapply here.
            self.results_table.set_saved_column_order(self._results_column_order)
            self.results_table.populate(df)
            n_pass = len(df) if df is not None else 0
            n_raw = len(raw) if raw is not None else 0
            self.btn_send.setEnabled(n_pass > 0)
            self.btn_export.setEnabled(n_pass > 0)
            self.btn_excel.setEnabled(n_pass > 0)
            if n_pass != n_raw:
                self.status.showMessage(
                    f"Viewing {label}: {n_pass} of {n_raw} results "
                    f"(view-filtered)"
                )
            else:
                self.status.showMessage(
                    f"Viewing {label}: {n_pass} results"
                )
        finally:
            self._timeframe_switch_in_flight = False
            # If the user picked a DIFFERENT period while the render
            # above was running, follow up with the latest selection.
            # QTimer.singleShot(0, ...) defers to the next event-loop
            # iteration so we never re-enter synchronously.
            try:
                latest = self.combo_timeframe.currentIndex()
                if latest != idx and latest >= 0:
                    from PyQt6.QtCore import QTimer
                    QTimer.singleShot(
                        0, lambda i=latest: self._on_timeframe_changed(i),
                    )
            except Exception as exc:
                log.debug("Timeframe follow-up dispatch failed: %s", exc)

    # ── Column layout management ─────────────────────────────────────
    # Column reorder / hide / persist logic (Columns ▾ dropdown wiring,
    # header-drag order, hidden-column set, scan-time reconcile) lives
    # in ColumnManager (columns.py). MainWindow keeps every historical
    # method name below as a thin delegate so signal wiring and tests
    # are untouched; the layout state itself (`_results_column_order`,
    # `_deleted_column_keys`, `_columns_dialog`) stays on the window.

    @property
    def _columns_mgr(self) -> ColumnManager:
        """Lazily-created column manager. Looked up via ``__dict__``
        (not getattr) because bare ``MainWindow.__new__`` test shells
        raise RuntimeError when attribute lookup falls through to the
        uninitialized Qt C++ side."""
        mgr = self.__dict__.get("_columns_mgr_obj")
        if mgr is None:
            mgr = ColumnManager(self)
            self.__dict__["_columns_mgr_obj"] = mgr
        return mgr

    @pyqtSlot(list)
    def _on_results_column_order_changed(self, keys: list):
        self._columns_mgr._on_results_column_order_changed(keys)

    @pyqtSlot(bool)
    def _on_interleave_quarters_toggled(self, checked: bool):
        self._columns_mgr._on_interleave_quarters_toggled(checked)

    # ── Columns dropdown wiring ──────────────────────────────────────

    def _current_columns_for_dialog(self) -> list[tuple]:
        return self._columns_mgr._current_columns_for_dialog()

    def _reorder_for_visual(self, active: list[tuple]) -> list[tuple]:
        return self._columns_mgr._reorder_for_visual(active)

    def _open_columns_dialog(self):
        self._columns_mgr._open_columns_dialog()

    @pyqtSlot(list, list)
    def _on_columns_dialog_updated(self, ordered_keys: list, hidden_keys: list):
        self._columns_mgr._on_columns_dialog_updated(ordered_keys, hidden_keys)

    @pyqtSlot()
    def _on_columns_dialog_reset(self):
        self._columns_mgr._on_columns_dialog_reset()

    def _reset_columns_to_default(self) -> None:
        self._columns_mgr._reset_columns_to_default()

    def _sync_columns_dialog(self) -> None:
        self._columns_mgr._sync_columns_dialog()

    def _reconcile_column_order_for_scan(
        self, canonical_keys: list[str],
    ) -> None:
        self._columns_mgr._reconcile_column_order_for_scan(canonical_keys)

    @pyqtSlot(list)
    def _on_rows_deletion_requested(self, symbols: list):
        """User asked to delete the selected rows from the active
        period's results. Hard-deletes from `_period_results` so the
        change persists across view-filter toggles, sort changes, and
        tab switches. Reset implicitly on the next scan (fresh
        `_period_results` overwrites the dict). Re-renders the table
        to reflect the deletion."""
        if not symbols:
            return
        period = self._active_period or ""
        if period not in self._period_results:
            return
        df = self._period_results[period]
        if df is None or df.empty or "symbol" not in df.columns:
            return
        before = len(df)
        keep = ~df["symbol"].astype(str).isin(set(symbols))
        df = df.loc[keep].reset_index(drop=True)
        self._period_results[period] = df
        deleted = before - len(df)
        log.info("Deleted %d row(s) from period '%s'", deleted, period)
        # A deletion may have eliminated rows that were waiting on the
        # cut clipboard. Clear it to avoid a paste targeting now-gone
        # symbols.
        try:
            self.results_table.clear_cut_clipboard()
        except Exception as exc:
            log.debug("cut-clipboard clear after row delete failed: %s", exc)
        # Re-render with view filters applied.
        try:
            shown = self._apply_view_filters(df)
            self.results_table.populate(shown)
        except Exception as exc:
            log.debug("populate after delete failed: %s", exc)

    @pyqtSlot(list)
    def _on_columns_deletion_requested(self, keys: list):
        self._columns_mgr._on_columns_deletion_requested(keys)

    @pyqtSlot(list, str)
    def _on_rows_paste_requested(self, cut_symbols: list, target_symbol: str):
        """User pasted previously-cut rows after `target_symbol`.
        Reorders `_period_results[active_period]` so the cut block
        sits immediately after the target row. No-op if any required
        piece is missing (active period absent, target row not in df,
        cut symbols not in df). Clears the clipboard on success."""
        if not cut_symbols or not target_symbol:
            return
        period = self._active_period or ""
        if period not in self._period_results:
            return
        df = self._period_results[period]
        if df is None or df.empty or "symbol" not in df.columns:
            return

        sym_col = df["symbol"].astype(str)
        cut_set = set(cut_symbols)
        if target_symbol in cut_set:
            log.debug("paste ignored: target in cut set")
            return
        # Preserve original order of cut_symbols when extracting
        cut_mask = sym_col.isin(cut_set)
        if not cut_mask.any():
            log.debug("paste ignored: no cut symbols present in df")
            self.results_table.clear_cut_clipboard()
            return
        # Build the moving block in the order the user originally
        # cut them (NOT df row order) — gives predictable paste output
        # when the user multi-selected disjoint rows.
        cut_block_pieces = []
        for s in cut_symbols:
            piece = df.loc[sym_col == s]
            if not piece.empty:
                cut_block_pieces.append(piece)
        if not cut_block_pieces:
            self.results_table.clear_cut_clipboard()
            return
        cut_block = pd.concat(cut_block_pieces, ignore_index=False)

        # Remaining rows (target preserved in place)
        remaining = df.loc[~cut_mask]
        target_idx_in_remaining = remaining.index[
            remaining["symbol"].astype(str) == target_symbol
        ]
        if len(target_idx_in_remaining) == 0:
            log.debug("paste ignored: target row not in df after cut removal")
            self.results_table.clear_cut_clipboard()
            return
        target_pos = remaining.index.get_loc(target_idx_in_remaining[0])
        before = remaining.iloc[: target_pos + 1]
        after = remaining.iloc[target_pos + 1:]

        new_df = pd.concat(
            [before, cut_block, after], ignore_index=True,
        )
        self._period_results[period] = new_df
        log.info(
            "Pasted %d row(s) after '%s' in period '%s'",
            len(cut_block), target_symbol, period,
        )
        self.results_table.clear_cut_clipboard()
        try:
            shown = self._apply_view_filters(new_df)
            self.results_table.populate(shown)
        except Exception as exc:
            log.debug("populate after paste failed: %s", exc)

    # ─── View-only filters (post-scan, hide rows in displayed table) ──

    # "Has earnings data" coverage set used by the Earnings Data view
    # filter. The 6 most-recent-quarter columns are always candidates;
    # the q-beats blocks (q1..q20 EPS + Rev triplets) are scanned when
    # present so a ticker with multi-quarter beats data still passes
    # even if its most-recent-quarter cells are NaN. Only columns that
    # actually exist in the rendered df are checked — filtering matches
    # whatever scan actually produced.
    _VIEW_EARNINGS_RECENT_DATA_COLS = (
        "reported_eps", "surprise_eps_dollar", "surprise_eps_pct",
        "reported_rev", "surprise_rev_dollar", "surprise_rev_pct",
        "yoy_eps_pct", "yoy_rev_pct",
    )
    # Q-beats blocks — q{1..20}_(reported_eps|surprise_eps_dollar|
    # surprise_eps_pct|reported_rev|surprise_rev_dollar|
    # surprise_rev_pct|yoy_eps_pct|yoy_rev_pct). Up to
    # MAX_BEATS_QUARTERS=20 columns per side. Generated lazily so the
    # constant doesn't bloat the class.
    @staticmethod
    def _q_beats_data_cols() -> tuple[str, ...]:
        suffixes = (
            "reported_eps", "surprise_eps_dollar", "surprise_eps_pct",
            "reported_rev", "surprise_rev_dollar", "surprise_rev_pct",
            "yoy_eps_pct", "yoy_rev_pct",
        )
        return tuple(
            f"q{i}_{s}" for i in range(1, 21) for s in suffixes
        )

    # "Has earnings date" coverage set used by the Earnings Dates view
    # filter — calendar-derived columns. Beats per-quarter date
    # columns (q{i}_report_date_eps / q{i}_report_date_rev) also count
    # so the dates filter matches the spirit of "any earnings
    # touchpoint" without depending on whether beats are populated.
    _VIEW_EARNINGS_DATE_COLS = (
        "last_report_date", "next_earnings_date",
        "days_since_er", "days_until_er",
    )

    @staticmethod
    def _q_beats_date_cols() -> tuple[str, ...]:
        return tuple(
            f"q{i}_report_date_{side}"
            for i in range(1, 21) for side in ("eps", "rev")
        )

    def _apply_view_filters(self, df):
        """Apply the Earnings Dates / Earnings Data / Color Match Only
        view filters to `df` and return a filtered copy. When no toggle
        is on, returns `df` unchanged. Always returns a NEW DataFrame
        so callers can mutate without affecting the source.

        Invariant: the dates filter is broader than the data filter —
        any row passing the data filter also passes the dates filter
        (data implies date), enforced by including the data coverage
        cols in the dates check.

        Used by:
          - `_on_timeframe_changed` (re-render on timeframe switch)
          - `_reapply_view_filters_for_active_period` (re-render on
            view-filter toggle change)
          - `_export_results` / `_excel_export_dialog` /
            `_send_to_watchlist` (the user sees what they export)
        """
        if df is None or df.empty:
            return df
        mask = pd.Series(True, index=df.index)

        # Earnings Data — non-NaN across recent-quarter cols + q-beats
        # data cols. Also used in the dates check to enforce the
        # data⇒date invariant.
        data_present_cols = [
            c for c in (
                *self._VIEW_EARNINGS_RECENT_DATA_COLS,
                *self._q_beats_data_cols(),
            ) if c in df.columns
        ]
        if data_present_cols:
            has_any_data = df[data_present_cols].notna().any(axis=1)
        else:
            has_any_data = pd.Series(False, index=df.index)

        if (hasattr(self, "chk_view_earnings_data_only")
                and self.chk_view_earnings_data_only.isChecked()):
            if not data_present_cols:
                # No earnings data columns in the frame at all → no
                # row has earnings data; filter empties the table. The
                # user gets an empty table, which is the correct
                # signal that this scan has no earnings data.
                mask &= False
            else:
                mask &= has_any_data

        if (hasattr(self, "chk_view_earnings_dates_only")
                and self.chk_view_earnings_dates_only.isChecked()):
            # Earnings Dates — pass if ANY of the date columns is
            # non-NaN OR the row has any earnings data (data⇒date).
            date_present_cols = [
                c for c in (
                    *self._VIEW_EARNINGS_DATE_COLS,
                    *self._q_beats_date_cols(),
                ) if c in df.columns
            ]
            if date_present_cols:
                has_any_date = df[date_present_cols].notna().any(axis=1)
            else:
                has_any_date = pd.Series(False, index=df.index)
            mask &= (has_any_date | has_any_data)

        if (hasattr(self, "chk_view_color_match_only")
                and self.chk_view_color_match_only.isChecked()):
            # Has-color-match = `_earnings_aligned_dates` column is
            # populated (a non-empty list). Scanner sets this only
            # when an indicator date matched an earnings date.
            if "_earnings_aligned_dates" not in df.columns:
                mask &= False
            else:
                col = df["_earnings_aligned_dates"]
                # `notna() & non-empty` — a NaN cell or empty list both fail
                has_match = col.apply(
                    lambda v: isinstance(v, list) and len(v) > 0
                )
                mask &= has_match

        out = df.loc[mask].reset_index(drop=True)

        # Drop user-hidden columns (header right-click → Delete column).
        # Done AFTER row filtering so the row mask doesn't depend on
        # column presence — a deleted column shouldn't change which
        # rows pass other view filters. The try/except tolerates
        # bypass-init shells used by some unit tests (Qt's attribute
        # lookup raises RuntimeError on uninitialized QObject
        # subclasses, even via getattr with a default).
        try:
            deleted = self._deleted_column_keys
        except (AttributeError, RuntimeError):
            deleted = set()
        if deleted:
            cols_to_drop = [c for c in deleted if c in out.columns]
            if cols_to_drop:
                out = out.drop(columns=cols_to_drop)
        return out

    def _reapply_view_filters_for_active_period(self):
        """Slot wired to view-filter toggles. Re-renders the table
        with the current period's data run through the latest
        view-filter state. Skips if no scan has happened yet."""
        if not getattr(self, "_period_order", None):
            return
        if not getattr(self, "_active_period", None):
            return
        idx = self.combo_timeframe.currentIndex()
        if idx < 0:
            return
        # Reuse the timeframe-switch path so re-entrancy guards +
        # column-width cache + populate fast-path all apply.
        self._on_timeframe_changed(idx)

    # ── Excel / CSV export delegates ─────────────────────────────────
    # The export pipeline (ExcelExportDialog handoff, multi-sheet XLSX
    # generation, color mapping, CSV concatenation, legacy CSV/TXT
    # export) lives in ExportsController (exports.py). MainWindow keeps
    # every historical method name below as a thin delegate so button
    # wiring and tests are untouched.

    @property
    def _exports(self) -> ExportsController:
        """Lazily-created exports controller. Looked up via ``__dict__``
        (not getattr) because bare ``MainWindow.__new__`` test shells
        raise RuntimeError when attribute lookup falls through to the
        uninitialized Qt C++ side."""
        ctl = self.__dict__.get("_exports_ctl")
        if ctl is None:
            ctl = ExportsController(self)
            self.__dict__["_exports_ctl"] = ctl
        return ctl

    def _ordered_active_columns_for_export(self) -> list[tuple]:
        return self._exports._ordered_active_columns_for_export()

    def _build_export_df(
        self, df: "pd.DataFrame", keys: list[str], wants_news: bool,
        prepend_period: Optional[str] = None,
    ) -> "pd.DataFrame":
        return self._exports._build_export_df(
            df, keys, wants_news, prepend_period=prepend_period,
        )

    @staticmethod
    def _sanitize_sheet_name(label: str, used: set[str]) -> str:
        return ExportsController._sanitize_sheet_name(label, used)

    def _excel_export_dialog(self):
        self._exports._excel_export_dialog()

    def _write_xlsx_multi_sheet(
        self, path: str, periods: list[str], keys: list[str], wants_news: bool,
        *, apply_colors: bool = False,
    ):
        self._exports._write_xlsx_multi_sheet(
            path, periods, keys, wants_news, apply_colors=apply_colors,
        )

    def _apply_xlsx_cell_colors(self, ws, keys: list[str], wants_news: bool):
        self._exports._apply_xlsx_cell_colors(ws, keys, wants_news)

    def _ordered_export_columns_with_news(
        self, keys: list[str], wants_news: bool,
    ) -> list[tuple[str, "str | None", bool]]:
        return self._exports._ordered_export_columns_with_news(keys, wants_news)

    def _is_export_color(self, rgb: tuple[int, int, int]) -> bool:
        return self._exports._is_export_color(rgb)

    def _write_csv_export(
        self, path: str, periods: list[str], keys: list[str], wants_news: bool,
    ):
        self._exports._write_csv_export(path, periods, keys, wants_news)

    def _export_results(self):
        self._exports._export_results()

    # ── Watchlist Bridge (Phase 4) ───────────────────────────────────

    def _send_to_watchlist(self):
        symbols = self.results_table.get_symbols()
        if not symbols:
            return

        self._wl_dialog = WatchlistDialog(len(symbols), self)
        self._wl_dialog.start_requested.connect(
            lambda cfg: self._launch_bridge(symbols, cfg)
        )
        self._wl_dialog.cancelled.connect(self._close_wl_dialog)
        self._wl_dialog.exec()

    def _manual_input_stw(self):
        """Open a dialog for manually entering comma-separated tickers."""
        dlg = QDialog(self)
        dlg.setWindowTitle("Manual Input — Send to Watchlist")
        dlg.setMinimumWidth(500)
        layout = QVBoxLayout(dlg)
        layout.setContentsMargins(30, 20, 30, 20)

        layout.addWidget(QLabel("Enter tickers (comma-separated):"))
        txt = QTextEdit()
        txt.setMinimumHeight(120)
        txt.setPlaceholderText("AAPL, MSFT, NVDA, TSLA, ...")
        layout.addWidget(txt)

        btn_row = QHBoxLayout()
        btn_go = QPushButton("  Send  ")
        btn_go.setStyleSheet(
            "QPushButton { background: #6a1b9a; color: white; font-size: 13px; "
            "font-weight: bold; padding: 8px 24px; border-radius: 4px; }"
        )
        btn_cancel = QPushButton("  Cancel  ")
        btn_cancel.setStyleSheet(
            "QPushButton { background: #555; color: white; font-size: 13px; "
            "padding: 8px 24px; border-radius: 4px; }"
        )
        btn_row.addWidget(btn_go)
        btn_row.addWidget(btn_cancel)
        btn_row.addStretch()
        layout.addLayout(btn_row)

        def _on_go():
            raw = txt.toPlainText()
            tickers = [t.strip().upper() for t in raw.replace("\n", ",").split(",")
                       if t.strip()]
            if not tickers:
                QMessageBox.warning(dlg, "No Tickers", "Enter at least one ticker.")
                return
            dlg.accept()
            # Show the watchlist dialog with these manual tickers
            wl = WatchlistDialog(len(tickers), self)
            wl.start_requested.connect(
                lambda cfg: self._launch_bridge(tickers, cfg)
            )
            wl.exec()

        btn_go.clicked.connect(_on_go)
        btn_cancel.clicked.connect(dlg.reject)
        dlg.exec()

    def _close_wl_dialog(self):
        if hasattr(self, "_wl_dialog") and self._wl_dialog:
            self._wl_dialog.hide()
            self._wl_dialog.deleteLater()
            self._wl_dialog = None

    def _launch_bridge(self, symbols: list[str], cfg: BridgeConfig):
        if hasattr(self, "_wl_dialog"):
            self._close_wl_dialog()

        self.btn_scan.setEnabled(False)
        self.btn_send.setEnabled(False)
        self.btn_manual.setEnabled(False)
        self.btn_stop_send.setEnabled(True)
        # btn_stop is the scan-stop button; it stays disabled during sends —
        # the dedicated btn_stop_send handles bridge interruption.

        # Remember this run's full symbols list + cfg so Stop Sending →
        # Resume / Restart can re-launch a new bridge with the right slice.
        self._bridge_symbols = list(symbols)
        self._bridge_cfg = cfg
        self._bridge_last_sent_idx = 0
        self._stop_sending_show_dialog = False

        self._bridge_worker = BridgeWorker(symbols, cfg)
        self._bridge_worker.log_msg.connect(self.log_panel.write_line)
        self._bridge_worker.countdown_tick.connect(self._on_bridge_countdown)
        self._bridge_worker.ticker_sent.connect(self._on_bridge_ticker)
        self._bridge_worker.done.connect(self._on_bridge_done)
        self._bridge_worker.batch_pause.connect(self._on_batch_pause)

        self._bridge_worker.start()

    def _stop_sending(self):
        """Halt an in-progress watchlist send. The bridge will finish its
        current ticker, fire on_done, and then we show the Cancel / Resume /
        Restart dialog from `_show_stop_sending_options`."""
        if not self._bridge_worker or not self._bridge_worker.isRunning():
            return
        self.btn_stop_send.setEnabled(False)
        self.status.showMessage("Stopping send (waiting for current ticker)...")
        self.log_panel.write_line("Stop Sending requested — finishing current ticker.")
        self._stop_sending_show_dialog = True
        self._bridge_worker.request_stop()

    def _show_stop_sending_options(self, sent_in_run: int):
        """Modal dialog after a Stop Sending halt, offering:
          Cancel send       — abort, do nothing more
          Resume from #N    — new bridge starting at the next-unsent ticker
          Restart from #1   — new bridge starting at the original index 0
        """
        if self._bridge_symbols is None or self._bridge_cfg is None:
            return

        symbols = self._bridge_symbols
        base_cfg = self._bridge_cfg
        run_total = len(symbols) - base_cfg.start_index
        absolute_next_idx = base_cfg.start_index + sent_in_run  # 0-based

        # If the run was started at #X and we sent N tickers, the natural
        # display number for "where would Resume start" is X+N+1 (1-based).
        resume_label = f"Resume from #{absolute_next_idx + 1}"
        restart_label = "Restart from #1"

        msg = QMessageBox(self)
        msg.setIcon(QMessageBox.Icon.Question)
        msg.setWindowTitle("Send Stopped")
        msg.setText(
            f"Send halted at ticker {sent_in_run} / {run_total} of this run.\n\n"
            f"What would you like to do?"
        )
        btn_cancel = msg.addButton("Cancel send",
                                   QMessageBox.ButtonRole.RejectRole)
        btn_resume = msg.addButton(resume_label,
                                   QMessageBox.ButtonRole.ActionRole)
        btn_restart = msg.addButton(restart_label,
                                    QMessageBox.ButtonRole.ResetRole)
        # Default focus to Resume (most likely intent after a transient halt)
        msg.setDefaultButton(btn_resume)
        msg.exec()
        clicked = msg.clickedButton()

        def _new_cfg(start_index: int) -> BridgeConfig:
            return BridgeConfig(
                delay_between_tickers=base_cfg.delay_between_tickers,
                countdown_seconds=base_cfg.countdown_seconds,
                dry_run=base_cfg.dry_run,
                confirm_key=base_cfg.confirm_key,
                start_index=start_index,
                batch_size=base_cfg.batch_size,
            )

        if clicked is btn_resume:
            if absolute_next_idx >= len(symbols):
                self.log_panel.write_line(
                    "Nothing left to send — all tickers were already sent."
                )
                return
            remaining = len(symbols) - absolute_next_idx
            self.log_panel.write_line(
                f"Resuming send from #{absolute_next_idx + 1} "
                f"({remaining} tickers remaining)."
            )
            self._launch_bridge(symbols, _new_cfg(absolute_next_idx))
        elif clicked is btn_restart:
            self.log_panel.write_line(
                f"Restarting send from #1 ({len(symbols)} tickers)."
            )
            self._launch_bridge(symbols, _new_cfg(0))
        else:
            self.log_panel.write_line(
                f"Send cancelled at #{sent_in_run} of this run."
            )

    @pyqtSlot(int, int, int)
    def _on_batch_pause(self, batch_num: int, sent: int, total: int):
        remaining = total - sent
        reply = QMessageBox.question(
            self, "Batch Complete",
            f"Batch {batch_num} complete ({sent} sent, {remaining} remaining).\n\n"
            "Click into the target input field, then press Yes to continue.\n"
            "Press No to stop.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.Yes,
        )
        if reply == QMessageBox.StandardButton.Yes:
            self._bridge_worker.resume_batch()
        else:
            self._bridge_worker.request_stop()

    @pyqtSlot(int)
    def _on_bridge_countdown(self, remaining: int):
        self.status.showMessage(
            f"Watchlist: starting in {remaining}s -- click into the target input field!"
        )

    @pyqtSlot(int, int, str, bool)
    def _on_bridge_ticker(self, idx: int, total: int, sym: str, dry: bool):
        mode = "dry" if dry else "sent"
        self.status.showMessage(f"Watchlist: [{idx}/{total}] {sym} ({mode})")
        # Track the count of tickers sent so far in this bridge run; used by
        # the Stop Sending → Resume option to compute the next start_index.
        self._bridge_last_sent_idx = idx

    @pyqtSlot(int, int)
    def _on_bridge_done(self, sent: int, skipped: int):
        self.btn_scan.setEnabled(True)
        self.btn_send.setEnabled(True)
        self.btn_manual.setEnabled(True)
        self.btn_stop_send.setEnabled(False)

        if skipped:
            self.status.showMessage(
                f"Watchlist: {sent} sent, {skipped} skipped (stopped early)"
            )
        else:
            self.status.showMessage(f"Watchlist: {sent} tickers sent successfully")

        self._bridge_worker = None

        # If the user explicitly clicked Stop Sending, show the post-stop
        # options (Cancel / Resume / Restart). Otherwise this was a natural
        # end-of-list completion and no dialog is needed.
        if self._stop_sending_show_dialog:
            self._stop_sending_show_dialog = False
            self._show_stop_sending_options(sent)

    # ── Presets ────────────────────────────────────────────────────────

    def _refresh_preset_list(self):
        self.preset_combo.clear()
        self.preset_combo.addItem("(select preset)")
        for p in sorted(PRESETS_DIR.glob("*.json")):
            self.preset_combo.addItem(p.stem)

    @staticmethod
    def _sanitize_preset_name(name: str) -> str:
        """Strip directory components + Windows-illegal characters from a
        user-supplied preset name so it can't escape PRESETS_DIR (path
        traversal) or build an invalid filename. '' if nothing usable left."""
        import re as _re
        base = (name or "").strip().replace("\\", "/").split("/")[-1]
        base = _re.sub(r'[<>:"/\\|?*\x00-\x1f]', "", base).strip(" .")
        return base

    def _save_preset(self):
        name = self.preset_combo.currentText()
        if name == "(select preset)" or not name.strip():
            # Prompt for name
            from PyQt6.QtWidgets import QInputDialog
            name, ok = QInputDialog.getText(self, "Save Preset", "Preset name:")
            if not ok or not name.strip():
                return
        # Sanitize before the name reaches any filesystem path (audit:
        # preset name was used unsanitized in PRESETS_DIR / f"{name}.json").
        name = self._sanitize_preset_name(name)
        if not name:
            QMessageBox.warning(
                self, "Save Preset",
                "That preset name has no usable characters — try another.",
            )
            return

        # Snapshot of which fixed-window timeframe checkboxes are
        # ticked. Keyed by day count (the same key used by
        # `tf_checks`) so reload picks up the right boxes even if
        # someone reorders the toolbar in the future.
        timeframe_days = sorted(d for d, chk in self.tf_checks.items()
                                if chk.isChecked())

        # Sequenced Run config — tuple form (start, end, n, unit) is
        # collapsed to a stable JSON-friendly dict. None when the
        # checkbox is off / never configured. We persist it WHENEVER
        # it exists, regardless of whether the checkbox is currently
        # ticked, so toggling Sequenced Run on later restores the
        # last-used range without re-prompting.
        sequenced_cfg = None
        if self._sequenced_cfg is not None:
            seq_start, seq_end, seq_n, seq_unit = self._sequenced_cfg
            sequenced_cfg = {
                "start": seq_start.isoformat(),
                "end": seq_end.isoformat(),
                "n": int(seq_n),
                "unit": str(seq_unit),
            }

        data = {
            # Phase 6 O11: schema version stamped into every saved preset.
            # v2 adds timeframe / sequenced run fields. v3 added the
            # unified `earnings_only_mode` toolbar toggle. v4 SPLITS
            # that into `earnings_dates_only` + `earnings_data_only`
            # (and likewise the view filters). v3 keys
            # (`earnings_only_mode`, `view_earnings_only`) are still
            # tolerated on load and mapped onto the data flags for
            # backward compat. Loader tolerates missing keys via .get
            # + defaults.
            "_preset_version": PRESET_SCHEMA_VERSION,
            "indicators": self.indicator_panel.to_dict(),
            "start_date": self.date_start.date().toString("yyyy-MM-dd"),
            "end_date": self.date_end.date().toString("yyyy-MM-dd"),
            "include_etf": self.chk_include_etf.isChecked(),
            "include_adr": self.chk_include_adr.isChecked(),
            "ipo_mode": self.chk_ipo_mode.isChecked(),
            "ipo_max_days": self.spin_ipo_days.value(),
            # v2: timeframe / series settings.
            "timeframe_days": timeframe_days,
            "custom_range": self.chk_custom_range.isChecked(),
            "sequenced_run": self.chk_sequenced_run.isChecked(),
            "sequenced_cfg": sequenced_cfg,
            # v4: split scan-level Earnings Mode toolbar toggles.
            "earnings_dates_only": self.chk_earnings_dates_only.isChecked(),
            "earnings_data_only": self.chk_earnings_data_only.isChecked(),
            # v4: split post-scan view filters (next to Timeframe).
            # Display-only — don't affect scanner output, just the
            # rendered table + exports + Send-to-Watchlist.
            "view_earnings_dates_only": self.chk_view_earnings_dates_only.isChecked(),
            "view_earnings_data_only": self.chk_view_earnings_data_only.isChecked(),
            "view_color_match_only": self.chk_view_color_match_only.isChecked(),
            "view_interleave_quarters": self.chk_view_interleave_quarters.isChecked(),
            # v5: column layout (manual order + hidden set). Loading a
            # preset wipes the output window, sets these, and updates
            # the Columns dropdown so the next scan honors the saved
            # layout. Empty list / empty set means "use canonical order
            # for the current scan settings."
            "column_order": list(self._results_column_order),
            "column_hidden": sorted(self._deleted_column_keys),
            # v6: session-bar omission toggles. Round-trip the user's
            # preferred posture per preset. Loading a preset with
            # `omit_previously_scanned=True` does NOT replay the
            # accumulated `_session_seen` set — only the checkbox state
            # is restored; the per-launch seen set starts empty as usual.
            "omit_previously_scanned": self.chk_omit_seen.isChecked(),
            "omit_earlier_period_hits": self.chk_omit_intra_run.isChecked(),
        }

        path = PRESETS_DIR / f"{name.strip()}.json"
        config.atomic_write_text(path, json.dumps(data, indent=2))
        self.log_panel.write_line(f"Preset saved: {name}")
        self._refresh_preset_list()
        # Select the saved preset
        idx = self.preset_combo.findText(name.strip())
        if idx >= 0:
            self.preset_combo.setCurrentIndex(idx)

    def _load_preset(self):
        name = self.preset_combo.currentText()
        if name == "(select preset)":
            return
        path = PRESETS_DIR / f"{name}.json"
        if not path.exists():
            QMessageBox.warning(self, "Not Found", f"Preset '{name}' not found.")
            return
        # Guard the read+parse: a truncated/hand-edited/non-UTF-8 preset raises
        # here, and an exception escaping this Qt slot can abort the app under
        # PyInstaller windowed mode (no sys.excepthook in main()). Fail with a
        # dialog before any session state is mutated.
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError, UnicodeDecodeError, ValueError) as exc:
            QMessageBox.warning(
                self, "Load Preset",
                f"Preset '{name}' is corrupt or unreadable:\n{exc}",
            )
            return

        # Phase 6 O11: warn when a preset comes from a newer build than this
        # code understands. Missing keys are tolerated by the .get() loads
        # below, so older presets continue to load cleanly.
        stamped = data.get("_preset_version")
        if stamped is not None and stamped > PRESET_SCHEMA_VERSION:
            self.log_panel.write_line(
                f"Preset '{name}' is version {stamped}; this build understands "
                f"up to v{PRESET_SCHEMA_VERSION} — some fields may be ignored."
            )

        if "indicators" in data:
            self.indicator_panel.from_dict(data["indicators"])
        if "start_date" in data:
            self.date_start.setDate(QDate.fromString(data["start_date"], "yyyy-MM-dd"))
        if "end_date" in data:
            self.date_end.setDate(QDate.fromString(data["end_date"], "yyyy-MM-dd"))
        if "include_etf" in data:
            self.chk_include_etf.setChecked(data["include_etf"])
        if "include_adr" in data:
            self.chk_include_adr.setChecked(data["include_adr"])
        if "ipo_mode" in data:
            self.chk_ipo_mode.setChecked(data["ipo_mode"])
        if "ipo_max_days" in data:
            self.spin_ipo_days.setValue(data["ipo_max_days"])
        # v4: split Earnings Mode toolbar toggles. Default False so
        # legacy presets behave like before (NaN passes earnings
        # filters). v3 `earnings_only_mode` is mapped onto BOTH new
        # flags for backward compat — v3 enabled both signals together.
        legacy_earn_only = bool(data.get("earnings_only_mode", False))
        if "earnings_dates_only" in data:
            self.chk_earnings_dates_only.setChecked(bool(data["earnings_dates_only"]))
        elif legacy_earn_only:
            self.chk_earnings_dates_only.setChecked(True)
        if "earnings_data_only" in data:
            self.chk_earnings_data_only.setChecked(bool(data["earnings_data_only"]))
        elif legacy_earn_only:
            self.chk_earnings_data_only.setChecked(True)
        # v4: post-scan view filters. blockSignals so we don't fire
        # _reapply_view_filters_for_active_period N times during load.
        # v3 `view_earnings_only` maps onto `view_earnings_data_only`
        # (the v3 semantics matched the new data filter — it checked
        # the 6 most-recent-quarter columns only).
        legacy_view_earn = data.get("view_earnings_only")
        for chk_name, key, legacy_val in (
            ("chk_view_earnings_dates_only", "view_earnings_dates_only", None),
            ("chk_view_earnings_data_only", "view_earnings_data_only",
             legacy_view_earn),
            ("chk_view_color_match_only", "view_color_match_only", None),
            ("chk_view_interleave_quarters", "view_interleave_quarters", None),
        ):
            if key in data:
                value = bool(data[key])
            elif legacy_val is not None:
                value = bool(legacy_val)
            else:
                continue
            chk = getattr(self, chk_name)
            chk.blockSignals(True)
            try:
                chk.setChecked(value)
            finally:
                chk.blockSignals(False)
        # v5: column layout (manual order + hidden set). Loading a
        # preset wipes the current results so the dropdown reflects
        # only the preset's saved column intent — the prior scan's
        # output isn't relevant once the user switches presets, and
        # leaving it on screen risks confusion when the preset's
        # column set differs from what's displayed. Loading also
        # populates the dropdown with the preset's order so the user
        # can preview before running. Pre-v5 presets that lack the
        # column metadata leave the existing layout alone (legacy
        # behavior).
        col_order = data.get("column_order")
        col_hidden = data.get("column_hidden")
        if col_order is not None or col_hidden is not None:
            self._period_results = {}
            self._period_order = []
            self._active_period = None
            self._last_results_df = None
            try:
                self.combo_timeframe.blockSignals(True)
                self.combo_timeframe.clear()
                self.combo_timeframe.setEnabled(False)
            finally:
                self.combo_timeframe.blockSignals(False)
            self.results_table.populate(pd.DataFrame())
            self.btn_send.setEnabled(False)
            self.btn_export.setEnabled(False)
            self.btn_excel.setEnabled(False)
            self._results_column_order = list(col_order) if col_order else []
            self._deleted_column_keys = set(col_hidden or [])
            try:
                self.results_table.set_saved_column_order(
                    self._results_column_order
                )
            except Exception as exc:
                log.debug(
                    "preset load: set_saved_column_order(%d keys) "
                    "failed: %s", len(self._results_column_order), exc,
                )
            self._sync_columns_dialog()

        # v6: session-bar omission toggles. No signal connections live
        # on these checkboxes (they're polled in `_run_scan` /
        # `ScanWorker.run`), so no blockSignals dance is required.
        # Missing keys default to False — pre-v6 presets behave like
        # the legacy "off on every launch" semantics.
        if "omit_previously_scanned" in data:
            self.chk_omit_seen.setChecked(bool(data["omit_previously_scanned"]))
        if "omit_earlier_period_hits" in data:
            self.chk_omit_intra_run.setChecked(
                bool(data["omit_earlier_period_hits"])
            )

        # If a scan is already on screen (only possible for old presets
        # since v5+ wipe above), re-apply the freshly-loaded view
        # filter state.
        if getattr(self, "_active_period", None):
            self._reapply_view_filters_for_active_period()

        # v2: restore the sequenced cfg + timeframe / custom-range
        # checkboxes. We bypass `_on_sequenced_run_toggled` entirely
        # because it would re-open the SequencedRunDialog whenever
        # `sequenced_run=True` even though we just restored cfg from
        # the preset. Instead we manually mirror its mutex side-effects
        # (disable/uncheck other timeframe boxes when sequenced is on).
        seq_cfg = data.get("sequenced_cfg")
        if seq_cfg is not None:
            try:
                self._sequenced_cfg = (
                    date.fromisoformat(seq_cfg["start"]),
                    date.fromisoformat(seq_cfg["end"]),
                    int(seq_cfg["n"]),
                    str(seq_cfg["unit"]),
                )
            except (KeyError, TypeError, ValueError) as exc:
                log.debug("Could not restore sequenced_cfg: %s", exc)
                self._sequenced_cfg = None

        seq_on = bool(data.get("sequenced_run", False))
        # Block the toggled signal — we'll apply mutex side-effects
        # ourselves so the dialog doesn't re-open.
        self.chk_sequenced_run.blockSignals(True)
        try:
            self.chk_sequenced_run.setChecked(seq_on)
        finally:
            self.chk_sequenced_run.blockSignals(False)

        if seq_on:
            # Sequenced mode: every per-timeframe checkbox is
            # off + disabled; custom range is off + disabled.
            for chk in self.tf_checks.values():
                chk.setChecked(False)
                chk.setEnabled(False)
            self.chk_custom_range.setChecked(False)
            self.chk_custom_range.setEnabled(False)
        else:
            # Sequenced off: re-enable the per-timeframe controls,
            # then restore which ones the saved preset had ticked.
            for chk in self.tf_checks.values():
                chk.setEnabled(True)
            self.chk_custom_range.setEnabled(True)
            if "timeframe_days" in data:
                saved_days = set(int(d) for d in (data["timeframe_days"] or []))
                for d, chk in self.tf_checks.items():
                    chk.setChecked(d in saved_days)
            if "custom_range" in data:
                self.chk_custom_range.setChecked(bool(data["custom_range"]))

        self.log_panel.write_line(f"Preset loaded: {name}")

    def _delete_preset(self):
        name = self.preset_combo.currentText()
        if name == "(select preset)":
            return
        path = PRESETS_DIR / f"{name}.json"
        if path.exists():
            path.unlink()
            self.log_panel.write_line(f"Preset deleted: {name}")
            self._refresh_preset_list()

    # ── Cleanup ────────────────────────────────────────────────────────

    def closeEvent(self, event):
        """Stop all background workers on exit (Phase 4 R3 + R4 + O15).

        Uses request_stop + wait uniformly — no QThread.terminate() which Qt
        docs flag as unsafe. If a worker doesn't honor stop within the
        timeout we log and proceed; Python process exit will reap any
        still-running daemon threads."""
        workers = [
            self._worker,
            self._update_worker,
            self._universe_worker,
            self._sector_worker,
            self._earnings_worker,
            self._zacks_worker,
            # Earnings fill workers — listed so an in-flight fill that's
            # mid-parquet-flush isn't ungracefully terminated on app
            # close, racing the atomic write.
            getattr(self, "_finnhub_worker", None),
            getattr(self, "_finviz_worker", None),
            getattr(self, "_bridge_worker", None),
            # Firefox cookie-wait worker: it polls cookies.sqlite on up to a
            # 6-hour loop and is request_stop()-able. Without it here, closing
            # the app mid cookie-refresh destroys a still-running QThread
            # ("QThread: Destroyed while thread is still running") and can
            # fire its signals into a torn-down window.
            getattr(self, "_cookie_wait_worker", None),
        ]
        # Persist window geometry so the next launch restores size + position
        # instead of force-centering at a fixed 1500x900 (see main()).
        try:
            self._qsettings().setValue("main_window/geometry", self.saveGeometry())
        except Exception as exc:
            log.debug("Could not persist window geometry: %s", exc)
        # Phase 1: signal stop to every live worker
        for w in workers:
            if w is None or not w.isRunning():
                continue
            try:
                if hasattr(w, "request_stop"):
                    w.request_stop()
            except Exception as exc:
                log.debug("request_stop on %s raised: %s", type(w).__name__, exc)
        # Phase 2: wait for each to exit (3s ceiling per worker)
        for w in workers:
            if w is None or not w.isRunning():
                continue
            if not w.wait(3000):
                log.warning(
                    "%s did not stop within 3s — leaving it to process exit",
                    type(w).__name__,
                )

        # Phase 2 I6: close the persistent log file handle
        self.log_panel.close_log()

        # Phase 6 O10: close and detach the per-subsystem log handlers so
        # the log files are flushed and stale handlers don't accumulate
        # across repeated launches in the same Python process.
        for h in getattr(self, "_subsystem_log_handlers", []):
            try:
                h.close()
                for logger_name in ("scanner.data", "scanner.universe",
                                    "scanner.tradestation"):
                    logging.getLogger(logger_name).removeHandler(h)
            except Exception as exc:
                log.debug(
                    "subsystem log handler close/detach failed: %s", exc,
                )
        # Detach the QtLogHandler that pipes scanner logs into the
        # GUI's LogPanel. The handler holds a Qt signal reference; if
        # we leave it on the `scanner` logger after the window is
        # destroyed, future log emissions try to fire the signal of a
        # freed widget and the process crashes (Windows access
        # violation). Mostly matters for test runs that construct +
        # discard MainWindows multiple times in a single Python
        # process, but harmless in production too.
        gui_handler = getattr(self, "_gui_log_handler", None)
        if gui_handler is not None:
            try:
                logging.getLogger("scanner").removeHandler(gui_handler)
            except Exception as exc:
                log.debug("GUI log handler detach failed: %s", exc)
            self._gui_log_handler = None
        super().closeEvent(event)


# ============================================================================
# Entry point
# ============================================================================

def _build_cookie_textedit_widget(
    initial: str, *, hide_initially: bool,
) -> tuple[QTextEdit, QPushButton]:
    """Module-level helper that returns a (QTextEdit, Show/Hide button)
    pair for cookie-entry dialogs. Cookies are masked when the textarea
    is seeded with existing content (audit M7); the user clicks Show
    to edit and Hide to remask.

    Used by both `MainWindow._set_zacks_cookies_dialog` /
    `_on_imperva_block_detected` AND `prompt_zacks_cookies_if_missing`
    at startup.
    """
    txt = QTextEdit()
    txt.setMinimumHeight(160)

    masked_style = (
        "QTextEdit { color: transparent; selection-color: transparent;"
        " selection-background-color: #555; }"
    )

    def apply_visibility(show: bool):
        if show or not txt.toPlainText():
            txt.setStyleSheet("")
            btn.setText("Hide")
        else:
            txt.setStyleSheet(masked_style)
            btn.setText("Show")

    btn = QPushButton("Show")
    btn.setFixedWidth(70)
    btn.setCheckable(True)
    btn.clicked.connect(lambda checked: apply_visibility(checked))

    if initial:
        txt.setPlainText(initial)
        txt.selectAll()
    btn.setChecked(not (hide_initially and bool(initial)))
    apply_visibility(btn.isChecked())
    return txt, btn


def prompt_zacks_cookies_if_missing(parent=None) -> None:
    """First-launch prompt for the Zacks browser cookie string. No-op
    if cookies are already stored. Mirrors the Finnhub prompt pattern
    but uses a multi-line dialog because the cookie blob is ~2 KB —
    too long for `QInputDialog.getText`.

    Called from `main()` after `prompt_finnhub_key_if_missing()` so
    the user sees Finnhub first, Zacks second."""
    from ..zacks_scraper import has_zacks_cookies, set_zacks_cookies
    if has_zacks_cookies():
        return

    dlg = QDialog(parent)
    dlg.setWindowTitle("Zacks Cookies (optional)")
    dlg.setMinimumWidth(640)
    layout = QVBoxLayout(dlg)
    layout.addWidget(QLabel(
        "Enter a Zacks browser cookie string to enable the Zacks "
        "earnings scraper (Per-Quarter Earnings filters, Consecutive "
        "Beats filters, daily smart-refresh).\n\n"
        "How: open zacks.com in a logged-in browser → DevTools → "
        "Application → Cookies → copy the full `key=value; key=value; …` "
        "header value and paste below.\n\n"
        "Stored under scanner_data/zacks_cookies.txt (atomic write). "
        "Leave blank or click Cancel to skip — you can set them later "
        "via Data → Set Zacks Cookies."
    ))

    txt, show_btn = _build_cookie_textedit_widget("", hide_initially=False)
    toggle_row = QHBoxLayout()
    toggle_row.addWidget(QLabel("Cookies:"))
    toggle_row.addStretch()
    toggle_row.addWidget(show_btn)
    layout.addLayout(toggle_row)
    layout.addWidget(txt)

    btns = QDialogButtonBox(
        QDialogButtonBox.StandardButton.Ok
        | QDialogButtonBox.StandardButton.Cancel
    )
    btns.button(QDialogButtonBox.StandardButton.Ok).setText("Save")
    btns.button(QDialogButtonBox.StandardButton.Cancel).setText("Skip")
    btns.accepted.connect(dlg.accept)
    btns.rejected.connect(dlg.reject)
    layout.addWidget(btns)

    if dlg.exec() != QDialog.DialogCode.Accepted:
        return
    cookie_str = txt.toPlainText().strip()
    if not cookie_str:
        return
    if set_zacks_cookies(cookie_str):
        log.info("Zacks cookies stored on first launch (%d chars)",
                 len(cookie_str))


def prompt_finnhub_key_if_missing(parent=None) -> None:
    """First-launch prompt for the Finnhub API key. No-op if one is
    already stored. Run after QApplication is created and before the main
    window appears so the dialog is the first thing the user sees."""
    if finnhub_client.is_configured():
        return
    text, ok = QInputDialog.getText(
        parent,
        "Finnhub API Key (optional)",
        (
            "Enter your Finnhub API key (free tier 60 req/min) to enable "
            "the Finnhub augment for targeted earnings and sector fills.\n\n"
            "Get one at https://finnhub.io/dashboard.\n"
            "Stored in the OS credential manager — no plaintext on disk.\n\n"
            "Leave blank or click Cancel to skip; the scanner will still "
            "work using the yfinance providers only."
        ),
        QLineEdit.EchoMode.Password,
    )
    if not ok:
        return
    text = (text or "").strip()
    if text and finnhub_client.set_api_key(text):
        log.info("Finnhub API key stored (ends …%s)", text[-4:])


def main():
    # Suppress DPI awareness warning on Windows multi-monitor setups
    import os
    os.environ.setdefault("QT_ENABLE_HIGHDPI_SCALING", "1")

    # Create scanner_data/ subdirs now that we're at a real entry point
    # (config.py no longer does this at import time).
    config.ensure_dirs()
    PRESETS_DIR.mkdir(parents=True, exist_ok=True)

    # Phase 4 R18: stamp / verify the parquet cache schema version. Logs a
    # warning if a future build inherits an older or newer cache.
    check_schema_version()

    # Prune raw earnings audit files older than the retention window so
    # they don't accumulate forever. Cheap (mtime-only walk); failures
    # are logged but never block startup.
    try:
        from .. import earnings_raw
        earnings_raw.prune_old_raw()
    except Exception as exc:
        # Use root logger here — scanner.* loggers may not be configured yet.
        import logging
        logging.getLogger("scanner.startup").debug(
            "Raw-layer prune skipped: %s", exc,
        )

    # One-time migration: re-dedup the on-disk earnings_history.parquet
    # under the new gap-fill per-(ticker, period_ending) source priority.
    # No-op after the first successful run (gated on a sentinel file).
    try:
        from .. import earnings_history
        earnings_history.migrate_to_gap_fill_dedup()
    except Exception as exc:
        import logging
        logging.getLogger("scanner.startup").warning(
            "gap_fill_dedup migration skipped: %s", exc,
        )

    # One-time migration: drop calendar-vs-fiscal phantom duplicates —
    # calendar-normed finnhub rows whose calendar quarter is already covered
    # by a fiscal-accurate finviz/zacks row (separate sentinel from the
    # gap-fill dedup above so installs whose gap-fill flag is already set
    # still run this once).
    try:
        from .. import earnings_history
        earnings_history.migrate_calendar_dedup()
    except Exception as exc:
        import logging
        logging.getLogger("scanner.startup").warning(
            "calendar_dedup migration skipped: %s", exc,
        )

    # One-time migration: backfill deeper finviz history from the raw layer
    # after the EARNINGS_HISTORY_YEARS cap was raised 5y → 10y. Network-free
    # (replays raw rows through the production converter); no-op after the
    # first successful run (sentinel-gated).
    try:
        from .. import earnings_history
        earnings_history.migrate_backfill_finviz_history_from_raw()
    except Exception as exc:
        import logging
        logging.getLogger("scanner.startup").warning(
            "finviz_backfill migration skipped: %s", exc,
        )

    # One-time migration: null reverse-split EPS artifacts (price-relative).
    # Runs after the backfill so it also cleans the recovered older quarters.
    try:
        from .. import earnings_history
        earnings_history.migrate_sanitize_absurd_eps()
    except Exception as exc:
        import logging
        logging.getLogger("scanner.startup").warning(
            "eps_sanitize migration skipped: %s", exc,
        )

    app = QApplication(sys.argv)
    app.setStyleSheet(DARK_STYLESHEET)

    # First-launch only: ask for the Finnhub API key, then the Zacks
    # cookie string. Both are silent if already set. Order: Finnhub first
    # (faster to enter — single line) so the user gets through it
    # quickly, Zacks second (multi-line paste).
    prompt_finnhub_key_if_missing()
    prompt_zacks_cookies_if_missing()

    window = MainWindow()
    window.show()

    # Restore the saved window geometry (size + position) from the previous
    # session if present; otherwise center a default 1500x900 on the primary
    # screen. Done AFTER show() so geometry is resolved.
    restored = False
    try:
        saved = window._qsettings().value("main_window/geometry")
        if saved:
            restored = bool(window.restoreGeometry(saved))
    except Exception:
        restored = False
    if not restored:
        screen = QApplication.primaryScreen()
        if screen:
            geo = screen.availableGeometry()
            w, h = 1500, 900
            x = geo.x() + (geo.width() - w) // 2
            y = geo.y() + (geo.height() - h) // 2
            window.setGeometry(x, y, w, h)

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
