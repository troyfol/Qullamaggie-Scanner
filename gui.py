"""
Trading Scanner — PyQt6 Desktop GUI (Phase 3)
================================================
Launch:  python -m trading_scanner.gui

Layout:
  ┌─────────────────────────────────────────────────────┐
  │  Toolbar: [Preset ▾] [Save] [Load] [Date pickers]  │
  ├──────────────┬──────────────────────────────────────┤
  │  Indicator   │  Results Table (sortable)            │
  │  Controls    │                                      │
  │  (scroll)    │                                      │
  │              ├──────────────────────────────────────┤
  │              │  Log Panel (expandable)              │
  │              ├──────────────────────────────────────┤
  │              │  Status bar + pre-transfer summary   │
  ├──────────────┴──────────────────────────────────────┤
  │  [Run Scan]  [Stop]  [Send to TradeStation]         │
  └─────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq_reader

from PyQt6.QtCore import (
    QDate, QSortFilterProxyModel, Qt, QThread, pyqtSignal, pyqtSlot,
)
from PyQt6.QtGui import QAction, QColor, QFont, QStandardItem, QStandardItemModel
from PyQt6.QtWidgets import (
    QApplication, QCheckBox, QComboBox, QDateEdit, QDialog,
    QDoubleSpinBox, QFileDialog, QGroupBox, QHBoxLayout, QHeaderView,
    QLabel, QLineEdit, QMainWindow, QMenu, QMessageBox, QPushButton,
    QScrollArea, QSizePolicy, QSplitter, QSpinBox, QStatusBar,
    QTableView, QTextEdit, QToolBar, QVBoxLayout, QWidget,
)

from . import config
from .scanner import ScanParams, ScanResult, run_scan
from .data_engine import download_one, load_ohlcv, _last_cached_date
from .ticker_universe import load_universe, refresh_universe
from .tradestation import TradeStationBridge, BridgeConfig

log = logging.getLogger("scanner.gui")

PRESETS_DIR = config.DATA_DIR / "presets"
PRESETS_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# Log handler that emits to the GUI
# ============================================================================

class QtLogHandler(logging.Handler):
    """Routes log records to a signal so the GUI can display them."""

    def __init__(self, signal):
        super().__init__()
        self._signal = signal

    def emit(self, record):
        try:
            msg = self.format(record)
            self._signal.emit(msg)
        except Exception:
            pass


# ============================================================================
# Background scan worker
# ============================================================================

class ScanWorker(QThread):
    """Runs run_scan() on a background thread."""

    progress = pyqtSignal(int, int, str)   # done, total, symbol
    finished = pyqtSignal(object)          # ScanResult
    log_msg = pyqtSignal(str)              # for log panel

    def __init__(self, symbols: list[str], params: ScanParams):
        super().__init__()
        self.symbols = symbols
        self.params = params
        self._stop_requested = False

    def run(self):
        def cb(done, total, sym):
            self.progress.emit(done, total, sym)

        result = run_scan(self.symbols, self.params, progress_cb=cb)
        self.finished.emit(result)

    def request_stop(self):
        self._stop_requested = True


# ============================================================================
# Universe refresh worker (first-run or stale universe CSV)
# ============================================================================

class UniverseWorker(QThread):
    """Downloads the ticker universe CSV in the background."""

    finished = pyqtSignal(object)  # pd.DataFrame or None
    log_msg = pyqtSignal(str)

    def run(self):
        try:
            self.log_msg.emit("Downloading ticker universe (first run)...")
            df = refresh_universe(force=True, skip_validation=True)
            self.log_msg.emit(f"Universe downloaded: {len(df)} tickers")
            self.finished.emit(df)
        except Exception as exc:
            self.log_msg.emit(f"Universe download failed: {exc}")
            self.finished.emit(None)


class UniverseRefreshWorker(QThread):
    """Refreshes the universe CSV if stale (>7 days). Fast no-op if fresh."""

    finished = pyqtSignal(object)
    log_msg = pyqtSignal(str)

    def run(self):
        try:
            df = refresh_universe(force=False, skip_validation=True)
            self.log_msg.emit(f"Universe: {len(df)} tickers")
            self.finished.emit(df)
        except Exception as exc:
            self.log_msg.emit(f"Universe refresh failed: {exc}")
            self.finished.emit(None)


# ============================================================================
# OHLCV update worker (incremental daily refresh)
# ============================================================================

class UpdateWorker(QThread):
    """Incrementally updates OHLCV parquet files in the background."""

    progress = pyqtSignal(int, int)     # done, total
    finished = pyqtSignal(int, int)     # updated, errors
    log_msg = pyqtSignal(str)

    def __init__(self, symbols: list[str], *,
                 backoff_enabled_ref: list | None = None,
                 backoff_threshold: int = 10,
                 backoff_wait: int = 30,
                 max_retries: int = 3):
        super().__init__()
        self.symbols = symbols
        # Mutable container so the GUI toggle takes effect immediately
        self._backoff_enabled_ref = backoff_enabled_ref or [True]
        self.backoff_threshold = backoff_threshold
        self.backoff_wait = backoff_wait
        self.max_retries = max_retries
        self._stop = False

    def request_stop(self):
        self._stop = True

    def run(self):
        try:
            self._do_update()
        except Exception as exc:
            log.error("UpdateWorker crashed: %s", exc, exc_info=True)
            self.log_msg.emit(f"OHLCV update error: {exc}")
            self.finished.emit(0, 0)

    def _do_update(self):
        import pandas as pd
        from datetime import timedelta
        from zoneinfo import ZoneInfo

        now_et = datetime.now(ZoneInfo("America/New_York"))
        today_et = now_et.date()
        market_closed = now_et.hour >= 17  # 5 PM ET (buffer past 4 PM close)

        # Determine the most recent trading day we should have data for
        if market_closed:
            # Market closed today — we expect today's data
            target_date = today_et
        else:
            # Market hasn't closed yet — we only expect yesterday's data
            target_date = today_et - timedelta(days=1)

        # Walk back past weekends
        wd = target_date.weekday()
        if wd == 5:      # Saturday
            target_date -= timedelta(days=1)
        elif wd == 6:    # Sunday
            target_date -= timedelta(days=2)

        target = pd.Timestamp(target_date)

        self.log_msg.emit("Checking OHLCV staleness...")
        log.info("Checking OHLCV staleness for %d tickers "
                 "(target date: %s, market_closed: %s)",
                 len(self.symbols), target_date, market_closed)

        stale = []
        missing = []
        for sym in self.symbols:
            last = _last_cached_date(sym)
            if last is None:
                missing.append(sym)
            else:
                # Normalize to tz-naive for comparison
                if last.tzinfo is not None:
                    last = last.tz_localize(None)
                if last < target:
                    stale.append(sym)

        to_update = stale + missing
        if not to_update:
            self.log_msg.emit("OHLCV data is up to date.")
            log.info("OHLCV data is up to date.")
            self.finished.emit(0, 0)
            return

        msg = (f"OHLCV update: {len(stale)} stale, {len(missing)} missing "
               f"-> updating {len(to_update)} tickers...")
        self.log_msg.emit(msg)
        log.info(msg)

        updated = 0
        errors = 0
        total = len(to_update)
        consec_fails = 0
        max_retries = self.max_retries
        backoff_threshold = self.backoff_threshold
        backoff_wait_base = self.backoff_wait

        i = 0
        idx = 0
        while idx < len(to_update):
            if self._stop:
                self.log_msg.emit(f"OHLCV update stopped at {i}/{total}")
                break

            sym = to_update[idx]
            i += 1

            try:
                res = download_one(sym)
                if res.status == "ok":
                    updated += 1
                    consec_fails = 0
                else:
                    errors += 1
                    consec_fails += 1
            except Exception as exc:
                errors += 1
                consec_fails += 1
                log.debug("OHLCV download error for %s: %s", sym, exc)

            idx += 1

            # Rate-limit detection: if we see many consecutive failures, back off
            if self._backoff_enabled_ref[0] and consec_fails >= backoff_threshold:
                for retry in range(1, max_retries + 1):
                    wait = backoff_wait_base * retry
                    self.log_msg.emit(
                        f"Rate limited ({consec_fails} consecutive errors). "
                        f"Backing off {wait}s (retry {retry}/{max_retries})..."
                    )
                    log.warning("Rate limit detected, backing off %ds", wait)

                    # Sleep in 5s chunks so we can check for stop requests
                    for _ in range(wait // 5):
                        if self._stop:
                            break
                        time.sleep(5)
                    if self._stop:
                        break

                    # Test with one ticker to see if we're unblocked
                    try:
                        test_res = download_one(to_update[min(idx, len(to_update) - 1)])
                        if test_res.status == "ok":
                            updated += 1
                            consec_fails = 0
                            idx += 1
                            self.log_msg.emit("Rate limit cleared, resuming...")
                            log.info("Rate limit cleared after %ds backoff", wait)
                            break
                    except Exception:
                        pass
                else:
                    # All retries exhausted
                    self.log_msg.emit(
                        f"Rate limit persists after {max_retries} retries. "
                        f"Stopping update. Will retry remaining on next launch."
                    )
                    log.warning("Rate limit persists, stopping update")
                    break

                if self._stop:
                    break
                continue

            # Normal polite pause
            time.sleep(config.YFINANCE_PAUSE_SEC)

            if i % 200 == 0 or i == total:
                self.progress.emit(i, total)
                self.log_msg.emit(f"OHLCV update: {i}/{total} ({updated} ok, {errors} err)")
                log.info("OHLCV update: %d/%d (%d ok, %d err)", i, total, updated, errors)

        msg = f"OHLCV update complete: {updated} updated, {errors} errors"
        self.log_msg.emit(msg)
        log.info(msg)
        self.finished.emit(updated, errors)


# ============================================================================
# Indicator control row widget
# ============================================================================

class IndicatorRow(QWidget):
    """A single indicator: checkbox toggle + label + param spinboxes."""

    def __init__(self, label: str, params: list[dict], parent=None):
        """
        params: list of dicts, each with keys:
            'name'    — internal key
            'label'   — display label
            'type'    — 'int' or 'float'
            'default' — default value
            'min', 'max', 'step' — optional
        """
        super().__init__(parent)
        self.toggle = QCheckBox()
        self.toggle.setChecked(True)
        self.toggle.setToolTip("Enable/disable this indicator")

        lbl = QLabel(label)
        lbl.setMinimumWidth(180)
        lbl.setFont(QFont("Segoe UI", 9, QFont.Weight.Bold))

        self.spinboxes: dict[str, QDoubleSpinBox | QSpinBox] = {}

        layout = QHBoxLayout(self)
        layout.setContentsMargins(4, 2, 4, 2)
        layout.addWidget(self.toggle)
        layout.addWidget(lbl)

        for p in params:
            plbl = QLabel(p["label"] + ":")
            plbl.setStyleSheet("color: #888;")
            layout.addWidget(plbl)

            if p["type"] == "int":
                sb = QSpinBox()
                sb.setMinimum(p.get("min", 0))
                sb.setMaximum(p.get("max", 999999))
                sb.setSingleStep(p.get("step", 1))
                sb.setValue(p["default"])
            else:
                sb = QDoubleSpinBox()
                sb.setMinimum(p.get("min", 0.0))
                sb.setMaximum(p.get("max", 999999999.0))
                sb.setSingleStep(p.get("step", 0.01))
                sb.setDecimals(p.get("decimals", 2))
                sb.setValue(p["default"])

            sb.setFixedWidth(100)
            self.spinboxes[p["name"]] = sb
            layout.addWidget(sb)

        layout.addStretch()

    def is_enabled(self) -> bool:
        return self.toggle.isChecked()

    def value(self, name: str):
        return self.spinboxes[name].value()

    def set_enabled(self, on: bool):
        self.toggle.setChecked(on)

    def set_value(self, name: str, val):
        if name in self.spinboxes:
            self.spinboxes[name].setValue(val)


# ============================================================================
# Indicator controls panel
# ============================================================================

class IndicatorPanel(QScrollArea):
    """Scrollable panel with all 14 indicator controls."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWidgetResizable(True)
        self.setMinimumWidth(580)
        self.setMaximumWidth(720)

        container = QWidget()
        self.vbox = QVBoxLayout(container)
        self.vbox.setSpacing(2)
        self.vbox.setContentsMargins(6, 6, 16, 6)

        self.rows: dict[str, IndicatorRow] = {}

        # --- Trend Filters ---
        self._section("Trend Filters")

        self._add("sma1", "Price > SMA (1st)", [
            {"name": "period", "label": "Period", "type": "int", "default": 200, "min": 1, "max": 500},
        ])
        self._add("sma2", "Price > SMA (2nd)", [
            {"name": "period", "label": "Period", "type": "int", "default": 50, "min": 1, "max": 500},
        ])
        self._add("sti", "Stockbee Trend Intensity", [
            {"name": "short_lb", "label": "Short", "type": "int", "default": 7, "min": 1, "max": 200},
            {"name": "long_lb", "label": "Long", "type": "int", "default": 65, "min": 1, "max": 500},
            {"name": "threshold", "label": "Min", "type": "float", "default": 1.05, "min": 0.5, "max": 3.0, "step": 0.01},
        ])
        self._add("dist_high", "Distance from High (%)", [
            {"name": "max_pct", "label": "Max %", "type": "float", "default": 5.0, "min": 0.0, "max": 100.0, "step": 0.5},
        ])

        # --- Momentum / Prior Move ---
        self._section("Momentum / Prior Move")

        self._add("pct_gain", "% Gain over Period", [
            {"name": "min_gain", "label": "Min %", "type": "float", "default": 20.0, "min": -999.0, "max": 9999.0, "step": 1.0},
        ])
        self._add("top_pct", "Top X Percentile (of Gain)", [
            {"name": "cutoff", "label": "Top %", "type": "float", "default": 10.0, "min": 0.1, "max": 100.0, "step": 1.0},
        ])
        self.rows["top_pct"].set_enabled(False)

        self._add("consec_gaps", "Consecutive Gap-Ups", [
            {"name": "min_gaps", "label": "Min", "type": "int", "default": 2, "min": 1, "max": 50},
        ])
        self.rows["consec_gaps"].set_enabled(False)

        self._add("current_gap", "Current Gap %", [
            {"name": "min_pct", "label": "Min %", "type": "float", "default": 2.0, "min": 0.0, "max": 100.0, "step": 0.5},
        ])
        self.rows["current_gap"].set_enabled(False)

        self._add("adr", "ADR% (Avg Daily Range)", [
            {"name": "lookback", "label": "Days", "type": "int", "default": 14, "min": 1, "max": 200},
            {"name": "min_pct", "label": "Min %", "type": "float", "default": 3.0, "min": 0.0, "max": 100.0, "step": 0.5},
        ])

        # --- Volatility Contraction ---
        self._section("Volatility Contraction")

        self._add("bbw", "Bollinger Band Width", [
            {"name": "period", "label": "Period", "type": "int", "default": 20, "min": 1, "max": 200},
            {"name": "num_std", "label": "Std", "type": "float", "default": 2.0, "min": 0.1, "max": 5.0, "step": 0.1},
            {"name": "max_bbw", "label": "Max", "type": "float", "default": 0.10, "min": 0.0, "max": 5.0, "step": 0.01},
        ])
        self.rows["bbw"].set_enabled(False)

        self._add("atr_ratio", "ATR Ratio (Short/Long)", [
            {"name": "short", "label": "Short", "type": "int", "default": 5, "min": 1, "max": 100},
            {"name": "long", "label": "Long", "type": "int", "default": 50, "min": 1, "max": 500},
            {"name": "max_ratio", "label": "Max", "type": "float", "default": 0.75, "min": 0.0, "max": 5.0, "step": 0.05},
        ])
        self.rows["atr_ratio"].set_enabled(False)

        # --- Volume / Liquidity ---
        self._section("Volume / Liquidity")

        self._add("vol_dryup", "Volume Dry-Up Ratio", [
            {"name": "recent", "label": "Recent", "type": "int", "default": 10, "min": 1, "max": 200},
            {"name": "prior", "label": "Prior", "type": "int", "default": 20, "min": 1, "max": 200},
            {"name": "max_ratio", "label": "Max", "type": "float", "default": 0.70, "min": 0.0, "max": 5.0, "step": 0.05},
        ])
        self.rows["vol_dryup"].set_enabled(False)

        self._add("min_price", "Minimum Price", [
            {"name": "floor", "label": "$", "type": "float", "default": 10.0, "min": 0.0, "max": 99999.0, "step": 1.0},
        ])
        self._add("avg_vol", "Average Volume (shares)", [
            {"name": "lookback", "label": "Days", "type": "int", "default": 20, "min": 1, "max": 200},
            {"name": "min_vol", "label": "Min", "type": "float", "default": 200000.0, "min": 0.0, "max": 999999999.0, "step": 10000.0, "decimals": 0},
        ])
        self._add("dollar_vol", "Dollar Volume", [
            {"name": "lookback", "label": "Days", "type": "int", "default": 20, "min": 1, "max": 200},
            {"name": "min_dv", "label": "Min $", "type": "float", "default": 5000000.0, "min": 0.0, "max": 999999999999.0, "step": 1000000.0, "decimals": 0},
        ])

        self.vbox.addStretch()
        self.setWidget(container)

    def _section(self, title: str):
        lbl = QLabel(f"  {title}")
        lbl.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        lbl.setStyleSheet("color: #4a90d9; margin-top: 8px;")
        self.vbox.addWidget(lbl)

    def _add(self, key: str, label: str, params: list[dict]):
        row = IndicatorRow(label, params)
        self.rows[key] = row
        self.vbox.addWidget(row)

    def build_scan_params(self, start: date, end: date) -> ScanParams:
        """Read all controls and build a ScanParams."""
        r = self.rows
        return ScanParams(
            start_date=start,
            end_date=end,
            # SMA 1
            sma1_enabled=r["sma1"].is_enabled(),
            sma1_period=r["sma1"].value("period"),
            # SMA 2
            sma2_enabled=r["sma2"].is_enabled(),
            sma2_period=r["sma2"].value("period"),
            # STI
            sti_enabled=r["sti"].is_enabled(),
            sti_short_lb=r["sti"].value("short_lb"),
            sti_long_lb=r["sti"].value("long_lb"),
            sti_threshold=r["sti"].value("threshold"),
            # Distance from high
            dist_high_enabled=r["dist_high"].is_enabled(),
            dist_high_max_pct=r["dist_high"].value("max_pct"),
            # % Gain
            pct_gain_enabled=r["pct_gain"].is_enabled(),
            pct_gain_min=r["pct_gain"].value("min_gain"),
            # Top percentile
            top_pct_enabled=r["top_pct"].is_enabled(),
            top_pct_cutoff=r["top_pct"].value("cutoff"),
            # Consecutive gaps
            consec_gaps_enabled=r["consec_gaps"].is_enabled(),
            consec_gaps_min=int(r["consec_gaps"].value("min_gaps")),
            # Current gap
            current_gap_enabled=r["current_gap"].is_enabled(),
            current_gap_min_pct=r["current_gap"].value("min_pct"),
            # ADR (momentum — minimum)
            adr_enabled=r["adr"].is_enabled(),
            adr_lookback=r["adr"].value("lookback"),
            adr_min_pct=r["adr"].value("min_pct"),
            # BBW
            bbw_enabled=r["bbw"].is_enabled(),
            bbw_period=r["bbw"].value("period"),
            bbw_num_std=r["bbw"].value("num_std"),
            bbw_max=r["bbw"].value("max_bbw"),
            # ATR ratio
            atr_ratio_enabled=r["atr_ratio"].is_enabled(),
            atr_short=r["atr_ratio"].value("short"),
            atr_long=r["atr_ratio"].value("long"),
            atr_max_ratio=r["atr_ratio"].value("max_ratio"),
            # Volume dry-up
            vol_dryup_enabled=r["vol_dryup"].is_enabled(),
            vol_dryup_recent=r["vol_dryup"].value("recent"),
            vol_dryup_prior=r["vol_dryup"].value("prior"),
            vol_dryup_max_ratio=r["vol_dryup"].value("max_ratio"),
            # Min price
            min_price_enabled=r["min_price"].is_enabled(),
            min_price_floor=r["min_price"].value("floor"),
            # Avg volume
            avg_vol_enabled=r["avg_vol"].is_enabled(),
            avg_vol_lookback=r["avg_vol"].value("lookback"),
            avg_vol_min=r["avg_vol"].value("min_vol"),
            # Dollar volume
            dollar_vol_enabled=r["dollar_vol"].is_enabled(),
            dollar_vol_lookback=r["dollar_vol"].value("lookback"),
            dollar_vol_min=r["dollar_vol"].value("min_dv"),
        )

    def to_dict(self) -> dict:
        """Serialize all control states to a dict (for preset save)."""
        out = {}
        for key, row in self.rows.items():
            entry = {"enabled": row.is_enabled()}
            for pname, sb in row.spinboxes.items():
                entry[pname] = sb.value()
            out[key] = entry
        return out

    def from_dict(self, d: dict):
        """Restore control states from a dict (for preset load)."""
        for key, entry in d.items():
            if key not in self.rows:
                continue
            row = self.rows[key]
            row.set_enabled(entry.get("enabled", True))
            for pname, val in entry.items():
                if pname == "enabled":
                    continue
                row.set_value(pname, val)


# ============================================================================
# Results table
# ============================================================================

# Column definitions: (header, dict_key, format_func)
RESULT_COLUMNS = [
    ("Ticker",        "symbol",         str),
    ("Close",         "close",          lambda x: f"${x:.2f}"),
    ("% Gain",        "pct_gain",       lambda x: f"{x:.1f}%"),
    ("STI",           "sti",            lambda x: f"{x:.3f}"),
    ("Dist High %",   "dist_high_pct",  lambda x: f"{x:.1f}%"),
    ("ADR%",          "adr_pct",        lambda x: f"{x:.2f}%"),
    ("BBW",           "bbw",            lambda x: f"{x:.4f}"),
    ("ATR Ratio",     "atr_ratio",      lambda x: f"{x:.3f}"),
    ("ConsecGaps",    "consec_gaps",    lambda x: str(int(x))),
    ("Gap%",          "current_gap_pct",lambda x: f"{x:.2f}%"),
    ("VolDryUp",      "vol_dryup",      lambda x: f"{x:.3f}"),
    ("Avg Vol",       "avg_vol",        lambda x: f"{x:,.0f}"),
    ("$ Vol",         "dollar_vol",     lambda x: f"${x:,.0f}"),
]


class NumericSortItem(QStandardItem):
    """QStandardItem that sorts numerically by stored data role."""

    def __lt__(self, other):
        lhs = self.data(Qt.ItemDataRole.UserRole)
        rhs = other.data(Qt.ItemDataRole.UserRole)
        if lhs is None:
            return True
        if rhs is None:
            return False
        try:
            return float(lhs) < float(rhs)
        except (ValueError, TypeError):
            return str(lhs) < str(rhs)


class ResultsTable(QTableView):
    """Sortable results table."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.model_src = QStandardItemModel()
        self.model_src.setHorizontalHeaderLabels([c[0] for c in RESULT_COLUMNS])

        self.proxy = QSortFilterProxyModel()
        self.proxy.setSourceModel(self.model_src)
        self.setModel(self.proxy)

        self.setSortingEnabled(True)
        self.setAlternatingRowColors(True)
        self.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self.horizontalHeader().setStretchLastSection(True)
        self.verticalHeader().setDefaultSectionSize(24)

    def populate(self, df):
        """Fill table from a scan results DataFrame."""
        self.model_src.removeRows(0, self.model_src.rowCount())

        if df is None or df.empty:
            return

        for _, row_data in df.iterrows():
            items = []
            for header, key, fmt in RESULT_COLUMNS:
                val = row_data.get(key)
                item = NumericSortItem()
                if val is not None and str(val) != "nan":
                    try:
                        item.setText(fmt(val))
                    except Exception:
                        item.setText(str(val))
                    item.setData(val, Qt.ItemDataRole.UserRole)
                else:
                    item.setText("N/A")
                    item.setData(None, Qt.ItemDataRole.UserRole)
                item.setEditable(False)
                items.append(item)
            self.model_src.appendRow(items)

        self.resizeColumnsToContents()

    def get_symbols(self) -> list[str]:
        """Return list of ticker symbols currently in the table."""
        symbols = []
        for r in range(self.model_src.rowCount()):
            item = self.model_src.item(r, 0)
            if item:
                symbols.append(item.text())
        return symbols


# ============================================================================
# Log panel
# ============================================================================

class LogPanel(QWidget):
    """Expandable log panel with real-time output and disk persistence."""

    append_signal = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.text = QTextEdit()
        self.text.setReadOnly(True)
        self.text.setFont(QFont("Consolas", 9))
        self.text.setStyleSheet("background: #1e1e1e; color: #d4d4d4;")

        btn_clear = QPushButton("Clear")
        btn_clear.setFixedWidth(60)
        btn_clear.clicked.connect(self.text.clear)

        top = QHBoxLayout()
        top.addWidget(QLabel("Log"))
        top.addStretch()
        top.addWidget(btn_clear)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addLayout(top)
        layout.addWidget(self.text)

        self.append_signal.connect(self._append)

        # Disk log file
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._log_path = config.LOG_DIR / f"scan_{ts}.log"

    @pyqtSlot(str)
    def _append(self, msg: str):
        self.text.append(msg)
        # Auto-scroll
        sb = self.text.verticalScrollBar()
        sb.setValue(sb.maximum())
        # Persist to disk
        try:
            with open(self._log_path, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except Exception:
            pass

    def write_line(self, msg: str):
        self.append_signal.emit(msg)


# ============================================================================
# TradeStation bridge worker + config dialog
# ============================================================================

class BridgeWorker(QThread):
    """Runs TradeStationBridge.start() on a background thread."""

    countdown_tick = pyqtSignal(int)       # remaining seconds
    ticker_sent = pyqtSignal(int, int, str, bool)  # idx, total, sym, dry_run
    done = pyqtSignal(int, int)            # sent, skipped
    log_msg = pyqtSignal(str)

    def __init__(self, symbols: list[str], cfg: BridgeConfig):
        super().__init__()
        self.bridge = TradeStationBridge(symbols, cfg)
        self.bridge.on_countdown = lambda r: self.countdown_tick.emit(r)
        self.bridge.on_ticker_sent = lambda i, t, s, d: self.ticker_sent.emit(i, t, s, d)
        self.bridge.on_done = lambda s, k: self.done.emit(s, k)
        self.bridge.on_log = lambda m: self.log_msg.emit(m)

    def run(self):
        self.bridge.start()

    def request_stop(self):
        self.bridge.request_stop()


class TradeStationDialog(QDialog):
    """
    Modal config dialog shown before launching the bridge.
    """

    start_requested = pyqtSignal(BridgeConfig)
    cancelled = pyqtSignal()

    def __init__(self, n_tickers: int, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Send to TradeStation")
        self.setModal(True)
        self.setMinimumWidth(500)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(40, 30, 40, 30)

        title = QLabel(f"Send {n_tickers} Tickers to TradeStation")
        title.setFont(QFont("Segoe UI", 14, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        layout.addWidget(QLabel(""))

        # Delay
        row1 = QHBoxLayout()
        row1.addWidget(QLabel("Delay between tickers (sec):"))
        self.spin_delay = QDoubleSpinBox()
        self.spin_delay.setRange(0.1, 10.0)
        self.spin_delay.setSingleStep(0.1)
        self.spin_delay.setValue(0.8)
        self.spin_delay.setMinimumWidth(80)
        row1.addWidget(self.spin_delay)
        row1.addStretch()
        layout.addLayout(row1)

        # Countdown
        row2 = QHBoxLayout()
        row2.addWidget(QLabel("Countdown before start (sec):"))
        self.spin_countdown = QSpinBox()
        self.spin_countdown.setRange(1, 30)
        self.spin_countdown.setValue(5)
        self.spin_countdown.setMinimumWidth(80)
        row2.addWidget(self.spin_countdown)
        row2.addStretch()
        layout.addLayout(row2)

        # Confirm key
        row3 = QHBoxLayout()
        row3.addWidget(QLabel("Key to press after each ticker:"))
        self.combo_key = QComboBox()
        self.combo_key.addItems(["enter", "tab", "space"])
        self.combo_key.setMinimumWidth(100)
        row3.addWidget(self.combo_key)
        row3.addStretch()
        layout.addLayout(row3)

        # Dry run
        self.chk_dry = QCheckBox("Dry run (log only, don't type)")
        layout.addWidget(self.chk_dry)

        layout.addWidget(QLabel(""))

        # Instructions
        instr = QLabel(
            "1. Click Start below\n"
            "2. During the countdown, click into TradeStation's watchlist search bar\n"
            "3. The scanner will type each ticker and press the confirm key\n"
            "4. Press Escape or move mouse to top-left corner to abort"
        )
        instr.setStyleSheet("color: #aaa; font-style: italic;")
        layout.addWidget(instr)

        layout.addWidget(QLabel(""))

        # Buttons
        btn_row = QHBoxLayout()
        btn_start = QPushButton("  Start  ")
        btn_start.setStyleSheet(
            "QPushButton { background: #2e7d32; color: white; font-size: 13px; "
            "font-weight: bold; padding: 8px 24px; border-radius: 4px; }"
        )
        btn_start.clicked.connect(self._on_start)
        btn_row.addWidget(btn_start)

        btn_cancel = QPushButton("  Cancel  ")
        btn_cancel.setStyleSheet(
            "QPushButton { background: #555; color: white; font-size: 13px; "
            "padding: 8px 24px; border-radius: 4px; }"
        )
        btn_cancel.clicked.connect(self._on_cancel)
        btn_row.addWidget(btn_cancel)

        btn_row.addStretch()
        layout.addLayout(btn_row)

    def _on_start(self):
        cfg = BridgeConfig(
            delay_between_tickers=self.spin_delay.value(),
            countdown_seconds=self.spin_countdown.value(),
            dry_run=self.chk_dry.isChecked(),
            confirm_key=self.combo_key.currentText(),
        )
        self.start_requested.emit(cfg)
        self.accept()

    def _on_cancel(self):
        self.cancelled.emit()
        self.reject()


# ============================================================================
# Main window
# ============================================================================

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Trading Scanner")
        self.setMinimumSize(1280, 800)
        self.resize(1500, 900)

        self._worker: Optional[ScanWorker] = None
        self._update_worker: Optional[UpdateWorker] = None
        self._universe_worker: Optional[UniverseWorker] = None
        self._symbols: list[str] = []
        self._universe_df: Optional[pd.DataFrame] = None

        self._build_ui()
        self._install_log_handler()
        self._startup()

    # ── UI construction ────────────────────────────────────────────────

    def _build_ui(self):
        # --- Toolbar ---
        toolbar = QToolBar("Main")
        toolbar.setMovable(False)
        self.addToolBar(toolbar)

        # Date pickers
        toolbar.addWidget(QLabel("  Start: "))
        self.date_start = QDateEdit()
        self.date_start.setCalendarPopup(True)
        self.date_start.setDate(QDate(2025, 1, 1))
        self.date_start.setDisplayFormat("yyyy-MM-dd")
        toolbar.addWidget(self.date_start)

        toolbar.addWidget(QLabel("  End: "))
        self.date_end = QDateEdit()
        self.date_end.setCalendarPopup(True)
        self.date_end.setDate(QDate.currentDate())
        self.date_end.setDisplayFormat("yyyy-MM-dd")
        toolbar.addWidget(self.date_end)

        # Quick date range buttons
        toolbar.addWidget(QLabel("  "))
        _qdr_style = (
            "QPushButton { background: #3a5f8a; color: #ddd; font-size: 11px; "
            "padding: 3px 8px; border-radius: 3px; border: 1px solid #4a7aaa; }"
            "QPushButton:hover { background: #4a7aaa; color: #fff; }"
        )
        for label, days in [("1D", 1), ("1W", 7), ("1M", 30), ("3M", 90), ("6M", 180)]:
            btn = QPushButton(label)
            btn.setFixedWidth(36)
            btn.setStyleSheet(_qdr_style)
            btn.clicked.connect(lambda checked, d=days: self._set_quick_range(d))
            toolbar.addWidget(btn)

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

        data_menu.addSeparator()

        # Backoff toggle — mutable list so running workers see changes live
        self._backoff_enabled_ref = [True]
        self._backoff_threshold = 10
        self._backoff_wait = 30
        self._max_retries = 3

        self.act_backoff_toggle = QAction("Enable Rate-Limit Backoff", self)
        self.act_backoff_toggle.setCheckable(True)
        self.act_backoff_toggle.setChecked(True)
        self.act_backoff_toggle.setToolTip(
            "When enabled, the updater pauses and retries after detecting "
            "many consecutive download failures (rate limiting). "
            "Takes effect immediately, even on a running update."
        )
        self.act_backoff_toggle.toggled.connect(
            lambda v: self._backoff_enabled_ref.__setitem__(0, v)
        )
        data_menu.addAction(self.act_backoff_toggle)

        act_backoff = QAction("Backoff Settings...", self)
        act_backoff.setToolTip("Configure rate-limit detection and retry behavior")
        act_backoff.triggered.connect(self._show_backoff_settings)
        data_menu.addAction(act_backoff)

        data_menu.addSeparator()

        # Ticker blacklist
        self._blacklist: set[str] = set()
        self._load_blacklist()

        act_blacklist = QAction("Ticker Blacklist...", self)
        act_blacklist.setToolTip(
            "Comma-separated list of tickers to always skip during "
            "OHLCV refresh (e.g. delisted or problematic symbols)."
        )
        act_blacklist.triggered.connect(self._show_blacklist_editor)
        data_menu.addAction(act_blacklist)

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

        # Right: results table above, log below
        right_splitter = QSplitter(Qt.Orientation.Vertical)
        self.results_table = ResultsTable()
        right_splitter.addWidget(self.results_table)

        self.log_panel = LogPanel()
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

        btn_bar.addStretch()

        self.btn_send = QPushButton("  Send to TradeStation  ")
        self.btn_send.setEnabled(False)
        self.btn_send.setStyleSheet(
            "QPushButton { background: #1565c0; color: white; font-size: 14px; "
            "padding: 8px 24px; border-radius: 4px; }"
            "QPushButton:hover { background: #1976d2; }"
            "QPushButton:disabled { background: #555; color: #999; }"
        )
        self.btn_send.clicked.connect(self._send_to_tradestation)
        btn_bar.addWidget(self.btn_send)

        main_layout.addLayout(btn_bar)

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

    # ── Log handler ────────────────────────────────────────────────────

    def _install_log_handler(self):
        handler = QtLogHandler(self.log_panel.append_signal)
        handler.setFormatter(logging.Formatter(
            "%(asctime)s  %(name)-22s  %(levelname)-7s  %(message)s",
            datefmt="%H:%M:%S",
        ))
        logging.getLogger("scanner").addHandler(handler)
        logging.getLogger("scanner").setLevel(logging.INFO)

    # ── Universe + Auto-update ────────────────────────────────────────

    def _startup(self):
        """Load universe and kick off OHLCV update. Handles first-run."""
        if not config.TICKER_CSV.exists():
            # First run -- download universe first, then update OHLCV
            self.status.showMessage("First run -- downloading ticker universe...")
            self.log_panel.write_line("First run detected. Downloading ticker universe...")
            self._universe_worker = UniverseWorker()
            self._universe_worker.log_msg.connect(self.log_panel.write_line)
            self._universe_worker.finished.connect(self._on_universe_downloaded)
            self._universe_worker.start()
        else:
            # Refresh universe if stale (picks up new IPOs/delistings)
            # refresh_universe() has a built-in 7-day staleness guard
            self._universe_worker = UniverseRefreshWorker()
            self._universe_worker.log_msg.connect(self.log_panel.write_line)
            self._universe_worker.finished.connect(
                lambda _: self._load_universe_and_update()
            )
            self._universe_worker.start()

    def _on_universe_downloaded(self, df):
        """Called when first-run universe download completes."""
        if df is None or df.empty:
            self.status.showMessage("Universe download failed. Check internet connection.")
            return
        self._load_universe_and_update()

    def _load_universe_and_update(self):
        """Load universe from CSV, then start background OHLCV update."""
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

        self._update_worker = UpdateWorker(
            update_syms,
            backoff_enabled_ref=self._backoff_enabled_ref,
            backoff_threshold=self._backoff_threshold,
            backoff_wait=self._backoff_wait,
            max_retries=self._max_retries,
        )
        self._update_worker.log_msg.connect(self.log_panel.write_line)
        self._update_worker.progress.connect(self._on_update_progress)
        self._update_worker.finished.connect(self._on_update_done)
        self._update_worker.start()

    @pyqtSlot(int, int)
    def _on_update_progress(self, done: int, total: int):
        pct = done * 100 // total if total else 0
        self._update_label.setText(
            f"OHLCV update: {done}/{total} ({pct}%)"
        )
        self._update_label.setStyleSheet(
            "color: #4a90d9; font-size: 11px; padding: 0 8px;"
        )

    @pyqtSlot(int, int)
    def _on_update_done(self, updated: int, errors: int):
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
        except FileNotFoundError:
            pass

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

    # ── Force refresh actions ─────────────────────────────────────────────

    def _force_universe_refresh(self):
        """Menu action: force re-download the full ticker universe."""
        if self._universe_worker and self._universe_worker.isRunning():
            QMessageBox.information(self, "In Progress",
                                    "A universe refresh is already running.")
            return
        self.status.showMessage("Force refreshing ticker universe...")
        self.log_panel.write_line("Force universe refresh requested by user.")
        self._universe_worker = UniverseWorker()
        self._universe_worker.log_msg.connect(self.log_panel.write_line)
        self._universe_worker.finished.connect(self._on_universe_downloaded)
        self._universe_worker.start()

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
        self._load_universe_and_update()

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

    def _reset_yfinance_session(self):
        """Clear yfinance's internal caches to get a fresh HTTP session."""
        import yfinance as yf
        try:
            # Clear the module-level cookie/crumb cache
            if hasattr(yf, 'utils') and hasattr(yf.utils, 'get_json'):
                # yfinance >=0.2.x stores session state internally
                pass
            # The most reliable way: nuke the shared session and cookie jar
            # yfinance uses a module-level _CACHE dict in data module
            for attr in ('_cache', '_CACHE'):
                for mod in (yf, getattr(yf, 'data', None), getattr(yf, 'utils', None)):
                    if mod and hasattr(mod, attr):
                        getattr(mod, attr).clear()
            # Clear any shared session objects
            if hasattr(yf, 'shared') and hasattr(yf.shared, '_REQUESTS_SESSION'):
                yf.shared._REQUESTS_SESSION = None
            # Clear Ticker cache if present
            if hasattr(yf, 'Ticker'):
                for attr in ('_cache', '_tz_cache'):
                    if hasattr(yf.Ticker, attr):
                        try:
                            getattr(yf.Ticker, attr).clear()
                        except Exception:
                            pass
            self.log_panel.write_line(
                "yfinance session reset. Cookies and crumb cache cleared."
            )
            self.status.showMessage("yfinance session reset")
        except Exception as exc:
            self.log_panel.write_line(f"Session reset error: {exc}")

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
        self._update_worker = UpdateWorker(
            missing,
            backoff_enabled_ref=self._backoff_enabled_ref,
            backoff_threshold=self._backoff_threshold,
            backoff_wait=self._backoff_wait,
            max_retries=self._max_retries,
        )
        self._update_worker.log_msg.connect(self.log_panel.write_line)
        self._update_worker.progress.connect(self._on_update_progress)
        self._update_worker.finished.connect(self._on_update_done)
        self._update_worker.start()

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

    _BLACKLIST_FILE = config.DATA_DIR / "blacklist.txt"

    @staticmethod
    def _normalize_ticker(t: str) -> str:
        """Normalize Unicode minus/dash variants to ASCII hyphen."""
        return (t.strip().upper()
                .replace("\u2212", "-")   # minus sign
                .replace("\u2013", "-")   # en dash
                .replace("\u2014", "-"))  # em dash

    def _load_blacklist(self):
        """Load blacklisted tickers from file."""
        if self._BLACKLIST_FILE.exists():
            text = self._BLACKLIST_FILE.read_text(encoding="utf-8").strip()
            self._blacklist = {
                self._normalize_ticker(t)
                for t in text.split(",") if t.strip()
            }
        else:
            self._blacklist = set()

    def _save_blacklist(self):
        """Persist blacklist to file."""
        self._BLACKLIST_FILE.parent.mkdir(parents=True, exist_ok=True)
        self._BLACKLIST_FILE.write_text(
            ", ".join(sorted(self._blacklist)),
            encoding="utf-8",
        )

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

    # ── Universe filters ─────────────────────────────────────────────────

    def _on_ipo_toggle(self, state):
        self.spin_ipo_days.setEnabled(state == Qt.CheckState.Checked.value)

    def _filtered_symbols(self) -> list[str]:
        """Apply universe pre-filters (ETF, ADR, IPO) to self._symbols."""
        syms = list(self._symbols)

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

        # IPO mode: only keep tickers with <= N days of data
        if self.chk_ipo_mode.isChecked():
            max_days = self.spin_ipo_days.value()
            ipo_syms = []
            for s in syms:
                pq = config.PARQUET_DIR / f"{s}.parquet"
                if pq.exists():
                    try:
                        row_count = pq_reader.read_metadata(str(pq)).num_rows
                        if row_count <= max_days:
                            ipo_syms.append(s)
                    except Exception:
                        pass
            syms = ipo_syms

        return syms

    # ── Quick date range ─────────────────────────────────────────────────

    def _set_quick_range(self, days: int):
        """Set start date to (end_date - days), keep end date as-is."""
        end_qd = self.date_end.date()
        start_qd = end_qd.addDays(-days)
        self.date_start.setDate(start_qd)

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

        # Read params from controls
        qd_start = self.date_start.date()
        qd_end = self.date_end.date()
        start = date(qd_start.year(), qd_start.month(), qd_start.day())
        end = date(qd_end.year(), qd_end.month(), qd_end.day())
        params = self.indicator_panel.build_scan_params(start, end)

        self.log_panel.write_line(
            f"Scanning {len(filtered)} tickers "
            f"(filtered from {len(self._symbols)} cached)"
        )

        self.btn_scan.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.btn_send.setEnabled(False)
        self.summary_label.setText("")
        self.status.showMessage("Scanning...")

        self._worker = ScanWorker(filtered, params)
        self._worker.progress.connect(self._on_scan_progress)
        self._worker.finished.connect(self._on_scan_done)
        self._worker.start()

    def _stop_scan(self):
        if self._worker:
            self._worker.request_stop()
            self.log_panel.write_line("Stop requested...")

    @pyqtSlot(int, int, str)
    def _on_scan_progress(self, done: int, total: int, sym: str):
        pct = done * 100 // total
        self.status.showMessage(f"Computing: {done}/{total} ({pct}%)")

    @pyqtSlot(object)
    def _on_scan_done(self, result: ScanResult):
        self.btn_scan.setEnabled(True)
        self.btn_stop.setEnabled(False)

        # Populate results table
        self.results_table.populate(result.results_df)

        n_pass = len(result.results_df)
        n_err = len(result.errors)

        # Enable Send button if there are results
        self.btn_send.setEnabled(n_pass > 0)

        # Pre-transfer summary
        if n_err == 0:
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
            f"Scan complete: {n_pass} results in {result.elapsed_sec:.1f}s"
        )

        self._worker = None

    # ── TradeStation Bridge (Phase 4) ─────────────────────────────────

    def _send_to_tradestation(self):
        symbols = self.results_table.get_symbols()
        if not symbols:
            return

        # Show modal config dialog
        self._ts_dialog = TradeStationDialog(len(symbols), self)
        self._ts_dialog.start_requested.connect(
            lambda cfg: self._launch_bridge(symbols, cfg)
        )
        self._ts_dialog.cancelled.connect(self._close_ts_dialog)
        self._ts_dialog.exec()

    def _close_ts_dialog(self):
        if hasattr(self, "_ts_dialog") and self._ts_dialog:
            self._ts_dialog.hide()
            self._ts_dialog.deleteLater()
            self._ts_dialog = None

    def _launch_bridge(self, symbols: list[str], cfg: BridgeConfig):
        self._close_ts_dialog()

        self.btn_scan.setEnabled(False)
        self.btn_send.setEnabled(False)
        self.btn_stop.setEnabled(True)

        self._bridge_worker = BridgeWorker(symbols, cfg)
        self._bridge_worker.log_msg.connect(self.log_panel.write_line)
        self._bridge_worker.countdown_tick.connect(self._on_bridge_countdown)
        self._bridge_worker.ticker_sent.connect(self._on_bridge_ticker)
        self._bridge_worker.done.connect(self._on_bridge_done)

        # Re-wire stop button to stop the bridge
        self.btn_stop.clicked.disconnect()
        self.btn_stop.clicked.connect(self._stop_bridge)

        self._bridge_worker.start()

    def _stop_bridge(self):
        if hasattr(self, "_bridge_worker") and self._bridge_worker:
            self._bridge_worker.request_stop()

    @pyqtSlot(int)
    def _on_bridge_countdown(self, remaining: int):
        self.status.showMessage(
            f"TradeStation: starting in {remaining}s -- click into watchlist search bar!"
        )

    @pyqtSlot(int, int, str, bool)
    def _on_bridge_ticker(self, idx: int, total: int, sym: str, dry: bool):
        mode = "dry" if dry else "sent"
        self.status.showMessage(f"TradeStation: [{idx}/{total}] {sym} ({mode})")

    @pyqtSlot(int, int)
    def _on_bridge_done(self, sent: int, skipped: int):
        self.btn_scan.setEnabled(True)
        self.btn_send.setEnabled(True)
        self.btn_stop.setEnabled(False)

        # Re-wire stop button back to scan stop
        self.btn_stop.clicked.disconnect()
        self.btn_stop.clicked.connect(self._stop_scan)

        if skipped:
            self.status.showMessage(
                f"TradeStation: {sent} sent, {skipped} skipped (stopped early)"
            )
        else:
            self.status.showMessage(f"TradeStation: {sent} tickers sent successfully")

        self._bridge_worker = None

    # ── Presets ────────────────────────────────────────────────────────

    def _refresh_preset_list(self):
        self.preset_combo.clear()
        self.preset_combo.addItem("(select preset)")
        for p in sorted(PRESETS_DIR.glob("*.json")):
            self.preset_combo.addItem(p.stem)

    def _save_preset(self):
        name = self.preset_combo.currentText()
        if name == "(select preset)" or not name.strip():
            # Prompt for name
            from PyQt6.QtWidgets import QInputDialog
            name, ok = QInputDialog.getText(self, "Save Preset", "Preset name:")
            if not ok or not name.strip():
                return

        data = {
            "indicators": self.indicator_panel.to_dict(),
            "start_date": self.date_start.date().toString("yyyy-MM-dd"),
            "end_date": self.date_end.date().toString("yyyy-MM-dd"),
            "include_etf": self.chk_include_etf.isChecked(),
            "include_adr": self.chk_include_adr.isChecked(),
            "ipo_mode": self.chk_ipo_mode.isChecked(),
            "ipo_max_days": self.spin_ipo_days.value(),
        }

        path = PRESETS_DIR / f"{name.strip()}.json"
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
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
        data = json.loads(path.read_text(encoding="utf-8"))

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
        """Stop background workers on exit."""
        if self._update_worker and self._update_worker.isRunning():
            self._update_worker.request_stop()
            self._update_worker.wait(3000)
        if self._universe_worker and self._universe_worker.isRunning():
            self._universe_worker.terminate()
        super().closeEvent(event)


# ============================================================================
# Entry point
# ============================================================================

def main():
    # Suppress DPI awareness warning on Windows multi-monitor setups
    import os
    os.environ.setdefault("QT_ENABLE_HIGHDPI_SCALING", "1")

    app = QApplication(sys.argv)

    # Dark theme stylesheet
    app.setStyleSheet("""
        QMainWindow, QWidget { background: #2b2b2b; color: #e0e0e0; }
        QToolBar { background: #333; border: none; padding: 4px; spacing: 4px; }
        QMenuBar { background: #333; color: #e0e0e0; }
        QMenuBar::item:selected { background: #4a90d9; }
        QMenu { background: #3c3c3c; color: #e0e0e0; border: 1px solid #555; }
        QMenu::item:selected { background: #4a90d9; }
        QMenu::separator { background: #555; height: 1px; margin: 4px 8px; }
        QLabel { color: #e0e0e0; }
        QGroupBox { color: #e0e0e0; border: 1px solid #555; border-radius: 4px;
                     margin-top: 8px; padding-top: 12px; }
        QGroupBox::title { subcontrol-origin: margin; left: 10px; }
        QCheckBox { color: #e0e0e0; spacing: 4px; }
        QCheckBox::indicator { width: 16px; height: 16px; }
        QSpinBox, QDoubleSpinBox, QDateEdit, QComboBox, QLineEdit {
            background: #3c3c3c; color: #e0e0e0; border: 1px solid #555;
            border-radius: 3px; padding: 2px 4px;
        }
        QSpinBox:focus, QDoubleSpinBox:focus, QDateEdit:focus,
        QComboBox:focus, QLineEdit:focus {
            border: 1px solid #4a90d9;
        }
        QPushButton {
            background: #444; color: #e0e0e0; border: 1px solid #555;
            border-radius: 3px; padding: 4px 12px;
        }
        QPushButton:hover { background: #555; }
        QPushButton:pressed { background: #333; }
        QPushButton:disabled { background: #3a3a3a; color: #666; }
        QTableView {
            background: #1e1e1e; color: #d4d4d4; gridline-color: #444;
            selection-background-color: #264f78; alternate-background-color: #252525;
        }
        QHeaderView::section {
            background: #333; color: #e0e0e0; border: 1px solid #444;
            padding: 4px; font-weight: bold;
        }
        QScrollArea { border: none; }
        QSplitter::handle { background: #444; }
        QStatusBar { background: #333; color: #aaa; }
        QScrollBar:vertical {
            background: #2b2b2b; width: 10px; margin: 0;
        }
        QScrollBar::handle:vertical {
            background: #555; min-height: 20px; border-radius: 5px;
        }
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
            height: 0;
        }
    """)

    window = MainWindow()
    window.show()

    # Center on primary screen AFTER show() so geometry is resolved
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
