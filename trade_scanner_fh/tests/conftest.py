"""
Pytest configuration for trade_scanner_fh tests.

Adds the parent directory (project root) to sys.path so that
`from trade_scanner_fh.X import Y` works when pytest is invoked from
within the trade_scanner_fh/ directory.

Also hosts the fixtures that were duplicated verbatim across many test
modules: `_qapp`, `tmp_parquets`, and `fake_scan_cache`. Test files keep
local fixtures only where they differ meaningfully (e.g. the per-source
`tmp_world` trees that neutralize each client's rate limiter).
"""
from pathlib import Path
import sys

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


@pytest.fixture(autouse=True)
def _isolate_main_window(monkeypatch, tmp_path_factory):
    """Every test: a real `MainWindow()` must not reach the network or the
    user's registry.

    * `MainWindow.__init__` calls `_startup()`, which starts a real
      universe download on a worker thread; its completion chains into
      `_load_universe_and_update` and a full OHLCV refresh from Yahoo into
      the import-time PARQUET_DIR (the package's dev store) — whenever any
      later test happens to process Qt events. Two test files guarded
      against this locally; three v8.0.0 files did not, and on 2026-09-26
      a suite run refreshed ~1,400 dev-store files and drew a Yahoo rate
      limit. Guarded here once, for every file, present and future.
    * `closeEvent` persists window geometry through `_qsettings()`, i.e.
      into HKCU\\Software\\trade_scanner_fh — every closed test window
      overwrote the real app's saved size and position. Settings now go to
      a throwaway INI file (the real QSettings API, so tests that save and
      read back still work). A test that installs its own fake on an
      instance still wins, as instance attributes shadow the class.

    * The post-update path (`_on_update_done`, driven directly by
      test_nasdaq_weekly_auto.py) launches `rebuild_split_artifacts()` on a
      background thread against the import-time split / anomaly paths, i.e.
      the dev store: every suite run rewrote its split_seam_skip.txt,
      split_anchors.parquet and ohlcv_anomalies.csv (found 2026-09-26). The
      split tests call `rebuild_split_artifacts` DIRECTLY with redirected
      paths, so only the background launcher is stubbed.

    Tests that exercise `_startup` itself would opt out by re-patching;
    none do today.
    """
    try:
        from trade_scanner_fh.gui import main_window as _mw
        from trade_scanner_fh.gui import earnings_coordinator as _ec
        from PyQt6.QtCore import QSettings
    except Exception:
        return
    ini = tmp_path_factory.mktemp("qsettings") / "settings.ini"
    monkeypatch.setattr(_mw.MainWindow, "_startup", lambda self: None)
    monkeypatch.setattr(_mw.MainWindow, "_load_universe_and_update",
                        lambda self, force=False: None)
    monkeypatch.setattr(_ec.EarningsRefreshCoordinator,
                        "_kick_off_split_artifacts_rebuild",
                        lambda self: None)
    monkeypatch.setattr(
        _mw.MainWindow, "_qsettings",
        lambda self: QSettings(str(ini), QSettings.Format.IniFormat),
    )


@pytest.fixture(scope="module")
def _qapp():
    """Module-level QApplication so widget tests can instantiate without
    pytest-qt. The codebase doesn't depend on pytest-qt so we keep tests
    self-contained."""
    from PyQt6.QtWidgets import QApplication
    app = QApplication.instance() or QApplication(sys.argv[:1])
    yield app
    # No teardown — let the process exit normally; QApplication.quit()
    # mid-suite makes subsequent fixture instantiations flaky.


def _redirect_data_dir_derived_paths(config, tmp_path, monkeypatch) -> None:
    """Redirect the config paths computed from DATA_DIR at import time.

    Monkeypatching config.DATA_DIR alone is a trap: these module-level
    Path constants were already baked from the REAL scanner_data/ when
    config was imported, so any fixture that swaps DATA_DIR must swap
    them too or a fill/raw-layer code path under test silently writes
    into the user's real tree.

    Audit 2026-08-16: this list was covering 6 of the 13 DATA_DIR-derived
    constants. `check_schema_version()` in a Pass 2 test wrote a real
    `_schema_version.txt` into the source tree's scanner_data/ohlcv/ —
    exactly the leak the paragraph above warns about. Every remaining
    constant is now redirected, so the trap is closed rather than
    re-documented."""
    monkeypatch.setattr(config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "earnings_history.parquet")
    monkeypatch.setattr(config, "EARNINGS_PARQUET",
                        tmp_path / "earnings_dates.parquet")
    monkeypatch.setattr(config, "FINVIZ_BULK_CHECKPOINT",
                        tmp_path / ".finviz_bulk_checkpoint.json")
    monkeypatch.setattr(config, "FINNHUB_BULK_CHECKPOINT",
                        tmp_path / ".finnhub_bulk_checkpoint.json")
    monkeypatch.setattr(config, "ZACKS_BULK_CHECKPOINT",
                        tmp_path / ".zacks_bulk_checkpoint.json")
    monkeypatch.setattr(config, "PARQUET_SCHEMA_FILE",
                        tmp_path / "ohlcv" / "_schema_version.txt")
    monkeypatch.setattr(config, "FINVIZ_BLACKLIST_FILE",
                        tmp_path / "finviz_blacklist.txt")
    monkeypatch.setattr(config, "FINNHUB_BLACKLIST_FILE",
                        tmp_path / "finnhub_blacklist.txt")
    # v7.0.0 finviz snapshot store. Separate from the earnings-side finviz
    # paths above: different page, different store, different skip list.
    monkeypatch.setattr(config, "FINVIZ_SNAPSHOT_PARQUET",
                        tmp_path / "finviz_snapshot.parquet")
    monkeypatch.setattr(config, "FINVIZ_SNAPSHOT_BLACKLIST_FILE",
                        tmp_path / "finviz_snapshot_blacklist.txt")
    monkeypatch.setattr(config, "FINVIZ_SNAPSHOT_BULK_CHECKPOINT",
                        tmp_path / ".finviz_snapshot_checkpoint.json")
    monkeypatch.setattr(config, "TICKER_CSV", tmp_path / "universe.csv")
    monkeypatch.setattr(config, "FAILED_TICKERS_LOG",
                        tmp_path / "failed_tickers.log")
    monkeypatch.setattr(config, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(config, "FTP_RAW_DIR", tmp_path / "ftp_raw")
    raw_root = tmp_path / "earnings_raw"
    monkeypatch.setattr(config, "RAW_EARNINGS_DIR", raw_root)
    # Pre-create the per-source folders (mirrors config.ensure_dirs and
    # the per-module tmp_raw fixtures) so raw-layer read paths don't
    # trip over a missing directory.
    for src in config.RAW_SOURCES:
        (raw_root / src).mkdir(parents=True, exist_ok=True)


@pytest.fixture
def tmp_parquets(tmp_path, monkeypatch):
    """Redirect both earnings parquet paths (and DATA_DIR, plus every
    other import-time DATA_DIR-derived path: the finviz/finnhub bulk
    checkpoints and the raw earnings layer) to a tmp directory so tests
    never touch the user's real cache."""
    from trade_scanner_fh import config
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    _redirect_data_dir_derived_paths(config, tmp_path, monkeypatch)
    return tmp_path


@pytest.fixture
def fake_scan_cache(tmp_path, monkeypatch):
    """Wire every cache directory + parquet path into tmp_path."""
    from trade_scanner_fh import config
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "PARQUET_DIR", tmp_path / "ohlcv")
    _redirect_data_dir_derived_paths(config, tmp_path, monkeypatch)
    monkeypatch.setattr(config, "SECTOR_MAP_PARQUET",
                        tmp_path / "sector_map.parquet")
    (tmp_path / "ohlcv").mkdir(parents=True, exist_ok=True)
    return tmp_path
