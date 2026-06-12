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
    into the user's real tree."""
    monkeypatch.setattr(config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "earnings_history.parquet")
    monkeypatch.setattr(config, "EARNINGS_PARQUET",
                        tmp_path / "earnings_dates.parquet")
    monkeypatch.setattr(config, "FINVIZ_BULK_CHECKPOINT",
                        tmp_path / ".finviz_bulk_checkpoint.json")
    monkeypatch.setattr(config, "FINNHUB_BULK_CHECKPOINT",
                        tmp_path / ".finnhub_bulk_checkpoint.json")
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
