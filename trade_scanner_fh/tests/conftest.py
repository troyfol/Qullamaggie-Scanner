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


@pytest.fixture
def tmp_parquets(tmp_path, monkeypatch):
    """Redirect both earnings parquet paths (and DATA_DIR) to a tmp
    directory so tests never touch the user's real cache."""
    from trade_scanner_fh import config
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "earnings_history.parquet")
    monkeypatch.setattr(config, "EARNINGS_PARQUET",
                        tmp_path / "earnings_dates.parquet")
    return tmp_path


@pytest.fixture
def fake_scan_cache(tmp_path, monkeypatch):
    """Wire every cache directory + parquet path into tmp_path."""
    from trade_scanner_fh import config
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "PARQUET_DIR", tmp_path / "ohlcv")
    monkeypatch.setattr(config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "earnings_history.parquet")
    monkeypatch.setattr(config, "EARNINGS_PARQUET",
                        tmp_path / "earnings_dates.parquet")
    monkeypatch.setattr(config, "SECTOR_MAP_PARQUET",
                        tmp_path / "sector_map.parquet")
    (tmp_path / "ohlcv").mkdir(parents=True, exist_ok=True)
    return tmp_path
