"""Tests for config.py — Phase 1 Fixes R1, R7, R11, R12."""
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from trade_scanner_fh import config


# ----------------------------------------------------------------------
# R1 — APP_ROOT resolves to the package directory in dev mode
# ----------------------------------------------------------------------

def test_app_root_points_to_package_in_dev():
    """In dev (unfrozen) mode, APP_ROOT must be the trade_scanner_fh package
    directory — NOT the venv's Scripts directory."""
    expected = Path(__file__).resolve().parent.parent  # tests/.. = trade_scanner_fh
    assert config.APP_ROOT == expected, (
        f"APP_ROOT resolved to {config.APP_ROOT!r}, expected package dir "
        f"{expected!r}. The dev-vs-packaged path fix may have regressed."
    )


def test_data_dir_beside_package_in_dev():
    """DATA_DIR must live inside the package dir, not the venv."""
    assert config.DATA_DIR.parent == config.APP_ROOT
    assert config.DATA_DIR.name == "scanner_data"
    assert "venv" not in str(config.DATA_DIR)


# ----------------------------------------------------------------------
# R7 — no directory side effects at import time
# ----------------------------------------------------------------------

def test_ensure_dirs_is_idempotent(tmp_path, monkeypatch):
    """ensure_dirs() creates subdirs and is safe to call multiple times."""
    # Redirect config paths to a temp dir
    monkeypatch.setattr(config, "DATA_DIR", tmp_path / "scanner_data")
    monkeypatch.setattr(config, "PARQUET_DIR", tmp_path / "scanner_data" / "ohlcv")
    monkeypatch.setattr(config, "LOG_DIR", tmp_path / "scanner_data" / "logs")
    monkeypatch.setattr(config, "FTP_RAW_DIR", tmp_path / "scanner_data" / "ftp_raw")

    config.ensure_dirs()
    assert (tmp_path / "scanner_data" / "ohlcv").is_dir()
    assert (tmp_path / "scanner_data" / "logs").is_dir()
    assert (tmp_path / "scanner_data" / "ftp_raw").is_dir()

    # Second call must not raise
    config.ensure_dirs()


# ----------------------------------------------------------------------
# R11 — most_recent_trading_day skips weekends and NYSE holidays
# ----------------------------------------------------------------------

@pytest.mark.parametrize("reference, expected", [
    # Trading day → same day
    (date(2026, 4, 23), date(2026, 4, 23)),   # Thursday
    # Weekend → back to Friday
    (date(2026, 4, 25), date(2026, 4, 24)),   # Saturday
    (date(2026, 4, 26), date(2026, 4, 24)),   # Sunday
    # Holiday: Christmas 2025 (Thu) → Wed Dec 24
    (date(2025, 12, 25), date(2025, 12, 24)),
    # New Year's Day 2026 (Thu) → Wed Dec 31
    (date(2026, 1, 1), date(2025, 12, 31)),
    # Jul 4 2026 is Saturday; Jul 3 is observed holiday.
    # Jul 5 Sun → expect Thu Jul 2 (Fri Jul 3 = holiday).
    (date(2026, 7, 5), date(2026, 7, 2)),
    # Good Friday Apr 3 2026 → Thu Apr 2
    (date(2026, 4, 3), date(2026, 4, 2)),
])
def test_most_recent_trading_day(reference, expected):
    assert config.most_recent_trading_day(reference) == expected


# ----------------------------------------------------------------------
# R12 — atomic writes leave no .tmp residue on success
# ----------------------------------------------------------------------

def test_atomic_write_text_success(tmp_path):
    path = tmp_path / "out.txt"
    config.atomic_write_text(path, "hello world")
    assert path.read_text(encoding="utf-8") == "hello world"
    # Temp file should not remain
    assert not (tmp_path / "out.txt.tmp").exists()


def test_atomic_write_text_overwrites(tmp_path):
    path = tmp_path / "out.txt"
    path.write_text("original")
    config.atomic_write_text(path, "replacement")
    assert path.read_text(encoding="utf-8") == "replacement"


def test_atomic_write_text_creates_parent(tmp_path):
    path = tmp_path / "nested" / "deeper" / "out.txt"
    config.atomic_write_text(path, "ok")
    assert path.read_text(encoding="utf-8") == "ok"


def test_atomic_write_parquet_success(tmp_path):
    path = tmp_path / "t.parquet"
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    config.atomic_write_parquet(df, path, engine="pyarrow", index=False)
    assert path.exists()
    # Temp file should not remain
    assert not (tmp_path / "t.parquet.tmp").exists()
    loaded = pd.read_parquet(path)
    pd.testing.assert_frame_equal(loaded, df)


# ----------------------------------------------------------------------
# Phase 2 — constant pins
# ----------------------------------------------------------------------

def test_validate_pause_sec_is_reduced():
    """I7: validation pause dropped from 3.0 → 1.0. Pin the value so it
    can't silently regress."""
    assert config.VALIDATE_PAUSE_SEC == 1.0


def test_save_ftp_raw_defaults_false():
    """I12: raw FTP files no longer persisted by default."""
    assert config.SAVE_FTP_RAW is False


# ----------------------------------------------------------------------
# SEC EDGAR contact email — user-configurable, no hardcoded address
# ----------------------------------------------------------------------

@pytest.fixture
def _sec_tmp(tmp_path, monkeypatch):
    """Redirect config.DATA_DIR to a temp dir and clear the env override
    so SEC-contact tests run against a clean slate."""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.delenv(config.SEC_CONTACT_ENV_VAR, raising=False)
    return tmp_path


def test_no_personal_email_hardcoded():
    """The placeholder must be a non-functional example address — never a
    real personal email."""
    assert config.SEC_CONTACT_PLACEHOLDER.endswith("@example.com")
    assert "gmail" not in config.SEC_CONTACT_PLACEHOLDER.lower()


def test_sec_contact_defaults_to_placeholder(_sec_tmp):
    """With no file and no env var, the resolver returns the placeholder
    and reports the source as unconfigured."""
    assert config.get_sec_contact_email() == config.SEC_CONTACT_PLACEHOLDER
    assert config.sec_contact_is_configured() is False


def test_set_and_get_sec_contact_email(_sec_tmp):
    assert config.set_sec_contact_email("dev@acme.io") is True
    assert config.get_sec_contact_email() == "dev@acme.io"
    assert config.sec_contact_is_configured() is True
    assert config.get_sec_user_agent() == "TradingScanner/1.0 dev@acme.io"


def test_set_sec_contact_email_strips_whitespace(_sec_tmp):
    config.set_sec_contact_email("  spaced@acme.io  ")
    assert config.get_sec_contact_email() == "spaced@acme.io"


def test_clear_sec_contact_email(_sec_tmp):
    config.set_sec_contact_email("dev@acme.io")
    assert config.set_sec_contact_email("") is True
    assert config.get_sec_contact_email() == config.SEC_CONTACT_PLACEHOLDER
    assert config.sec_contact_is_configured() is False


def test_sec_contact_env_var_override(_sec_tmp, monkeypatch):
    """$SEC_CONTACT_EMAIL is used when no file is present."""
    monkeypatch.setenv(config.SEC_CONTACT_ENV_VAR, "env@acme.io")
    assert config.get_sec_contact_email() == "env@acme.io"
    assert config.sec_contact_is_configured() is True


def test_sec_contact_file_beats_env_var(_sec_tmp, monkeypatch):
    """The on-disk file takes priority over the environment variable."""
    monkeypatch.setenv(config.SEC_CONTACT_ENV_VAR, "env@acme.io")
    config.set_sec_contact_email("file@acme.io")
    assert config.get_sec_contact_email() == "file@acme.io"
