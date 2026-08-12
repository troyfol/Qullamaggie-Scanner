"""Wave-4 remediation regression tests — audit 2026-08-12.

Covers INT-8, INT-11, INT-13, INT-14, INT-17, SEC-6, SEC-10, SEC-12 and the
EFF-10 equivalences. INT-1/2 (truncation), INT-4 (reconcile), INT-5 (skip
lists), EFF-1/EFF-3 (scan) and SEC-3 (allowlist) have their own files.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from trade_scanner_fh import config, data_engine, indicators, sector_map


# ── INT-11: a bad re-sent bar must not overwrite a good cached one ─────

def _bars(dates, closes):
    return pd.DataFrame(
        {"Open": closes, "High": closes, "Low": closes,
         "Close": closes, "Volume": [1_000] * len(closes)},
        index=pd.to_datetime(dates),
    )


def test_conflicting_bar_is_rejected_in_favour_of_the_cache():
    """`keep="last"` let a bad yfinance response overwrite a good bar and
    become load-bearing forever — OHLCV parquets carry no source or fetch-time
    column, so a cemented bad bar can't be identified or re-fetched."""
    old = _bars(["2026-08-03", "2026-08-04"], [100.0, 101.0])
    new = _bars(["2026-08-04", "2026-08-05"], [500.0, 102.0])   # 395% jump
    out = data_engine._reject_conflicting_bars("TEST", old, new)
    assert pd.Timestamp("2026-08-04") not in out.index   # conflict dropped
    assert pd.Timestamp("2026-08-05") in out.index       # genuinely new kept


def test_agreeing_resent_bar_is_kept():
    """A provider re-sending the boundary bar unchanged is normal and must
    flow through — the guard only fires past PRICE_JUMP_PCT."""
    old = _bars(["2026-08-04"], [100.0])
    new = _bars(["2026-08-04", "2026-08-05"], [100.4, 101.0])
    out = data_engine._reject_conflicting_bars("TEST", old, new)
    assert len(out) == 2


def test_bar_conflict_check_never_breaks_an_update():
    """Fails open: anything unexpected returns new_df untouched rather than
    letting a guard turn into a download failure."""
    old = pd.DataFrame({"NotClose": [1]}, index=pd.to_datetime(["2026-08-04"]))
    new = _bars(["2026-08-04"], [100.0])
    assert data_engine._reject_conflicting_bars("TEST", old, new) is new
    assert len(data_engine._reject_conflicting_bars(
        "TEST", _bars([], []), new)) == 1


# ── INT-13: sector staleness, ETF re-map ───────────────────────────────

@pytest.fixture
def sector_tmp(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "SECTOR_MAP_PARQUET",
                        tmp_path / "sector_map.parquet")
    return tmp_path


def test_stale_sector_tickers_returns_oldest_first(sector_tmp):
    now = datetime.now()
    sector_map.save_sector_map(pd.DataFrame([
        {"ticker": "FRESH", "sector": "Tech", "sector_etf": "XLK",
         "updated_at": now - timedelta(days=5)},
        {"ticker": "OLD", "sector": "Tech", "sector_etf": "XLK",
         "updated_at": now - timedelta(days=200)},
        {"ticker": "OLDEST", "sector": "Tech", "sector_etf": "XLK",
         "updated_at": now - timedelta(days=400)},
    ]))
    assert sector_map.stale_sector_tickers(max_age_days=180) == ["OLDEST", "OLD"]


def test_rows_with_no_updated_at_count_as_stale(sector_tmp):
    """Pre-INT-13 rows carry no usable timestamp; re-fetching one is cheap."""
    sector_map.save_sector_map(pd.DataFrame([
        {"ticker": "NOSTAMP", "sector": "Tech", "sector_etf": "XLK",
         "updated_at": pd.NaT},
    ]))
    assert sector_map.stale_sector_tickers(max_age_days=180) == ["NOSTAMP"]


def test_remap_fills_blank_sector_etf_when_the_map_now_covers_it(
        sector_tmp, monkeypatch):
    """`sector_etf == ""` was indistinguishable from "not yet mapped" and was
    never revisited, so tickers whose sector SECTOR_ETF_MAP later gained kept
    losing sector-relative strength."""
    monkeypatch.setattr(config, "SECTOR_ETF_MAP", {"Widgets": "XLW"})
    sector_map.save_sector_map(pd.DataFrame([
        {"ticker": "AAA", "sector": "Widgets", "sector_etf": "",
         "updated_at": datetime.now()},
        {"ticker": "BBB", "sector": "Unknown", "sector_etf": "",
         "updated_at": datetime.now()},
        {"ticker": "CCC", "sector": "Widgets", "sector_etf": "XLW",
         "updated_at": datetime.now()},
    ]))
    assert sector_map.remap_missing_sector_etfs() == 1
    df = sector_map.load_sector_map().set_index("ticker")
    assert df.loc["AAA", "sector_etf"] == "XLW"
    assert df.loc["BBB", "sector_etf"] == ""      # still uncovered — untouched
    # Second run is a no-op, so this is safe to call at every launch.
    assert sector_map.remap_missing_sector_etfs() == 0


def test_remap_is_a_noop_with_no_map(sector_tmp):
    assert sector_map.remap_missing_sector_etfs() == 0


# ── INT-14: log retention ──────────────────────────────────────────────

def test_prune_old_logs_removes_only_aged_files(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "LOG_DIR", tmp_path)
    import os
    old = tmp_path / "scan_old.log"
    new = tmp_path / "scan_new.log"
    rotated = tmp_path / "ohlcv_old.log.1"
    keep = tmp_path / "notalog.txt"
    for f in (old, new, rotated, keep):
        f.write_text("x", encoding="utf-8")
    aged = (datetime.now() - timedelta(days=90)).timestamp()
    os.utime(old, (aged, aged))
    os.utime(rotated, (aged, aged))

    removed = config.prune_old_logs(retention_days=30)

    assert removed == 2               # the .log and the rotated .log.1
    assert not old.exists() and not rotated.exists()
    assert new.exists()
    assert keep.exists()              # non-log files are never touched


def test_prune_old_logs_tolerates_a_missing_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "LOG_DIR", tmp_path / "nope")
    assert config.prune_old_logs() == 0


# ── INT-17: atomic schema stamp ────────────────────────────────────────

def test_schema_version_stamp_is_atomic(tmp_path, monkeypatch):
    """A crash mid-write left a truncated sidecar, which read_schema_version
    then reports as a MISMATCH against a cache that is actually fine."""
    monkeypatch.setattr(config, "PARQUET_DIR", tmp_path)
    monkeypatch.setattr(config, "PARQUET_SCHEMA_FILE",
                        tmp_path / ".schema_version")
    data_engine.stamp_schema_version()
    assert data_engine.read_schema_version() == config.PARQUET_SCHEMA_VERSION
    # No temp residue left behind.
    assert not list(tmp_path.glob("*.tmp"))


# ── SEC-6: the spec closes the traceback-dialog path ───────────────────

def test_spec_disables_the_windowed_traceback_dialog():
    """console=False + disable_windowed_traceback=False popped a traceback
    DIALOG on an unhandled crash — a plausible secret-in-traceback path when
    combined with SEC-5."""
    spec = (config.APP_ROOT.parent / "Trade_Scanner_FH.spec")
    if not spec.exists():          # not present in a frozen/installed layout
        pytest.skip("spec file not in this tree")
    body = spec.read_text(encoding="utf-8")
    assert "disable_windowed_traceback=True" in body


# ── SEC-10: hotkey target-window guard ─────────────────────────────────

def test_target_window_guard_is_off_by_default(monkeypatch):
    """Empty hints = no check, because the right value depends on the user's
    target app and a wrong hint would break a live trading workflow."""
    from trade_scanner_fh import hotkey
    monkeypatch.setattr(config, "HOTKEY_TARGET_WINDOW_HINTS", [])
    allowed, _desc = hotkey.target_window_allowed()
    assert allowed is True


def test_target_window_guard_refuses_a_mismatch(monkeypatch):
    from trade_scanner_fh import hotkey
    monkeypatch.setattr(config, "HOTKEY_TARGET_WINDOW_HINTS", ["TradeStation"])
    monkeypatch.setattr(hotkey, "_foreground_window_desc",
                        lambda: ("Solitaire", "SolitaireClass"))
    allowed, desc = hotkey.target_window_allowed()
    assert allowed is False
    assert "Solitaire" in desc


def test_target_window_guard_matches_title_or_class(monkeypatch):
    from trade_scanner_fh import hotkey
    monkeypatch.setattr(config, "HOTKEY_TARGET_WINDOW_HINTS", ["tradestation"])
    monkeypatch.setattr(hotkey, "_foreground_window_desc",
                        lambda: ("Chart - AAPL", "TradeStationMainWnd"))
    assert hotkey.target_window_allowed()[0] is True   # matched on CLASS
    monkeypatch.setattr(hotkey, "_foreground_window_desc",
                        lambda: ("TradeStation 10", "SomeClass"))
    assert hotkey.target_window_allowed()[0] is True   # matched on TITLE


def test_target_window_guard_fails_open_when_it_cannot_read(monkeypatch):
    """No pywin32 / no foreground handle must ALLOW, matching the existing
    off-screen-coordinate behaviour — a false refusal mid-trade is worse than
    the residual risk."""
    from trade_scanner_fh import hotkey
    monkeypatch.setattr(config, "HOTKEY_TARGET_WINDOW_HINTS", ["TradeStation"])
    monkeypatch.setattr(hotkey, "_foreground_window_desc", lambda: ("", ""))
    assert hotkey.target_window_allowed()[0] is True


# ── SEC-12: export formula-injection neutralisation ────────────────────

@pytest.mark.parametrize("payload", [
    "=cmd|' /C calc'!A0",
    "+1+1",
    "@SUM(A1)",
    "-1+1+cmd|' /C calc'!A0",
    "\tinjected",
    "\rinjected",
])
def test_formula_payloads_are_neutralised(payload):
    from trade_scanner_fh.gui.exports import _neutralize_formula
    out = _neutralize_formula(payload)
    assert out.startswith("'")
    assert out[1:] == payload


@pytest.mark.parametrize("benign", ["AAPL", "-3.2%", "-12.5", "2026-08-12",
                                    "", "1,234.5"])
def test_benign_cells_are_untouched(benign):
    """A leading '-' is only escaped when the string isn't a plain negative
    number, so exported percentages keep their exact text."""
    from trade_scanner_fh.gui.exports import _neutralize_formula
    assert _neutralize_formula(benign) == benign


def test_numeric_cells_stay_numeric():
    """Numbers must not become strings — Excel has to keep formatting them
    as numbers."""
    from trade_scanner_fh.gui.exports import _neutralize_formula
    assert _neutralize_formula(-12.5) == -12.5
    assert _neutralize_formula(7) == 7
    assert _neutralize_formula(None) is None


def test_neutralize_frame_only_touches_object_columns():
    from trade_scanner_fh.gui.exports import _neutralize_formulas
    df = pd.DataFrame({"sym": ["=EVIL", "AAPL"], "gain": [-3.5, 2.0]})
    out = _neutralize_formulas(df.copy())
    assert list(out["sym"]) == ["'=EVIL", "AAPL"]
    assert list(out["gain"]) == [-3.5, 2.0]
    assert out["gain"].dtype == df["gain"].dtype


# ── EFF-10: optimised helpers must be equivalent, not merely faster ────

def _price_frame(n=300):
    idx = pd.date_range("2026-01-01", periods=n, freq="B")
    closes = [100.0 + (i % 17) * 0.5 for i in range(n)]
    return pd.DataFrame(
        {"Open": closes, "High": [c + 1 for c in closes],
         "Low": [c - 1 for c in closes], "Close": closes,
         "Volume": [1_000_000] * n},
        index=idx,
    )


def test_price_above_sma_matches_the_rolling_implementation():
    df = _price_frame()
    for period in (20, 50, 200):
        expected = df["Close"].rolling(period).mean().iloc[-1]
        assert indicators.price_above_sma(df, period=period)["sma_value"] == \
            pytest.approx(expected)


def test_bollinger_band_width_matches_the_rolling_implementation():
    df = _price_frame()
    period, num_std = 20, 2.0
    close = df["Close"]
    middle = close.rolling(period).mean().iloc[-1]
    std = close.rolling(period).std().iloc[-1]
    expected = ((middle + num_std * std) - (middle - num_std * std)) / middle
    assert indicators.bollinger_band_width(
        df, period=period, num_std=num_std) == pytest.approx(expected)


def test_sma_still_returns_nan_when_history_is_short():
    df = _price_frame(n=10)
    assert pd.isna(indicators.price_above_sma(df, period=200)["sma_value"])
    assert pd.isna(indicators.bollinger_band_width(df, period=200))


# ── EFF-8: one glob instead of 15,948 stats ────────────────────────────

def test_cached_symbols_lists_exactly_the_parquets(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "PARQUET_DIR", tmp_path)
    for name in ("AAPL", "MSFT", "BRK-B"):
        (tmp_path / f"{name}.parquet").write_bytes(b"x")
    (tmp_path / "notes.txt").write_text("ignore me", encoding="utf-8")
    assert data_engine.cached_symbols() == {"AAPL", "MSFT", "BRK-B"}


def test_cached_symbols_tolerates_a_missing_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "PARQUET_DIR", tmp_path / "gone")
    assert data_engine.cached_symbols() == set()
