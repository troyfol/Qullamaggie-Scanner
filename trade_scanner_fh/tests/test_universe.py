"""Tests for ticker_universe.py — Phase 1 Fixes R6, R17."""
import pandas as pd
import pytest

from trade_scanner_fh import ticker_universe as tu


# ----------------------------------------------------------------------
# R6 — ADR detection regex (\b word boundary, not literal backspace)
# ----------------------------------------------------------------------

@pytest.mark.parametrize("name, expected", [
    # Classic ADR phrasings
    ("Alibaba Group Holding Ltd - ADR", True),
    ("Toyota Motor Corp ADR", True),
    ("Novartis AG (ADR)", True),
    ("Petrobras ADS", True),
    ("American Depositary Shares", True),
    ("Some Co Depositary Shares", True),
    # Non-ADR names that embed 'adr'/'ads' inside other words must NOT match
    ("Padron Industries", False),         # contains 'adr' inside
    ("Sadr Petroleum Holdings", False),   # contains 'adr' inside
    ("Adspace Networks", False),          # contains 'ads' inside
    ("Common Stock", False),
    ("", False),
])
def test_detect_adr(name, expected):
    assert tu._detect_adr(name) == expected


# ----------------------------------------------------------------------
# R17 — Universe symbol normalization + dedupe behavior
# ----------------------------------------------------------------------

def test_normalise_symbol_dot_to_dash():
    """NASDAQ FTP uses BRK.B; yfinance wants BRK-B."""
    assert tu._normalise_symbol("BRK.B") == "BRK-B"
    assert tu._normalise_symbol("brk.a") == "BRK-A"
    assert tu._normalise_symbol("  AAPL  ") == "AAPL"


def test_dedupe_collapses_normalized_dot_and_dash_forms():
    """BRK.B from FTP and BRK-B from GitHub must collapse to one row after
    normalization + drop_duplicates. Pins the current merge behavior so
    future refactors don't regress it."""
    ftp_df = pd.DataFrame([
        {"symbol_raw": "BRK.B", "name": "Berkshire Hathaway B",
         "exchange": "NYSE", "market_category": "", "etf": False,
         "adr": False, "source": "nasdaq_ftp_traded"},
        {"symbol_raw": "AAPL", "name": "Apple Inc",
         "exchange": "NASDAQ", "market_category": "", "etf": False,
         "adr": False, "source": "nasdaq_ftp_traded"},
    ])
    ftp_df["symbol"] = ftp_df["symbol_raw"].apply(tu._normalise_symbol)

    github_syms = {tu._normalise_symbol(s) for s in {"BRK-B", "MSFT", "AAPL"}}

    ftp_set = set(ftp_df["symbol"].unique())
    new_from_github = github_syms - ftp_set

    # MSFT is new; BRK-B already covered by BRK.B → BRK-B normalization
    assert new_from_github == {"MSFT"}

    extra_rows = [
        {"symbol_raw": sym, "symbol": sym, "name": "", "exchange": "",
         "market_category": "", "etf": False, "adr": False, "source": "github"}
        for sym in new_from_github
    ]
    combined = pd.concat([ftp_df, pd.DataFrame(extra_rows)], ignore_index=True)
    combined = combined.drop_duplicates(subset="symbol", keep="first").reset_index(drop=True)

    assert set(combined["symbol"]) == {"BRK-B", "AAPL", "MSFT"}
    # FTP row with metadata wins for BRK-B (kept "first")
    brk_row = combined.loc[combined["symbol"] == "BRK-B"].iloc[0]
    assert brk_row["name"] == "Berkshire Hathaway B"
    assert brk_row["source"] == "nasdaq_ftp_traded"


def test_filter_symbols_drops_warrants_rights_units_wi():
    df = pd.DataFrame({
        "symbol": ["AAPL", "ABCDW", "ABCDR", "ABCDU", "ABCDWI", "GOOD+1"],
    })
    filtered = tu._filter_symbols(df)
    assert set(filtered["symbol"]) == {"AAPL"}


# ----------------------------------------------------------------------
# Phase 4 R8 — Validation batch retry with per-ticker probe fallback
# ----------------------------------------------------------------------

def test_validate_falls_back_to_single_ticker_probes_on_persistent_failure(monkeypatch):
    """When yf.download raises on both first call AND retry, the
    validation path must fall back to per-ticker probes instead of
    blanket-marking the batch as failed."""
    from unittest.mock import patch

    # _run_validation_batch always raises → triggers retry path
    def always_raise(batch):
        raise RuntimeError("simulated network error")

    # _probe_single_ticker marks odd-numbered tickers valid
    def fake_probe(sym):
        return sym in {"T1", "T3"}

    monkeypatch.setattr(tu, "_run_validation_batch", always_raise)
    monkeypatch.setattr(tu, "_probe_single_ticker", fake_probe)
    # Zero out pauses so the test is fast
    monkeypatch.setattr(tu.config, "VALIDATE_PAUSE_SEC", 0.0)
    monkeypatch.setattr(tu.config, "VALIDATE_BATCH_SIZE", 500)
    monkeypatch.setattr(tu.time, "sleep", lambda *_: None)  # noqa: E501

    valid, failed = tu._validate_via_yfinance(["T0", "T1", "T2", "T3"])

    # If the batch had been blanket-failed (pre-R8 behavior) valid would
    # be empty. With the probe fallback, T1 and T3 are recovered.
    assert valid == {"T1", "T3"}
    assert failed == {"T0", "T2"}


def test_validate_retry_on_transient_error(monkeypatch):
    """If the first batch call fails but the retry succeeds, no per-ticker
    probe is needed."""
    from unittest.mock import patch

    call_count = {"n": 0}

    def flaky(batch):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("transient")
        return {"T0", "T1"}, set()

    probe_called = {"n": 0}

    def fake_probe(sym):
        probe_called["n"] += 1
        return True

    monkeypatch.setattr(tu, "_run_validation_batch", flaky)
    monkeypatch.setattr(tu, "_probe_single_ticker", fake_probe)
    monkeypatch.setattr(tu.config, "VALIDATE_PAUSE_SEC", 0.0)
    monkeypatch.setattr(tu.time, "sleep", lambda *_: None)

    valid, failed = tu._validate_via_yfinance(["T0", "T1"])

    # Retry succeeded → probe must not have been called
    assert probe_called["n"] == 0
    assert valid == {"T0", "T1"}
    assert failed == set()
