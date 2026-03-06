"""
Ticker Universe Manager — Multi-Source Acquisition
====================================================
Downloads the full US equity universe from multiple free sources,
merges/deduplicates, validates against yfinance, and caches as CSV
with rich metadata.

Sources:
  1. NASDAQ FTP (nasdaqtraded.txt, nasdaqlisted.txt, otherlisted.txt)
  2. GitHub rreichel3/US-Stock-Symbols
  3. SEC EDGAR company_tickers.json

Public API:
    refresh_universe(force=False, skip_validation=False) -> pd.DataFrame
    load_universe() -> pd.DataFrame
"""

import ftplib
import io
import logging
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
import yfinance as yf

from . import config

log = logging.getLogger("scanner.universe")

_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "en-US,en;q=0.9",
}

# ADR detection keywords (case-insensitive)
_ADR_KEYWORDS = re.compile(
    r"american\s+depositary|[\b\s]adr[\b\s.,]|[\b\s]ads[\b\s.,]|depositary\s+shares",
    re.IGNORECASE,
)


# ============================================================================
# Source 1: NASDAQ FTP
# ============================================================================

def _download_ftp_file(filename: str) -> str:
    """Download a single file from NASDAQ FTP, return its text content."""
    log.info("FTP: downloading %s ...", filename)
    buf = io.BytesIO()
    ftp = ftplib.FTP(config.NASDAQ_FTP_HOST, timeout=30)
    try:
        ftp.login("anonymous", "")
        ftp.cwd(config.NASDAQ_FTP_DIR)
        ftp.retrbinary(f"RETR {filename}", buf.write)
    finally:
        try:
            ftp.quit()
        except Exception:
            pass
    raw = buf.getvalue().decode("utf-8", errors="replace")
    # Cache raw file locally for debugging
    (config.FTP_RAW_DIR / filename).write_text(raw, encoding="utf-8")
    return raw


def _parse_nasdaqtraded(text: str) -> pd.DataFrame:
    """
    Parse nasdaqtraded.txt (pipe-delimited, last row is footer).
    Returns DataFrame with normalised columns.
    """
    lines = text.strip().splitlines()
    # Drop footer row (starts with "File Creation Time")
    if lines and lines[-1].startswith("File Creation Time"):
        footer = lines.pop()
        log.info("FTP nasdaqtraded.txt footer: %s", footer.strip())

    df = pd.read_csv(io.StringIO("\n".join(lines)), sep="|", dtype=str)
    df.columns = [c.strip() for c in df.columns]
    log.info("FTP nasdaqtraded.txt: %d rows parsed", len(df))

    # Filter out test issues
    if "Test Issue" in df.columns:
        before = len(df)
        df = df[df["Test Issue"].str.strip() != "Y"].copy()
        log.info("FTP: removed %d test issues, %d remain", before - len(df), len(df))

    # Build normalised output
    out = pd.DataFrame()
    out["symbol_raw"] = df.get("NASDAQ Symbol", df.get("Symbol", pd.Series(dtype=str)))
    out["symbol_raw"] = out["symbol_raw"].astype(str).str.strip()
    out["name"] = df.get("Security Name", pd.Series("", index=df.index)).str.strip()
    out["exchange"] = df.get("Listing Exchange", pd.Series("", index=df.index)).str.strip()
    out["market_category"] = df.get("Market Category", pd.Series("", index=df.index)).str.strip()
    out["etf"] = df.get("ETF", pd.Series("N", index=df.index)).str.strip().str.upper() == "Y"
    out["source"] = "nasdaq_ftp_traded"
    return out


def _parse_listed_file(text: str, source_label: str) -> pd.DataFrame:
    """Parse nasdaqlisted.txt or otherlisted.txt (pipe-delimited)."""
    lines = text.strip().splitlines()
    if lines and lines[-1].startswith("File Creation Time"):
        lines.pop()

    df = pd.read_csv(io.StringIO("\n".join(lines)), sep="|", dtype=str)
    df.columns = [c.strip() for c in df.columns]
    log.info("FTP %s: %d rows parsed", source_label, len(df))

    # Filter test issues if column exists
    for col in ("Test Issue", "Test issue"):
        if col in df.columns:
            df = df[df[col].str.strip() != "Y"].copy()

    out = pd.DataFrame()
    out["symbol_raw"] = df.iloc[:, 0].astype(str).str.strip()  # first col is always symbol
    if "Security Name" in df.columns:
        out["name"] = df["Security Name"].str.strip()
    else:
        out["name"] = ""
    out["source"] = source_label
    return out


def _fetch_nasdaq_ftp() -> pd.DataFrame:
    """Download all three FTP files, parse, and merge into one DataFrame."""
    frames = []

    # nasdaqtraded.txt — the master file
    try:
        text = _download_ftp_file("nasdaqtraded.txt")
        traded = _parse_nasdaqtraded(text)
        frames.append(traded)
    except Exception:
        log.exception("Failed to fetch/parse nasdaqtraded.txt")

    # nasdaqlisted.txt
    try:
        text = _download_ftp_file("nasdaqlisted.txt")
        listed = _parse_listed_file(text, "nasdaq_ftp_listed")
        frames.append(listed)
    except Exception:
        log.exception("Failed to fetch/parse nasdaqlisted.txt")

    # otherlisted.txt
    try:
        text = _download_ftp_file("otherlisted.txt")
        other = _parse_listed_file(text, "nasdaq_ftp_other")
        frames.append(other)
    except Exception:
        log.exception("Failed to fetch/parse otherlisted.txt")

    if not frames:
        return pd.DataFrame()

    merged = pd.concat(frames, ignore_index=True)
    return merged


# ============================================================================
# Source 2: GitHub rreichel3/US-Stock-Symbols
# ============================================================================

def _fetch_github_tickers() -> set[str]:
    """Download the all_tickers.txt file from GitHub."""
    log.info("GitHub: downloading all_tickers.txt ...")
    try:
        resp = requests.get(config.GITHUB_TICKERS_URL, headers=_HTTP_HEADERS, timeout=30)
        resp.raise_for_status()
        tickers = {
            line.strip() for line in resp.text.splitlines() if line.strip()
        }
        log.info("GitHub: %d tickers downloaded", len(tickers))
        return tickers
    except Exception:
        log.exception("Failed to fetch GitHub tickers")
        return set()


# ============================================================================
# Source 3: SEC EDGAR
# ============================================================================

def _fetch_sec_edgar() -> set[str]:
    """Download SEC EDGAR company_tickers.json."""
    log.info("SEC EDGAR: downloading company_tickers.json ...")
    try:
        resp = requests.get(
            config.SEC_TICKERS_URL,
            headers={**_HTTP_HEADERS, "Accept": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        tickers = {
            entry["ticker"].strip().upper()
            for entry in data.values()
            if "ticker" in entry and entry["ticker"].strip()
        }
        log.info("SEC EDGAR: %d tickers downloaded", len(tickers))
        return tickers
    except Exception:
        log.exception("Failed to fetch SEC EDGAR tickers")
        return set()


# ============================================================================
# Symbol normalisation & filtering
# ============================================================================

def _normalise_symbol(sym: str) -> str:
    """Convert NASDAQ FTP symbol format to yfinance format (dots -> dashes)."""
    return sym.strip().upper().replace(".", "-")


def _is_warrant_right_unit_wi(sym: str) -> dict[str, bool]:
    """Classify a symbol as warrant / right / unit / when-issued."""
    return {
        "is_warrant": bool(re.match(r"^[A-Z]{4,5}W$", sym)),
        "is_right": bool(re.match(r"^[A-Z]{4,5}R$", sym)),
        "is_unit": bool(re.match(r"^[A-Z]{4,5}U$", sym)),
        "is_when_issued": sym.endswith("WI"),
    }


def _detect_adr(name: str) -> bool:
    """Check if the security name indicates an ADR/ADS."""
    if not name:
        return False
    return bool(_ADR_KEYWORDS.search(name))


def _filter_symbols(df: pd.DataFrame) -> pd.DataFrame:
    """Apply default exclusion filters (warrants, rights, units, WI)."""
    before = len(df)
    mask = pd.Series(True, index=df.index)

    if config.EXCLUDE_WARRANTS:
        mask &= ~df["symbol"].str.match(r"^[A-Z]{4,5}W$")
    if config.EXCLUDE_RIGHTS:
        mask &= ~df["symbol"].str.match(r"^[A-Z]{4,5}R$")
    if config.EXCLUDE_UNITS:
        mask &= ~df["symbol"].str.match(r"^[A-Z]{4,5}U$")
    if config.EXCLUDE_WHEN_ISSUED:
        mask &= ~df["symbol"].str.endswith("WI")

    # Also drop symbols with unusual chars that yfinance can't handle
    mask &= ~df["symbol"].str.contains(r"[+=%#@!]", regex=True)
    # Drop empty symbols
    mask &= df["symbol"].str.len() > 0

    df = df[mask].copy()
    removed = before - len(df)
    if removed:
        log.info("Filtered out %d warrants/rights/units/WI/invalid symbols, %d remain",
                 removed, len(df))
    return df


# ============================================================================
# yfinance validation
# ============================================================================

def _validate_via_yfinance(symbols: list[str]) -> tuple[set[str], set[str]]:
    """
    Validate symbols by attempting to download 1 day of data from yfinance.
    Returns (valid_set, failed_set).
    """
    valid = set()
    failed = set()
    total = len(symbols)
    batch_size = config.VALIDATE_BATCH_SIZE

    log.info("yfinance validation: %d tickers in batches of %d ...", total, batch_size)

    for i in range(0, total, batch_size):
        batch = symbols[i : i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (total + batch_size - 1) // batch_size

        try:
            # Download 1 day for the batch — yf.download with group_by='ticker'
            joined = " ".join(batch)
            data = yf.download(
                joined,
                period="5d",
                progress=False,
                threads=True,
                group_by="ticker",
            )

            if len(batch) == 1:
                # Single ticker: data is a plain DataFrame
                sym = batch[0]
                if data is not None and not data.empty and data["Close"].notna().any():
                    valid.add(sym)
                else:
                    failed.add(sym)
            else:
                # Multi-ticker: data has multi-level columns (ticker, field)
                for sym in batch:
                    try:
                        if sym in data.columns.get_level_values(0):
                            sub = data[sym]
                            if sub is not None and not sub.empty and sub["Close"].notna().any():
                                valid.add(sym)
                            else:
                                failed.add(sym)
                        else:
                            failed.add(sym)
                    except Exception:
                        failed.add(sym)

        except Exception as exc:
            log.warning("Validation batch %d error: %s — marking %d tickers as failed",
                        batch_num, exc, len(batch))
            failed.update(batch)

        log.info("  Validation batch %d/%d: %d valid so far, %d failed",
                 batch_num, total_batches, len(valid), len(failed))

        if i + batch_size < total:
            time.sleep(config.VALIDATE_PAUSE_SEC)

    return valid, failed


# ============================================================================
# Public API
# ============================================================================

def refresh_universe(
    force: bool = False,
    skip_validation: bool = False,
) -> pd.DataFrame:
    """
    Multi-source ticker universe refresh:
      1. NASDAQ FTP (nasdaqtraded + nasdaqlisted + otherlisted)
      2. GitHub rreichel3/US-Stock-Symbols
      3. SEC EDGAR company_tickers.json
      4. Merge, dedupe, filter
      5. yfinance validation pass (unless skip_validation=True)
      6. Save to config.TICKER_CSV

    If the CSV is fresh (< UNIVERSE_STALE_DAYS) and force=False, returns cached.
    """
    csv_path: Path = config.TICKER_CSV

    if not force and csv_path.exists():
        age = datetime.now() - datetime.fromtimestamp(csv_path.stat().st_mtime)
        if age < timedelta(days=config.UNIVERSE_STALE_DAYS):
            log.info("Universe CSV is %s old -- skipping refresh.", age)
            return pd.read_csv(csv_path)

    log.info("=" * 60)
    log.info("Ticker Universe Update Started")
    log.info("=" * 60)

    # ── Source 1: NASDAQ FTP ──
    ftp_df = _fetch_nasdaq_ftp()
    ftp_count = 0
    if not ftp_df.empty:
        # Normalise symbols
        ftp_df["symbol"] = ftp_df["symbol_raw"].apply(_normalise_symbol)
        ftp_df["adr"] = ftp_df["name"].apply(_detect_adr)
        ftp_count = ftp_df["symbol"].nunique()
        log.info("Source 1 (NASDAQ FTP): %d unique symbols", ftp_count)

    # ── Source 2: GitHub ──
    github_syms = _fetch_github_tickers()
    github_syms = {_normalise_symbol(s) for s in github_syms}

    # ── Source 3: SEC EDGAR ──
    sec_syms = _fetch_sec_edgar()
    sec_syms = {_normalise_symbol(s) for s in sec_syms}

    # ── Merge ──
    # Start with FTP data (has metadata), then add symbols from other sources
    if not ftp_df.empty:
        all_syms_ftp = set(ftp_df["symbol"].unique())
    else:
        all_syms_ftp = set()

    new_from_github = github_syms - all_syms_ftp
    new_from_sec = sec_syms - all_syms_ftp - github_syms

    log.info("Source 2 (GitHub) contributed %d NEW tickers", len(new_from_github))
    log.info("Source 3 (SEC EDGAR) contributed %d NEW tickers", len(new_from_sec))

    # Create rows for tickers only found in GitHub/SEC (no metadata)
    extra_rows = []
    for sym in new_from_github:
        extra_rows.append({
            "symbol_raw": sym, "symbol": sym, "name": "",
            "exchange": "", "market_category": "", "etf": False,
            "adr": False, "source": "github",
        })
    for sym in new_from_sec:
        extra_rows.append({
            "symbol_raw": sym, "symbol": sym, "name": "",
            "exchange": "", "market_category": "", "etf": False,
            "adr": False, "source": "sec_edgar",
        })

    if extra_rows:
        extra_df = pd.DataFrame(extra_rows)
        combined = pd.concat([ftp_df, extra_df], ignore_index=True)
    else:
        combined = ftp_df.copy() if not ftp_df.empty else pd.DataFrame()

    if combined.empty:
        raise RuntimeError("No tickers obtained from any source.")

    # Dedupe by symbol (keep first = FTP row with metadata if available)
    combined = combined.drop_duplicates(subset="symbol", keep="first").reset_index(drop=True)
    total_merged = len(combined)
    log.info("Merged unique tickers: %d", total_merged)

    # ── Filter ──
    combined = _filter_symbols(combined)

    # ── yfinance validation ──
    if skip_validation:
        log.info("Skipping yfinance validation (skip_validation=True)")
        combined["validated"] = True
    else:
        # Check if we have a previous universe to diff against
        prev_syms = set()
        if csv_path.exists():
            try:
                prev_df = pd.read_csv(csv_path)
                prev_syms = set(prev_df["symbol"].unique())
            except Exception:
                pass

        current_syms = set(combined["symbol"].unique())
        new_tickers = current_syms - prev_syms

        if prev_syms and len(new_tickers) < len(current_syms):
            # Incremental: only validate new tickers
            log.info("Incremental update: %d new tickers to validate, %d already validated",
                     len(new_tickers), len(current_syms) - len(new_tickers))
            # Previously validated tickers: mark as valid
            combined["validated"] = combined["symbol"].isin(prev_syms)

            if new_tickers:
                valid, failed = _validate_via_yfinance(sorted(new_tickers))
                combined.loc[combined["symbol"].isin(valid), "validated"] = True
                combined.loc[combined["symbol"].isin(failed), "validated"] = False
                log.info("Validation: %d valid, %d failed out of %d new",
                         len(valid), len(failed), len(new_tickers))
                _log_failed_tickers(failed)
        else:
            # Full validation
            log.info("Full validation pass for %d tickers ...", len(combined))
            valid, failed = _validate_via_yfinance(combined["symbol"].tolist())
            combined["validated"] = combined["symbol"].isin(valid)
            log.info("Validation complete: %d valid, %d failed", len(valid), len(failed))
            _log_failed_tickers(failed)

        # Keep only validated
        before_val = len(combined)
        combined = combined[combined["validated"]].copy()
        log.info("After validation: %d tickers (%d removed)", len(combined), before_val - len(combined))

    # ── Summary stats ──
    n_etf = combined["etf"].sum() if "etf" in combined.columns else 0
    n_adr = combined["adr"].sum() if "adr" in combined.columns else 0
    n_other = len(combined) - n_etf - n_adr
    log.info("Final universe: %d tickers (%d common/pref, %d ETFs, %d ADRs)",
             len(combined), n_other, n_etf, n_adr)

    # ── Save ──
    keep_cols = ["symbol", "name", "exchange", "market_category", "etf", "adr", "source"]
    keep_cols = [c for c in keep_cols if c in combined.columns]
    out = combined[keep_cols].reset_index(drop=True)
    out.to_csv(csv_path, index=False)
    log.info("Saved -> %s", csv_path)

    log.info("=" * 60)
    log.info("Ticker Universe Update Complete")
    log.info("=" * 60)
    return out


def _log_failed_tickers(failed: set[str]) -> None:
    """Append failed tickers to the failed_tickers.log file."""
    if not failed:
        return
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(config.FAILED_TICKERS_LOG, "a", encoding="utf-8") as f:
        for sym in sorted(failed):
            f.write(f"[{ts}] FAILED: {sym}\n")
    log.info("Logged %d failed tickers to %s", len(failed), config.FAILED_TICKERS_LOG)


def load_universe() -> pd.DataFrame:
    """Load the cached universe CSV. Raises FileNotFoundError if missing."""
    csv_path = config.TICKER_CSV
    if not csv_path.exists():
        raise FileNotFoundError(
            f"No cached universe at {csv_path}. Run refresh_universe() first."
        )
    return pd.read_csv(csv_path)
