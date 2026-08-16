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

# ADR detection keywords (case-insensitive). \b is a word boundary here — an
# earlier version used [\b\s] inside character classes, which matches a literal
# backspace rather than a word boundary.
_ADR_KEYWORDS = re.compile(
    r"american\s+depositary|\bADR\b|\bADS\b|depositary\s+shares",
    re.IGNORECASE,
)


# ============================================================================
# Source 1: NASDAQ FTP
# ============================================================================

def _download_https_symbol_file(filename: str) -> str:
    """Fetch one NASDAQ symbol-directory file over HTTPS.

    Audit 2026-08-12 (SEC-8): this is the authenticated path. The symbol
    universe is the integrity root for SEC-3 — it reaches both URL builders
    and the filesystem path builder — and the FTP alternative is anonymous
    plaintext with no integrity check whatsoever, so a MITM there controls
    every ticker the app touches. Size-capped like every other remote body
    the app fetches (the largest real file is ~1 MB).
    """
    url = f"{config.NASDAQ_HTTPS_BASE}/{filename}"
    log.info("HTTPS: downloading %s ...", filename)
    resp = requests.get(url, timeout=30, headers=_HTTP_HEADERS)
    resp.raise_for_status()
    if len(resp.content) > config.NASDAQ_MAX_RESPONSE_BYTES:
        raise ValueError(
            f"{filename}: {len(resp.content)} bytes exceeds the "
            f"{config.NASDAQ_MAX_RESPONSE_BYTES}-byte cap"
        )
    return resp.text


def _download_ftp_file(filename: str) -> str:
    """Download a single NASDAQ symbol-directory file, return its text.

    Tries HTTPS first (SEC-8) and falls back to the historical anonymous FTP
    only if that fails, so a blocked host or a NASDAQ-side change can't break
    the universe refresh outright. Name kept for its callers and tests.
    """
    if config.NASDAQ_PREFER_HTTPS:
        try:
            raw = _download_https_symbol_file(filename)
            if config.SAVE_FTP_RAW:
                config.FTP_RAW_DIR.mkdir(parents=True, exist_ok=True)
                (config.FTP_RAW_DIR / filename).write_text(raw, encoding="utf-8")
            return raw
        except Exception as exc:
            log.warning(
                "HTTPS fetch of %s failed (%s) — falling back to PLAINTEXT "
                "anonymous FTP; the symbol feed is unauthenticated on this "
                "path", filename, exc,
            )

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
    # Cache raw file locally for debugging (opt-in via config.SAVE_FTP_RAW)
    if config.SAVE_FTP_RAW:
        config.FTP_RAW_DIR.mkdir(parents=True, exist_ok=True)
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

# Cap public universe-mirror downloads so a compromised CDN / MITM
# can't balloon memory. Real payloads: GitHub all_tickers.txt is ~120
# KB; SEC company_tickers.json is ~1.7 MB. 10 MB ceiling is generous.
_UNIVERSE_DOWNLOAD_MAX_BYTES = 10 * 1024 * 1024


def _read_capped(resp, max_bytes: int) -> bytes:
    """Stream the response body into memory but abort if the running
    byte count exceeds ``max_bytes``. Defends the universe downloads
    against a hostile origin ballooning memory."""
    chunks: list[bytes] = []
    total = 0
    for chunk in resp.iter_content(chunk_size=64 * 1024):
        if not chunk:
            continue
        total += len(chunk)
        if total > max_bytes:
            raise ValueError(
                f"response exceeded {max_bytes} byte cap "
                f"(read {total} bytes before abort)"
            )
        chunks.append(chunk)
    return b"".join(chunks)


def _fetch_github_tickers() -> set[str]:
    """Download the all_tickers.txt file from GitHub."""
    log.info("GitHub: downloading all_tickers.txt ...")
    try:
        resp = requests.get(
            config.GITHUB_TICKERS_URL,
            headers=_HTTP_HEADERS, timeout=30, stream=True,
        )
        resp.raise_for_status()
        raw = _read_capped(resp, _UNIVERSE_DOWNLOAD_MAX_BYTES)
        text = raw.decode("utf-8", errors="replace")
        tickers = {
            line.strip() for line in text.splitlines() if line.strip()
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
    """Download SEC EDGAR company_tickers.json.

    SEC's fair-access policy returns 403 Forbidden for generic User-Agents,
    so EDGAR requires a real contact email (see config.get_sec_user_agent).
    When no contact email is configured, this source is skipped and an
    empty set is returned — the other universe sources still run.
    """
    if not config.sec_contact_is_configured():
        log.warning(
            "SEC EDGAR: skipped — no contact email configured. Set one via "
            "Settings → Set SEC Contact Email… (or the %s environment "
            "variable) to enable this source.",
            config.SEC_CONTACT_ENV_VAR,
        )
        return set()
    log.info("SEC EDGAR: downloading company_tickers.json ...")
    try:
        resp = requests.get(
            config.SEC_TICKERS_URL,
            headers={
                "User-Agent": config.get_sec_user_agent(),
                "Accept": "application/json",
            },
            timeout=30,
            stream=True,
        )
        resp.raise_for_status()
        raw = _read_capped(resp, _UNIVERSE_DOWNLOAD_MAX_BYTES)
        import json as _json
        data = _json.loads(raw.decode("utf-8"))
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

    # Audit 2026-08-12 (SEC-3): ALLOWLIST, not a denylist. The old
    # `[+=%#@!]` denylist blocked query/fragment injection but let path
    # traversal through — `AAPL/../../admin` survived it and reached the
    # finviz URL builder verbatim. Symbols come from NASDAQ FTP (plaintext,
    # anonymous), a third-party GitHub mirror and SEC EDGAR, so a MITM on the
    # feed controls this column, which flows into both URL builders and into
    # filesystem paths. The pattern is validated against the live universe and
    # rejects none of the 405 preferred/rights symbols (ABR$D, AIIA^, …).
    mask &= df["symbol"].str.match(config.URL_SAFE_TICKER_RE, na=False)

    df = df[mask].copy()
    removed = before - len(df)
    if removed:
        log.info("Filtered out %d warrants/rights/units/WI/invalid symbols, %d remain",
                 removed, len(df))
    return df


# ============================================================================
# yfinance validation
# ============================================================================

class EmptyValidationBatch(RuntimeError):
    """The provider answered without raising, but returned nothing usable.

    Audit 2026-08-16 (F3): this is what RATE LIMITING looks like — an empty or
    all-NaN frame, not an exception. `_validate_via_yfinance`'s retry and
    per-ticker fallback were keyed on `except Exception`, so for the single
    most likely failure mode they never ran, and every symbol in the batch was
    marked failed and then DELETED from the universe. Raising turns the silent
    case into the loud one the fallback already handles.
    """


def _run_validation_batch(batch: list[str]) -> tuple[set[str], set[str]]:
    """One-shot batch validation via `yf.download(..., period='5d')`. Returns
    (valid, failed) sets over the batch. Raises on network/API error — and on a
    wholly-empty response (see EmptyValidationBatch) — so the caller can apply
    retry/fallback logic."""
    valid: set[str] = set()
    failed: set[str] = set()
    joined = " ".join(batch)
    data = yf.download(
        joined,
        period="5d",
        progress=False,
        threads=True,
        group_by="ticker",
    )

    # A batch of real listed symbols never comes back completely empty. When it
    # does, the provider is throttling us, not telling us the symbols are dead.
    if data is None or getattr(data, "empty", True):
        raise EmptyValidationBatch(
            f"yfinance returned an empty frame for {len(batch)} symbol(s)"
        )

    if len(batch) == 1:
        sym = batch[0]
        if data is not None and not data.empty and data["Close"].notna().any():
            valid.add(sym)
        else:
            failed.add(sym)
        return valid, failed

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
    return valid, failed


def _probe_single_ticker(sym: str) -> bool:
    """Last-resort single-ticker validation. Returns True if yfinance has
    any close data for the symbol over the last 5 days."""
    try:
        d = yf.Ticker(sym).history(period="5d")
        return bool(d is not None and not d.empty and d["Close"].notna().any())
    except Exception:
        return False


def _validate_via_yfinance(symbols: list[str]) -> tuple[set[str], set[str]]:
    """
    Validate symbols by attempting to download 1 day of data from yfinance.
    Returns (valid_set, failed_set).

    Phase 4 R8: on batch exception, retry once after a pause. If the retry
    also fails, fall back to per-ticker probes so a single flaky HTTP error
    cannot blanket-fail 500 otherwise-valid tickers.
    """
    valid: set[str] = set()
    failed: set[str] = set()
    total = len(symbols)
    batch_size = config.VALIDATE_BATCH_SIZE

    log.info("yfinance validation: %d tickers in batches of %d ...", total, batch_size)

    for i in range(0, total, batch_size):
        batch = symbols[i : i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (total + batch_size - 1) // batch_size

        try:
            b_valid, b_failed = _run_validation_batch(batch)
            valid.update(b_valid)
            failed.update(b_failed)
        except Exception as exc:
            log.warning("Validation batch %d error: %s — retrying once",
                        batch_num, exc)
            time.sleep(config.VALIDATE_PAUSE_SEC * 2)
            try:
                b_valid, b_failed = _run_validation_batch(batch)
                valid.update(b_valid)
                failed.update(b_failed)
            except Exception as exc2:
                log.warning("Validation batch %d retry failed (%s) — "
                            "probing %d tickers individually",
                            batch_num, exc2, len(batch))
                for sym in batch:
                    if _probe_single_ticker(sym):
                        valid.add(sym)
                    else:
                        failed.add(sym)
                    time.sleep(0.1)  # gentle per-ticker pacing

        log.info("  Validation batch %d/%d: %d valid so far, %d failed",
                 batch_num, total_batches, len(valid), len(failed))

        if i + batch_size < total:
            time.sleep(config.VALIDATE_PAUSE_SEC)

    return valid, failed


# ============================================================================
# Public API
# ============================================================================

def _load_previous_universe(csv_path: Path) -> set:
    """Symbols from the existing universe.csv, or an empty set if there is no
    file yet.

    Audit 2026-08-16 (F3): this read used to be `except Exception: pass`
    inline. A locked file, a partial read, or a renamed column silently yielded
    an EMPTY set — which flips the caller from "validate only what's new" to
    "re-validate all ~16,000 symbols". Pair that with a throttling provider and
    the refresh replaces universe.csv with whatever survived.

    A MISSING file is a legitimate first run and returns empty. A file that
    exists and cannot be read raises: the safest outcome is to leave the
    existing universe alone, since it is at most UNIVERSE_STALE_DAYS out of
    date, whereas a truncated one silently narrows every scan.
    """
    if not csv_path.exists():
        return set()
    try:
        prev_df = pd.read_csv(csv_path)
        return set(prev_df["symbol"].unique())
    except Exception as exc:
        log.error(
            "Could not read the existing universe at %s (%s) — ABORTING the "
            "refresh rather than re-validating everything against a "
            "possibly-throttled provider and overwriting it with whatever "
            "survives.", csv_path, exc,
        )
        raise RuntimeError(
            f"universe.csv exists but could not be read: {exc}"
        ) from exc


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

    # ── Read the existing universe ONCE (audit 2026-08-16, F3) ──
    # Needed twice: as the "already validated" set below, and as the baseline
    # for the shrink floor at the end. Read up front so an unreadable file
    # fails the run immediately, before hours of network work.
    prev_syms = _load_previous_universe(csv_path)
    prev_count = len(prev_syms)

    # ── Source 1: NASDAQ FTP ──
    ftp_df = _fetch_nasdaq_ftp()
    ftp_count = 0
    if not ftp_df.empty:
        # Normalise symbols. Audit 2026-08-12 (EFF-10): vectorised `.str`
        # accessors instead of a Python callable per row over ~16k rows.
        # Same semantics as `_normalise_symbol` / `_detect_adr`, which remain
        # the scalar reference implementations (and are still unit-tested).
        ftp_df["symbol"] = (
            ftp_df["symbol_raw"].astype(str).str.strip().str.upper()
            .str.replace(".", "-", regex=False)
        )
        ftp_df["adr"] = (
            ftp_df["name"].fillna("").astype(str)
            .str.contains(_ADR_KEYWORDS, regex=True, na=False)
        )
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

    # Audit 2026-08-16 (F3): refuse a catastrophic shrink. universe.csv feeds
    # EVERY scan and every fill's work list, and validation failures are
    # destructive — a symbol that fails is dropped from the file. The codebase
    # already applies exactly this kind of sanity cap to an upstream response
    # (FINANCEDATABASE_MAX_ROWS refuses an absurd dataset); the universe, which
    # matters far more, had none. Keeping the previous file is always the safer
    # error: it is at most UNIVERSE_STALE_DAYS out of date, whereas a truncated
    # one silently narrows every scan until someone notices.
    if prev_count and len(out) < prev_count * (1.0 - config.UNIVERSE_MAX_SHRINK_PCT / 100.0):
        raise RuntimeError(
            f"Refusing to write universe.csv: {len(out):,} symbols is more "
            f"than {config.UNIVERSE_MAX_SHRINK_PCT:.0f}% below the previous "
            f"{prev_count:,}. This usually means the validation provider was "
            f"throttling. The existing universe.csv has been left untouched; "
            f"re-run the refresh later."
        )
    # Atomic write (temp + os.replace) so a crash/power-loss/kill mid-write
    # can't leave a truncated universe.csv that the next load_universe() then
    # fails to parse — matches the "atomic writes everywhere" invariant the
    # parquet caches already follow. This was the lone non-atomic CSV holdout.
    config.atomic_write_csv(out, csv_path, index=False)
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
