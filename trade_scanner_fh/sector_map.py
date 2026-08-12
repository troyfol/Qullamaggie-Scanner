"""
Sector mapping data layer.

Maps each ticker to its GICS sector and corresponding sector ETF.
Storage: scanner_data/sector_map.parquet
"""

import logging
import threading
import time
from datetime import datetime

import pandas as pd

from . import config

log = logging.getLogger("scanner.sector")

# Serializes the load → merge → save cycle for sector_map.parquet across
# worker threads. Audit 2026-08-12 (INT-2): both writers (bulk_fill_sectors and
# _flush_sector_rows) did unprotected read-modify-write from QThreads, so two
# concurrent flushes could each load the same snapshot and the second would
# discard the first's rows. Re-entrant to match HISTORY_WRITE_LOCK's contract.
SECTOR_WRITE_LOCK = threading.RLock()

# Read-retry budget, mirroring earnings_history. Audit 2026-08-12 (INT-2): this
# module had NO retry at all, so a single transient lock truncated the map.
_READ_ATTEMPTS = 3
_READ_BACKOFF_SEC = 0.4

# Set by load_sector_map(): True when the file EXISTS but could not be read.
_LAST_READ_FAILED = False


def sector_map_read_failed() -> bool:
    """True if the most recent ``load_sector_map()`` failed to READ an existing
    file, as opposed to finding no file. Audit 2026-08-12 (INT-2)."""
    return _LAST_READ_FAILED


# ---------------------------------------------------------------------------
# Load / Save
# ---------------------------------------------------------------------------

def load_sector_map() -> pd.DataFrame | None:
    """Read sector_map.parquet.  Returns None if file does not exist.

    Audit 2026-08-12 (INT-2): now retries a transient read failure and records
    the missing-vs-unreadable distinction via ``sector_map_read_failed()`` so
    ``_flush_sector_rows`` cannot mistake an unreadable map for an absent one
    and overwrite the whole thing (measured: 400 rows → 1).
    """
    global _LAST_READ_FAILED
    path = config.SECTOR_MAP_PARQUET
    if not path.exists():
        return None
    last_exc = None
    for attempt in range(_READ_ATTEMPTS):
        try:
            df = pd.read_parquet(path)
            _LAST_READ_FAILED = False
            return df
        except Exception as exc:  # noqa: BLE001 - logged after the retries
            last_exc = exc
            if attempt < _READ_ATTEMPTS - 1:
                time.sleep(_READ_BACKOFF_SEC * (attempt + 1))
    _LAST_READ_FAILED = True
    log.error(
        "Failed to read sector_map.parquet after %d attempts: %s "
        "(callers MUST NOT treat this as an empty map)",
        _READ_ATTEMPTS, last_exc,
    )
    return None


def save_sector_map(df: pd.DataFrame) -> None:
    """Write sector_map.parquet (atomic temp-file rename so a crash mid-write
    cannot corrupt the cache)."""
    config.atomic_write_parquet(
        df, config.SECTOR_MAP_PARQUET, engine="pyarrow", index=False,
    )


def stale_sector_tickers(
    sector_df: pd.DataFrame | None = None,
    *,
    max_age_days: int | None = None,
) -> list[str]:
    """Tickers whose sector row is older than ``max_age_days``.

    Audit 2026-08-12 (INT-13): nothing ever READ ``updated_at``. There was no
    ``refresh_stale_sectors``, and ``targeted_fill_sectors`` only fills GAPS,
    so once a ticker had a sector row it was never revisited and a GICS
    reclassification never propagated. The live map was last written 99 days
    before the audit.

    Returned oldest-first so a bounded batch takes the most stale rows. Rows
    with no parseable ``updated_at`` count as stale — that is the pre-INT-13
    state, and re-fetching one is cheap.
    """
    days = (max_age_days if max_age_days is not None
            else config.SECTOR_STALE_DAYS)
    if sector_df is None:
        sector_df = load_sector_map()
    if sector_df is None or sector_df.empty:
        return []
    if "updated_at" not in sector_df.columns:
        return [str(t) for t in sector_df["ticker"]]
    ts = pd.to_datetime(sector_df["updated_at"], errors="coerce")
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
    stale = sector_df.loc[ts.isna() | (ts < cutoff)].copy()
    if stale.empty:
        return []
    stale["_ts"] = ts.loc[stale.index]
    stale = stale.sort_values("_ts", na_position="first")
    return [str(t) for t in stale["ticker"]]


def remap_missing_sector_etfs() -> int:
    """Fill ``sector_etf`` on rows that have a sector but no ETF, whenever
    ``config.SECTOR_ETF_MAP`` now covers that sector. Returns rows changed.

    Audit 2026-08-12 (INT-13): ``sector_etf == ""`` is indistinguishable from
    "not yet mapped", and ``get_sector_etf`` returns None for it. When
    SECTOR_ETF_MAP gained a sector name, the existing "" rows were never
    revisited, so those tickers permanently lost sector-relative strength.
    Network-free — a pure re-derivation from data already on disk — so it is
    safe to run at launch. Never raises.
    """
    try:
        with SECTOR_WRITE_LOCK:
            df = load_sector_map()
            if df is None or df.empty:
                return 0
            if "sector" not in df.columns or "sector_etf" not in df.columns:
                return 0
            blank = df["sector_etf"].isna() | (
                df["sector_etf"].astype(str).str.strip() == "")
            if not blank.any():
                return 0
            resolved = df.loc[blank, "sector"].astype(str).map(
                config.SECTOR_ETF_MAP)
            fixable = resolved.notna()
            if not fixable.any():
                return 0
            idx = resolved.loc[fixable].index
            df.loc[idx, "sector_etf"] = resolved.loc[fixable]
            save_sector_map(df)
            log.info("Re-mapped sector_etf for %d ticker(s) whose sector is "
                     "now covered by SECTOR_ETF_MAP", len(idx))
            return int(len(idx))
    except Exception as exc:      # must never block launch
        log.warning("sector_etf re-map skipped: %s", exc)
        return 0


def get_sector_etf(ticker: str, sector_df: pd.DataFrame) -> str | None:
    """Look up a ticker's sector ETF from the sector map DataFrame."""
    if sector_df is None or sector_df.empty:
        return None
    match = sector_df.loc[sector_df["ticker"] == ticker, "sector_etf"]
    if match.empty:
        return None
    val = match.iloc[0]
    if pd.isna(val) or val == "":
        return None
    return str(val)


# ---------------------------------------------------------------------------
# Bulk fill via financedatabase
# ---------------------------------------------------------------------------

def bulk_fill_sectors(
    universe_symbols: list[str],
    blacklist: set[str],
    progress_cb=None,
    stop_flag: list[bool] | None = None,
) -> tuple[int, int]:
    """
    Populate sector_map.parquet using the financedatabase package.

    Returns (filled_count, skipped_count).
    """
    try:
        import financedatabase as fd
    except ImportError:
        msg = ("financedatabase package not installed. "
               "Run: pip install financedatabase")
        log.error(msg)
        if progress_cb:
            progress_cb(-1, -1)  # signal error
        return 0, 0

    log.info("Bulk fill: loading FinanceDatabase equities...")
    if progress_cb:
        progress_cb(0, 1)

    # Audit 2026-08-12 (SEC-8): `fd.Equities().select()` downloads and
    # decompresses a dataset from GitHub at call time, inside a third-party
    # package — so it bypasses every timeout, size cap and rate limiter this
    # codebase carefully applies elsewhere, and is a decompression-bomb plus
    # supply-chain surface in one call. The library exposes no size or timeout
    # hook, so bound it here: a row-count sanity check catches an absurd
    # response before the per-row loop, and the run stays cancellable.
    try:
        equities = fd.Equities()
        fd_df = equities.select()
    except Exception as exc:
        log.error("Failed to load FinanceDatabase: %s", exc)
        return 0, 0

    if fd_df is None or fd_df.empty:
        log.warning("FinanceDatabase returned empty dataset")
        return 0, 0

    if len(fd_df) > config.FINANCEDATABASE_MAX_ROWS:
        log.error(
            "FinanceDatabase returned %d rows, above the %d-row sanity cap — "
            "refusing to process it. The real dataset is ~150k rows; this is "
            "either a library change or a bad upstream response.",
            len(fd_df), config.FINANCEDATABASE_MAX_ROWS,
        )
        return 0, 0

    # fd_df index is named 'symbol' — reset to column
    fd_df = fd_df.reset_index()
    if "symbol" in fd_df.columns:
        fd_df = fd_df.rename(columns={"symbol": "ticker"})
    elif "index" in fd_df.columns:
        fd_df = fd_df.rename(columns={"index": "ticker"})
    else:
        fd_df = fd_df.rename(columns={fd_df.columns[0]: "ticker"})

    # Filter to universe symbols, exclude blacklist
    universe_set = set(universe_symbols) - blacklist
    fd_df = fd_df[fd_df["ticker"].isin(universe_set)].copy()

    if "sector" not in fd_df.columns:
        log.error("FinanceDatabase data has no 'sector' column")
        return 0, 0

    # Map sector → sector ETF
    # Audit 2026-08-12 (INT-13): `updated_at` is stamped per row at append time,
    # not once at run start. The old run-start stamp made every row of a
    # multi-hour fill claim the same age, which is the same defect that was
    # already fixed for earnings (see earnings_history "Audit M2").
    rows = []
    filled = 0
    skipped = 0
    total = len(fd_df)

    for i, (_, row) in enumerate(fd_df.iterrows()):
        if stop_flag and stop_flag[0]:
            log.info("Bulk fill stopped by user at %d/%d", i, total)
            break

        ticker = row["ticker"]
        sector = row.get("sector")

        if pd.isna(sector) or sector == "" or sector is None:
            skipped += 1
            continue

        sector_etf = config.SECTOR_ETF_MAP.get(str(sector))
        rows.append({
            "ticker": ticker,
            "sector": str(sector),
            "sector_etf": sector_etf or "",
            "updated_at": datetime.now(),
            # INT-13: record WHICH provider supplied this sector — a
            # finnhub-sourced and a yfinance-sourced value were previously
            # indistinguishable, so a bad provider couldn't be targeted.
            "source": "financedatabase",
        })
        filled += 1

        if progress_cb and i % 500 == 0:
            progress_cb(i, total)

    if not rows:
        log.info("Bulk fill: no sector data found for universe tickers")
        return 0, skipped

    new_df = pd.DataFrame(rows)

    # Merge with existing data (existing entries preserved, new ones added/updated)
    # Audit 2026-08-12 (INT-2): guarded + locked, same as _flush_sector_rows.
    with SECTOR_WRITE_LOCK:
        existing = load_sector_map()
        if existing is None and sector_map_read_failed():
            log.error(
                "sector_map.parquet unreadable — abandoning the bulk merge of "
                "%d row(s) rather than truncating the map; re-run the fill",
                len(new_df),
            )
            return 0, skipped
        if existing is not None and not existing.empty:
            # Update existing, add new
            combined = pd.concat([existing, new_df])
            combined = combined.drop_duplicates(subset=["ticker"], keep="last")
        else:
            combined = new_df

        save_sector_map(combined)

    if progress_cb:
        progress_cb(total, total)

    log.info(
        "Bulk fill complete: %d sectors mapped, %d had no sector data, "
        "%d total in map",
        filled, skipped, len(combined),
    )
    return filled, skipped


# ---------------------------------------------------------------------------
# Targeted fill via yfinance .info
# ---------------------------------------------------------------------------

def _flush_sector_rows(new_rows: list[dict]) -> bool:
    """Merge a batch of new sector rows into the on-disk parquet (atomic).

    Returns True when the merge reached disk (or there was nothing to write),
    False when it was DEFERRED because the map existed but was unreadable
    (audit 2026-08-12, INT-2 — the caller must keep the batch for a retry).
    """
    if not new_rows:
        return True
    with SECTOR_WRITE_LOCK:
        existing = load_sector_map()
        # Audit 2026-08-12 (INT-2): never mistake "could not read the map" for
        # "there is no map" — that overwrote 400 rows with 1 in testing.
        if existing is None and sector_map_read_failed():
            log.error(
                "sector_map.parquet unreadable — deferring merge of %d row(s) "
                "to avoid truncating the map; it will be retried",
                len(new_rows),
            )
            return False
        new_df = pd.DataFrame(new_rows)
        if existing is not None and not existing.empty:
            combined = pd.concat([existing, new_df])
            combined = combined.drop_duplicates(subset=["ticker"], keep="last")
        else:
            combined = new_df
        save_sector_map(combined)
    return True


def targeted_fill_sectors(
    gap_tickers: list[str],
    blacklist: set[str],
    progress_cb=None,
    stop_flag: list[bool] | None = None,
    delay: float = 0.5,
    flush_every: int = 50,
) -> tuple[int, int]:
    """
    Fill sector data for tickers missing from sector_map.parquet using
    yfinance .info calls (one per ticker).

    Persists progress incrementally every `flush_every` successful fills
    so a long run that gets interrupted does not lose its work.

    Returns (filled_count, error_count).
    """
    import yfinance as yf
    from . import finnhub_client

    tickers = [t for t in gap_tickers if t not in blacklist]
    if not tickers:
        log.info("Targeted sector fill: no gaps to fill")
        return 0, 0

    finnhub_active = finnhub_client.is_configured()
    log.info(
        "Targeted sector fill: %d tickers to process (Finnhub %s)",
        len(tickers), "ENABLED" if finnhub_active else "disabled — yfinance only",
    )

    # Audit 2026-08-12 (INT-13): `updated_at` stamped per row at fetch time, not
    # once at run start — a multi-hour fill previously claimed one uniform age.
    pending: list[dict] = []
    filled = 0
    errors = 0
    total = len(tickers)

    for i, sym in enumerate(tickers):
        if stop_flag and stop_flag[0]:
            log.info("Targeted sector fill stopped at %d/%d", i, total)
            break

        sector: str | None = None

        # Provider 1: Finnhub /stock/profile2 (when configured)
        source_name = ""
        if finnhub_active:
            sector = finnhub_client.fetch_sector(sym)
            if sector:
                source_name = "finnhub"

        # Provider 2: yfinance .info fallback
        if not sector:
            try:
                info = yf.Ticker(sym).info
                yf_sector = info.get("sector")
                if yf_sector and not pd.isna(yf_sector):
                    sector = str(yf_sector)
                    source_name = "yfinance"
            except Exception as exc:
                log.debug("yfinance .info failed for %s: %s", sym, exc)

        if sector:
            sector_etf = config.SECTOR_ETF_MAP.get(str(sector), "")
            pending.append({
                "ticker": sym,
                "sector": str(sector),
                "sector_etf": sector_etf,
                "updated_at": datetime.now(),
                "source": source_name,
            })
            filled += 1
        else:
            errors += 1

        if progress_cb:
            progress_cb(i + 1, total)

        # Periodic flush so progress survives a kill / scanner close mid-fill
        # Audit 2026-08-12 (INT-2): only drop the batch once it reached disk.
        if len(pending) >= flush_every:
            if _flush_sector_rows(pending) is not False:
                log.info(
                    "Targeted sectors: flushed %d rows (%d/%d processed, "
                    "%d filled, %d errors so far)",
                    len(pending), i + 1, total, filled, errors,
                )
                pending = []
            else:
                log.warning(
                    "Targeted sectors: retaining %d row(s) in memory — the "
                    "sector-map write deferred and will be retried",
                    len(pending),
                )

        if (i + 1) % 200 == 0:
            log.info(
                "Targeted sectors: %d/%d processed (%d filled, %d errors)",
                i + 1, total, filled, errors,
            )

        time.sleep(delay)

    # Final flush of any remaining rows
    if pending:
        _flush_sector_rows(pending)

    log.info(
        "Targeted sector fill: %d filled, %d errors/no-data", filled, errors
    )
    return filled, errors
