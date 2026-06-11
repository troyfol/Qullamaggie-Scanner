"""
Sector mapping data layer.

Maps each ticker to its GICS sector and corresponding sector ETF.
Storage: scanner_data/sector_map.parquet
"""

import logging
import time
from datetime import datetime

import pandas as pd

from . import config

log = logging.getLogger("scanner.sector")


# ---------------------------------------------------------------------------
# Load / Save
# ---------------------------------------------------------------------------

def load_sector_map() -> pd.DataFrame | None:
    """Read sector_map.parquet.  Returns None if file does not exist."""
    path = config.SECTOR_MAP_PARQUET
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        log.warning("Failed to read sector_map.parquet: %s", exc)
        return None


def save_sector_map(df: pd.DataFrame) -> None:
    """Write sector_map.parquet (atomic temp-file rename so a crash mid-write
    cannot corrupt the cache)."""
    config.atomic_write_parquet(
        df, config.SECTOR_MAP_PARQUET, engine="pyarrow", index=False,
    )


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

    try:
        equities = fd.Equities()
        fd_df = equities.select()
    except Exception as exc:
        log.error("Failed to load FinanceDatabase: %s", exc)
        return 0, 0

    if fd_df is None or fd_df.empty:
        log.warning("FinanceDatabase returned empty dataset")
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
    now = datetime.now()
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
            "updated_at": now,
        })
        filled += 1

        if progress_cb and i % 500 == 0:
            progress_cb(i, total)

    if not rows:
        log.info("Bulk fill: no sector data found for universe tickers")
        return 0, skipped

    new_df = pd.DataFrame(rows)

    # Merge with existing data (existing entries preserved, new ones added/updated)
    existing = load_sector_map()
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

def _flush_sector_rows(new_rows: list[dict]) -> None:
    """Merge a batch of new sector rows into the on-disk parquet (atomic)."""
    if not new_rows:
        return
    existing = load_sector_map()
    new_df = pd.DataFrame(new_rows)
    if existing is not None and not existing.empty:
        combined = pd.concat([existing, new_df])
        combined = combined.drop_duplicates(subset=["ticker"], keep="last")
    else:
        combined = new_df
    save_sector_map(combined)


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

    now = datetime.now()
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
        if finnhub_active:
            sector = finnhub_client.fetch_sector(sym)

        # Provider 2: yfinance .info fallback
        if not sector:
            try:
                info = yf.Ticker(sym).info
                yf_sector = info.get("sector")
                if yf_sector and not pd.isna(yf_sector):
                    sector = str(yf_sector)
            except Exception as exc:
                log.debug("yfinance .info failed for %s: %s", sym, exc)

        if sector:
            sector_etf = config.SECTOR_ETF_MAP.get(str(sector), "")
            pending.append({
                "ticker": sym,
                "sector": str(sector),
                "sector_etf": sector_etf,
                "updated_at": now,
            })
            filled += 1
        else:
            errors += 1

        if progress_cb:
            progress_cb(i + 1, total)

        # Periodic flush so progress survives a kill / scanner close mid-fill
        if len(pending) >= flush_every:
            _flush_sector_rows(pending)
            log.info(
                "Targeted sectors: flushed %d rows (%d/%d processed, "
                "%d filled, %d errors so far)",
                len(pending), i + 1, total, filled, errors,
            )
            pending = []

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
