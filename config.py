"""
Central configuration for the Trading Scanner project.
All paths, defaults, and tunable constants live here.
"""

import sys
from pathlib import Path

# -- Paths ------------------------------------------------------------------
# Portable: data lives next to the executable (or next to the package in dev)
APP_ROOT = Path(sys.executable).resolve().parent
DATA_DIR = APP_ROOT / "scanner_data"
PARQUET_DIR = DATA_DIR / "ohlcv"          # one .parquet per ticker
LOG_DIR = DATA_DIR / "logs"
TICKER_CSV = DATA_DIR / "universe.csv"    # cached ticker list (full metadata)
FAILED_TICKERS_LOG = DATA_DIR / "failed_tickers.log"
FTP_RAW_DIR = DATA_DIR / "ftp_raw"       # raw FTP downloads

# Ensure directories exist on import
for _d in (DATA_DIR, PARQUET_DIR, LOG_DIR, FTP_RAW_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# -- NASDAQ FTP (Source 1) --------------------------------------------------
NASDAQ_FTP_HOST = "ftp.nasdaqtrader.com"
NASDAQ_FTP_DIR = "SymbolDirectory"
NASDAQ_FTP_FILES = ["nasdaqtraded.txt", "nasdaqlisted.txt", "otherlisted.txt"]

# -- GitHub rreichel3 (Source 2) --------------------------------------------
GITHUB_TICKERS_URL = (
    "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols"
    "/main/all/all_tickers.txt"
)

# -- SEC EDGAR (Source 4) ---------------------------------------------------
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

# -- Ticker filtering defaults ----------------------------------------------
EXCLUDE_WARRANTS = True     # symbols ending W (5-char)
EXCLUDE_RIGHTS = True       # symbols ending R (5-char)
EXCLUDE_UNITS = True        # symbols ending U (5-char)
EXCLUDE_WHEN_ISSUED = True  # symbols ending WI

# -- yfinance validation ---------------------------------------------------
VALIDATE_BATCH_SIZE = 500          # tickers per yf.download() batch
VALIDATE_PAUSE_SEC = 3.0           # pause between validation batches
YFINANCE_BATCH_SIZE = 50           # tickers per yfinance.download() call
YFINANCE_PAUSE_SEC = 0.5           # pause between OHLCV batches

# -- OHLCV Download --------------------------------------------------------
OHLCV_HISTORY_YEARS = 5          # max years of daily data to cache

# -- Data Validation -------------------------------------------------------
PRICE_JUMP_PCT = 50.0            # flag if single-day % change exceeds this
MAX_MISSING_DAYS_FLAG = 5        # flag if > N trading days missing in a row

# -- Universe staleness (days) ---------------------------------------------
UNIVERSE_STALE_DAYS = 7
