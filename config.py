"""
Central configuration for the Trading Scanner project.
All paths, defaults, and tunable constants live here.
"""

import sys
from datetime import date, timedelta
from pathlib import Path

# -- Paths ------------------------------------------------------------------
# When packaged (PyInstaller), sys.executable is the built exe so scanner_data/
# lives beside it. In dev (python -m trade_scanner_fh), sys.executable is the
# venv's python.exe — we anchor to the package directory instead so dev and
# packaged runs both keep scanner_data/ next to the application code/exe.
if getattr(sys, "frozen", False):
    APP_ROOT = Path(sys.executable).resolve().parent
else:
    APP_ROOT = Path(__file__).resolve().parent

DATA_DIR = APP_ROOT / "scanner_data"
PARQUET_DIR = DATA_DIR / "ohlcv"          # one .parquet per ticker
LOG_DIR = DATA_DIR / "logs"
TICKER_CSV = DATA_DIR / "universe.csv"    # cached ticker list (full metadata)
FAILED_TICKERS_LOG = DATA_DIR / "failed_tickers.log"
FTP_RAW_DIR = DATA_DIR / "ftp_raw"       # raw FTP downloads

# -- Raw earnings layer (Phase 1 of Finnhub augmentation) -----------------
# One append-only parquet per source per UTC calendar day so reconciler
# logic can be replayed against frozen captures without re-scraping. See
# earnings_raw.py for the schema per source.
RAW_EARNINGS_DIR = DATA_DIR / "earnings_raw"
RAW_SOURCE_ZACKS = "zacks"
RAW_SOURCE_FINNHUB = "finnhub"
RAW_SOURCE_NASDAQ = "nasdaq"
RAW_SOURCE_YAHOO = "yahoo"
RAW_SOURCES = (RAW_SOURCE_ZACKS, RAW_SOURCE_FINNHUB, RAW_SOURCE_NASDAQ, RAW_SOURCE_YAHOO)
# Files older than this many days get pruned at app startup.
RAW_RETENTION_DAYS = 30


def ensure_dirs() -> None:
    """Create the standard scanner_data/ subdirectories.

    Called from entry points (GUI main(), fill workers, tests) rather than at
    import time so merely importing config has no filesystem side effects.
    """
    for d in (DATA_DIR, PARQUET_DIR, LOG_DIR, FTP_RAW_DIR, RAW_EARNINGS_DIR):
        d.mkdir(parents=True, exist_ok=True)
    for src in RAW_SOURCES:
        (RAW_EARNINGS_DIR / src).mkdir(parents=True, exist_ok=True)


# -- Atomic file helpers ----------------------------------------------------

def atomic_write_text(path: Path, content: str, encoding: str = "utf-8") -> None:
    """Write text via a temp file + os.replace so a crash mid-write cannot
    corrupt the target file."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(content, encoding=encoding)
    tmp.replace(path)


def atomic_write_parquet(df, path: Path, **kwargs) -> None:
    """Write a DataFrame to parquet via a temp file + os.replace."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(tmp, **kwargs)
    tmp.replace(path)


# -- NYSE trading calendar (hardcoded; refresh set after 2028) --------------
# Full-closure holidays only — we use daily bars so early-close days are not
# special-cased. Update this set when extending past _NYSE_HOLIDAYS_MAX_YEAR.
_NYSE_HOLIDAYS: set[date] = {
    # 2024
    date(2024, 1, 1), date(2024, 1, 15), date(2024, 2, 19), date(2024, 3, 29),
    date(2024, 5, 27), date(2024, 6, 19), date(2024, 7, 4), date(2024, 9, 2),
    date(2024, 11, 28), date(2024, 12, 25),
    # 2025  (Jan 9 = Carter state funeral)
    date(2025, 1, 1), date(2025, 1, 9), date(2025, 1, 20), date(2025, 2, 17),
    date(2025, 4, 18), date(2025, 5, 26), date(2025, 6, 19), date(2025, 7, 4),
    date(2025, 9, 1), date(2025, 11, 27), date(2025, 12, 25),
    # 2026  (Jul 3 observed for Jul 4 on Saturday)
    date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16), date(2026, 4, 3),
    date(2026, 5, 25), date(2026, 6, 19), date(2026, 7, 3), date(2026, 9, 7),
    date(2026, 11, 26), date(2026, 12, 25),
    # 2027  (Jun 18 for Jun 19/Sat; Jul 5 for Jul 4/Sun; Dec 24 for Dec 25/Sat)
    date(2027, 1, 1), date(2027, 1, 18), date(2027, 2, 15), date(2027, 3, 26),
    date(2027, 5, 31), date(2027, 6, 18), date(2027, 7, 5), date(2027, 9, 6),
    date(2027, 11, 25), date(2027, 12, 24),
    # 2028  (Jan 1 is Sat → not observed by NYSE)
    date(2028, 1, 17), date(2028, 2, 21), date(2028, 4, 14), date(2028, 5, 29),
    date(2028, 6, 19), date(2028, 7, 4), date(2028, 9, 4), date(2028, 11, 23),
    date(2028, 12, 25),
}
_NYSE_HOLIDAYS_MAX_YEAR = 2028


def most_recent_trading_day(reference: date) -> date:
    """Return the most recent NYSE full-trading-day on or before `reference`.
    Walks backwards past weekends and full-closure holidays.

    If `reference` is past the hardcoded holiday range, weekends are still
    honored but holidays after the range will be treated as trading days —
    keep the hardcoded set current.
    """
    d = reference
    for _ in range(14):  # bounded walk — 14 days is plenty for any weekend+holiday run
        if d.weekday() < 5 and d not in _NYSE_HOLIDAYS:
            return d
        d -= timedelta(days=1)
    return d  # defensive fallback; should not be reached


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
# SEC's fair-access policy requires every EDGAR API caller to declare a real
# contact email in the request User-Agent. A generic "Mozilla/5.0" returns
# 403 Forbidden — see
# https://www.sec.gov/about/webmaster-frequently-asked-questions.
#
# The contact email is per-user — never hardcoded. get_sec_contact_email()
# resolves it at request time, in priority order:
#   1. scanner_data/sec_contact.txt  (set via Settings → Set SEC Contact Email…)
#   2. the SEC_CONTACT_EMAIL environment variable
#   3. SEC_CONTACT_PLACEHOLDER below — a non-functional placeholder
# When only the placeholder is available, ticker_universe skips the SEC
# source (the other three universe sources still run).
SEC_USER_AGENT_PRODUCT = "TradingScanner/1.0"
SEC_CONTACT_PLACEHOLDER = "your.email@example.com"
SEC_CONTACT_ENV_VAR = "SEC_CONTACT_EMAIL"
_SEC_CONTACT_FILENAME = "sec_contact.txt"


def _sec_contact_path() -> Path:
    """Path to the per-user SEC contact-email file. Computed lazily so
    tests can monkeypatch config.DATA_DIR."""
    return DATA_DIR / _SEC_CONTACT_FILENAME


def get_sec_contact_email() -> str:
    """Resolve the SEC EDGAR contact email.

    Priority: scanner_data/sec_contact.txt → $SEC_CONTACT_EMAIL →
    SEC_CONTACT_PLACEHOLDER. Always returns a non-empty string."""
    import os
    try:
        path = _sec_contact_path()
        if path.exists():
            v = path.read_text(encoding="utf-8").strip()
            if v:
                return v
    except OSError:
        pass
    env = (os.environ.get(SEC_CONTACT_ENV_VAR) or "").strip()
    if env:
        return env
    return SEC_CONTACT_PLACEHOLDER


def set_sec_contact_email(email: str) -> bool:
    """Persist the SEC contact email to scanner_data/sec_contact.txt.
    Pass an empty string to clear it. Returns True on success."""
    path = _sec_contact_path()
    try:
        email = (email or "").strip()
        if email:
            atomic_write_text(path, email)
        else:
            path.unlink(missing_ok=True)
        return True
    except OSError:
        return False


def sec_contact_is_configured() -> bool:
    """True iff a real (non-placeholder) contact email is available so the
    SEC EDGAR universe source can run."""
    email = get_sec_contact_email()
    return bool(email) and email != SEC_CONTACT_PLACEHOLDER and "@" in email


def get_sec_user_agent() -> str:
    """Build the User-Agent string for SEC EDGAR requests."""
    return f"{SEC_USER_AGENT_PRODUCT} {get_sec_contact_email()}"

# -- Ticker filtering defaults ----------------------------------------------
EXCLUDE_WARRANTS = True     # symbols ending W (5-char)
EXCLUDE_RIGHTS = True       # symbols ending R (5-char)
EXCLUDE_UNITS = True        # symbols ending U (5-char)
EXCLUDE_WHEN_ISSUED = True  # symbols ending WI

# -- yfinance validation ---------------------------------------------------
VALIDATE_BATCH_SIZE = 500          # tickers per yf.download() batch
VALIDATE_PAUSE_SEC = 1.0           # pause between validation batches (Phase 2 I7: 3.0 → 1.0)
YFINANCE_BATCH_SIZE = 50           # tickers per yfinance.download() call
YFINANCE_PAUSE_SEC = 0.5           # pause between OHLCV batches

# -- Debug / diagnostics ----------------------------------------------------
SAVE_FTP_RAW = False               # Phase 2 I12: persist raw NASDAQ FTP files for debugging

# -- OHLCV Download --------------------------------------------------------
OHLCV_HISTORY_YEARS = 5          # max years of daily data to cache

# -- Parquet schema versioning (Phase 4 R18) -------------------------------
# Bump when the per-ticker OHLCV parquet column set or dtypes change in a way
# that breaks forward/backward load compatibility. On mismatch, the scanner
# logs a warning and (in future versions) may refuse to merge old + new data.
PARQUET_SCHEMA_VERSION = 1
PARQUET_SCHEMA_FILE = PARQUET_DIR / "_schema_version.txt"

# -- Data Validation -------------------------------------------------------
PRICE_JUMP_PCT = 50.0            # flag if single-day % change exceeds this
MAX_MISSING_DAYS_FLAG = 5        # flag if > N trading days missing in a row

# -- Universe staleness (days) ---------------------------------------------
UNIVERSE_STALE_DAYS = 7

# -- Reference / Benchmark Tickers ----------------------------------------
# Always kept in OHLCV cache; used for RS calculations, never in scan results
REFERENCE_TICKERS = [
    "SPY",   # S&P 500 — benchmark for RS vs. S&P
    "ONEQ",  # NASDAQ Composite — benchmark for RS vs. NASDAQ
    "XLK",   # Technology
    "XLF",   # Financials
    "XLE",   # Energy
    "XLV",   # Health Care
    "XLI",   # Industrials
    "XLC",   # Communication Services
    "XLY",   # Consumer Discretionary
    "XLP",   # Consumer Staples
    "XLB",   # Materials
    "XLRE",  # Real Estate
    "XLU",   # Utilities
]

# -- Sector mapping --------------------------------------------------------
SECTOR_MAP_PARQUET = DATA_DIR / "sector_map.parquet"
EARNINGS_PARQUET = DATA_DIR / "earnings_dates.parquet"
# Zacks-fork addition — per-quarter earnings history (EPS + revenue +
# surprises). The legacy EARNINGS_PARQUET above keeps storing just
# last/next earnings dates and continues to drive the Days Since /
# Days Until filters unchanged. See earnings_history.py for the schema.
EARNINGS_HISTORY_PARQUET = DATA_DIR / "earnings_history.parquet"
# Phase 2: hard cap on history depth for both Zacks and Finnhub fills.
# Anything older than `today - EARNINGS_HISTORY_YEARS` is dropped before
# write. Keeps the parquet bounded on disk and aligns the two sources
# so dedup (Zacks > Finnhub by period_ending) covers the same window.
EARNINGS_HISTORY_YEARS = 5

# Phase 2 — Finnhub fill resilience knobs.
# Free-tier limit is 60 req/min; 1.15s pacing yields ~52/min with
# headroom for retries. See finnhub_earnings_backfill.md.
FINNHUB_MIN_INTERVAL_SEC = 1.15
# After this many consecutive non-empty failures (429 / 5xx / network
# errors — NOT empty `[]` responses, those count as ETF identifications)
# the bulk worker pauses, verifies the API key, and rewinds to the first
# ticker in the failure window. Empty responses reset the streak.
FINNHUB_CONSEC_BLOCK_LIMIT = 3
# Initial pause length after a block trigger. Doubles on each subsequent
# block within the same run, up to the max — at which point the worker
# halts and asks the user.
FINNHUB_INITIAL_BLOCK_PAUSE_SEC = 60
FINNHUB_MAX_BLOCK_PAUSE_SEC = 300
# After this many block-triggered pauses within a single run, halt.
FINNHUB_MAX_BLOCKS_PER_RUN = 3
# Bulk-run checkpoint lives here; resumes survive process restart.
FINNHUB_BULK_CHECKPOINT = DATA_DIR / ".finnhub_bulk_checkpoint.json"
# Per-ticker side-blacklist file (mirrors zacks_blacklist.txt pattern).
# Tickers that return [] from /stock/earnings get added here and skipped
# on subsequent runs. Universe-level blacklist is unioned in at run start.
FINNHUB_BLACKLIST_FILE = DATA_DIR / "finnhub_blacklist.txt"

# -- Zacks daily smart refresh (Phase 5 — DEPRECATED in Phase 5 of the
# Finnhub augmentation; kept as a constant so test fixtures + the
# helper function still resolve. Auto-trigger at launch removed; the
# helper is no longer wired to anything that runs automatically.)
ZACKS_AUTO_REFRESH_ENABLED = False

# -- Nasdaq weekly calendar auto-refresh (Phase 5 of Finnhub
# augmentation) — replaces the per-launch Zacks smart-refresh as the
# only piece of automation that survives.
NASDAQ_AUTO_REFRESH_ENABLED = True
# Min interval between auto-refreshes. Manual "Refresh Now" stamps the
# timestamp so back-to-back manual + auto runs don't double up.
NASDAQ_WEEKLY_REFRESH_DAYS = 7

# Days-since-last-report thresholds that mark a ticker as "due" for a
# smart-refresh poll. Quarterly cadence is ~90 days, so 95 = "should have
# reported by now if calendar is fresh"; 100 = "long-stale, recheck even
# when we have no forward calendar".
ZACKS_REFRESH_STALE_DAYS = 95
ZACKS_REFRESH_LONG_STALE_DAYS = 100
# Window (in days) around `today` within which a `next_earnings` value
# counts as "in the past or just happened" — i.e., the ticker is highly
# likely to have a fresh report we should pull.
ZACKS_REFRESH_NEXT_RECENT_DAYS = 7

# When the smart-refresh candidate set exceeds this count, the launch
# sequence assumes the user is on a first-time install (or recovering
# from data loss) and prompts before running — a 14k-ticker daily
# refresh is functionally a multi-hour bulk fill, not a daily top-up.
# Above the threshold the user picks "Run Now" / "Skip" / "Disable
# auto-refresh"; under the threshold the smart refresh proceeds
# silently as designed.
ZACKS_SMART_REFRESH_BULK_THRESHOLD = 1000

SECTOR_ETF_MAP = {
    # ── Top-level GICS sectors (paid-tier Finnhub `gsector`, financedatabase) ──
    "Technology": "XLK",
    "Information Technology": "XLK",
    "Health Care": "XLV",
    "Healthcare": "XLV",
    "Financials": "XLF",
    "Financial Services": "XLF",
    "Consumer Discretionary": "XLY",
    "Consumer Cyclical": "XLY",
    "Consumer Staples": "XLP",
    "Consumer Defensive": "XLP",
    "Industrials": "XLI",
    "Energy": "XLE",
    "Materials": "XLB",
    "Basic Materials": "XLB",
    "Real Estate": "XLRE",
    "Utilities": "XLU",
    "Communication Services": "XLC",
    "Communication": "XLC",

    # ── GICS sub-industries / industry-group names returned by Finnhub's
    # free-tier `finnhubIndustry` field (paid `gsector` is more granular's
    # parent). Mapping to SPDR sector ETF so the scanner's relative-strength
    # vs sector calc has a benchmark for these tickers. ──

    # Financials
    "Banking": "XLF",
    "Insurance": "XLF",

    # Health Care
    "Biotechnology": "XLV",
    "Pharmaceuticals": "XLV",
    "Life Sciences Tools & Services": "XLV",

    # Information Technology
    "Semiconductors": "XLK",

    # Industrials (incl. transportation, capital goods, commercial services)
    "Aerospace & Defense": "XLI",
    "Airlines": "XLI",
    "Building": "XLI",                       # GICS Construction & Engineering
    "Commercial Services & Supplies": "XLI",
    "Construction": "XLI",
    "Electrical Equipment": "XLI",
    "Industrial Conglomerates": "XLI",
    "Logistics & Transportation": "XLI",
    "Machinery": "XLI",
    "Marine": "XLI",                         # Marine transportation
    "Professional Services": "XLI",
    "Road & Rail": "XLI",
    "Trading Companies & Distributors": "XLI",
    "Transportation Infrastructure": "XLI",

    # Materials
    "Chemicals": "XLB",
    "Metals & Mining": "XLB",
    "Packaging": "XLB",                      # Containers & Packaging

    # Consumer Discretionary
    "Auto Components": "XLY",
    "Automobiles": "XLY",
    "Consumer products": "XLY",              # ambiguous; default discretionary
    "Distributors": "XLY",                   # GICS Distributors (cons-disc)
    "Diversified Consumer Services": "XLY",
    "Hotels, Restaurants & Leisure": "XLY",
    "Leisure Products": "XLY",
    "Retail": "XLY",                         # default discretionary
    "Textiles, Apparel & Luxury Goods": "XLY",

    # Consumer Staples
    "Beverages": "XLP",
    "Food Products": "XLP",
    "Tobacco": "XLP",

    # Communication Services
    "Communications": "XLC",
    "Media": "XLC",
    "Telecommunication": "XLC",
}
