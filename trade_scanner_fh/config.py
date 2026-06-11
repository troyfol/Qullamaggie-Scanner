"""
Central configuration for the Trading Scanner project.
All paths, defaults, and tunable constants live here.
"""

import os
import re
import sys
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover - fallback if tzdata missing
    ZoneInfo = None  # type: ignore

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
RAW_SOURCE_FINVIZ = "finviz"
RAW_SOURCES = (
    RAW_SOURCE_ZACKS,
    RAW_SOURCE_FINNHUB,
    RAW_SOURCE_NASDAQ,
    RAW_SOURCE_YAHOO,
    RAW_SOURCE_FINVIZ,
)
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

def _unique_tmp_path(path: Path) -> Path:
    """Collision-proof sibling temp path for an atomic write.

    The name includes the PID + a uuid so two concurrent writers to the same
    target can never share (and clobber) one ``.tmp`` file — without this, an
    unsynchronized second writer could interleave into the first's temp and
    ``os.replace`` could promote a half-written file (real corruption, not
    just a lost update). Lives beside the target so the rename stays on the
    same volume (atomic on Windows)."""
    return path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"


def atomic_write_text(path: Path, content: str, encoding: str = "utf-8") -> None:
    """Write text via a temp file + os.replace so a crash mid-write cannot
    corrupt the target file."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(content, encoding=encoding)
    tmp.replace(path)


def atomic_write_parquet(df, path: Path, **kwargs) -> None:
    """Write a DataFrame to parquet via a unique temp file + os.replace.

    Readers always see either the old complete file or the new complete file,
    never a torn write — even when two threads write the same target at once
    (the temp name is per-writer). The temp is removed if the write fails so a
    failed write leaves no residue."""
    tmp = _unique_tmp_path(path)
    tmp.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(tmp, **kwargs)
        tmp.replace(path)
    except BaseException:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def atomic_write_csv(df, path: Path, **kwargs) -> None:
    """Write a DataFrame to CSV via a unique temp file + os.replace
    (same crash/concurrency safety as ``atomic_write_parquet``)."""
    tmp = _unique_tmp_path(path)
    tmp.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_csv(tmp, **kwargs)
        tmp.replace(path)
    except BaseException:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


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
    # 2029
    date(2029, 1, 1), date(2029, 1, 15), date(2029, 2, 19), date(2029, 3, 30),
    date(2029, 5, 28), date(2029, 6, 19), date(2029, 7, 4), date(2029, 9, 3),
    date(2029, 11, 22), date(2029, 12, 25),
    # 2030
    date(2030, 1, 1), date(2030, 1, 21), date(2030, 2, 18), date(2030, 4, 19),
    date(2030, 5, 27), date(2030, 6, 19), date(2030, 7, 4), date(2030, 9, 2),
    date(2030, 11, 28), date(2030, 12, 25),
    # 2031
    date(2031, 1, 1), date(2031, 1, 20), date(2031, 2, 17), date(2031, 4, 11),
    date(2031, 5, 26), date(2031, 6, 19), date(2031, 7, 4), date(2031, 9, 1),
    date(2031, 11, 27), date(2031, 12, 25),
    # 2032  (Jun 19 Sat → Fri 18; Jul 4 Sun → Mon 5; Dec 25 Sat → Fri 24)
    date(2032, 1, 1), date(2032, 1, 19), date(2032, 2, 16), date(2032, 3, 26),
    date(2032, 5, 31), date(2032, 6, 18), date(2032, 7, 5), date(2032, 9, 6),
    date(2032, 11, 25), date(2032, 12, 24),
}
_NYSE_HOLIDAYS_MAX_YEAR = 2032
_warned_holiday_expiry = False


def most_recent_trading_day(reference: date) -> date:
    """Return the most recent NYSE full-trading-day on or before `reference`.
    Walks backwards past weekends and full-closure holidays.

    If `reference` is past the hardcoded holiday range, weekends are still
    honored but holidays after the range will be treated as trading days —
    keep the hardcoded set current.
    """
    global _warned_holiday_expiry
    if reference.year > _NYSE_HOLIDAYS_MAX_YEAR and not _warned_holiday_expiry:
        _warned_holiday_expiry = True
        import logging
        logging.getLogger("scanner").warning(
            "NYSE holiday table only covers through %d; dates in %d are not "
            "holiday-adjusted (weekends still honored). Extend _NYSE_HOLIDAYS.",
            _NYSE_HOLIDAYS_MAX_YEAR, reference.year,
        )
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
# The contact email resolves at request time, in priority order:
#   1. scanner_data/sec_contact.txt  (set via Settings → Set SEC Contact Email…)
#   2. the SEC_CONTACT_EMAIL environment variable
#   3. SEC_CONTACT_DEFAULT below — a non-functional placeholder.
# The default is deliberately a placeholder so no real contact email ever
# lives in source. sec_contact_is_configured() keeps the SEC source dormant
# until a real email is supplied via the file or env var (one-time setup
# through Settings → Set SEC Contact Email…).
SEC_USER_AGENT_PRODUCT = "TradingScanner/1.0"
SEC_CONTACT_DEFAULT = "your.email@example.com"
SEC_CONTACT_PLACEHOLDER = SEC_CONTACT_DEFAULT  # back-compat alias for older references
SEC_CONTACT_ENV_VAR = "SEC_CONTACT_EMAIL"
_SEC_CONTACT_FILENAME = "sec_contact.txt"


def _sec_contact_path() -> Path:
    """Path to the per-user SEC contact-email file. Computed lazily so
    tests can monkeypatch config.DATA_DIR."""
    return DATA_DIR / _SEC_CONTACT_FILENAME


def get_sec_contact_email() -> str:
    """Resolve the SEC EDGAR contact email.

    Priority: scanner_data/sec_contact.txt → $SEC_CONTACT_EMAIL →
    SEC_CONTACT_DEFAULT. Always returns a non-empty string."""
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
    return SEC_CONTACT_DEFAULT


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
    """True iff a usable contact email is available so the SEC EDGAR universe
    source can run. SEC_CONTACT_DEFAULT is a non-functional placeholder, so
    this stays False — and the SEC source stays dormant — until the user
    supplies a real email via scanner_data/sec_contact.txt (Settings → Set
    SEC Contact Email…) or the SEC_CONTACT_EMAIL env var."""
    email = get_sec_contact_email()
    return bool(email) and "@" in email and email != "your.email@example.com"


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
# Baked-in default — user-overridable via Settings → Advanced…
# (scanner_data/user_config.json; see the user-config section at the bottom).
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
# Always kept in OHLCV cache; used for RS calculations, never in scan results.
# Baked-in default — user-overridable via Settings → Advanced….
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
# Hard cap on history depth for all three fills (finviz / zacks / finnhub).
# Anything with a period_ending older than `today - EARNINGS_HISTORY_YEARS`
# is dropped before write. Keeps the parquet bounded and aligns the sources
# so the per-(ticker, period_ending) dedup covers the same window.
# 2026-06: raised 5 → 10 (finviz freely provides ~10y+ in raw; 5y truncated
# usable history). A one-time finviz-from-raw backfill recovers the extra
# depth without re-fetching — see earnings_history.migrate_backfill_finviz_history_from_raw.
# Baked-in default — user-overridable via Settings → Advanced….
EARNINGS_HISTORY_YEARS = 10
# Sanity bounds for reported EPS — filter reverse-split adjustment artifacts
# on heavily-reverse-split nano-caps that store nonsensical per-share values
# (observed up to ~-4e11/share). MAX_PLAUSIBLE_EPS is an ABSOLUTE cap (no
# real stock has a quarterly |EPS| this large) applied at every write where
# price isn't available. EPS_PRICE_IMPLAUSIBLE_MULT is the price-relative
# rule used by the cleanup migration (where the OHLCV close is available): a
# real stock's quarterly |EPS| is a small fraction of its share price, so
# |EPS| exceeding this multiple of the current close is an artifact (e.g. a
# $0.50 nano-cap "earning" $600/share). Generous 10x margin over the legit
# max (~10-20% of price) so no real row is ever nulled.
MAX_PLAUSIBLE_EPS = 100_000.0
EPS_PRICE_IMPLAUSIBLE_MULT = 10.0
# YoY denominator floor: skip the year-over-year % when the prior-year base
# is below this (the % off a near-zero base is dominated by the tiny base,
# not the business change — e.g. $0.0001 prior EPS → millions of %). Leave
# NaN instead of a meaningless blow-up. EPS in $/share; revenue in $millions.
MIN_YOY_EPS_BASE = 0.05
MIN_YOY_REV_BASE = 1.0
# Finnhub /calendar/earnings is fetched as a SINGLE from→to call to recover
# real announcement dates for /stock/earnings rows. Its lookback is kept
# bounded (independent of the larger history cap) so the date range can't
# blow up the calendar endpoint; finnhub history rows older than this window
# simply fall back to report_date_proxy=True (and the calendar-vs-fiscal
# collapse drops finnhub rows a finviz/zacks row already covers anyway).
FINNHUB_CALENDAR_LOOKBACK_YEARS = 5
# Finnhub is the least-effective earnings source (calendar-quartered, no
# revenue, proxy dates) and is the only one that produces calendar-vs-fiscal
# rows the dedup must collapse. Keep it OFF the automatic launch-time smart
# refresh — it's manual-only via the Finnhub bulk/gap/spot menu actions and
# the manual "Run Earnings Smart Refresh Now". finviz + zacks still auto-run.
FINNHUB_IN_AUTO_REFRESH = False

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

# -- Finviz earnings source (highest priority — adjusted/non-GAAP) --------
# Added 2026-05-31 as the top-priority per-quarter earnings source
# (finviz > zacks > finnhub). Scrapes the per-ticker earnings tab
# (`quote.ashx?t=SYM&ty=ea`) which embeds an `earningsData` JSON array;
# we take the adjusted (`epsActual`/`epsEstimate`/`salesActual`) fields,
# validated to match Zacks ~98% to the penny. No API key — HTTP scrape
# via curl_cffi Chrome TLS impersonation (same as Zacks).
#
# Finviz throttles aggressive scrapers and the `ty=ea` page is ~1.16 MB,
# so the bulk fill is paced DELIBERATELY SLOW for an overnight run that
# stays well under finviz's tolerance. Default 4.0s ± jitter ≈ ~13 req/min
# → a ~10k-ticker (ex-ETF/ADR) universe takes ~11 hours. ETFs / ADRs and
# the universe OHLCV blacklist are pre-skipped by the caller's combined
# skip set, so we never waste a request on a fund.
FINVIZ_MIN_INTERVAL_SEC = 4.0
# Random ± jitter added to each request interval so the pattern isn't a
# perfectly regular metronome (gentler on finviz's bot heuristics).
FINVIZ_JITTER_SEC = 1.0
# Consecutive real-failure threshold (429 / 5xx / network / block page —
# NOT empty/uncovered responses) before the bulk worker pauses + backs off.
FINVIZ_CONSEC_BLOCK_LIMIT = 3
# Initial block pause; doubles per subsequent block within a run, capped.
# Longer than Finnhub's because a finviz throttle takes longer to clear.
FINVIZ_INITIAL_BLOCK_PAUSE_SEC = 120
FINVIZ_MAX_BLOCK_PAUSE_SEC = 1800
FINVIZ_MAX_BLOCKS_PER_RUN = 5
# Bulk-run checkpoint for resumability across restarts.
FINVIZ_BULK_CHECKPOINT = DATA_DIR / ".finviz_bulk_checkpoint.json"
# Per-ticker side-blacklist file. Tickers finviz doesn't cover (no
# earningsData — ETFs, funds, brand-new listings) get added here.
FINVIZ_BLACKLIST_FILE = DATA_DIR / "finviz_blacklist.txt"
# Hard cap on the scraped page size to defend against a runaway response.
FINVIZ_MAX_RESPONSE_BYTES = 20 * 1024 * 1024
# Same defense for the other attacker-controllable upstreams (Imperva-fronted
# Zacks; Finnhub). A legitimate page/response is tens of KB, so a 25 MB ceiling
# never trips on real data but caps the memory + parse cost of a hostile or
# MITM'd body before it's buffered/brace-walked/JSON-parsed (audit M23).
ZACKS_MAX_RESPONSE_BYTES = 25 * 1024 * 1024
FINNHUB_MAX_RESPONSE_BYTES = 25 * 1024 * 1024

# -- Parse-failure spike alarm (scraper resilience, step B2) ---------------
# A sudden cluster of parse_error classifications across many tickers means
# the SOURCE changed its page / JSON layout (a parser break on OUR side),
# not that dozens of tickers individually went bad. The fill loops (shared
# fill_framework loop + the Zacks loop in earnings_history) track the
# parse-failure fraction of the run and HALT loudly once it spikes, instead
# of churning the rest of the universe — and the affected tickers are never
# blacklisted (a parser break must not poison the per-source blacklists).
# The alarm only arms once at least MIN_SAMPLE tickers have been attempted,
# so a couple of flaky pages at the start of a run can't false-trip it.
PARSE_SPIKE_MIN_SAMPLE = 25   # fetch attempts before the alarm may trip
PARSE_SPIKE_FAIL_PCT = 40.0   # halt when parse failures reach this % of attempts

# -- EDGAR earnings source ------------------------------------------------
# REMOVED 2026-05-31. The SEC submissions + XBRL companyfacts per-quarter
# earnings source (GAAP EPS/revenue) was dropped — GAAP figures aren't
# useful for this scanner's trading use case. The SEC ticker→CIK download
# that builds universe Source 3 lives separately under "SEC EDGAR
# (Source 4)" above (SEC_TICKERS_URL + the sec_contact helpers) and is
# unaffected. A finviz earnings source is planned to take EDGAR's slot.

# -- Zacks daily smart refresh (Phase 5 — DEPRECATED in Phase 5 of the
# Finnhub augmentation; kept as a constant so test fixtures + the
# helper function still resolve. Auto-trigger at launch removed; the
# helper is no longer wired to anything that runs automatically.)
ZACKS_AUTO_REFRESH_ENABLED = False

# -- Nasdaq calendar auto-refresh (Phase 5 of Finnhub augmentation) —
# replaces the per-launch Zacks smart-refresh as the only piece of
# automation that survives.
NASDAQ_AUTO_REFRESH_ENABLED = True
# Min days between calendar sweeps. DAILY as of 2026-06 (was weekly=7):
# the earnings smart-refresh candidate selector keys off this calendar's
# `last_earnings`, so a stale weekly calendar meant reports in the gap
# between sweeps were never detected → never fetched. A daily sweep
# (~2 min: ±90 weekday calls @ 1s) keeps `last_earnings` current so a
# freshly-reported quarter is flagged for capture the next launch.
# Manual "Refresh Now" stamps the timestamp so back-to-back manual +
# auto runs don't double up.
NASDAQ_REFRESH_DAYS = 1

# Smart-refresh staleness thresholds (source-neutral — drives the
# finviz / zacks / finnhub refresh candidate selection). The selector is
# keyed on the earnings CALENDAR's `last_earnings` (most-recent PAST
# report date, ~99% populated) vs. the most-recent report we've actually
# captured — NOT on `next_earnings`, which the reconcile clears to always
# be a future date (so it can never read as "already happened").
#
# Rule C fallback: when a ticker has no `last_earnings` in the calendar at
# all, there's no event to reason about, so re-check on a fixed cadence
# (~one quarter) since the last captured report.
EARNINGS_REFRESH_NOCAL_STALE_DAYS = 90
# Re-poll guard: a ticker fetched within this many days is NOT re-queued
# even if it still looks stale. Bounds the daily re-poll on names the
# calendar says reported but no source actually carries yet (otherwise
# Rule B would loop on them every pass). Gives sources a few days to
# publish before we try again.
EARNINGS_REFRESH_RECHECK_GUARD_DAYS = 5
# Uncaptured-fresh window: when the calendar shows a report we haven't
# captured yet AND that report is within this many days, BYPASS the re-poll
# guard so we retry every launch until the source publishes the actual
# (sources often post the EPS a day or two after the announcement, which the
# 5-day guard would otherwise make us miss for a business week). Capped at
# this window so a permanently-uncoverable name (calendar has a date no
# source carries) falls back to the guarded cadence instead of churning
# forever. ~3 weeks comfortably covers slow finviz/zacks publication plus
# weekends/holidays.
EARNINGS_REFRESH_UNCAPTURED_FRESH_DAYS = 21

# -- Market close (for the launch-time OHLCV "already current" gate) --------
# US equity regular-session close. Used to decide whether the cached OHLCV
# is current: if the last completed update ran AFTER the most recent market
# close, there's no new bar to fetch and the launch update (and the earnings
# refresh that hangs off it) is skipped.
MARKET_TZ = "America/New_York"
MARKET_CLOSE_HOUR = 16  # 4:00 PM ET


def last_market_close(now: Optional[datetime] = None) -> datetime:
    """Most recent US-equity regular-session close at or before ``now``.

    Returns a tz-aware datetime in ``MARKET_TZ``. Weekends step back to
    Friday's close. Holidays are NOT modeled — on a holiday this returns
    that day's 16:00 ET, which can only make the OHLCV gate consider the
    cache *due* one extra time (a harmless no-op refetch), never skip a
    real update.

    ``now`` may be naive (assumed already in market tz) or tz-aware (any
    zone — converted). Defaults to the current time in ``MARKET_TZ``.
    """
    tz = ZoneInfo(MARKET_TZ) if ZoneInfo is not None else None
    if now is None:
        now = datetime.now(tz) if tz is not None else datetime.now()
    elif now.tzinfo is not None and tz is not None:
        now = now.astimezone(tz)
    close = now.replace(
        hour=MARKET_CLOSE_HOUR, minute=0, second=0, microsecond=0,
    )
    if now < close:
        # Today's close hasn't happened yet — use the previous session.
        close -= timedelta(days=1)
    while close.weekday() >= 5:  # 5=Sat, 6=Sun → walk back to Friday
        close -= timedelta(days=1)
    return close

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

# ============================================================================
# User-configurable overrides (Settings → Advanced…)
# ============================================================================
# A handful of the tunables above are exposed in the GUI and persisted to
# scanner_data/user_config.json (gitignore-covered along with the rest of
# scanner_data/). load_user_config() runs once at the BOTTOM of this module —
# i.e. at import time, before any consumer module loads — and every consumer
# reads `config.<NAME>` attributes at call time, so the user's values are in
# effect from first use. Validation is strict and the fallback is always the
# baked-in default: a corrupt or hand-mangled file can never crash import, it
# just silently (debug log) reverts the bad field.
_USER_CONFIG_FILENAME = "user_config.json"

# Baked-in defaults, captured BEFORE any override is applied, so a bad or
# deleted user_config.json always has something safe to fall back to. The
# list default is stored as a tuple so nothing can mutate it in place.
_USER_CONFIG_DEFAULTS: dict = {
    "OHLCV_HISTORY_YEARS": OHLCV_HISTORY_YEARS,
    "EARNINGS_HISTORY_YEARS": EARNINGS_HISTORY_YEARS,
    "REFERENCE_TICKERS": tuple(REFERENCE_TICKERS),
}

# Clamp ranges for the integer overrides (years of history). Public — the
# Advanced settings dialog reads these for its spinbox ranges so the GUI and
# the validation here can never drift apart.
USER_CONFIG_INT_RANGES: dict = {
    "OHLCV_HISTORY_YEARS": (1, 25),
    "EARNINGS_HISTORY_YEARS": (1, 25),
}

# Plausible exchange ticker: leading letter, then letters/digits/dot/hyphen,
# 10 chars max (covers BRK.B / BF-B style class shares). \Z (not $) so a
# trailing newline can't sneak past .match(). Public — the Advanced dialog
# uses it to name the offending entry in its warning.
PLAUSIBLE_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}\Z")


def user_config_path() -> Path:
    """Path to the per-user override file. Computed lazily so tests can
    monkeypatch config.DATA_DIR (mirrors _sec_contact_path)."""
    return DATA_DIR / _USER_CONFIG_FILENAME


def _coerce_history_years(key: str, value) -> Optional[int]:
    """Validate one years-of-history override. A genuine int is clamped to
    the sane range for `key`; anything else returns None so the caller
    falls back to the baked-in default. bool is rejected explicitly — it's
    an int subclass, and `true` in the JSON would otherwise clamp to 1."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    lo, hi = USER_CONFIG_INT_RANGES[key]
    return max(lo, min(hi, value))


def _coerce_reference_tickers(value) -> Optional[list]:
    """Validate a REFERENCE_TICKERS override: a non-empty list of plausible
    ticker strings. Normalizes (strip/upper, order-preserving dedup) and
    returns the clean list — or None when ANY entry is implausible, so a
    half-broken list falls back whole rather than silently dropping
    benchmarks the RS calculations expect."""
    if not isinstance(value, list) or not value:
        return None
    out: list = []
    for item in value:
        if not isinstance(item, str):
            return None
        sym = item.strip().upper()
        if not PLAUSIBLE_TICKER_RE.match(sym):
            return None
        if sym not in out:
            out.append(sym)
    return out


def _validated_user_overrides(raw: dict) -> dict:
    """Run every known override field in `raw` through its validator.
    Invalid values and unknown keys are dropped (debug-logged) — never
    raised — so one bad field can't take down the rest."""
    import logging
    log = logging.getLogger("scanner")
    out: dict = {}
    for key in ("OHLCV_HISTORY_YEARS", "EARNINGS_HISTORY_YEARS"):
        if key in raw:
            val = _coerce_history_years(key, raw[key])
            if val is None:
                log.debug(
                    "user_config %s invalid (%r) — using default %r",
                    key, raw[key], _USER_CONFIG_DEFAULTS[key],
                )
            else:
                out[key] = val
    if "REFERENCE_TICKERS" in raw:
        val = _coerce_reference_tickers(raw["REFERENCE_TICKERS"])
        if val is None:
            log.debug(
                "user_config REFERENCE_TICKERS invalid (%r) — using default",
                raw["REFERENCE_TICKERS"],
            )
        else:
            out["REFERENCE_TICKERS"] = val
    return out


def _apply_user_overrides(overrides: dict) -> None:
    """Set the live module attributes: each known field gets its validated
    override, or its baked-in default when absent — so re-loading after the
    file is deleted/cleared restores the defaults too. The list default is
    copied so callers can never mutate the baked-in tuple."""
    g = globals()
    for key, default in _USER_CONFIG_DEFAULTS.items():
        val = overrides.get(key, default)
        g[key] = list(val) if isinstance(val, (list, tuple)) else val


def load_user_config() -> dict:
    """Load scanner_data/user_config.json and apply the valid overrides to
    the module attributes. Returns the dict of overrides actually applied.

    Missing file, unreadable file, corrupt JSON, a non-object top level,
    wrong types, out-of-range ints — every failure mode falls back to the
    baked-in defaults with only a debug log; importing config can never
    crash on a bad user_config.json."""
    import json
    import logging
    log = logging.getLogger("scanner")
    raw = None
    try:
        path = user_config_path()
        if path.exists():
            raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        # json.JSONDecodeError subclasses ValueError.
        log.debug("user_config.json unreadable — using defaults: %s", exc)
        raw = None
    if not isinstance(raw, dict):
        if raw is not None:
            log.debug(
                "user_config.json top level is %s, expected object — "
                "using defaults", type(raw).__name__,
            )
        raw = {}
    overrides = _validated_user_overrides(raw)
    _apply_user_overrides(overrides)
    return overrides


def save_user_config(values: dict) -> bool:
    """Validate `values`, persist the valid fields to user_config.json
    (atomic write), and apply them to the live module attributes so the
    change takes effect immediately — no restart needed.

    Full-state semantics: the file is REPLACED with exactly the validated
    fields, and any known field missing from `values` (or invalid) reverts
    to its baked-in default both on disk and in memory — disk and module
    state can never disagree. Returns True on success, False on a write
    error (validation problems never raise; bad fields are just dropped)."""
    import json
    overrides = _validated_user_overrides(values)
    try:
        atomic_write_text(
            user_config_path(),
            json.dumps(overrides, indent=2) + "\n",
        )
    except OSError:
        return False
    _apply_user_overrides(overrides)
    return True


# Apply any persisted user overrides NOW, at the bottom of the import, so the
# baked-in defaults above are already defined (and captured in
# _USER_CONFIG_DEFAULTS) before being overridden. Read-only — a missing
# scanner_data/ is fine (no directory side effects at import, per R7).
load_user_config()
