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

# Optional override: point a dev/source run at an EXISTING scanner_data tree
# (e.g. the packaged exe's data under dist/scanner_data) without rebuilding
# the exe. Set TRADE_SCANNER_FH_DATA_DIR to that folder. Every derived path
# below is `DATA_DIR / ...`, so this one override cascades to all of them.
# Unset → the normal beside-the-app location. Useful for testing source
# changes against real data; note that fills / OHLCV updates WRITE to it.
#
# The name is package-specific on purpose: a generic name (e.g.
# SCANNER_DATA_DIR) risks colliding with an unrelated variable already in the
# user's environment, which would silently redirect even the FROZEN exe's
# data directory. Keep this prefixed so only an explicit opt-in takes effect.
#
# Audit 2026-08-12 (SEC-2): the override is a DEV convenience, so it is now
# gated on `not sys.frozen`. Without that gate a user-writable environment
# variable (HKCU\Environment needs no admin) could redirect the packaged exe's
# reads AND writes — including the DPAPI-wrapped cookie write — to an
# attacker-chosen directory, or to a UNC path, turning launch into outbound SMB
# authentication. UNC and non-absolute targets are rejected outright.
_DATA_DIR_OVERRIDE = os.environ.get("TRADE_SCANNER_FH_DATA_DIR", "").strip()
_DATA_DIR_OVERRIDE_IGNORED = ""   # non-empty → reason, surfaced at startup


def _validate_data_dir_override(raw: str) -> tuple[Optional[Path], str]:
    """Return (path, "") if `raw` is an acceptable override, else (None, reason).

    Rejects UNC paths (\\\\host\\share and \\\\?\\ forms) because resolving one
    makes every launch authenticate outbound to an arbitrary host.
    """
    if not raw:
        return (None, "")
    if getattr(sys, "frozen", False):
        return (None, "ignored in a packaged build (dev-only override)")
    # Catch UNC before resolve() — resolve() happily accepts \\host\share.
    if raw.startswith("\\\\") or raw.startswith("//"):
        return (None, "UNC paths are not permitted")
    try:
        p = Path(raw).expanduser().resolve()
    except (OSError, ValueError, RuntimeError) as exc:
        return (None, f"unusable path ({exc})")
    # A resolved drive-less path on Windows, or a UNC that slipped through.
    if os.name == "nt" and (p.drive.startswith("\\\\") or not p.drive):
        return (None, "must be a local absolute path")
    return (p, "")


_override_path, _DATA_DIR_OVERRIDE_IGNORED = _validate_data_dir_override(
    _DATA_DIR_OVERRIDE
)
if _override_path is not None:
    DATA_DIR = _override_path
else:
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
#
# Audit 2026-08-12 (INT-8): FOUR separate destructive operations justify
# themselves by pointing at this layer as the recovery path —
# migrate_to_gap_fill_dedup ("Dropped rows are NOT gone forever"),
# migrate_calendar_dedup, migrate_sanitize_absurd_eps, and
# migrate_backfill_finviz_history_from_raw, which actually depends on it. A
# flat 30 days against a TEN-YEAR store does not deliver that guarantee: at
# audit time the oldest surviving file was 4 weeks old, earnings_raw/yahoo/
# was empty and finnhub/ held one file.
#
# Audit 2026-08-16 (F10): the 2026-08-12 pass raised only the SMALL sources to
# a year and left finviz + zacks — which carry essentially all of the
# per-quarter history — on 30 days. Those two are the ones F1's truncation
# guard falls back on, so the short window undercut the recovery claim exactly
# where it mattered. All sources now get a year.
RAW_RETENTION_DAYS = 365
RAW_RETENTION_DAYS_BY_SOURCE: dict = {}

# Newest N run files per source that are kept REGARDLESS of age. Age alone is
# not a recovery guarantee: after a quiet stretch longer than the retention
# window the prune empties the directory, and the layer is at its least useful
# precisely when the store has been sitting untouched. Measured 2026-08-15, the
# live layer held 3 days of finviz captures against a 215k-row store, so the
# floor is what actually makes "the raw layer is the recovery path" true.
RAW_MIN_RUNS_KEPT = 5


# v1 left every pre-existing FILE with an empty DACL — see harden_data_dir_acl.
# The bump makes the corrected routine run once more on installs that already
# ran v1, which repairs them (the `/reset` pass restores inheritance).
_ACL_SENTINEL_NAME = ".acl_hardened_v2.done"


def harden_data_dir_acl() -> tuple[bool, str]:
    """Restrict DATA_DIR to the current user + SYSTEM (Windows only).

    Audit 2026-08-12 (SEC-1): because DATA_DIR sits beside the application
    rather than under the user profile, it inherits permissive ACEs — the live
    tree granted ``NT AUTHORITY\\Authenticated Users: Modify`` on every file the
    app trusts implicitly (the parquets, universe.csv, user_config.json,
    presets, the skip lists) AND on the DPAPI-wrapped cookie blob and
    sec_contact.txt. Any local user could rewrite the scanner's inputs or read
    the contact email.

    Uses ``icacls`` to remove inherited ACEs and grant only the current user and
    SYSTEM. Runs once, gated by a sentinel, because it walks the whole tree
    (14k+ files) and re-running is pure cost. Returns ``(changed, detail)``;
    never raises — a hardening failure must not stop the app from starting.

    2026-08-13: the original single-command form left every pre-existing file
    with an empty DACL and therefore unreadable. It is now two passes — see the
    comment at the call site below — and the sentinel was bumped to v2 so an
    install that ran the broken version repairs itself on next launch.
    """
    if os.name != "nt":
        return (False, "not Windows — no ACL change needed")
    sentinel = DATA_DIR / _ACL_SENTINEL_NAME
    if sentinel.exists():
        return (False, "already hardened")
    if not DATA_DIR.exists():
        return (False, "DATA_DIR does not exist yet")
    import subprocess

    user = os.environ.get("USERNAME", "")
    if not user:
        return (False, "USERNAME not set — cannot scope the grant")
    # Two passes, and the split is load-bearing.
    #
    # v1 ran ONE command: `/inheritance:r /grant user:(OI)(CI)F ... /T`. With
    # /T, icacls applies `/inheritance:r` to every file too — but (OI) and (CI)
    # are *container* inheritance flags that yield no valid ACE on a file
    # object. So each pre-existing file had its inherited ACEs stripped and
    # received nothing in return, ending with an EMPTY DACL, which denies
    # everyone. Files written AFTER the run were fine (a new file inherits from
    # the already-correct parent), which is why the damage looked arbitrary:
    # on the live tree it hit exactly the 4 files not rewritten since —
    # sec_contact.txt and the three saved presets, which the app then reported
    # as unreadable/corrupt.
    #
    # Pass 1 hardens the directory itself (no /T) so the permissive
    # Authenticated Users ACE is gone and the (OI)(CI) grants become the
    # inheritable source of truth. Pass 2 resets the children, which drops
    # their explicit ACEs and re-enables inheritance, so every file picks up
    # user + SYSTEM from the parent. Pass 2 also REPAIRS anything v1 broke.
    argv_base = ["icacls", str(DATA_DIR)]
    try:
        # No shell=True — fixed argv lists.
        proc = subprocess.run(
            argv_base + [
                "/inheritance:r",
                "/grant", f"{user}:(OI)(CI)F",
                "/grant", "SYSTEM:(OI)(CI)F",
                "/Q",
            ],
            capture_output=True, text=True, timeout=300,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return (False, f"icacls failed: {exc}")
    if proc.returncode != 0:
        return (False, f"icacls returned {proc.returncode}: "
                       f"{(proc.stderr or proc.stdout or '').strip()[:200]}")
    try:
        # /C keeps going past individual failures (a file locked by a running
        # instance); /T walks the existing tree.
        proc = subprocess.run(
            argv_base + ["/reset", "/T", "/C", "/Q"],
            capture_output=True, text=True, timeout=300,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return (False, f"icacls child reset failed: {exc}")
    if proc.returncode != 0:
        return (False, f"icacls /reset returned {proc.returncode}: "
                       f"{(proc.stderr or proc.stdout or '').strip()[:200]}")
    try:
        atomic_write_text(sentinel, "ok\n")
    except OSError:
        pass   # hardening succeeded; only the sentinel write failed
    return (True, f"restricted to {user} + SYSTEM")


# -- Log retention (audit 2026-08-12, INT-14) -----------------------------
# Logging used a plain FileHandler with no RotatingFileHandler anywhere in the
# package and no reaper: the live logs/ dir held 493 files / 77 MB across 123
# sessions, many of them 0-byte bridge_*.log from sessions that never used the
# TradeStation bridge. Rotation caps one runaway session; the age prune caps
# the population.
LOG_MAX_BYTES = 5 * 1024 * 1024
LOG_BACKUP_COUNT = 2
LOG_RETENTION_DAYS = 30


def prune_old_logs(retention_days: Optional[int] = None) -> int:
    """Delete log files older than ``retention_days``. Returns count removed.

    Called from ``ensure_dirs`` so it runs once per launch. Never raises — a
    log-housekeeping failure must not stop the app from starting.
    """
    days = retention_days if retention_days is not None else LOG_RETENTION_DAYS
    if not LOG_DIR.exists():
        return 0
    cutoff = (datetime.now() - timedelta(days=days)).timestamp()
    removed = 0
    try:
        for f in LOG_DIR.glob("*.log*"):
            try:
                if f.is_file() and f.stat().st_mtime < cutoff:
                    f.unlink()
                    removed += 1
            except OSError:
                continue      # in use by this or another session
    except OSError:
        return removed
    return removed


def ensure_dirs() -> None:
    """Create the standard scanner_data/ subdirectories.

    Called from entry points (GUI main(), fill workers, tests) rather than at
    import time so merely importing config has no filesystem side effects.
    """
    for d in (DATA_DIR, PARQUET_DIR, LOG_DIR, FTP_RAW_DIR, RAW_EARNINGS_DIR):
        d.mkdir(parents=True, exist_ok=True)
    for src in RAW_SOURCES:
        (RAW_EARNINGS_DIR / src).mkdir(parents=True, exist_ok=True)
    # Audit 2026-08-12 (INT-14): bound the log population. Cheap (an mtime
    # walk) and self-suppressing once the directory is inside the window.
    prune_old_logs()
    # Audit 2026-08-16 (F8): reap atomic-write temps orphaned by a hard kill.
    prune_stale_temp_files()


# -- Orphaned atomic-write temp files (audit 2026-08-16, F8) ---------------
# The three atomic writers unlink their temp on an EXCEPTION, but a hard kill
# leaves it behind and nothing ever swept for them. Observed on the live tree:
# `.earnings_dates.parquet.3696.<uuid>.tmp`, 57,731 bytes, orphaned two days
# and LARGER than the 55,138-byte file it was going to become. This will keep
# happening because the documented rebuild procedure requires closing the
# running exe — i.e. killing a process that may be mid-write.
#
# The pattern is deliberately exact (`.<name>.<pid>.<32-hex-uuid>.tmp`, see
# _unique_tmp_path) so this can only ever match a temp WE created — never a
# user file, and never anything inside firefox_zacks_profile/.
_TEMP_FILE_RE = re.compile(r"^\..+\.\d+\.[0-9a-f]{32}\.tmp$")

# Grace period before a temp is considered orphaned. Comfortably longer than
# any single write, so a temp belonging to a concurrently-running writer (or
# to a second instance) is never removed out from under it.
TEMP_FILE_MAX_AGE_HOURS = 24


def prune_stale_temp_files(max_age_hours: Optional[float] = None) -> int:
    """Delete orphaned atomic-write temp files under DATA_DIR. Returns the
    count removed.

    Called from ``ensure_dirs`` so it runs once per launch. Never raises — a
    housekeeping failure must not stop the app from starting.
    """
    hours = (max_age_hours if max_age_hours is not None
             else TEMP_FILE_MAX_AGE_HOURS)
    if not DATA_DIR.exists():
        return 0
    cutoff = (datetime.now() - timedelta(hours=hours)).timestamp()
    removed = 0
    try:
        for f in DATA_DIR.rglob(".*.tmp"):
            try:
                if not _TEMP_FILE_RE.match(f.name):
                    continue
                if f.is_file() and f.stat().st_mtime < cutoff:
                    f.unlink()
                    removed += 1
            except OSError:
                continue      # in use, or vanished under us
    except OSError:
        return removed
    return removed


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


def _flush_to_disk(fh) -> None:
    """Force an open file's bytes out of the OS cache before the rename.

    Audit 2026-08-16 (F7). All three atomic writers were write-temp-then-rename
    with NO fsync, and their docstrings claimed "a crash mid-write cannot
    corrupt the target file". That holds for a PROCESS crash — the OS still
    owns the written pages and flushes them — but not for a power loss or a
    BSOD, where NTFS can make the rename durable before the data pages. The
    result is a zero-length or partially-populated file sitting under the REAL
    filename, which is strictly worse than the pre-atomic behaviour: the `.tmp`
    name no longer marks it as suspect.

    Directory fsync is deliberately not attempted — it isn't available on
    Windows, and the file-level flush is the part that closes this window.
    """
    try:
        fh.flush()
        os.fsync(fh.fileno())
    except (OSError, ValueError, AttributeError):
        # A filesystem that can't fsync (some network shares) must not turn a
        # successful write into a failed one — we're strictly better off than
        # before either way.
        pass


def atomic_write_text(path: Path, content: str, encoding: str = "utf-8") -> None:
    """Write text via a temp file + os.replace so a crash mid-write cannot
    corrupt the target file.

    Audit 2026-08-12 (INT-15 / SEC-11): this used a PREDICTABLE sibling name
    (``<name>.tmp``) and left residue when ``replace()`` failed, unlike its
    parquet/CSV siblings. It is the writer for every JSON/txt store — the skip
    lists, the migration sentinels, user_config.json, presets, schedules,
    sec_contact.txt and the DPAPI-wrapped zacks_cookies.txt — so a predictable
    temp name in a user-writable directory was both a corruption risk (two
    writers interleaving into one temp, then promoting a half-written file) and
    a plant/symlink target. Now uses the same unique name + cleanup contract as
    ``atomic_write_parquet``.
    """
    tmp = _unique_tmp_path(path)
    tmp.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(tmp, "w", encoding=encoding, newline="") as fh:
            fh.write(content)
            _flush_to_disk(fh)          # F7 — durable before the rename
        tmp.replace(path)
    except BaseException:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def atomic_write_parquet(df, path: Path, **kwargs) -> None:
    """Write a DataFrame to parquet via a unique temp file + os.replace.

    Readers always see either the old complete file or the new complete file,
    never a torn write — even when two threads write the same target at once
    (the temp name is per-writer). The temp is removed if the write fails so a
    failed write leaves no residue, and the bytes are fsync'd before the rename
    so a power loss can't promote an empty file (F7)."""
    tmp = _unique_tmp_path(path)
    tmp.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(tmp, "wb") as fh:
            df.to_parquet(fh, **kwargs)
            _flush_to_disk(fh)
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
        # newline="" so pandas owns the line terminator — writing through a
        # text handle in text mode would otherwise translate "\n" to "\r\n"
        # again on Windows and produce blank lines between rows.
        with open(tmp, "w", encoding=kwargs.pop("encoding", "utf-8"),
                  newline="") as fh:
            df.to_csv(fh, **kwargs)
            _flush_to_disk(fh)
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


# -- NASDAQ symbol directory (Source 1) -------------------------------------
NASDAQ_FTP_HOST = "ftp.nasdaqtrader.com"
NASDAQ_FTP_DIR = "SymbolDirectory"
NASDAQ_FTP_FILES = ["nasdaqtraded.txt", "nasdaqlisted.txt", "otherlisted.txt"]

# Audit 2026-08-12 (SEC-8): prefer HTTPS. The FTP fetch is ANONYMOUS AND
# PLAINTEXT with no integrity check, and it is the integrity root for SEC-3 —
# a MITM on this feed controls the ticker universe, which flows into both URL
# builders and into filesystem paths. NASDAQ serves the identical files over
# TLS from the same origin (verified: same pipe-delimited headers, 13,137 /
# 5,590 / 7,549 lines). FTP remains as a fallback so a corporate proxy that
# blocks this host can't break the universe refresh outright.
NASDAQ_HTTPS_BASE = "https://www.nasdaqtrader.com/dynamic/SymDir"
NASDAQ_PREFER_HTTPS = True
# Same response-size discipline the other remote fetches use. The largest real
# file (nasdaqtraded.txt) is ~1 MB, so 25 MB never trips on real data but caps
# the memory + parse cost of a hostile or MITM'd body.
NASDAQ_MAX_RESPONSE_BYTES = 25 * 1024 * 1024

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

# Warm the load_ohlcv LRU cache across the universe at launch
# (data_engine.prefetch_ohlcv) so the first scan isn't dominated by cold
# parquet reads (~85% of a cold scan is parquet I/O). Baked-in default —
# user-overridable via scanner_data/user_config.json (same mechanism as
# OHLCV_HISTORY_YEARS above).
PREFETCH_OHLCV_AT_LAUNCH = False

# -- Parquet schema versioning (Phase 4 R18) -------------------------------
# Bump when the per-ticker OHLCV parquet column set or dtypes change in a way
# that breaks forward/backward load compatibility. On mismatch, the scanner
# logs a warning and (in future versions) may refuse to merge old + new data.
PARQUET_SCHEMA_VERSION = 1
PARQUET_SCHEMA_FILE = PARQUET_DIR / "_schema_version.txt"

# -- Data Validation -------------------------------------------------------
PRICE_JUMP_PCT = 50.0            # flag if single-day % change exceeds this
MAX_MISSING_DAYS_FLAG = 5        # flag if > N trading days missing in a row

# Tickers whose cached price series is NOT continuous across a split ex-date:
# close[ex] / close[ex-1] equals the split ratio itself, so the series carries a
# raw discontinuity where a back-adjusted series should be smooth.
#
# This is a defect AT THE SOURCE, not in the cache. A completely fresh
# yfinance pull reproduces the seam byte-for-byte (verified on BESS, PRKA, SILO
# and LAAI), and EDGAR independently confirms the split is real and of the
# claimed magnitude on APRE (x19.98 vs x20 expected), SILO (x52.7 vs x50),
# SIGY (x41 vs x40), HLTC (x4.0 vs x4) and AREB (x39,999.9 vs x40,000). The
# direction is inverted: a reverse split RAISES price, and these series fall by
# the ratio. Re-anchoring in download_one is working correctly and has nothing
# to fix; recency is ruled out too (median 345 bars after the ex-date, none
# within 5, so the refetch-overlap window is not the mechanism).
#
# DO NOT REBUILD THESE TICKERS — it cannot help, exactly as with the four
# already-known bad-at-source names. They are excluded at SCAN time only, so
# downloads keep refreshing them and the exclusion reverses itself for free if
# the upstream ever fixes the series.
#
# Deliberately NOT folded into blacklist.txt: that file is hand-maintained, and
# it suppresses DOWNLOADS rather than scan rows.
SPLIT_SEAM_SKIP_FILE = DATA_DIR / "split_seam_skip.txt"

# Precomputed {ticker -> most recent qualifying split ex-date}, written once per
# launch by data_engine.rebuild_split_artifacts().
#
# Exists purely so the scan path never has to widen its parquet read. EFF-5
# projects `Stock Splits` away because carrying it measured 10.4 ms -> 1.9 ms
# per file (~155 s -> ~28 s across the universe); reading split dates at scan
# time would hand that entire saving back on every scan. ~2.7k rows — only
# tickers that actually have a split appear.
SPLIT_ANCHORS_PARQUET = DATA_DIR / "split_anchors.parquet"

# How far close[ex]/close[ex-1] may sit from the split ratio (or from 1.0)
# before the seam test calls the result ambiguous rather than deciding. A
# genuine reverse split of any consequence puts the two hypotheses orders of
# magnitude apart, so this is generous on purpose.
SPLIT_SEAM_TOL = 1.5

# Relative tolerance (% of the bar's own price level) before an OHLC bound
# violation — High below Open/Low/Close, or Low above Open/Close — is treated
# as real rather than provider rounding. The upstream's split/dividend-adjusted
# prices are rounded, which routinely leaves Close a few 1e-6 above High.
# A full-store sweep (2026-08-13, 11,854 parquets) found 98.7% of raw breaches
# sat under 0.01% of the bar's price level while every genuine artifact
# exceeded 1%, so 0.1% separates the two populations with room on both sides.
OHLC_INVARIANT_TOL_PCT = 0.1

# Calendar days of ALREADY-CACHED history an incremental OHLCV update
# re-requests alongside the genuinely new bars. The provider's most recent
# daily bar stays provisional for hours after the close: a 2026-08-13 sample of
# 60 tickers against the bars captured by the 2026-08-12 17:21-19:10 refill
# found Volume wrong on 60/60 (median 1.2% short, p90 22.6%, worst 99%) and
# High/Low wrong on ~half. Re-fetching the tail lets the finalized figures
# replace the provisional ones. 0 still re-requests the last cached day itself
# (the one that matters most); there is no setting that restores the old
# never-look-back behaviour, which was the bug.
OHLCV_REFETCH_OVERLAP_DAYS = 5

# How far BELOW the cached value a re-sent bar's Volume may fall before the
# bar is rejected in favour of the cache (audit 2026-08-16, F5).
#
# The overlap above exists so FINAL bars replace PROVISIONAL ones, but the
# guard that decides whether a re-sent bar may overwrite a cached one compared
# `Close` and nothing else — while the field actually measured wrong was
# Volume (60/60 sampled tickers, median 1.2% short, p90 22.6%), with High and
# Low wrong on roughly half. A bar whose Close matches to the cent but whose
# Volume is a fraction of the true figure is exactly the provisional signature,
# and it sailed through.
#
# Direction is what makes this safe: settled daily volume is revised UP as late
# prints clear, essentially never down. So a HIGHER incoming Volume is the
# correction we want and is always adopted; a materially LOWER one means the
# incoming bar is the less-final of the two and the whole bar is rejected —
# which protects that bar's High/Low too, since a provisional bar is wrong in
# all three fields at once. 10% sits above the p90 provisional shortfall while
# staying far below any plausible legitimate downward revision.
#
# Split re-adjustments also rescale Volume, but those take the full-history
# re-download path (last_date is reset to None), so they never reach this guard.
OHLCV_VOLUME_REGRESSION_PCT = 10.0

# -- OHLCV interior gap detection (audit 2026-08-16, F2) -------------------
# Staleness was judged ONLY by a file's last bar, so a ticker whose most recent
# bar is current was never re-fetched no matter what its interior looked like —
# and `download_one`'s incremental window starts at `last_date - overlap`, so
# even when it did run it could only repair the last few days.
#
# Measured 2026-08-15 against SPY's own session index: 261 of 581 sampled
# tickers (44.9%) were missing sessions strictly INSIDE their own date range,
# and all 261 had a current last bar. The missing dates were not scattered —
# essentially every affected ticker was missing the same three sessions
# (2026-07-21, 2026-07-22, 2026-07-31), which is one upstream partial response
# written as complete and then cemented.
OHLCV_GAP_CHECK_ENABLED = True
# The session calendar is taken from a ticker already in the cache rather than
# a holiday table: `_NYSE_HOLIDAYS` only starts at 2024 while OHLCV history can
# reach 25 years back, so a calendar-derived expectation would report ~10 false
# gaps per pre-2024 year. SPY is a REFERENCE_TICKER, so it is always cached and
# always refreshed.
OHLCV_GAP_REFERENCE_TICKER = "SPY"
# Ceiling on how many gapped tickers ONE run repairs. Unbounded, the first run
# after this ships would queue ~5,300 full re-pulls (45% of 11,854) at yfinance
# pacing — many hours, and not what a launch should silently start. Mirrors
# SECTOR_STALE_MAX_PER_RUN: worst-first, amortised across sessions.
OHLCV_GAP_MAX_REBUILDS_PER_RUN = 200
# A ticker whose gaps SURVIVE a full rebuild has a source-level hole (a real
# trading halt, or bars the provider genuinely lacks). Re-attempting it every
# launch would churn forever, so an attempt is recorded and not repeated inside
# this window. Same shape as SKIP_RECHECK_DAYS.
OHLCV_GAP_RECHECK_DAYS = 90
OHLCV_GAP_ATTEMPTS_FILE = ".ohlcv_gap_attempts.json"

# -- OHLCV anomaly report (audit 2026-08-16, F12) --------------------------
# `validate_ticker` is the only OHLCV integrity check in the project — negative
# and zero prices, OHLC bound violations, duplicate dates, price jumps, date
# gaps — and its output was logged at INFO and discarded. Nothing reached the
# GUI, nothing was persisted, nothing could be trended. Written beside
# earnings_disagreements.csv, which is the same pattern already working.
# Name only; resolved against DATA_DIR at call time so test fixtures redirect it.
OHLCV_ANOMALIES_CSV_NAME = "ohlcv_anomalies.csv"

# -- Earnings quarter-gap detection (audit 2026-08-16, F6) -----------------
# `verify_integrity` had 14 checks and none asked whether a ticker's quarters
# are CONTIGUOUS, so the F1 truncation could run for months without leaving a
# visible trace. Restricted to a recent window because legitimate gaps (IPOs,
# companies going dark and returning, fiscal-year changes, and the 25-year cap
# leaving one very old quarter followed by nothing) dominate the deep history:
# 2,700 gap events across 1,262 of 4,858 tickers store-wide, largely benign.
EARNINGS_GAP_CHECK_YEARS = 3

# -- Universe staleness (days) ---------------------------------------------
UNIVERSE_STALE_DAYS = 7
# Refuse to write a universe.csv that shrank by more than this percentage
# against the previous one (audit 2026-08-16, F3).
#
# universe.csv feeds EVERY scan and every fill's work list, and validation
# failures are destructive — a symbol that fails is dropped from the file. The
# codebase already applies this kind of sanity cap to a third-party dataset
# (FINANCEDATABASE_MAX_ROWS refuses an absurd response); the universe, which
# matters considerably more, had none. Real churn between refreshes is a few
# percent — delistings and new listings — so 25% is far above any legitimate
# move while still catching a throttled validation pass.
UNIVERSE_MAX_SHRINK_PCT = 25.0

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
# Per-source date observations, keyed (ticker, source) — audit 2026-08-16, F4.
# `earnings_dates.parquet` holds one row per ticker, so nasdaq / yahoo /
# finviz's forward date all wrote into the SAME row and whichever fill ran last
# won. The reconciler could only read that collapsed row back, so its documented
# priority chain never applied to the date-backed sources. This store keeps each
# source's observation separate; the reconciler still writes the one-row-per-
# ticker consumer file above, so no reader changes.
#
# Deliberately a FUNCTION, not a module constant. F17 in this same audit found
# that 7 of the 13 import-time DATA_DIR-derived constants weren't redirected by
# the test fixtures, so tests wrote into the user's live tree — and adding this
# one as a constant reproduced that within the hour. Resolving at call time
# means patching DATA_DIR alone is sufficient, which every fixture already does.
EARNINGS_SOURCE_FILENAME = "earnings_dates_by_source.parquet"


def earnings_source_parquet() -> Path:
    """Path to the per-source earnings-date store (resolved at call time)."""
    return DATA_DIR / EARNINGS_SOURCE_FILENAME
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
# Upper sanity bound on a YoY percentage. Audit 2026-08-12 (INT-16): the floors
# above bound the DIVISOR but not the resulting ratio, so a base that clears the
# floor by a hair still yields five-figure percentages (the live store held 28
# rows beyond ±10,000%, topping out at 120,240%). A quarter that genuinely grew
# 50x year-over-year is +5,000%, so 10,000% is far above any real move while
# still catching the artifacts. Used by verify_integrity check #13 and by
# compute_yoy_columns, which nulls anything beyond it at computation time.
YOY_SANITY_MAX_PCT = 10_000.0
# Rolling automatic backups of earnings_history.parquet, taken before each
# CANONICAL (sorted) save. Audit 2026-08-12 (INT-6): the project had no
# automatic backup at all, so any write-path defect was unrecoverable. Three
# snapshots of an ~8 MB file costs ~25 MB and covers the realistic window
# (notice a problem within a few fills).
HISTORY_BACKUP_COUNT = 3
# Minimum age of the newest snapshot before another rotation is allowed.
#
# Audit 2026-08-16 (F9): the depth above is spaced by WRITE, not by time, and
# canonical saves are far more frequent than "a few fills" assumed — the
# launch-time smart refresh finalizes three sources, each of the four launch
# migrations that finds work saves canonically, and (before this pass) every
# single-ticker spot fill did too. One launch could therefore consume the whole
# backup depth, leaving three snapshots of the same afternoon. Measured
# 2026-08-15: autobak1/2/3 were 11.94 / 11.92 / 11.98 MB, all from one session.
#
# Skipping a rotation is safe by construction: it leaves the EXISTING snapshot
# in place, and an older snapshot is strictly more useful for recovery than a
# fresh one taken after the damage. 12h keeps roughly one per launch-day, so
# the three slots span three days instead of three writes.
HISTORY_BACKUP_MIN_INTERVAL_HOURS = 12
# Cross-source EPS disagreement flagging (report-only diagnostics).
# When two sources both carry a row for the same (ticker, period_ending)
# slot, dedupe_history silently keeps the priority winner
# (finviz > zacks > finnhub). earnings_history.find_cross_source_disagreements
# additionally FLAGS the slots where the sources materially disagree so the
# silent resolution stays visible. A pair is flagged when both rows have a
# non-null reported_eps differing by MORE than EPS_DISAGREEMENT_ABS_TOL
# dollars, OR both have a non-null surprise_eps_pct differing by MORE than
# SURPRISE_DISAGREEMENT_PP_TOL percentage points (strict >; ties pass). The
# report is written to scanner_data/<EARNINGS_DISAGREEMENTS_CSV_NAME> on
# every canonical (deduping) history save. Purely diagnostic — changing
# these tolerances never changes which row the dedup keeps.
EPS_DISAGREEMENT_ABS_TOL = 0.10        # dollars of reported_eps
SURPRISE_DISAGREEMENT_PP_TOL = 2.0     # percentage points of surprise_eps_pct
# Name only — resolved against DATA_DIR at call time (like the migration
# flags) so test fixtures that monkeypatch DATA_DIR redirect the report too.
EARNINGS_DISAGREEMENTS_CSV_NAME = "earnings_disagreements.csv"
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

# Zacks bulk-run checkpoint (audit 2026-08-16, S1). The zacks fill was the one
# source with NO resume: a killed bulk restarted from zero, and at 1.5 s pacing
# a universe run is ~6.5 hours. finviz and finnhub have had this since the
# fill_framework extraction; zacks was left behind with the rest of its loop.
ZACKS_BULK_CHECKPOINT = DATA_DIR / ".zacks_bulk_checkpoint.json"

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

# -- Hotkey target-window guard (audit 2026-08-12, SEC-10) ----------------
# `pyautogui` types BLIND: whatever holds focus at t+delay_ms receives the
# ticker and the confirm key. The existing mitigations are strong (no global
# OS hotkey — the cue is an in-app Qt event filter; waits for physical
# button/modifier release; lowercase typing so a capital can't fire a Trade
# Bar order hotkey; off-screen refusal; confirm-key allowlist), but nothing
# checks WHICH window is about to be typed into.
#
# Each entry is a case-insensitive substring matched against the foreground
# window's title AND class name; a send proceeds if ANY matches. EMPTY = the
# guard is off, which is the default because the correct value depends on the
# user's target application (TradeStation, TitanX, …) and a wrong hint would
# silently break a working trading workflow. The destination window is logged
# on every send regardless, which is what lets a user fill this in accurately.
#
# The guard FAILS OPEN: if the check itself cannot run (no pywin32, no
# foreground handle) the send proceeds, matching the existing off-screen
# behaviour. Only a positive mismatch against a configured hint aborts.
HOTKEY_TARGET_WINDOW_HINTS: list = []

# Sanity ceiling on the financedatabase equities dataset (audit SEC-8). The
# real dataset is ~150k rows; anything far beyond that is a library change or
# a bad/hostile upstream response, not data worth iterating over.
FINANCEDATABASE_MAX_ROWS = 1_000_000

# -- Sector map staleness (audit 2026-08-12, INT-13) ----------------------
# Nothing ever read sector_map's `updated_at`, and targeted_fill_sectors only
# filled GAPS — so once a ticker had a sector row it was never revisited and a
# GICS reclassification never propagated. The live map was last written 99 days
# before the audit. Rows older than this are re-offered to the targeted fill.
SECTOR_STALE_DAYS = 180
# Ceiling on how many stale rows one targeted fill refreshes, so the sweep
# stays amortised across sessions instead of turning one menu click into a
# 15k-ticker yfinance run.
SECTOR_STALE_MAX_PER_RUN = 500

# -- Fill flush cadence (audit 2026-08-12, EFF-7) -------------------------
# How many successfully-pulled tickers accumulate before a fill rewrites
# earnings_history.parquet. Each flush is a full read of ~148k rows → mask →
# concat → sanitize → guard → column reorder → category coercion → atomic
# write, so at the old cadence of 25 a 15k-ticker fill did ~600 of them.
#
# The reason to raise it is not speed — the deliberate network pacing dwarfs
# the I/O (finviz is 4.0 s/ticker, so a universe fill is ~17 h regardless) —
# it is INT-1: every one of those cycles was an opportunity for the truncation
# bug to fire. Fewer, larger flushes shrink that exposure window ~8x.
#
# The checkpoint makes progress durable, so the cost of a hard kill is bounded
# by this many tickers of re-fetching rather than by the whole run.
FILL_FLUSH_EVERY = 200

# -- Scan compute parallelism (audit 2026-08-12, EFF-1) -------------------
# The per-ticker compute loop was strictly single-threaded over the whole
# universe, once per timeframe. Parquet reads and most numpy/pandas kernels
# release the GIL, so a small pool is the largest scan-latency lever here.
# 6 mirrors the OHLCV downloader's width. Set to 1 to force the serial path
# (useful when diffing scan output against an older build).
SCAN_MAX_WORKERS = 6

# -- Skip-list re-validation (audit 2026-08-12, INT-5) --------------------
# The per-source skip lists were a one-way ratchet: a single "empty" response
# — which for finviz includes a bare HTTP 404, and for finnhub an empty
# earnings array — excluded a ticker permanently. That correctly captures an
# ETF or a warrant, but it also captures a brand-new IPO with no earnings yet,
# a ticker mid-rename, and anything 404-ing during a site migration. The lists
# had reached 10,246 / 10,048 / 6,394 entries against a 15,948-symbol universe
# and could only grow.
#
# Entries now carry an ADDED_ON date, so an entry auto-added for an "empty"
# response can be re-offered to its source after this many days. Manual entries
# and any other reason code are never re-checked — the user curated those.
SKIP_RECHECK_DAYS = 90
# Ceiling on one re-check batch. Re-including 10k tickers would queue an ~11h
# finviz fill from a single menu click; a bounded batch keeps the follow-up
# gap fill something the user can reason about, and the action can be repeated.
SKIP_RECHECK_MAX = 500
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
    "PREFETCH_OHLCV_AT_LAUNCH": PREFETCH_OHLCV_AT_LAUNCH,
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

# Audit 2026-08-12 (SEC-3): the ALLOWLIST applied to any symbol that reaches a
# URL path or a filesystem path. Symbols arrive from NASDAQ FTP (plaintext,
# anonymous — the realistic MITM vector), a third-party GitHub mirror and SEC
# EDGAR, and two builders interpolate them straight into the URL path/query,
# where requests' safe-parameter encoding does not apply:
#     https://www.zacks.com/stock/research/{ticker}/earnings-calendar
#     https://finviz.com/quote.ashx?t={sym}&ty=ea
# The only prior gate was a DENYLIST (`[+=%#@!]`), which blocks `AAPL?x=1` and
# `AAPL#frag` but NOT `AAPL/../../admin` — verified reaching the finviz URL.
#
# DELIBERATELY WIDER than PLAUSIBLE_TICKER_RE above, which is for the
# REFERENCE_TICKERS benchmark list. Reusing that stricter pattern here would
# silently drop 405 legitimate symbols currently in universe.csv — preferred
# shares and rights like ABR$D, AIIA^, AXIA$C — turning a security fix into a
# data-integrity bug. Validated against the live 15,948-symbol file: this
# pattern rejects zero real symbols (longest live symbol is 7 chars).
URL_SAFE_TICKER_RE = re.compile(r"^[A-Z0-9][A-Z0-9.\-$^]{0,11}\Z")


def url_safe_ticker(symbol) -> Optional[str]:
    """Normalise ``symbol`` and return it only if it is safe to interpolate
    into a URL path. Returns None for anything else — callers must treat that
    as "skip this ticker", never as "pass it through unvalidated"."""
    if not isinstance(symbol, str):
        return None
    sym = symbol.strip().upper()
    return sym if URL_SAFE_TICKER_RE.match(sym) else None


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


def _coerce_bool_flag(value) -> Optional[bool]:
    """Validate a boolean override. Only a genuine JSON true/false passes;
    anything else — including 0/1 ints and "true" strings — returns None
    so the caller falls back to the baked-in default (mirrors the strict
    bool rejection in _coerce_history_years, inverted)."""
    if isinstance(value, bool):
        return value
    return None


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
    if "PREFETCH_OHLCV_AT_LAUNCH" in raw:
        val = _coerce_bool_flag(raw["PREFETCH_OHLCV_AT_LAUNCH"])
        if val is None:
            log.debug(
                "user_config PREFETCH_OHLCV_AT_LAUNCH invalid (%r) — "
                "using default %r",
                raw["PREFETCH_OHLCV_AT_LAUNCH"],
                _USER_CONFIG_DEFAULTS["PREFETCH_OHLCV_AT_LAUNCH"],
            )
        else:
            out["PREFETCH_OHLCV_AT_LAUNCH"] = val
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
