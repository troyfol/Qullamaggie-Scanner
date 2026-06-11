"""zacks_scraper.py — Zacks earnings scraper (HTTP-only, no browser).

Original TinyEarn © 2018-2020 Hussien Hussien, MIT licensed.
https://github.com/hussien-hussien/TinyEarn

Modified for trade_scanner_fh:
- **Dropped Selenium / Firefox / geckodriver entirely** — and also dropped
  the Playwright migration the fork spec originally called for. Live
  testing showed Zacks fronts the page with Imperva, which fingerprints
  the *TLS handshake* (JA3/JA4) and HTTP/2 frame ordering — meaning
  every browser-engine approach (headless and headful Chromium / Firefox
  / patchright / playwright-stealth) gets caught at the TLS layer before
  any browser-level evasion has a chance to matter. plain `requests` is
  also caught by the same TLS check.
- **Solved with `curl_cffi`** — a libcurl-backed `requests` drop-in that
  emits a TLS Hello byte-for-byte identical to real Chrome on Windows.
  Imperva's TLS-fingerprint check sees a real-Chrome JA3, lets the
  request through, and the page returns the embedded
  `document.obj_data = {…}` JavaScript object literal containing both
  the EPS and Sales tables already populated. We extract that blob and
  parse it directly — far simpler, faster, and more robust than
  scraping a rendered DOM.
- URL updated: `/earnings-announcements` was 301-redirected to
  `/earnings-calendar` in a Zacks restructure (sometime ≤ 2026).
- Time-of-day column normalization (Zacks now ships it as the 7th cell
  of every earnings/sales row).
- Surprise % is returned in **percent units** (5.34 = "5.34%"); the
  original TinyEarn divided by 100. This matches the beat-threshold
  semantics in TINYEARNINGS_FORK.md §7.3.
"""
from __future__ import annotations

import html as _html
import json
import logging
import math
import re
from typing import Optional

import pandas as pd
# curl_cffi.requests is API-compatible with `requests` — same Session,
# Session.get(), .text, .status_code — but uses libcurl with Chrome's TLS
# fingerprint to bypass Imperva's bot-detection at the handshake layer.
from curl_cffi import requests

from . import config

# Audit M1: substring fingerprints unique to the Imperva interstitial
# ("Pardon Our Interruption" page). Live Zacks pages always include
# `_Incapsula_Resource` as part of their bot-detection script, so those
# strings are NOT distinguishing — only the page-level "Pardon Our
# Interruption" header text and the explicit Incapsula incident error
# message identify the interstitial. We check these only AFTER
# `_extract_obj_data` returns nothing, since a real page always has
# obj_data and we never need to look further.
_INTERSTITIAL_MARKERS = (
    "Pardon Our Interruption",
    "Request unsuccessful. Incapsula incident ID",
)

# Failure-kind sentinels recorded on `ZacksSession.last_failure_kind`
# after each `fetch()` call. None = success.
FAIL_BLOCKED = "blocked"
FAIL_NOT_FOUND = "not_found"
FAIL_HTTP_ERROR = "http_error"
FAIL_PARSE_ERROR = "parse_error"

# curl_cffi browser-impersonation profile. chrome131 is widely tested and
# stable; bump to a newer profile (chrome146 etc.) only if Zacks's Imperva
# starts flagging chrome131. Available profiles change with curl_cffi
# version — see `curl_cffi.requests.impersonate.BrowserTypeLiteral`.
_IMPERSONATE_PROFILE = "chrome131"

log = logging.getLogger("scanner.zacks")

_BASE_URL = "https://www.zacks.com/stock/research/{ticker}/earnings-calendar"

# Default headers for every Zacks request. The User-Agent matters — Zacks's
# Imperva front rejects requests that don't look like a real browser. Other
# headers are present-but-not-strictly-required-yet; cheap insurance.
_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# obj_data keys → spec columns
_EPS_KEY = "earnings_announcements_earnings_table"
_REV_KEY = "earnings_announcements_sales_table"

# Cookie storage. Originally keyring-backed, but Windows Credential
# Manager has a per-secret size limit (~1.5 KB practical) that the Zacks
# cookie blob (~2 KB) trips. Falling back to a file under scanner_data/.
# Cookies are session tokens — equivalent in sensitivity to what the
# user's browser already stores on disk — not API keys, so file storage
# is appropriate.
_COOKIES_FILENAME = "zacks_cookies.txt"


def _cookies_path():
    """Compute the cookie-file path lazily so tests can monkeypatch
    config.DATA_DIR to a temp directory."""
    return config.DATA_DIR / _COOKIES_FILENAME


# At-rest encryption for the cookie file (audit: session tokens were stored
# plaintext). Wrapped with Windows DPAPI (CryptProtectData, user scope) so the
# blob is only decryptable by the same Windows user on the same machine. A
# marker prefix lets reads distinguish encrypted from legacy-plaintext files,
# so pre-existing plaintext cookies keep working and survive the upgrade. If
# DPAPI is unavailable (non-Windows / pywin32 missing) the value degrades to
# plaintext — never a hard failure.
_DPAPI_PREFIX = "DPAPI1:"


def _dpapi_protect(plain: str) -> str:
    try:
        import base64
        import win32crypt
        blob = win32crypt.CryptProtectData(
            plain.encode("utf-8"), "zacks_cookies", None, None, None, 0,
        )
        return _DPAPI_PREFIX + base64.b64encode(blob).decode("ascii")
    except Exception as exc:  # noqa: BLE001 - degrade to plaintext, never fail
        log.debug("DPAPI protect unavailable (%s) — storing cookies plaintext", exc)
        return plain


def _dpapi_unprotect(stored: str) -> Optional[str]:
    try:
        import base64
        import win32crypt
        blob = base64.b64decode(stored[len(_DPAPI_PREFIX):].encode("ascii"))
        _desc, data = win32crypt.CryptUnprotectData(blob, None, None, None, 0)
        return data.decode("utf-8")
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not decrypt stored Zacks cookies: %s", exc)
        return None

# Time-of-day mapping: Zacks free-text → normalized {Open, Close, Market, Unknown}.
# Lookup is case-insensitive after stripping; anything unrecognized → "Unknown".
_TIME_MAP = {
    "before open":         "Open",
    "after close":         "Close",
    "during market hours": "Market",
    "time not supplied":   "Unknown",
}


# ──────────────────────────────────────────────────────────────────────
# Cell parsers
# ──────────────────────────────────────────────────────────────────────

_HTML_TAG_RE = re.compile(r"<[^>]+>")


_WHITESPACE_RE = re.compile(r"\s+")


def _strip_html(s: object) -> str:
    """Strip HTML tags and HTML entities common in Zacks cells.
    Returns a plain string ('' for None / NaN).

    Audit L4: previously hand-handled only `&nbsp;` and `\\xa0`. Switched
    to `html.unescape` which covers the full entity table — `&amp;`,
    `&lt;`, numeric escapes, etc. — so a Zacks formatting change that
    introduces new entities won't silently corrupt cell values.
    """
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    txt = _HTML_TAG_RE.sub("", str(s))
    txt = _html.unescape(txt)
    txt = txt.replace("\xa0", " ")
    return txt.strip()


def _normalize_time(raw: object) -> str:
    """Map a raw Zacks time-of-day cell to {Open, Close, Market, Unknown}.

    Audit L5: collapses runs of whitespace (incl. tabs / newlines /
    nbsp post-_strip_html) into a single space so "Before  Open" and
    "Before\\u00a0Open" both normalize to "Open".
    """
    s = _strip_html(raw).lower()
    if not s:
        return "Unknown"
    s = _WHITESPACE_RE.sub(" ", s)
    return _TIME_MAP.get(s, "Unknown")


def _clean_value(s: object) -> Optional[float]:
    """Parse a Zacks numeric cell into a float. None on '--', '—', empty,
    or unparseable. Strips $, %, commas, surrounding HTML and whitespace.
    Preserves displayed magnitude (does NOT divide percent by 100)."""
    text = _strip_html(s)
    if not text or text in ("--", "—") or text.lower() == "nan":
        return None
    text = text.replace("$", "").replace("%", "").replace(",", "").replace(" ", "")
    if not text:
        return None
    try:
        v = float(text)
    except ValueError:
        return None
    # Reject inf / -inf / nan ("1e999", "inf", …) so a malformed cell can't
    # write a non-finite EPS/revenue into the parquet (audit: _clean_value
    # accepts inf). The downstream EPS-artifact guard only caps magnitude.
    if not math.isfinite(v):
        return None
    return v


def _parse_zacks_date(s: object) -> Optional[pd.Timestamp]:
    """Parse a Zacks date cell into a tz-naive Timestamp, None on failure."""
    text = _strip_html(s)
    if not text or text == "--":
        return None
    ts = pd.to_datetime(text, errors="coerce")
    if pd.isna(ts):
        return None
    try:
        if getattr(ts, "tz", None) is not None:
            ts = ts.tz_localize(None)
    except (AttributeError, TypeError):
        pass
    return ts


# ──────────────────────────────────────────────────────────────────────
# obj_data extraction (JS object literal → Python dict)
# ──────────────────────────────────────────────────────────────────────

_OBJ_DATA_START_RE = re.compile(r"document\.obj_data\s*=\s*\{")


def _extract_obj_data(html: str) -> Optional[dict]:
    """Locate the `document.obj_data = {…};` assignment in the page HTML
    and return the parsed dict. Returns None if not found / unparseable."""
    m = _OBJ_DATA_START_RE.search(html)
    if not m:
        return None

    # Walk braces with proper string-aware tracking so a `}` inside a JSON
    # string (Zacks cells contain HTML) doesn't terminate prematurely.
    start = m.end() - 1  # position of opening '{'
    depth = 0
    in_str = False
    escape = False
    end: Optional[int] = None
    n = len(html)
    for i in range(start, n):
        c = html[i]
        if escape:
            escape = False
            continue
        if in_str:
            if c == "\\":
                escape = True
            elif c == '"':
                in_str = False
            continue
        if c == '"':
            in_str = True
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end is None:
        return None

    raw = html[start:end]
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        log.debug("obj_data parse failed: %s", exc)
        return None


# B2 resilience: drift-tolerant fallback matcher. The strict extractor
# above keys on the exact `document.obj_data = {` form; a Zacks JS
# refactor (window.obj_data, var obj_data, document["obj_data"] = ...,
# "obj_data": {...} inside a bigger literal) would silently break it even
# though the payload itself is unchanged. This regex accepts any
# `obj_data`-ish token followed by an assignment-or-key separator and an
# opening brace.
_OBJ_DATA_FALLBACK_RE = re.compile(r"""obj_data['"\]\s]*[=:]\s*\{""")

# Bare token used by fetch() to tell a page-format break ("obj_data is
# there but neither parser can read it" → FAIL_PARSE_ERROR) from a real
# coverage gap (no obj_data anywhere → FAIL_NOT_FOUND).
_OBJ_DATA_TOKEN = "obj_data"


def _extract_obj_data_fallback(html: str) -> Optional[dict]:
    """Fallback for `_extract_obj_data` tolerant of assignment-form drift
    (B2 resilience). Tries every obj_data-ish assignment in the page and
    hands the opening brace to `json.JSONDecoder.raw_decode`, which scans
    a single JSON value and tolerates arbitrary trailing script text — so
    no end-detection (brace walk) is needed at all. Returns the first
    candidate that parses to a dict, else None."""
    decoder = json.JSONDecoder()
    for m in _OBJ_DATA_FALLBACK_RE.finditer(html):
        start = m.end() - 1  # position of opening '{'
        try:
            parsed, _end = decoder.raw_decode(html, start)
        except json.JSONDecodeError as exc:
            log.debug("fallback obj_data candidate at %d unparseable: %s",
                      m.start(), exc)
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


# ──────────────────────────────────────────────────────────────────────
# Row → spec-schema dict
# ──────────────────────────────────────────────────────────────────────

def _row_to_dict(row: list, *, kind: str) -> Optional[dict]:
    """Convert one obj_data table row (list of 7 cells) into a fragment of
    the §2.2 output dict. `kind` ∈ {"eps", "rev"} sets which numeric fields
    are populated. Returns None if the row is too malformed to use."""
    if not isinstance(row, list) or len(row) < 6:
        return None
    report_date = _parse_zacks_date(row[0])
    period_ending = _parse_zacks_date(row[1])
    if report_date is None and period_ending is None:
        return None

    out: dict = {
        "report_date": report_date,
        "period_ending": period_ending,
        "report_time": _normalize_time(row[6]) if len(row) >= 7 else "Unknown",
    }
    est = _clean_value(row[2])
    rep = _clean_value(row[3])
    surp = _clean_value(row[4])
    surp_pct = _clean_value(row[5])
    if kind == "eps":
        out["estimated_eps"]    = est
        out["reported_eps"]     = rep
        out["surprise_eps"]     = surp
        out["surprise_eps_pct"] = surp_pct
    else:
        out["estimated_rev"]    = est
        out["reported_rev"]     = rep
        out["surprise_rev"]     = surp
        out["surprise_rev_pct"] = surp_pct
    return out


def _merge_and_filter(
    eps_rows: list[dict], rev_rows: list[dict], cutoff: pd.Timestamp,
) -> list[dict]:
    """Merge EPS and Revenue rows on `report_date`, drop anything older
    than `cutoff`, and emit one dict per quarter newest-first with all 11
    spec fields populated (None for missing values)."""
    by_date: dict[pd.Timestamp, dict] = {}

    for r in eps_rows:
        rd = r.get("report_date")
        if rd is None or rd < cutoff:
            continue
        by_date[rd] = dict(r)

    for r in rev_rows:
        rd = r.get("report_date")
        if rd is None or rd < cutoff:
            continue
        if rd in by_date:
            for k in ("estimated_rev", "reported_rev",
                      "surprise_rev", "surprise_rev_pct"):
                if k in r:
                    by_date[rd][k] = r[k]
            if by_date[rd].get("period_ending") is None and r.get("period_ending"):
                by_date[rd]["period_ending"] = r["period_ending"]
        else:
            by_date[rd] = dict(r)

    fields = (
        "period_ending", "report_date", "report_time",
        "estimated_eps", "reported_eps", "surprise_eps", "surprise_eps_pct",
        "estimated_rev", "reported_rev", "surprise_rev", "surprise_rev_pct",
    )
    return [
        {k: by_date[rd].get(k) for k in fields}
        for rd in sorted(by_date.keys(), reverse=True)
    ]


# ──────────────────────────────────────────────────────────────────────
# Cookie injection (escape hatch for Imperva IP-flag situations)
# ──────────────────────────────────────────────────────────────────────
#
# When curl_cffi's TLS fingerprint match isn't enough — typically because
# the runtime IP has accumulated suspicion from prior attempts — Imperva
# serves the "Pardon Our Interruption" interstitial on every URL. The
# robust fix is to give the scraper cookies that Imperva already considers
# valid, harvested from a real browser session that completed the JS
# challenge. From a flagged IP those cookies still work because the
# reese84 + visid_incap pair is the trust proof.
#
# Procedure for the user:
#   1. Open zacks.com/stock/research/AAPL/earnings-calendar in normal Chrome
#   2. F12 → Application → Cookies → https://www.zacks.com → select all
#   3. Copy as cookie-header format (key=value; key=value; ...) OR paste the
#      raw "Cookie:" header value
#   4. Pass into set_zacks_cookies(...) — stored in OS credential manager.
#
# Cookies typically last hours-to-days. When they expire, scraping reverts
# to "interstitial" responses; the user re-runs the export procedure.

def get_zacks_cookies() -> Optional[str]:
    """Return the stored Zacks cookie string, or None if not configured."""
    path = _cookies_path()
    try:
        if not path.exists():
            return None
        v = path.read_text(encoding="utf-8").strip()
        if not v:
            return None
        # DPAPI-encrypted (new) → decrypt; otherwise legacy plaintext → as-is.
        if v.startswith(_DPAPI_PREFIX):
            return _dpapi_unprotect(v)
        return v
    except OSError as exc:
        log.debug("Cookie file read failed: %s", exc)
        return None


def set_zacks_cookies(cookie_string: str) -> bool:
    """Persist a `key=value; key=value; …` cookie string under
    scanner_data/. Pass an empty string to clear. Returns True on success.
    Uses an atomic temp+rename write to avoid corruption mid-update.

    Audit L3: `unlink(missing_ok=True)` lets a "clear when not present"
    call succeed quietly instead of being caught by the outer
    OSError handler and logged as a write failure.
    """
    path = _cookies_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if cookie_string and cookie_string.strip():
            config.atomic_write_text(path, _dpapi_protect(cookie_string.strip()))
        else:
            path.unlink(missing_ok=True)
        return True
    except OSError as exc:
        log.warning("Could not write Zacks cookies file: %s", exc)
        return False


def has_zacks_cookies() -> bool:
    """True iff a non-empty cookie string is stored."""
    return bool(get_zacks_cookies())


# ──────────────────────────────────────────────────────────────────────
# Persistent-profile Firefox launcher (manual close → cookie capture)
# ──────────────────────────────────────────────────────────────────────
#
# Design (May 2026 rewrite — replaces the broken two-stage auto-acquire):
#
# - One persistent Firefox profile lives at
#   `scanner_data/firefox_zacks_profile/`. It survives across runs AND
#   exe rebuilds (per the user's standing instruction that scanner_data/
#   is sacred), so any login / CAPTCHA the user solves once carries
#   through forever.
# - On profile creation we drop a `user.js` that suppresses every
#   Firefox first-run dialog. Subsequent launches see the saved state
#   and start straight on the URL.
# - When the user (or the Imperva auto-pause flow) triggers a refresh,
#   we spawn Firefox visible at the URL. Window placement on a chosen
#   monitor is opt-in via a separate Settings menu item — by default
#   Firefox lands wherever the OS puts it.
# - We do NOT poll for "JS challenge complete" or apply timeouts. The
#   user closes Firefox manually when they're done; that's our cue to
#   read cookies.sqlite and persist them.
# - Close detection uses psutil to enumerate firefox.exe processes
#   whose cmdline contains our profile path. When that count reaches
#   zero, the profile is unlocked and cookies.sqlite can be read.
# - cookies.sqlite is read via stdlib sqlite3 — Firefox does NOT
#   encrypt cookies on disk (Chrome does, but we don't need Chrome
#   here). That eliminates the browser_cookie3 dependency that was
#   silently missing from the bundled exe.

# Profile dir lives under scanner_data/. Sacred per-user data — never
# nuked by exe rebuilds.
_ZACKS_PROFILE_DIRNAME = "firefox_zacks_profile"

# Hard-coded URL the cookie-refresh launches into. AAPL/earnings-calendar
# is the canonical "warm" page for triggering Imperva's JS challenge —
# any path that returns a real Zacks page (with `document.obj_data`)
# would work; we pick AAPL because it always exists.
_ZACKS_REFRESH_URL = "https://www.zacks.com/stock/research/AAPL/earnings-calendar"

# Trimmed user.js — only suppresses the dialogs that block JS execution
# on a fresh profile. Once Firefox has run on the persistent profile a
# few times, prefs.js holds these values too, but having user.js means
# the values are reasserted every launch — defense against a Firefox
# update introducing a new nag pref the saved prefs.js wouldn't cover.
_USER_JS_CONTENT = """\
// Auto-generated by trade_scanner_fh/zacks_scraper.py — do not edit.
// First-run experience.
user_pref("browser.startup.homepage_override.mstone", "ignore");
user_pref("browser.startup.firstrunSkipsHomepage", true);
user_pref("startup.homepage_welcome_url", "");
user_pref("startup.homepage_welcome_url.additional", "");
user_pref("browser.aboutwelcome.enabled", false);
user_pref("trailhead.firstrun.didSeeAboutWelcome", true);
// Default-browser nag.
user_pref("browser.shell.checkDefaultBrowser", false);
// Telemetry consent.
user_pref("toolkit.telemetry.reportingpolicy.firstRun", false);
user_pref("datareporting.policy.dataSubmissionPolicyAcceptedVersion", 2);
user_pref("datareporting.policy.firstRunURL", "");
// Update prompts.
user_pref("app.update.enabled", false);
user_pref("app.update.auto", false);
// Tab-close confirmation when Firefox exits.
user_pref("browser.tabs.warnOnClose", false);
user_pref("browser.tabs.warnOnCloseOtherTabs", false);
user_pref("browser.warnOnQuit", false);
user_pref("browser.warnOnQuitShortcut", false);
// Sign-in-to-sync nag.
user_pref("identity.fxaccounts.toolbar.enabled", false);
// Pop-up blocker (Imperva sometimes opens iframes via window.open).
user_pref("dom.disable_open_during_load", false);
// Force a clean launch — ignore saved sessionstore + ignore homepage so
// the URL we pass on the cmdline is the ONLY page Firefox loads.
// browser.startup.page values: 0 = blank, 1 = homepage, 3 = restore tabs.
// Without this, a profile that has accumulated sessionstore data
// restores its prior tabs and treats our cmdline URL as secondary
// (or drops it entirely on some Firefox versions).
user_pref("browser.startup.page", 0);
user_pref("browser.startup.homepage", "about:blank");
user_pref("browser.sessionstore.resume_session_once", false);
user_pref("browser.sessionstore.resume_from_crash", false);
"""

# Domain we filter to. Imperva sets cookies on the apex domain
# `.zacks.com`; we also accept the bare `zacks.com` form.
_ZACKS_DOMAIN_SUFFIX = "zacks.com"

# Cookie names that signal Imperva's JS challenge completed.
_REQUIRED_IMPERVA_COOKIES = ("reese84", "visid_incap")


def get_zacks_profile_dir():
    """Resolve the persistent Firefox-profile path. Lazy lookup so tests
    can monkeypatch config.DATA_DIR."""
    return config.DATA_DIR / _ZACKS_PROFILE_DIRNAME


def ensure_zacks_profile_dir():
    """Create the profile dir + write user.js. Idempotent — safe to
    call every launch. Returns the resolved profile path."""
    profile = get_zacks_profile_dir()
    profile.mkdir(parents=True, exist_ok=True)
    try:
        (profile / "user.js").write_text(_USER_JS_CONTENT, encoding="utf-8")
    except OSError as exc:
        log.debug("Could not write user.js to %s: %s", profile, exc)
    return profile


def _has_imperva_signature(cookie_str: str) -> bool:
    """True iff the cookie string contains at least one of the cookies
    Imperva sets after a successful JS challenge. Used to reject
    profiles whose only cookies are stale `cookie_disclosure` /
    analytics entries with no actual auth tokens."""
    if not cookie_str:
        return False
    lower = cookie_str.lower()
    return any(needle in lower for needle in _REQUIRED_IMPERVA_COOKIES)


def _has_complete_imperva_tokens(cookie_str: str) -> bool:
    """Stricter check than `_has_imperva_signature` — requires BOTH
    `reese84` AND a `visid_incap_*` cookie. Used by the polling capture
    path inside `FirefoxCookieWaitWorker` to avoid grabbing cookies
    mid-challenge: Imperva sets `visid_incap` early in the JS challenge
    flow but only emits `reese84` once the challenge completes
    successfully, so requiring both is the right gate for "challenge
    finished — safe to capture."""
    if not cookie_str:
        return False
    lower = cookie_str.lower()
    return "reese84" in lower and "visid_incap" in lower


def _cookie_signature(cookie_str: str) -> str:
    """Return a stable identifier for the freshness-relevant subset of
    an Imperva cookie blob. Two cookie strings with the same signature
    represent the same Imperva session — used to detect "we read the
    same stale cookies again, browser hasn't refreshed."""
    if not cookie_str:
        return ""
    parts = []
    for kv in cookie_str.split(";"):
        kv = kv.strip()
        name, _, val = kv.partition("=")
        n_lower = name.strip().lower()
        if n_lower == "reese84" or n_lower.startswith("visid_incap"):
            parts.append(f"{n_lower}={val.strip()[:60]}")
    return "|".join(sorted(parts))


def _firefox_executable():
    """Auto-detect firefox.exe on Windows from the standard install
    locations. Returns None if not found."""
    from pathlib import Path
    candidates = [
        Path(r"C:\Program Files\Mozilla Firefox\firefox.exe"),
        Path(r"C:\Program Files (x86)\Mozilla Firefox\firefox.exe"),
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


def is_firefox_holding_profile(profile_dir) -> bool:
    """True iff any firefox.exe process is running with `-profile` =
    our profile dir. Uses psutil cmdline matching, which handles
    Firefox's launcher-fork pattern correctly (the actual browser PID
    inherits the cmdline arg even after the launcher PID exits).

    Returns False if psutil isn't available — caller should treat
    that as "can't tell, assume closed". The cookies.sqlite read uses
    `mode=ro&immutable=1` so it tolerates a still-open Firefox anyway.
    """
    try:
        import psutil
    except ImportError:
        return False
    from pathlib import Path
    try:
        target = str(Path(profile_dir).resolve()).lower()
    except OSError:
        target = str(profile_dir).lower()
    for proc in psutil.process_iter(["name", "cmdline"]):
        try:
            name = (proc.info.get("name") or "").lower()
            if name != "firefox.exe":
                continue
            cmdline = proc.info.get("cmdline") or []
            joined = " ".join(str(a).lower() for a in cmdline)
            if target in joined:
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
        except Exception:
            continue
    return False


def read_cookies_from_firefox_profile(profile_dir) -> str:
    """Read cookies for `*.zacks.com` from the profile's cookies.sqlite,
    formatted as a `name=value; name=value; …` string suitable for
    `set_zacks_cookies()`. Returns "" on any failure or if the DB has
    no matching cookies.

    Uses stdlib sqlite3 with `file:?mode=ro&immutable=1` so reads work
    even if Firefox is still running. Filters out expired cookies.
    Firefox does not encrypt cookies on disk — no decryption needed.
    """
    import sqlite3
    import time as _time
    from pathlib import Path
    db = Path(profile_dir) / "cookies.sqlite"
    if not db.exists():
        return ""
    db_uri = f"file:{db.as_posix()}?mode=ro&immutable=1"
    try:
        conn = sqlite3.connect(db_uri, uri=True, timeout=2.0)
    except sqlite3.Error as exc:
        log.debug("sqlite3.connect(%s) failed: %s", db_uri, exc)
        return ""
    try:
        # `expiry` is unix seconds; 0 means session cookie. Imperva's
        # reese84 has a real expiry, so we filter on it; session
        # cookies pass through (they may include short-lived tokens
        # that are still valid for the API call).
        now = int(_time.time())
        rows = conn.execute(
            """
            SELECT name, value, host
            FROM moz_cookies
            WHERE host LIKE '%zacks.com%'
              AND (expiry = 0 OR expiry > ?)
            """,
            (now,),
        ).fetchall()
    except sqlite3.Error as exc:
        log.debug("cookies.sqlite query failed: %s", exc)
        return ""
    finally:
        try:
            conn.close()
        except Exception:
            pass

    parts = []
    for name, value, host in rows:
        if not name or value is None:
            continue
        h = (host or "").lstrip(".")
        if not h.endswith(_ZACKS_DOMAIN_SUFFIX):
            continue
        v = str(value).strip()
        if v.startswith('"') and v.endswith('"'):
            v = v[1:-1]
        parts.append(f"{name}={v}")
    return "; ".join(parts)


# Window-enumeration helpers (used only when the caller passes geometry
# to `launch_firefox_for_zacks_cookies`). Snapshot-before-spawn ID's the
# new window via Z-order delta — Firefox's launcher fork would defeat a
# PID-based match.

def _enum_visible_windows():
    """Yield (hwnd, pid, title, class_name) for every visible top-level
    window on Windows."""
    try:
        import win32gui
        import win32process
    except ImportError:
        return
    found = []

    def _enum(hwnd, _):
        if not win32gui.IsWindowVisible(hwnd):
            return True
        try:
            title = win32gui.GetWindowText(hwnd)
            cls = win32gui.GetClassName(hwnd)
            _, wpid = win32process.GetWindowThreadProcessId(hwnd)
            found.append((hwnd, wpid, title, cls))
        except Exception:
            pass
        return True

    try:
        win32gui.EnumWindows(_enum, None)
    except Exception:
        pass
    yield from found


def _all_mozilla_hwnds() -> set:
    """Set of every visible top-level Mozilla-class hwnd on screen."""
    out = set()
    for hwnd, _pid, _title, cls in _enum_visible_windows():
        if "Mozilla" in cls or "Firefox" in cls:
            out.add(hwnd)
    return out


def _wait_for_new_mozilla_window(
    pre_hwnds, *, spawn_pid: int = 0,
    max_wait_sec: float = 8.0,
    poll_interval_sec: float = 0.4,
):
    """Poll for Mozilla-class windows that appeared AFTER `pre_hwnds`
    was snapshotted. Returns the new hwnds as a list."""
    import time as _time
    deadline = _time.time() + max_wait_sec
    while _time.time() < deadline:
        cur = _all_mozilla_hwnds()
        new_hwnds = list(cur - pre_hwnds)
        if new_hwnds:
            return new_hwnds
        _time.sleep(poll_interval_sec)
    if spawn_pid:
        for hwnd, pid, _t, cls in _enum_visible_windows():
            if pid == spawn_pid and ("Mozilla" in cls or "Firefox" in cls):
                return [hwnd]
    return []


def _move_and_maximize_windows(hwnds, *, geometry) -> int:
    """Position every hwnd at `geometry`'s top-left, then maximize
    so the window fills that monitor. Returns count of windows
    successfully manipulated. `geometry` = (x, y, w, h) in physical
    pixels of the target monitor."""
    try:
        import win32gui
        import win32con
    except ImportError:
        return 0
    n = 0
    x, y, w, h = geometry
    for hwnd in hwnds:
        try:
            # SWP_SHOWWINDOW = 0x0040. Move to the target monitor's
            # top-left corner with the monitor's full dimensions, then
            # explicitly maximize — this guarantees Firefox fills the
            # right monitor regardless of how it spawned.
            win32gui.SetWindowPos(hwnd, 0, x, y, w, h, 0x0040)
            win32gui.ShowWindow(hwnd, win32con.SW_MAXIMIZE)
            n += 1
        except Exception as exc:
            log.debug("SetWindowPos / ShowWindow failed: %s", exc)
    return n


def launch_firefox_for_zacks_cookies(
    *, geometry=None,
    url: str = _ZACKS_REFRESH_URL,
    progress_log=None,
):
    """Spawn Firefox visible on the persistent Zacks profile, pointed
    at `url` (default = the AAPL earnings calendar — a reliable
    Imperva-challenged page). If `geometry` is given as
    `(x, y, w, h)` in physical pixels of a target monitor, we move the
    new window to those coordinates and maximize it on that monitor.

    Caller is responsible for waiting on Firefox close (poll
    `is_firefox_holding_profile()`) and then calling
    `read_cookies_from_firefox_profile()` to capture the cookies.

    Returns the spawned PID, or None if launch failed.
    """
    import subprocess

    def _say(msg: str):
        log.info(msg)
        if progress_log is not None:
            try:
                progress_log(msg)
            except Exception:
                pass

    firefox = _firefox_executable()
    if firefox is None:
        _say("Firefox not found at C:\\Program Files\\Mozilla Firefox\\ "
             "(or x86 variant). Install Firefox or paste cookies "
             "manually via Data → Set Zacks Cookies...")
        return None

    profile = ensure_zacks_profile_dir()
    _say(f"Firefox: {firefox}")
    _say(f"Profile (persistent): {profile}")
    _say(f"URL: {url}")

    pre_hwnds = _all_mozilla_hwnds() if geometry is not None else set()

    # `-new-window <url>` is the explicit form: Firefox treats it as the
    # ONLY page to load, defeating both the saved homepage and any
    # session-restore data the profile carries. Passing the URL as a
    # trailing positional (the prior approach) caused some Firefox
    # versions to silently drop it when sessionstore was active.
    args = [
        str(firefox),
        "-no-remote",            # don't connect to a running personal Firefox
        "-foreground",           # bring our window to top so the URL gets focus
        "-profile", str(profile),
        "-new-window", url,
    ]
    creationflags = 0
    try:
        creationflags = (
            subprocess.CREATE_NEW_PROCESS_GROUP
            | getattr(subprocess, "DETACHED_PROCESS", 0)
        )
    except AttributeError:
        pass
    try:
        proc = subprocess.Popen(args, creationflags=creationflags)
    except OSError as exc:
        _say(f"Failed to launch Firefox: {exc}")
        return None

    _say(
        f"Launched Firefox (pid={proc.pid}). Browser is now yours — "
        "use it as needed, then close it and the scanner will capture "
        "the cookies automatically."
    )

    if geometry is not None:
        new_hwnds = _wait_for_new_mozilla_window(
            pre_hwnds, spawn_pid=proc.pid, max_wait_sec=8.0,
        )
        if new_hwnds:
            n = _move_and_maximize_windows(new_hwnds, geometry=geometry)
            if n > 0:
                _say(
                    f"Positioned + maximized Firefox on monitor "
                    f"{geometry[2]}×{geometry[3]} @ "
                    f"({geometry[0]},{geometry[1]})."
                )
            else:
                _say("WARNING: could not move Firefox window — "
                     "it may have appeared on the wrong monitor.")
        else:
            _say(
                "WARNING: no new Firefox window detected within 8s; "
                "skipping monitor placement (you can move the window "
                "manually if needed)."
            )
    else:
        _say(
            "No cookie-browser monitor preference set — Firefox is "
            "wherever the OS placed it. Use Settings → Set Cookie "
            "Browser Monitor to pin it to a specific monitor."
        )

    return proc.pid


def _parse_cookie_string(cookie_string: str) -> dict[str, str]:
    """Parse a 'key=value; key=value; ...' header value into a dict.
    Tolerant of whitespace + missing semicolons + duplicate keys (last wins)."""
    out: dict[str, str] = {}
    if not cookie_string:
        return out
    for part in cookie_string.replace("\n", ";").split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, _, v = part.partition("=")
        out[k.strip()] = v.strip()
    return out


# ──────────────────────────────────────────────────────────────────────
# HTTP session — keeps a single Connection alive across many fetches
# ──────────────────────────────────────────────────────────────────────

class ZacksSession:
    """Context manager wrapping a requests.Session for HTTP keep-alive
    across many ticker fetches.

    Use in bulk loops to share the underlying TCP connection (and any
    Imperva-set cookies) across calls:

        with ZacksSession() as session:
            for sym in tickers:
                rows = session.fetch(sym)
                ...

    For one-off lookups, the standalone `fetch_earnings_history()` builds
    and disposes its own session internally."""

    def __init__(self, *, timeout_sec: float = 20.0):
        self._timeout = float(timeout_sec)
        self._session: Optional[requests.Session] = None
        # Audit M1: classification of the last fetch() failure so the
        # bulk-fill loop can drive the auto-pause heuristic only when
        # the failure was a real Imperva block, not "ticker not on Zacks"
        # / network glitch / parse hiccup. None when last fetch succeeded.
        self.last_failure_kind: Optional[str] = None

    def __enter__(self) -> "ZacksSession":
        # impersonate=<chrome version> is the secret sauce — libcurl
        # mirrors Chrome's exact TLS Hello so Imperva's JA3 fingerprint
        # check sees a real browser and doesn't gate behind a JS
        # challenge.
        self._session = requests.Session(impersonate=_IMPERSONATE_PROFILE)
        self._session.headers.update(_DEFAULT_HEADERS)

        # Layer 2 (escape hatch): if the user has injected real-browser
        # cookies via set_zacks_cookies(), pre-populate the session jar.
        # Imperva treats requests-with-valid-reese84 as authenticated even
        # from a flagged IP, so this gets through in cases where TLS
        # impersonation alone isn't enough.
        cookie_str = get_zacks_cookies()
        if cookie_str:
            jar = _parse_cookie_string(cookie_str)
            for name, value in jar.items():
                # curl_cffi's Session.cookies.set follows requests semantics
                self._session.cookies.set(name, value, domain=".zacks.com", path="/")
            log.debug("ZacksSession started with %d injected cookies", len(jar))
        return self

    def __exit__(self, *exc) -> None:
        if self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None

    def refresh_cookies(self) -> int:
        """Reload cookies from disk into the live session jar. Used by
        the auto-pause/resume flow when the user pastes a fresh cookie
        string after Imperva starts blocking mid-run.

        Returns the number of cookies loaded (0 if cookies weren't
        configured or session isn't entered)."""
        if self._session is None:
            return 0
        cookie_str = get_zacks_cookies()
        if not cookie_str:
            return 0
        jar = _parse_cookie_string(cookie_str)
        # curl_cffi's cookies.clear() takes no args and wipes the whole
        # jar — desirable here because stale Imperva-issued cookies in
        # the live session could otherwise outvote the new ones.
        try:
            self._session.cookies.clear()
        except Exception:
            pass
        for name, value in jar.items():
            self._session.cookies.set(name, value, domain=".zacks.com", path="/")
        log.info("ZacksSession refreshed with %d cookies", len(jar))
        return len(jar)

    def fetch(self, symbol: str, years: int = 5) -> Optional[list[dict]]:
        """Scrape one ticker's earnings history. Returns a list of quarter
        dicts in newest-first order, or None on any failure (timeout, HTTP
        error, parse error, Imperva block, ticker not on Zacks). Never raises.

        Audit M1: on each call, sets `self.last_failure_kind` to a
        FAIL_* sentinel (or None on success) so the bulk-fill loop can
        distinguish Imperva blocks (count toward auto-pause) from
        coverage gaps (don't count).
        """
        if self._session is None:
            raise RuntimeError(
                "ZacksSession not entered — wrap calls in `with ZacksSession() as s:`"
            )
        url = _BASE_URL.format(ticker=symbol.upper())
        cutoff = pd.Timestamp.today().normalize() - pd.DateOffset(years=years)

        try:
            resp = self._session.get(url, timeout=self._timeout, allow_redirects=True)
        except requests.RequestException as exc:
            log.debug("[%s] HTTP error: %s", symbol, exc)
            self.last_failure_kind = FAIL_HTTP_ERROR
            return None

        if resp.status_code != 200:
            log.debug("[%s] HTTP %d", symbol, resp.status_code)
            self.last_failure_kind = FAIL_HTTP_ERROR
            return None

        text = resp.text
        # Cap the body before the char-by-char brace-walk in _extract_obj_data
        # so a hostile / MITM'd Imperva response (this source is explicitly
        # treated as attacker-controllable) can't drive a multi-hundred-MB
        # buffer + a hundreds-of-millions-iteration scan (audit M23). Real
        # Zacks pages are tens of KB; 25 MB never trips on a legit page.
        if len(text) > config.ZACKS_MAX_RESPONSE_BYTES:
            log.debug("[%s] response too large (%d chars) — rejecting",
                      symbol, len(text))
            self.last_failure_kind = FAIL_HTTP_ERROR
            return None
        obj_data = _extract_obj_data(text)
        if not obj_data:
            # Audit M1: with no obj_data, decide whether this is "ticker
            # not on Zacks" (legit empty) or "Imperva served the
            # interstitial". The Pardon-Our-Interruption / Incapsula
            # incident markers only appear on the block page; live
            # Zacks pages always have obj_data, so we never reach here
            # for them.
            if any(marker in text for marker in _INTERSTITIAL_MARKERS):
                log.debug("[%s] Imperva interstitial detected", symbol)
                self.last_failure_kind = FAIL_BLOCKED
                return None
            # B2 resilience: not a block page — before classifying, try
            # the drift-tolerant fallback extractor in case Zacks changed
            # the obj_data assignment syntax out from under the strict
            # regex / brace walk.
            obj_data = _extract_obj_data_fallback(text)
            if obj_data:
                log.warning(
                    "[%s] obj_data recovered via fallback parser — Zacks "
                    "page syntax has drifted (update _OBJ_DATA_START_RE)",
                    symbol,
                )
            elif obj_data is not None:
                # Parsed but EMPTY: the page carries a literal
                # `obj_data = {}` — perfectly readable, Zacks just has no
                # tables for this ticker. Fall through to the
                # FAIL_NOT_FOUND check below so the ticker stays
                # auto-blacklistable and never counts toward the
                # parse-spike alarm (only a genuinely-unparseable page is
                # a parser break).
                pass
            elif _OBJ_DATA_TOKEN in text:
                # obj_data exists on the page but neither parser could
                # read it → a page-format break, NOT a coverage gap.
                # Keeping these out of FAIL_NOT_FOUND matters: the GUI
                # auto-blacklists not_found tickers after each run, and
                # a parser break must never poison the skip list.
                log.warning("[%s] obj_data present but unparseable — "
                            "page-format break suspected", symbol)
                self.last_failure_kind = FAIL_PARSE_ERROR
                return None
            else:
                log.debug("[%s] obj_data not found in page (Zacks may not cover this ticker)", symbol)
                self.last_failure_kind = FAIL_NOT_FOUND
                return None

        eps_raw = obj_data.get(_EPS_KEY) or []
        rev_raw = obj_data.get(_REV_KEY) or []
        if not eps_raw and not rev_raw:
            self.last_failure_kind = FAIL_NOT_FOUND
            return None

        eps_rows = [d for r in eps_raw if (d := _row_to_dict(r, kind="eps")) is not None]
        rev_rows = [d for r in rev_raw if (d := _row_to_dict(r, kind="rev")) is not None]

        merged = _merge_and_filter(eps_rows, rev_rows, cutoff)
        log.debug("[%s] fetched %d quarters (within %d-yr window)",
                  symbol, len(merged), years)
        self.last_failure_kind = None
        return merged


def fetch_earnings_history(
    symbol: str, *, years: int = 5, timeout_sec: float = 20.0,
) -> Optional[list[dict]]:
    """Standalone one-off fetch — wraps a fresh `ZacksSession` internally.
    Use the session form for bulk loops."""
    try:
        with ZacksSession(timeout_sec=timeout_sec) as session:
            return session.fetch(symbol, years=years)
    except Exception as exc:
        log.debug("[%s] fetch_earnings_history failed: %s", symbol, exc)
        return None
