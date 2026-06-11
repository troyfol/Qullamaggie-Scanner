"""Tests for the manual cookie-refresh flow (May 2026 rewrite).

Covers:
  - `_has_imperva_signature` / `_cookie_signature` semantics
  - `ensure_zacks_profile_dir` writes user.js with the prefs that
    suppress Firefox first-run dialogs
  - `read_cookies_from_firefox_profile` reads cookies.sqlite via
    stdlib sqlite3 (no browser_cookie3 dep)
  - `is_firefox_holding_profile` matches firefox.exe processes by
    cmdline (handles the launcher-fork pattern)
  - `launch_firefox_for_zacks_cookies` spawns Firefox with
    -no-remote / -foreground / -profile / URL
  - The Imperva-fill bulk-threshold guard in main_window
  - The MainWindow cookie-browser monitor preference (set / clear)
  - The MainWindow.act_zacks_open_cookie_browser launches the new
    flow with the saved geometry
"""
from __future__ import annotations

import sqlite3
import time
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from trade_scanner_fh import zacks_scraper as zs


@pytest.fixture
def tmp_data_dir(tmp_path, monkeypatch):
    """Redirect config.DATA_DIR to a tmp path. The persistent profile
    + cookies file land under here per scanner_data convention."""
    from trade_scanner_fh import config as cfg
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)
    return tmp_path


# ──────────────────────────────────────────────────────────────────────
# _has_imperva_signature / _cookie_signature
# ──────────────────────────────────────────────────────────────────────

def test_imperva_signature_requires_real_token():
    assert zs._has_imperva_signature("reese84=foo")
    assert zs._has_imperva_signature("visid_incap_xxx=bar")
    assert not zs._has_imperva_signature("cookie_disclosure=Y")
    assert not zs._has_imperva_signature("")


def test_complete_imperva_tokens_requires_both_cookies():
    """The mid-flight capture path's gate: visid_incap is set early in
    the JS challenge, reese84 only lands when the challenge completes.
    Capturing on visid_incap alone would grab cookies mid-flight."""
    # Both present (in either order, with arbitrary visid_incap suffix).
    assert zs._has_complete_imperva_tokens(
        "reese84=A; visid_incap_2944342=B")
    assert zs._has_complete_imperva_tokens(
        "visid_incap_999=Y; foo=bar; reese84=Z")
    # Only one present — challenge in flight, NOT safe to capture.
    assert not zs._has_complete_imperva_tokens("reese84=A")
    assert not zs._has_complete_imperva_tokens("visid_incap_1=B")
    # Neither — empty / stale-only profile.
    assert not zs._has_complete_imperva_tokens("")
    assert not zs._has_complete_imperva_tokens(
        "cookie_disclosure=Y; nr_data=foo")


def test_cookie_signature_changes_when_reese84_changes():
    a = "reese84=AAAAA; visid_incap_1=foo"
    b = "reese84=BBBBB; visid_incap_1=foo"
    assert zs._cookie_signature(a) != zs._cookie_signature(b)


def test_cookie_signature_ignores_irrelevant_keys():
    a = "reese84=X; visid_incap_1=Y"
    b = "reese84=X; visid_incap_1=Y; nr_data=junk; cookie_disclosure=Y"
    assert zs._cookie_signature(a) == zs._cookie_signature(b)


def test_cookie_signature_empty_for_blank():
    assert zs._cookie_signature("") == ""


# ──────────────────────────────────────────────────────────────────────
# ensure_zacks_profile_dir + user.js content
# ──────────────────────────────────────────────────────────────────────

def test_profile_dir_lives_under_data_dir(tmp_data_dir):
    expected = tmp_data_dir / "firefox_zacks_profile"
    assert zs.get_zacks_profile_dir() == expected


def test_ensure_profile_creates_dir_and_writes_user_js(tmp_data_dir):
    profile = zs.ensure_zacks_profile_dir()
    assert profile.exists()
    assert profile.is_dir()
    user_js = profile / "user.js"
    assert user_js.exists()
    content = user_js.read_text(encoding="utf-8")
    # Each of these dialogs would block JS on a fresh profile if the
    # corresponding pref were missing — these are what makes the
    # persistent profile usable on first launch.
    must_have = [
        ("browser.aboutwelcome.enabled", "false"),
        ("browser.shell.checkDefaultBrowser", "false"),
        ("datareporting.policy.dataSubmissionPolicyAcceptedVersion", "2"),
        ("app.update.enabled", "false"),
        ("browser.tabs.warnOnClose", "false"),
        ("identity.fxaccounts.toolbar.enabled", "false"),
        # Issue #3 fix: Firefox must launch CLEAN — ignore the saved
        # sessionstore data and the configured homepage so the cmdline
        # URL is the only page the browser loads. Without these, a
        # profile with prior tabs would restore them and silently drop
        # our AAPL earnings-calendar URL.
        ("browser.startup.page", "0"),
        ("browser.startup.homepage", '"about:blank"'),
        ("browser.sessionstore.resume_session_once", "false"),
        ("browser.sessionstore.resume_from_crash", "false"),
    ]
    for pref, val in must_have:
        line = f'user_pref("{pref}", {val});'
        assert line in content, f"missing critical pref: {line}"


def test_ensure_profile_is_idempotent(tmp_data_dir):
    p1 = zs.ensure_zacks_profile_dir()
    p2 = zs.ensure_zacks_profile_dir()
    assert p1 == p2
    assert (p1 / "user.js").exists()


# ──────────────────────────────────────────────────────────────────────
# read_cookies_from_firefox_profile (stdlib sqlite3)
# ──────────────────────────────────────────────────────────────────────

def _make_cookies_sqlite(profile_dir: Path, rows: list[tuple]):
    """Build a cookies.sqlite that mirrors Firefox's moz_cookies schema
    just enough for the read function to work. Each row is
    (name, value, host, expiry)."""
    profile_dir.mkdir(parents=True, exist_ok=True)
    db = profile_dir / "cookies.sqlite"
    if db.exists():
        db.unlink()
    conn = sqlite3.connect(str(db))
    conn.execute(
        """
        CREATE TABLE moz_cookies (
            id INTEGER PRIMARY KEY,
            originAttributes TEXT NOT NULL DEFAULT '',
            name TEXT,
            value TEXT,
            host TEXT,
            path TEXT,
            expiry INTEGER,
            lastAccessed INTEGER,
            creationTime INTEGER,
            isSecure INTEGER,
            isHttpOnly INTEGER,
            inBrowserElement INTEGER DEFAULT 0,
            sameSite INTEGER DEFAULT 0,
            rawSameSite INTEGER DEFAULT 0,
            schemeMap INTEGER DEFAULT 0
        )
        """
    )
    for (name, value, host, expiry) in rows:
        conn.execute(
            """
            INSERT INTO moz_cookies
              (name, value, host, path, expiry, lastAccessed,
               creationTime, isSecure, isHttpOnly)
            VALUES (?, ?, ?, '/', ?, 0, 0, 0, 0)
            """,
            (name, value, host, expiry),
        )
    conn.commit()
    conn.close()
    return db


def test_read_cookies_returns_zacks_subset(tmp_data_dir):
    profile = tmp_data_dir / "firefox_zacks_profile"
    future = int(time.time()) + 3600
    _make_cookies_sqlite(profile, [
        ("reese84", "abc123", ".zacks.com", future),
        ("visid_incap_2944342", "xyz", ".zacks.com", future),
        ("foreign", "x", ".other.com", future),
    ])
    cookie_str = zs.read_cookies_from_firefox_profile(profile)
    assert "reese84=abc123" in cookie_str
    assert "visid_incap_2944342=xyz" in cookie_str
    assert "foreign" not in cookie_str
    assert "other.com" not in cookie_str


def test_read_cookies_filters_expired(tmp_data_dir):
    profile = tmp_data_dir / "firefox_zacks_profile"
    past = int(time.time()) - 3600
    future = int(time.time()) + 3600
    _make_cookies_sqlite(profile, [
        ("reese84", "fresh", ".zacks.com", future),
        ("expired_token", "stale", ".zacks.com", past),
    ])
    cookie_str = zs.read_cookies_from_firefox_profile(profile)
    assert "reese84=fresh" in cookie_str
    assert "expired_token" not in cookie_str


def test_read_cookies_keeps_session_cookies(tmp_data_dir):
    """Firefox stores session cookies with expiry=0; we keep them
    since Imperva sometimes hands out short-lived tokens that way."""
    profile = tmp_data_dir / "firefox_zacks_profile"
    future = int(time.time()) + 3600
    _make_cookies_sqlite(profile, [
        ("reese84", "abc", ".zacks.com", future),
        ("session_token", "live", ".zacks.com", 0),
    ])
    cookie_str = zs.read_cookies_from_firefox_profile(profile)
    assert "session_token=live" in cookie_str


def test_read_cookies_returns_empty_when_db_missing(tmp_data_dir):
    profile = tmp_data_dir / "firefox_zacks_profile"
    # No cookies.sqlite created.
    profile.mkdir(parents=True, exist_ok=True)
    assert zs.read_cookies_from_firefox_profile(profile) == ""


def test_read_cookies_strips_quote_wrapping(tmp_data_dir):
    profile = tmp_data_dir / "firefox_zacks_profile"
    future = int(time.time()) + 3600
    _make_cookies_sqlite(profile, [
        ("reese84", '"quoted-value"', ".zacks.com", future),
    ])
    cookie_str = zs.read_cookies_from_firefox_profile(profile)
    assert cookie_str == "reese84=quoted-value"


# ──────────────────────────────────────────────────────────────────────
# is_firefox_holding_profile (psutil cmdline match)
# ──────────────────────────────────────────────────────────────────────

def test_is_firefox_holding_profile_matches_by_cmdline(tmp_data_dir):
    """The cmdline matcher must catch a firefox.exe whose -profile arg
    points at our dir, regardless of process tree topology."""
    profile = tmp_data_dir / "firefox_zacks_profile"
    profile.mkdir(parents=True, exist_ok=True)
    target = str(profile.resolve()).lower()

    fake_proc = MagicMock()
    fake_proc.info = {
        "name": "firefox.exe",
        "cmdline": ["firefox.exe", "-no-remote", "-profile", target,
                    "https://www.zacks.com/"],
    }
    other_proc = MagicMock()
    other_proc.info = {
        "name": "firefox.exe",
        "cmdline": ["firefox.exe", "-profile", "C:\\Other\\Profile"],
    }
    chrome_proc = MagicMock()
    chrome_proc.info = {
        "name": "chrome.exe",
        "cmdline": ["chrome.exe", "--user-data-dir", target],
    }

    with patch("psutil.process_iter",
               return_value=[fake_proc, other_proc, chrome_proc]):
        assert zs.is_firefox_holding_profile(profile) is True


def test_is_firefox_holding_profile_false_when_none_match(tmp_data_dir):
    profile = tmp_data_dir / "firefox_zacks_profile"
    profile.mkdir(parents=True, exist_ok=True)
    other_proc = MagicMock()
    other_proc.info = {"name": "firefox.exe",
                       "cmdline": ["firefox.exe", "-profile", "C:\\Other"]}
    with patch("psutil.process_iter", return_value=[other_proc]):
        assert zs.is_firefox_holding_profile(profile) is False


def test_is_firefox_holding_profile_false_when_psutil_missing(
    tmp_data_dir, monkeypatch,
):
    """Caller treats False from psutil-import-failure as 'assume
    closed' — sqlite read tolerates a still-open Firefox via
    immutable=1 anyway."""
    import sys
    monkeypatch.setitem(sys.modules, "psutil", None)
    with patch("builtins.__import__", side_effect=ImportError):
        assert zs.is_firefox_holding_profile(tmp_data_dir) is False


def test_is_firefox_holding_profile_handles_psutil_errors(tmp_data_dir):
    """A NoSuchProcess / AccessDenied during enumeration must not
    crash the check — those processes are simply skipped."""
    import psutil
    profile = tmp_data_dir / "firefox_zacks_profile"
    profile.mkdir(parents=True, exist_ok=True)

    bad_proc = MagicMock()
    bad_proc.info = {"name": "firefox.exe", "cmdline": []}
    type(bad_proc).info = property(
        lambda self: (_ for _ in ()).throw(psutil.AccessDenied()),
    )

    with patch("psutil.process_iter", return_value=[bad_proc]):
        # Should return False (no matches), NOT raise.
        assert zs.is_firefox_holding_profile(profile) is False


# ──────────────────────────────────────────────────────────────────────
# launch_firefox_for_zacks_cookies (subprocess + flags)
# ──────────────────────────────────────────────────────────────────────

def test_launch_returns_none_when_firefox_not_installed(
    tmp_data_dir, monkeypatch,
):
    monkeypatch.setattr(zs, "_firefox_executable", lambda: None)
    msgs = []
    pid = zs.launch_firefox_for_zacks_cookies(progress_log=msgs.append)
    assert pid is None
    assert any("Firefox not found" in m for m in msgs)


def test_launch_passes_no_remote_foreground_profile_url(
    tmp_data_dir, monkeypatch,
):
    captured = {}

    def fake_popen(args, **kwargs):
        captured["args"] = list(args)
        m = MagicMock()
        m.pid = 12345
        return m

    monkeypatch.setattr(
        zs, "_firefox_executable",
        lambda: Path(r"C:\fake\firefox.exe"),
    )
    monkeypatch.setattr("subprocess.Popen", fake_popen)
    monkeypatch.setattr(zs, "_all_mozilla_hwnds", lambda: set())
    monkeypatch.setattr(zs, "_wait_for_new_mozilla_window",
                        lambda *a, **kw: [])

    pid = zs.launch_firefox_for_zacks_cookies()
    assert pid == 12345
    args = captured["args"]
    assert "-no-remote" in args
    assert "-foreground" in args
    # Profile arg points at the persistent dir under DATA_DIR
    profile_idx = args.index("-profile")
    assert args[profile_idx + 1].endswith("firefox_zacks_profile")
    # `-new-window <url>` must be present and pair with the URL.
    # Without `-new-window`, Firefox treats a trailing positional URL
    # as secondary to any saved sessionstore tabs and may drop it.
    nw_idx = args.index("-new-window")
    assert args[nw_idx + 1] == zs._ZACKS_REFRESH_URL
    # URL is the canonical AAPL earnings page
    assert args[-1] == zs._ZACKS_REFRESH_URL


def test_launch_uses_custom_url_when_supplied(tmp_data_dir, monkeypatch):
    captured = {}

    def fake_popen(args, **kwargs):
        captured["args"] = list(args)
        m = MagicMock()
        m.pid = 1
        return m

    monkeypatch.setattr(zs, "_firefox_executable",
                        lambda: Path(r"C:\fake\firefox.exe"))
    monkeypatch.setattr("subprocess.Popen", fake_popen)
    monkeypatch.setattr(zs, "_all_mozilla_hwnds", lambda: set())
    monkeypatch.setattr(zs, "_wait_for_new_mozilla_window",
                        lambda *a, **kw: [])

    zs.launch_firefox_for_zacks_cookies(url="https://example.com/probe")
    assert captured["args"][-1] == "https://example.com/probe"


def test_launch_seeds_user_js_before_popen(tmp_data_dir, monkeypatch):
    """The audit fix: user.js must exist on disk BEFORE Firefox
    launches, so first-run dialogs are suppressed."""
    captured = {}

    def fake_popen(args, **kwargs):
        i = args.index("-profile")
        profile = Path(args[i + 1])
        captured["user_js_present_at_spawn"] = (profile / "user.js").exists()
        m = MagicMock()
        m.pid = 1
        return m

    monkeypatch.setattr(zs, "_firefox_executable",
                        lambda: Path(r"C:\fake\firefox.exe"))
    monkeypatch.setattr("subprocess.Popen", fake_popen)
    monkeypatch.setattr(zs, "_all_mozilla_hwnds", lambda: set())
    monkeypatch.setattr(zs, "_wait_for_new_mozilla_window",
                        lambda *a, **kw: [])

    zs.launch_firefox_for_zacks_cookies()
    assert captured["user_js_present_at_spawn"] is True


def test_launch_applies_geometry_when_supplied(tmp_data_dir, monkeypatch):
    """Geometry triggers the snapshot-before-spawn + window-move flow."""
    captured = {}

    def fake_popen(args, **kwargs):
        m = MagicMock()
        m.pid = 1
        return m

    def fake_move(hwnds, *, geometry):
        captured["hwnds"] = list(hwnds)
        captured["geometry"] = geometry
        return len(hwnds)

    monkeypatch.setattr(zs, "_firefox_executable",
                        lambda: Path(r"C:\fake\firefox.exe"))
    monkeypatch.setattr("subprocess.Popen", fake_popen)
    monkeypatch.setattr(zs, "_all_mozilla_hwnds", lambda: set())
    monkeypatch.setattr(zs, "_wait_for_new_mozilla_window",
                        lambda *a, **kw: [42])
    monkeypatch.setattr(zs, "_move_and_maximize_windows", fake_move)

    geom = (1920, 0, 2560, 1440)
    zs.launch_firefox_for_zacks_cookies(geometry=geom)
    assert captured["hwnds"] == [42]
    assert captured["geometry"] == geom


def test_launch_no_geometry_skips_window_move(tmp_data_dir, monkeypatch):
    """Without a saved monitor preference, we skip window enumeration
    entirely (no SetWindowPos calls)."""
    enum_called = []

    def fake_popen(args, **kwargs):
        m = MagicMock()
        m.pid = 1
        return m

    monkeypatch.setattr(zs, "_firefox_executable",
                        lambda: Path(r"C:\fake\firefox.exe"))
    monkeypatch.setattr("subprocess.Popen", fake_popen)
    monkeypatch.setattr(
        zs, "_all_mozilla_hwnds",
        lambda: enum_called.append("yes") or set(),
    )
    monkeypatch.setattr(zs, "_wait_for_new_mozilla_window",
                        lambda *a, **kw: pytest.fail(
                            "wait_for_new_mozilla_window must not run "
                            "without geometry",
                        ))
    zs.launch_firefox_for_zacks_cookies()
    # enum is allowed to be called once for the snapshot, but with
    # no geometry we skip the window-wait that would inevitably
    # follow. Assert via the fake-fail above.


# ──────────────────────────────────────────────────────────────────────
# MainWindow cookie-browser monitor preference (Settings menu)
# ──────────────────────────────────────────────────────────────────────

def test_set_cookie_monitor_persists_geom(_qapp, tmp_data_dir):
    from trade_scanner_fh.gui.main_window import MainWindow

    parent = MainWindow.__new__(MainWindow)
    parent._cookie_monitor_geom = None
    parent.log_panel = MagicMock()
    parent._resolve_current_monitor_geometry = lambda: (100, 200, 1920, 1080)
    saved = {}
    parent._save_cookie_monitor_pref = (
        lambda g: saved.update({"geom": g})
    )

    import types
    bound = types.MethodType(
        MainWindow._set_cookie_browser_monitor_action, parent,
    )
    bound()
    assert parent._cookie_monitor_geom == (100, 200, 1920, 1080)
    assert saved["geom"] == (100, 200, 1920, 1080)


def test_clear_cookie_monitor_wipes_geom(_qapp, tmp_data_dir):
    from trade_scanner_fh.gui.main_window import MainWindow

    parent = MainWindow.__new__(MainWindow)
    parent._cookie_monitor_geom = (1, 2, 3, 4)
    parent.log_panel = MagicMock()
    saved = {"geom": "untouched"}
    parent._save_cookie_monitor_pref = (
        lambda g: saved.update({"geom": g})
    )

    import types
    bound = types.MethodType(
        MainWindow._clear_cookie_browser_monitor_action, parent,
    )
    bound()
    assert parent._cookie_monitor_geom is None
    assert saved["geom"] is None


def test_set_cookie_monitor_warns_when_geometry_unresolvable(
    _qapp, tmp_data_dir,
):
    from PyQt6.QtWidgets import QMessageBox
    from trade_scanner_fh.gui.main_window import MainWindow

    parent = MainWindow.__new__(MainWindow)
    parent._cookie_monitor_geom = None
    parent.log_panel = MagicMock()
    parent._resolve_current_monitor_geometry = lambda: None
    parent._save_cookie_monitor_pref = MagicMock()

    with patch.object(QMessageBox, "warning") as warn:
        import types
        bound = types.MethodType(
            MainWindow._set_cookie_browser_monitor_action, parent,
        )
        bound()
    warn.assert_called_once()
    assert parent._cookie_monitor_geom is None


# ──────────────────────────────────────────────────────────────────────
# MainWindow refresh-cookies action launches new flow + worker
# ──────────────────────────────────────────────────────────────────────

def test_refresh_action_launches_firefox_with_saved_geometry(
    _qapp, tmp_data_dir,
):
    from trade_scanner_fh.gui.main_window import MainWindow

    parent = MainWindow.__new__(MainWindow)
    parent._cookie_monitor_geom = (50, 60, 1280, 720)
    parent._cookie_wait_worker = None
    parent._zacks_worker_awaiting_resume = None
    parent.log_panel = MagicMock()
    parent.act_zacks_open_cookie_browser = MagicMock()

    captured = {}

    def fake_launch(**kwargs):
        captured.update(kwargs)
        return 1234

    captured_worker = {}

    class FakeWorker:
        def __init__(self, profile_dir, *, pre_signature=""):
            captured_worker["profile_dir"] = profile_dir
            captured_worker["pre_sig"] = pre_signature
            self.log_msg = MagicMock()
            self.finished = MagicMock()
        def isRunning(self):
            return False
        def start(self):
            captured_worker["started"] = True

    with patch(
        "trade_scanner_fh.zacks_scraper.launch_firefox_for_zacks_cookies",
        side_effect=fake_launch,
    ), patch(
        "trade_scanner_fh.gui.workers.FirefoxCookieWaitWorker",
        FakeWorker,
    ):
        import types
        bound = types.MethodType(
            MainWindow._refresh_zacks_cookies_action, parent,
        )
        bound()

    assert captured.get("geometry") == (50, 60, 1280, 720)
    assert callable(captured.get("progress_log"))
    assert captured_worker.get("started") is True
    parent.act_zacks_open_cookie_browser.setEnabled.assert_called_with(False)


def test_refresh_action_blocks_double_launch(_qapp, tmp_data_dir):
    """A second click while a wait-worker is already running must
    show an info message instead of spawning another Firefox."""
    from PyQt6.QtWidgets import QMessageBox
    from trade_scanner_fh.gui.main_window import MainWindow

    parent = MainWindow.__new__(MainWindow)
    parent._cookie_monitor_geom = None
    parent.log_panel = MagicMock()
    fake_worker = MagicMock()
    fake_worker.isRunning.return_value = True
    parent._cookie_wait_worker = fake_worker

    with patch.object(QMessageBox, "information") as info, patch(
        "trade_scanner_fh.zacks_scraper."
        "launch_firefox_for_zacks_cookies",
        side_effect=AssertionError(
            "must not relaunch while a wait is in flight",
        ),
    ):
        import types
        bound = types.MethodType(
            MainWindow._refresh_zacks_cookies_action, parent,
        )
        bound()
    info.assert_called_once()


# ──────────────────────────────────────────────────────────────────────
# Bulk-fill threshold guard in main_window (preserved from old suite)
# ──────────────────────────────────────────────────────────────────────


def test_bulk_threshold_prompt_yes_kicks_off_targeted_worker(
    _qapp, tmp_data_dir, monkeypatch,
):
    from PyQt6.QtWidgets import QMessageBox
    from trade_scanner_fh import config as cfg
    from trade_scanner_fh.gui.main_window import MainWindow

    monkeypatch.setattr(cfg, "ZACKS_SMART_REFRESH_BULK_THRESHOLD", 10)
    bulk_candidates = [f"T{i:04d}" for i in range(50)]
    monkeypatch.setattr(
        "trade_scanner_fh.earnings_history.find_smart_refresh_candidates",
        lambda u, b: bulk_candidates,
    )

    parent = MainWindow.__new__(MainWindow)
    parent._zacks_worker = None
    parent._universe_df = pd.DataFrame({"symbol": bulk_candidates})
    parent._symbols = bulk_candidates
    parent._blacklist = set()
    parent._zacks_blacklist = set()
    parent._zacks_auto_refresh_ref = [True]
    parent.log_panel = MagicMock()
    parent.act_zacks_auto_refresh = MagicMock()

    spawned = {}

    class FakeWorker:
        def __init__(self, syms, blk, mode):
            spawned["syms"] = syms
            spawned["mode"] = mode
            self.log_msg = MagicMock()
            self.progress = MagicMock()
            self.finished = MagicMock()
            self.failure_breakdown = MagicMock()
            self.imperva_block_detected = MagicMock()
        def isRunning(self):
            return False
        def start(self):
            spawned["started"] = True

    with patch(
        "trade_scanner_fh.gui.main_window.ZacksFillWorker",
        FakeWorker,
    ), patch.object(
        QMessageBox, "question",
        return_value=QMessageBox.StandardButton.Yes,
    ):
        import types
        bound = types.MethodType(
            MainWindow._kick_off_zacks_smart_refresh, parent,
        )
        bound()

    assert spawned.get("started") is True
    assert spawned["mode"] == "targeted"
    assert spawned["syms"] == bulk_candidates


def test_bulk_threshold_prompt_no_skips_run(
    _qapp, tmp_data_dir, monkeypatch,
):
    from PyQt6.QtWidgets import QMessageBox
    from trade_scanner_fh import config as cfg
    from trade_scanner_fh.gui.main_window import MainWindow

    monkeypatch.setattr(cfg, "ZACKS_SMART_REFRESH_BULK_THRESHOLD", 10)
    bulk_candidates = [f"T{i:04d}" for i in range(50)]
    monkeypatch.setattr(
        "trade_scanner_fh.earnings_history.find_smart_refresh_candidates",
        lambda u, b: bulk_candidates,
    )

    parent = MainWindow.__new__(MainWindow)
    parent._zacks_worker = None
    parent._universe_df = pd.DataFrame({"symbol": bulk_candidates})
    parent._symbols = bulk_candidates
    parent._blacklist = set()
    parent._zacks_blacklist = set()
    parent._zacks_auto_refresh_ref = [True]
    parent.log_panel = MagicMock()
    parent.act_zacks_auto_refresh = MagicMock()

    spawned = {}

    class FakeWorker:
        def __init__(self, *a, **kw):
            spawned["called"] = True
        def isRunning(self):
            return False
        def start(self):
            spawned["started"] = True

    with patch(
        "trade_scanner_fh.gui.main_window.ZacksFillWorker",
        FakeWorker,
    ), patch.object(
        QMessageBox, "question",
        return_value=QMessageBox.StandardButton.No,
    ):
        import types
        bound = types.MethodType(
            MainWindow._kick_off_zacks_smart_refresh, parent,
        )
        bound()

    assert "started" not in spawned
    assert parent._zacks_auto_refresh_ref[0] is True


def test_bulk_threshold_prompt_cancel_disables_toggle(
    _qapp, tmp_data_dir, monkeypatch,
):
    from PyQt6.QtWidgets import QMessageBox
    from trade_scanner_fh import config as cfg
    from trade_scanner_fh.gui.main_window import MainWindow

    monkeypatch.setattr(cfg, "ZACKS_SMART_REFRESH_BULK_THRESHOLD", 10)
    bulk_candidates = [f"T{i:04d}" for i in range(50)]
    monkeypatch.setattr(
        "trade_scanner_fh.earnings_history.find_smart_refresh_candidates",
        lambda u, b: bulk_candidates,
    )

    parent = MainWindow.__new__(MainWindow)
    parent._zacks_worker = None
    parent._universe_df = pd.DataFrame({"symbol": bulk_candidates})
    parent._symbols = bulk_candidates
    parent._blacklist = set()
    parent._zacks_blacklist = set()
    parent._zacks_auto_refresh_ref = [True]
    parent.log_panel = MagicMock()
    fake_action = MagicMock()
    parent.act_zacks_auto_refresh = fake_action

    with patch.object(
        QMessageBox, "question",
        return_value=QMessageBox.StandardButton.Cancel,
    ):
        import types
        bound = types.MethodType(
            MainWindow._kick_off_zacks_smart_refresh, parent,
        )
        bound()

    assert parent._zacks_auto_refresh_ref[0] is False
    fake_action.setChecked.assert_called_with(False)


def test_under_threshold_skips_prompt_and_starts_directly(
    _qapp, tmp_data_dir, monkeypatch,
):
    from PyQt6.QtWidgets import QMessageBox
    from trade_scanner_fh import config as cfg
    from trade_scanner_fh.gui.main_window import MainWindow

    monkeypatch.setattr(cfg, "ZACKS_SMART_REFRESH_BULK_THRESHOLD", 100)
    small = ["AAPL", "MSFT", "PLUG"]
    monkeypatch.setattr(
        "trade_scanner_fh.earnings_history.find_smart_refresh_candidates",
        lambda u, b: small,
    )

    parent = MainWindow.__new__(MainWindow)
    parent._zacks_worker = None
    parent._universe_df = pd.DataFrame({"symbol": ["AAPL", "MSFT", "PLUG"]})
    parent._symbols = ["AAPL", "MSFT", "PLUG"]
    parent._blacklist = set()
    parent._zacks_blacklist = set()
    parent._zacks_auto_refresh_ref = [True]
    parent.log_panel = MagicMock()

    spawned = {}

    class FakeWorker:
        def __init__(self, syms, blk, mode):
            spawned["syms"] = syms
            spawned["mode"] = mode
            self.log_msg = MagicMock()
            self.progress = MagicMock()
            self.finished = MagicMock()
            self.failure_breakdown = MagicMock()
            self.imperva_block_detected = MagicMock()
        def isRunning(self):
            return False
        def start(self):
            spawned["started"] = True

    with patch(
        "trade_scanner_fh.gui.main_window.ZacksFillWorker",
        FakeWorker,
    ), patch.object(
        QMessageBox, "question",
        side_effect=AssertionError("prompt should not appear"),
    ):
        import types
        bound = types.MethodType(
            MainWindow._kick_off_zacks_smart_refresh, parent,
        )
        bound()

    assert spawned.get("started") is True
    assert spawned["mode"] == "targeted"
    assert spawned["syms"] == small


# ──────────────────────────────────────────────────────────────────────
# FirefoxCookieWaitWorker — mid-flight capture (Recommendation A)
# ──────────────────────────────────────────────────────────────────────
#
# These tests drive the worker's run() method synchronously. Because
# QThread.run() is just a regular method when called directly (no
# event loop, no second OS thread), signal emissions still fire and
# can be observed via signal.connect(...) — but they're delivered
# in-line on the calling thread, so we can collect them in a list
# without any event-loop pumping.

def test_cookie_wait_worker_captures_mid_flight_when_tokens_land(
    tmp_data_dir, monkeypatch,
):
    """When BOTH reese84 and visid_incap appear in cookies.sqlite while
    Firefox is still running AND the signature differs from
    pre-launch, the worker captures and exits without waiting for the
    user to close Firefox."""
    from trade_scanner_fh.gui.workers import FirefoxCookieWaitWorker

    profile = tmp_data_dir / "firefox_zacks_profile"
    future = int(time.time()) + 3600
    _make_cookies_sqlite(profile, [
        ("reese84", "fresh-token-abc", ".zacks.com", future),
        ("visid_incap_2944342", "xyz", ".zacks.com", future),
    ])

    # Firefox stays "open" for the duration of the test — close
    # detection must NOT be the path that fires.
    monkeypatch.setattr(zs, "is_firefox_holding_profile", lambda _: True)

    worker = FirefoxCookieWaitWorker(
        profile, pre_signature="reese84=stale-old-value", poll_interval_sec=0.01,
    )
    captured = []
    worker.finished.connect(lambda *args: captured.append(args))
    worker.run()

    assert len(captured) == 1, (
        f"expected exactly one finished emission, got {captured}"
    )
    success, n, is_new, _pre = captured[0]
    assert success is True
    assert is_new is True
    assert n > 0
    # Cookies should have been persisted via set_zacks_cookies → file.
    saved = zs.get_zacks_cookies()
    assert "reese84=fresh-token-abc" in saved


def test_cookie_wait_worker_does_not_capture_stale_cookies_pre_existing(
    tmp_data_dir, monkeypatch,
):
    """If cookies.sqlite already has both Imperva tokens but the
    signature MATCHES the pre-launch signature (i.e. the profile
    carried over the same session it had before), the mid-flight path
    must NOT capture — that would short-circuit a real refresh. The
    worker keeps polling until either a real session rotation happens
    or the user closes Firefox."""
    from trade_scanner_fh.gui.workers import FirefoxCookieWaitWorker
    import threading as _threading

    profile = tmp_data_dir / "firefox_zacks_profile"
    future = int(time.time()) + 3600
    _make_cookies_sqlite(profile, [
        ("reese84", "stale-token", ".zacks.com", future),
        ("visid_incap_1", "stale-incap", ".zacks.com", future),
    ])

    # Pre-signature MATCHES what's on disk → no rotation detected.
    pre_sig = zs._cookie_signature(zs.read_cookies_from_firefox_profile(profile))
    assert pre_sig  # sanity

    # Firefox stays "open" → only path that exits the loop is the
    # request_stop we trigger from a side thread.
    monkeypatch.setattr(zs, "is_firefox_holding_profile", lambda _: True)

    worker = FirefoxCookieWaitWorker(
        profile, pre_signature=pre_sig, poll_interval_sec=0.01,
    )
    captured = []
    worker.finished.connect(lambda *args: captured.append(args))

    def _stop_after_settle():
        # Give the worker enough ticks to see the stale cookies
        # multiple times — confirms it's NOT capturing them.
        time.sleep(0.15)
        worker.request_stop()

    _threading.Thread(target=_stop_after_settle, daemon=True).start()
    worker.run()

    assert len(captured) == 1
    success, _n, _is_new, _pre = captured[0]
    # request_stop fired before any rotation → cancelled, NOT success.
    assert success is False


def test_cookie_wait_worker_waits_when_only_visid_incap_present(
    tmp_data_dir, monkeypatch,
):
    """visid_incap alone (challenge in progress) is NOT enough to
    trigger mid-flight capture — the worker must keep polling. We
    verify by tripping request_stop and asserting the verdict is
    cancellation, not success."""
    from trade_scanner_fh.gui.workers import FirefoxCookieWaitWorker
    import threading as _threading

    profile = tmp_data_dir / "firefox_zacks_profile"
    future = int(time.time()) + 3600
    _make_cookies_sqlite(profile, [
        ("visid_incap_99", "in-flight", ".zacks.com", future),
    ])

    monkeypatch.setattr(zs, "is_firefox_holding_profile", lambda _: True)

    worker = FirefoxCookieWaitWorker(
        profile, pre_signature="", poll_interval_sec=0.01,
    )
    captured = []
    worker.finished.connect(lambda *args: captured.append(args))

    def _stop_soon():
        time.sleep(0.1)
        worker.request_stop()

    _threading.Thread(target=_stop_soon, daemon=True).start()
    worker.run()

    assert len(captured) == 1
    success, _n, _is_new, _pre = captured[0]
    assert success is False  # cancelled before tokens completed


def test_cookie_wait_worker_falls_through_to_close_path(
    tmp_data_dir, monkeypatch,
):
    """Legacy on-close path: when poll_for_tokens=False, the worker
    reverts to waiting for Firefox to close before reading. The
    on-close path must still work for users who want the old
    "close-to-finalize" behavior or for debugging."""
    from trade_scanner_fh.gui.workers import FirefoxCookieWaitWorker

    profile = tmp_data_dir / "firefox_zacks_profile"
    future = int(time.time()) + 3600
    _make_cookies_sqlite(profile, [
        ("reese84", "after-close", ".zacks.com", future),
        ("visid_incap_1", "xyz", ".zacks.com", future),
    ])

    # Firefox transitions open → closed on the second poll iteration.
    state = {"polls": 0}
    def fake_holding(_):
        state["polls"] += 1
        return state["polls"] < 2
    monkeypatch.setattr(zs, "is_firefox_holding_profile", fake_holding)

    worker = FirefoxCookieWaitWorker(
        profile, pre_signature="", poll_interval_sec=0.01,
        poll_for_tokens=False,
    )
    captured = []
    worker.finished.connect(lambda *args: captured.append(args))
    worker.run()

    assert len(captured) == 1
    success, n, is_new, _pre = captured[0]
    assert success is True
    assert is_new is True
    assert n > 0


def test_cookie_wait_worker_close_without_tokens_fails(
    tmp_data_dir, monkeypatch,
):
    """If Firefox closes without any Imperva tokens being captured
    (e.g. user backed out before the JS challenge), the worker emits
    a failure verdict so the GUI can fall back to manual paste."""
    from trade_scanner_fh.gui.workers import FirefoxCookieWaitWorker

    profile = tmp_data_dir / "firefox_zacks_profile"
    future = int(time.time()) + 3600
    # Only stale analytics cookies — no Imperva tokens.
    _make_cookies_sqlite(profile, [
        ("cookie_disclosure", "Y", ".zacks.com", future),
        ("nr_data", "junk", ".zacks.com", future),
    ])

    state = {"polls": 0}
    def fake_holding(_):
        state["polls"] += 1
        return state["polls"] < 2  # closed on second poll
    monkeypatch.setattr(zs, "is_firefox_holding_profile", fake_holding)

    worker = FirefoxCookieWaitWorker(
        profile, pre_signature="", poll_interval_sec=0.01,
    )
    captured = []
    worker.finished.connect(lambda *args: captured.append(args))
    worker.run()

    assert len(captured) == 1
    success, _n, _is_new, _pre = captured[0]
    assert success is False
