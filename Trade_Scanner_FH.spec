# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for Trade_Scanner_FH — the Finnhub fork of the
trading scanner.

  - Output: dist/Trade_Scanner_FH/ (windowed, --onedir). Was --onefile until
    2026-08-13; see the comment above EXE() for why it changed and what it
    means for scanner_data/ placement and for releases.
  - hiddenimports cover the trade_scanner_fh package plus its lazy
    runtime deps (yfinance, lxml, keyring, openpyxl, curl_cffi,
    psutil, win32api, finnhub, …).
  - excludes PySide6 / shiboken6 so the build doesn't abort when the
    shared environment carries two Qt bindings; this app is PyQt6-only.
  - The Zacks scraper is HTTP-only (curl_cffi TLS impersonation);
    no Playwright / Chromium is bundled.

Works with venv or conda.
"""

import sys
from pathlib import Path

# Use base_prefix to find the actual Python installation (works in venv and conda)
BASE_PREFIX = Path(getattr(sys, 'base_prefix', sys.prefix))
CONDA_BIN = BASE_PREFIX / "Library" / "bin"
QT_PLUGINS = BASE_PREFIX / "Library" / "lib" / "qt6" / "plugins"

# --- C-library DLLs needed by Python extension modules ---
REQUIRED_DLLS = [
    "ffi.dll",              # _ctypes.pyd
    "ffi-8.dll",            # _ctypes.pyd (newer builds)
    "libexpat.dll",         # pyexpat.pyd
    "libssl-3-x64.dll",    # _ssl.pyd
    "libcrypto-3-x64.dll", # _ssl.pyd
    "sqlite3.dll",          # _sqlite3.pyd
    "liblzma.dll",          # _lzma.pyd
    "LIBBZ2.dll",           # _bz2.pyd
]

binaries = []
for dll_name in REQUIRED_DLLS:
    dll_path = CONDA_BIN / dll_name
    if dll_path.exists():
        binaries.append((str(dll_path), "."))

# --- Qt6 DLLs (conda stores them separately from PyQt6) ---
QT6_DLLS = [
    "Qt6Core.dll", "Qt6Gui.dll", "Qt6Widgets.dll",
    "Qt6Network.dll", "Qt6Svg.dll", "Qt6OpenGL.dll",
    "Qt6PrintSupport.dll", "Qt6DBus.dll",
]
for dll_name in QT6_DLLS:
    dll_path = CONDA_BIN / dll_name
    if dll_path.exists():
        binaries.append((str(dll_path), "."))

# --- Qt6 platform plugins (required to create a window) ---
for plugin_dir in ["platforms", "styles"]:
    plugin_path = QT_PLUGINS / plugin_dir
    if plugin_path.exists():
        for dll in plugin_path.glob("*.dll"):
            binaries.append((str(dll), f"PyQt6/Qt6/plugins/{plugin_dir}"))

# --- ICU DLLs (Qt6Core depends on these) ---
for icu_dll in CONDA_BIN.glob("icu*.dll"):
    binaries.append((str(icu_dll), "."))

a = Analysis(
    ['launch_scanner.py'],
    pathex=[],
    binaries=binaries,
    datas=[],
    hiddenimports=[
        'yfinance', 'pyautogui', 'pyarrow', 'PyQt6.sip',
        'finance_calendars', 'finance_calendars.finance_calendars',
        # lxml is an *optional* dep of pandas/yfinance — yfinance.earnings_dates
        # imports it lazily, so PyInstaller doesn't auto-detect it. Without
        # this, every targeted earnings fill silently fails with ImportError.
        'lxml', 'lxml.etree', 'lxml.html',
        # keyring + Windows backend so the Finnhub credential prompt persists
        # across launches via Windows Credential Manager.
        'keyring', 'keyring.backends', 'keyring.backends.Windows',
        # openpyxl is the engine pandas uses for the XLSX export from the
        # Excel button. Pulled in lazily by pandas.to_excel, so PyInstaller
        # doesn't auto-detect it without an explicit hidden import.
        'openpyxl', 'openpyxl.workbook', 'openpyxl.styles',
        'trade_scanner_fh', 'trade_scanner_fh.config',
        'trade_scanner_fh.scanner', 'trade_scanner_fh.indicators',
        'trade_scanner_fh.data_engine', 'trade_scanner_fh.ticker_universe',
        'trade_scanner_fh.tradestation',
        'trade_scanner_fh.hotkey',
        'trade_scanner_fh.sector_map', 'trade_scanner_fh.earnings_cache',
        'trade_scanner_fh.finnhub_client',
        # Zacks earnings integration modules (added in this fork)
        'trade_scanner_fh.zacks_scraper',
        'trade_scanner_fh.earnings_history',
        'trade_scanner_fh.earnings_reconcile',
        # (EDGAR earnings modules removed 2026-05-31.)
        # Finviz earnings source (top-priority adjusted) — scraped via
        # curl_cffi; imported lazily from the GUI menu handlers +
        # FinvizFillWorker, so name them explicitly.
        'trade_scanner_fh.finviz_client',
        'trade_scanner_fh.finviz_fill',
        # Other lazily-imported fill modules. PyInstaller's static
        # analysis CURRENTLY catches these via the main_window menu
        # handlers, but if a future refactor moves the import into a
        # more dynamic site they'd silently disappear from the frozen
        # build — listing them explicitly is pure defensive bundling.
        'trade_scanner_fh.earnings_raw',
        'trade_scanner_fh.finnhub_fill',
        'trade_scanner_fh.yahoo_fill',
        'trade_scanner_fh.nasdaq_fill',
        # curl_cffi is the libcurl-backed `requests` drop-in the Zacks
        # scraper uses for Chrome 131 TLS impersonation. It loads its
        # native lib lazily — PyInstaller's auto-discovery misses the
        # submodule chain without an explicit hint.
        'curl_cffi', 'curl_cffi.requests',
        # Cookie-refresh dependencies (May 2026 rewrite):
        # - psutil: enumerate firefox.exe processes by cmdline so the
        #   FirefoxCookieWaitWorker can detect when the user closes
        #   the persistent-profile Firefox.
        # - sqlite3: stdlib, auto-included. Reads cookies.sqlite from
        #   the persistent profile directly (Firefox does not encrypt
        #   cookies on disk; no Cryptodome / browser_cookie3 needed).
        # - win32gui/win32process: window placement on the user's
        #   chosen cookie-browser monitor.
        'psutil',
        'win32api', 'win32con', 'win32gui', 'win32process',
        # win32crypt (pywin32 DPAPI) — used lazily by zacks_scraper to
        # encrypt the cookie file at rest (CryptProtectData). Listed
        # explicitly so the frozen build bundles it and the encryption is
        # actually active (it degrades to plaintext if the import is missing).
        'win32crypt',
        # gui is a subpackage — list each module explicitly
        'trade_scanner_fh.gui', 'trade_scanner_fh.gui.main_window',
        'trade_scanner_fh.gui.workers', 'trade_scanner_fh.gui.widgets',
        'trade_scanner_fh.gui.dialogs', 'trade_scanner_fh.gui.theme',
        'trade_scanner_fh.gui.hotkey_dialog',
        # NOTE: Playwright was originally planned per TINYEARNINGS_FORK.md
        # §2 but live testing showed Zacks's Imperva front blocks every
        # browser fingerprint (headless and headful Chromium / Firefox /
        # patchright) while plain `requests` works. zacks_scraper.py is
        # HTTP-only — no Playwright, no Chromium binary to bundle. The
        # §12-first-bullet PyInstaller-Chromium concern is moot.
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    # PySide6 / shiboken6 live in the shared build env (sibling projects
    # use them) but this app is PyQt6-only. PyInstaller aborts the build
    # if it sees two Qt bindings packages, so exclude PySide6 explicitly.
    # scipy is safe to exclude: the app has zero scipy imports, and
    # pandas/yfinance only lazy-import it on paths never hit here
    # (yfinance repair=True is never passed).
    #
    # 2026-08-13: the build env is SHARED with unrelated projects, and their
    # dependency closures were being swept into this app. Measured on the
    # extracted payload: llvmlite.dll alone was 101.7 MB — 26% of 393 MB —
    # pulled in by numba, which is required by openai-whisper. sklearn (18 MB)
    # arrives via pyannote / pytorch-metric-learning / neurokit2, and jedi
    # (3.3 MB) via ipython. None of it is reachable from this app.
    #
    # Verified before excluding, not assumed: with all of these blocked at
    # import time, all 21 runtime modules imported, every third-party dep
    # (pandas / numpy / pyarrow / yfinance / lxml / curl_cffi / keyring /
    # openpyxl) imported, a pyarrow parquet round-trip succeeded, and 22 of 23
    # indicator functions executed against real OHLCV — with ZERO blocked
    # imports intercepted. `engine="numba"` appears nowhere in the source, so
    # pandas' optional numba path is never taken.
    excludes=['matplotlib', 'tkinter', 'test', 'unittest',
              'PySide6', 'shiboken6', 'scipy',
              'numba', 'llvmlite', 'sklearn', 'scikit-learn',
              'jedi', 'IPython', 'torch', 'whisper',
              'pyannote', 'neurokit2', 'financetoolkit'],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure)

# --onedir (2026-08-13). The build was --onefile, which meant the bootloader
# unpacked the WHOLE payload to %TEMP%\_MEIxxxxxx on every single launch before
# one line of app code ran. Measured on the shipped exe: 393 MB across 4,429
# files, 8.4 s of pure extraction on a warm run — against 0.108 s of actual
# app-side startup work. Windows Defender real-time protection then scans all
# 4,429 freshly written files, and 21 abandoned _MEI directories totalling
# 5.2 GB had accumulated in TEMP from launches that did not exit cleanly (the
# bootloader only removes its directory on a clean shutdown), on a volume with
# 23 GB free. That is the whole "slow to launch and often hangs" story.
#
# onedir has NO extraction step: the exe loads its dependencies from the
# _internal/ folder beside it, so launch cost drops to process start + imports.
# The trade-off, and the reason this was deferred at the 2026-08-12 audit, is
# that the release artifact becomes a FOLDER rather than one file — it needs
# zipping for a GitHub release.
#
# NOTE: config.APP_ROOT is `Path(sys.executable).parent` when frozen, so the
# app now looks for scanner_data/ beside dist/Trade_Scanner_FH/Trade_Scanner_FH.exe
# rather than beside dist/Trade_Scanner_FH.exe. The existing data directory has
# to be reachable from the new location or the app starts cold.
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='Trade_Scanner_FH',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
    # Audit 2026-08-12 (SEC-6): with console=False an unhandled exception
    # otherwise raises a traceback DIALOG. Combined with SEC-5's latent
    # token-in-URL leak that is a plausible secret-in-traceback path, and the
    # dialog tells a user nothing actionable anyway — the same traceback is
    # already written to scanner_data/logs/. Signing remains deferred (a
    # certificate is an external dependency).
    disable_windowed_traceback=True,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='barchart_zacks.ico',
    # Windows VERSIONINFO resource — versions mirror
    # trade_scanner_fh.__version__.
    version='version_info.txt',
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name='Trade_Scanner_FH',
)
