# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for Trade_Scanner_FH — the Finnhub fork of the
trading scanner.

  - Output exe: Trade_Scanner_FH.exe (windowed, single-file).
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
    excludes=['matplotlib', 'tkinter', 'test', 'unittest',
              'PySide6', 'shiboken6'],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='Trade_Scanner_FH',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='barchart_zacks.ico',
)
