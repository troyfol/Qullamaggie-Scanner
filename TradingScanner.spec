# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for TradingScanner — conda-friendly onefile build."""

import sys
from pathlib import Path

CONDA_PREFIX = Path(sys.prefix)
CONDA_BIN = CONDA_PREFIX / "Library" / "bin"
QT_PLUGINS = CONDA_PREFIX / "Library" / "lib" / "qt6" / "plugins"

# --- Conda C-library DLLs needed by Python extension modules ---
REQUIRED_DLLS = [
    "ffi.dll",              # _ctypes.pyd
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
        'trading_scanner', 'trading_scanner.config',
        'trading_scanner.scanner', 'trading_scanner.indicators',
        'trading_scanner.data_engine', 'trading_scanner.ticker_universe',
        'trading_scanner.tradestation', 'trading_scanner.gui',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['matplotlib', 'tkinter', 'test', 'unittest'],
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
    name='TradingScanner',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='trading_scanner/barchart.ico',
)
