"""Lockstep guard for version_info.txt (the Windows VERSIONINFO resource
PyInstaller embeds in the exe).

A version bump that updates ``trade_scanner_fh.__version__`` but forgets
the resource file would silently ship an exe stamped with the old
version. Parse the file with PyInstaller's own loader so the assertions
see exactly what a build would embed."""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from trade_scanner_fh import __version__

versioninfo = pytest.importorskip(
    "PyInstaller.utils.win32.versioninfo",
    reason="PyInstaller (build-time dep) not installed",
)

# tests/ -> trade_scanner_fh/ -> repo root, next to the .spec file.
_VERSION_INFO_TXT = Path(__file__).resolve().parents[2] / "version_info.txt"


def _load():
    return versioninfo.load_version_info_from_text_file(
        str(_VERSION_INFO_TXT))


def _string_table(vi) -> dict[str, str]:
    """{name: value} from the resource's first StringTable."""
    for kid in vi.kids:
        if isinstance(kid, versioninfo.StringFileInfo):
            return {s.name: s.val for s in kid.kids[0].kids}
    raise AssertionError("version_info.txt has no StringFileInfo block")


def test_string_versions_match_package_version():
    """FileVersion / ProductVersion carry the full __version__ tag."""
    strings = _string_table(_load())
    assert strings["FileVersion"] == __version__
    assert strings["ProductVersion"] == __version__


def test_numeric_filevers_matches_version_prefix():
    """filevers/prodvers derive from __version__'s numeric prefix
    ("1.0.0-zacks.0" -> (1, 0, 0, ...)). FixedFileInfo packs the tuple
    into MS/LS dwords, so unpack before comparing."""
    ffi = _load().ffi
    filevers = (ffi.fileVersionMS >> 16, ffi.fileVersionMS & 0xffff,
                ffi.fileVersionLS >> 16, ffi.fileVersionLS & 0xffff)
    prodvers = (ffi.productVersionMS >> 16, ffi.productVersionMS & 0xffff,
                ffi.productVersionLS >> 16, ffi.productVersionLS & 0xffff)
    prefix = re.match(r"(\d+)\.(\d+)\.(\d+)", __version__)
    assert prefix is not None, f"unparseable __version__: {__version__!r}"
    assert filevers[:3] == tuple(int(p) for p in prefix.groups())
    assert prodvers == filevers
