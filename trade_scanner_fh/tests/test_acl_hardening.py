"""DATA_DIR ACL hardening — SEC-1 (2026-08-12), corrected 2026-08-13.

The original form ran a single command:

    icacls DATA_DIR /inheritance:r /grant user:(OI)(CI)F /grant SYSTEM:(OI)(CI)F /T

`/T` applies `/inheritance:r` to every FILE as well, but `(OI)`/`(CI)` are
container-inheritance flags that produce no valid ACE on a file object. Each
pre-existing file therefore lost its inherited ACEs and gained nothing —
ending with an EMPTY DACL, which denies everyone including the owner.

Files created AFTER the run inherited correctly from the (correctly hardened)
parent, so the damage looked arbitrary. On the live tree it hit exactly the
four files not rewritten since: `sec_contact.txt` and the three saved presets.
The app surfaced them as unreadable/corrupt.

These tests run the REAL `icacls` against a temp tree — a mock would have
happily reproduced the bug, since the bug was in what icacls does with the
arguments, not in whether they were passed.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from trade_scanner_fh import config

pytestmark = pytest.mark.skipif(
    os.name != "nt", reason="ACL hardening is Windows-only"
)


@pytest.fixture
def tree(tmp_path, monkeypatch):
    """A DATA_DIR-shaped tree whose files all pre-date the hardening run."""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    (tmp_path / "presets").mkdir()
    (tmp_path / "sec_contact.txt").write_text("a@b.com\n", encoding="utf-8")
    (tmp_path / "presets" / "gap scan.json").write_text(
        json.dumps({"_preset_version": 1, "indicators": []}), encoding="utf-8",
    )
    (tmp_path / "universe.csv").write_text("symbol\nAAPL\n", encoding="utf-8")
    return tmp_path


def _readable(p: Path) -> bool:
    try:
        p.read_bytes()
        return True
    except OSError:
        return False


def test_preexisting_files_stay_readable_after_hardening(tree):
    """The regression. Every one of these was readable before the call and
    must still be after it — this is exactly what v1 broke."""
    targets = [
        tree / "sec_contact.txt",
        tree / "presets" / "gap scan.json",
        tree / "universe.csv",
    ]
    assert all(_readable(p) for p in targets), "fixture is not readable"

    changed, detail = config.harden_data_dir_acl()

    assert changed, detail
    for p in targets:
        assert _readable(p), f"{p.name} became unreadable: {detail}"


def test_preexisting_file_keeps_its_contents_and_still_parses(tree):
    preset = tree / "presets" / "gap scan.json"

    config.harden_data_dir_acl()

    assert json.loads(preset.read_text(encoding="utf-8"))["_preset_version"] == 1


def test_hardened_files_have_a_non_empty_dacl(tree):
    """An empty DACL denies everyone — the precise shape of the v1 damage.
    Assert the ACL is actually populated, not merely that a read succeeded
    (the owner can re-grant themselves, so a read is the weaker signal)."""
    win32security = pytest.importorskip("win32security")
    config.harden_data_dir_acl()

    for p in (tree / "sec_contact.txt", tree / "presets" / "gap scan.json"):
        sd = win32security.GetFileSecurity(
            str(p), win32security.DACL_SECURITY_INFORMATION,
        )
        dacl = sd.GetSecurityDescriptorDacl()
        assert dacl is not None, f"{p.name} has a null DACL"
        assert dacl.GetAceCount() > 0, f"{p.name} has an EMPTY DACL"


def test_files_written_after_hardening_are_readable(tree):
    """New files must inherit the hardened grants from the parent."""
    config.harden_data_dir_acl()

    fresh = tree / "presets" / "written after.json"
    fresh.write_text(json.dumps({"ok": True}), encoding="utf-8")

    assert json.loads(fresh.read_text(encoding="utf-8"))["ok"] is True


def test_hardening_is_sentinel_gated(tree):
    first, _ = config.harden_data_dir_acl()
    second, detail = config.harden_data_dir_acl()

    assert first is True
    assert second is False and "already hardened" in detail
    assert (tree / config._ACL_SENTINEL_NAME).exists()


def test_sentinel_version_forces_a_rerun_over_the_v1_marker(tree):
    """An install that ran the broken v1 must NOT be considered done — the
    rerun is what repairs its empty-DACL files."""
    (tree / ".acl_hardened_v1.done").write_text("ok\n", encoding="utf-8")

    changed, detail = config.harden_data_dir_acl()

    assert changed, f"v1 marker wrongly suppressed the repair: {detail}"


def test_missing_data_dir_is_not_an_error(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path / "nope")

    changed, detail = config.harden_data_dir_acl()

    assert changed is False and "does not exist" in detail
