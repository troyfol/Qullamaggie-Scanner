"""Tests for the user-config overrides (scanner_data/user_config.json).

Covers config.load_user_config() / config.save_user_config(): the
load/fallback/clamp/corrupt-JSON matrix plus the live-attribute apply.
All filesystem traffic is redirected to tmp_path by monkeypatching
config.DATA_DIR (same approach as test_config.py's _sec_tmp fixture);
no network, no Qt.
"""
import json
from pathlib import Path

import pytest

from trade_scanner_fh import config


@pytest.fixture
def _ucfg_tmp(tmp_path, monkeypatch):
    """Redirect config.DATA_DIR to a temp dir and register the four
    overridable attributes with monkeypatch (re-set to their current
    values) so anything a test applies via load/save_user_config() is
    rolled back at teardown — the rest of the suite must keep seeing
    whatever values were live before. Then reset to the baked-in
    defaults (no file in tmp_path) so every test starts clean even if
    the dev machine has a real user_config.json."""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(
        config, "OHLCV_HISTORY_YEARS", config.OHLCV_HISTORY_YEARS)
    monkeypatch.setattr(
        config, "EARNINGS_HISTORY_YEARS", config.EARNINGS_HISTORY_YEARS)
    monkeypatch.setattr(
        config, "REFERENCE_TICKERS", list(config.REFERENCE_TICKERS))
    monkeypatch.setattr(
        config, "PREFETCH_OHLCV_AT_LAUNCH", config.PREFETCH_OHLCV_AT_LAUNCH)
    config.load_user_config()  # no file yet → baked-in defaults
    return tmp_path


def _write(tmp_path: Path, payload) -> Path:
    path = tmp_path / "user_config.json"
    if isinstance(payload, (str, bytes)):
        if isinstance(payload, bytes):
            path.write_bytes(payload)
        else:
            path.write_text(payload, encoding="utf-8")
    else:
        path.write_text(json.dumps(payload), encoding="utf-8")
    return path


# ----------------------------------------------------------------------
# Baked-in defaults — pinned (mirrors test_config.py's constant pins)
# ----------------------------------------------------------------------

def test_baked_in_defaults_pinned():
    """The fallback values must stay exactly what shipped: a regression
    here silently changes what every invalid/missing override reverts
    to."""
    d = config._USER_CONFIG_DEFAULTS
    assert d["OHLCV_HISTORY_YEARS"] == 5
    assert d["EARNINGS_HISTORY_YEARS"] == 10
    assert list(d["REFERENCE_TICKERS"][:2]) == ["SPY", "ONEQ"]
    assert len(d["REFERENCE_TICKERS"]) == 13
    assert d["PREFETCH_OHLCV_AT_LAUNCH"] is False


def test_load_user_config_called_at_import():
    """Pin the import-time apply: load_user_config() must be invoked at
    the bottom of config.py so consumer modules see the user's values
    from first use (source inspection, same style as the audit-H1
    signal-connection pin)."""
    import inspect
    src = Path(inspect.getsourcefile(config)).read_text(encoding="utf-8")
    assert "\nload_user_config()" in src


def test_user_config_path_follows_data_dir(_ucfg_tmp):
    """user_config_path() must resolve lazily off config.DATA_DIR so a
    monkeypatched/packaged DATA_DIR moves the file with it."""
    assert config.user_config_path() == _ucfg_tmp / "user_config.json"


# ----------------------------------------------------------------------
# Load: missing / valid / partial
# ----------------------------------------------------------------------

def test_missing_file_keeps_defaults(_ucfg_tmp):
    applied = config.load_user_config()
    assert applied == {}
    assert config.OHLCV_HISTORY_YEARS == 5
    assert config.EARNINGS_HISTORY_YEARS == 10
    assert config.REFERENCE_TICKERS[:2] == ["SPY", "ONEQ"]


def test_load_applies_valid_overrides(_ucfg_tmp):
    _write(_ucfg_tmp, {
        "OHLCV_HISTORY_YEARS": 8,
        "EARNINGS_HISTORY_YEARS": 15,
        "REFERENCE_TICKERS": ["SPY", "QQQ", "IWM"],
    })
    applied = config.load_user_config()
    assert applied == {
        "OHLCV_HISTORY_YEARS": 8,
        "EARNINGS_HISTORY_YEARS": 15,
        "REFERENCE_TICKERS": ["SPY", "QQQ", "IWM"],
    }
    assert config.OHLCV_HISTORY_YEARS == 8
    assert config.EARNINGS_HISTORY_YEARS == 15
    assert config.REFERENCE_TICKERS == ["SPY", "QQQ", "IWM"]


def test_partial_file_leaves_other_fields_at_default(_ucfg_tmp):
    _write(_ucfg_tmp, {"EARNINGS_HISTORY_YEARS": 3})
    config.load_user_config()
    assert config.EARNINGS_HISTORY_YEARS == 3
    assert config.OHLCV_HISTORY_YEARS == 5
    assert len(config.REFERENCE_TICKERS) == 13


def test_unknown_keys_ignored(_ucfg_tmp):
    _write(_ucfg_tmp, {"BOGUS_KEY": 42, "OHLCV_HISTORY_YEARS": 7})
    applied = config.load_user_config()
    assert applied == {"OHLCV_HISTORY_YEARS": 7}
    assert not hasattr(config, "BOGUS_KEY")


def test_reload_after_delete_restores_defaults(_ucfg_tmp):
    """Fields absent from the file (e.g. the file was deleted) must
    revert to the baked-in defaults on the next load — overrides can't
    stick around in module state."""
    _write(_ucfg_tmp, {"OHLCV_HISTORY_YEARS": 20})
    config.load_user_config()
    assert config.OHLCV_HISTORY_YEARS == 20
    config.user_config_path().unlink()
    config.load_user_config()
    assert config.OHLCV_HISTORY_YEARS == 5


# ----------------------------------------------------------------------
# Int validation: clamping + type fallbacks
# ----------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    (0, 1),       # below floor → clamped up
    (1, 1),       # at floor
    (25, 25),     # at ceiling
    (99, 25),     # above ceiling → clamped down
    (-5, 1),      # negative → clamped up
])
def test_int_overrides_clamped(_ucfg_tmp, raw, expected):
    _write(_ucfg_tmp, {
        "OHLCV_HISTORY_YEARS": raw,
        "EARNINGS_HISTORY_YEARS": raw,
    })
    config.load_user_config()
    assert config.OHLCV_HISTORY_YEARS == expected
    assert config.EARNINGS_HISTORY_YEARS == expected


@pytest.mark.parametrize("bad", [
    "ten",      # string
    12.5,       # float
    True,       # bool — int subclass, must be rejected explicitly
    None,
    [5],
    {"y": 5},
])
def test_non_int_falls_back_to_default(_ucfg_tmp, bad):
    _write(_ucfg_tmp, {"OHLCV_HISTORY_YEARS": bad})
    applied = config.load_user_config()
    assert applied == {}
    assert config.OHLCV_HISTORY_YEARS == 5


# ----------------------------------------------------------------------
# REFERENCE_TICKERS validation
# ----------------------------------------------------------------------

@pytest.mark.parametrize("bad", [
    "SPY",                      # not a list
    [],                         # empty
    ["SPY", 42],                # non-string entry
    ["SPY", ""],                # blank entry
    ["SPY", "not a ticker!"],   # implausible symbol
    ["1SPY"],                   # leading digit
    ["WAYTOOLONGSYM"],          # > 10 chars
    {"SPY": 1},                 # wrong container
])
def test_invalid_ticker_list_falls_back_whole(_ucfg_tmp, bad):
    """ANY bad entry reverts the whole list — never a silent partial
    benchmark set."""
    _write(_ucfg_tmp, {"REFERENCE_TICKERS": bad})
    applied = config.load_user_config()
    assert applied == {}
    assert len(config.REFERENCE_TICKERS) == 13
    assert config.REFERENCE_TICKERS[0] == "SPY"


def test_ticker_list_normalized(_ucfg_tmp):
    """Entries are stripped/uppercased and deduped (order-preserving);
    dotted/hyphenated class shares pass."""
    _write(_ucfg_tmp, {
        "REFERENCE_TICKERS": [" spy ", "brk.b", "BF-B", "SPY"],
    })
    config.load_user_config()
    assert config.REFERENCE_TICKERS == ["SPY", "BRK.B", "BF-B"]


def test_plausible_ticker_re_rejects_trailing_newline():
    """PLAUSIBLE_TICKER_RE must anchor with \\Z, not $: with $,
    re.match("AAPL\\n") would MATCH (a $ matches just before a trailing
    newline), letting a newline-bearing 'ticker' through validation.
    Pins both the accept and the reject."""
    assert config.PLAUSIBLE_TICKER_RE.match("AAPL") is not None
    assert config.PLAUSIBLE_TICKER_RE.match("AAPL\n") is None


# ----------------------------------------------------------------------
# PREFETCH_OHLCV_AT_LAUNCH validation (F5)
# ----------------------------------------------------------------------

def test_prefetch_flag_defaults_false(_ucfg_tmp):
    """No file → the launch-prefetch toggle stays OFF (the baked-in
    default), so old installs and old user_config.json files see no
    behavior change."""
    applied = config.load_user_config()
    assert applied == {}
    assert config.PREFETCH_OHLCV_AT_LAUNCH is False


@pytest.mark.parametrize("value", [True, False])
def test_prefetch_flag_valid_override_applied(_ucfg_tmp, value):
    """A genuine JSON bool round-trips: applied to the live attribute
    and reported in the applied-overrides dict (explicit false too —
    it's a valid value, not 'missing')."""
    _write(_ucfg_tmp, {"PREFETCH_OHLCV_AT_LAUNCH": value})
    applied = config.load_user_config()
    assert applied == {"PREFETCH_OHLCV_AT_LAUNCH": value}
    assert config.PREFETCH_OHLCV_AT_LAUNCH is value


@pytest.mark.parametrize("bad", [
    1,            # int — truthy but not a bool
    0,            # int — falsy but not a bool
    "true",       # string
    "False",      # string
    1.0,          # float
    None,
    [True],       # wrong container
    {"on": True},
])
def test_prefetch_flag_rejects_non_bool(_ucfg_tmp, bad):
    """Anything but a genuine JSON true/false falls back to the baked-in
    default — same strictness as the int fields' bool rejection."""
    _write(_ucfg_tmp, {"PREFETCH_OHLCV_AT_LAUNCH": bad})
    applied = config.load_user_config()
    assert applied == {}
    assert config.PREFETCH_OHLCV_AT_LAUNCH is False


def test_prefetch_flag_reverts_on_delete(_ucfg_tmp):
    """An applied true override must not stick in module state once the
    file is gone — mirrors test_reload_after_delete_restores_defaults."""
    _write(_ucfg_tmp, {"PREFETCH_OHLCV_AT_LAUNCH": True})
    config.load_user_config()
    assert config.PREFETCH_OHLCV_AT_LAUNCH is True
    config.user_config_path().unlink()
    config.load_user_config()
    assert config.PREFETCH_OHLCV_AT_LAUNCH is False


def test_prefetch_flag_save_round_trip(_ucfg_tmp):
    """save_user_config persists the bool, applies it live, and drops an
    invalid value back to default (never written to disk)."""
    assert config.save_user_config({
        "PREFETCH_OHLCV_AT_LAUNCH": True,
    }) is True
    assert config.PREFETCH_OHLCV_AT_LAUNCH is True
    on_disk = json.loads(
        config.user_config_path().read_text(encoding="utf-8"))
    assert on_disk == {"PREFETCH_OHLCV_AT_LAUNCH": True}
    # Invalid on save → dropped from disk AND reverted in memory.
    assert config.save_user_config({
        "PREFETCH_OHLCV_AT_LAUNCH": "yes",
    }) is True
    on_disk = json.loads(
        config.user_config_path().read_text(encoding="utf-8"))
    assert on_disk == {}
    assert config.PREFETCH_OHLCV_AT_LAUNCH is False


# ----------------------------------------------------------------------
# Corrupt file: must never crash, always defaults
# ----------------------------------------------------------------------

@pytest.mark.parametrize("payload", [
    "{not json at all",               # malformed JSON
    "",                               # empty file
    "[1, 2, 3]",                      # valid JSON, wrong top-level type
    '"just a string"',                # valid JSON, wrong top-level type
    "null",
    b"\xff\xfe\x00garbage",           # not even UTF-8
])
def test_corrupt_file_falls_back_silently(_ucfg_tmp, payload):
    _write(_ucfg_tmp, payload)
    applied = config.load_user_config()  # must not raise
    assert applied == {}
    assert config.OHLCV_HISTORY_YEARS == 5
    assert config.EARNINGS_HISTORY_YEARS == 10
    assert len(config.REFERENCE_TICKERS) == 13
    assert config.PREFETCH_OHLCV_AT_LAUNCH is False


# ----------------------------------------------------------------------
# Save: round-trip, validation, live apply, write errors
# ----------------------------------------------------------------------

def test_save_round_trip(_ucfg_tmp):
    assert config.save_user_config({
        "OHLCV_HISTORY_YEARS": 12,
        "EARNINGS_HISTORY_YEARS": 6,
        "REFERENCE_TICKERS": ["SPY", "QQQ"],
    }) is True
    # Applied live, no re-load needed…
    assert config.OHLCV_HISTORY_YEARS == 12
    assert config.EARNINGS_HISTORY_YEARS == 6
    assert config.REFERENCE_TICKERS == ["SPY", "QQQ"]
    # …and persisted (a fresh load sees the same values).
    on_disk = json.loads(
        config.user_config_path().read_text(encoding="utf-8"))
    assert on_disk == {
        "OHLCV_HISTORY_YEARS": 12,
        "EARNINGS_HISTORY_YEARS": 6,
        "REFERENCE_TICKERS": ["SPY", "QQQ"],
    }
    assert config.load_user_config() == on_disk


def test_save_clamps_and_drops_invalid_fields(_ucfg_tmp):
    """Invalid fields never reach disk; out-of-range ints are clamped
    on the way through (the file only ever holds validated values)."""
    assert config.save_user_config({
        "OHLCV_HISTORY_YEARS": 100,            # → clamped to 25
        "EARNINGS_HISTORY_YEARS": "bogus",     # → dropped
        "REFERENCE_TICKERS": ["SPY", "???"],   # → dropped whole
    }) is True
    on_disk = json.loads(
        config.user_config_path().read_text(encoding="utf-8"))
    assert on_disk == {"OHLCV_HISTORY_YEARS": 25}
    # Dropped fields revert to the baked-in defaults in memory too —
    # full-state semantics keep disk and module state in agreement.
    assert config.OHLCV_HISTORY_YEARS == 25
    assert config.EARNINGS_HISTORY_YEARS == 10
    assert len(config.REFERENCE_TICKERS) == 13


def test_save_write_error_returns_false_and_leaves_state(_ucfg_tmp,
                                                         monkeypatch):
    """A failed write must report False and NOT half-apply the values."""
    def boom(*a, **kw):
        raise OSError("disk full")
    monkeypatch.setattr(config, "atomic_write_text", boom)
    assert config.save_user_config({"OHLCV_HISTORY_YEARS": 9}) is False
    assert config.OHLCV_HISTORY_YEARS == 5


def test_save_creates_data_dir_if_missing(_ucfg_tmp):
    """atomic_write_text mkdirs the parent, so saving on a fresh install
    (no scanner_data/ yet) must work."""
    nested = _ucfg_tmp / "fresh" / "scanner_data"
    config.DATA_DIR = nested  # fixture's monkeypatch restores this
    assert config.save_user_config({"OHLCV_HISTORY_YEARS": 4}) is True
    assert (nested / "user_config.json").exists()
    assert config.OHLCV_HISTORY_YEARS == 4
