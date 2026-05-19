"""Tests for zacks_scraper.py — pure-parser tests against a cached real
Zacks page. Live HTTP is mocked so this suite is offline + deterministic."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from trade_scanner_fh import zacks_scraper as zs


_FIXTURE = Path(__file__).resolve().parent / "_zacks_aapl_probe.html"


# ----------------------------------------------------------------------
# _strip_html / _normalize_time / _clean_value / _parse_zacks_date
# ----------------------------------------------------------------------

def test_strip_html_removes_tags_and_entities():
    raw = '<div class="right pos positive">+5.34%</div>'
    assert zs._strip_html(raw) == "+5.34%"
    assert zs._strip_html("plain text") == "plain text"
    assert zs._strip_html("\xa0nbsp\xa0") == "nbsp"
    assert zs._strip_html(None) == ""
    assert zs._strip_html(float("nan")) == ""


@pytest.mark.parametrize("raw, expected", [
    ("Before Open",        "Open"),
    ("After Close",        "Close"),
    ("During Market Hours", "Market"),
    ("Time Not Supplied",  "Unknown"),
    ("",                   "Unknown"),
    ("totally unexpected", "Unknown"),
    (None,                 "Unknown"),
    # Wrapped in HTML still maps correctly
    ('<span>before open</span>', "Open"),
])
def test_normalize_time(raw, expected):
    assert zs._normalize_time(raw) == expected


@pytest.mark.parametrize("raw, expected", [
    ("$2.84",   2.84),
    ("+5.34%",  5.34),
    ("$1,234.56", 1234.56),
    ("--",      None),
    ("—",       None),
    ("",        None),
    (None,      None),
    ('<div class="right pos positive">+0.19</div>', 0.19),
    ('<div class="right pos positive">+7.17%</div>', 7.17),
    # Sales values can have commas + $
    ("$143,756.00", 143756.0),
])
def test_clean_value(raw, expected):
    got = zs._clean_value(raw)
    if expected is None:
        assert got is None
    else:
        assert got == pytest.approx(expected)


def test_parse_zacks_date_round_trips():
    ts = zs._parse_zacks_date("1/29/26")
    assert ts == pd.Timestamp("2026-01-29")


def test_parse_zacks_date_period_ending_format():
    # Zacks period_ending uses "M/YYYY" (e.g., "12/2025") — pd.to_datetime
    # interprets this as Dec 1, 2025
    ts = zs._parse_zacks_date("12/2025")
    assert ts == pd.Timestamp("2025-12-01")


def test_parse_zacks_date_garbage_is_none():
    assert zs._parse_zacks_date("--") is None
    assert zs._parse_zacks_date("") is None
    assert zs._parse_zacks_date(None) is None
    assert zs._parse_zacks_date("not a date") is None


# ----------------------------------------------------------------------
# _extract_obj_data — brace-walking JS object literal extraction
# ----------------------------------------------------------------------

def _load_fixture() -> str:
    return _FIXTURE.read_text(encoding="utf-8")


def test_obj_data_extracted_from_fixture():
    """The cached AAPL page has obj_data with all expected table keys."""
    data = zs._extract_obj_data(_load_fixture())
    assert data is not None
    assert "earnings_announcements_earnings_table" in data
    assert "earnings_announcements_sales_table" in data


def test_obj_data_eps_table_rows_have_seven_cells():
    """Each row in EPS / Sales tables is a 7-cell list:
    [report_date, period_ending, est, rep, surprise, surp%, time]."""
    data = zs._extract_obj_data(_load_fixture())
    eps = data["earnings_announcements_earnings_table"]
    assert len(eps) > 10  # AAPL has ~55 quarters in the cache
    for row in eps[:5]:
        assert isinstance(row, list)
        assert len(row) == 7


def test_obj_data_returns_none_for_unrelated_page():
    assert zs._extract_obj_data("<html>no obj_data here</html>") is None


def test_obj_data_handles_html_inside_string_values():
    """Cells contain `<div>...</div>` HTML — brace walk must not be confused
    by any { } that might appear inside escaped strings."""
    sample = (
        "<script>document.obj_data = {"
        '"k": [["a"], ["b"]],'
        '"escaped": "string with } in it",'
        '"end": 1'
        "};</script>"
    )
    data = zs._extract_obj_data(sample)
    assert data == {"k": [["a"], ["b"]], "escaped": "string with } in it", "end": 1}


# ----------------------------------------------------------------------
# _row_to_dict — translates one obj_data row into the §2.2 fragment
# ----------------------------------------------------------------------

def test_row_to_dict_eps():
    row = [
        "1/29/26", "12/2025", "$2.65", "$2.84",
        '<div class="right pos positive">+0.19</div>',
        '<div class="right pos positive">+7.17%</div>',
        "After Close",
    ]
    d = zs._row_to_dict(row, kind="eps")
    assert d is not None
    assert d["report_date"]   == pd.Timestamp("2026-01-29")
    assert d["period_ending"] == pd.Timestamp("2025-12-01")
    assert d["report_time"]   == "Close"
    assert d["estimated_eps"] == pytest.approx(2.65)
    assert d["reported_eps"]  == pytest.approx(2.84)
    assert d["surprise_eps"]  == pytest.approx(0.19)
    assert d["surprise_eps_pct"] == pytest.approx(7.17)
    # rev fields not present for kind='eps'
    assert "estimated_rev" not in d


def test_row_to_dict_rev():
    row = [
        "1/29/26", "12/2025", "$137,808.72", "$143,756.00",
        '<div class="right pos positive">+5,947.28</div>',
        '<div class="right pos positive">+4.32%</div>',
        "After Close",
    ]
    d = zs._row_to_dict(row, kind="rev")
    assert d["estimated_rev"]   == pytest.approx(137808.72)
    assert d["reported_rev"]    == pytest.approx(143756.0)
    assert d["surprise_rev"]    == pytest.approx(5947.28)
    assert d["surprise_rev_pct"] == pytest.approx(4.32)


def test_row_to_dict_short_row_returns_none():
    assert zs._row_to_dict(["only", "two"], kind="eps") is None


def test_row_to_dict_unparseable_dates_returns_none():
    row = ["--", "--", "--", "--", "--", "--", "--"]
    assert zs._row_to_dict(row, kind="eps") is None


def test_row_to_dict_six_columns_uses_unknown_time():
    """If the row lacks the time column (older Zacks layout), report_time
    falls back to 'Unknown' rather than crashing."""
    row = ["1/29/26", "12/2025", "$2.65", "$2.84", "0.19", "7.17"]
    d = zs._row_to_dict(row, kind="eps")
    assert d is not None
    assert d["report_time"] == "Unknown"


# ----------------------------------------------------------------------
# _merge_and_filter — joins EPS+Rev, applies cutoff, normalizes shape
# ----------------------------------------------------------------------

def test_merge_filter_intersects_on_report_date():
    eps = [
        {"report_date": pd.Timestamp("2026-01-29"),
         "period_ending": pd.Timestamp("2025-12-01"),
         "report_time": "Close",
         "estimated_eps": 2.65, "reported_eps": 2.84,
         "surprise_eps": 0.19, "surprise_eps_pct": 7.17},
    ]
    rev = [
        {"report_date": pd.Timestamp("2026-01-29"),
         "period_ending": pd.Timestamp("2025-12-01"),
         "report_time": "Close",
         "estimated_rev": 137808.72, "reported_rev": 143756.0,
         "surprise_rev": 5947.28, "surprise_rev_pct": 4.32},
    ]
    cutoff = pd.Timestamp("2020-01-01")
    out = zs._merge_and_filter(eps, rev, cutoff)
    assert len(out) == 1
    row = out[0]
    # All 11 spec fields present
    expected_fields = {
        "period_ending", "report_date", "report_time",
        "estimated_eps", "reported_eps", "surprise_eps", "surprise_eps_pct",
        "estimated_rev", "reported_rev", "surprise_rev", "surprise_rev_pct",
    }
    assert set(row.keys()) == expected_fields
    assert row["reported_eps"] == 2.84
    assert row["reported_rev"] == 143756.0


def test_merge_filter_drops_rows_before_cutoff():
    eps = [
        {"report_date": pd.Timestamp("2026-01-29"),
         "period_ending": pd.Timestamp("2025-12-01"),
         "report_time": "Close",
         "estimated_eps": 2.65, "reported_eps": 2.84,
         "surprise_eps": 0.19, "surprise_eps_pct": 7.17},
        {"report_date": pd.Timestamp("2018-01-29"),
         "period_ending": pd.Timestamp("2017-12-01"),
         "report_time": "Close",
         "estimated_eps": 1.0, "reported_eps": 1.1,
         "surprise_eps": 0.1, "surprise_eps_pct": 10.0},
    ]
    cutoff = pd.Timestamp("2021-01-01")
    out = zs._merge_and_filter(eps, [], cutoff)
    assert len(out) == 1
    assert out[0]["report_date"] == pd.Timestamp("2026-01-29")


def test_merge_filter_eps_only_fills_rev_with_none():
    """Spec §2.2: every row emits all 11 fields. Missing rev → None."""
    eps = [
        {"report_date": pd.Timestamp("2026-01-29"),
         "period_ending": pd.Timestamp("2025-12-01"),
         "report_time": "Close",
         "estimated_eps": 2.65, "reported_eps": 2.84,
         "surprise_eps": 0.19, "surprise_eps_pct": 7.17},
    ]
    out = zs._merge_and_filter(eps, [], pd.Timestamp("2020-01-01"))
    assert len(out) == 1
    row = out[0]
    assert row["estimated_rev"] is None
    assert row["reported_rev"] is None
    assert row["surprise_rev"] is None
    assert row["surprise_rev_pct"] is None


def test_merge_filter_returns_newest_first():
    eps = [
        {"report_date": pd.Timestamp(d), "period_ending": pd.Timestamp(d),
         "report_time": "Close", "estimated_eps": 1.0, "reported_eps": 1.0,
         "surprise_eps": 0.0, "surprise_eps_pct": 0.0}
        for d in ("2024-01-29", "2026-01-29", "2025-04-30")
    ]
    out = zs._merge_and_filter(eps, [], pd.Timestamp("2020-01-01"))
    dates = [r["report_date"] for r in out]
    assert dates == sorted(dates, reverse=True)
    assert dates[0] == pd.Timestamp("2026-01-29")


# ----------------------------------------------------------------------
# End-to-end: ZacksSession.fetch with mocked HTTP using the saved fixture
# ----------------------------------------------------------------------

class _FakeResp:
    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code


def test_fetch_against_aapl_fixture_returns_full_history():
    """End-to-end: a real Zacks AAPL page (cached as a fixture) → produces
    a full §2.2-shaped list with EPS+Rev populated."""
    html = _load_fixture()

    with patch("trade_scanner_fh.zacks_scraper.requests.Session") as mock_cls:
        sess = mock_cls.return_value
        sess.headers = {}  # supports .update()
        sess.get.return_value = _FakeResp(html)
        sess.close.return_value = None

        with zs.ZacksSession() as scraper:
            rows = scraper.fetch("AAPL", years=20)

    assert rows is not None
    assert len(rows) >= 20  # AAPL fixture has ~55 EPS quarters + 67 sales

    most_recent = rows[0]
    # All 11 spec fields present
    for f in (
        "period_ending", "report_date", "report_time",
        "estimated_eps", "reported_eps", "surprise_eps", "surprise_eps_pct",
        "estimated_rev", "reported_rev", "surprise_rev", "surprise_rev_pct",
    ):
        assert f in most_recent

    # Most recent has both EPS and revenue values
    assert most_recent["reported_eps"] is not None
    assert most_recent["reported_rev"] is not None
    assert most_recent["report_time"] in {"Open", "Close", "Market", "Unknown"}

    # The list is in newest-first order
    dates = [r["report_date"] for r in rows]
    assert dates == sorted(dates, reverse=True)


def test_fetch_returns_none_on_http_error():
    with patch("trade_scanner_fh.zacks_scraper.requests.Session") as mock_cls:
        sess = mock_cls.return_value
        sess.headers = {}
        sess.get.return_value = _FakeResp("", status_code=403)
        sess.close.return_value = None

        with zs.ZacksSession() as scraper:
            rows = scraper.fetch("BLOCKED")
    assert rows is None


def test_fetch_returns_none_when_obj_data_missing():
    """The Imperva interstitial is a small page with no obj_data assignment."""
    interstitial = "<html><head><title>Pardon Our Interruption</title></head></html>"
    with patch("trade_scanner_fh.zacks_scraper.requests.Session") as mock_cls:
        sess = mock_cls.return_value
        sess.headers = {}
        sess.get.return_value = _FakeResp(interstitial)
        sess.close.return_value = None

        with zs.ZacksSession() as scraper:
            rows = scraper.fetch("AAPL")
    assert rows is None


def test_fetch_outside_session_raises():
    """Calling fetch() before __enter__ should fail loudly — that's a bug,
    not a runtime data condition."""
    s = zs.ZacksSession()
    with pytest.raises(RuntimeError):
        s.fetch("AAPL")


# ----------------------------------------------------------------------
# fetch_earnings_history — the standalone wrapper
# ----------------------------------------------------------------------

def test_standalone_fetch_returns_none_on_failure():
    """The wrapper swallows all exceptions and returns None."""
    with patch.object(
        zs, "ZacksSession",
        side_effect=RuntimeError("simulated failure"),
    ):
        assert zs.fetch_earnings_history("AAPL") is None


# ----------------------------------------------------------------------
# Cookie injection (escape hatch for Imperva IP-flag situations)
# ----------------------------------------------------------------------

@pytest.fixture
def tmp_cookie_storage(tmp_path, monkeypatch):
    """Redirect the cookie-storage path to a tmp directory so tests don't
    touch the real scanner_data/zacks_cookies.txt."""
    monkeypatch.setattr(zs.config, "DATA_DIR", tmp_path)
    return tmp_path


def test_get_cookies_empty_returns_none(tmp_cookie_storage):
    assert zs.get_zacks_cookies() is None
    assert zs.has_zacks_cookies() is False


def test_set_get_cookies_round_trip(tmp_cookie_storage):
    cookies = "reese84=abc123; visid_incap_2944342=xyz789"
    assert zs.set_zacks_cookies(cookies) is True
    assert zs.get_zacks_cookies() == cookies
    assert zs.has_zacks_cookies() is True
    # Verify the file was actually written under the tmp dir
    assert (tmp_cookie_storage / "zacks_cookies.txt").exists()


def test_set_cookies_strips_whitespace(tmp_cookie_storage):
    zs.set_zacks_cookies("   reese84=abc123  \n")
    assert zs.get_zacks_cookies() == "reese84=abc123"


def test_set_empty_cookies_clears(tmp_cookie_storage):
    zs.set_zacks_cookies("reese84=abc123")
    assert zs.has_zacks_cookies()
    assert zs.set_zacks_cookies("") is True
    assert zs.get_zacks_cookies() is None
    # File should be deleted, not just emptied
    assert not (tmp_cookie_storage / "zacks_cookies.txt").exists()


def test_set_handles_long_cookie_blob(tmp_cookie_storage):
    """The original keyring backing tripped on Windows's ~1.5 KB credential
    size limit. File storage must handle ~2-4 KB blobs without issue."""
    # Simulate a real Imperva cookie blob (~2.5 KB)
    big = "; ".join(f"name{i}=" + ("X" * 50) for i in range(40))
    assert zs.set_zacks_cookies(big) is True
    assert zs.get_zacks_cookies() == big


def test_parse_cookie_string_basic():
    out = zs._parse_cookie_string("a=1; b=2; c=3")
    assert out == {"a": "1", "b": "2", "c": "3"}


def test_parse_cookie_string_handles_whitespace_and_newlines():
    raw = "  reese84=ABC ; visid_incap_2944342=DEF\nGHI=foo "
    out = zs._parse_cookie_string(raw)
    assert out == {"reese84": "ABC", "visid_incap_2944342": "DEF", "GHI": "foo"}


def test_parse_cookie_string_skips_malformed_pairs():
    """Pairs missing an '=' are silently skipped (don't crash on bad input)."""
    out = zs._parse_cookie_string("a=1; bad_no_equals; c=3; ")
    assert out == {"a": "1", "c": "3"}


def test_parse_cookie_string_empty():
    assert zs._parse_cookie_string("") == {}
    assert zs._parse_cookie_string("   ") == {}


def test_session_loads_cookies_from_storage_on_enter(tmp_cookie_storage):
    """When a cookie string is stored, ZacksSession.__enter__ pre-populates
    the requests session's cookie jar with each name=value pair."""
    zs.set_zacks_cookies("reese84=PROVE_IM_REAL; visid_incap_2944342=SESS")

    captured: dict[str, str] = {}

    class _FakeJar:
        def set(self, name, value, **kwargs):
            captured[name] = value

    class _FakeSession:
        def __init__(self):
            self.headers = {}
            self.cookies = _FakeJar()
        def close(self):
            pass

    with patch.object(zs.requests, "Session", return_value=_FakeSession()):
        with zs.ZacksSession() as s:
            assert captured == {
                "reese84": "PROVE_IM_REAL",
                "visid_incap_2944342": "SESS",
            }


def test_session_works_without_stored_cookies(tmp_cookie_storage):
    """No stored cookies → no calls to .cookies.set, session opens normally."""
    captured: list[tuple] = []

    class _FakeJar:
        def set(self, name, value, **kwargs):
            captured.append((name, value))

    class _FakeSession:
        def __init__(self):
            self.headers = {}
            self.cookies = _FakeJar()
        def close(self):
            pass

    with patch.object(zs.requests, "Session", return_value=_FakeSession()):
        with zs.ZacksSession() as s:
            assert captured == []
