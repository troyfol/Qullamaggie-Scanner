"""Tests for config.last_market_close — the helper behind the launch-time
OHLCV 'already current' gate. US-equity close is 16:00 ET; weekends walk
back to Friday; holidays are intentionally not modeled."""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from trade_scanner_fh import config

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def _close(now):
    return config.last_market_close(now)


# ──────────────────────────────────────────────────────────────────────
# Weekday cases
# ──────────────────────────────────────────────────────────────────────

def test_after_close_returns_same_day_close():
    # Wed 2026-05-27 18:00 ET → Wed 16:00 ET
    c = _close(datetime(2026, 5, 27, 18, 0, tzinfo=ET))
    assert (c.year, c.month, c.day, c.hour) == (2026, 5, 27, 16)


def test_before_close_returns_previous_session():
    # Wed 10:00 → Tue (prior weekday) 16:00
    c = _close(datetime(2026, 5, 27, 10, 0, tzinfo=ET))
    assert (c.month, c.day, c.hour) == (5, 26, 16)


def test_exactly_at_close_counts_as_closed():
    # now == close → that close (not the day before)
    c = _close(datetime(2026, 5, 27, 16, 0, tzinfo=ET))
    assert (c.month, c.day, c.hour) == (5, 27, 16)


def test_one_minute_before_close_uses_prior_session():
    c = _close(datetime(2026, 5, 27, 15, 59, tzinfo=ET))
    assert (c.month, c.day) == (5, 26)


# ──────────────────────────────────────────────────────────────────────
# Weekend walk-back
# ──────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("now", [
    datetime(2026, 5, 30, 12, 0, tzinfo=ET),  # Saturday
    datetime(2026, 5, 31, 12, 0, tzinfo=ET),  # Sunday
    datetime(2026, 6, 1, 9, 0, tzinfo=ET),    # Monday before close
])
def test_weekend_and_monday_premarket_walk_back_to_friday(now):
    c = _close(now)
    assert c.weekday() == 4  # Friday
    assert (c.month, c.day, c.hour) == (5, 29, 16)


def test_saturday_after_hours_still_friday():
    c = _close(datetime(2026, 5, 30, 20, 0, tzinfo=ET))
    assert (c.month, c.day) == (5, 29)


# ──────────────────────────────────────────────────────────────────────
# Timezone handling
# ──────────────────────────────────────────────────────────────────────

def test_utc_input_is_converted_to_et():
    # 2026-05-27 23:30 UTC == 19:30 ET (after close) → Wed 16:00 ET
    c = _close(datetime(2026, 5, 27, 23, 30, tzinfo=UTC))
    assert (c.month, c.day, c.hour) == (5, 27, 16)


def test_utc_input_before_et_close_uses_prior_session():
    # 2026-05-27 18:00 UTC == 14:00 ET (before close) → Tue 16:00 ET
    c = _close(datetime(2026, 5, 27, 18, 0, tzinfo=UTC))
    assert (c.month, c.day) == (5, 26)


def test_returns_tz_aware_in_market_tz():
    c = _close(datetime(2026, 5, 27, 18, 0, tzinfo=ET))
    assert c.tzinfo is not None
    assert c.utcoffset() == datetime(2026, 5, 27, tzinfo=ET).utcoffset()


def test_default_now_is_callable():
    # Smoke: no-arg call resolves to "now" without raising and returns a
    # weekday close in the past-or-present.
    c = config.last_market_close()
    assert c.weekday() < 5
    assert c.hour == config.MARKET_CLOSE_HOUR
