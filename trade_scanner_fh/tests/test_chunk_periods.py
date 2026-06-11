"""Tests for scanner.chunk_periods — Sequenced Run period chunker —
and for the SequencedRunDialog quick-select controls."""
from datetime import date

import pytest

from trade_scanner_fh.scanner import chunk_periods


def test_sequenced_run_dialog_today_button_sets_end_date(_qapp):
    """The "Today" quick-select next to End snaps the end-date input
    to today's date in one click."""
    from datetime import date as _date
    from PyQt6.QtCore import QDate
    from PyQt6.QtWidgets import QPushButton
    from trade_scanner_fh.gui.dialogs import SequencedRunDialog

    dlg = SequencedRunDialog(
        default_start=_date(2025, 1, 1),
        default_end=_date(2025, 6, 1),
    )
    # End starts at the default value
    assert dlg._date_end.date() == QDate(2025, 6, 1)

    # Find the Today button by text
    today_btns = [
        b for b in dlg.findChildren(QPushButton) if b.text() == "Today"
    ]
    assert len(today_btns) == 1, "Today quick-select button should exist exactly once"
    today_btns[0].click()

    # End should now be today's date.
    expected = QDate.currentDate()
    assert dlg._date_end.date() == expected, (
        f"Expected end={expected.toString('yyyy-MM-dd')}, "
        f"got {dlg._date_end.date().toString('yyyy-MM-dd')}"
    )


def test_sequenced_run_dialog_today_button_does_not_touch_start(_qapp):
    """Clicking Today affects ONLY the End date — Start stays put."""
    from datetime import date as _date
    from PyQt6.QtCore import QDate
    from PyQt6.QtWidgets import QPushButton
    from trade_scanner_fh.gui.dialogs import SequencedRunDialog

    dlg = SequencedRunDialog(
        default_start=_date(2025, 1, 1),
        default_end=_date(2025, 6, 1),
    )
    today_btns = [
        b for b in dlg.findChildren(QPushButton) if b.text() == "Today"
    ]
    today_btns[0].click()
    # Start unchanged
    assert dlg._date_start.date() == QDate(2025, 1, 1)


# ──────────────────────────────────────────────────────────────────────
# User-spec example: 1-year range, 2-month chunks, even split → 6 chunks
# ──────────────────────────────────────────────────────────────────────

def test_user_example_two_month_chunks_over_calendar_year():
    chunks = chunk_periods(date(2025, 1, 1), date(2025, 12, 31), 2, "months")
    assert chunks == [
        (date(2025, 11, 1), date(2025, 12, 31)),
        (date(2025, 9, 1),  date(2025, 10, 31)),
        (date(2025, 7, 1),  date(2025, 8, 31)),
        (date(2025, 5, 1),  date(2025, 6, 30)),
        (date(2025, 3, 1),  date(2025, 4, 30)),
        (date(2025, 1, 1),  date(2025, 2, 28)),
    ]


# ──────────────────────────────────────────────────────────────────────
# Uneven divide — leftover oldest chunk is shorter (option (b))
# ──────────────────────────────────────────────────────────────────────

def test_uneven_divide_leftover_runs_as_smaller_chunk():
    chunks = chunk_periods(date(2025, 1, 1), date(2025, 12, 31), 5, "months")
    # 12 / 5 = 2 full chunks (Aug-Dec, Mar-Jul) + leftover Jan-Feb (2 months)
    assert chunks == [
        (date(2025, 8, 1),  date(2025, 12, 31)),
        (date(2025, 3, 1),  date(2025, 7, 31)),
        (date(2025, 1, 1),  date(2025, 2, 28)),
    ]


# ──────────────────────────────────────────────────────────────────────
# Days
# ──────────────────────────────────────────────────────────────────────

def test_days_chunks_walk_backwards_from_end():
    chunks = chunk_periods(date(2025, 1, 1), date(2025, 1, 31), 7, "days")
    # 31 days / 7 = 4 full chunks + 3-day leftover
    assert chunks == [
        (date(2025, 1, 25), date(2025, 1, 31)),  # 7 days
        (date(2025, 1, 18), date(2025, 1, 24)),  # 7 days
        (date(2025, 1, 11), date(2025, 1, 17)),  # 7 days
        (date(2025, 1, 4),  date(2025, 1, 10)),  # 7 days
        (date(2025, 1, 1),  date(2025, 1, 3)),   # 3-day leftover
    ]


def test_days_chunks_evenly_divide():
    chunks = chunk_periods(date(2025, 1, 1), date(2025, 1, 28), 7, "days")
    assert chunks == [
        (date(2025, 1, 22), date(2025, 1, 28)),
        (date(2025, 1, 15), date(2025, 1, 21)),
        (date(2025, 1, 8),  date(2025, 1, 14)),
        (date(2025, 1, 1),  date(2025, 1, 7)),
    ]


# ──────────────────────────────────────────────────────────────────────
# Weeks
# ──────────────────────────────────────────────────────────────────────

def test_weeks_chunks_two_weeks():
    chunks = chunk_periods(date(2025, 1, 1), date(2025, 1, 28), 2, "weeks")
    # 28 days / 14 = 2 full chunks of 14 days
    assert chunks == [
        (date(2025, 1, 15), date(2025, 1, 28)),
        (date(2025, 1, 1),  date(2025, 1, 14)),
    ]


# ──────────────────────────────────────────────────────────────────────
# Cross-year months (boundary handling)
# ──────────────────────────────────────────────────────────────────────

def test_months_crossing_year_boundary():
    """Range Nov 2024 → Feb 2025 with 3-month chunks: a single 4-month
    span split into one 3-month chunk + 1-month leftover that happens to
    cross years."""
    chunks = chunk_periods(date(2024, 11, 1), date(2025, 2, 28), 3, "months")
    assert chunks == [
        (date(2024, 12, 1), date(2025, 2, 28)),  # Dec 2024 → Feb 2025
        (date(2024, 11, 1), date(2024, 11, 30)), # leftover Nov 2024
    ]


# ──────────────────────────────────────────────────────────────────────
# Edge cases
# ──────────────────────────────────────────────────────────────────────

def test_start_after_end_returns_empty():
    assert chunk_periods(date(2025, 12, 31), date(2025, 1, 1), 1, "months") == []


def test_single_day_range():
    chunks = chunk_periods(date(2025, 6, 15), date(2025, 6, 15), 7, "days")
    assert chunks == [(date(2025, 6, 15), date(2025, 6, 15))]


def test_chunk_smaller_than_range_in_days():
    chunks = chunk_periods(date(2025, 6, 1), date(2025, 6, 30), 1, "days")
    assert len(chunks) == 30
    assert chunks[0] == (date(2025, 6, 30), date(2025, 6, 30))
    assert chunks[-1] == (date(2025, 6, 1), date(2025, 6, 1))


def test_invalid_unit_raises():
    with pytest.raises(ValueError):
        chunk_periods(date(2025, 1, 1), date(2025, 12, 31), 2, "fortnights")


def test_zero_chunk_size_raises():
    with pytest.raises(ValueError):
        chunk_periods(date(2025, 1, 1), date(2025, 12, 31), 0, "months")
