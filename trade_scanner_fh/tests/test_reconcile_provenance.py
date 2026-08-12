"""Reconcile provenance — audit 2026-08-12 (INT-4).

`earnings_dates.parquet` holds exactly one mutable row per ticker, but
`reconcile_earnings_dates` used it as a multi-source provenance store: it read
its inputs by exact `source ==` match out of the same rows whose label it then
rewrote. `finviz_fill` writes the forward date as `source="finviz"` (the
per-quarter history deliberately holds only PAST quarters), so pass #1
relabelled the row `finviz_derived` and pass #2 — finding no `source ==
"finviz"` row — NaT'd a date that exists nowhere else. The live store carried
the fingerprint on 1,500 of 1,505 `_derived` rows.

`last_source` / `next_source` are the immutable per-position origins the
reconciler now reads, so each pass is idempotent.

Note: the 1,500 dates already destroyed are NOT recoverable by any code fix —
they have to be re-fetched. These tests pin that no NEW loss occurs.
"""
from __future__ import annotations

import pandas as pd
import pytest

from trade_scanner_fh import earnings_cache as ec
from trade_scanner_fh import earnings_reconcile as er

TODAY = pd.Timestamp("2026-08-12")


def _hist(ticker="TESTA", source="finviz"):
    """One PAST quarter with a real announcement date."""
    return pd.DataFrame([{
        "ticker": ticker,
        "period_ending": pd.Timestamp("2026-06-30"),
        "report_date": pd.Timestamp("2026-07-10"),
        "reported_eps": 1.23,
        "source": source,
    }])


def _dates(**overrides):
    row = {
        "ticker": "TESTA",
        "last_earnings": pd.NaT,
        "next_earnings": pd.Timestamp("2026-09-15"),
        "updated_at": pd.Timestamp("2026-08-01"),
        "source": "finviz",
    }
    row.update(overrides)
    return pd.DataFrame([row])


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    monkeypatch.setattr(ec.config, "EARNINGS_PARQUET",
                        tmp_path / "earnings_dates.parquet")


def _reconcile_n(times, history_df, dates_df, tickers=("TESTA",)):
    """Run reconcile `times` times through the on-disk cache, like production
    does — the bug only appears on the SECOND pass."""
    ec.save_earnings_cache(dates_df)
    for _ in range(times):
        er.reconcile_earnings_dates(
            list(tickers), today=TODAY, history_df=history_df,
        )
    return ec.load_earnings_cache()


def test_finviz_forward_date_survives_repeated_reconcile():
    """The regression itself: three passes, date intact each time."""
    out = _reconcile_n(3, _hist(), _dates())
    row = out.loc[out.ticker == "TESTA"].iloc[0]
    assert row["next_earnings"] == pd.Timestamp("2026-09-15")
    assert row["last_earnings"] == pd.Timestamp("2026-07-10")


def test_reconcile_records_per_position_origins():
    out = _reconcile_n(1, _hist(), _dates())
    row = out.loc[out.ticker == "TESTA"].iloc[0]
    assert row["last_source"] == "finviz"
    assert row["next_source"] == "finviz"
    assert row["source"] == "finviz_derived"     # compound label unchanged


def test_reconcile_is_idempotent():
    """Pass 2 and pass 3 must produce the same dates as pass 1."""
    one = _reconcile_n(1, _hist(), _dates())
    three = _reconcile_n(3, _hist(), _dates())
    cols = ["ticker", "last_earnings", "next_earnings",
            "last_source", "next_source", "source"]
    pd.testing.assert_frame_equal(
        one[cols].reset_index(drop=True), three[cols].reset_index(drop=True),
    )


def test_nasdaq_attribution_is_not_laundered_into_yahoo():
    """A nasdaq-supplied date must keep naming nasdaq. Previously the yahoo
    bucket absorbed any label containing "yahoo", so a `nasdaq+yahoo_aug` row
    was re-read as yahoo data and relabelled `yahoo` — permanently erasing the
    nasdaq attribution."""
    dates = pd.DataFrame([{
        "ticker": "TESTB",
        "last_earnings": pd.Timestamp("2026-07-01"),
        "next_earnings": pd.Timestamp("2026-10-01"),
        "updated_at": pd.Timestamp("2026-08-01"),
        "source": "nasdaq",
    }])
    out = _reconcile_n(3, None, dates, tickers=("TESTB",))
    row = out.loc[out.ticker == "TESTB"].iloc[0]
    assert row["next_earnings"] == pd.Timestamp("2026-10-01")
    assert row["last_source"] == "nasdaq"
    assert row["next_source"] == "nasdaq"


def test_legacy_rows_without_position_columns_still_reconcile():
    """A pre-INT-4 row has no last_source/next_source. It must keep the old
    whole-row semantics rather than being dropped from every lookup."""
    dates = _dates()
    assert "last_source" not in dates.columns
    out = _reconcile_n(1, _hist(), dates)
    row = out.loc[out.ticker == "TESTA"].iloc[0]
    assert row["next_earnings"] == pd.Timestamp("2026-09-15")


def test_merge_does_not_let_a_null_date_erase_a_known_one():
    """`finviz_fill` writes last_earnings=NaT with the forward date, so a plain
    keep="last" wiped the last_earnings a nasdaq sweep had put there — and
    find_smart_refresh_candidates keys Rule B on last_earnings, so the refresh
    selector was degrading itself."""
    ec.save_earnings_cache(pd.DataFrame([{
        "ticker": "TESTC",
        "last_earnings": pd.Timestamp("2026-07-05"),
        "next_earnings": pd.NaT,
        "updated_at": pd.Timestamp("2026-08-01"),
        "source": "nasdaq",
    }]))
    ec._merge_and_save([{
        "ticker": "TESTC",
        "last_earnings": pd.NaT,
        "next_earnings": pd.Timestamp("2026-11-01"),
        "updated_at": pd.Timestamp("2026-08-10"),
        "source": "finviz",
    }], None)
    row = ec.load_earnings_cache().iloc[0]
    assert row["last_earnings"] == pd.Timestamp("2026-07-05")
    assert row["next_earnings"] == pd.Timestamp("2026-11-01")
    assert row["source"] == "finviz"      # newest row still owns identity


def test_merge_collapses_three_or_more_rows_per_ticker():
    """Each date falls back through ALL older rows, not just the immediately
    preceding one."""
    combined = pd.DataFrame([
        {"ticker": "T", "last_earnings": pd.Timestamp("2026-01-01"),
         "next_earnings": pd.NaT, "source": "a"},
        {"ticker": "T", "last_earnings": pd.NaT,
         "next_earnings": pd.Timestamp("2026-05-01"), "source": "b"},
        {"ticker": "T", "last_earnings": pd.NaT,
         "next_earnings": pd.NaT, "source": "c"},
    ])
    out = ec._collapse_by_ticker(combined)
    assert len(out) == 1
    assert out.iloc[0]["last_earnings"] == pd.Timestamp("2026-01-01")
    assert out.iloc[0]["next_earnings"] == pd.Timestamp("2026-05-01")
    assert out.iloc[0]["source"] == "c"


def test_merge_leaves_unduplicated_frames_alone():
    combined = pd.DataFrame([
        {"ticker": "A", "last_earnings": pd.Timestamp("2026-01-01"),
         "next_earnings": pd.NaT, "source": "x"},
        {"ticker": "B", "last_earnings": pd.NaT,
         "next_earnings": pd.Timestamp("2026-02-01"), "source": "y"},
    ])
    out = ec._collapse_by_ticker(combined)
    pd.testing.assert_frame_equal(out, combined.reset_index(drop=True))
