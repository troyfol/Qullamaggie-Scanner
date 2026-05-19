"""Tests for earnings_reconcile.py — the Zacks-primary / Yahoo-secondary
unifier for earnings_dates.parquet."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest

from trade_scanner_fh import (
    earnings_cache as ec,
    earnings_history as eh,
    earnings_reconcile as er,
)


# ──────────────────────────────────────────────────────────────────────
# Fixtures + helpers
# ──────────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_parquets(tmp_path, monkeypatch):
    """Redirect both parquet paths into a tmp directory so reconciler
    tests don't touch the user's real cache."""
    monkeypatch.setattr(eh.config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(eh.config, "EARNINGS_HISTORY_PARQUET",
                        tmp_path / "earnings_history.parquet")
    monkeypatch.setattr(eh.config, "EARNINGS_PARQUET",
                        tmp_path / "earnings_dates.parquet")
    return tmp_path


def _hist_row(
    ticker: str, period_str: str, report_str: str, *,
    eps_est=2.0, eps_rep=2.1, source="zacks",
) -> dict:
    return {
        "ticker": ticker,
        "period_ending": pd.Timestamp(period_str),
        "report_date": pd.Timestamp(report_str),
        "report_time": "Close",
        "estimated_eps": eps_est, "reported_eps": eps_rep,
        "surprise_eps": 0.1, "surprise_eps_pct": 5.0,
        "estimated_rev": 100.0, "reported_rev": 105.0,
        "surprise_rev": 5.0, "surprise_rev_pct": 5.0,
        "source": source,
        "updated_at": pd.Timestamp(datetime.now()),
    }


def _dates_row(ticker: str, last: str | None, nxt: str | None,
               *, source: str | None = None) -> dict:
    out = {
        "ticker": ticker,
        "last_earnings": pd.Timestamp(last) if last else pd.NaT,
        "next_earnings": pd.Timestamp(nxt) if nxt else pd.NaT,
        "updated_at": pd.Timestamp(datetime(2026, 1, 1)),
    }
    if source is not None:
        out["source"] = source
    return out


# ──────────────────────────────────────────────────────────────────────
# Case 1 — Zacks present
# ──────────────────────────────────────────────────────────────────────

def test_zacks_only_writes_zacks_dates(tmp_parquets):
    eh.save_earnings_history(pd.DataFrame([
        _hist_row("AAPL", "2025-12-01", "2026-01-29"),
        _hist_row("AAPL", "2025-09-01", "2025-10-30"),
    ]))
    z, y, aug = er.reconcile_earnings_dates(
        ["AAPL"], today=pd.Timestamp("2026-04-30"),
    )
    assert (z, y, aug) == (1, 0, 0)

    df = ec.load_earnings_cache()
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    assert aapl["last_earnings"] == pd.Timestamp("2026-01-29")
    assert pd.isna(aapl["next_earnings"])


def test_zacks_with_future_quarter_sets_next(tmp_parquets):
    eh.save_earnings_history(pd.DataFrame([
        _hist_row("AAPL", "2026-03-01", "2026-05-15"),
        _hist_row("AAPL", "2025-12-01", "2026-01-29"),
    ]))
    z, y, aug = er.reconcile_earnings_dates(
        ["AAPL"], today=pd.Timestamp("2026-04-30"),
    )
    assert (z, y, aug) == (1, 0, 0)

    df = ec.load_earnings_cache()
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    assert aapl["last_earnings"] == pd.Timestamp("2026-01-29")
    assert aapl["next_earnings"] == pd.Timestamp("2026-05-15")


def test_zacks_next_wins_over_yahoo_next(tmp_parquets):
    """When Zacks has its OWN next_earnings, Yahoo's stays unused."""
    eh.save_earnings_history(pd.DataFrame([
        _hist_row("AAPL", "2026-03-01", "2026-05-15"),  # Zacks next
        _hist_row("AAPL", "2025-12-01", "2026-01-29"),
    ]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates_row("AAPL", "2026-01-29", "2026-06-01"),  # Yahoo had a different next
    ]))
    er.reconcile_earnings_dates(["AAPL"], today=pd.Timestamp("2026-04-30"))

    df = ec.load_earnings_cache()
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    assert aapl["next_earnings"] == pd.Timestamp("2026-05-15")  # Zacks wins


# ──────────────────────────────────────────────────────────────────────
# Case 4 — augmentation: Zacks-last + Yahoo-next when Zacks has no future
# ──────────────────────────────────────────────────────────────────────

def test_zacks_with_no_next_augments_with_yahoo(tmp_parquets):
    """Zacks doesn't predict the future. If Yahoo has a strictly-later
    next_earnings, keep it so Days Until ER still works."""
    eh.save_earnings_history(pd.DataFrame([
        _hist_row("AAPL", "2025-12-01", "2026-01-29"),
    ]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates_row("AAPL", "2026-01-29", "2026-05-01"),  # Yahoo had a future date
    ]))
    z, y, aug = er.reconcile_earnings_dates(
        ["AAPL"], today=pd.Timestamp("2026-04-30"),
    )
    assert (z, y, aug) == (0, 0, 1)

    df = ec.load_earnings_cache()
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    # last from Zacks; next from Yahoo augmentation
    assert aapl["last_earnings"] == pd.Timestamp("2026-01-29")
    assert aapl["next_earnings"] == pd.Timestamp("2026-05-01")


def test_no_augmentation_when_yahoo_next_is_before_zacks_last(tmp_parquets):
    """Stale Yahoo data (older than Zacks's last) is not used."""
    eh.save_earnings_history(pd.DataFrame([
        _hist_row("AAPL", "2025-12-01", "2026-01-29"),
    ]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates_row("AAPL", "2025-10-30", "2025-12-15"),  # both before Zacks last
    ]))
    z, y, aug = er.reconcile_earnings_dates(
        ["AAPL"], today=pd.Timestamp("2026-04-30"),
    )
    assert (z, y, aug) == (1, 0, 0)
    df = ec.load_earnings_cache()
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    assert pd.isna(aapl["next_earnings"])


def test_no_augmentation_when_yahoo_next_is_nat(tmp_parquets):
    """Yahoo row exists but its next_earnings is NaT → nothing to augment."""
    eh.save_earnings_history(pd.DataFrame([
        _hist_row("AAPL", "2025-12-01", "2026-01-29"),
    ]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates_row("AAPL", "2026-01-29", None),  # Yahoo had no next
    ]))
    z, y, aug = er.reconcile_earnings_dates(
        ["AAPL"], today=pd.Timestamp("2026-04-30"),
    )
    assert (z, y, aug) == (1, 0, 0)


# ──────────────────────────────────────────────────────────────────────
# Phase 1 regression — stale-Yahoo `next_earnings` must not persist past
# its actual date. Pre-Phase-1 the augment branch only checked
# `yhd_next > zacks_last`, so once a Yahoo `next_earnings` was captured
# it would carry forward forever — even after that date passed — until
# either Zacks coughed up a real future date or Yahoo got re-fetched.
# Now we also require `yhd_next > today`.
# ──────────────────────────────────────────────────────────────────────

def test_stale_yahoo_next_does_not_persist_after_passing(tmp_parquets):
    """today=2026-05-01. Zacks last = 2026-01-29 with no future. Yahoo's
    captured next_earnings = 2026-03-15 — strictly after Zacks's last,
    BUT in the past relative to today. Pre-fix: the augment branch
    would re-stamp 2026-03-15 forever. Post-fix: rejected → next is NaT.
    """
    eh.save_earnings_history(pd.DataFrame([
        _hist_row("AAPL", "2025-12-01", "2026-01-29"),
    ]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates_row("AAPL", "2026-01-29", "2026-03-15"),  # Yahoo next now in past
    ]))
    z, y, aug = er.reconcile_earnings_dates(
        ["AAPL"], today=pd.Timestamp("2026-05-01"),
    )
    assert aug == 0, "augment branch must reject a Yahoo next that is in the past"
    assert z == 1
    df = ec.load_earnings_cache()
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    assert pd.isna(aapl["next_earnings"])


def test_future_yahoo_next_still_augments_post_fix(tmp_parquets):
    """Positive-case companion to the regression test: a Yahoo next that
    IS in the future must still be picked up. Confirms the fix didn't
    over-correct and break the original augment behavior."""
    eh.save_earnings_history(pd.DataFrame([
        _hist_row("AAPL", "2025-12-01", "2026-01-29"),
    ]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates_row("AAPL", "2026-01-29", "2026-05-15"),  # in future
    ]))
    z, y, aug = er.reconcile_earnings_dates(
        ["AAPL"], today=pd.Timestamp("2026-05-01"),
    )
    assert (z, y, aug) == (0, 0, 1)
    df = ec.load_earnings_cache()
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    assert aapl["next_earnings"] == pd.Timestamp("2026-05-15")


# ──────────────────────────────────────────────────────────────────────
# Phase 1 — `source` column stamping in earnings_dates.parquet
# ──────────────────────────────────────────────────────────────────────

def test_zacks_derived_rows_get_source_label(tmp_parquets):
    eh.save_earnings_history(pd.DataFrame([
        _hist_row("AAPL", "2025-12-01", "2026-01-29"),
    ]))
    er.reconcile_earnings_dates(["AAPL"], today=pd.Timestamp("2026-04-30"))
    df = ec.load_earnings_cache()
    assert df.loc[df["ticker"] == "AAPL", "source"].iloc[0] == "zacks_derived"


def test_augmented_rows_get_zacks_yahoo_aug_source_label(tmp_parquets):
    eh.save_earnings_history(pd.DataFrame([
        _hist_row("AAPL", "2025-12-01", "2026-01-29"),
    ]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates_row("AAPL", "2026-01-29", "2026-05-15"),  # future Yahoo next
    ]))
    er.reconcile_earnings_dates(["AAPL"], today=pd.Timestamp("2026-04-30"))
    df = ec.load_earnings_cache()
    assert df.loc[df["ticker"] == "AAPL", "source"].iloc[0] == "zacks+yahoo_aug"


# ──────────────────────────────────────────────────────────────────────
# Case 2 — Yahoo only, no Zacks
# ──────────────────────────────────────────────────────────────────────

def test_yahoo_only_row_is_preserved(tmp_parquets):
    """Tickers Zacks doesn't cover keep their existing Yahoo row.
    Uses a future next_earnings so the Phase 4 ``> today`` filter
    doesn't clear it — the test's intent is yahoo passthrough, not
    stale-date preservation (the Phase 1 stale-Yahoo regression test
    covers the latter)."""
    ec.save_earnings_cache(pd.DataFrame([
        _dates_row("OBSCURE", "2026-01-15", "2026-05-15"),
    ]))
    z, y, aug = er.reconcile_earnings_dates(
        ["OBSCURE"], today=pd.Timestamp("2026-04-30"),
    )
    assert (z, y, aug) == (0, 1, 0)
    df = ec.load_earnings_cache()
    row = df.loc[df["ticker"] == "OBSCURE"].iloc[0]
    assert row["last_earnings"] == pd.Timestamp("2026-01-15")
    assert row["next_earnings"] == pd.Timestamp("2026-05-15")


# ──────────────────────────────────────────────────────────────────────
# affected_tickers semantics
# ──────────────────────────────────────────────────────────────────────

def test_affected_tickers_does_not_touch_other_rows(tmp_parquets):
    """Tickers outside `affected_tickers` are untouched (case 2-style
    preservation), even when those tickers ALSO exist in earnings_history."""
    eh.save_earnings_history(pd.DataFrame([
        _hist_row("AAPL", "2025-12-01", "2026-01-29"),
        _hist_row("MSFT", "2025-12-01", "2026-01-28"),
    ]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates_row("MSFT", "2025-10-15", "2026-01-15"),  # stale data
        _dates_row("Y", "2025-12-01", "2026-04-01"),
    ]))
    er.reconcile_earnings_dates(["AAPL"], today=pd.Timestamp("2026-04-30"))

    df = ec.load_earnings_cache()
    # AAPL got a new row
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    assert aapl["last_earnings"] == pd.Timestamp("2026-01-29")
    # MSFT was NOT in affected_tickers → its stale row is preserved as-is
    msft = df.loc[df["ticker"] == "MSFT"].iloc[0]
    assert msft["last_earnings"] == pd.Timestamp("2025-10-15")
    assert msft["next_earnings"] == pd.Timestamp("2026-01-15")
    # Y row unchanged
    y = df.loc[df["ticker"] == "Y"].iloc[0]
    assert y["next_earnings"] == pd.Timestamp("2026-04-01")


def test_full_sweep_when_affected_is_none(tmp_parquets):
    """affected_tickers=None reconciles every ticker known to either parquet."""
    eh.save_earnings_history(pd.DataFrame([
        _hist_row("AAPL", "2025-12-01", "2026-01-29"),
        _hist_row("MSFT", "2025-12-01", "2026-01-28"),
    ]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates_row("OBSCURE", "2026-01-15", "2026-04-15"),
    ]))

    z, y, aug = er.reconcile_earnings_dates(
        affected_tickers=None, today=pd.Timestamp("2026-04-30"),
    )
    assert z == 2  # AAPL + MSFT from Zacks
    assert y == 1  # OBSCURE preserved
    assert aug == 0

    df = ec.load_earnings_cache()
    assert set(df["ticker"]) == {"AAPL", "MSFT", "OBSCURE"}


def test_reconcile_empty_input_no_crash(tmp_parquets):
    """Both parquets empty → no error, no rows written, all counts 0."""
    z, y, aug = er.reconcile_earnings_dates(
        affected_tickers=None, today=pd.Timestamp("2026-04-30"),
    )
    assert (z, y, aug) == (0, 0, 0)
    assert ec.load_earnings_cache() is None


def test_reconcile_unknown_ticker_skipped(tmp_parquets):
    """A ticker in affected_tickers but absent from both parquets is just skipped."""
    eh.save_earnings_history(pd.DataFrame([
        _hist_row("AAPL", "2025-12-01", "2026-01-29"),
    ]))
    z, y, aug = er.reconcile_earnings_dates(
        ["AAPL", "GHOST"], today=pd.Timestamp("2026-04-30"),
    )
    assert z == 1 and y == 0 and aug == 0
    df = ec.load_earnings_cache()
    assert "GHOST" not in set(df["ticker"])


# Phase 6.5: `fill_yahoo_gaps` was removed (functionally equivalent to
# the menu-driven Targeted Fill Earnings Dates (Yahoo) action after
# Phase 4/5.5 made every fill auto-reconcile). The 3 tests that
# patched `fill_yahoo_gaps` were deleted alongside it.


# ──────────────────────────────────────────────────────────────────────
# Integration: a Zacks fill + Yahoo prior data correctly augments
# ──────────────────────────────────────────────────────────────────────

def test_integration_yahoo_data_then_zacks_fill_keeps_yahoo_next(tmp_parquets):
    """Real-world flow:
      1. User runs Yahoo bulk fill → earnings_dates has Yahoo last+next.
      2. User runs Zacks bulk fill → earnings_history populated.
      3. Reconcile (auto-triggered after Zacks flush) → keeps Yahoo's
         next because Zacks doesn't predict the future."""
    # Step 1 — pre-existing Yahoo data
    ec.save_earnings_cache(pd.DataFrame([
        _dates_row("AAPL", "2026-01-29", "2026-05-01"),  # Yahoo predicted next
    ]))

    # Step 2 — Zacks fill writes to earnings_history (only past quarters)
    eh.save_earnings_history(pd.DataFrame([
        _hist_row("AAPL", "2025-12-01", "2026-01-29"),  # this matches Yahoo's last
        _hist_row("AAPL", "2025-09-01", "2025-10-30"),
    ]))

    # Step 3 — reconcile (what bulk_fill_zacks's flush would do)
    z, y, aug = er.reconcile_earnings_dates(
        ["AAPL"], today=pd.Timestamp("2026-04-30"),
    )
    assert (z, y, aug) == (0, 0, 1)  # augmented

    df = ec.load_earnings_cache()
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    assert aapl["last_earnings"] == pd.Timestamp("2026-01-29")  # from Zacks
    assert aapl["next_earnings"] == pd.Timestamp("2026-05-01")  # from Yahoo augmentation


# ──────────────────────────────────────────────────────────────────────
# Phase 4 — multi-source priority chain
# ──────────────────────────────────────────────────────────────────────

def _finn_row(ticker, period, report, *, proxy=False):
    """Build a Finnhub-source history row. ``proxy=False`` means the
    row carries a real announcement date (came from /calendar/earnings
    join); proxy=True means report_date is a period_ending stand-in
    and must NOT be promoted to next_earnings by the reconciler."""
    return {
        "ticker": ticker,
        "period_ending": pd.Timestamp(period),
        "report_date": pd.Timestamp(report),
        "report_time": "Unknown",
        "estimated_eps": 1.9, "reported_eps": 2.0,
        "surprise_eps": 0.1, "surprise_eps_pct": 5.0,
        "estimated_rev": 100.0, "reported_rev": 105.0,
        "surprise_rev": 5.0, "surprise_rev_pct": 5.0,
        "source": "finnhub",
        "updated_at": pd.Timestamp(datetime.now()),
        "report_date_proxy": proxy,
    }


def test_phase4_zacks_history_beats_finnhub_history_for_last(tmp_parquets):
    """When both Zacks and Finnhub history have past rows for the same
    ticker, Zacks wins on last_earnings (priority chain)."""
    eh.save_earnings_history(pd.DataFrame([
        _hist_row("AAPL", "2025-12-01", "2026-01-29", source="zacks"),
        _finn_row("AAPL", "2025-12-31", "2026-01-30"),  # 1 day off
    ]))
    z, y, aug = er.reconcile_earnings_dates(
        ["AAPL"], today=pd.Timestamp("2026-04-30"),
    )
    df = ec.load_earnings_cache()
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    assert aapl["last_earnings"] == pd.Timestamp("2026-01-29")  # Zacks wins
    assert aapl["source"] == "zacks_derived"
    assert (z, y, aug) == (1, 0, 0)


def test_phase4_finnhub_history_alone_emits_finnhub_derived(tmp_parquets):
    """Ticker covered only by Finnhub → finnhub_derived label, counts
    as 'z' in the legacy counter (any history source = z)."""
    eh.save_earnings_history(pd.DataFrame([
        _finn_row("AAPL", "2025-12-31", "2026-01-29"),
    ]))
    z, y, aug = er.reconcile_earnings_dates(
        ["AAPL"], today=pd.Timestamp("2026-04-30"),
    )
    df = ec.load_earnings_cache()
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    assert aapl["last_earnings"] == pd.Timestamp("2026-01-29")
    assert aapl["source"] == "finnhub_derived"
    assert (z, y, aug) == (1, 0, 0)


def test_phase4_finnhub_proxy_row_excluded_from_next(tmp_parquets):
    """Finnhub future rows with report_date_proxy=True are
    period_ending stand-ins, not real announcements — must NOT be
    promoted to next_earnings."""
    eh.save_earnings_history(pd.DataFrame([
        _finn_row("AAPL", "2025-12-31", "2026-01-29", proxy=False),
        # Future row but only proxy — period_ending used as report_date
        _finn_row("AAPL", "2026-06-30", "2026-06-30", proxy=True),
    ]))
    z, y, aug = er.reconcile_earnings_dates(
        ["AAPL"], today=pd.Timestamp("2026-04-30"),
    )
    df = ec.load_earnings_cache()
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    # next_earnings must be NaT — proxy rows excluded from future-set
    assert pd.isna(aapl["next_earnings"])


def test_phase4_finnhub_real_future_row_used_as_next(tmp_parquets):
    """The proxy=False counterpart of the previous test: a Finnhub
    future row WITH a real announcement date is promoted to
    next_earnings."""
    eh.save_earnings_history(pd.DataFrame([
        _finn_row("AAPL", "2025-12-31", "2026-01-29", proxy=False),
        _finn_row("AAPL", "2026-03-31", "2026-05-15", proxy=False),
    ]))
    z, y, aug = er.reconcile_earnings_dates(
        ["AAPL"], today=pd.Timestamp("2026-04-30"),
    )
    df = ec.load_earnings_cache()
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    assert aapl["next_earnings"] == pd.Timestamp("2026-05-15")
    assert aapl["source"] == "finnhub_derived"


def test_phase4_nasdaq_beats_finnhub_under_demoted_priority(tmp_parquets):
    """Post-binary-policy: Finnhub is demoted to last in the priority
    chain. When Nasdaq has any data for a ticker, Finnhub history is
    bypassed entirely — even for last_earnings. Result is a clean
    'nasdaq' label (both positions from Nasdaq), not the old
    'finnhub+nasdaq_aug' mix."""
    eh.save_earnings_history(pd.DataFrame([
        _finn_row("AAPL", "2025-12-31", "2026-01-29"),
    ]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates_row("AAPL", "2026-01-29", "2026-05-15", source="nasdaq"),
    ]))
    z, y, aug = er.reconcile_earnings_dates(
        ["AAPL"], today=pd.Timestamp("2026-04-30"),
    )
    df = ec.load_earnings_cache()
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    assert aapl["last_earnings"] == pd.Timestamp("2026-01-29")  # nasdaq
    assert aapl["next_earnings"] == pd.Timestamp("2026-05-15")  # nasdaq
    assert aapl["source"] == "nasdaq"
    assert (z, y, aug) == (0, 1, 0)  # both positions from dates → y


def test_phase4_finnhub_used_only_when_no_other_source(tmp_parquets):
    """Finnhub history fills in only when no Zacks/Nasdaq/Yahoo source
    covers the ticker — last-resort priority."""
    eh.save_earnings_history(pd.DataFrame([
        _finn_row("LONELY", "2025-12-31", "2026-01-29"),
    ]))
    # No earnings_dates entries for LONELY at all.
    er.reconcile_earnings_dates(
        ["LONELY"], today=pd.Timestamp("2026-04-30"),
    )
    df = ec.load_earnings_cache()
    row = df.loc[df["ticker"] == "LONELY"].iloc[0]
    assert row["last_earnings"] == pd.Timestamp("2026-01-29")
    assert row["source"] == "finnhub_derived"


def test_phase4_nasdaq_beats_yahoo_for_next(tmp_parquets):
    """When both Nasdaq and Yahoo have a future next_earnings, Nasdaq
    wins (priority chain order)."""
    ec.save_earnings_cache(pd.DataFrame([
        _dates_row("AAPL", "2026-01-15", "2026-05-15", source="nasdaq"),
        _dates_row("AAPL", "2026-01-15", "2026-06-01", source="yahoo"),
    ]))
    er.reconcile_earnings_dates(["AAPL"], today=pd.Timestamp("2026-04-30"))
    df = ec.load_earnings_cache()
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    assert aapl["next_earnings"] == pd.Timestamp("2026-05-15")  # Nasdaq
    assert aapl["source"] == "nasdaq"


def test_phase4_yahoo_only_counts_as_y(tmp_parquets):
    """Pure Yahoo passthrough → y counter."""
    ec.save_earnings_cache(pd.DataFrame([
        _dates_row("OBSCURE", "2026-01-15", "2026-05-15", source="yahoo"),
    ]))
    z, y, aug = er.reconcile_earnings_dates(
        ["OBSCURE"], today=pd.Timestamp("2026-04-30"),
    )
    assert (z, y, aug) == (0, 1, 0)
    df = ec.load_earnings_cache()
    row = df.loc[df["ticker"] == "OBSCURE"].iloc[0]
    assert row["source"] == "yahoo"


def test_phase4_nasdaq_yahoo_aug_label(tmp_parquets):
    """last from one dates source + next from the other → mixed-dates
    aug label. Nasdaq supplies last; Yahoo supplies next."""
    ec.save_earnings_cache(pd.DataFrame([
        _dates_row("AAPL", "2026-01-29", None, source="nasdaq"),  # last only
        _dates_row("AAPL", None, "2026-05-15", source="yahoo"),   # next only
    ]))
    er.reconcile_earnings_dates(["AAPL"], today=pd.Timestamp("2026-04-30"))
    df = ec.load_earnings_cache()
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    assert aapl["last_earnings"] == pd.Timestamp("2026-01-29")  # nasdaq
    assert aapl["next_earnings"] == pd.Timestamp("2026-05-15")  # yahoo
    assert aapl["source"] == "nasdaq+yahoo_aug"


def test_phase4_stale_next_filter_applies_to_all_sources(tmp_parquets):
    """A past next_earnings from ANY source must be cleared, not just
    yahoo. Generalization of the Phase 1 stale-Yahoo bug fix."""
    ec.save_earnings_cache(pd.DataFrame([
        # Both nasdaq and yahoo have stale (past) nexts
        _dates_row("X", "2026-01-15", "2026-03-15", source="nasdaq"),
        _dates_row("X", "2026-01-15", "2026-03-15", source="yahoo"),
    ]))
    er.reconcile_earnings_dates(["X"], today=pd.Timestamp("2026-05-01"))
    df = ec.load_earnings_cache()
    x = df.loc[df["ticker"] == "X"].iloc[0]
    assert pd.isna(x["next_earnings"])  # stale cleared


def test_phase4_no_data_anywhere_skips_ticker(tmp_parquets):
    """A ticker in affected_tickers with no data anywhere returns
    empty counts and writes no row."""
    z, y, aug = er.reconcile_earnings_dates(
        ["GHOST"], today=pd.Timestamp("2026-04-30"),
    )
    assert (z, y, aug) == (0, 0, 0)
    df = ec.load_earnings_cache()
    assert df is None or df.empty


def test_phase4_skips_finnhub_proxy_row_in_chain(tmp_parquets):
    """When Finnhub-only ticker has all proxy rows, last_earnings comes
    from past period_ending values (proxy=True is OK for past) but
    next_earnings stays NaT (proxy excluded from future chain)."""
    eh.save_earnings_history(pd.DataFrame([
        _finn_row("AAPL", "2025-12-31", "2025-12-31", proxy=True),
    ]))
    er.reconcile_earnings_dates(["AAPL"], today=pd.Timestamp("2026-04-30"))
    df = ec.load_earnings_cache()
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    # Past proxy row promotes its report_date to last_earnings
    assert aapl["last_earnings"] == pd.Timestamp("2025-12-31")
    # No future row → next is NaT
    assert pd.isna(aapl["next_earnings"])


def test_phase4_full_universe_sweep_finds_all_known_tickers(tmp_parquets):
    """affected_tickers=None → reconciler considers every ticker in
    every lookup, not just one source."""
    eh.save_earnings_history(pd.DataFrame([
        _hist_row("AAPL", "2025-12-01", "2026-01-29", source="zacks"),
        _finn_row("MSFT", "2025-12-31", "2026-01-28"),
    ]))
    ec.save_earnings_cache(pd.DataFrame([
        _dates_row("NVDA", "2026-01-15", "2026-05-01", source="nasdaq"),
        _dates_row("GOOG", "2026-01-30", "2026-05-10", source="yahoo"),
    ]))
    er.reconcile_earnings_dates(today=pd.Timestamp("2026-04-30"))
    df = ec.load_earnings_cache()
    assert set(df["ticker"]) == {"AAPL", "MSFT", "NVDA", "GOOG"}


# ──────────────────────────────────────────────────────────────────────
# Audit-fix integration: a real Zacks → Nasdaq sequence must NOT lose
# the Zacks-derived consolidation. Pre-fix, a Nasdaq fill would
# overwrite the zacks_derived dates_df row by-ticker (single-row PK)
# and the Days-Since-ER filter would then read Nasdaq's date instead
# of Zacks's announcement date.
# ──────────────────────────────────────────────────────────────────────

def test_audit_fix_zacks_then_nasdaq_round_trip_keeps_zacks_dates(tmp_parquets, monkeypatch):
    """Step 1: Zacks fill writes history rows + reconciler emits
    zacks_derived dates row. Step 2: Nasdaq fill writes a nasdaq row
    for the same ticker — but its post-write reconcile re-derives the
    zacks_derived row, restoring Zacks's authoritative announcement
    date. The Days-Since-ER filter sees the zacks_derived value either
    way."""
    from trade_scanner_fh import nasdaq_fill
    # Step 1 — Zacks history present, reconcile to emit zacks_derived row.
    eh.save_earnings_history(pd.DataFrame([
        _hist_row("AAPL", "2025-12-01", "2026-01-29", source="zacks"),
    ]))
    er.reconcile_earnings_dates(["AAPL"], today=pd.Timestamp("2026-04-30"))
    df = ec.load_earnings_cache()
    assert df.loc[df["ticker"] == "AAPL", "source"].iloc[0] == "zacks_derived"

    # Step 2 — Nasdaq fill targeting AAPL with a calendar-derived date
    # (different from the Zacks announcement date so we can detect
    # which one wins). The post-fill reconcile must re-derive the
    # zacks_derived label.
    from datetime import date
    today = date.today()

    def fake_get_earnings_by_date(d):
        if d == today:
            return pd.DataFrame(
                {"_placeholder": [None]},
                index=pd.Index(["AAPL"], name="symbol"),
            )
        return pd.DataFrame()

    import finance_calendars.finance_calendars as fc
    monkeypatch.setattr(fc, "get_earnings_by_date", fake_get_earnings_by_date)
    monkeypatch.setattr(nasdaq_fill.time, "sleep", lambda *_: None)

    nasdaq_fill.bulk_fill_nasdaq(
        ["AAPL"], blacklist=set(), days_back=2, days_forward=2, delay=0,
    )

    df = ec.load_earnings_cache()
    aapl = df.loc[df["ticker"] == "AAPL"].iloc[0]
    # Audit assertion: the Zacks announcement date wins, NOT today's
    # Nasdaq calendar entry. Source is zacks_derived (or zacks+nasdaq_aug
    # if Nasdaq had a future date, but it doesn't here).
    assert aapl["last_earnings"] == pd.Timestamp("2026-01-29"), (
        "Zacks's authoritative announcement date must survive the "
        "Nasdaq fill — that's the whole point of the post-fill reconcile."
    )
    assert "zacks" in str(aapl["source"]).lower()
