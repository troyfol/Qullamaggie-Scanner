"""§11 Phase 3 manual smoke test: tiny 3-ticker live fill.

Runs bulk_fill_zacks against AAPL/MSFT/PLUG and verifies that:
  - earnings_history.parquet exists with all three tickers
  - sort order is (ticker ASC, period_ending DESC)
  - earnings_dates.parquet was reconciled
  - compute_consecutive_beats works on the live data

Uses the real cookies stored in scanner_data/zacks_cookies.txt — make sure
they're current before running. ASCII-only output to dodge cp1252 issues.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from trade_scanner_fh import config, earnings_history as eh
from trade_scanner_fh.earnings_cache import load_earnings_cache


TICKERS = ["AAPL", "MSFT", "PLUG"]


def main() -> int:
    config.ensure_dirs()

    print(f"=== Phase 3 live fill: {TICKERS} ===")
    print(f"earnings_history.parquet: {config.EARNINGS_HISTORY_PARQUET}")
    if config.EARNINGS_HISTORY_PARQUET.exists():
        print(f"  exists already, {config.EARNINGS_HISTORY_PARQUET.stat().st_size} bytes")
    else:
        print("  not yet written")

    filled, errors = eh.bulk_fill_zacks(
        TICKERS, blacklist=set(),
        delay_sec=1.5, flush_every=2,
    )
    print(f"\nresult: filled={filled}, errors={errors}\n")

    df = eh.load_earnings_history()
    if df is None or df.empty:
        print("FAIL: no rows in parquet")
        return 1

    print(f"=== earnings_history.parquet ({len(df)} total rows) ===")
    for t in TICKERS:
        sub = eh.get_ticker_history(t, df)
        if sub.empty:
            print(f"  {t}: 0 rows")
            continue
        most_recent = sub.iloc[0]
        print(
            f"  {t}: {len(sub)} rows; "
            f"most recent period_ending={most_recent['period_ending'].date()}, "
            f"report_date={most_recent['report_date'].date()}, "
            f"reported_eps={most_recent['reported_eps']}, "
            f"surprise_eps_pct={most_recent['surprise_eps_pct']}"
        )

    print("\n=== sort-order check (ticker ASC, period_ending DESC) ===")
    cur_ticker = None
    cur_period = None
    bad = 0
    for _, row in df.iterrows():
        t = row["ticker"]
        p = row["period_ending"]
        if t != cur_ticker:
            cur_ticker = t
            cur_period = p
            continue
        if p > cur_period:
            print(f"  ORDER ERROR: ticker={t} period {p} > {cur_period}")
            bad += 1
        cur_period = p
    print(f"  {bad} ordering errors out of {len(df)} rows")

    print("\n=== earnings_dates reconciliation ===")
    dates_df = load_earnings_cache()
    if dates_df is None:
        print("  FAIL: earnings_dates.parquet not written")
        return 1
    for t in TICKERS:
        rec = dates_df.loc[dates_df["ticker"] == t]
        if rec.empty:
            print(f"  {t}: not in earnings_dates.parquet")
        else:
            r = rec.iloc[0]
            print(f"  {t}: last_earnings={r['last_earnings']}, next_earnings={r['next_earnings']}")

    print("\n=== consecutive-beats demo ===")
    for t in TICKERS:
        sub = eh.get_ticker_history(t, df)
        if sub.empty:
            continue
        eps_streak = eh.compute_consecutive_beats(sub, "eps", 0.0)
        rev_streak = eh.compute_consecutive_beats(sub, "rev", 0.0)
        print(f"  {t}: EPS-beat streak (>0%) = {eps_streak}, REV-beat streak (>0%) = {rev_streak}")

    if bad > 0 or filled < len(TICKERS):
        return 1
    print("\nOVERALL: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
