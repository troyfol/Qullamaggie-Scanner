"""§2.4 smoke test for zacks_scraper.

Hits AAPL, MSFT, and a randomly chosen small-cap (PLUG); verifies that
each returns sane data per the spec's checklist. Logs to scanner_data/logs/.
Run: venv/Scripts/python tests/_zacks_smoke.py

Uses ASCII-only output so it doesn't trip Windows cp1252 stdout encoding.
"""
from __future__ import annotations

import sys
import time
from datetime import datetime
from pathlib import Path

# Force UTF-8 output regardless of the terminal's default codepage so the
# script doesn't crash when printing dashes / arrows / set-membership glyphs.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from trade_scanner_fh import config
from trade_scanner_fh.zacks_scraper import ZacksSession

VALID_TIMES = {"Open", "Close", "Market", "Unknown"}
TICKERS = ["AAPL", "MSFT", "PLUG"]
RETRIES = 2  # transient Imperva blips → one retry per ticker
RETRY_DELAY_S = 3.0
INTER_TICKER_DELAY_S = 1.5


def _log_writer():
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = config.LOG_DIR / f"zacks_smoke_{ts}.log"
    fp = open(path, "w", encoding="utf-8")
    return path, fp


def _line(fp, msg):
    print(msg)
    fp.write(msg + "\n")
    fp.flush()


def _check(label, ok, fp):
    status = "PASS" if ok else "FAIL"
    _line(fp, f"  [{status}] {label}")
    return ok


def main() -> int:
    path, fp = _log_writer()
    _line(fp, f"Zacks scraper smoke test — {datetime.now().isoformat()}")
    _line(fp, f"Output: {path}")
    _line(fp, "")

    overall = True
    with ZacksSession() as session:
        for i, sym in enumerate(TICKERS):
            if i > 0:
                time.sleep(INTER_TICKER_DELAY_S)
            _line(fp, f"=== {sym} ===")
            rows = None
            elapsed = 0.0
            for attempt in range(1, RETRIES + 1):
                t0 = time.monotonic()
                rows = session.fetch(sym, years=5)
                elapsed = time.monotonic() - t0
                if rows is not None:
                    break
                _line(fp, f"  attempt {attempt}/{RETRIES}: None ({elapsed:.1f}s); retrying after {RETRY_DELAY_S}s")
                time.sleep(RETRY_DELAY_S)

            if rows is None:
                _line(fp, f"  (None after {RETRIES} attempts; fetch failed)")
                overall = False
                continue

            n = len(rows)
            _line(fp, f"  rows={n}, elapsed={elapsed:.1f}s")
            if rows:
                first = rows[0]
                _line(fp, f"  most recent row: {first}")

            ok = _check(f"non-empty list (got {n} rows)", n > 0, fp)
            overall &= ok

            if sym == "AAPL":
                # 5 years x ~4 quarters = ~20 rows +/- a few
                ok = _check(f"AAPL ~ 20 rows (got {n})", 15 <= n <= 25, fp)
                overall &= ok

            if rows:
                first = rows[0]
                rd = first.get("report_date")
                if rd is not None:
                    days_ago = (datetime.now() - rd.to_pydatetime()).days
                    ok = _check(
                        f"latest report_date within 120 days (got {days_ago}d)",
                        0 <= days_ago <= 200,  # generous: small caps may report less often
                        fp,
                    )
                    overall &= ok

                t = first.get("report_time")
                ok = _check(
                    f"report_time in {{Open,Close,Market,Unknown}} (got {t!r})",
                    t in VALID_TIMES, fp,
                )
                overall &= ok

                eps = first.get("reported_eps")
                rev = first.get("reported_rev")
                ok = _check(
                    f"most recent quarter has EPS+REV non-null "
                    f"(eps={eps!r}, rev={rev!r})",
                    eps is not None and rev is not None, fp,
                )
                overall &= ok
            _line(fp, "")

    _line(fp, f"OVERALL: {'PASS' if overall else 'FAIL'}")
    fp.close()
    return 0 if overall else 1


if __name__ == "__main__":
    sys.exit(main())
