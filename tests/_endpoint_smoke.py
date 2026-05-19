"""One-off endpoint diagnostic. Not part of the regular pytest suite —
exercises every external data source the scanner depends on so we can
verify each is currently reachable and returning sane data.

Run: venv/Scripts/python tests/_endpoint_smoke.py
"""
from __future__ import annotations

import sys
import time
import traceback
from datetime import date, timedelta
from pathlib import Path

# Make trade_scanner_fh importable when running directly from the repo
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))


def _sec(t0: float) -> str:
    return f"{time.monotonic() - t0:.2f}s"


def _check(name: str, fn) -> dict:
    print(f"\n[{name}] ...", flush=True)
    t0 = time.monotonic()
    try:
        result = fn()
        elapsed = _sec(t0)
        print(f"  OK ({elapsed}) — {result}")
        return {"name": name, "ok": True, "elapsed": elapsed, "info": result}
    except Exception as exc:
        elapsed = _sec(t0)
        print(f"  FAIL ({elapsed}) — {type(exc).__name__}: {exc}")
        traceback.print_exc(limit=2)
        return {"name": name, "ok": False, "elapsed": elapsed,
                "error": f"{type(exc).__name__}: {exc}"}


def check_nasdaq_ftp():
    import ftplib
    import io
    buf = io.BytesIO()
    ftp = ftplib.FTP("ftp.nasdaqtrader.com", timeout=20)
    try:
        ftp.login("anonymous", "")
        ftp.cwd("SymbolDirectory")
        ftp.retrbinary("RETR nasdaqlisted.txt", buf.write)
    finally:
        try:
            ftp.quit()
        except Exception:
            pass
    raw = buf.getvalue().decode("utf-8", errors="replace")
    lines = raw.strip().splitlines()
    return f"{len(raw):,} bytes, {len(lines):,} lines"


def check_github_tickers():
    import requests
    from trade_scanner_fh import config
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(config.GITHUB_TICKERS_URL, headers=headers, timeout=20)
    r.raise_for_status()
    syms = [s.strip() for s in r.text.splitlines() if s.strip()]
    return f"HTTP {r.status_code}, {len(syms):,} symbols"


def check_sec_edgar():
    import requests
    from trade_scanner_fh import config
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }
    r = requests.get(config.SEC_TICKERS_URL, headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json()
    return f"HTTP {r.status_code}, {len(data):,} entries"


def check_yfinance_history():
    import yfinance as yf
    df = yf.Ticker("AAPL").history(period="5d", auto_adjust=True)
    if df is None or df.empty:
        raise RuntimeError("empty DataFrame from yf.Ticker.history")
    return f"AAPL 5d: {len(df)} rows, latest close ${df['Close'].iloc[-1]:.2f}"


def check_yfinance_info():
    import yfinance as yf
    info = yf.Ticker("AAPL").info
    sector = info.get("sector") or "(none)"
    industry = info.get("industry") or "(none)"
    return f"AAPL .info: sector={sector!r}, industry={industry!r}"


def check_yfinance_earnings_dates():
    import yfinance as yf
    ed = yf.Ticker("AAPL").earnings_dates
    if ed is None or ed.empty:
        return "AAPL .earnings_dates returned empty (None or 0 rows)"
    return f"AAPL .earnings_dates: {len(ed)} rows, columns={list(ed.columns)}"


def check_yfinance_batch_download():
    import yfinance as yf
    data = yf.download(
        "AAPL MSFT NVDA",
        period="5d",
        progress=False,
        threads=True,
        group_by="ticker",
    )
    if data is None or data.empty:
        raise RuntimeError("empty DataFrame from yf.download batch")
    n = len(data.columns.get_level_values(0).unique())
    return f"3-ticker batch: {n} tickers, {len(data)} rows"


def check_finance_calendars():
    from finance_calendars.finance_calendars import get_earnings_by_date
    # Try a recent Tuesday so we usually hit a business day with data
    today = date.today()
    probe = today - timedelta(days=(today.weekday() - 1) % 7)
    df = get_earnings_by_date(probe)
    if df is None or df.empty:
        return f"get_earnings_by_date({probe}) returned empty"
    return f"get_earnings_by_date({probe}): {len(df)} entries, columns={list(df.columns)[:4]}"


def check_financedatabase():
    import financedatabase as fd
    eq = fd.Equities()
    df = eq.select()
    if df is None or df.empty:
        raise RuntimeError("empty DataFrame from fd.Equities().select()")
    sectors = df["sector"].dropna().unique() if "sector" in df.columns else []
    return f"fd.Equities: {len(df):,} rows, {len(sectors)} unique sectors"


def main():
    checks = [
        ("NASDAQ FTP (nasdaqlisted.txt)",  check_nasdaq_ftp),
        ("GitHub rreichel3/US-Stock-Symbols", check_github_tickers),
        ("SEC EDGAR company_tickers.json", check_sec_edgar),
        ("yfinance Ticker.history",         check_yfinance_history),
        ("yfinance Ticker.info",            check_yfinance_info),
        ("yfinance Ticker.earnings_dates",  check_yfinance_earnings_dates),
        ("yfinance.download (batch)",       check_yfinance_batch_download),
        ("finance-calendars get_earnings_by_date", check_finance_calendars),
        ("financedatabase (offline)",       check_financedatabase),
    ]
    results = [_check(name, fn) for name, fn in checks]

    print("\n" + "=" * 70)
    print(" ENDPOINT SUMMARY")
    print("=" * 70)
    for r in results:
        flag = "OK  " if r["ok"] else "FAIL"
        extra = r.get("info", r.get("error", ""))
        print(f" [{flag}] {r['elapsed']:>6}  {r['name']:42}  {extra}")
    n_ok = sum(1 for r in results if r["ok"])
    print(f"\n {n_ok}/{len(results)} endpoints reachable.")


if __name__ == "__main__":
    main()
