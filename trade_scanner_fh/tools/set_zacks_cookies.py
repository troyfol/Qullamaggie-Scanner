"""set_zacks_cookies.py — one-shot cookie injection helper.

Usage (one of):

    # 1. Paste the cookie string as the first argv:
    venv\\Scripts\\python tools\\set_zacks_cookies.py "reese84=...; visid_incap_2944342=..."

    # 2. Read from a file:
    venv\\Scripts\\python tools\\set_zacks_cookies.py --file C:\\path\\to\\cookies.txt

    # 3. Interactive paste (no quoting needed; ends on blank line):
    venv\\Scripts\\python tools\\set_zacks_cookies.py

    # Clear stored cookies:
    venv\\Scripts\\python tools\\set_zacks_cookies.py --clear

    # Smoke-check the stored cookies against AAPL on Zacks live:
    venv\\Scripts\\python tools\\set_zacks_cookies.py --test

How to obtain the cookie string:

    1. Open Chrome (or Firefox). Visit:
       https://www.zacks.com/stock/research/AAPL/earnings-calendar
    2. F12 → Network tab → click the document request (the page URL itself).
    3. In the request headers, find the "Cookie:" header.
    4. Right-click → Copy value (or "Copy as cURL" and pull out the
       --cookie argument).
    5. Paste into this script. The string should look like:
         reese84=...; visid_incap_2944342=...; nlbi_2944342=...; ga=...
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure the package import works whether you run from the project root or
# from inside trade_scanner_fh/.
_HERE = Path(__file__).resolve()
sys.path.insert(0, str(_HERE.parent.parent.parent))

from trade_scanner_fh import zacks_scraper as zs


def _read_interactive() -> str:
    print("Paste the Zacks cookie string. Empty line to finish:")
    lines: list[str] = []
    try:
        while True:
            line = input()
            if not line:
                break
            lines.append(line)
    except EOFError:
        pass
    return " ".join(lines).strip()


def _live_test() -> int:
    if not zs.has_zacks_cookies():
        print("No cookies currently stored — paste them first.")
        return 1
    print("Hitting AAPL via Zacks with stored cookies + curl_cffi…")
    rows = zs.fetch_earnings_history("AAPL", years=5, timeout_sec=20.0)
    if rows is None:
        print("FAIL: no rows returned. Cookies may be expired or wrong.")
        return 1
    print(f"OK: {len(rows)} quarters fetched.")
    if rows:
        print(f"Most recent: {rows[0]}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cookie_string", nargs="?", default=None,
                        help="Cookie string (key=value; key=value; …)")
    parser.add_argument("--file", help="Path to a file containing the cookie string")
    parser.add_argument("--clear", action="store_true",
                        help="Remove the stored cookies")
    parser.add_argument("--test", action="store_true",
                        help="Live-fetch AAPL using the stored cookies")
    args = parser.parse_args()

    if args.test:
        return _live_test()

    if args.clear:
        zs.set_zacks_cookies("")
        print("Cleared stored Zacks cookies.")
        return 0

    cookie_str: str = ""
    if args.file:
        # Guard the file read: a missing/huge/binary file should print a clear
        # error, not a traceback or a multi-GB slurp (audit: read was
        # unguarded + uncapped). A real cookie blob is a few KB.
        try:
            p = Path(args.file)
            if p.stat().st_size > 1_000_000:
                print(f"Cookie file too large ({p.stat().st_size} bytes) — "
                      "expected a few KB. Aborting.")
                return 1
            cookie_str = p.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            print(f"Could not read cookie file {args.file!r}: {exc}")
            return 1
    elif args.cookie_string:
        cookie_str = args.cookie_string
    else:
        cookie_str = _read_interactive()

    cookie_str = cookie_str.strip()
    if not cookie_str:
        print("Empty input — nothing stored.")
        return 1

    if zs.set_zacks_cookies(cookie_str):
        parsed = zs._parse_cookie_string(cookie_str)
        path = zs._cookies_path()
        print(f"Stored {len(parsed)} cookies under {path}")
        # Print the keys but not the values (cookies are session tokens)
        print(f"Keys: {sorted(parsed.keys())}")
        if "reese84" not in parsed and "incap_ses" not in " ".join(parsed.keys()):
            print()
            print("WARNING: didn't see Imperva-typical cookie names "
                  "(reese84, visid_incap_*, incap_ses_*).")
            print("Make sure you copied the cookies AFTER visiting "
                  "zacks.com/stock/research/AAPL/earnings-calendar in your browser.")
        return 0
    print("ERROR: cookie write failed (filesystem error?).")
    return 1


if __name__ == "__main__":
    sys.exit(main())
