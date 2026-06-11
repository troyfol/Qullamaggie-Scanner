"""Diagnostic — try patchright (stealth-patched fork of playwright)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from patchright.sync_api import sync_playwright

URL = "https://www.zacks.com/stock/research/AAPL/earnings-announcements"

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True, channel="chromium")
    ctx = browser.new_context(
        viewport={"width": 1280, "height": 900},
        locale="en-US",
    )
    page = ctx.new_page()
    print(f"goto {URL}")
    resp = page.goto(URL, timeout=45000, wait_until="domcontentloaded")
    print(f"status: {resp.status if resp else 'no response'}")
    print(f"title: {page.title()!r}")
    try:
        page.wait_for_selector("#earnings_announcements_earnings_table", timeout=30000)
        print("EPS TABLE FOUND")
        # Read it
        import io
        import pandas as pd
        dfs = pd.read_html(io.StringIO(page.content()), attrs={"id": "earnings_announcements_earnings_table"})
        if dfs:
            print(f"rows: {len(dfs[0])}")
            print(dfs[0].head(3).to_string())
    except Exception as exc:
        print(f"selector timeout: {exc!r}")
        body_len = page.evaluate("() => document.body.innerHTML.length")
        print(f"body html length: {body_len}")
        Path(__file__).parent.joinpath("_zacks_diag3.html").write_text(
            page.content(), encoding="utf-8"
        )
        print("saved _zacks_diag3.html")

    browser.close()
