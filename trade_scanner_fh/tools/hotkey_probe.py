"""hotkey_probe.py — SAFE, staged TradeStation hotkey bisection probe.

Runs the scanner's hotkey  click → type → Enter  sequence in ISOLATED
stages so you can pin down EXACTLY which step places an order in
TradeStation. No scan, no scanner data, no GUI required.

╔══════════════════════════════════════════════════════════════════════╗
║  RUN THIS ONLY AGAINST A SIMULATED / PAPER-TRADING CHART.            ║
║  Every stage can place a LIVE order if TradeStation is in live mode. ║
╚══════════════════════════════════════════════════════════════════════╝

Stages (run them in this order, watching the [diag] focus lines):
  click-only : move + click the saved point. NO typing, NO Enter.
               → If an order stages/fires here, the CLICK is the culprit
                 (the point is on a Trade Bar / one-click-trade control).
  type       : click, then type the ticker. NO Enter (end-key = None).
               → If an order fires now but not in click-only, the TYPED
                 SYMBOL into that field is what trades.
  full       : click, type, then press Enter.
               → If it only fires here, the ENTER is what submits.

Usage (from the project root, with the full-stack interpreter that has
pyautogui + pywin32 — e.g. C:\\python\\envs\\eda-pipeline\\python.exe):

  python hotkey_probe.py --list-config
  python hotkey_probe.py --stage click-only
  python hotkey_probe.py --stage type --ticker TEST
  python hotkey_probe.py --stage full --ticker TEST
  python hotkey_probe.py --stage click-only --x 1115 --y 783   # override point

Position + delay default to the SAME QSettings the app uses
(org=trade_scanner_fh, app=Trade_Scanner_FH); --x/--y/--delay override.
"""
from __future__ import annotations

import argparse
import sys
import time

from trade_scanner_fh import hotkey


# ── Saved-config access (same store as the GUI) ───────────────────────

def _load_saved_cfg():
    """Read the saved hotkey click position / delay from the app's
    QSettings. Returns (x, y, delay_ms) with None x/y if never set."""
    try:
        from PyQt6.QtCore import QSettings, QCoreApplication
        # QSettings needs a QApplication-ish context for org/app; passing
        # them explicitly to the ctor avoids needing a running app.
        _ = QCoreApplication.instance()
        s = QSettings("trade_scanner_fh", "Trade_Scanner_FH")

        def _int(key, default=None):
            v = s.value(key)
            if v is None:
                return default
            try:
                return int(v)
            except (TypeError, ValueError):
                return default

        return (_int("hotkey/click_x"), _int("hotkey/click_y"),
                _int("hotkey/delay_ms", 200))
    except Exception as exc:  # pragma: no cover - environment dependent
        print(f"(could not read saved hotkey config: {exc})")
        return (None, None, 200)


def _snap(phase: str) -> None:
    print(f"  [diag] {phase}: {hotkey._focus_snapshot()}", flush=True)


def _countdown(seconds: int) -> None:
    print("\n" + "=" * 70)
    print("  !! SIMULATED / PAPER MODE ONLY - this can place a real order.")
    print("=" * 70)
    for n in range(seconds, 0, -1):
        print(f"  Firing in {n}s ...  (move mouse to a screen corner to ABORT)",
              flush=True)
        time.sleep(1)


def run(stage: str, x: int, y: int, ticker: str, delay_ms: int,
        countdown: int) -> int:
    if not hotkey._coord_on_screen(x, y):
        print(f"REFUSING: click point ({x},{y}) is off-screen.")
        return 2

    import pyautogui
    saved_fs, saved_pause = pyautogui.FAILSAFE, pyautogui.PAUSE
    pyautogui.FAILSAFE = True   # mouse to a corner aborts
    pyautogui.PAUSE = 0.05

    print(f"\nStage      : {stage}")
    print(f"Click point: ({x},{y})")
    print(f"Delay      : {delay_ms} ms")
    if stage != "click-only":
        print(f"Ticker     : {ticker!r}")
    print(f"Enter      : {'YES' if stage == 'full' else 'no'}")

    _countdown(countdown)

    try:
        print(f"\n  pre-click: target-under-point={hotkey._window_at(x, y)} | "
              f"{hotkey._focus_snapshot()} | input-held="
              f"{hotkey._any_cue_input_held()}", flush=True)

        hotkey._wait_for_input_release()
        pyautogui.click(int(x), int(y))
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)
        _snap("post-click (before any typing)")

        if stage in ("type", "full"):
            pyautogui.typewrite(str(ticker), interval=0.03)
            _snap(f"after typing {ticker!r} (NO Enter yet)")

        if stage == "full":
            pyautogui.press("enter")
            _snap("after Enter")
    finally:
        pyautogui.FAILSAFE = saved_fs
        pyautogui.PAUSE = saved_pause

    print("\n  Done. Check TradeStation: did an order stage/fill this stage?")
    return 0


def main(argv=None) -> int:
    # Window titles in the diagnostics can carry non-cp1252 chars; never let
    # a print crash the probe mid-sequence (would leave input half-sent).
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
    except Exception:
        pass

    p = argparse.ArgumentParser(
        description="Staged TradeStation hotkey bisection probe (SIM ONLY).",
    )
    p.add_argument("--stage", choices=["click-only", "type", "full"],
                   help="Which portion of the sequence to run.")
    p.add_argument("--ticker", default="TEST",
                   help="Symbol to type for type/full stages (default TEST).")
    p.add_argument("--x", type=int, help="Override click X (default: saved).")
    p.add_argument("--y", type=int, help="Override click Y (default: saved).")
    p.add_argument("--delay", type=int,
                   help="Override click→type delay in ms (default: saved).")
    p.add_argument("--countdown", type=int, default=5,
                   help="Seconds before firing (default 5).")
    p.add_argument("--list-config", action="store_true",
                   help="Print the saved hotkey config and exit.")
    p.add_argument("--capture", action="store_true",
                   help="Countdown, then print the CURRENT mouse position. "
                        "Hover over a target (e.g. the Command Line box) to "
                        "grab its (x,y). Fires no clicks/keys.")
    args = p.parse_args(argv)

    sx, sy, sdelay = _load_saved_cfg()

    if args.list_config:
        print("Saved hotkey config (QSettings trade_scanner_fh/Trade_Scanner_FH):")
        print(f"  click_x  = {sx}")
        print(f"  click_y  = {sy}")
        print(f"  delay_ms = {sdelay}")
        return 0

    if args.capture:
        import pyautogui
        print("Hover the mouse over your target (e.g. the Command Line edit "
              "box). Reading position in:")
        for n in range(int(args.countdown), 0, -1):
            print(f"  {n} ...", flush=True)
            time.sleep(1)
        x, y = pyautogui.position()
        print(f"\n  Mouse position: --x {x} --y {y}")
        print(f"  Window there  : {hotkey._window_at(x, y)}")
        return 0

    if not args.stage:
        p.error("--stage is required (or use --list-config)")

    x = args.x if args.x is not None else sx
    y = args.y if args.y is not None else sy
    delay_ms = args.delay if args.delay is not None else sdelay

    if x is None or y is None:
        print("No click position available. Either set one in the app "
              "(Hotkey Settings) or pass --x and --y.")
        return 2

    return run(args.stage, int(x), int(y), args.ticker, int(delay_ms or 0),
               int(args.countdown))


if __name__ == "__main__":
    sys.exit(main())
