<!-- markdownlint-disable MD013 MD024 -->
# Trade_Scanner_FH — Audit Remediation Log (2026-06-09)

Companion to [`AUDIT_2026-06-09.md`](AUDIT_2026-06-09.md) (the findings report). This
records what was **implemented**, **deferred (with rationale)**, and how it was
**verified**, across remediation Waves 0–7.

- **Verification after every wave:** full `pytest` suite (conda env
  `C:\python\envs\eda-pipeline\python.exe`) + a targeted functional smoke.
- **Final state:** **933 passed / 0 failed** (started at 929; net +4 from new
  regression tests). 2 warnings are pre-existing (`etf/adr` fillna downcast in
  `main_window.py` — out of scope).
- **No exe was rebuilt.** Source-only changes. `dist2/` (public fork) untouched;
  `scanner_data/` caches untouched.

Finding IDs (e.g. `H2`, `M24`, `robust-…`) reference `AUDIT_2026-06-09.md`.

---

## Implemented

### Wave 0 — Quick wins
| ID | Change | File(s) |
|----|--------|---------|
| H6 | CSV export headers now read the live `active_columns` (was static `RESULT_COLUMNS`, positionally mislabeling dynamic/reordered columns) | `gui/main_window.py` `_export_results` |
| M20 | `try/except` in `_load_blacklist` / `_load_greylist` (a locked/corrupt file no longer aborts launch) | `gui/main_window.py` |
| M21 | `try/except` around `_load_preset` JSON parse (corrupt preset shows a dialog instead of crashing the slot) | `gui/main_window.py` |
| L32 | Zero-guard on `_on_scan_progress` integer division | `gui/main_window.py` |
| M27 | `LogPanel.setMaximumBlockCount(10000)` (bounds the always-on log document) | `gui/widgets.py` |
| L16 | Log panel auto-scrolls only when already pinned to bottom (stops fighting the reading user) | `gui/widgets.py` |
| L56 | Hoisted `import random` out of the per-row render loop | `gui/widgets.py` |

### Wave 1 — Cache-corruption hardening (data integrity)
| ID | Change | File(s) |
|----|--------|---------|
| — | `atomic_write_parquet` uses a unique per-writer temp name (`.{name}.{pid}.{uuid}.tmp`) + unlink-on-failure; new `atomic_write_csv` helper | `config.py` |
| H2 | `_finalize_fill` (finviz/finnhub/earnings_history) wraps load→YoY→save in `HISTORY_WRITE_LOCK`; Auto-fix `_do_fix` refuses while a fill runs and re-reads under the lock | `finviz_fill.py`, `finnhub_fill.py`, `earnings_history.py`, `gui/main_window.py` |
| H3 | New `earnings_cache.DATES_WRITE_LOCK`; `_merge_and_save` + `reconcile_earnings_dates` re-read the freshest snapshot under the lock (no concurrent lost-updates) | `earnings_cache.py`, `earnings_reconcile.py` |
| M9 | `load_earnings_cache` guards `.dt` with `is_datetime64_any_dtype` (object/null date column no longer crashes→None→truncates the cache); `_merge_and_save` refuses to overwrite when the file exists but reads as unreadable | `earnings_cache.py` |
| M18 | `download_one` re-fetches the full OHLCV window if the parquet vanished mid-update (no partial-tail truncation) | `data_engine.py` |
| M10 | Universe CSV written via `atomic_write_csv` | `ticker_universe.py` |
| L50 | Retry-once on a transient parquet read failure (both loaders) | `earnings_history.py`, `earnings_cache.py` |

### Wave 2 — Automation safety (pyautogui)
| ID | Change | File(s) |
|----|--------|---------|
| H4 | `BridgeWorker.run()` wrapped in `try/except` → emits `done(0,0)` on a failsafe abort (buttons no longer lock forever; no uncaught-exception process abort) | `gui/workers.py` |
| H5/M1 | `hotkey.send_ticker` refuses an off-screen saved coordinate (virtual-desktop aware, fail-open) before clicking+typing | `hotkey.py` |
| M11 | Hotkey dispatch serialized via an in-flight flag + refused during an active bridge send (no interleaved pyautogui streams) | `gui/main_window.py` |

### Wave 3 — GUI lifecycle / responsiveness (safe subset)
| ID | Change | File(s) |
|----|--------|---------|
| M8 | `FirefoxCookieWaitWorker` added to `closeEvent` worker-shutdown set (no destroyed-while-running QThread on quit) | `gui/main_window.py` |
| L17 | Window geometry persisted in `closeEvent` and restored in `main()` (replaces the forced re-center) | `gui/main_window.py` |
| M4 | `rebuild_ticker` is now download-then-swap (moves old parquet aside, restores it if the re-download fails — no data gap) | `data_engine.py` |

### Wave 4 — Correctness / reconciliation
| ID | Change | File(s) |
|----|--------|---------|
| M16 | RS-S&P / RS-NASDAQ filters fail **closed** when the benchmark column is absent (was silently passing every ticker) | `scanner.py` |
| M2 | `Force OHLCV Refresh` takes a `force=True` path that bypasses the freshness gate (no longer a silent no-op) | `gui/main_window.py` |
| M5 | Manual "Run Earnings Smart Refresh Now" applies the same all-three-sources-blocked candidate trim as the auto path (shared `_trim_all_blocked_candidates` helper) | `gui/main_window.py` |
| M13 | Resumed bulk runs seed `affected_total` from the prior session's completed set so those tickers get reconciled at end-of-run | `finnhub_fill.py`, `finviz_fill.py` |
| M24 | A targeted reconcile now **removes** an affected ticker's stale row when it yields no usable date (was silently preserved) | `earnings_reconcile.py` |

### Wave 5 — Efficiency
| ID | Change | File(s) |
|----|--------|---------|
| H1 | `compute_yoy_columns` **vectorized** (self-join on the prior-year key) — replaces an O(n) Python row loop over the whole ~138k-row parquet that ran on every scan setup + every fill finalize. Semantics preserved exactly (DateOffset(years=1), last-wins dedup, MIN_YOY floor); guarded by a dup-period parity test | `earnings_history.py` |
| — | OHLCV LRU cache `maxsize` 10000 → 24000 (above a full universe; mtime-keyed so it can't serve stale data) | `data_engine.py` |
| — | `FirefoxCookieWaitWorker` stop is now a `threading.Event` (instant cancel; tightens the `closeEvent` wait) | `gui/workers.py` |
| — | Bulk-fill `flush_every` default 25 → 100 (bulk entry points only) — ~4× fewer full-parquet rewrites over a multi-hour run; gap/spot stay at 25 | `finnhub_fill.py`, `finviz_fill.py` |

### Wave 6 — Security hardening
| ID | Change | File(s) |
|----|--------|---------|
| M23 | Response-size cap before parse/brace-walk on the attacker-controllable upstreams (Zacks 25 MB, Finnhub 25 MB) | `config.py`, `zacks_scraper.py`, `finnhub_client.py` |
| sec-zacks-cookies-plaintext | Zacks cookie file now **DPAPI-encrypted** at rest (`CryptProtectData`, user scope) with a marker prefix; **backward-compatible read** of legacy plaintext files and a plaintext fallback when DPAPI is unavailable (no lockout) | `zacks_scraper.py` |
| sec-finnhub-token-in-url | `allow_redirects=False` on the token-bearing Finnhub request | `finnhub_client.py` |
| robust-clean-value-accepts-inf | `_clean_value` / `_to_float` reject inf/NaN (no non-finite EPS/revenue into the parquet) | `zacks_scraper.py`, `finviz_fill.py` |
| robust-finnhub-fetch-dates | Narrowed the date-parse `except`, added per-event `isinstance` + a 5000-row iteration cap | `finnhub_client.py` |
| robust-bridge-confirm-key | Bridge confirm-key coerced to a whitelist (`enter`/`tab`/`space`) before `pyautogui.press` | `tradestation.py` |
| robust-set-zacks-cookies | `set_zacks_cookies` CLI tool guards the `--file` read (size cap + error handling) | `tools/set_zacks_cookies.py` |

### Wave 7 — Remaining hardening
| ID | Change | File(s) |
|----|--------|---------|
| M19 | NYSE holiday table extended 2029–2032 (programmatically computed, incl. weekend-observance shifts) + a one-time warning when the reference year exceeds the covered range | `config.py` |
| M22 | `_on_imperva_block` short-circuits to `"stop"` when a stop was requested as the block tripped (no dropped stop / no deadlock on `wait()`) | `gui/workers.py` |
| robust-adr-pct-zero-close | `adr_pct` masks non-positive Close bars (no inf into the filter / "inf%" display) | `indicators.py` |
| robust-surge-neg-inf | `surge_detection` returns NaN (not the `-inf` sentinel) when every window low is non-positive | `indicators.py` |
| robust-top-percentile-empty | Top-percentile stage guards an empty cleaned distribution (skips + warns instead of `nanpercentile([])` emptying the scan) | `scanner.py` |
| robust-run-id-collision | `new_run_id` uses the full uuid4 hex (no same-second collision merging two runs' captures) | `earnings_raw.py` |
| robust-preset-name-unsanitized | Preset names sanitized (drop directory components + Windows-illegal chars) before reaching a filesystem path | `gui/main_window.py` |

### New / updated regression tests
- `test_data_engine.py` — rebuild download-then-swap (success) + preserve-on-failure (replaces the old delete-then-download test).
- `test_earnings_reconcile.py` — targeted reconcile clears a stale no-data row (M24).
- `test_earnings_history.py` — `compute_yoy_columns` dup-period last-wins parity (H1).
- `test_audit_fixes.py` — Zacks cookies round-trip + legacy-plaintext backward-compat (DPAPI).
- `test_smart_refresh_workers.py` — fixture seeds per-source blacklists (manual trim, M5).
- `test_earnings_raw.py` — run-id format updated to full uuid hex.

---

## Deferred (with rationale)

These were assessed and intentionally **not** changed, to honor the
no-regression constraint. Each is a candidate for dedicated follow-up.

| ID | Why deferred |
|----|--------------|
| M3, M6, M7, L14 (GUI ops on main thread) | Proper fix = worker-thread migration with non-trivial signal wiring + relocating intricate post-processing (skip-list mutation via cross-thread callbacks). A busy-cursor mitigation is ineffective on an already-blocked event loop. High regression risk → scoped follow-up. |
| M26 (column-width truncation) | Content-aware cache invalidation **conflicts with an existing perf test** (`test_populate_uses_cached_widths_on_same_shape_render`) that deliberately asserts the fast path even for larger values. Fixing it risks the timeframe-switch hang the cache exists to prevent. |
| M25 (yahoo-bucket source launder), M12 (stale calendar last_earnings), M15 (next-only NaT clobber) | Intricate reconcile/cache-internal edges in the most test-sensitive subsystem (40+ reconcile tests). The report's own fixes carry explicit caveats about regressing named tests + source attribution; M15's window self-corrects at the end-of-run reconcile. Need dedicated per-fix test development. |
| Efficiency micro-opts (surge-ignition scan, skip-set memoize, data-present-mask, raw-run-rewrite, finviz size-cap pre-buffer, scan-loop parallelization) | H1 captured the dominant cost. These are LOW micro-opts; several carry test-interaction or "measure-first" caveats and aren't worth the risk. |
| sec-universe-cleartext-ftp (FTP → HTTPS) | LOW: the ticker universe is **public** data (confidentiality moot; only MITM-integrity applies). Switching the download mechanism risks breaking universe builds for little gain. |
| sec-firefox-popup-blocker-disabled | The setting is **intentional** — Imperva opens its challenge via `window.open`, and the profile is dedicated/ephemeral/single-purpose (not general browsing). Removing it could regress cookie capture. |
| sec-personal-email-in-source | **Intentional** for the personal build per the `config.py` docstring; the public `dist2/` fork uses a placeholder. No change unless distributing. |
| Assorted LOWs (rs-ratio date-misalignment, validate-ticker gaps/anomalies, checkpoint-resume worklist match, consecutive-beats period order, finnhub block-pause interruptibility, sec-edgar strict decode, finviz earningsData locator, spec comment, _bbcp_dump path, backoff-ref shared-mutable, last-failure-kind global, raw-retention mtime) | LOW-value defensive items; left as a backlog. None are data-loss or security-critical in the single-user threat model. |

---

## Build note

The application has **not** been rebuilt. When you are ready, rebuild from the
conda env (`C:\python\envs\eda-pipeline\python.exe`), close the running app
first (the exe lock causes `WinError 5`), and keep `dist2/` + `scanner_data/`
untouched.
