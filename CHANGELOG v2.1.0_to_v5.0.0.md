# Trade_Scanner_FH — Changelog

## v5.1.0 (2026-06-11)

Three commits on top of the v5.0.0 public-repo snapshot — `34da785` (Phase 1 quick wins), `f9cef4f` (Phase 2 refactors), `a36f338` (Phase 3 features). Test suite grew **942 → 1187** (0 failures) across the three phases, with pytest-cov wired in along the way.

### Sanitization (Public-Repo Prep)

- **SEC contact email placeholder scheme** — no real contact email lives in source. `config.SEC_CONTACT_DEFAULT` is a deliberately non-functional placeholder (`your.email@example.com`); the working address resolves at runtime via `scanner_data/sec_contact.txt` (gitignored; `Settings → Set SEC Contact Email…`) → the `SEC_CONTACT_EMAIL` env var → the placeholder. `sec_contact_is_configured()` keeps the SEC EDGAR universe source dormant until a real address is supplied.
- **`version_info.txt` carries no personal/company identifiers** — `CompanyName` / `LegalCopyright` intentionally empty.
- **Live-session diag captures removed**; personal debug artifacts excluded via `.gitignore`.

### Observability & Version Stamping (Phase 1)

- **All 13 silent except-swallows in `main_window.py` now log** — the stale-End-date pre-scan check upgraded to WARNING
- **New `_log_error` / `_start_worker` helpers** — 14 worker-bringup call sites migrated; connect-before-start ordering pinned by tests
- **Window title shows `__version__`**; new `version_info.txt` VERSIONINFO resource wired into the PyInstaller `EXE()` (numeric fields derive from `trade_scanner_fh.__version__`; kept in sync by tests)
- **Spec: `scipy` excluded** (~18 MB; verified unused)

### Settings → Advanced… (Phase 1)

- **`OHLCV_HISTORY_YEARS`, `EARNINGS_HISTORY_YEARS`, `REFERENCE_TICKERS` now user-configurable** via gitignored `scanner_data/user_config.json` — values clamped, corrupt-JSON-safe
- **`ZacksFillWorker` years default 5 → `config.EARNINGS_HISTORY_YEARS` (10)** — fixes a stale literal left over from the 2026-06-07 history-cap raise

### GUI Decomposition & Shared Fill Framework (Phase 2)

- **`MainWindow` decomposed** (8044 → 6912 lines; 7302 after the Phase 3 feature wave) into new `gui/` modules: `earnings_coordinator.py` (841 lines — 3-bar progress panel, smart-refresh chaining, nasdaq cadence, per-source worker bringup), `blacklists.py`, `exports.py`, `columns.py`. Every historical method name kept as a thin delegate.
- **`fill_framework.py`** — shared checkpoint / flush / finalize / backoff-rewind orchestrator for the finviz/finnhub fill pair (the two fill modules shrank 498 lines onto a 547-line shared module); hooks resolve through the module at call time so test monkeypatching of private names keeps working
- **`_log_error` panel double-emit fixed** (panel_skip record filter)

### Scraper Resilience & Spike Alarm (Phase 2)

- **Zacks drift-tolerant `obj_data` fallback parser** — `FAIL_PARSE_ERROR` reserved for genuinely unparseable pages; empty `obj_data` still classifies as `FAIL_NOT_FOUND`
- **Finviz two-marker block-vs-empty classification**
- **Parse-failure spike alarm** — ≥40% parse-failure rate over ≥25 samples halts the fill loudly, preserves the checkpoint, never blacklists
- **Ticker-regex `\Z` hardening**

### Feature Wave (Phase 3)

- **RVOL indicator** (last-bar volume ÷ prior-20-bar mean) through the full pipeline: 3-state panel row, funnel stage, RVOL result column. Old presets load unchanged.
- **ATR Stop display column** — `close − 2 × ATR(14)`
- **ADR% rework + $ADR filter + ADR Stop + configurable stop multipliers (2026-06, post-Phase 3)** — ADR% moved to the classic ratio form `mean(100 × (High/Low − 1))` with default lookback 14 → 20 (stored preset lookbacks honored); new $ADR indicator (= ADR%/100 × End-date close, shares the ADR% lookback) through the full pipeline (3-state panel row, `$ADR >=` funnel stage, `$ADR` column, default off); new ADR Stop display column (`close − adr_stop_multiplier × $ADR`); both stop columns now read per-scan multipliers from ScanParams (`atr_stop_multiplier` default 2.0 = the previously hard-coded behavior, `adr_stop_multiplier` default 1.0) via "Stop col ×" spinboxes — columns only, never the filters. Old presets load unchanged. +51 tests (suite 1187 → 1238; 50 in the new `test_adr_dollar_stops.py` + a default-lookback pin in `test_indicators.py`).
- **Watchlist diffing** — per-(preset, period) baselines in `scanner_data/scan_history.json`; new `Chg` column flags NEW tickers plus per-period new/dropped log lines. Stopped/crashed scans never poison the baseline; one-off period labels skipped; 90-day prune.
- **Single-level undo for deleted result rows** (Ctrl+Z or context menu)
- **In-app scan scheduler** (`gui/scheduler.py`) — per-day HH:MM entries, `Scans → Schedule…` manager, auto quick-export + tray-icon toast on completion; unattended fires route modals to the log and abort if the preset fails to load
- **Quick Export** — one-click timestamped XLSX to `scanner_data/exports/`
- **Cross-source EPS disagreement report** (report-only) — `scanner_data/earnings_disagreements.csv` regenerated at every canonical save
- **Launch-time OHLCV prefetch** (opt-in Advanced toggle) — warms the parquet LRU on a stoppable background thread after the startup update

### Testing & Coverage

- **Suite: 942 → 1187** (+45 Phase 1, +52 Phase 2, +148 Phase 3); 0 failures throughout
- **pytest-cov wired in** (78% coverage at Phase 2)
- **conftest centralization** — canonical `_qapp` / `tmp_parquets` / `fake_scan_cache` fixtures; checkpoint/raw-dir paths redirected under test (closes a test-isolation trap)

---

## v2.1.0 → v5.0.0 — Changes Since Previous Distribution

The previous distribution shipped as **Equity Momentum Scanner** (`trading_scanner`, README dated 2026-03-06, ~92 lines describing a single-source price/technical scanner). The current build is **Trading Scanner — Finnhub Fork** (`trade_scanner_fh`, README ~1487 lines) — a fundamentally larger product. This document lists the additions.

### Data Sources

- **Earnings data is entirely new.** The old build had no earnings concept at all. The fork adds four upstream sources writing to two parquet files:
  - **Zacks** (primary, per-quarter EPS/Rev history) — `zacks_scraper.py` via `curl_cffi` Chrome-131 TLS impersonation; HTTP-only, no browser engine
  - **Finnhub** (deep history fallback) — `finnhub_client.py` + `finnhub_fill.py`, OS-keyring-stored API key, resumable bulk fills
  - **Nasdaq calendar** — `finance-calendars` for last/next earnings dates (±90-day window)
  - **Yahoo** (`yfinance.earnings_dates`) — gap + spot fills
- **Multi-source priority chain reconciler** (`earnings_reconcile.py`) — `zacks → nasdaq → yahoo → finnhub` for last/next dates; emits `{src}_derived` / `{src}+{src}_aug` source labels
- **Binary source policy** on `earnings_history.parquet` — any Zacks coverage on a ticker drops all Finnhub rows for that ticker (GAAP-vs-adjusted EPS semantic gap)
- **Append-only raw audit layer** — `earnings_raw/{source}/*.parquet`, one file per fill run, 30-day retention
- **YoY EPS% / Revenue% derived columns** — locally computed at every fill-finalize
- **SEC EDGAR universe source now requires a user-configurable contact email** (`Settings → Set SEC Contact Email…` → `scanner_data/sec_contact.txt` or `SEC_CONTACT_EMAIL` env var). Source is skipped until configured.
- **Sector map** — new `sector_map.py` / `sector_map.parquet`, Finnhub `/stock/profile2` primary, FinanceDatabase + yfinance fallback, 56-key sub-industry → SPDR ETF routing
- **Weekly Nasdaq calendar auto-refresh** — only auto-firing data source; all other fills are manual menu actions
- **Coverage diagnostics** — `Data → Earnings Coverage Report`, `Verify earnings_history Integrity` (9 schema/policy checks with auto-fix)

### HOTKEY / TradeStation Integration

- **Per-row HOTKEY ticker sender** (`hotkey.py` + `gui/hotkey_dialog.py`) — brand new. Fire ONE ticker at a time into any external app via pyautogui by clicking a result row (the old build only had bulk Send-to-Watchlist).
- **Configurable cue** — right-click, Shift+Left, Ctrl+Left, Middle, or Enter key (selected row)
- **Click-position capture countdown** — live cursor readout with 5-second snapshot
- **Configurable end sequence** — None, Enter, Tab, Ctrl/Shift/Alt+Enter
- **Optional return-click** — keyboard-only loop (arrow keys → Enter → focus returns to scanner)
- **Hot-pink button styling** with bold purple text when armed (visual hazard cue)
- **Daemon-thread dispatch** — slow target apps don't freeze the GUI
- **Off-on-launch safety default** — toggle does not persist across sessions
- **Existing bulk TradeStation bridge preserved** — `BridgeWorker` + `Send to Watchlist`

### Output Organization

- **3-state filter model** (per-row Off / Filter / Display Only) with **red-on-fail coloring** — display-only mode computes the value but skips the funnel filter
- **Match-color anchoring system** — when a non-earnings indicator date (max_gap_date, surge_start_date, up_gap_start_date, etc.) lands on an earnings report date, the whole unit (indicator cell + date cell + Q-i triplet) paints with one palette color. Per-ticker seeded; cool half of color wheel only.
- **Fuzzy match tolerance** — `Settings → Color Match Tolerance…` (0–7 days, default ±1)
- **Columns ▾ dropdown** — non-modal manager: drag to reorder, checkbox to hide. Saves into presets.
- **Cut + Paste rows** (single-shot clipboard, persists across view-filter toggles)
- **Cut + Paste columns** via header right-click
- **Delete rows** (Delete key or right-click) — mutates `_period_results`, survives sort/tab switches
- **Delete columns** via header right-click — survives across scans
- **Multi-select** — shift/ctrl-click on rows or column headers
- **Three view-only post-scan filters** next to the Timeframe dropdown:
  - Earnings Dates (view)
  - Earnings Data (view)
  - Color Match Only
- **Interleave Q EPS+Rev** view toggle — alternates EPS/Rev quarter blocks
- **Dynamic per-quarter columns** — up to 20 quarters of Q-i triplets (date / value / surprise) per beats filter
- **Two global earnings toolbar toggles** — `Earnings Dates` and `Earnings Data` (replaces 9 per-row `Include No Data` checkboxes)
- **Crash-hardened table rendering** — per-row try/except, chunked populate (200 rows / `processEvents`)
- **Reset to Default** — clears manual order + hidden set

### Output File / Export Option

- **Excel/CSV export entirely new** — old build had no export feature at all.
- **`ExcelExportDialog`** with format combo (XLSX / CSV), per-period or all-periods selection
- **Pre-checked column list** with Select All / Select None
- **Bundled per-quarter beats toggles** — one checkbox each for "Consecutive EPS Beats per-quarter columns" and "Consecutive Rev Beats per-quarter columns" (instead of N×5 individual checkboxes)
- **Cell coloration in XLSX** — mirrors on-screen match-color palette + streak green + display-only fail red via openpyxl font colors
- **Honors manual column drag order** — export iterates the visual layout, not canonical order
- **View filters apply to export** — what you see is what gets written
- **Multi-period export** — sequenced runs land each period on its own sheet, filtered independently
- **Period-ordering invariant** — multi-period selections now always execute and display smallest→longest (1D → 1W → 1M → 3M → 6M; custom range last). Sequenced runs continue to walk backwards from the end date, so the most proximal chunk is index 0. Locks down "first claim wins" for the new intra-run omit toggle.
- **"Omit earlier-period hits (this run)"** — new session-bar toggle next to "Omit previously scanned tickers". When on, tickers that pass an earlier (shorter / more proximal) period are stripped from the symbol list before any later period scans, so each ticker only appears in its earliest qualifying timeframe. Off by default; doesn't persist across launches. Distinct from the across-scan `chk_omit_seen` — the two can be active simultaneously.

### Indicators

- **Count: 14 → 21** (`indicators.py` module docstring updated accordingly)
- **New: Surge Detection with 4 modes** (combobox-selected):
  - `trend` — drawdown-gated continuous rally to global peak
  - `ignition` — re-anchors trend start to a catalyst bar (Day % gain + Vol × median)
  - `close` — legacy fixed-window close-to-close
  - `high_low` — legacy fixed-window low-to-high
- **Per-mode field activation** with greyout styling for inactive fields
- **New: Max Positive Gap** — gap % + date, eligible for match-coloring
- **New: Up Gap / Down Gap** with start dates
- **New earnings-side filters** (gated on the two global toolbar toggles):
  - 6 individual per-quarter columns: `reported_eps`, `surprise_eps_dollar`, `surprise_eps_pct`, `reported_rev`, `surprise_rev_dollar`, `surprise_rev_pct`
  - Consecutive EPS Beats + Consecutive Rev Beats with **`min = 0` valid threshold** (surface streak count without pass/fail) and **per-side quarter cap** (1–20 or 0 = no cap)
  - Days since earnings / Days until earnings / Days until max
  - Curr YoY EPS % / Curr YoY Rev % + per-quarter Q-i YoY % columns
- **Display-only support** on every indicator with `_flag_min` / `_flag_max` helpers in `_compute_display_only_fails`

### Settings & Credentials

A `Settings` menu didn't exist before — this is all new.

- `Settings → Set / Clear Cookie Browser Monitor to Current Window` — pin Firefox cookie-refresh launcher to a specific monitor (QSettings)
- `Settings → Hotkey Settings…` — full HOTKEY config dialog
- `Settings → Color Match Tolerance…` — fuzzy-day window
- `Settings → Set SEC Contact Email…` — SEC EDGAR User-Agent contact
- **First-launch credential prompts** — Finnhub API key (keyring) then Zacks cookies (paste dialog)
- **`Data → Refresh Zacks Cookies (Open Browser)…`** — launches managed Firefox profile, mid-flight `cookies.sqlite` capture via stdlib `sqlite3` ro+immutable, auto-detects close via psutil

### Storage Layout

- **New persistent files**: `sector_map.parquet`, `earnings_dates.parquet`, `earnings_history.parquet`, `earnings_raw/{source}/`, `zacks_cookies.txt`, `firefox_zacks_profile/`, `sec_contact.txt`, `zacks_blacklist.txt`, `finnhub_blacklist.txt`, `failed_tickers.log`, `.finnhub_bulk_checkpoint.json`
- **`scanner_data/` is "sacred"** — never touched by rebuilds (explicit invariant + tested)
- **Atomic writes everywhere** — `config.atomic_write_parquet` / `atomic_write_text` (tmp + `os.replace`)
- **Blacklists are now newline-separated** (was comma-separated)

### Stability & Crash Hardening

Post-2026-05 audit pass.

- `ScanWorker.run` / `_on_scan_done` / `_populate_row` all wrapped in try/except
- `_safe_streak` helper guards `bool(NaN) is True` crash trap
- `LogPanel._append` flushes after every write
- `ResultsTable.populate` yields to `processEvents()` every 200 rows
- Per-ticker exception isolation in `run_scan` (errors accumulate, scan continues)

### Build & Dependencies

- **Python 3.11** (was 3.11+), shared `eda-pipeline` env
- **New runtime deps**: `curl_cffi`, `lxml`, `keyring`, `openpyxl`, `psutil`, `pywin32`, `finance-calendars`, `financedatabase`
- **Dropped (never imported)**: `scipy`, `beautifulsoup4`, `finnhub-python`
- **Spec excludes `PySide6` / `shiboken6`** — sibling env carries it; PyInstaller aborts on two Qt bindings
- **`requirements.txt`, `LICENSE`, `.gitignore`** all added (didn't exist before)
- **Spec hidden imports** now cover all lazy deps explicitly

### Testing

- **778-test pytest suite** (old README didn't mention testing at all) across ~37 `test_*.py` modules
- 28 tests for HOTKEY alone
- Integrity / coverage / reconcile / palette / dynamic-column / preset-roundtrip regressions all pinned

### Preset Format

- **Bumped v1 → v5.** Old build was implicit "save indicator + scan-window configs."
- v3 added `earnings_only_mode`, `view_earnings_only`, `view_color_match_only`
- v4 split `earnings_only_mode` into `earnings_dates_only` + `earnings_data_only`; same split for view filters
- v5 added `column_order`, `column_hidden`, `view_interleave_quarters`
- **Backward-compat loader** — v1–v4 presets still load cleanly via `.get()` defaults

### Renaming / Scope

- Package: `trading_scanner` → `trade_scanner_fh`
- Exe: `TradingScanner.exe` (implied) → `Trade_Scanner_FH.exe`
- Title: "Equity Momentum Scanner" → "Trading Scanner — Finnhub Fork"
- Module count: **9 → 28** Python files (top-level package + `gui/` + `tools/`)
