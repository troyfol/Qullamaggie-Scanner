# Trading Scanner — Finnhub Fork (`trade_scanner_fh`)

A standalone PyQt6 desktop application for scanning the entire US equity universe against configurable technical, momentum, volatility, volume, relative-strength, and earnings indicators. Packaged as a single-file Windows executable via PyInstaller.

<img width="1915" height="1026" alt="Screenshot 2026-05-19" src="https://github.com/user-attachments/assets/65d7453f-4d08-414a-b732-00e72ffaf4ad" />

This README is written to be self-sufficient for any developer (human or LLM) who needs to extend the system — adding a new data feed, modifying a filter's mathematical definition, adding a new indicator, or rewiring the GUI. It documents not just what the system does but **how the pieces compose**, what **invariants must not break**, and exactly **where to plug new code in**.
---

## Table of contents

1. [Architecture at a glance](#architecture-at-a-glance)
2. [Module-by-module map](#module-by-module-map)
3. [End-to-end data flow](#end-to-end-data-flow)
4. [Key data structures](#key-data-structures)
5. [Filter / indicator semantics — the three-state model](#filter--indicator-semantics--the-three-state-model)
6. [Display-only mode & red-on-fail coloring](#display-only-mode--red-on-fail-coloring)
7. [Match-color anchoring system](#match-color-anchoring-system)
8. [Adding a new indicator](#adding-a-new-indicator)
9. [Adding or modifying a filter](#adding-or-modifying-a-filter)
10. [Adding a new OHLCV data source (e.g. Polygon)](#adding-a-new-ohlcv-data-source-eg-polygon)
11. [Adding a new earnings data source](#adding-a-new-earnings-data-source)
12. [Editing mathematical assumptions of existing filters](#editing-mathematical-assumptions-of-existing-filters)
13. [The Zacks scraper subsystem](#the-zacks-scraper-subsystem)
14. [Cookie acquisition flow (Firefox + mid-flight capture)](#cookie-acquisition-flow-firefox--mid-flight-capture)
15. [GUI subsystem](#gui-subsystem)
16. [Storage layout](#storage-layout)
17. [Testing](#testing)
18. [Build & deploy](#build--deploy)
19. [Critical invariants](#critical-invariants)
20. [Disclaimer](#disclaimer)
    
**Not financial advice.** This software is for informational and educational use only — see the full [Disclaimer](#disclaimer) at the bottom of this README before relying on any output.
---

## Architecture at a glance

```text
┌──────────────────────────────────────────────────────────────────┐
│  GUI layer (PyQt6 main thread)                                   │
│   gui/main_window.py    MainWindow — toolbar, slots, menus       │
│   gui/widgets.py        IndicatorPanel, ResultsTable, LogPanel   │
│   gui/dialogs.py        Modal dialogs (sequenced run, cookies…)  │
│   gui/theme.py          Dark stylesheet                          │
└────────────────────┬─────────────────────────────────────────────┘
                     │ start worker
                     ▼
┌──────────────────────────────────────────────────────────────────┐
│  Worker layer (QThread)                                          │
│   gui/workers.py    ScanWorker, ZacksFillWorker,                 │
│                     FinnhubFillWorker, UpdateWorker,             │
│                     UniverseWorker, UniverseRefreshWorker,       │
│                     SectorFillWorker, EarningsFillWorker,        │
│                     BridgeWorker, FirefoxCookieWaitWorker        │
└────────────────────┬─────────────────────────────────────────────┘
                     │ run_scan()
                     ▼
┌──────────────────────────────────────────────────────────────────┐
│  Pipeline layer                                                  │
│   scanner.py        ScanParams, _compute_ticker, _build_filter_  │
│                     stages, _compute_display_only_fails, run_scan│
│   indicators.py     21 pure indicator functions (SMA, ATR, …)    │
└────────────────────┬─────────────────────────────────────────────┘
                     │ load_ohlcv() / load_earnings_*
                     ▼
┌──────────────────────────────────────────────────────────────────┐
│  Data layer                                                      │
│   data_engine.py        OHLCV cache (yfinance)                   │
│   ticker_universe.py    Universe download (NASDAQ FTP / GH / SEC)│
│   sector_map.py         Sector → ETF map                         │
│   earnings_cache.py     Schema/IO for earnings_dates.parquet     │
│   earnings_history.py   Schema/IO for earnings_history.parquet + │
│                         Zacks bulk/targeted fills                │
│   earnings_reconcile.py Multi-source priority chain unifier      │
│   earnings_raw.py       Append-only raw audit/replay layer       │
│   nasdaq_fill.py        Nasdaq finance-calendars bulk fill       │
│   yahoo_fill.py         yfinance gap + spot fills                │
│   finnhub_fill.py       Finnhub deep-history bulk/gap/spot       │
│   zacks_scraper.py      HTTP scraper (curl_cffi + Imperva bypass)│
│   finnhub_client.py     Finnhub REST primitives                  │
└──────────────────────────────────────────────────────────────────┘
                     │ persistent storage
                     ▼
┌──────────────────────────────────────────────────────────────────┐
│  scanner_data/  (per-user, sacred — survives all rebuilds)       │
│   ohlcv/*.parquet            per-ticker daily OHLCV (5 yr)       │
│   universe.csv               NASDAQ + NYSE universe snapshot     │
│   sector_map.parquet         ticker → sector ETF                 │
│   earnings_dates.parquet     last/next dates (5-col + source)    │
│   earnings_history.parquet   per-quarter EPS+Rev (Zacks+Finnhub) │
│   earnings_raw/{source}/*.parquet   append-only audit per run    │
│   .finnhub_bulk_checkpoint.json  resumable bulk progress         │
│   zacks_cookies.txt          Imperva session tokens              │
│   firefox_zacks_profile/     persistent Firefox profile          │
│   blacklist.txt              universal ticker blacklist          │
│   zacks_blacklist.txt        Zacks-only skip list                │
│   finnhub_blacklist.txt      Finnhub-only skip list (ETFs)       │
│   presets/, logs/, ftp_raw/                                      │
└──────────────────────────────────────────────────────────────────┘
```

### Earnings data architecture

Four independent upstream sources, each with its own dedicated fill
module and per-source blacklist where applicable:

| Source      | Module                  | Writes to                                    | Blacklist                 | Cap                         |
| ----------- | ----------------------- | -------------------------------------------- | ------------------------- | --------------------------- |
| **Zacks**   | `earnings_history.py`   | `earnings_history.parquet` (source=zacks)    | `zacks_blacklist.txt`     | 5 yr                        |
| **Finnhub** | `finnhub_fill.py`       | `earnings_history.parquet` (source=finnhub)  | `finnhub_blacklist.txt`   | 5 yr                        |
| **Nasdaq**  | `nasdaq_fill.py`        | `earnings_dates.parquet` (source=nasdaq)     | (universe blacklist only) | ±90 d window                |
| **Yahoo**   | `yahoo_fill.py`         | `earnings_dates.parquet` (source=yahoo)      | (universe blacklist only) | (whatever yfinance returns) |

Every fill writes its raw response to `earnings_raw/{source}/<run_id>.parquet`
before collapsing to the consumer schema, so reconciler logic can be
replayed against frozen captures without re-hitting upstreams. Files
older than `config.RAW_RETENTION_DAYS` (30) prune at app startup.

**Period_ending normalization**: every Finnhub fill stamps
`period_ending` to **day-1 of the fiscal-quarter month** at row
construction (e.g. Finnhub returns `2026-03-31` → stored as
`2026-03-01`). Zacks already uses day-1 natively. The normalization
makes `(ticker, period_ending)` a stable cross-source dedup key.
`report_date` is preserved exactly as the source supplied it.

**Binary source policy on `earnings_history.parquet`**:
`dedupe_history` enforces a strict ticker-level rule — for any ticker
with at least one Zacks row, ALL Finnhub rows are dropped (even on
periods Zacks doesn't cover). Finnhub-only tickers keep their Finnhub
rows. The semantic gap between Zacks adjusted EPS and Finnhub GAAP EPS
makes per-field merging unsafe, so the policy preserves source
consistency over data completeness. Zero overlap between the two
sources at the (ticker, period_ending) level after dedup.

**Reconciler priority chain** for `earnings_dates.parquet`
(`earnings_reconcile.py`):

- `last_earnings`: zacks history → nasdaq dates → yahoo dates → finnhub history
- `next_earnings`: same chain, with `> today` filter (no source can persist a stale next)
- Finnhub-history future rows with `report_date_proxy=True` are
  excluded from the next_earnings set (period-end stand-ins don't count
  as real announcement dates)
- Source label collapses to `{src}_derived` when same source supplies both
  positions, or `{last_src}+{next_src}_aug` for mixed (e.g. `zacks+yahoo_aug`)
- Finnhub is **demoted to last** in the priority chain — a `finnhub_derived`
  date only lands when no Zacks/Nasdaq/Yahoo source has any data for the
  ticker. Same logic as the binary policy on history: if any other source
  covers the ticker, Finnhub is excluded.

Every fill auto-triggers a reconcile against its affected tickers so
later runs can't clobber a freshly-derived consolidation.

**Automation**: only the once-per-week Nasdaq calendar sweep auto-fires
at launch (toggle: Data → "Auto-refresh Nasdaq calendar weekly"). Zacks,
Finnhub, and Yahoo are 100% manual via menu actions.

**Diagnostics**: `Data → Diagnostics → Earnings Coverage Report` partitions
the universe into zacks-only / finnhub-only / both / neither buckets +
shows most-recent reported quarter per source. `Data → Diagnostics →
Verify earnings_history Integrity` runs nine soft-PK / schema / policy
checks (duplicate keys, orphan rows, null sources, dtype drift, period
predates 5y cap, cross-source overlap, etc.) with one-click auto-fix
for repairable findings.

**Derived columns** — beyond what the upstream sources supply, the parquet
also carries two locally-computed YoY % columns (`yoy_eps_pct`,
`yoy_rev_pct`) refreshed at every fill-finalize via
`compute_yoy_columns`. They join `(ticker, period_ending)` against the
same-quarter-prior-year row and apply `(cur - prior) / |prior| * 100`.
NaN when prior year missing or denominator is zero. The scanner exposes
them as both top-level filters (Curr YoY EPS / Rev %) and per-quarter
columns (Q-i YoY EPS / Rev %) inside the consec-beats blocks.

---

## Module-by-module map

Line counts and entry points as of 2026-05.

### Top-level package
| File | Lines | Role | Key entry points |
|------|------:|------|------------------|
| `__main__.py` | 4 | `python -m trade_scanner_fh` shim | calls `gui.main()` |
| `launch_scanner.py` | 4 | PyInstaller entry — sole script in the spec | calls `gui.main()` |
| `__init__.py` | 13 | Package init | re-exports |
| `config.py` | 399 | All paths, constants, tunables | `DATA_DIR`, `EARNINGS_HISTORY_PARQUET`, `REFERENCE_TICKERS`, `SECTOR_ETF_MAP`, `atomic_write_parquet`, `most_recent_trading_day`, `get_sec_user_agent` / `get_sec_contact_email` / `set_sec_contact_email` / `sec_contact_is_configured` |
| `data_engine.py` | 477 | OHLCV download & cache | `download_one`, `download_many`, `load_ohlcv`, `validate_ticker`, `rebuild_ticker` |
| `ticker_universe.py` | 566 | Universe download (3 sources) | `refresh_universe`, `load_universe` |
| `indicators.py` | 632 | 21 pure indicator functions | One function per indicator (see §[Adding a new indicator](#adding-a-new-indicator)) |
| `scanner.py` | 1614 | The funnel pipeline | `ScanParams`, `_compute_ticker`, `_compute_display_only_fails`, `_build_filter_stages`, `run_scan`, `ScanResult` |
| `sector_map.py` | 288 | Sector mapping persistence | `bulk_fill_sectors`, `targeted_fill_sectors`, `load_sector_map` |
| `earnings_cache.py` | 131 | Schema/IO for earnings_dates.parquet (bulk + targeted fills now live in nasdaq_fill / yahoo_fill) | `load_earnings_cache`, `save_earnings_cache`, `get_earnings_dates`, `_merge_and_save`, `COLUMNS` |
| `earnings_history.py` | 1334 | Schema/IO for earnings_history.parquet + Zacks bulk/targeted fills + read-side binary dedup + YoY columns + integrity diagnostics | `bulk_fill_zacks`, `targeted_fill_zacks`, `find_gap_tickers`, `find_smart_refresh_candidates`, `compute_consecutive_beats`, `compute_yoy_columns`, `dedupe_history`, `get_ticker_history`, `load_earnings_history`, `save_earnings_history`, `verify_integrity`, `fix_integrity_issues`, `coverage_report` |
| `earnings_reconcile.py` | 364 | Multi-source priority chain unifier with Finnhub demoted to last position | `reconcile_earnings_dates` |
| `earnings_raw.py` | 277 | Append-only raw audit/replay layer (one parquet per fill run per source) | `new_run_id`, `append_zacks_rows`, `append_finnhub_rows`, `append_nasdaq_rows`, `append_yahoo_rows`, `read_raw`, `prune_old_raw` |
| `nasdaq_fill.py` | 141 | Nasdaq finance-calendars bulk fill (writes earnings_dates only) | `bulk_fill_nasdaq` |
| `yahoo_fill.py` | 216 | yfinance gap + spot fills (writes earnings_dates only) | `targeted_fill_yahoo`, `spot_fill_yahoo` |
| `finnhub_fill.py` | 859 | Finnhub deep-history bulk/gap/spot with step-back-on-block + resumable checkpoint + period_ending day-1 normalization + fiscal-year multi-record dedup | `bulk_fill_finnhub`, `gap_fill_finnhub`, `spot_fill_finnhub`, `find_finnhub_gap_tickers` |
| `zacks_scraper.py` | 1013 | HTTP scraper + Firefox cookie path | `ZacksSession`, `fetch_earnings_history`, `launch_firefox_for_zacks_cookies`, `read_cookies_from_firefox_profile`, `set_zacks_cookies` |
| `finnhub_client.py` | 370 | Finnhub REST primitives (rate limiter, key storage, /stock/earnings, /calendar/earnings, /stock/profile2, failure-kind sentinels) | `fetch_earnings_history`, `fetch_calendar_earnings_window`, `fetch_earnings_dates`, `fetch_company_profile`, `fetch_sector`, `verify_api_key`, `get_api_key`, `set_api_key`, `last_failure_kind` |
| `hotkey.py` | 214 | Qt-free per-row HOTKEY ticker sender (testable headless) | `HotkeyConfig`, `send_ticker` |
| `tradestation.py` | 198 | TradeStation watchlist bridge | `TradeStationBridge` |
| `tools/set_zacks_cookies.py` | 124 | One-shot CLI cookie-injection helper | `main`, `_live_test`, `_read_interactive` |

### GUI package (`gui/`)
| File | Lines | Role | Key entry points |
|------|------:|------|------------------|
| `__init__.py` | 15 | Re-exports `main` | `main()` |
| `main_window.py` | 6401 | `MainWindow` — entire window, all menus, all slot wiring | `MainWindow`, `PRESET_SCHEMA_VERSION` |
| `widgets.py` | 2770 | Reusable widgets | `IndicatorRow`, `IndicatorPanel`, `ResultsTable`, `ReorderableHeader`, `LogPanel`, `RESULT_COLUMNS`, `_ALIGN_PALETTE`, `_safe_streak`, `_anchor_date_value` |
| `workers.py` | 1137 | All QThread workers | `ScanWorker`, `ZacksFillWorker`, `FinnhubFillWorker`, `FirefoxCookieWaitWorker`, `UpdateWorker`, `UniverseWorker`, `UniverseRefreshWorker`, `SectorFillWorker`, `EarningsFillWorker`, `BridgeWorker` |
| `dialogs.py` | 770 | Modal dialogs | `WatchlistDialog`, `ExcelExportDialog`, `SequencedRunDialog`, `ColumnsManagerDialog` |
| `hotkey_dialog.py` | 544 | Per-row HOTKEY settings UI | `HotkeySettingsDialog`, `PositionCaptureCountdown` |
| `theme.py` | 52 | Dark stylesheet | `DARK_STYLESHEET` |

The top-level `hotkey.py` module (sibling of `tradestation.py`) holds the
testable, Qt-free `HotkeyConfig` + `send_ticker` for the per-row HOTKEY
sender. See [Per-row HOTKEY ticker sender](#per-row-hotkey-ticker-sender)
below.

---

## End-to-end data flow

A user-initiated scan flows like this:

```text
[1] User clicks Scan in MainWindow toolbar
       │
       ▼
[2] MainWindow._run_scan() builds the timeframe list
    + calls self.indicator_panel.build_scan_params(start, end)
    → list[(label, ScanParams)]
       │
       ▼
[3] ScanWorker(symbols, params_list) — QThread.start()
    │   wraps everything in try/except so a crash emits `finished`
    │   with partial results instead of killing the exe
       │
       ▼ (worker thread)
[4] For each (label, params): scanner.run_scan(symbols, params)
       │
       ▼
[5] run_scan:
    a) Pre-load benchmark OHLCV (SPY/ONEQ/sector ETFs) → benchmark_data
    b) Pre-load sector_map → sector_lookup dict
    c) Pre-load earnings_history → earnings_history_lookup dict
       (gated on `*_enabled OR *_display_only` for any earnings filter
        — see scanner.py _compute_ticker for details)
    d) Pre-load earnings_dates → earnings_lookup dict
       (gated identically — see BUG-7 fix in audit log)
    e) For each ticker:
        row = _compute_ticker(symbol, params, ...lookups)
        - per-ticker exceptions caught, accumulated in result.errors
    f) Concatenate rows → DataFrame
    g) Apply filter stages from _build_filter_stages(params)
        - each stage is (name, mask_fn); display-only filters are SKIPPED
    h) Apply universe-wide top_pct filter if enabled
    i) Append _compute_display_only_fails to each row
        - dict of {column_key: True} for cells that would have failed
        - drives the red-on-fail coloring in the GUI
       │
       ▼
[6] ScanResult{params, results_df, errors, funnel, elapsed_sec}
       │
       ▼ (signal back to main thread)
[7] WorkerScanResult{period_results, period_order, errors, ...}
       │
       ▼
[8] MainWindow._on_scan_done(result) — wrapped in try/except
    → ResultsTable.populate(df) renders the model
    → populate is chunked (200 rows / processEvents) to keep GUI responsive
    → per-row try/except so a single bad row can't crash the table
```

**Critical contracts at each boundary:**

- `_compute_ticker` returns either a `dict` (per-row data) or `None` (skip ticker). It must NEVER raise — exceptions are caught at the `run_scan` ticker loop, but defensive coding inside is preferred.
- `_build_filter_stages` returns `list[(stage_name, mask_fn)]`. Each `mask_fn` takes the in-flight DataFrame and returns a boolean Series. Stages not gated on `_enabled AND NOT _display_only` are deliberately omitted.
- `_compute_display_only_fails` returns a dict keyed by COLUMN NAME (not filter name). Empty dict → no fail flags. Only cells with `True` get red foreground; absent keys / `False` get default color.
- `ResultsTable.populate(df)` MUST tolerate columns with NaN, None, NaT, mixed dtypes, and missing keys. Per-row try/except inside ensures a bad row doesn't crash the whole render.

---

## Key data structures

### `ScanParams` (scanner.py:92-405)

A `@dataclass` with EVERY tunable for the funnel. Roughly 140 fields organized as follows:

```python
@dataclass
class ScanParams:
    start_date: date
    end_date: date

    # Per-filter triplet (most filters):
    {filter}_enabled: bool             # Filter mode
    {filter}_display_only: bool        # Display-only mode (mutex with enabled)
    {filter}_min: float (or _max, etc) # Threshold value
    # ... plus any filter-specific params (lookback, period, etc.)
```

**Mutex contract:** at the GUI level, `_enabled` and `_display_only` cannot both be True for the same row. Enforced in `IndicatorRow._on_filter_toggled` / `_on_display_only_toggled` (widgets.py) using `blockSignals` to prevent ping-pong. The scanner code does NOT enforce this — it independently checks each. Both being True would compute the value AND apply the filter; both being False would skip the indicator entirely.

**The 6 individual earnings filters** (reported_eps, surprise_eps_dollar, surprise_eps_pct, reported_rev, surprise_rev_dollar, surprise_rev_pct) each follow this triplet pattern. As of 2026-05 they're per-column gated — column appears only when its specific `_enabled OR _display_only` is on.

**Two beats filters** (consec_eps_beats, consec_rev_beats) gate their corresponding Q-i triplet block AND the streak count column. When either is active, `last_report_date` is suppressed (redundant with Q-1 Date).

The `consec_*_beats_min` spinbox accepts **0 as a valid threshold**. Setting min=0 makes the streak filter trivially pass every ticker (streak ≥ 0 is always true) AND the display-only red-on-fail can never fire (streak < 0 is impossible). Intended use: surface the streak count + Q-i blocks for context when the user wants to see the data without any pass/fail signal.

The `consec_*_beats_quarter_cap` spinbox (label "Q Cap" in the panel) is an **optional ceiling on how many Q-i columns are populated** for that side. Default 0 means "no cap" — the scanner populates every available quarter up to MAX_BEATS_QUARTERS=20. Setting cap=4 limits to Q-1..Q-4 even when the ticker has 20 quarters of history. EPS and Rev caps are independent. Implementation: applied at the scanner level via `past_desc.head(cap)` so unpopulated quarters never reach the DataFrame, which means `_build_dynamic_columns` naturally renders only the capped count.

The number of Q-i columns rendered is **based on populated data**, not capped by the streak count. A ticker with a streak that broke at Q-3 still shows Q-3..Q-N (up to MAX_BEATS_QUARTERS=20 quarters of history, or the user's quarter_cap if smaller). The streak count drives only the in-streak green-text coloring inside `_populate_row`; display gating is decoupled. Rationale: post-streak earnings cells must remain eligible for match-coloring against non-earnings indicator dates — a `max_gap_date` that lands on Q-4's earnings day should color the Q-4 unit even though Q-4 isn't part of an active streak.

### Result DataFrame schema (built by `run_scan`)

| Column | Always present? | Source |
|--------|-----------------|--------|
| `symbol`, `close`, `price`, `pct_gain`, `gain_start_date` | Yes (per-ticker baseline) | `_compute_ticker` |
| `sma{period}` | Iff sma1/sma2 enabled-or-display | `indicators.price_above_sma` |
| `sti`, `dist_high_pct`, `consec_gaps`, ... | Iff matching filter active | `indicators.*` |
| `max_gap_pct` + `max_gap_date` | Iff max_gap active | `indicators.max_positive_gap` (returns tuple) |
| `surge_pct` + `surge_start_date` + `surge_end_date` + `surge_window` | Iff surge active | `indicators.surge_*` |
| `reported_eps`, `surprise_eps_*`, `reported_rev`, `surprise_rev_*` | Per-column gating (Option B 2026-05) | `mr.get(...)` from earnings_history_lookup |
| `last_report_date` | Iff individual earnings active AND no beats active | (suppressed when beats covers it) |
| `consec_eps_beats`, `q1..qN_*_eps` | Iff `consec_eps_beats_enabled OR _display_only`; N = `min(populated_quarters, consec_eps_beats_quarter_cap if >0 else MAX_BEATS_QUARTERS=20)`. NOT capped by streak length. | `compute_consecutive_beats` + per-quarter projection |
| `consec_rev_beats`, `q1..qN_*_rev` | Iff `consec_rev_beats_enabled OR _display_only`; same N-rule with `consec_rev_beats_quarter_cap` (independent from EPS) | Same for revenue side |
| `_earnings_aligned_dates` | Iff any non-earnings indicator date matches an earnings date | hidden — drives match-color in widget |
| `_display_only_fails` | Iff any display-only filter has fail flags for this row | hidden — drives red-on-fail in widget |

### `RESULT_COLUMNS` (widgets.py:1089-1132)

A list of `(header_label, dataframe_key, formatter_fn)` tuples. The display-side mapping. The widget's `_build_dynamic_columns` filters this list to only include columns whose `dataframe_key` is in `_ALWAYS_VISIBLE_KEYS` or appears in the result frame's columns. Then dynamic Q-i blocks are appended based on the **maximum populated quarter count** in the frame (the `_max_present(suffix)` helper), NOT the maximum streak length. This decoupling is what lets post-streak earnings cells remain eligible for match-coloring.

### `earnings_history.parquet` schema (earnings_history.py)

```text
ticker             string
period_ending      datetime64[ns]   day-1 of fiscal-quarter month (normalized)
report_date        datetime64[ns]   announcement date — EXACT, source-supplied
report_time        string           Open / Close / Market / Unknown
estimated_eps      float64
reported_eps       float64
surprise_eps       float64
surprise_eps_pct   float64
estimated_rev      float64
reported_rev       float64
surprise_rev       float64
surprise_rev_pct   float64
source             string           "zacks" | "finnhub"
updated_at         datetime64[ns]
report_date_proxy  bool             True if report_date is a period_ending
                                    stand-in (Finnhub free tier sometimes
                                    omits real announcement dates)
yoy_eps_pct        float64          Year-over-year EPS % growth — computed
                                    at fill-finalize time from same-quarter-
                                    prior-year row. NaN when prior year
                                    missing or prior reported_eps == 0.
yoy_rev_pct        float64          Year-over-year revenue % growth — same
                                    derivation rules as yoy_eps_pct.
```

Sort on save: `(ticker ASC, period_ending DESC)`. Cadence-gap detection in `compute_consecutive_beats` uses `period_ending` (NOT `report_date`) — see §[Editing mathematical assumptions](#editing-mathematical-assumptions-of-existing-filters) for the rationale.

`period_ending` is always day-1 of the fiscal-quarter month (e.g. Q1 2026 → `2026-03-01`) regardless of source. Cross-source `(ticker, period_ending)` joins are reliable. `report_date_proxy=True` rows are excluded from the reconciler's next_earnings candidate set — only real announcement dates can be promoted.

**YoY columns (added 2026-05)** are derived locally — NOT pulled from any source — by `earnings_history.compute_yoy_columns(df)`. The helper runs at the end of every fill (`_finalize_fill` in both Zacks and Finnhub paths), refreshing the entire parquet so newly-arrived prior-year rows back-fill their current-year counterparts' YoY values. Formula: `(cur - prior) / |prior| * 100`, applied to `reported_eps` and `reported_rev` independently. The `|prior|` denominator handles negative-prior cases correctly (negative-to-positive transition produces a positive YoY%, matching the "improvement" intent).

**External readers**: this is an additive schema change. Existing readers that select specific columns (`pd.read_parquet(path, columns=[...])`) are unaffected. Readers that read the whole file (`pd.read_parquet(path)`) will see two new float64 columns at the end. The pre-YoY column order is unchanged.

### Per-ticker OHLCV parquet (data_engine.py)

`scanner_data/ohlcv/{TICKER}.parquet` — DatetimeIndex, columns: `Open`, `High`, `Low`, `Close`, `Volume`. Up to `OHLCV_HISTORY_YEARS` (5) years of daily bars. Loaded lazily via `data_engine.load_ohlcv(symbol)` which returns a tz-naive DataFrame.

---

## Filter / indicator semantics — the three-state model

Every indicator row (except `top_pct` which is population-level and `min_price` which has no separate value) supports a 3-state mode controlled by two checkboxes plus a threshold input:

| State | Filter checkbox | Display Only checkbox | Behavior |
|-------|----------------|----------------------|----------|
| **Off** | unchecked | unchecked | Indicator NOT computed. Column does NOT appear in output. |
| **Filter** | checked | unchecked | Indicator computed. Funnel filter applied — drops rows below threshold. |
| **Display Only** | unchecked | checked | Indicator computed. Funnel filter SKIPPED. Cells render in default color if value would have passed the threshold, in **red** if it would have failed. |

**Mutex enforced at GUI level only.** The scanner doesn't care if both are True — it would compute AND filter — but the GUI prevents it.

**The threshold input is always editable**, in both Filter mode and Display Only mode. Threshold is needed for filtering in Filter mode and for the red-on-fail comparison in Display Only mode.

**Beats min=0 special case.** The `consec_eps_beats_min` and `consec_rev_beats_min` spinboxes accept 0 as a valid threshold. With min=0:

- In Filter mode, every ticker passes (streak ≥ 0 is always true).
- In Display Only mode, no cell ever flags red (streak < 0 is impossible).

The user can then surface the streak count + Q-i blocks for context without any threshold gating or red coloring at all.

---

## Global earnings filter mode (toolbar toggles)

Two checkboxes at the **leftmost** position of the main toolbar — `Earnings Dates` and `Earnings Data` — control how the corresponding subsets of earnings filters handle tickers without coverage. **Replace the 9 per-row `Include No Data` checkboxes** that previously lived on each earnings indicator row, and split the prior unified "Earnings Only" toggle into two independent flags because dates and data are conceptually distinct coverage signals.

| Toggle | Gates these filters | Coverage signal |
|--------|---------------------|-----------------|
| **Earnings Dates** | `days_since_earnings`, `days_until_earnings`, `days_until_max` | calendar `last_report_date` / `next_earnings_date` derived |
| **Earnings Data** | `reported_eps`, `surprise_eps_dollar/pct`, `reported_rev`, `surprise_rev_dollar/pct`, consec EPS / Rev beats | Zacks reported result values |

| State | Filter behavior on NaN values in that filter's column |
|-------|--------------------------------------------------------|
| **OFF** (default) | NaN passes the filter cleanly. Non-earnings filters (SMA, gap, surge, etc.) can still select tickers Zacks doesn't cover. The funnel mask is `(value >= threshold) \| value.isna()`. |
| **ON** | NaN fails the filter. Only tickers with actual data make it through. The funnel mask is `value >= threshold` — NaN evaluates False and drops the row. |

**Dates ⊇ Data invariant.** The dates filter is always a SUPERSET of the data filter — anything with earnings data necessarily has an earnings date. To enforce this, when `earnings_dates_only=True` and a row's calendar date column is NaN but the row has any non-NaN value across the 6 most-recent-quarter Zacks data columns, the dates filter still passes the row. This means a ticker can have stale or missing calendar coverage but still pass the dates filter as long as Zacks gave us actual results.

**Wired into:** [scanner.py:`ScanParams.earnings_dates_only` + `earnings_data_only`](trade_scanner_fh/scanner.py) (two `bool` fields, both default False). Read from `MainWindow.chk_earnings_dates_only` / `chk_earnings_data_only` and passed into every period's `ScanParams` via `IndicatorPanel.build_scan_params(start, end, earnings_dates_only=..., earnings_data_only=...)`.

Same flags drive both the funnel mask (in `_build_filter_stages`) and the display-only red-on-fail (in `_compute_display_only_fails`) so the visual signal is consistent with what the filter would have done.

**Pandas-NaN safety:** the funnel masks all use `(v >= t) | (v.isna() & nan_passes)` which never raises on NaN — both branches evaluate cleanly regardless of how many NaN cells the column has. Verified by `tests/test_phase7_filters.py::test_earnings_data_only_drops_nan_rows_else_passes_them` and `test_earnings_dates_filter_respects_data_implies_date_invariant`.

---

## View-only filters (post-scan)

Two checkboxes immediately to the right of the **Timeframe** dropdown filter what's **displayed** in the results table without re-running the scan. Both states persist across timeframe switches and across preset save/load.

| Checkbox | Behavior |
|----------|----------|
| **Earnings Dates (view)** | Hides rows that have NEITHER any earnings-date column populated (`last_report_date`, `next_earnings_date`, `days_since_er`, `days_until_er`, plus q-beats `q{i}_report_date_eps/_rev` columns) NOR any earnings-data value. Broader than the data filter — anything with data passes (data ⇒ date). |
| **Earnings Data (view)** | Hides rows where ALL earnings-data columns are NaN. Coverage set = the 6 most-recent-quarter columns (`reported_eps`, `surprise_eps_dollar/pct`, `reported_rev`, `surprise_rev_dollar/pct`) PLUS any populated q-beats data columns (`q{1..20}_reported_eps`, `q{1..20}_surprise_*`, etc.) — so a ticker with multi-quarter beats data passes even when its most-recent-quarter cells are NaN. |
| **Color Match Only** | Hides rows where `_earnings_aligned_dates` is empty / NaN / missing — i.e., tickers without at least one indicator-date-on-an-earnings-date match. |

All three filters are AND-combined when more than one is on. Invariant: a row passing the data view filter also passes the dates view filter (data implies date) — the dates check explicitly OR's in the data coverage mask to enforce this.

**Implementation:** [main_window.py:`_apply_view_filters(df)`](trade_scanner_fh/gui/main_window.py) returns a filtered copy of `df`. Wired into:

- **Initial post-scan render** (`_on_scan_done_impl`): the active period's df runs through the filter before populate.
- **Timeframe switch** (`_on_timeframe_changed`): same — every switch re-applies the current filter state.
- **Toggle change** (`_reapply_view_filters_for_active_period`): re-renders the active period when the user flips either checkbox.
- **Excel/CSV export** (`_write_xlsx_multi_sheet`, `_write_csv_export`): each selected period runs through the filter so what's exported matches what's displayed. Per-period filtering — multi-period exports filter each period independently.
- **Send-to-Watchlist** (`_send_to_watchlist`): reads `results_table.get_symbols()` which is already populated from the filtered df.

**What's NOT affected:** `self._period_results` always holds the unfiltered scan output. Toggling a view filter off restores all rows without re-scanning.

**Preset persistence (v4):** `view_earnings_dates_only`, `view_earnings_data_only`, and `view_color_match_only` save/load via QSettings-backed preset JSON. Loading a preset with view-filter state on a live scan triggers an immediate re-render of the active period. v3 presets with the legacy `view_earnings_only` key load cleanly — that key maps onto `view_earnings_data_only` (matches the v3 semantics, which checked the 6 most-recent-quarter columns).

---

## Display-only mode & red-on-fail coloring

Implemented at:
- **Scanner side**: `scanner._compute_display_only_fails(params, row)` (scanner.py:876-1040) returns a `{column_key: True}` dict for cells that would have failed the threshold. Stashed on the row as `_display_only_fails`.
- **Widget side**: `ResultsTable._populate_row` (widgets.py:2489-2672) reads `_display_only_fails` and applies `_FAIL_RED` (`#e74c3c`) foreground to flagged cells.

**Order of foreground precedence** (later wins on conflict):
1. Default text color
2. Streak-green (`_STREAK_GREEN = #4caf50`) for cells inside an active beats streak
3. Red-on-fail (`_FAIL_RED`) for display-only flagged cells
4. Earnings-alignment palette color (most specific signal — see next section)

**To add red-on-fail support to a new filter**, add a corresponding `_flag_min` / `_flag_max` call (or a custom block) inside `_compute_display_only_fails` matching the filter's comparison logic in `_build_filter_stages`. Use the same column-key the value lives under in the row dict.

---

## Surge Detection modes

Four modes ship in [`indicators.py`](trade_scanner_fh/indicators.py), selected via the **Mode** combobox in the surge row of the indicator panel and dispatched in [`scanner.py:_compute_ticker`](trade_scanner_fh/scanner.py).

| Mode (`surge_mode`) | Detector | What it picks as Surge Start |
|---------------------|----------|------------------------------|
| `trend` (default) | `surge_trend_continuous` | The earliest bar from which the rally to the global peak never gave back more than `Max DD %` from any intermediate running high. Catches the entire run-up, including any leading capitulation low. |
| `ignition` | `surge_ignition` (wraps trend) | Re-anchors the start to the **catalyst bar** — first bar inside the trend rally whose day-over-day close gain ≥ `Day %` AND volume ≥ `Vol ×` × median of the prior 20 days' volumes. If no qualifying bar exists, falls back to the trend start. |
| `close` | `surge_detection(use_high_low=False)` | Legacy fixed-window: maximum close-to-close gain over `Days` rolling windows. |
| `high_low` | `surge_detection(use_high_low=True)` | Legacy fixed-window: maximum low-to-high (intraday-aware) gain over `Days` rolling windows. |

**Per-mode field activation** (others are greyed and visibly muted via `IndicatorRow._GREYED_INPUT_STYLE`):

| Mode | `Min %` | `Max DD %` | `Vol ×` | `Day %` | `Days` |
|------|:-------:|:----------:|:-------:|:-------:|:------:|
| trend | ✓ | ✓ | – | – | – |
| ignition | ✓ | ✓ | ✓ | ✓ | – |
| close | ✓ | – | – | – | ✓ |
| high_low | ✓ | – | – | – | ✓ |

**Ignition reported `surge_pct`** is the gain from the catalyst close to the global peak — NOT the full trend-continuous run-up. This makes the % a useful "what would I have caught if I'd entered on the catalyst" number. The trend's `Min %` filter still applies (so a tiny post-catalyst move below the threshold is filtered out at the funnel layer).

**Non-strict fallback:** ignition mode does NOT filter out rallies that lack a catalyst bar — it just falls back to the trend-continuous start so the row still surfaces. To make ignition strict (require a catalyst), tighten `Vol ×` and `Day %` until the funnel's `Min %` filter knocks out non-catalyst rallies (whose post-trend-start gain remains under the threshold).

**Why ignition is built on top of trend rather than as a standalone detector:** the user wants the SAME definition of "what counts as a rally" (drawdown-gated continuity to global peak) — only the START label changes. Wrapping `surge_trend_continuous` keeps the rally-detection logic in one place; future tweaks (a new drawdown definition, etc.) automatically apply to both modes.

**Tests:** [`test_surge_trend_continuous.py`](trade_scanner_fh/tests/test_surge_trend_continuous.py) (trend mode + UI) and [`test_surge_ignition.py`](trade_scanner_fh/tests/test_surge_ignition.py) (ignition algorithm + greyout + preset round-trip + scanner dispatch + filter-window cap regression).

---

## Match-color anchoring system

When a non-earnings indicator date (max_gap_date, surge_start_date, up_gap_start_date, down_gap_start_date, min_gap_date, surge_end_date) matches a ticker's earnings report date, the scanner stashes that date in `_earnings_aligned_dates` (scanner.py:853-862). The widget then assigns each matched date a deterministic-random palette color (per-ticker seeded so cross-ticker matches get distinct colors) and **paints every cell in the matched "unit" with that color**.

### How the unit definition works

The widget defines an "anchor date" for each column via `_anchor_date_value(key, row_data)` (widgets.py:1262-1298):

| Column type | Anchor date |
|-------------|-------------|
| Date column (e.g. `max_gap_date`, `q3_report_date_eps`) | self |
| Static indicator value (`max_gap_pct`, `surge_pct`, etc.) | mapped via `_INDICATOR_VALUE_TO_DATE` |
| Q-i value (`q3_reported_eps`, `q3_surprise_rev_dollar`, etc.) | same-quarter date column (`q3_report_date_eps` / `q3_report_date_rev`) |
| Most-recent earnings (`reported_eps`, `surprise_rev_pct`, etc.) | `last_report_date` if present, else `q1_report_date_eps` / `_rev` |
| Anything else (symbol, close, pct_gain, streak counts) | `None` — no match-coloring |

For each cell, the widget looks up its anchor date value, checks if that value is in `aligned_color_map`, and applies the matching palette color. This means an entire unit (e.g. `max_gap_pct` + `max_gap_date`, or a Q-i triplet `q3_report_date_rev` + `q3_reported_rev` + `q3_surprise_rev_dollar` + `q3_surprise_rev_pct`) shares one color.

### The palette (`_ALIGN_PALETTE`, widgets.py:1934-1945)

10 hand-curated colors restricted to the cool half of the wheel (cyan → teal → blue → indigo → purple → violet). Excludes:
- Reds: hue ∈ [330°, 30°]
- Yellows: hue ∈ [30°, 90°]
- Greens: hue ∈ [90°, 165°]
- Brightness < 150

Test guard: `test_aligned_palette_excludes_red_yellow_green_families` in `tests/test_column_reorder_and_palette.py`.

### Per-match randomization

For each unique aligned date in a row, the picker seeds `random.Random(f"{symbol}|{iso_date}")` and draws a base palette index, then linear-probes forward to avoid in-row collisions. Properties:
- Stable: same `(ticker, date)` re-renders the same color across sorts and timeframe switches.
- Cross-ticker independence: AAPL and MSFT on the same date almost always get DIFFERENT colors.
- Within-row uniqueness: distinct dates in the same row always get distinct colors (up to palette size).

**To add a new column to the match-color system**, just add an entry to `_INDICATOR_VALUE_TO_DATE` (for static columns) or rely on the Q-i regex match in `_anchor_date_value` (for new q-block schemas). Make sure the anchor date column is itself populated in the row dict.

### Fuzzy match tolerance (±N calendar days)

Default tolerance is **±1 day** — covers common timing offsets (after-hours report vs next-day price reaction, half-day weekend offset, calendar-provider rounding). User-tunable via **Settings → Color Match Tolerance…** (range 0–7; 0 = exact-only legacy behavior). Persists across sessions via `QSettings` under `match_color/tolerance_days`. Wired through:

- [`ScanParams.match_color_tolerance_days`](trade_scanner_fh/scanner.py) — defaults to 1.
- `MainWindow._match_color_tolerance` (loaded in `__init__` via `_load_match_color_tolerance_pref`, saved via `_save_match_color_tolerance_pref`).
- Passed into every period's params via `IndicatorPanel.build_scan_params(..., match_color_tolerance_days=...)`.

**Affects ONLY the visual color pairing** — does not change any funnel filter, ticker count, or earnings statistic. Already-rendered results are not re-coloured when the tolerance changes; the new value applies to the NEXT scan.

**Canonical map (paired-color sharing):** when an indicator date X matches a report date Y at offset ≠ 0, both X and Y end up in `_earnings_aligned_dates`. Naively this would assign each its own color (different ISO → different palette seed). To make the pair render in ONE color, the scanner also emits `_earnings_aligned_canon: dict[iso, canonical_iso]` mapping every matched date to its match's canonical (always the report date). The widget keys the color map off canonical isos, so X-cell and Y-cell both look up to Y's color. Tie-break when multiple report dates fall within tolerance: pick the closest (then earliest on ties) — deterministic and stable.

**Backward compat:** exact matches (offset == 0 for all matched indicators in a row) emit no canonical map — the row payload stays lean, and the widget falls back to the legacy one-color-per-iso seeding. Unit-tested by `test_exact_match_does_not_emit_canonical_map` and `test_widget_falls_back_when_no_canonical_map`.

---

## Adding a new indicator

Five steps. Concrete example: adding "Volume-Weighted Average Price (VWAP) distance %".

### Step 1: Add the pure indicator function (`indicators.py`)

```python
def vwap_distance_pct(df: pd.DataFrame, *, lookback: int = 20) -> float:
    """
    VWAP distance % = (close - VWAP) / VWAP * 100 over the last `lookback` bars.
    Positive = price above VWAP, negative = below.
    """
    if len(df) < lookback or "Volume" not in df.columns:
        return np.nan
    tail = df.iloc[-lookback:]
    close = tail["Close"].iloc[-1]
    typical = (tail["High"] + tail["Low"] + tail["Close"]) / 3.0
    vol = tail["Volume"]
    vwap = (typical * vol).sum() / vol.sum() if vol.sum() > 0 else np.nan
    if not np.isfinite(vwap) or vwap == 0:
        return np.nan
    return (close - vwap) / vwap * 100.0
```

Keep the function pure: takes a DataFrame slice, returns a scalar (or tuple if it also produces a date — see `max_positive_gap` for the pattern).

### Step 2: Add `ScanParams` fields (`scanner.py:92-405`)

```python
# In the appropriate section (Trend / Momentum / Volume / etc.):
vwap_dist_enabled: bool = False
vwap_dist_display_only: bool = False
vwap_dist_lookback: int = 20
vwap_dist_min: float = 0.0     # default: anything above VWAP
```

### Step 3: Wire computation into `_compute_ticker` (`scanner.py:436-873`)

```python
if params.vwap_dist_enabled or params.vwap_dist_display_only:
    row["vwap_dist_pct"] = indicators.vwap_distance_pct(
        window, lookback=params.vwap_dist_lookback,
    )
```

Use the `enabled OR display_only` gate — without it, display-only mode wouldn't compute the value.

### Step 4: Wire filter stage into `_build_filter_stages` (`scanner.py:1047-1411`)

```python
if params.vwap_dist_enabled and not params.vwap_dist_display_only:
    stages.append((
        f"VWAP Dist >= {params.vwap_dist_min:.1f}%",
        lambda df, p=params: df["vwap_dist_pct"] >= p.vwap_dist_min,
    ))
```

Use `enabled AND NOT display_only` — display-only must skip the filter.

### Step 5: Add red-on-fail support (`scanner.py:_compute_display_only_fails`)

```python
_flag_min("vwap_dist", "vwap_dist_pct", "vwap_dist_min")
```

Use `_flag_min` for `>=` filters, `_flag_max` for `<=` filters. For range filters or NaN-include policies, write a custom block (see the `atr` and `days_since_er` blocks in scanner.py for examples).

### Step 6: Add the GUI row (`gui/widgets.py:IndicatorPanel`)

```python
self._add("vwap_dist", "VWAP Distance (%)", [
    {"name": "lookback", "label": "Days", "type": "int",
     "default": 20, "min": 1, "max": 200},
    {"name": "min_pct", "label": "Min %", "type": "float",
     "default": 0.0, "min": -100.0, "max": 100.0, "step": 0.5},
])
```

### Step 7: Add the result column (`gui/widgets.py:RESULT_COLUMNS`)

```python
("VWAP Dist %", "vwap_dist_pct", lambda x: f"{x:+.2f}%"),
```

### Step 8: Wire `build_scan_params` (`gui/widgets.py:IndicatorPanel.build_scan_params`)

```python
vwap_dist_enabled=r["vwap_dist"].is_enabled(),
vwap_dist_display_only=r["vwap_dist"].is_display_only(),
vwap_dist_lookback=r["vwap_dist"].value("lookback"),
vwap_dist_min=r["vwap_dist"].value("min_pct"),
```

### Step 9: Add a test

```python
# tests/test_indicators.py
def test_vwap_distance_pct():
    df = pd.DataFrame({
        "Open": [100, 101, 102, 103],
        "High": [101, 102, 103, 104],
        "Low":  [99, 100, 101, 102],
        "Close":[100, 101, 102, 103],
        "Volume":[1000, 1000, 1000, 1000],
    }, index=pd.date_range("2024-01-01", periods=4))
    result = indicators.vwap_distance_pct(df, lookback=4)
    assert isinstance(result, float)
    # close=103, typicals avg=101.5, equal-weight vwap≈101.5 → +1.48%
    assert result == pytest.approx(1.48, abs=0.01)
```

---

## Adding or modifying a filter

A "filter" without a new indicator (e.g., re-defining the threshold semantics of an existing indicator):

1. Edit the `_build_filter_stages` block at `scanner.py:1047-1411`. Update the lambda to reflect the new comparison.
2. Update the corresponding `_flag_min` / `_flag_max` call in `_compute_display_only_fails` so the red-on-fail logic mirrors the new filter logic. **THIS IS A COMMON FOOT-GUN**: forgetting to update both means display-only mode shows misleading red coloring.
3. Add a regression test that uses the new threshold semantics.

---

## Adding a new OHLCV data source (e.g. Polygon)

The current OHLCV pipeline is yfinance-only via `data_engine.py`. To add Polygon (or any other source) cleanly:

### Architectural pattern: provider interface

Currently `data_engine.download_one(symbol)` calls `yf.download(...)` directly. To support multiple providers without forking the cache:

**Step 1: Define a provider interface.** Create `data_providers.py`:

```python
from abc import ABC, abstractmethod
from datetime import date
import pandas as pd
from dataclasses import dataclass
from typing import Optional

@dataclass
class OHLCVResult:
    symbol: str
    df: Optional[pd.DataFrame]   # DatetimeIndex, OHLCV columns
    status: str                  # "ok" | "no_data" | "error"
    error_msg: str = ""

class OHLCVProvider(ABC):
    name: str

    @abstractmethod
    def fetch(
        self, symbol: str, *,
        start: Optional[date] = None, end: Optional[date] = None,
    ) -> OHLCVResult: ...

    @abstractmethod
    def fetch_many(
        self, symbols: list[str], *,
        start: Optional[date] = None, end: Optional[date] = None,
        progress_cb=None,
    ) -> list[OHLCVResult]: ...
```

**Step 2: Wrap the existing yfinance code as `YFinanceProvider`** in `data_providers.py`. Move the yfinance imports + `yf.download()` call out of `data_engine.py` into this provider.

**Step 3: Implement `PolygonProvider`** in the same file:

```python
class PolygonProvider(OHLCVProvider):
    name = "polygon"

    def __init__(self, api_key: str):
        self._api_key = api_key
        self._session = requests.Session()
        self._base = "https://api.polygon.io/v2/aggs/ticker"

    def fetch(self, symbol, *, start=None, end=None) -> OHLCVResult:
        # /v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}
        url = f"{self._base}/{symbol}/range/1/day/{start}/{end}"
        try:
            resp = self._session.get(
                url, params={"apiKey": self._api_key, "limit": 50000}, timeout=20,
            )
            if resp.status_code == 429:
                # Rate-limited — caller should back off
                return OHLCVResult(symbol, None, "error", "rate_limited")
            data = resp.json()
            if data.get("status") != "OK" or not data.get("results"):
                return OHLCVResult(symbol, None, "no_data")
            # Polygon returns: [{"v": vol, "o": open, "h": high, "l": low,
            #                   "c": close, "t": ms_timestamp, ...}]
            df = pd.DataFrame(data["results"])
            df.index = pd.to_datetime(df["t"], unit="ms").dt.normalize()
            df = df.rename(columns={
                "o": "Open", "h": "High", "l": "Low",
                "c": "Close", "v": "Volume",
            })[["Open", "High", "Low", "Close", "Volume"]]
            return OHLCVResult(symbol, df, "ok")
        except Exception as exc:
            return OHLCVResult(symbol, None, "error", str(exc))

    def fetch_many(self, symbols, *, start=None, end=None, progress_cb=None):
        # Polygon free tier = 5 calls/min; paid tiers = much higher.
        # Use a thread pool with rate-limit-aware throttling.
        from concurrent.futures import ThreadPoolExecutor, as_completed
        results = []
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {
                pool.submit(self.fetch, s, start=start, end=end): s
                for s in symbols
            }
            for i, fut in enumerate(as_completed(futures), 1):
                results.append(fut.result())
                if progress_cb:
                    progress_cb(i, len(symbols), futures[fut])
        return results
```

**Step 4: Refactor `data_engine.py` to delegate to a provider:**

```python
# In data_engine.py
from . import config

_provider_cache = None

def get_provider() -> OHLCVProvider:
    global _provider_cache
    if _provider_cache is None:
        from .data_providers import YFinanceProvider, PolygonProvider
        if config.OHLCV_PROVIDER == "polygon":
            key = os.environ.get("POLYGON_API_KEY") or config.POLYGON_API_KEY
            _provider_cache = PolygonProvider(key)
        else:
            _provider_cache = YFinanceProvider()
    return _provider_cache

def download_one(symbol: str) -> ScrapeResult:
    provider = get_provider()
    # ... existing incremental-update logic ...
    result = provider.fetch(symbol, start=start_date, end=end_date)
    # ... existing parquet-write logic ...
```

**Step 5: Add config knobs** (`config.py`):

```python
OHLCV_PROVIDER = "yfinance"  # "yfinance" | "polygon"
POLYGON_API_KEY = ""         # also overridable via POLYGON_API_KEY env var
```

**Step 6: Add a Data menu item** (`gui/main_window.py`) for "Set Polygon API Key..." that prompts and persists via QSettings (or `keyring`).

**Step 7: Update PyInstaller spec** (`Trade_Scanner_FH.spec`) — no new hidden imports needed if `requests` is already bundled.

**What's preserved:** the parquet cache schema, `load_ohlcv`, all the indicator math, the entire scanner pipeline. Provider swap is invisible to everything downstream.

**What's NOT preserved:** the `ScrapeReport` summary text mentions yfinance specifically — update or genericize. The validation logic in `data_engine.validate_ticker` may flag Polygon-specific quirks (different missing-bar policy, different volume scale). Run validation on a sample post-cutover.

---

## Adding a new earnings data source

Earnings has four providers today (Zacks + Finnhub for history; Nasdaq + Yahoo for dates). To add a fifth (say IEX Cloud as a history source):

1. Implement the fetch function in a new module — return rows in `earnings_history.COLUMNS` schema with `source="iex"`. Stamp `period_ending` to **day-1 of the fiscal-quarter month** at row construction (matching the Zacks/Finnhub convention) so cross-source dedup keys match.
2. Add bulk/targeted fill functions following the `finnhub_fill.bulk_fill_finnhub` template — writes raw rows to `earnings_raw/iex/<run_id>.parquet`, then collapses to the consumer schema. Use `_flush_pending_to_disk` from `earnings_history.py` (per-`(ticker, source)` PK replacement) so re-pulls don't accumulate dups.
3. Update `dedupe_history` if the new source needs binary-policy treatment beyond Zacks vs. Finnhub. Current rule: Zacks wins ticker-level over Finnhub. For a third history source, decide whether it's authoritative-over-Zacks (rare), peer-with-Zacks (binary policy across all three), or last-resort-like-Finnhub.
4. Update `_LAST_PRIORITY` / `_NEXT_PRIORITY` in `earnings_reconcile.py` to insert the new source in the chain. Current order: `zacks > nasdaq > yahoo > finnhub`. For IEX-as-history, likely "after Zacks, before Nasdaq" — making it `zacks > iex > nasdaq > yahoo > finnhub`.
5. Add a Data menu action to trigger the IEX fill — follow the pattern at `gui/main_window.py` for the existing per-source actions (Bulk / Targeted / Spot, with Stop button + worker thread).
6. Add the source to the `cross_source_period_overlap` integrity check if the policy applies (it's currently zacks-vs-finnhub specific).
7. The scanner's `earnings_history_lookup` is source-agnostic — it just reads the parquet — so no changes there.

---

## Editing mathematical assumptions of existing filters

All math lives in **`indicators.py`** as pure functions. Change a definition there and it propagates through the whole pipeline.

### Common edits and where they live

| Edit | File:location |
|------|---------------|
| Change SMA from simple to exponential | `indicators.price_above_sma` (replace `.rolling().mean()` with `.ewm().mean()`) |
| Change ATR from simple-mean to Wilder's smoothing | `indicators.atr_value` (replace the trailing-N mean with EMA(α=1/period)) |
| Change RS calculation from arithmetic to log returns | `indicators.relative_strength_ratio` |
| Change BBW divisor from middle-band to absolute (Keltner-style) | `indicators.bollinger_band_width` |
| Change consecutive-beats cadence test from period_ending to fiscal-year | `earnings_history.compute_consecutive_beats` (currently uses period_ending — see comments inside for the late-filing rationale) |
| Change "missing quarter" threshold from 135 to 200 days | `earnings_history._MAX_QUARTER_GAP_DAYS` |
| Change surge from close-to-close to high-to-low default | `IndicatorPanel._add("surge", ...)` change `default: "trend"` → `"high_low"` |

### Things that look like math but aren't

- `ScanParams` defaults are starting values, not assumptions. Changing them only affects new presets / first-launch behavior.
- `RESULT_COLUMNS` formatters control display only — `f"{x:.2f}%"` doesn't change the underlying value.
- The funnel filter lambdas in `_build_filter_stages` are comparison logic, not math. The math is upstream in `indicators.py`. If you change a comparison (e.g. swap `>=` for `>`), update the matching `_flag_min`/`_flag_max` in `_compute_display_only_fails` so red-on-fail mirrors filter behavior.

### Cadence-detection precedent (BUG-9 case study)

`compute_consecutive_beats` uses `period_ending` for cadence detection (not `report_date`). Reasoning embedded in the docstring at `earnings_history.py:329-362`:

> A ticker that beats every quarter for 5 quarters but delays Q3's announcement by 6 weeks would have report_date gaps of ~88 / ~88 / 132 / 175 / 88 days. The 175-day report_date gap would falsely truncate the streak under the prior implementation. Under period_ending the gaps are all ~91 days, no false break.

If you decide to revert this for any reason (e.g., you actually WANT late filings to break the streak as an audit-suspicious signal), the change is one line at `earnings_history.py:388`:
```python
peds = pd.to_datetime(df["period_ending"], errors="coerce")  # → df["report_date"]
```
And update the docstring. The test `test_beats_late_announcement_does_not_break_streak` would need to flip its assertion.

---

## The Zacks scraper subsystem

Live at `zacks_scraper.py`. Key facts:

- **TLS impersonation via `curl_cffi`** (chrome131 profile). Imperva's bot detection is a JA3/JA4 fingerprint check at the TLS handshake; plain `requests` is rejected before any HTTP-level evasion can apply. `curl_cffi` emits a TLS Hello byte-for-byte identical to real Chrome.
- **HTTP-only — no browser engine**. Playwright was tried and dropped (see comments at scraper.py:7-22). Every browser engine (headless or headful Chromium / Firefox / patchright) gets caught at the TLS layer.
- **Parses the embedded `document.obj_data = {...}` JS object** rather than scraping the rendered DOM. Faster, more robust to layout changes.
- **Cookie jar**: file-backed at `scanner_data/zacks_cookies.txt`. Stored as a `name=value; name=value; ...` header string (matches the format used to inject into `curl_cffi.Session.cookies`).
- **Failure classification** (FAIL_BLOCKED / FAIL_NOT_FOUND / FAIL_HTTP_ERROR): drives the auto-pause heuristic. Only confirmed Imperva blocks count toward the consecutive-failure threshold. A fourth sentinel, `FAIL_PARSE_ERROR`, is defined but currently unassigned — an unparseable `obj_data` blob falls through to FAIL_NOT_FOUND.

### `ZacksSession` lifecycle

```python
with ZacksSession() as session:
    for sym in tickers:
        rows = session.fetch(sym, years=5)
        if rows is None:
            kind = session.last_failure_kind  # FAIL_* sentinel
        ...
```

The session injects file-backed cookies on enter, can `refresh_cookies()` mid-session (called by the auto-pause flow after the user supplies fresh cookies), and is closed on exit.

### Filling earnings_history

Two modes wrap `_fill_via_zacks` (earnings_history.py):

| Mode | Trigger | Tickers |
|------|---------|---------|
| **Bulk fill** | `Data → Bulk Fill Earnings (Zacks)` menu | Universe ∩ ¬blacklist (~15k, ~6.5h at 1.5s pacing) |
| **Targeted fill** | `Data → Targeted Fill Earnings (Zacks)` menu | `find_gap_tickers` → tickers with no rows yet |

Finnhub side has three additional modes (`finnhub_fill.py`):

| Mode | Trigger | Tickers |
|------|---------|---------|
| **Bulk fill** | `Data → Bulk Fill Earnings (Finnhub)` menu | Universe ∩ ¬blacklist; resumable via `FINNHUB_BULK_CHECKPOINT` |
| **Gap fill** | `Data → Gap Fill Earnings (Finnhub)` menu | `find_finnhub_gap_tickers` → tickers absent from history |
| **Spot fill** | `Data → Spot Fill Earnings (Finnhub)...` menu | One ticker by name |

All modes:

- Share the same flush logic (`_flush_pending_to_disk`) — every N successful pulls writes to `earnings_history.parquet` atomically (replace-by-`(ticker, source)`-PK semantics so re-pulls don't duplicate quarters; canonicalization-safe — uses the row's `ticker` field for the mask, not the pending dict key).
- Final `_finalize_fill` does one canonical sort + one `reconcile_earnings_dates` call.
- All writes go through `config.atomic_write_parquet` (tmp file + `os.replace`) — crash-safe.
- Zacks runs are 100% manual. Auto-launch refresh has been removed; the only auto-fill trigger is the once-per-week Nasdaq calendar sweep.

---

## Cookie acquisition flow (Firefox + mid-flight capture)

When Imperva starts blocking (5 consecutive `FAIL_BLOCKED` failures — the `consec_error_limit` default), the worker pauses and the GUI launches Firefox at the persistent profile (`scanner_data/firefox_zacks_profile/`). The user solves any CAPTCHA / login challenge.

### Mid-flight capture (May 2026)

`FirefoxCookieWaitWorker` (workers.py:945-1137) polls `cookies.sqlite` via stdlib `sqlite3` with `mode=ro&immutable=1` — works while Firefox holds the file. Two completion paths in one loop:

1. **Mid-flight**: cookies.sqlite contains BOTH `reese84` AND `visid_incap_*` AND the signature differs from pre-launch → capture immediately, persist via `set_zacks_cookies()`, emit `finished(success=True)` while Firefox stays open.
2. **On-close fallback**: psutil cmdline-match detects Firefox closed → final read + persist.

The pre-signature check prevents insta-capture of stale cookies the profile carried over.

### Firefox launch (Issue #3 hardening)

`launch_firefox_for_zacks_cookies` (zacks_scraper.py:739-840) uses:
- Persistent profile dir at `scanner_data/firefox_zacks_profile/`
- `user.js` written each launch (`_USER_JS_CONTENT`, zacks_scraper.py:432-469) suppresses first-run dialogs AND forces `browser.startup.page=0` + `browser.sessionstore.resume_session_once=false` so the cmdline URL is the only thing Firefox loads
- Launch argv: `firefox -no-remote -foreground -profile <path> -new-window <url>`. The explicit `-new-window` defeats sessionstore restoration that some Firefox versions apply to trailing positional URLs.

### Optional monitor placement

`Settings → Set Cookie Browser Monitor to Current Window` captures the scanner's current monitor geometry and persists via `QSettings`. Future Firefox launches open maximized on that monitor. Useful for multi-monitor setups.

---

## GUI subsystem

### Threading model

| Thread | What runs |
|--------|-----------|
| Main (Qt event loop) | All widgets, model updates, slot handlers, `populate()` |
| ScanWorker | `run_scan()` — heavy CPU loop |
| ZacksFillWorker | The 6.5h Zacks fill — uses `_on_imperva_block` callback to coordinate with main thread for cookie refresh |
| FinnhubFillWorker | Finnhub deep-history bulk / gap / spot fills |
| UpdateWorker / UniverseWorker / UniverseRefreshWorker / SectorFillWorker / EarningsFillWorker | Background data refreshes |
| FirefoxCookieWaitWorker | Polls Firefox + cookies.sqlite |
| BridgeWorker | TradeStation watchlist push |

Cross-thread coordination uses Qt signals (queued connections) for one-way flow and `threading.Event` for the auto-pause two-way coordination (worker blocks on Event, main thread sets Event after dialog).

### Crash hardening (post-2026-05 audit)

- `ScanWorker.run()` wrapped in try/except — emits `finished` with partial results on crash instead of dying silently.
- `_on_scan_done` slot wrapped — shows "Scan crashed — see log" banner instead of taking down the exe.
- `ResultsTable._populate_row` is per-row try/except — a single bad row leaves an empty cell instead of crashing the whole table.
- `_safe_streak` helper handles NaN/None/non-numeric (Python's `bool(nan) is True` would otherwise turn `int(nan or 0)` into a crash).
- `LogPanel._append` flushes after every write so post-mortem diagnostics actually have data.
- `ResultsTable.populate` chunks via `QApplication.processEvents()` every 200 rows so Windows can't flag the process as "not responding" during a 15k-run render.

### Data menu structure

The Data menu is split into groups by separators. The five earnings /
cross-check / diagnostics groups each open with an inline disabled-action
label row (`— Earnings (Zacks — primary) —`, etc.); the Universe/OHLCV and
Sector-map groups are separator-divided only — they have no label row. The
grouping reflects the four-source architecture plus diagnostics:

```text
(Universe & OHLCV — separator-grouped, no label row)
    Force Universe Refresh
    Force OHLCV Refresh
    Download Missing Tickers Only
    Stop OHLCV Refresh
    Reset yfinance Session
    Rebuild Tickers...
    Set Finnhub API Key...
    Enable Rate-Limit Backoff            (checkable)
    Backoff Settings...
    Ticker Blacklist...
    Ticker Greylist...

(Sector map — separator-grouped, no label row)
    Bulk Fill Sector Map
    Targeted Fill Sector Map
    Stop Sector Fill

— Earnings (Zacks — primary) —
    Bulk Fill Earnings (Zacks)
    Targeted Fill Earnings (Zacks)
    Stop Zacks Fill
    Set Zacks Cookies...
    Refresh Zacks Cookies (Open Browser)...
    Show Last Zacks Failures...
    Edit Zacks Skip List...

— Earnings (Finnhub — deep history) —
    Bulk Fill Earnings (Finnhub)
    Gap Fill Earnings (Finnhub)
    Spot Fill Earnings (Finnhub)...
    Stop Finnhub Fill
    Edit Finnhub Skip List...

— Earnings dates (Nasdaq + Yahoo) —
    Bulk Fill Earnings Dates (Nasdaq)
    Targeted Fill Earnings Dates (Yahoo)
    Spot Fill Earnings Dates (Yahoo)...
    Stop Earnings-Dates Fill
    Auto-refresh Nasdaq calendar weekly  (checkable)

— Cross-Check & Reconcile —
    Reconcile earnings_dates.parquet

— Diagnostics —
    Earnings Coverage Report...
    Verify earnings_history Integrity...
    Data Coverage Gaps...
```

### Settings menu structure

```text
Set Cookie Browser Monitor to Current Window
Clear Cookie Browser Monitor
—
Hotkey Settings…
Color Match Tolerance…
—
Set SEC Contact Email…
```

- **Set / Clear Cookie Browser Monitor** — pin (or forget) the monitor the
  Firefox cookie-refresh browser opens on. Persisted via `QSettings`.
- **Hotkey Settings…** — per-row HOTKEY ticker-sender config (see
  [Per-row HOTKEY ticker sender](#per-row-hotkey-ticker-sender)).
- **Color Match Tolerance…** — fuzzy-day window (0–7, default ±1) for the
  match-color date pairing.
- **Set SEC Contact Email…** — the contact email SEC EDGAR requires in the
  request User-Agent for the ticker-universe download. Stored in
  `scanner_data/sec_contact.txt` (or via the `SEC_CONTACT_EMAIL` env var);
  until set, the SEC universe source is skipped.

Each fill writes to its dedicated parquet (`earnings_history` for Zacks/Finnhub; `earnings_dates` for Nasdaq/Yahoo) and triggers an auto-reconcile against affected tickers. Internal identifiers, slot method names (e.g. `_on_zacks_fill_done`, `_on_finnhub_fill_done`), and parquet `source` column values use bare source names (`"zacks"`, `"finnhub"`, `"nasdaq"`, `"yahoo"`).

### Delete rows from results table

Hard-delete rows from the active period's scan output via:

- **`Delete` key** when one or more rows are selected
- **Right-click → "Delete selected row(s)"** on the results table

Multi-select via standard Qt extended selection (shift-click for ranges, ctrl-click for non-contiguous). The deletion mutates `MainWindow._period_results[<active_period>]` directly, so it persists across view-filter toggles, sort changes, timeframe-tab switches, and exports. Reset implicitly on the next scan (fresh `_period_results` overwrites the dict).

`ResultsTable.rows_deletion_requested` signal is the wire — emitted by both triggers, handled by `MainWindow._on_rows_deletion_requested`.

### Cut + Paste rows (manual reorder)

Reorder rows manually via right-click:

1. Select one or more rows (multi-select with shift/ctrl-click).
2. Right-click → **"Cut N selected rows"** stashes the symbols on a single-shot clipboard. Display is unchanged — the rows stay where they are until paste.
3. Right-click on a different row → **"Paste N rows after '<target>'"** moves the cut block to immediately after the target row.

The paste signal (`ResultsTable.rows_paste_requested(cut_symbols, target_symbol)`) is handled by `MainWindow._on_rows_paste_requested`, which mutates `_period_results[active_period]` and re-renders. The reorder persists across view-filter toggles, sort changes, and tab switches.

Clipboard is cleared on paste, on row deletion (any cut symbol may have been deleted), and on the next scan. Pasting onto a row that's part of the cut set is rejected (would orphan the target).

### Delete columns from view

Right-click any column header → **"Delete N columns"** removes those columns from the rendered table. Hidden columns persist across view-filter toggles, sort changes, and tab switches but DON'T touch `_period_results` — the scan data is preserved, only the rendered slice is trimmed.

The always-visible core columns (`symbol`, `close`, `pct_gain`, `gain_start_date`) are filtered out before the deletion request reaches MainWindow — they can never be hidden via this menu (would break export and core display invariants).

Hidden-column set is reset on every fresh scan so a new scan with different filters / column shape doesn't have to fight stale hides.

Multi-column hide: shift/ctrl-click multiple headers to build the multi-select group (selected headers highlight with a marker), then right-click → "Delete N columns".

### Cut + Paste columns (manual reorder)

Symmetric with row cut/paste — reorder columns via the header right-click menu:

1. Multi-select column headers via shift/ctrl-click (selected headers highlight).
2. Right-click → **"Cut N columns"** stashes the selection on a single-shot clipboard.
3. Right-click on a different column header → **"Paste N columns after this"** moves the cut block immediately after the targeted column.

Implementation lives entirely in `ReorderableHeader` — no MainWindow round-trip needed since column order is owned by the Qt header. The paste handler computes the destination visual index by counting cut columns sitting to the LEFT of the target (so they don't double-shift) and calls `_move_block_to_visual`. Emits `order_changed` so any saved column order persists across timeframe switches.

Clipboard is single-shot — cleared on paste, on column delete, and on every fresh `populate()` (column set may have changed).

### Excel export with bundled per-quarter columns

The export dialog (`ExcelExportDialog`) bundles all per-quarter beats columns into two single toggles — **"Consecutive EPS Beats per-quarter columns"** and **"Consecutive Rev Beats per-quarter columns"** — so the user gets one checkbox per side instead of N×5 individual checkboxes for deep streaks. The bundle covers every q-i suffix:

- `q{k}_report_date_{eps,rev}` — the per-block date column
- `q{k}_reported_{eps,rev}` — the actual reported value
- `q{k}_surprise_{eps,rev}_dollar` — surprise in dollars
- `q{k}_surprise_{eps,rev}_pct` — surprise as percent
- `q{k}_yoy_{eps,rev}_pct` — YoY % growth (added 2026-05)

`MainWindow._build_export_df` iterates the LIVE active column layout (RESULT_COLUMNS plus dynamic q-i blocks), so checking the bundle actually flows the q-i values through to XLSX/CSV — fix for the long-standing limitation where bundle-checked q-i columns were silently dropped at export time.

The dialog opens with **every column pre-checked**; the user uses **Select All** / **Select None** plus individual toggles to refine. The earlier "Auto (data only)" button — which pre-checked only columns with non-null values in the active period — was dropped 2026-05 because dynamic q-i columns kept slipping past it and the Select-All / bundle pair already covers the same intent in one click.

### Interleave Quarters (view-only)

Checkbox `Interleave Q EPS+Rev` next to the Color Match Only toggle. When **both** Consec EPS Beats and Consec Rev Beats produce per-quarter blocks, this toggle alternates the layout so all data for one quarter sits adjacent:

```text
Default:     | EPS Beats | Q-1 EPS | Q-2 EPS | … | Rev Beats | Q-1 Rev | Q-2 Rev | …
Interleaved: | EPS Beats | Rev Beats | Q-1 EPS | Q-1 Rev | Q-2 EPS | Q-2 Rev | …
```

No-op when only one side has beats data — guarantees zero behavior change for users who run EPS-only or Rev-only scans. Asymmetric `n_eps != n_rev` (e.g., user set different per-side quarter caps) is handled by emitting whichever side still has data at each quarter index.

Persisted in QSettings + presets under `view_interleave_quarters`. Implementation: `_build_dynamic_columns(df, interleave_quarters=True)` builds the alternating layout; `ResultsTable.set_interleave_quarters(bool)` flips the flag and invalidates the column-width cache; `MainWindow._on_interleave_quarters_toggled` clears any saved column order (a prior manual reorder would otherwise be re-applied AFTER populate and undo the new q-i layout) and re-renders the active period. Regression covered by `test_interleave_toggle_overrides_saved_column_order`.

### Columns dropdown (manual layout + visibility manager)

Toolbar button **`Columns ▾`** (right of IPO Mode Max Days) opens a non-modal popup (`ColumnsManagerDialog`) that lists the currently-active output columns:

- **Top → bottom in the list = leftmost → rightmost in the table.**
- Drag any row (multi-select via Ctrl/Shift) to reorder; the popup pushes changes through to `_results_column_order` in real time so the table re-renders without an Apply step.
- Each row has a checkbox: unchecked = hidden (mirrors `_deleted_column_keys`, the same hidden-set the header right-click → "Delete column" action populates).
- Always-visible core columns (`symbol`, `close`, `pct_gain`, `gain_start_date`) render greyed-out and the checkbox is locked on; the popup also has a runtime guard that snaps them back to checked if a stray Qt event flips them.
- **Reset to Default** button (also available via the table-header right-click "Reset to Default") clears both the manual order AND the hidden set so the next render uses the canonical layout that the current scan settings would produce.

#### Reconcile rules across scans

When a scan completes (`_reconcile_column_order_for_scan`), the saved order is updated as follows:

- **Saved order empty** → leave it empty so canonical wins.
- **New filter/display variable adds an output column** → the new key(s) get **prepended to the front** of the saved order in canonical order (which mirrors the indicator panel's top-to-bottom arrangement, i.e. "first added = leftmost in table"). Existing saved entries keep their relative positions behind the additions.
- **Filter/display variable removed** → its key is dropped from the saved order; the rest of the user's layout survives intact.
- **Hidden set (`_deleted_column_keys`)** carries across scans without modification — users don't have to re-hide noisy columns each run. Reset clears it.

Toolbar input changes that only affect ticker-input shape (timeframe, sequence range, earnings-dates / earnings-data filter, include ETFs / ADRs, IPO mode, etc.) do **not** trigger column changes — they don't add or remove columns from the output, so the saved order passes through unchanged.

#### Preset persistence

Presets save the manual layout under two new v5 keys:

- `column_order`: list of keys in user-preferred sequence (`_results_column_order`).
- `column_hidden`: list of hidden keys (`_deleted_column_keys`).

**Loading a v5 preset wipes the current results window** so the dropdown reflects only the preset's saved column intent (the prior scan's output isn't relevant once the user switches presets, and leaving stale rows on screen risks confusion when the preset's column set differs from what's displayed). The next scan honors the preset's order. Pre-v5 presets that lack the column metadata leave the existing layout alone (legacy v4 behavior preserved).

#### Excel export now respects manual reorder

`_build_export_df` was previously iterating `active_columns` (canonical filtered order) and only checking key membership in the dialog's selection — meaning the user's drag order lived only on screen, not in the saved workbook. Fixed 2026-05: the function now iterates `keys` directly (which carries the visual order from the dialog, populated via `_ordered_active_columns_for_export()`), looking up `(header, fmt_func)` per key from the live layout. Regression covered by `test_excel_export_honors_manual_column_drag_order`.

### Excel export with cell coloration

The export dialog (`ExcelExportDialog`) carries an **"Include cell colors (.xlsx only)"** checkbox alongside the format combo. Behavior:

- **XLSX selected**: checkbox enabled, default checked. When checked, the writer mirrors the on-screen text colors (match-color palette + streak green + display-only fail red) into per-cell font colors via openpyxl. Colors apply only to the active period's sheet (the live table model only holds one period at a time).
- **CSV selected**: checkbox greyed out (CSV has no cell-color concept). `wants_colors()` returns False unconditionally.

Implementation: `MainWindow._apply_xlsx_cell_colors(ws, keys, wants_news)` walks `ResultsTable.model_src` row-by-row, reads each cell's `foreground().color()`, and applies a matching `openpyxl.styles.Font(color=hex)` to the corresponding workbook cell. A safelist (`_is_export_color`) ensures only the curated palette / streak-green / fail-red RGB tuples actually get exported — implicit-default foregrounds are skipped.

### Per-row HOTKEY ticker sender

Independent of the bulk **Send to Watchlist** flow (which pushes the entire results list at once via `tradestation.py` + `BridgeWorker`). HOTKEY mode lets the user fire ONE ticker at a time — picking a row by mouse cue — into any external app's input field via pyautogui.

**Components:**

- `hotkey.py` — Qt-free, headless-testable: `HotkeyConfig` dataclass + `send_ticker(ticker, cfg)` (click → wait → typewrite → end-key). Mirrors `tradestation.py`'s pattern of saving / restoring `pyautogui.FAILSAFE` and `PAUSE` around each invocation.
- `gui/hotkey_dialog.py` — `HotkeySettingsDialog` (modal editor) with an inline position-capture countdown (live cursor-coordinate readout + a `CAPTURE_SECONDS`-long timer, snapshotting `QCursor.pos()` when it elapses). `PositionCaptureCountdown` is a standalone countdown dialog kept for the same purpose; `PositionCaptureOverlay` survives only as a backwards-compat alias — the earlier transparent-fullscreen click-overlay was dropped as unreliable on multi-monitor setups.
- `gui/main_window.py` — bottom-button-bar `btn_hotkey` (right of the Excel button), Settings menu entry, `_load_hotkey_pref` / `_save_hotkey_pref` (QSettings), `_toggle_hotkey`, `_open_hotkey_settings`, and the `eventFilter` that intercepts the cue on the results-table viewport.

**HOTKEY button styling:**

| State | Background | Text color | Notes |
|------:|:-----------|:-----------|:------|
| Off (default) | `#555` grey | `#ddd` | Same neutral pill as other "secondary" buttons |
| On | `#ff1493` hot pink | `#c000ff` bright purple, **bold** | Unmistakable visual hazard signal |

**Settings menu entry — `Settings → Hotkey Settings…`** opens a modal with:

| Control | Range / options | Default |
|---------|-----------------|---------|
| Click position (Set Click Position…) | Any global screen coord | `(not set)` |
| Send cue | Right-click, Shift+Left, Ctrl+Left, Middle, Enter key (selected row) | Right-click |
| Delay click → type | 0 – 5000 ms | 200 ms |
| End sequence | None, Enter, Tab, Ctrl/Shift/Alt+Enter | Enter |
| Return click position (Set Return Click… / Clear) | Any global screen coord, optional | `(not set)` |
| Reset to Defaults | Clears positions + restores defaults | — |

**Click position capture:** the dialog runs an inline countdown — it shows a live readout of the current cursor coordinates plus a 5-second timer (`CAPTURE_SECONDS`), then snapshots `QCursor.pos()` as the target `(x, y)` when the timer elapses. The coords are global screen coords (multi-monitor safe). Cancel / Esc aborts. (The earlier click-intercepting transparent overlay was replaced because it misbehaved across monitors.)

**Cue interception:** `MainWindow.eventFilter` is installed on `results_table.viewport()`. When `_hotkey_enabled` is True AND the QMouseEvent matches `_hotkey_cfg.cue` (button + modifiers), the filter resolves the row under the cursor via `view.indexAt(event.position().toPoint())`, selects it for visual feedback, dispatches `send_ticker` on a daemon `threading.Thread` so the GUI stays responsive, and returns True to swallow the event (suppresses the table's normal context menu / drag-select side effects). When the toggle is off, the filter passes through and the cue (e.g. right-click) does its normal table thing.

**Return click (optional):** when `return_click_x/y` are set, an extra click fires AFTER the end-sequence keystroke. Use case — pair with the `Enter key (selected row)` cue so the full loop is keyboard-only: arrow keys move through the table, Enter fires the send (primary click → type → end-key → return click brings focus back to the scanner), arrow keys move again. Reuses the same `delay_ms` knob between the end-key and the return click so the target app has time to process the submit before focus is stolen back. Toggle off via the `Clear` button next to the position label.

**Persistence:** the config fields (`click_x`, `click_y`, `delay_ms`, `cue`, `end_sequence`, `return_click_x`, `return_click_y`) persist via `QSettings("trade_scanner_fh", "Trade_Scanner_FH")` under the `hotkey/` group. The on/off toggle does **NOT** persist — defaults to off on each launch so a stale screen position can't fire surprise input.

**Edge cases:**

- Clicking HOTKEY when no position is set → prompt "Open Hotkey Settings?" → if user saves a position, auto-flip ON.
- Cue fires on empty area (no row at cursor) → log "Hotkey: no row under cursor — click on a results row." and no-op.
- `_open_hotkey_settings` after the user clears the position via Reset → if HOTKEY was on, force it off so the cue doesn't silently no-op.
- Send sequence runs on a daemon thread so a slow target app (or a 5s delay) doesn't freeze the scanner UI.

Tested in `tests/test_hotkey.py` (28 tests): config defaults, normalization clamping, option enumeration completeness, send guards (no position / empty ticker), full sequence per `end_sequence` variant, pyautogui FAILSAFE/PAUSE restoration on success and on exception, no-clobber-at-import (mirrors the `tradestation.py` invariant).

### "Send All Misses to Zacks Skip List" button

`Data → Show Last Zacks Failures...` opens a per-kind breakdown of the most recent Zacks fill's failures (Imperva blocks, not-found, HTTP errors, parse errors). The dialog has three buttons:

- **Copy All to Clipboard** — copies the full per-kind text dump.
- **Send All Misses to Zacks Skip List** — bulk-adds every ticker in the breakdown (across ALL kinds) to `scanner_data/zacks_blacklist.txt`. Implementation in `MainWindow._send_zacks_misses_to_skip_list`:
  - Normalizes each ticker via `_normalize_ticker` (Unicode dash variants → ASCII hyphen).
  - Dedupes within the batch + against the in-memory `_zacks_blacklist` set.
  - Atomic write via `config.atomic_write_text` — one ticker per line, sorted.
  - Reports counts to the user: `N new added, M already on list, K total unique`.
  - Idempotent — re-clicks after a successful save are no-ops.
  - On write failure, rolls back the in-memory mutations so a retry sees the same state.

Note that `FAIL_NOT_FOUND` tickers are ALREADY auto-added to the skip list by `_on_zacks_failures` after every fill — the manual button is for the OTHER kinds (blocked / http_error / parse_error) when those tickers are consistently noisy and the user wants to drop them from future fills entirely.

Tested in `tests/test_audit_gui_fixes.py`: `test_send_misses_to_skip_list_*` (6 tests covering dedup, normalization, idempotence, empty/blank handling, and rollback on save failure).

### Preset format (v5)

JSON files at `scanner_data/presets/{name}.json`. `PRESET_SCHEMA_VERSION = 5`. v5 fields:

```json
{
  "_preset_version": 5,
  "indicators": { "<row_key>": {"enabled": bool, "display_only": bool, ...params}, ... },
  "start_date": "YYYY-MM-DD",
  "end_date": "YYYY-MM-DD",
  "include_etf": bool,
  "include_adr": bool,
  "ipo_mode": bool,
  "ipo_max_days": int,
  "timeframe_days": [int, ...],
  "custom_range": bool,
  "sequenced_run": bool,
  "sequenced_cfg": {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD", "n": int, "unit": str} | null,
  "earnings_dates_only": bool,
  "earnings_data_only": bool,
  "view_earnings_dates_only": bool,
  "view_earnings_data_only": bool,
  "view_color_match_only": bool,
  "view_interleave_quarters": bool,
  "column_order": [str, ...],
  "column_hidden": [str, ...]
}
```

**v5 changes (replaced v4):**

- **Added** `column_order` and `column_hidden` — the manual column layout + hidden-column set from the Columns dropdown (see [Columns dropdown](#columns-dropdown-manual-layout--visibility-manager)). Loading a v5 preset wipes the current results window so the dropdown reflects the preset's saved column intent. Pre-v5 presets lacking these keys leave the existing layout alone.
- **Added** `view_interleave_quarters` — the Interleave Q EPS+Rev view toggle.

**v4 changes (replaced v3):**

- **Split** `earnings_only_mode` → `earnings_dates_only` + `earnings_data_only` — two independent global toolbar toggles. Dates gates `days_since/until` filters; data gates the 6 Zacks per-quarter filters + consec beats. Invariant: dates ⊇ data.
- **Split** `view_earnings_only` → `view_earnings_dates_only` + `view_earnings_data_only` — parallel split for the post-scan view filters.
- **Backward compat:** v3 `earnings_only_mode=true` loads as both `earnings_dates_only=true` AND `earnings_data_only=true`. v3 `view_earnings_only` loads as `view_earnings_data_only` (matches prior semantics of checking the 6 most-recent-quarter columns only). Missing keys default to `False`.

**v3 changes (replaced v2):**

- **Added** `earnings_only_mode` (later split in v4) — global toolbar toggle that drove NaN-handling for every earnings filter (replaces 9 per-row `*_include_no_data` flags).
- **Added** `view_earnings_only` (later split in v4), `view_color_match_only` — post-scan view filter checkbox states (next to Timeframe dropdown). Display-only — don't change scan results, just which rows render + export.
- **Removed (silently ignored on load)** the per-filter `include_no_data` keys that lived under `indicators[*]` in v1/v2 presets.

Loader (`MainWindow._load_preset`) tolerates missing keys via `.get()` for forward-compat. v1/v2 presets load cleanly; the missing v3/v4 keys default to `False` (matching the prior "NaN passes" behavior of the removed per-row `include_no_data=True` defaults on the days_* rows).

---

## Storage layout

`scanner_data/` lives next to the executable in production (or under `data_engine.config.DATA_DIR` for development). Every entry is **sacred** — never touched by an exe rebuild.

| Path | Type | Source | Purpose |
|------|------|--------|---------|
| `ohlcv/{TICKER}.parquet` | DataFrame | yfinance (or polygon if added) | Daily OHLCV cache, 5yr |
| `universe.csv` | CSV | NASDAQ FTP + GitHub + SEC | Ticker universe with metadata |
| `sector_map.parquet` | DataFrame | Finnhub `/stock/profile2` (with FinanceDatabase + yfinance fallback) | ticker → (sector, sector_etf); 56-key SECTOR_ETF_MAP routes Finnhub sub-industries to SPDR ETFs |
| `earnings_dates.parquet` | DataFrame | Reconcile output (Zacks / Nasdaq / Yahoo / Finnhub priority chain) | last/next earnings dates per ticker (1:1 with ticker) |
| `earnings_history.parquet` | DataFrame | Zacks scraper + Finnhub `/stock/earnings` | Per-quarter EPS / revenue history (5yr); binary source policy |
| `earnings_raw/{source}/<run_id>.parquet` | DataFrame | Each fill's raw response | Append-only audit/replay layer; pruned at startup if older than `RAW_RETENTION_DAYS` (30) |
| `zacks_cookies.txt` | text | Firefox cookie capture | Imperva session tokens |
| `firefox_zacks_profile/` | Firefox profile | Firefox itself | Persistent profile (login, cookies) |
| `blacklist.txt` | comma-separated text | User | Tickers to skip during refresh |
| `zacks_blacklist.txt` | newline-separated text | Auto + user | Zacks-specific skip list (auto-added on FAIL_NOT_FOUND); one ticker per line |
| `finnhub_blacklist.txt` | newline-separated text | Auto + user | Finnhub-specific skip list (auto-added on empty `/stock/earnings` response — typically ETFs); one ticker per line |
| `sec_contact.txt` | text | User (Settings → Set SEC Contact Email…) | Contact email SEC EDGAR requires in the request User-Agent for the universe download. Absent → SEC source skipped. Also settable via the `SEC_CONTACT_EMAIL` env var. |
| `presets/{name}.json` | JSON | User | Saved indicator + scan-window configs |
| `logs/*.log` | text | LogPanel | Per-session diagnostic logs (`scan_*`, `ohlcv_*`, `universe_*`, `bridge_*`) |
| `failed_tickers.log` | text | `ticker_universe` | Tickers dropped during yfinance universe validation |
| `ftp_raw/` | text | NASDAQ FTP (when SAVE_FTP_RAW=True) | Raw downloads for debugging |

QSettings (registry-backed under `HKCU\Software\trade_scanner_fh\Trade_Scanner_FH\`) holds:
- Cookie-browser monitor preference
- Window geometry (Qt default behavior)

---

## Testing

Test suite at `trade_scanner_fh/tests/` — **778 tests** as of 2026-05 (777 passing; one known calendar-drift failure, `test_yahoo_fill.py::test_spot_fill_writes_one_row`, whose fixture earnings dates have aged into the past). Run all:

```bash
cd c:/python/EDA_Project/Trade_Scanner_FH
c:/python/envs/eda-pipeline/python.exe -m pytest -q
```

### Test file layout

| File | Coverage |
|------|----------|
| `test_scanner.py` | Funnel pipeline end-to-end |
| `test_indicators.py` | Each pure indicator function |
| `test_data_engine.py` | OHLCV cache + parquet I/O |
| `test_earnings_history.py` | History parquet, `compute_consecutive_beats` (incl. divergent-cadence regression), binary source policy, integrity checks (incl. cross_source_period_overlap) |
| `test_earnings_cache.py` | earnings_dates.parquet schema + IO |
| `test_earnings_reconcile.py` | Multi-source priority chain (zacks → nasdaq → yahoo → finnhub), aug-label generation, stale-date filtering, finnhub-as-last-resort |
| `test_earnings_aligned_dates.py` | Date-alignment + match-color anchoring |
| `test_earnings_raw.py` | Raw audit/replay layer per source |
| `test_zacks_scraper.py` | curl_cffi parser, fetch, session |
| `test_finnhub_client.py` | Finnhub REST primitives, rate limiter, key storage |
| `test_finnhub_fill.py` | Bulk/gap/spot fills, period_ending day-1 normalization, fiscal-year multi-record dedup, canonicalization handling |
| `test_nasdaq_fill.py` | finance-calendars bulk fill |
| `test_nasdaq_weekly_auto.py` | Once-per-week auto-trigger |
| `test_yahoo_fill.py` | yfinance gap + spot fills |
| `test_imperva_pause.py` | Auto-pause heuristic on Zacks block streak |
| `test_browser_cookie_autorefresh.py` | Firefox launcher, cookie capture, mid-flight capture |
| `test_cookie_dialog_smoke.py` | Cookie-paste dialog |
| `test_zacks_failure_breakdown.py` | FAIL_* sentinel classification |
| `test_smart_refresh.py` | Candidate selection (gap / just-reported / long-stale) |
| `test_display_only.py` | Display-only mode + red-on-fail (incl. NaN-streak crash regression) |
| `test_filter_stages.py` | `_build_filter_stages` correctness |
| `test_phase7_filters.py` | Per-quarter Zacks filters + EPS/Rev mutex |
| `test_phase8_output.py` | Result column formatting |
| `test_phase9_integration.py` | End-to-end with earnings filters |
| `test_q_date_columns.py` | Per-quarter report-date columns |
| `test_column_reorder_and_palette.py` | ResultsTable header reorder + alignment palette |
| `test_main_window_imports.py` | Smoke check that GUI module imports cleanly |
| `test_audit_fixes.py` | Pinned regressions for prior audit findings |
| `test_audit_gui_fixes.py` | GUI-side audit regressions |
| `test_chunk_periods.py` | Sequenced-run chunking math |
| `test_surge_trend_continuous.py` | Trend-continuous surge algorithm |
| `test_surge_ignition.py` | Ignition surge mode — catalyst-bar re-anchoring + UI greyout |
| `test_sector_map.py` | Bulk + targeted sector mapping |
| `test_universe.py` | Universe download + merge logic |
| `test_tradestation.py` | TradeStation watchlist bridge |
| `test_hotkey.py` | Per-row HOTKEY ticker sender |
| `test_config.py` | Holiday calendar, atomic writes |

### Test invariants

- Pure indicator tests use synthetic DataFrames (no I/O) — safe for CI.
- Scanner integration tests use `fake_scan_cache` / `fake_cache` fixtures that monkeypatch `config.DATA_DIR` to a tmp path.
- GUI widget tests use the `_qapp` module-scoped QApplication fixture.
- The `tests/_*.py` files (underscore prefix) are diagnostic scripts NOT run by pytest collection — use them for manual smoke tests against live endpoints.

---

## Build & deploy

### Dev environment

```bash
cd c:/python/EDA_Project/Trade_Scanner_FH
# Shared interpreter at C:\python\envs\eda-pipeline (Python 3.11+) is the
# expected dev/run environment. Required packages:
pip install numpy pandas scipy yfinance pyarrow pyautogui PyQt6 \
            pyinstaller requests finance-calendars beautifulsoup4 \
            financedatabase keyring lxml openpyxl curl_cffi psutil \
            pywin32 finnhub-python
```

### Build the exe

```bash
# 1. Run the test suite — must be green.
cd c:/python/EDA_Project/Trade_Scanner_FH
c:/python/envs/eda-pipeline/python.exe -m pytest -q

# 2. Nuke stale build artifacts AND bytecode caches.
#    --clean alone does NOT clear __pycache__ — that's the gotcha
#    that bit prior rebuilds. Stale .pyc files can mask source edits.
rm -rf build
find . -name __pycache__ -type d -not -path "./venv/*" -not -path "./dist/*" \
       -exec rm -rf {} + 2>/dev/null

# 3. Build. --clean clears build/, --noconfirm overwrites dist/.
c:/python/envs/eda-pipeline/python.exe -m PyInstaller \
    Trade_Scanner_FH.spec --clean --noconfirm

# 4. Verify:
ls -lh dist/Trade_Scanner_FH.exe          # ~201 MB, fresh timestamp
ls dist/scanner_data/                     # all data files preserved
```

### Spec file (`Trade_Scanner_FH.spec`)

PyInstaller spec — pulls C-library DLLs from `BASE_PREFIX/Library/bin` so it works in venv or conda. Adds explicit hidden imports for lazy modules (yfinance, lxml, keyring, openpyxl, curl_cffi, psutil, win32api, finnhub).

When adding a new data source that uses lazy imports, update the `hiddenimports` list in the spec.

**Qt binding exclusion.** The spec's `excludes` list drops `matplotlib`, `tkinter`, `test`, `unittest`, **and `PySide6` / `shiboken6`**. The shared build environment also carries PySide6 (sibling projects use it), and PyInstaller aborts the build the moment it detects two Qt bindings packages. This app is PyQt6-only, so PySide6 / shiboken6 are excluded explicitly — without that, the build fails at the `hook-PySide6` stage.

### CRITICAL: `scanner_data/` preservation

The user's instruction is `scanner_data/` is **never** touched by a rebuild. PyInstaller writes only to `dist/Trade_Scanner_FH.exe`; the existing `dist/scanner_data/` is left intact. Rebuilds can safely delete:

- `build/` — PyInstaller intermediate (regenerated)
- `__pycache__/` directories under the source tree
- `dist/Trade_Scanner_FH.exe` — replaced

Rebuilds MUST NOT touch:

- Anything else under `dist/scanner_data/`

The Firefox profile dir (`firefox_zacks_profile/`) was the one exception in earlier iterations when it accumulated session-restore data; the May 2026 user.js fix made deletion unnecessary (it's now safe to keep across rebuilds).

---

## Critical invariants

These are properties the codebase depends on. Breaking any one is a regression worth catching in tests.

### Data layer
1. **`scanner_data/` is sacred** — no rebuild, no migration, no auto-cleanup ever wipes user data.
2. **Atomic writes** — every parquet/text write goes through `config.atomic_write_parquet` / `atomic_write_text`. Crash mid-write must never corrupt the canonical file.
3. **OHLCV cache is tz-naive** on disk and at access time. Benchmark loads in `run_scan` strip tz explicitly.
4. **Reference tickers** (`config.REFERENCE_TICKERS`) are excluded from scan results but always kept in the OHLCV cache for RS calculations.

### Pipeline layer
5. **`_compute_ticker` never raises** to the caller — exceptions are caught at the per-ticker level in `run_scan`, logged into `result.errors`.
6. **`_build_filter_stages` returns only enabled-non-display-only filters.** Display-only filters MUST be skipped to preserve the "compute but don't filter" semantic.
7. **`_compute_display_only_fails` mirrors `_build_filter_stages`** for every filter that has a `_display_only` flag. Drift between the two means the red-on-fail color shows for cells that wouldn't actually have been filtered.
8. **`compute_consecutive_beats` cadence test uses `period_ending`** (not `report_date`) — see BUG-9 analysis. Late filings shouldn't break beat streaks.
9. **NaN coercion**: any code that does `int(value)` where `value` could be a Pandas-NaN must use `_safe_streak` (or equivalent) — `bool(nan) is True` makes `int(value or 0)` a crash trap.

### GUI layer
10. **Filter ↔ Display Only mutex** at the GUI level — both checkboxes can't be on simultaneously per row.
11. **Threshold inputs are always editable** (in both Filter and Display Only modes). Display Only needs the threshold for red-on-fail.
12. **`ResultsTable.populate` is per-row try/except** — a bad row leaves an empty cell, never crashes the table.
13. **`ResultsTable.populate` yields to `processEvents()` every 200 rows** — keeps the GUI responsive at 15k+ rows.
14. **`ScanWorker.run`, `_on_scan_done`, and `_populate_row` are all wrapped in try/except** — uncaught exceptions in main-thread Qt slots can abort the exe under PyInstaller windowed mode on Windows.
15. **`LogPanel._append` flushes after every write** — so crash diagnostics survive an abort.

### Match-color subsystem
16. **`_earnings_aligned_dates` only contains earnings dates that match a NON-EARNINGS indicator date column.** No false alignment from gain_start_date (excluded — was BUG-2).
17. **Per-match randomization is seeded by `(ticker, date)`** — stable across re-renders, distinct across tickers.
18. **`_ALIGN_PALETTE` band**: hue ∈ [165°, 330°], brightness ≥ 150. No reds, yellows, greens, or dark colors. Test enforces.

### Earnings columns (Option B)

19. **Each of the 6 individual earnings columns gates on its own `_enabled OR _display_only`.** Off → column absent. No "always-on context" surfacing.
20. **`last_report_date` shows ONLY when** at least one individual earnings column is active AND no beats column is active. When beats is active, Q-1 Date covers the same value.
21. **Q-i column display gating is decoupled from streak length.** `_build_dynamic_columns` uses `_max_present(suffix)` to render every populated quarter (up to MAX_BEATS_QUARTERS=20), NOT `min(streak, present)`. The streak count drives only the green-text coloring inside `_populate_row`. This is what keeps post-streak earnings cells eligible for match-coloring against non-earnings indicator dates.
22. **`consec_*_beats_min = 0` is a valid threshold.** Spinbox minimum is 0, not 1. With min=0 the filter trivially passes everyone AND the display-only red-on-fail can never fire.
23. **`consec_*_beats_quarter_cap` is per-side and independent.** EPS cap controls only `q*_*_eps` columns; Rev cap controls only `q*_*_rev`. Default 0 means no cap (use full MAX_BEATS_QUARTERS=20). Values 1-20 limit population at the scanner level via `past_desc.head(cap)` so unpopulated quarters never reach the DataFrame.
24. **NaN handling for earnings filters is driven by TWO independent global flags — `ScanParams.earnings_dates_only` (gates `days_since/until` filters) and `ScanParams.earnings_data_only` (gates the 6 Zacks per-quarter filters + consec beats).** Per-row `*_include_no_data` flags were removed (9 of them). Both `_build_filter_stages` and `_compute_display_only_fails` consult these flags so funnel filtering and red-on-fail coloring stay consistent. Mask form: `(v >= t) | (v.isna() & nan_passes)` — NaN-safe in both branches.
25. **Dates ⊇ Data invariant.** A row passing the earnings-data filter MUST also pass the earnings-dates filter (data implies date). Enforced at both layers: the funnel masks for the dates filters explicitly OR in `_data_present_mask(df)` so a row with NaN date columns but populated data columns still passes; the view filter's dates check OR's in the data coverage mask. Tests: `test_earnings_dates_filter_respects_data_implies_date_invariant`, `test_view_filter_dates_supersets_data`.
26. **View-only filters (Earnings Dates / Earnings Data / Color Match Only)** affect display + export but NEVER the underlying scan results. `MainWindow._period_results` always holds the unfiltered scan output; `_apply_view_filters(df)` produces a fresh filtered copy on every render and on every export. Toggling a view filter off restores all rows without re-scanning.
27. **View-only "Earnings Data" coverage signal** = at least one non-NaN value across the 6 most-recent-quarter columns (`reported_eps`, `surprise_eps_*`, `reported_rev`, `surprise_rev_*`) OR any of the q-beats data columns (`q{1..20}_reported_eps`, `q{1..20}_surprise_*`, etc.). The q-beats inclusion lets a ticker pass when it has multi-quarter beats data even if its most-recent-quarter cells are NaN. Only columns actually present in the rendered df are checked.
28. **View-only "Earnings Dates" coverage signal** = at least one non-NaN value across calendar columns (`last_report_date`, `next_earnings_date`, `days_since_er`, `days_until_er`) OR any q-beats date column (`q{1..20}_report_date_eps`, `q{1..20}_report_date_rev`) OR any earnings DATA column (data ⇒ date). Always passes a strict superset of what the data view filter passes.
29. **View-only "Color Match Only" coverage signal** = `_earnings_aligned_dates` is a non-empty list. Empty list, NaN, or missing column all evaluate to "no match" → row dropped.

### Build / deploy

30. **`scanner_data/` survives rebuilds.** Verified by checking ohlcv parquet count + presets dir + cookies file before and after.
31. **`__pycache__/` MUST be cleared before any rebuild.** Stale bytecode can mask source edits — burned us once with the multi-column-drag and Display-Only regressions.

---

## Disclaimer

**This software is for informational and educational purposes only and does not constitute financial advice, investment advice, trading advice, or any other kind of advice.** You should not treat any of the software's output as a recommendation to buy, sell, or hold any security or financial instrument.

The developer(s) of this software:

- Make no representations or warranties regarding the accuracy, completeness, or reliability of any data, calculations, or scan results produced by this application.
- Are not responsible for any financial losses, damages, or other consequences arising from the use of this software or reliance on its output.
- Do not guarantee that the data sourced from third-party providers (including but not limited to Yahoo Finance, NASDAQ, SEC EDGAR, Zacks, Finnhub, or any future provider) is accurate, timely, or complete.

**Use at your own risk.** All investment decisions should be made based on your own research and judgment, ideally in consultation with a qualified financial professional. Past performance of any security identified by this scanner is not indicative of future results.

The Zacks scraper is built on a derivation of [TinyEarn](https://github.com/hussien-hussien/TinyEarn) (© 2018–2020 Hussien Hussien, MIT licensed) and uses TLS impersonation to bypass Imperva's bot-detection layer. Users are responsible for ensuring their use of this scraper complies with Zacks' terms of service.
