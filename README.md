# Trading Scanner — Finnhub Fork (`trade_scanner_fh`)

A standalone PyQt6 desktop application for scanning the entire US equity universe against configurable technical, momentum, volatility, volume, relative-strength, and earnings indicators. Packaged as a single-file Windows executable via PyInstaller.

Forked from `trading_scanner_zacks` and built around **three independent per-quarter earnings-history sources**, all on the **adjusted / non-GAAP** basis: **Finviz** (top priority — scraped `epsActual`/`salesActual` from the finviz earnings tab; matches Zacks ~98% with finer revenue precision and real announcement dates+times), **Zacks** (adjusted EPS, real announcement dates), and **Finnhub** (deep-history fallback). Coverage uses a **gap-fill source policy** — per `(ticker, period_ending)`, the highest-priority source wins (`finviz > zacks > finnhub`), but each ticker can carry rows from multiple sources covering different fiscal-quarter slots. Plus per-source raw audit layers, daily Nasdaq calendar auto-refresh, Yahoo gap-fill, integrity diagnostics, and a multi-source priority chain reconciler for `last_earnings`/`next_earnings` dates (`nasdaq > yahoo > finviz > zacks > finnhub`).

> **Source history.** A SEC EDGAR (GAAP) earnings source existed through 2026-05-31 but was removed — GAAP figures aren't useful for this scanner's trading use case, and every remaining EPS source is single-basis (adjusted). The separate SEC `company_tickers.json` download is retained purely as a **universe-building** ticker source (not earnings). Finviz replaced EDGAR's priority slot the same day.

This README is written to be self-sufficient for any developer (human or LLM) who needs to extend the system — adding a new data feed, modifying a filter's mathematical definition, adding a new indicator, or rewiring the GUI. It documents not just what the system does but **how the pieces compose**, what **invariants must not break**, and exactly **where to plug new code in**.

> **Security & robustness audit (2026-06-09).** A multi-vector audit (efficiency, security, robustness, GUI usability) was run and remediated. Findings live in [`AUDIT_2026-06-09.md`](AUDIT_2026-06-09.md); the remediation log (what was fixed / deferred / how verified) is in [`AUDIT_FIXES_2026-06-09.md`](AUDIT_FIXES_2026-06-09.md). Highlights now in the codebase: per-writer unique temp names + `HISTORY_WRITE_LOCK`/`DATES_WRITE_LOCK` serialization on the parquet caches (no concurrent lost-updates/corruption), download-then-swap `rebuild_ticker`, vectorized `compute_yoy_columns`, DPAPI-encrypted Zacks cookies (backward-compatible), response-size caps on the Imperva-fronted scrapers, `pyautogui` failsafe/coordinate guards, and the NYSE holiday table extended through 2032. Suite at the time of that audit: 933 tests (since grown — see [Testing](#testing)).

> **2026-06 refactor + feature waves.** Three follow-up waves landed in June 2026: **(1)** observability + version stamping (window title + Windows VERSIONINFO resource mirror `trade_scanner_fh.__version__`) + the **Settings → Advanced…** user-config (`scanner_data/user_config.json`); **(2)** a MainWindow decomposition (`gui/earnings_coordinator.py`, `gui/blacklists.py`, `gui/exports.py`, `gui/columns.py`), the shared `fill_framework.py` fill orchestrator, and scraper resilience (drift-tolerant fallback parsers + a parse-failure spike alarm); **(3)** new scan features — RVOL filter, ATR Stop column, watchlist diffing (`Chg` column), single-level undo delete, Quick Export, an in-app scan scheduler (`gui/scheduler.py`), a report-only cross-source EPS disagreement CSV, and an opt-in launch-time OHLCV prefetch. A follow-up (2026-06) reworked the **ADR% formula** (classic ratio form `mean(100 × (High/Low − 1))`, default lookback 14 → 20) and added the **$ADR filter + ADR Stop column + per-scan stop multipliers** for both stop columns. A bug-fix wave **(4, v5.2.0, 2026-06-17)** followed: the **hotkey/TradeStation order-misfire fix** (type tickers lowercase so `pyautogui`'s Shift-for-capitals can't trigger Trade Bar order hotkeys — see HOTKEY mode § Safety), **Finnhub 403 handling** (route out-of-plan symbols to the skip list without counting them toward the block-backoff streak), **non-sequenced preset date re-anchoring** (End → most recent trading day, Start preserves the saved span; sequenced runs keep exact dates), and a dev-only `TRADE_SCANNER_FH_DATA_DIR` override. Each is documented in its section below. Suite: **1,243 tests pass**.

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
14. [EDGAR scrape and conversion (REMOVED — forking reference only)](#edgar-scrape-and-conversion-removed--forking-reference-only)
15. [Cookie acquisition flow (Firefox + mid-flight capture)](#cookie-acquisition-flow-firefox--mid-flight-capture)
16. [GUI subsystem](#gui-subsystem)
17. [Storage layout](#storage-layout)
18. [Testing](#testing)
19. [Build & deploy](#build--deploy)
20. [Critical invariants](#critical-invariants)
21. [Disclaimer](#disclaimer)


**Not financial advice.** This software is for informational and educational use only — see the full [Disclaimer](#disclaimer) at the bottom of this README before relying on any output.
---

## Architecture at a glance

```text
┌──────────────────────────────────────────────────────────────────┐
│  GUI layer (PyQt6 main thread)                                   │
│   gui/main_window.py    MainWindow — toolbar, slots, menus       │
│   gui/earnings_coordinator.py  EarningsRefreshCoordinator —      │
│                         smart-refresh chaining, Nasdaq cadence,  │
│                         3-bar earnings progress panel            │
│   gui/widgets.py        IndicatorPanel, ResultsTable, LogPanel   │
│   gui/dialogs.py        Modal dialogs (sequenced run, cookies…)  │
│   gui/columns.py        ColumnManager — column order/hidden state│
│   gui/exports.py        ExportsController — XLSX/CSV + Quick Exp.│
│   gui/blacklists.py     BlacklistManager — per-source skip lists │
│   gui/scheduler.py      ScanScheduler + Schedule… dialog (F3)    │
│   gui/theme.py          Dark stylesheet                          │
└────────────────────┬─────────────────────────────────────────────┘
                     │ start worker
                     ▼
┌──────────────────────────────────────────────────────────────────┐
│  Worker layer (QThread)                                          │
│   gui/workers.py    ScanWorker, ZacksFillWorker,                 │
│                     FinnhubFillWorker, FinvizFillWorker,         │
│                     UpdateWorker, PrefetchWorker,                │
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
│   indicators.py     23 pure indicator functions (SMA, ATR, RVOL…)│
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
│   fill_framework.py     Shared checkpoint/flush/finalize/backoff │
│                         loop for the finviz/finnhub fill pair    │
│   scan_history.py       Watchlist-diff persistence (GUI-free)    │
│   nasdaq_fill.py        Nasdaq finance-calendars bulk fill       │
│   yahoo_fill.py         yfinance gap + spot fills                │
│   finnhub_fill.py       Finnhub deep-history bulk/gap/spot       │
│   finviz_fill.py        Finviz earnings bulk/gap/spot (top pri)  │
│   zacks_scraper.py      HTTP scraper (curl_cffi + Imperva bypass)│
│   finnhub_client.py     Finnhub REST primitives                  │
│   finviz_client.py      Finviz ty=ea earningsData scrape         │
└──────────────────────────────────────────────────────────────────┘
                     │ persistent storage
                     ▼
┌──────────────────────────────────────────────────────────────────┐
│  scanner_data/  (per-user, sacred — survives all rebuilds)       │
│   ohlcv/*.parquet            per-ticker daily OHLCV (def. 5 yr)  │
│   universe.csv               NASDAQ + NYSE universe snapshot     │
│   sector_map.parquet         ticker → sector ETF                 │
│   earnings_dates.parquet     last/next dates (5-col + source)    │
│   earnings_history.parquet   per-quarter EPS+Rev                 │
│                              (Finviz + Zacks + Finnhub, gap-fill)│
│   earnings_raw/{source}/*.parquet   append-only audit per run    │
│   .finnhub_bulk_checkpoint.json  resumable bulk progress         │
│   .finviz_bulk_checkpoint.json   resumable bulk progress         │
│   .gap_fill_dedup_v1.done    one-time migration sentinel         │
│   zacks_cookies.txt          Imperva session tokens              │
│   firefox_zacks_profile/     persistent Firefox profile          │
│   blacklist.txt              universal ticker blacklist          │
│   zacks_blacklist.txt        Zacks-only skip list                │
│   finnhub_blacklist.txt      Finnhub-only skip list (ETFs)       │
│   finviz_blacklist.txt       Finviz-only skip list (uncovered)   │
│   user_config.json           Settings → Advanced… overrides      │
│   scan_history.json          watchlist-diff baselines + summary  │
│   schedules.json             scan-scheduler entries              │
│   earnings_disagreements.csv cross-source EPS disagreement report│
│   exports/                   Quick Export XLSX snapshots         │
│   presets/, logs/, ftp_raw/                                      │
└──────────────────────────────────────────────────────────────────┘
```

### Earnings data architecture

Five independent upstream sources, each with its own dedicated fill
module and per-source blacklist where applicable:

| Source      | Module                  | Writes to                                    | Blacklist                 | Cap                         |
| ----------- | ----------------------- | -------------------------------------------- | ------------------------- | --------------------------- |
| **Finviz**  | `finviz_fill.py`        | `earnings_history.parquet` (source=finviz)   | `finviz_blacklist.txt`    | 10 yr*                      |
| **Zacks**   | `earnings_history.py`   | `earnings_history.parquet` (source=zacks)    | `zacks_blacklist.txt`     | 10 yr*                      |
| **Finnhub** | `finnhub_fill.py`       | `earnings_history.parquet` (source=finnhub)  | `finnhub_blacklist.txt`   | 10 yr*                      |
| **Nasdaq**  | `nasdaq_fill.py`        | `earnings_dates.parquet` (source=nasdaq)     | (universe blacklist only) | ±90 d window                |
| **Yahoo**   | `yahoo_fill.py`         | `earnings_dates.parquet` (source=yahoo)      | (universe blacklist only) | (whatever yfinance returns) |

\* The history cap is `config.EARNINGS_HISTORY_YEARS` — default 10 (raised
from 5 on 2026-06-07), user-tunable via **Settings → Advanced…**
(`scanner_data/user_config.json`). All three history sources share it; a
save-time re-prune in `save_earnings_history` drops rows older than the cap.

Every fill writes its raw response to `earnings_raw/{source}/<run_id>.parquet`
before collapsing to the consumer schema, so reconciler logic can be
replayed against frozen captures without re-hitting upstreams. Files
older than `config.RAW_RETENTION_DAYS` (30) prune at app startup.

**Period_ending normalization**: every source stamps `period_ending` to
**day-1 of the fiscal-quarter month** at row construction (e.g. Finviz /
Finnhub returns `2026-03-31` → stored as `2026-03-01`). Zacks already uses
day-1 natively. The normalization makes `(ticker, period_ending)` a stable
cross-source dedup key. `report_date` is preserved exactly as supplied
(Finviz / Zacks: real announcement dates; Finnhub: announcement when
`/calendar/earnings` has it, else proxy).

**Gap-fill source policy on `earnings_history.parquet`**: `dedupe_history`
applies per-`(ticker, period_ending)` source priority — `finviz > zacks
> finnhub`. The highest-priority source wins each fiscal-quarter slot.
Rows from lower-priority sources on DIFFERENT periods survive as
gap-fill, so a single ticker can carry Finviz for most quarters + Zacks
for a quarter Finviz hasn't pulled + Finnhub for a quarter neither
covers. Dedup runs at WRITE time inside `save_earnings_history`, AND
read-side per consumer (the on-disk parquet legitimately keeps multiple
source rows per quarter; `get_ticker_history` and the scanner lookup
both re-dedup on read). Because all three sources are the same adjusted
basis (see below), there is no cross-basis mixing — every slot still
carries clear source provenance via the `source` column.

**Source reporting basis.** Every EPS source that writes
`earnings_history.parquet` is on the **adjusted / non-GAAP** (Street)
basis, verified empirically against the live parquet: finviz `epsActual`
matches Zacks ~98% to the penny, and Zacks vs Finnhub agree to the penny
at the median. (Finnhub's non-GAAP basis is also provable structurally —
100% of its rows satisfy `actual − estimate == surprise`, and analyst
estimates are always adjusted.) The GAAP source, SEC EDGAR, was removed
2026-05-31.

| Source  | Writes EPS/Rev into        | Basis                   | Estimates / surprise |
|---------|----------------------------|-------------------------|----------------------|
| Finviz  | `earnings_history.parquet` | adjusted / non-GAAP     | yes                  |
| Zacks   | `earnings_history.parquet` | adjusted / non-GAAP     | yes                  |
| Finnhub | `earnings_history.parquet` | adjusted / non-GAAP     | yes                  |
| Nasdaq  | `earnings_dates.parquet`   | n/a (dates only)        | no                   |
| Yahoo   | `earnings_dates.parquet`   | n/a (dates only)        | no                   |

The scanner's read-side dedup uses `dedupe_history(..., backfill_estimates=True)`:
when a slot's winner is missing estimate/surprise values, it inherits
them from a lower-priority same-slot row. Since all sources share the
adjusted basis, no basis correction is needed; YoY is recomputed from
the displayed reported EPS after dedup.

A one-time launch migration (`migrate_to_gap_fill_dedup`) re-dedups
the on-disk parquet under the new priority on first launch of the
updated build, sentineled at `scanner_data/.gap_fill_dedup_v1.done`.
Originals are preserved in `earnings_raw/` so a replay is always
available.

**Reconciler priority chain** for `earnings_dates.parquet`
(`earnings_reconcile.py`):

- `last_earnings`: nasdaq > yahoo > finviz > zacks > finnhub
- `next_earnings`: same chain. Live calendar feeds (nasdaq, yahoo)
  outrank history-derived sources; among history sources finviz leads
  (real announcement dates+times) and Finnhub stays last.
- `> today` filter applies to every source on `next_earnings` so a
  stale date from any source is cleared.
- Finnhub-history future rows with `report_date_proxy=True` are
  excluded from the next_earnings candidate set (period-end stand-ins
  don't count as real announcement dates).
- Source label collapses to `{src}_derived` (or bare `nasdaq`/`yahoo`
  for date-only sources) when both positions come from the same
  source, else `{last_src}+{next_src}_aug` for mixed.

Every fill auto-triggers a reconcile against its affected tickers so
later runs can't clobber a freshly-derived consolidation.

**Automation**: only the daily Nasdaq calendar sweep auto-fires at launch
(toggle: Data → "Auto-refresh Nasdaq calendar daily"; calendar-day check,
`NASDAQ_REFRESH_DAYS = 1`, so any new day re-sweeps regardless of clock
time). When that sweep + reconcile promotes a freshly-reported quarter,
the earnings smart-refresh is deferred so it runs in the *same* launch
against the just-updated calendar (see the smart-refresh section) — that
smart refresh is the one automatic trigger for the three history sources
(Finviz / Zacks / Finnhub). Their bulk + gap fills, and Yahoo, remain 100%
manual via menu actions. A second optional launch-time automation — the
OHLCV cache prefetch (`Settings → Advanced…`, default off) — touches only
the OHLCV parquet LRU, never earnings.

**Universe-derived auto-skip (ETFs + ADRs)**: `universe.csv` from the
NASDAQ FTP source carries clean `etf` and `adr` boolean columns. All
three per-quarter earnings sources (Finviz, Zacks, Finnhub) pre-skip
flagged ETF/ADR symbols from every bulk and gap fill — none of these
sources have useful data for funds (ETFs don't have operating EPS or
revenue) and ADR coverage is spotty enough that fills mostly burn
requests. The auto-skip set is computed dynamically from `universe.csv`
on every fill (NOT persisted to per-source skip-list files, so a
universe refresh that re-flags a ticker flows through immediately).
Spot fill bypasses the auto-skip — when the user explicitly types a
symbol they mean it, even if `universe.csv` flags it as a fund.

**Skip-set parity**: every earnings fill — Zacks bulk + targeted,
Finnhub bulk + gap, Finviz bulk + gap — honors the same three-layer
skip:

1. Universe blacklist (`scanner_data/blacklist.txt`) — driven by
   OHLCV download failures + manual edits; opt-out from everything.
2. Per-source skip list (`zacks_blacklist.txt` /
   `finnhub_blacklist.txt` / `finviz_blacklist.txt`) — auto-populated
   on definitive-miss classifications + user-editable via the Data
   menu's "Edit … Skip List…" actions.
3. ETF/ADR auto-skip from `universe.csv` flags (above).

The combined-skip helpers (`_zacks_skip_set`,
`_combined_finnhub_skip_set`, `_combined_finviz_skip_set` on
`MainWindow`, backed by `gui/blacklists.py:BlacklistManager` for the
load/save/normalize plumbing) layer all three. Tests in
[`tests/test_etf_adr_auto_skip.py`](trade_scanner_fh/tests/test_etf_adr_auto_skip.py)
pin the parity + the no-leak-to-persisted-files guardrail.

**Diagnostics**: `Data → Diagnostics → Earnings Coverage Report` shows
per-source coverage counts (finviz / zacks / finnhub) — sets overlap
under gap-fill — plus the canonical "no coverage at all" gap and the
most-recent reported quarter per source. `Data → Diagnostics → Verify
earnings_history Integrity` runs ten soft-PK / schema / policy checks
(duplicate keys, orphan rows, null sources, dtype drift, period predates
the `EARNINGS_HISTORY_YEARS` cap, same-slot cross-source overlap,
calendar-vs-fiscal phantom duplicates, etc.) with one-click auto-fix
for repairable findings. The same-slot overlap check re-applies the
per-priority dedup when triggered.

**Derived columns** — beyond what the upstream sources supply, the parquet
also carries two locally-computed YoY % columns (`yoy_eps_pct`,
`yoy_rev_pct`) refreshed at every fill-finalize via
`compute_yoy_columns`. They join `(ticker, period_ending)` against the
same-quarter-prior-year row and apply `(cur - prior) / |prior| * 100`.
NaN when prior year missing or denominator is zero. The scanner exposes
them as both top-level filters (Curr YoY EPS / Rev %) and per-quarter
columns (Q-i YoY EPS / Rev %) inside the consec-beats blocks.

**Cross-source disagreement report (report-only, 2026-06)** — every
canonical save (`save_earnings_history` with dedup on) first runs
`report_cross_source_disagreements`, which compares same-`(ticker,
period_ending)` rows from *different* sources and flags pairs whose
`reported_eps` differ by more than `config.EPS_DISAGREEMENT_ABS_TOL`
($0.10) or whose `surprise_eps_pct` differ by more than
`config.SURPRISE_DISAGREEMENT_PP_TOL` (2.0 pp). Findings are atomically
rewritten to `scanner_data/earnings_disagreements.csv`
(`config.EARNINGS_DISAGREEMENTS_CSV_NAME`) — the file always reflects the
latest save, so stale findings self-clear on the next run; an empty scan
writes a header-only CSV. **Report-only**: it never changes which row
wins dedup, and a failure inside the report never blocks the save. Loud
log line when non-empty, silent when clean. Tests in
[`tests/test_disagreements.py`](trade_scanner_fh/tests/test_disagreements.py).

---

## Module-by-module map

Line counts and entry points as of 2026-06 (post the Phase 1–3 waves).
The package is **101 Python files** including tests (23 top-level
modules, 12 `gui/` modules, 1 `tools/` helper, 65 files under `tests/`).

### Top-level package
| File | Lines | Role | Key entry points |
|------|------:|------|------------------|
| `__main__.py` | 4 | `python -m trade_scanner_fh` shim | calls `gui.main()` |
| `launch_scanner.py` | 4 | PyInstaller entry — sole script in the spec | calls `gui.main()` |
| `__init__.py` | 13 | Package init + version | `__version__` |
| `config.py` | 865 | All paths, constants, tunables + the user-config override layer | `DATA_DIR`, `EARNINGS_HISTORY_PARQUET`, `REFERENCE_TICKERS`, `SECTOR_ETF_MAP`, `atomic_write_parquet` / `atomic_write_text` / `atomic_write_csv`, `most_recent_trading_day`, `get_sec_user_agent` / `get_sec_contact_email` / `set_sec_contact_email` / `sec_contact_is_configured`, `load_user_config` / `save_user_config` / `user_config_path` |
| `data_engine.py` | 613 | OHLCV download & cache | `download_one`, `download_many`, `load_ohlcv`, `validate_ticker`, `rebuild_ticker`, `prefetch_ohlcv` |
| `ticker_universe.py` | 603 | Universe download (3 sources) | `refresh_universe`, `load_universe` |
| `indicators.py` | 706 | 23 pure indicator functions | One function per indicator (see §[Adding a new indicator](#adding-a-new-indicator)) |
| `scanner.py` | 1776 | The funnel pipeline | `ScanParams`, `_compute_ticker`, `_compute_display_only_fails`, `_build_filter_stages`, `run_scan`, `ScanResult`, `ATR_STOP_MULTIPLIER`, `ADR_STOP_MULTIPLIER` |
| `scan_history.py` | 304 | GUI-free watchlist-diff persistence (`scan_history.json`) | `diff_and_record`, `record_scan_results`, `load_history`, `save_history`, `prune_latest`, `ScanDiff` |
| `sector_map.py` | 288 | Sector mapping persistence | `bulk_fill_sectors`, `targeted_fill_sectors`, `load_sector_map` |
| `earnings_cache.py` | 180 | Schema/IO for earnings_dates.parquet (bulk + targeted fills now live in nasdaq_fill / yahoo_fill) | `load_earnings_cache`, `save_earnings_cache`, `get_earnings_dates`, `_merge_and_save`, `COLUMNS` |
| `earnings_history.py` | 2322 | Schema/IO for earnings_history.parquet + Zacks bulk/targeted fills + write-side per-slot priority dedup (finviz > zacks > finnhub) + one-time migration + YoY columns + integrity diagnostics + disagreement report | `bulk_fill_zacks`, `targeted_fill_zacks`, `find_gap_tickers`, `find_smart_refresh_candidates`, `compute_consecutive_beats`, `compute_yoy_columns`, `dedupe_history`, `get_ticker_history`, `load_earnings_history`, `save_earnings_history`, `migrate_to_gap_fill_dedup`, `verify_integrity`, `fix_integrity_issues`, `coverage_report`, `find_cross_source_disagreements`, `report_cross_source_disagreements` |
| `fill_framework.py` | 547 | Shared checkpoint/flush/finalize/backoff-rewind orchestrator for the finviz/finnhub fill pair (hooks resolved through the calling module at call time so test monkeypatching of private names keeps working) | `run_fill_loop`, `FillSpec`, `Checkpoint`, `save_checkpoint` / `load_checkpoint` / `clear_checkpoint`, `flush_pending_to_disk`, `find_gap_tickers`, `finalize_fill` |
| `finviz_client.py` | 254 | Finviz earnings scrape — fetch `quote.ashx?t=SYM&ty=ea`, extract the `earningsData` JSON array (curl_cffi Chrome impersonation, slow jittered rate limiter, failure-kind sentinels, two-marker block-vs-empty classification) | `fetch_earnings`, `_extract_earnings_data`, `is_configured`, `last_failure_kind` |
| `finviz_fill.py` | 578 | Bulk / gap / spot finviz fills (top-priority adjusted source) — adjusted-field mapping, forward-row skip, history-years cap, checkpoint resume, block backoff (loop delegated to `fill_framework.run_fill_loop`) | `bulk_fill_finviz`, `gap_fill_finviz`, `spot_fill_finviz`, `find_finviz_gap_tickers` |
| `earnings_reconcile.py` | 418 | Multi-source priority chain unifier (`nasdaq > yahoo > finviz > zacks > finnhub`) | `reconcile_earnings_dates` |
| `earnings_raw.py` | 300 | Append-only raw audit/replay layer (one parquet per fill run per source) | `new_run_id`, `append_zacks_rows`, `append_finnhub_rows`, `append_finviz_rows`, `append_nasdaq_rows`, `append_yahoo_rows`, `read_raw`, `prune_old_raw` |
| `nasdaq_fill.py` | 141 | Nasdaq finance-calendars bulk fill (writes earnings_dates only) | `bulk_fill_nasdaq` |
| `yahoo_fill.py` | 216 | yfinance gap + spot fills (writes earnings_dates only) | `targeted_fill_yahoo`, `spot_fill_yahoo` |
| `finnhub_fill.py` | 618 | Finnhub deep-history bulk/gap/spot with step-back-on-block + resumable checkpoint + period_ending day-1 normalization + fiscal-year multi-record dedup (loop delegated to `fill_framework.run_fill_loop`) | `bulk_fill_finnhub`, `gap_fill_finnhub`, `spot_fill_finnhub`, `find_finnhub_gap_tickers` |
| `zacks_scraper.py` | 1136 | HTTP scraper + Firefox cookie path (strict + drift-tolerant fallback `obj_data` parsers) | `ZacksSession`, `fetch_earnings_history`, `launch_firefox_for_zacks_cookies`, `read_cookies_from_firefox_profile`, `set_zacks_cookies` |
| `finnhub_client.py` | 390 | Finnhub REST primitives (rate limiter, key storage, /stock/earnings, /calendar/earnings, /stock/profile2, failure-kind sentinels) | `fetch_earnings_history`, `fetch_calendar_earnings_window`, `fetch_earnings_dates`, `fetch_company_profile`, `fetch_sector`, `verify_api_key`, `get_api_key`, `set_api_key`, `last_failure_kind` |
| `hotkey.py` | 326 | Qt-free per-row HOTKEY ticker sender (testable headless) | `HotkeyConfig`, `send_ticker` |
| `tradestation.py` | 212 | TradeStation watchlist bridge | `TradeStationBridge` |
| `tools/set_zacks_cookies.py` | 136 | One-shot CLI cookie-injection helper | `main`, `_live_test`, `_read_interactive` |

### GUI package (`gui/`)
| File | Lines | Role | Key entry points |
|------|------:|------|------------------|
| `__init__.py` | 15 | Re-exports `main` | `main()` |
| `main_window.py` | 7302 | `MainWindow` — window, menus, slot wiring (decomposed 2026-06: earnings coordination, blacklists, exports, and column state moved to the four helper modules below; every historical method name survives on `MainWindow` as a thin delegate) | `MainWindow`, `PRESET_SCHEMA_VERSION` |
| `earnings_coordinator.py` | 841 | Earnings-refresh orchestration extracted from MainWindow: launch-time smart-refresh chaining, daily Nasdaq cadence, per-source worker bringup, the 3-bar earnings progress panel | `EarningsRefreshCoordinator` |
| `blacklists.py` | 116 | Per-source skip-list load/save/normalize plumbing | `BlacklistManager`, `normalize_ticker` |
| `exports.py` | 558 | XLSX/CSV export pipeline + Quick Export | `ExportsController` (incl. `quick_export`) |
| `columns.py` | 281 | Manual column order + hidden-set state and the cross-scan reconcile rules | `ColumnManager` |
| `scheduler.py` | 762 | In-app scan scheduler (F3): persistence, due-entry math, manager dialog, tray-icon toasts | `ScanScheduler`, `ScheduleEntry`, `ScheduleDialog`, `ScheduleEntryDialog`, `load_schedules` / `save_schedules`, `entry_is_due` |
| `widgets.py` | 2922 | Reusable widgets | `IndicatorRow`, `IndicatorPanel`, `ResultsTable`, `ReorderableHeader`, `LogPanel`, `RESULT_COLUMNS`, `_ALIGN_PALETTE`, `_safe_streak`, `_anchor_date_value`, `restore_rows_at_positions` |
| `workers.py` | 1360 | All QThread workers | `ScanWorker`, `ZacksFillWorker`, `FinnhubFillWorker`, `FinvizFillWorker`, `FirefoxCookieWaitWorker`, `UpdateWorker`, `PrefetchWorker`, `UniverseWorker`, `UniverseRefreshWorker`, `SectorFillWorker`, `EarningsFillWorker`, `BridgeWorker` |
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
    Ordering invariant — most proximal timeframe always first:
      • Multi-period: standard checkboxes sorted by `days` ascending
        (1D → 1W → 1M → 3M → 6M); custom range appended last.
      • Sequenced run: `chunk_periods()` walks BACKWARDS from end date,
        so the chunk closest to today is index 0.
    Session bar controls layered on top of the timeframe list:
      • `chk_omit_seen`        — across-scan: strips tickers in
                                 `self._session_seen` BEFORE dispatch.
      • `chk_omit_intra_run`   — within-run: passed to ScanWorker as a
                                 flag; the worker strips each period's
                                 hits from the symbol list before the
                                 next period scans.
       │
       ▼
[3] ScanWorker(symbols, params_list, sequenced=…, omit_intra_run=…)
    │   QThread.start(); wraps everything in try/except so a crash
    │   emits `finished` with partial results instead of killing the exe
       │
       ▼ (worker thread)
[4] For each (label, params): scanner.run_scan(working_symbols, params)
    where `working_symbols` is a mutable copy of `symbols`. When
    `omit_intra_run` is True, the set of tickers that passed period i
    is removed from `working_symbols` before period i+1 dispatches,
    so each ticker only ever appears in its earliest qualifying period.
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

### `ScanParams` (scanner.py:107 ff.)

A `@dataclass` with EVERY tunable for the funnel. ~147 fields organized as follows:

```python
@dataclass
class ScanParams:
    start_date: date = date(2025, 1, 1)
    end_date: date = field(default_factory=date.today)   # dynamic, per-instance

    # Per-filter triplet (most filters):
    {filter}_enabled: bool             # Filter mode
    {filter}_display_only: bool        # Display-only mode (mutex with enabled)
    {filter}_min: float (or _max, etc) # Threshold value
    # ... plus any filter-specific params (lookback, period, etc.)
```

**Dynamic `end_date` default (2026-06-06):** `end_date` uses `field(default_factory=date.today)` rather than a frozen literal, so a bare `ScanParams()` (e.g. in a test or script) never anchors the window to a stale hard-coded date. The GUI always sets it explicitly from the End picker; the factory only matters for direct construction.

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
| `rvol` | Iff rvol enabled-or-display | `indicators.relative_volume` (last bar's volume ÷ mean of the prior `rvol_lookback` bars) |
| `adr_dollar` | Iff the $ADR row is enabled-or-display | `indicators.adr_dollar` = `adr_pct/100 × End-date Close` (shares `adr_lookback` with ADR% — same masked ratio mean, so the two always agree) |
| `adr_stop` | Iff the $ADR row is enabled-or-display (derived display column — no own filter, no own panel row) | `close − adr_stop_multiplier (default 1.0) × $ADR` computed inline in `_compute_ticker` |
| `atr_stop` | Iff the ATR row is enabled-or-display (derived display column — no own filter, no own panel row) | `close − atr_stop_multiplier (default 2.0, the old hard-coded ATR_STOP_MULTIPLIER) × ATR` computed inline in `_compute_ticker` |
| `chg` | Stamped post-scan into `_period_results` by the watchlist diff (`NEW` / blank) | `MainWindow._apply_watchlist_diff` via `scan_history.diff_and_record` |
| `max_gap_pct` + `max_gap_date` | Iff max_gap active | `indicators.max_positive_gap` (returns tuple) |
| `surge_pct` + `surge_start_date` + `surge_end_date` + `surge_window` | Iff surge active | `indicators.surge_*` |
| `reported_eps`, `surprise_eps_*`, `reported_rev`, `surprise_rev_*` | Per-column gating (Option B 2026-05) | `mr.get(...)` from earnings_history_lookup |
| `last_report_date` | Iff individual earnings active AND no beats active | (suppressed when beats covers it) |
| `consec_eps_beats`, `q1..qN_*_eps` | Iff `consec_eps_beats_enabled OR _display_only`; N = `min(populated_quarters, consec_eps_beats_quarter_cap if >0 else MAX_BEATS_QUARTERS=20)`. NOT capped by streak length. | `compute_consecutive_beats` + per-quarter projection |
| `consec_rev_beats`, `q1..qN_*_rev` | Iff `consec_rev_beats_enabled OR _display_only`; same N-rule with `consec_rev_beats_quarter_cap` (independent from EPS) | Same for revenue side |
| `_earnings_aligned_dates` | Iff any non-earnings indicator date matches an earnings date | hidden — drives match-color in widget |
| `_display_only_fails` | Iff any display-only filter has fail flags for this row | hidden — drives red-on-fail in widget |

### `RESULT_COLUMNS` (widgets.py:1136 ff.)

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
source             string           "finviz" | "zacks" | "finnhub"
updated_at         datetime64[ns]
report_date_proxy  bool             True if report_date is a period_ending
                                    stand-in (Finnhub free tier sometimes
                                    omits real announcement dates).
                                    Always False for Finviz and Zacks
                                    (real announcement dates).
yoy_eps_pct        float64          Year-over-year EPS % growth — computed
                                    at fill-finalize time from same-quarter-
                                    prior-year row. NaN when prior year
                                    missing or prior reported_eps == 0.
yoy_rev_pct        float64          Year-over-year revenue % growth — same
                                    derivation rules as yoy_eps_pct.
```

Sort on save: `(ticker ASC, period_ending DESC)`. Cadence-gap detection in `compute_consecutive_beats` uses `period_ending` (NOT `report_date`) — see §[Editing mathematical assumptions](#editing-mathematical-assumptions-of-existing-filters) for the rationale.

`period_ending` is always day-1 of the fiscal-quarter month (e.g. Q1 2026 → `2026-03-01`) regardless of source. Cross-source `(ticker, period_ending)` joins are reliable. `report_date_proxy=True` rows are excluded from the reconciler's next_earnings candidate set — only real announcement dates can be promoted.

**Scanner display pick skips proxies too (Fix B, 2026-06-06).** When
`_compute_ticker` selects the most-recent reported quarter for the display
columns (`reported_eps` / `last_report_date`) AND when it builds the
multi-quarter beats block (`compute_consecutive_beats` + the `q{i}_*`
columns), it now prefers real rows over `report_date_proxy=True` rows. A
Finnhub calendar proxy carries a *period-end* stamp (e.g. `03/31`) that can
out-sort the real announcement row (e.g. finviz `03/02`), which would
otherwise show a later "Last Report Date" with the proxy's (often
placeholder) EPS. The pick uses a `past_pref` filter that drops proxies —
but **keep-if-orphan**: a ticker whose *only* coverage of a quarter is a
proxy still shows it (don't blank a quarter with no real alternative).
Covered by `test_most_recent_proxy.py` (real-wins, orphan-kept, and the
beats block agreeing with the single-quarter pick).

**YoY columns** are derived locally — NOT pulled from any source — by `earnings_history.compute_yoy_columns(df)`. The helper runs at the end of every fill (`_finalize_fill` in the Zacks path; `fill_framework.finalize_fill` for Finviz/Finnhub), refreshing the entire parquet so newly-arrived prior-year rows back-fill their current-year counterparts' YoY values. Formula: `(cur - prior) / |prior| * 100`, applied to `reported_eps` and `reported_rev` independently. The `|prior|` denominator handles negative-prior cases correctly (negative-to-positive transition produces a positive YoY%, matching the "improvement" intent).

**External readers**: this is an additive schema change. Existing readers that select specific columns (`pd.read_parquet(path, columns=[...])`) are unaffected. Readers that read the whole file (`pd.read_parquet(path)`) will see two new float64 columns at the end. The pre-YoY column order is unchanged.

### Per-ticker OHLCV parquet (data_engine.py)

`scanner_data/ohlcv/{TICKER}.parquet` — DatetimeIndex, columns: `Open`, `High`, `Low`, `Close`, `Volume`. Up to `OHLCV_HISTORY_YEARS` (default 5, user-tunable via Settings → Advanced…) years of daily bars. Loaded lazily via `data_engine.load_ohlcv(symbol)` which returns a tz-naive DataFrame. An opt-in launch-time prefetch (`data_engine.prefetch_ohlcv` on a `PrefetchWorker` thread; `config.PREFETCH_OHLCV_AT_LAUNCH`, default off) can warm the parquet LRU after the startup OHLCV update finishes — one-shot per launch, stoppable, and sequenced so it never contends with the updater.

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
| **Earnings Data** | `reported_eps`, `surprise_eps_dollar/pct`, `reported_rev`, `surprise_rev_dollar/pct`, consec EPS / Rev beats | reported result values from the history sources (finviz/zacks/finnhub) |

| State | Filter behavior on NaN values in that filter's column |
|-------|--------------------------------------------------------|
| **OFF** (default) | NaN passes the filter cleanly. Non-earnings filters (SMA, gap, surge, etc.) can still select tickers no history source covers. The funnel mask is `(value >= threshold) \| value.isna()`. |
| **ON** | NaN fails the filter. Only tickers with actual data make it through. The funnel mask is `value >= threshold` — NaN evaluates False and drops the row. |

**Dates ⊇ Data invariant.** The dates filter is always a SUPERSET of the data filter — anything with earnings data necessarily has an earnings date. To enforce this, when `earnings_dates_only=True` and a row's calendar date column is NaN but the row has any non-NaN value across the 6 most-recent-quarter earnings data columns, the dates filter still passes the row. This means a ticker can have stale or missing calendar coverage but still pass the dates filter as long as a history source gave us actual results.

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
- **Scanner side**: `scanner._compute_display_only_fails(params, row)` (scanner.py:986 ff.) returns a `{column_key: True}` dict for cells that would have failed the threshold. Stashed on the row as `_display_only_fails`.
- **Widget side**: `ResultsTable._populate_row` (widgets.py:2632 ff.) reads `_display_only_fails` and applies `_FAIL_RED` (`#e74c3c`) foreground to flagged cells.

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

## RVOL filter, ADR%/$ADR, and stop columns (2026-06)

Additions that follow the standard pipeline but are easy to confuse,
so the contracts are spelled out:

**RVOL (Relative Volume)** is a full standard indicator row — 3-state
panel row, funnel stage, output column:

- `indicators.relative_volume(df, lookback=20)` = last bar's volume ÷
  mean volume of the **prior** `lookback` bars (the last bar is excluded
  from its own baseline). 1.0 = average, 2.0 = double. NaN when fewer
  than `lookback + 1` bars, when the prior-window mean is zero /
  non-finite, or when the last bar's volume is NaN.
- Computed against the **full history up to the End date**
  (`full_to_end`), not the scan window — a 1D scan still gets a
  20-bar baseline.
- `ScanParams`: `rvol_enabled` / `rvol_display_only` /
  `rvol_lookback` (default 20) / `rvol_min` (default 1.5). Funnel stage:
  `RVOL >= {min}`; display-only red-on-fail via `_flag_min`.
- Deliberately independent from `surge_ignition`'s internal volume gate
  (that one is a *median*-based multiple evaluated per candidate bar
  inside a rally; RVOL is a *mean*-based snapshot of the latest bar).

**ADR% formula change (2026-06):** `indicators.adr_pct` now uses the
classic ratio form `mean(100 × (High/Low − 1))` over the trailing
`lookback` bars (previously `mean((High − Low)/Close) × 100`), and the
default lookback moved **14 → 20** (`ScanParams.adr_lookback` + the
panel row's "Days" spinbox). Bad bars (Low ≤ 0) are masked out of the
mean, mirroring the old form's zero-Close guard. **Presets that
explicitly stored a lookback keep their stored value** — only the
default changed (`from_dict` only overwrites present keys).

**$ADR (Avg Daily Range $)** is a full standard indicator row — 3-state
panel row, funnel stage, output column:

- `indicators.adr_dollar(df, lookback=20)` =
  `adr_pct/100 × End-date Close` — derived from the SAME masked ratio
  mean as ADR%, so the two always agree. NaN when ADR% is NaN or the
  last Close is NaN / non-positive.
- **Shares the ADR% row's lookback** (`ScanParams.adr_lookback` — there
  is deliberately no separate `adr_dollar_lookback`), so one "Days"
  value drives both.
- `ScanParams`: `adr_dollar_enabled` / `adr_dollar_display_only`
  (both default **off** — old presets load unchanged) /
  `adr_dollar_min` (default 0.50). Funnel stage: `$ADR >= {min}`
  (NaN fails); display-only red-on-fail via `_flag_min`.

**ATR Stop and ADR Stop** are NOT filters and have **no panel rows of
their own**. Each is a derived display column that piggybacks on its
parent row: whenever ATR ($ADR) is enabled-or-display-only,
`_compute_ticker` also emits
`atr_stop = close − atr_stop_multiplier × ATR(atr_period)`
(`adr_stop = close − adr_stop_multiplier × $ADR(adr_lookback)`). NaN
when the parent value is NaN; absent when the parent row is off.
Rendered as `$x.xx` via `RESULT_COLUMNS` keys `atr_stop` / `adr_stop`.
There is deliberately no filter stage — a stop level is trade context,
not a screening criterion.

**Configurable stop multipliers (2026-06):** the multipliers are
per-scan tunable via `ScanParams.atr_stop_multiplier` (**default 2.0**
— the previously hard-coded behavior; `scanner.ATR_STOP_MULTIPLIER` is
now the dataclass default) and `ScanParams.adr_stop_multiplier`
(**default 1.0** = `scanner.ADR_STOP_MULTIPLIER`), edited via the
"Stop col ×" spinboxes on the ATR / $ADR panel rows. The multipliers
feed the stop **columns only** — they never affect the ATR Min/Max $
or $ADR Min $ filters.

Presets saved before these existed load unchanged (missing keys default
off; missing multipliers default to 2.0 / 1.0, so an old preset renders
ATR Stop identical to before). Tests:
[`tests/test_rvol_atr_stop.py`](trade_scanner_fh/tests/test_rvol_atr_stop.py)
and [`tests/test_adr_dollar_stops.py`](trade_scanner_fh/tests/test_adr_dollar_stops.py).

---

## Match-color anchoring system

When a non-earnings indicator date (max_gap_date, surge_start_date, up_gap_start_date, down_gap_start_date, min_gap_date, surge_end_date) matches a ticker's earnings report date, the scanner stashes that date in `_earnings_aligned_dates` (scanner.py:963 ff.). The widget then assigns each matched date a deterministic-random palette color (per-ticker seeded so cross-ticker matches get distinct colors) and **paints every cell in the matched "unit" with that color**.

### How the unit definition works

The widget defines an "anchor date" for each column via `_anchor_date_value(key, row_data)` (widgets.py:1319 ff.):

| Column type | Anchor date |
|-------------|-------------|
| Date column (e.g. `max_gap_date`, `q3_report_date_eps`) | self |
| Static indicator value (`max_gap_pct`, `surge_pct`, etc.) | mapped via `_INDICATOR_VALUE_TO_DATE` |
| Q-i value (`q3_reported_eps`, `q3_surprise_rev_dollar`, etc.) | same-quarter date column (`q3_report_date_eps` / `q3_report_date_rev`) |
| Most-recent earnings (`reported_eps`, `surprise_rev_pct`, etc.) | `last_report_date` if present, else `q1_report_date_eps` / `_rev` |
| Anything else (symbol, close, pct_gain, streak counts) | `None` — no match-coloring |

For each cell, the widget looks up its anchor date value, checks if that value is in `aligned_color_map`, and applies the matching palette color. This means an entire unit (e.g. `max_gap_pct` + `max_gap_date`, or a Q-i triplet `q3_report_date_rev` + `q3_reported_rev` + `q3_surprise_rev_dollar` + `q3_surprise_rev_pct`) shares one color.

### The palette (`_ALIGN_PALETTE`, widgets.py:2030 ff.)

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

Nine steps. Concrete example: adding "Volume-Weighted Average Price (VWAP) distance %". (The 2026-06 RVOL indicator followed exactly this recipe — see `tests/test_rvol_atr_stop.py` for the resulting test shape.)

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

### Step 2: Add `ScanParams` fields (`scanner.py:107 ff.`)

```python
# In the appropriate section (Trend / Momentum / Volume / etc.):
vwap_dist_enabled: bool = False
vwap_dist_display_only: bool = False
vwap_dist_lookback: int = 20
vwap_dist_min: float = 0.0     # default: anything above VWAP
```

### Step 3: Wire computation into `_compute_ticker` (`scanner.py:487 ff.`)

```python
if params.vwap_dist_enabled or params.vwap_dist_display_only:
    row["vwap_dist_pct"] = indicators.vwap_distance_pct(
        window, lookback=params.vwap_dist_lookback,
    )
```

Use the `enabled OR display_only` gate — without it, display-only mode wouldn't compute the value.

### Step 4: Wire filter stage into `_build_filter_stages` (`scanner.py:1159 ff.`)

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

1. Edit the `_build_filter_stages` block at `scanner.py:1159 ff.`. Update the lambda to reflect the new comparison.
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

Earnings has five providers today (Finviz + Zacks + Finnhub for history; Nasdaq + Yahoo for dates). To add a sixth (say IEX Cloud as a history source):

1. Implement the fetch function in a new module — return rows in `earnings_history.COLUMNS` schema with `source="iex"`. Stamp `period_ending` to **day-1 of the fiscal-quarter month** at row construction (matching the Finviz/Zacks/Finnhub convention) so cross-source dedup keys match.
2. Add bulk/gap/spot fill functions following the `finviz_fill` / `finnhub_fill` template — both now delegate the loop to `fill_framework.run_fill_loop` with a `FillSpec` (checkpoint resume, periodic flush, block backoff-rewind, parse-spike alarm, finalize all come for free). Raw rows go to `earnings_raw/iex/<run_id>.parquet` before collapsing to the consumer schema; flushes use `fill_framework.flush_pending_to_disk` (per-`(ticker, source)` PK replacement) so re-pulls don't accumulate dups.
3. Update `_SOURCE_PRIORITY` in `dedupe_history` if the new source should slot into the per-`(ticker, period_ending)` priority chain. Current ordering: `finviz > zacks > finnhub`. A new source typically picks a position in the chain (most authoritative → least) and the lower-priority sources only fill slots no higher-priority source has covered.
4. Update `_LAST_PRIORITY` / `_NEXT_PRIORITY` in `earnings_reconcile.py` to insert the new source in the chain. Current order: `nasdaq > yahoo > finviz > zacks > finnhub` (live calendar feeds first, then history sources by authority). For IEX-as-history, pick its slot among the history sources — e.g. `nasdaq > yahoo > iex > finviz > zacks > finnhub`.
5. Add a Data menu action to trigger the IEX fill — follow the pattern at `gui/main_window.py` for the existing per-source actions (Bulk / Gap / Spot, with Stop button + worker thread), and give the worker a bar in the earnings progress panel (`gui/earnings_coordinator.py`).
6. The `cross_source_slot_overlap` integrity check and the cross-source disagreement report are source-agnostic (they key on `(ticker, period_ending)` + `source`), so they pick the new source up automatically once `_SOURCE_PRIORITY` knows it.
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

`compute_consecutive_beats` uses `period_ending` for cadence detection (not `report_date`). Reasoning embedded in the docstring at `earnings_history.py:777 ff.`:

> A ticker that beats every quarter for 5 quarters but delays Q3's announcement by 6 weeks would have report_date gaps of ~88 / ~88 / 132 / 175 / 88 days. The 175-day report_date gap would falsely truncate the streak under the prior implementation. Under period_ending the gaps are all ~91 days, no false break.

If you decide to revert this for any reason (e.g., you actually WANT late filings to break the streak as an audit-suspicious signal), the change is one line at `earnings_history.py:841`:
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
- **Failure classification** (FAIL_BLOCKED / FAIL_NOT_FOUND / FAIL_HTTP_ERROR / FAIL_PARSE_ERROR): drives the auto-pause heuristic. Only confirmed Imperva blocks count toward the consecutive-failure threshold. Since 2026-06, `FAIL_PARSE_ERROR` is live and reserved for **genuinely unparseable pages** — the `obj_data` token is present but neither the strict nor the fallback parser can read it. A readable-but-empty `obj_data = {}` (Zacks has no data for the ticker) still classifies as FAIL_NOT_FOUND.

### Drift-tolerant parsing + parse-failure spike alarm (2026-06)

Two resilience layers added so a silent Zacks page-format change can't
quietly blacklist half the universe:

- **Fallback `obj_data` parser** (`_extract_obj_data_fallback`,
  zacks_scraper.py): the strict parser pins the exact
  `document.obj_data = {` form; the fallback tolerates assignment-form
  drift (`window.obj_data`, `var obj_data`, `"obj_data": {…}` inside a
  larger literal, …) by trying every `obj_data`-ish assignment in the
  page. A recovery via the fallback logs a WARNING ("Zacks page format
  drifted") so the strict regex can be updated.
- **Parse-failure spike alarm** (`config.PARSE_SPIKE_MIN_SAMPLE = 25`,
  `config.PARSE_SPIKE_FAIL_PCT = 40.0`): every fill loop — the Zacks loop
  in `earnings_history.py` and the shared `fill_framework.run_fill_loop`
  used by Finviz/Finnhub — tracks parse failures as a fraction of fetch
  attempts. Once at least 25 attempts have been made and ≥40% of them
  are `parse_error`, the run **halts loudly** (log + status), the
  resumable checkpoint is **preserved** (a spike halt is treated like a
  user stop, not natural completion), and **no ticker is blacklisted**
  — a format break is upstream's fault, not the tickers'. Tests:
  [`tests/test_parse_spike.py`](trade_scanner_fh/tests/test_parse_spike.py),
  [`tests/test_zacks_scraper.py`](trade_scanner_fh/tests/test_zacks_scraper.py).

The Finviz client got a matching hardening: a **two-marker
block-vs-empty classification** — a 200 page without `earningsData` is
`FAIL_EMPTY` (uncovered ticker, blacklist-eligible) only when it still
looks like a real finviz quote page; otherwise it's `FAIL_BLOCKED`
(bot-block / challenge page, never blacklisted), and an unparseable
`earningsData` array is `FAIL_PARSE` so it feeds the same spike alarm.

### `ZacksSession` lifecycle

```python
with ZacksSession() as session:
    for sym in tickers:
        rows = session.fetch(sym, years=config.EARNINGS_HISTORY_YEARS)
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

- Share the same flush logic — every N successful pulls writes to `earnings_history.parquet` atomically (replace-by-`(ticker, source)`-PK semantics so re-pulls don't duplicate quarters; canonicalization-safe — uses the row's `ticker` field for the mask, not the pending dict key). The Zacks path keeps its `_flush_pending_to_disk` in `earnings_history.py`; the Finviz and Finnhub loops were unified onto `fill_framework.py` (2026-06) — `run_fill_loop(FillSpec, …)` owns checkpoint resume, periodic flush via `flush_pending_to_disk`, block backoff-rewind, the parse-spike alarm, and finalize. Hooks are resolved through the calling module at call time, so tests that monkeypatch the per-source private names keep working.
- Final finalize (`_finalize_fill` / `fill_framework.finalize_fill`) does one canonical sort + YoY recompute + one `reconcile_earnings_dates` call.
- All writes go through `config.atomic_write_parquet` (tmp file + `os.replace`) — crash-safe.
- Manual Zacks menu runs (`Bulk`/`Targeted Fill Earnings (Zacks)`) are still on demand. The automatic trigger is now the **launch-time concurrent smart refresh** (below) plus the daily Nasdaq calendar sweep.

### Launch-time concurrent smart refresh

> Since 2026-06 the implementation lives in
> [`gui/earnings_coordinator.py`](trade_scanner_fh/gui/earnings_coordinator.py)
> (`EarningsRefreshCoordinator`) — smart-refresh chaining, the daily
> Nasdaq cadence, per-source worker bringup, and the progress panel were
> extracted from MainWindow. Every method named below still exists on
> `MainWindow` as a thin delegate, so historical references remain valid.

After an OHLCV update completes (and only on the real-update path — a
fresh-cache skip never reaches it), `_kick_off_smart_refresh` computes one
shared per-ticker candidate set (`find_smart_refresh_candidates` — tickers
due for a new quarter, minus the OHLCV blacklist + ETF/ADR auto-skip) and
launches **all three history sources against it concurrently** (finviz +
zacks + finnhub, each on its own `QThread`). Each source then applies its
own per-source skip set; whichever lands a quarter first wins the
`finviz > zacks > finnhub` dedup. A bulk-sized candidate set (>
`ZACKS_SMART_REFRESH_BULK_THRESHOLD`) prompts Run / Skip / Disable first.

**Daily calendar + same-launch capture (2026-06-06).** The candidate
selector keys off the earnings calendar's `last_earnings`
(`earnings_dates.parquet`), so it can only flag a ticker once the calendar
knows it reported. The Nasdaq calendar sweep is now **daily**
(`config.NASDAQ_REFRESH_DAYS = 1`, a *calendar-day* check in
`_is_nasdaq_refresh_due` — not a rolling 24 h gap, so the first launch of a
new day re-sweeps regardless of clock time) instead of weekly. And
`_on_update_done` now **defers** the smart refresh until that launch's Nasdaq
sweep + reconcile finish (fired from `_on_nasdaq_auto_refresh_done` via the
`_pending_smart_refresh` flag) — so the selector reads a freshly-updated
`last_earnings` and captures a just-reported quarter in the *same* launch
rather than the next one. When no sweep is due, the refresh runs immediately.
(Still gated to the real OHLCV-update path — a fresh-cache / weekend launch
does no earnings work; decoupling that gate is a known follow-up.)

All three workers run in `mode="targeted"` — the smart-refresh mode that
iterates *exactly* the supplied candidate list (no checkpoint resume, no
re-computing gaps). For Finviz/Finnhub this is the same primitive as their
`gap` mode; for Zacks it is `targeted_fill_zacks`.

**Manual trigger.** `Data → Run Earnings Smart Refresh Now`
(`_run_earnings_smart_refresh_now`) fires the identical concurrent process
on demand — useful when the OHLCV freshness gate skipped the update
(cache already current) so the auto path never ran. It reuses the same
candidate selection + bulk-threshold guard, then `_launch_smart_refresh_workers`
(the launch block, factored out so both paths share it). If nothing is
due (everything fresh), it offers a **sample test pass** against the first
25 universe tickers so the concurrent fill + progress panel are still
observable. No-ops with an info dialog if a fill is already running.

> **Fixed 2026-05-31:** `FinvizFillWorker`/`FinnhubFillWorker` previously
> only recognised `{"bulk","gap"}` and crashed instantly with
> `ValueError: unknown …Worker mode: 'targeted'`, so two of the three
> sources died on arrival and only Zacks ran. Both workers now accept
> `"targeted"` (routed to their `gap_fill_*` primitive).

**Progress panel.** Three concurrent workers used to clobber the single
shared status bar, leaving no usable progress surface. A dedicated
earnings-fill panel sits just above the status bar (built in
`_build_earnings_progress_panel`): one colour-coded bar per source
(finviz = blue, zacks = purple, finnhub = green) showing live
`%p% (done/total)`, **hover any bar** for a tooltip with status +
`Progress: d/t` + `Filled / Errors`, and a single **Stop Earnings
Refresh** button below that halts every running fill at once. When all
sources finish the button flips to **Hide** and the panel auto-collapses
after 12 s. The panel is reused for manual single-source fills too; a
fresh batch resets stale "done" bars back to idle. (State-based collapse
logic — not `QThread.isRunning()` — because the workers emit their custom
`finished` signal from inside `run()`, so the just-finished thread still
reports running when collapse is evaluated.)

---

## The Finviz earnings scraper (top-priority source)

Added 2026-05-31 as the highest-priority per-quarter source
(`finviz > zacks > finnhub`). Like Zacks it is an HTTP scrape via
`curl_cffi` Chrome TLS impersonation (no API key), but far simpler — the
data is a clean JSON blob, not HTML tables.

**Endpoint.** `https://finviz.com/quote.ashx?t={SYM}&ty=ea` (equivalent
to the `…/stock?t=SYM&ta=1&p=d&ty=ea` URL finviz links to). The page
(~1.16 MB) embeds a JSON array under the `"earningsData"` key — one
object per fiscal quarter. `finviz_client._extract_earnings_data` locates
the key and parses the array with `json.JSONDecoder().raw_decode` (a real
JSON scanner — robust to brackets/escapes inside string values, unlike
naive bracket-counting).

**Fields used (adjusted / non-GAAP).** Each entry carries *both* bases;
we take the adjusted ones and ignore the GAAP `eps*Reported*` fields:

| finviz field   | maps to            | note |
|----------------|--------------------|------|
| `epsActual`    | `reported_eps`     | adjusted — matches Zacks to the penny on ~98% of quarters |
| `epsEstimate`  | `estimated_eps`    | analyst consensus (adjusted) |
| `salesActual`  | `reported_rev`     | $M, finer precision than Zacks |
| `salesEstimate`| `estimated_rev`    | |
| `fiscalEndDate`| `period_ending`    | normalized to day-1 of the fiscal-quarter-end month |
| `earningsDate` | `report_date` + `report_time` | time → `Close` (hr≥16) / `Open` (hr<12) / `Unknown`; `report_date_proxy=False` |

`surprise_eps`/`surprise_rev` (+ pct) are derived (`actual − estimate`).
Rows without an `epsActual` are forward analyst estimates and are skipped
from the per-quarter history; the `EARNINGS_HISTORY_YEARS` cap (10) is
applied like every other source.

**`next_earnings` contribution (2026-06-06).** A skipped forward row still
carries a real scheduled `earningsDate`. `finviz_fill` captures the nearest
*future* one per ticker (`_next_date_from_entries`) and writes a
`source="finviz"` row (last = NaT, next = date) into `earnings_dates.parquet`;
`earnings_reconcile` then builds finviz's lookup as **history → `last` +
dates → `next`** (finviz is already in `_NEXT_PRIORITY`). So finviz augments
the `next_earnings` chain (`nasdaq > yahoo > finviz > zacks > finnhub`)
*without* putting future NaN-actual rows into the per-quarter history. The
durable flush happens inside `_persist_progress` (before the checkpoint
advances) so a crashed overnight bulk doesn't silently drop captured dates.
*Caveat:* finviz's forward horizon is near-term only, and the page's
top-level `"earningsDate"` field (distinct from the `earningsData` array — it
backs the snapshot "May 27 AMC" cell) is **not** yet read, so a thin ticker
whose `earningsData` has no forward row contributes no finviz next date.

**Fill modes** (`finviz_fill.py`): bulk / gap / spot, mirroring the
Finnhub control surface (checkpoint resume at `FINVIZ_BULK_CHECKPOINT`,
block backoff, `finviz_blacklist.txt` for uncovered tickers). The bulk
is **deliberately slow** — one ~1.16 MB request per ticker at
`FINVIZ_MIN_INTERVAL_SEC=4.0` ± `FINVIZ_JITTER_SEC` (~13 req/min, ~11 h
for a ~10k universe) so it runs safely overnight without tripping
finviz's throttle. ETFs/ADRs and the universe OHLCV blacklist are
pre-skipped via the GUI's `_combined_finviz_skip_set`, so funds never
cost a request.

> Because finviz is top priority, running a finviz fill makes it win all
> overlapping `(ticker, period_ending)` slots; write-time dedup then
> drops the superseded zacks/finnhub rows from disk (originals stay in
> `earnings_raw/`).

---

## EDGAR scrape and conversion (REMOVED — forking reference only)

> **⚠️ Removed from this app 2026-05-31.** SEC EDGAR was a GAAP earnings
> source; GAAP figures aren't useful for this scanner's trading use case,
> and every remaining EPS source is single-basis (adjusted). The
> `edgar_client.py` / `edgar_fill.py` modules and all `source=edgar`
> parquet rows were deleted, and **Finviz** took EDGAR's top-priority
> slot. This section is retained purely as a **forking reference** for
> porting the XBRL extraction algorithm into another project; none of the
> code below exists in the current tree. (The separate SEC
> `company_tickers.json` download survives as a *universe-building*
> ticker source — that's unrelated to the earnings algorithm here.)

The EDGAR algorithm was designed to be self-contained and forkable —
`edgar_client.py` carried the parsing primitives, `edgar_fill.py`
wrapped the loop, and zero dependencies on the rest of the scanner were
required to lift the algorithm into another project.

### Two endpoints

| Endpoint        | URL                                                                                | What it returns                                                                                                                                                       | Size       |
| --------------- | ---------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| **Submissions** | `https://data.sec.gov/submissions/CIK{padded}.json`                                | Last ~1000 filings: parallel arrays `form[] filingDate[] accessionNumber[] reportDate[]`.                                                                             | ~50-500 KB |
| **Companyfacts**| `https://data.sec.gov/api/xbrl/companyfacts/CIK{padded}.json`                      | Every us-gaap XBRL fact the issuer has ever reported, nested as `facts.us-gaap.{Tag}.units.{Unit}` → list of `{accn, start, end, val, fy, fp, form, filed}` dicts.    | ~1-10 MB   |

Both keyed by zero-padded CIK (10 digits — AAPL = `0000320193`).

No API key. SEC fair-access requires a `User-Agent` with a real contact
email. Resolved at request time by `config.get_sec_user_agent()` via
`scanner_data/sec_contact.txt` → `SEC_CONTACT_EMAIL` env var →
`SEC_CONTACT_DEFAULT`. Rate-limited to `EDGAR_MIN_INTERVAL_SEC` (default
0.4 s = ~2.5 req/sec single-pacing, ~5 req/sec aggregate when both
per-ticker calls fire back-to-back — safely under the 10 req/sec hard
cap) via the process-wide `_limiter`. The default was tuned down from
0.21 s after a bulk run tripped SEC's rate-limiter mid-stream at
ticker ~4200; sustained 5+ req/sec aggregate burst patterns trigger
the limiter even when the per-call pacing is under the published cap.

### `list_earnings_filings(cik)` — strict 10-K / 10-Q filter

Walks the `filings.recent.form[] / filingDate[] / accessionNumber[] /
reportDate[]` parallel arrays. Strict filter:

- `form ∈ {"10-K", "10-Q"}` (exact — no `10-K/A` amendments, no `8-K`
  earnings-release announcements, no `20-F` foreign filers).
- `filingDate ≤ today` (defensive against future-dated entries).
- `accession` matches `\d{10}-\d{2}-\d{6}`.

Returns `[{"form", "accession", "file_date", "report_date"}, ...]`
sorted most-recent-first by file_date.

### Three conversion primitives in `edgar_client`

**A. `filing_period(facts_dict, accession)` → `(start_iso, end_iso) | (None, None)`**

Walks every fact with the given `accn`, keeps facts that have BOTH
`start` and `end` (period-flow / income-statement facts; balance-sheet
items with only `end` are dropped). Returns the `(start, end)` of the
latest `end`.

Why match by `accn` rather than `fp + fy`? A 10-K reports the same
revenue concept at THREE different `(start, end)` tuples (current FY,
prior FY, two-years-prior FY) all tagged `fp=FY fy={filing_fy}`.
Calendar-quarter guessing misattributes on AAPL (FY ends late Sept),
NVDA (52/53-week fiscal calendar), MSFT (FY ends late Jun). `accn` +
latest `end` is the only robust pin.

**B. `find_fact_by_period(facts_dict, tag_chain, start_iso, end_iso, tolerance_days)` → `float | None`**

Walks `tag_chain` left-to-right. For each tag, walks all units, all
facts. Keeps only `form ∈ {"10-K","10-Q"}` and `(start, end)` matching
the target (exactly when `tolerance_days=0`, else within
`±tolerance_days` on both ends). On tie, prefers the later `filed` date
(restatements win — same convention Bloomberg / Capital IQ use). First
tag with a hit returns.

**C. `extract_quarter_values(facts_dict, accession)` → `{period_start, period_end, reported_eps, reported_rev_usd}`**

Composes A + B. Resolves the filing's `(start, end)` via `filing_period`,
then runs `find_fact_by_period` against `EPS_TAGS` and `REVENUE_TAGS`.
Returns the raw USD revenue — `edgar_fill._filing_to_history_dict`
divides by 1,000,000 before writing the parquet row (parquet uses $M to
match Zacks's convention).

### Tag chains (left = most specific, right = oldest fallback)

```python
REVENUE_TAGS = (
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "SalesRevenueNet",
    "SalesRevenueGoodsNet",
)
EPS_TAGS = (
    "EarningsPerShareDiluted",
    "EarningsPerShareBasic",
    "IncomeLossFromContinuingOperationsPerDilutedShare",
)
```

Companies tag the same concept with different us-gaap tags depending on
when they were last reviewed + which filing software template. Walking
left-to-right with first-hit-wins handles cross-company variance.

### `build_accn_index` — the perf primitive

```python
def build_accn_index(facts_dict) -> dict:
    """Returns {accession: [fact_dict, ...]} for every us-gaap fact."""
    index: dict = {}
    ns = (facts_dict or {}).get("facts", {}).get("us-gaap") or {}
    for tag_name, tag_data in ns.items():
        for unit_name, fact_list in (tag_data.get("units") or {}).items():
            for f in fact_list:
                accn = f.get("accn")
                if not accn: continue
                fc = dict(f); fc["tag"] = tag_name; fc["unit"] = unit_name
                index.setdefault(accn, []).append(fc)
    return index
```

Built once per CIK at `fetch_companyfacts` time and attached to the
facts dict as `_accn_index`. `facts_for_filing` checks for it and
fast-paths through it; per-filing lookup drops from `O(all facts in
blob)` to `O(facts for filing)`. Without the index, a 24-quarter bulk
fill walks the whole namespace 24× per ticker.

### Period conversion (parquet ↔ EDGAR)

- Parquet `period_ending`: day-1 of the last month of the fiscal
  quarter (e.g. Q1 2026 → `2026-03-01`). Works for both calendar and
  non-calendar fiscal companies.
- EDGAR `period_end`: actual last day (`2026-03-31`).
- `period_end_to_parquet_day1(period_end_iso)` converts the EDGAR form
  to the parquet form. Round-trips: same fiscal quarter regardless of
  source, so `(ticker, period_ending)` dedup works cross-source.

### Per-ticker fetch flow in `edgar_fill._fetch_one_ticker`

1. `edgar_client.get_cik(ticker)` → padded CIK string or None. None →
   `is_no_cik=True` (blacklist-eligible — definitive permanent miss).
2. `fetch_submissions(cik)` → submissions blob. None on fetch failure
   → carries `last_failure_kind()` (`rate_limited`, `network`, etc.)
   and counts toward the block streak; NOT blacklisted.
3. `list_earnings_filings(cik, submissions, today)` → filtered
   10-K/10-Q list. Empty list → `is_empty=True, empty_reason=NO_FILINGS`
   (blacklist-eligible — issuer has never filed 10-K/10-Q).
4. Filter the filings to the 5-year cutoff window. Empty after filter
   → `is_empty=True, empty_reason=OUT_OF_WINDOW` (blacklist-eligible —
   issuer is delisted or hasn't filed in 5+ years).
5. `fetch_companyfacts(cik)` → facts blob with `_accn_index` pre-built.
   None on fetch failure → carries `last_failure_kind()`; NOT blacklisted.
   If `facts.us-gaap` is empty/missing → `is_empty=True,
   empty_reason=NO_US_GAAP` (blacklist-eligible — foreign filer using
   `ifrs-full`).
6. Per filing: `extract_quarter_values(facts, accession)` →
   `_filing_to_history_dict` → row dict in canonical schema.
   `period_ending` is normalized to day-1; `report_date` = filing date
   exactly; estimates / surprises NaN (EDGAR has no analyst data).
7. Same-period dedup (a 10-K and a prior 10-Q can both cover the same
   quarter): keep the newest filing.
8. If after all that no rows were produced (filings + facts both
   present but the parser found no EPS / revenue under the current
   tag chains) → `is_empty=True, empty_reason=NO_VALUES`. **NOT**
   blacklist-eligible — a future tag-chain expansion might pick this
   ticker up on the next gap_fill, so the ticker stays available.

### Empty-reason taxonomy + blacklist eligibility

`_FetchResult` carries an `empty_reason` field set alongside `is_empty`
that distinguishes definitive permanent coverage misses (safe to
auto-blacklist) from parser-side or transient misses (must NOT
auto-blacklist). `_BLACKLISTABLE_EMPTY_REASONS` in `edgar_fill.py`
(deleted module — see the removal note above) declared the set:

| Reason             | Blacklist? | Why |
|--------------------|:----------:|-----|
| `no_cik`           | ✓ | Ticker not in SEC registry; permanent. |
| `NO_FILINGS`       | ✓ | Issuer has zero 10-K / 10-Q filings; permanent. |
| `OUT_OF_WINDOW`    | ✓ | Filings exist but all > 5y old; delisted / dormant. |
| `NO_US_GAAP`       | ✓ | Foreign filer using `ifrs-full`; permanent under current tag chains. |
| `NO_VALUES`        | ✗ | Filings + facts present but parser found nothing; future tag-chain update could unlock — leave on gap list. |
| transient (429/network) | ✗ | Counts toward block streak; per-failure log at WARNING. |

Tests in `tests/test_edgar_fill.py` (deleted with the module) pinned
each of these classifications + the matching `is_blacklistable` flag.

### Block-recovery policy

`_fill_via_edgar` uses a two-stage recovery on consecutive real
failures (`EDGAR_CONSEC_BLOCK_LIMIT = 5` defaults):

- **First block in the failure window** → pause
  (`EDGAR_INITIAL_BLOCK_PAUSE_SEC = 300 s`, doubling per subsequent
  block, capped at `EDGAR_MAX_BLOCK_PAUSE_SEC = 1800 s`), then **rewind
  to first_fail_idx** so transient blips (one-off 5xx / network) get
  a retry under fresh state.
- **Second block in the same window** → pause again, then **skip past
  the failure cluster** instead of rewinding. The same N tickers were
  about to re-fail under SEC's still-active rate-limit; better to move
  on so the rest of the universe still gets processed. The skipped
  tickers stay un-completed in the checkpoint so a future `gap_fill`
  picks them up.
- After `EDGAR_MAX_BLOCKS_PER_RUN = 3` blocks total → invoke
  `on_block_callback`; the GUI worker returns `"stop"` and the loop
  exits cleanly. **The checkpoint is preserved on any non-natural
  halt** so the next launch resumes from where it left off — only
  full completion (every ticker processed) clears it.

### What EDGAR can't do

- **Surprise %** — XBRL has no analyst estimates. EDGAR-source rows
  always carry NaN `surprise_*` columns; the scanner's surprise filters
  treat NaN per the global earnings-data flag.
- **Foreign filers using `ifrs-full`** — 20-F filings (Alibaba, Toyota)
  don't match the us-gaap tag chains. Gracefully degrade to coverage
  miss; ticker auto-added to EDGAR blacklist.
- **Pre-2009 quarters** — XBRL became mandatory in 2009.
- **Future dates** — EDGAR filings are always past. EDGAR never wins
  the reconciler's next_earnings slot.

### Forking checklist

If forking the algorithm into another project, you need:

1. `requests` for HTTP plus a rate limiter. SEC's hard cap is
   10 req/sec but sustained ≥5 req/sec aggregate bursts trip the
   limiter in practice — `EDGAR_MIN_INTERVAL_SEC = 0.4` (~2.5 req/sec
   single, ~5 req/sec aggregate when both per-ticker calls fire
   back-to-back) is the empirically safe default.
2. A `User-Agent` constant with a real contact email (SEC requirement;
   `403 Forbidden` without).
3. Three module-level constants: `_SUBMISSIONS_URL`,
   `_COMPANYFACTS_URL`, `_TICKERS_URL` plus the tag chains.
4. The five primitives: `build_accn_index`, `facts_for_filing` (with
   fast-path on `_accn_index`), `filing_period`, `find_fact_by_period`,
   `extract_quarter_values`.
5. A submissions cache (`OrderedDict` + lock + LRU cap of ~20) and a
   companyfacts cache (same shape, ~20 entries). The fetch helpers in
   `edgar_client` deliberately don't add an LRU — the higher-level
   per-fill loop processes each CIK once per run so caching there
   would just add memory.
6. `list_earnings_filings`-style helper for the strict 10-K/10-Q filter.
7. Streaming + 50 MB cap on `fetch_companyfacts` (`requests.get(stream=
   True)` + chunk-drain into byte cap; otherwise hostile origin can
   balloon memory).

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

### MainWindow decomposition (2026-06)

`gui/main_window.py` was decomposed to keep it tractable: earnings-refresh
orchestration moved to `gui/earnings_coordinator.py`
(`EarningsRefreshCoordinator` — smart-refresh chaining, Nasdaq cadence,
per-source worker bringup, the 3-bar progress panel), per-source skip-list
plumbing to `gui/blacklists.py` (`BlacklistManager`), the XLSX/CSV export
pipeline + Quick Export to `gui/exports.py` (`ExportsController`), and
manual column order / hidden-set state to `gui/columns.py`
(`ColumnManager`). The scan scheduler is `gui/scheduler.py`
(`ScanScheduler`). **Every historical method name was kept on `MainWindow`
as a thin delegate** (e.g. `MainWindow._quick_export` →
`ExportsController.quick_export`), so existing references — in this README,
in tests, and in muscle memory — still resolve. New logic should go in the
helper module, not back into MainWindow.

### Threading model

| Thread | What runs |
|--------|-----------|
| Main (Qt event loop) | All widgets, model updates, slot handlers, `populate()`; also the `ScanScheduler`'s ~30 s `QTimer` (firing only *triggers* the normal worker path) |
| ScanWorker | `run_scan()` — heavy CPU loop |
| ZacksFillWorker | The 6.5h Zacks fill — uses `_on_imperva_block` callback to coordinate with main thread for cookie refresh |
| FinnhubFillWorker | Finnhub deep-history bulk / gap / spot fills |
| FinvizFillWorker | Finviz bulk / gap / spot fills (top-priority source) |
| UpdateWorker / UniverseWorker / UniverseRefreshWorker / SectorFillWorker / EarningsFillWorker | Background data refreshes |
| PrefetchWorker | Opt-in launch-time OHLCV cache warm (`data_engine.prefetch_ohlcv`); starts only after the startup update finishes or is skipped; stoppable |
| FirefoxCookieWaitWorker | Polls Firefox + cookies.sqlite |
| BridgeWorker | TradeStation watchlist push |

Cross-thread coordination uses Qt signals (queued connections) for one-way flow and `threading.Event` for the auto-pause two-way coordination (worker blocks on Event, main thread sets Event after dialog).

### Session controls (the bar under the Scan button)

Two independent omission toggles, each scoping at a different layer.
Default off on first launch; checkbox state round-trips through saved
presets (v6 preset schema — see [Preset format](#preset-format-v6))
so a user can capture their preferred session-filter posture per
preset. The accumulated `_session_seen` set itself is NOT preset-saved
— only the toggle state; the seen set always starts empty on launch.
The `Reset Session` button clears `_session_seen` and the scan counter.

**`chk_omit_seen`** — "Omit previously scanned tickers"

- *Scope:* across every scan in this launch.
- *State store:* `self._session_seen: set[str]` (main_window.py:141) — union of every period's hits from every scan, updated in `_on_scan_done_impl`.
- *Filter point:* `_run_scan` strips matching tickers from `filtered` BEFORE building `params_list`.

**`chk_omit_intra_run`** — "Omit earlier-period hits (this run)"

- *Scope:* within a single multi-period or sequenced run.
- *State store:* `working_symbols: list[str]` inside `ScanWorker.run()` — lives only for the scan; mutated between iterations of `params_list`.
- *Filter point:* the worker strips the previous period's hit symbols from `working_symbols` BEFORE dispatching the next period's `run_scan`.

**Why two layers.** `chk_omit_seen` is for "I've already triaged those
tickers in a previous scan this session — don't show them again."
`chk_omit_intra_run` is for "if a ticker qualifies on 1D, I don't also
want to see it on 1M / 3M / 6M — give it to the most proximal period
and move on." The two can be active simultaneously; the across-scan
filter runs first (drops universe entries before dispatch), then the
intra-run filter applies between periods inside the worker.

**Ordering dependency.** The intra-run filter only behaves correctly
because the worker iterates `params_list` in the order the GUI built
it. `_run_scan` enforces smallest-timeframe-first for multi-period
selection (sort by `days` ascending; custom range last) and relies on
`chunk_periods()` returning newest-chunk-first for sequenced runs. If
that ordering ever changes, the "first claim wins" semantics flip.

**Logging.** When the intra-run filter is on and stripping has
occurred, the worker emits `"Intra-run filter: scanning N of M
tickers (K hit earlier period(s))"` before each non-first period.
When all symbols have been claimed, the period is skipped with an
empty DataFrame still recorded in `period_results` so the timeframe
dropdown shows it as "0 results" rather than vanishing.

### Latest-trading-day anchoring (2026-06-06)

The scanner shows earnings **as-of** the scan End date (`report_date <=
end_ts`), so an End field left behind the freshest data silently hides
quarters that were reported in between. Two changes keep the End anchored
to real data:

- **Quick-range buttons re-anchor End.** Clicking a timeframe button
  (`1D` / `1W` / `1M` / `3M` / `6M`) now calls `_set_quick_range(days)`,
  which sets **End** to the latest available trading day and **Start** to
  `End − days` — so "1D" means the last trading day, not "today minus the
  stale End field." The latest date comes from `_latest_data_date()`: the
  max bar date over a 30-ticker strided sample of the cached OHLCV (all
  actively-traded names share the trading calendar, so a small sample's
  max is the latest available date), cached and invalidated after every
  OHLCV update (`_on_update_done` clears `_latest_data_date_cache` *first*).
  Falls back to `config.last_market_close()` when no OHLCV is cached yet.
  The launch default for the End picker is also `last_market_close()`
  rather than `QDate.currentDate()`, so a weekend/holiday launch doesn't
  point End at a day with no bar.
- **Stale-End warning at scan launch.** When a scan runs with a manual
  End (not via a quick-range button) that is *behind* `_latest_data_date()`,
  `_run_scan` emits a ⚠ log line — "End date {end} is behind the latest
  available data ({latest}) — scanning AS-OF a past date, so
  recently-reported quarters are excluded. Click a timeframe button to
  jump to the latest." The manual date pickers + Custom Range are
  untouched for deliberate historical scans; the warning just surfaces the
  trap. Covered by `test_end_date_anchor.py`.

### Crash hardening (post-2026-05 audit)

- `ScanWorker.run()` wrapped in try/except — emits `finished` with partial results on crash instead of dying silently.
- `_on_scan_done` slot wrapped — shows "Scan crashed — see log" banner instead of taking down the exe.
- `ResultsTable._populate_row` is per-row try/except — a single bad row leaves an empty cell instead of crashing the whole table.
- `_safe_streak` helper handles NaN/None/non-numeric (Python's `bool(nan) is True` would otherwise turn `int(nan or 0)` into a crash).
- `LogPanel._append` flushes after every write so post-mortem diagnostics actually have data.
- `ResultsTable.populate` chunks via `QApplication.processEvents()` every 200 rows so Windows can't flag the process as "not responding" during a 15k-run render.

### Data menu structure

The Data menu is split into groups by separators. The six earnings /
cross-check / diagnostics groups each open with an inline disabled-action
label row (`— Earnings (Zacks — primary) —`, etc.); the Universe/OHLCV and
Sector-map groups are separator-divided only — they have no label row. The
grouping reflects the five-source architecture plus diagnostics:

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

— Earnings (Finviz — top priority) —
    Bulk Fill Earnings (Finviz)
    Gap Fill Earnings (Finviz)
    Spot Fill Earnings (Finviz)...
    Stop Finviz Fill
    Edit Finviz Skip List...

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
    Auto-refresh Nasdaq calendar daily   (checkable)
    Run Earnings Smart Refresh Now

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
—
Advanced…
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
- **Advanced…** — user-configurable tunables persisted to the gitignored
  `scanner_data/user_config.json`: `OHLCV_HISTORY_YEARS` (1–25, default 5),
  `EARNINGS_HISTORY_YEARS` (1–25, default 10), the
  `REFERENCE_TICKERS` benchmark list, and the launch-time OHLCV prefetch
  toggle (`PREFETCH_OHLCV_AT_LAUNCH`, default off). `config.load_user_config()`
  applies valid overrides at module import (the bottom of `config.py`);
  the dialog's OK applies them to the live config module immediately — no
  restart. Values are clamped to their ranges, and a corrupt / non-object
  `user_config.json` degrades to defaults instead of crashing
  (`tests/test_user_config.py`).

### Scans menu structure (F3, 2026-06)

```text
Quick Export
Schedule…
```

- **Quick Export** — one-click timestamped XLSX snapshot to
  `scanner_data/exports/` (see [Quick Export + scan scheduler](#quick-export--scan-scheduler-f3)).
- **Schedule…** — manage scheduled scans (same section).

Each fill writes to its dedicated parquet (`earnings_history` for Finviz/Zacks/Finnhub; `earnings_dates` for Nasdaq/Yahoo) and triggers an auto-reconcile against affected tickers. Internal identifiers, slot method names (e.g. `_on_zacks_fill_done`, `_on_finnhub_fill_done`), and parquet `source` column values use bare source names (`"finviz"`, `"zacks"`, `"finnhub"`, `"nasdaq"`, `"yahoo"`).

### Delete rows from results table

Hard-delete rows from the active period's scan output via:

- **`Delete` key** when one or more rows are selected
- **Right-click → "Delete selected row(s)"** on the results table

Multi-select via standard Qt extended selection (shift-click for ranges, ctrl-click for non-contiguous). The deletion mutates `MainWindow._period_results[<active_period>]` directly, so it persists across view-filter toggles, sort changes, timeframe-tab switches, and exports. Reset implicitly on the next scan (fresh `_period_results` overwrites the dict).

`ResultsTable.rows_deletion_requested` signal is the wire — emitted by both triggers, handled by `MainWindow._on_rows_deletion_requested`.

**Undo delete (single level, F2).** The most recent deletion batch can be restored via **Ctrl+Z** (results table focused) or **right-click → "Undo delete"**. The delete handler snapshots the doomed rows + their positional indices before mutating; `_on_undo_delete_requested` reinserts them at their original spots via the pure helper `widgets.restore_rows_at_positions`. One level only — a new delete overwrites the snapshot, an undo consumes it (double-undo no-ops), and a fresh scan clears it (alongside the cut clipboard). Wire: `ResultsTable.undo_delete_requested` → `MainWindow._on_undo_delete_requested`; the table only tracks availability (`set_undo_available`) so its context menu can show/hide the action.

### Watchlist diffing (Chg column + per-period log lines)

On every scan completion, `MainWindow._apply_watchlist_diff` (backed by the GUI-free `scan_history.py`) compares each period's ticker set with the previous run of the same **(preset name or `adhoc`, period)** key, persisted in `scanner_data/scan_history.json` (atomic writes via `config.atomic_write_text`; corrupt/missing file = "no prior run"). Effects:

- **"Chg" column** in the results table — `NEW` for tickers absent from the prior run of the same key, blank otherwise (blank everywhere on a key's first-ever run).
- **One log-panel line per period** — `vs last <preset>/<period> run: N new, M dropped (DROPPED: ...)`, listing up to 20 dropped symbols (`+k more` beyond that); first run logs `no prior run`.

Only the latest prior set per key is stored (single-level diff), plus a bounded rolling summary list (timestamp, preset, period, count — last 200) for the future scheduler to extend. A diff/persist failure never blocks scan completion (whole step wrapped in try/except).

### Quick Export + scan scheduler (F3)

**Quick Export** (menu **Scans → Quick Export**, also fired automatically after every scheduled scan): writes the current results — ALL periods, current visible columns (the table's active layout in the user's drag order, view filters applied) — straight to `scanner_data/exports/scan_<presetOrAdhoc>_<YYYYMMDD-HHMMSS>.xlsx` via the same multi-sheet XLSX pipeline as the Excel dialog, with defaults (no News columns, no colors) and **no dialog**. The full path lands in the log panel. The exports dir is created lazily; implementation in `ExportsController.quick_export` (gui/exports.py).

**Scan scheduler** (`gui/scheduler.py`, managed via **Scans → Schedule…**): entries `{label, preset_name, time "HH:MM" local, days (weekday list, 0=Mon), enabled}` persist atomically to `scanner_data/schedules.json` (corrupt file degrades to "no schedules"). A ~30s `QTimer` owned by `ScanScheduler` (a QObject on MainWindow) fires due entries — each **at most once per day** (last-fired date recorded per entry; an app launched after the configured time still fires that day's entry). Firing loads the named preset through the existing preset-load path and triggers the existing scan path, so the F2 watchlist diff + `scan_history.json` rolling summary extend automatically. On completion the scheduler auto Quick-Exports the results and shows a Windows toast via a lazily-created `QSystemTrayIcon.showMessage` ("Scheduled scan '<label>': N results, M new" — counts from the primary period). Firing is skipped with a log line when a scan is already running (entry retries on a later tick, NOT consumed) or the preset no longer exists (consumed for the day). The Schedule… dialog is a table of entries with Add/Edit/Remove and an in-place Enabled checkbox; every mutation persists immediately.

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

- `hotkey.py` — Qt-free, headless-testable: `HotkeyConfig` dataclass + `send_ticker(ticker, cfg)` (click → wait → typewrite → end-key). Mirrors `tradestation.py`'s pattern of saving / restoring `pyautogui.FAILSAFE` and `PAUSE` around each invocation. The ticker is typed **lowercase** — see the safety note below.
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

**Safety — lowercase typing (v5.2.0):** `send_ticker` (and the bulk `tradestation.py` bridge) type the symbol in **lowercase**. `pyautogui.typewrite` produces capital letters by *holding Shift around each letter*, and `Shift`+letter collides with platform order-entry hotkeys — notably **TradeStation's chart Trade Bar**, where each capital of an UPPERCASE ticker fired a separate order hotkey (staging/submitting orders and splitting the symbol across the order bar and the command line). Lowercase letters carry no modifier, so they can't trigger a modifier+key hotkey; symbol entry is case-insensitive (`sdot` → SDOT) so search/load still works. A module-level `HOTKEY_FOCUS_DIAGNOSTICS` flag (default off) can re-enable per-send foreground/focused-window logging via `GetGUIThreadInfo` for future input-routing triage; `hotkey_probe.py` (repo root) is a standalone, staged (click-only / type / full) bisection probe for the same purpose (**SIM-mode only**).

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

### Preset format (v6)

JSON files at `scanner_data/presets/{name}.json`. `PRESET_SCHEMA_VERSION = 6`. v6 fields:

```json
{
  "_preset_version": 6,
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
  "column_hidden": [str, ...],
  "omit_previously_scanned": bool,
  "omit_earlier_period_hits": bool
}
```

**v6 changes (replaced v5):**

- **Added** `omit_previously_scanned` and `omit_earlier_period_hits` — the two session-bar omission toggles (`chk_omit_seen` and `chk_omit_intra_run`; see [Session controls](#session-controls-the-bar-under-the-scan-button)). Previously these defaulted off on every launch regardless of preset; now they round-trip per preset. The accumulated `_session_seen` set itself is NOT saved — only the checkbox state; the seen set always starts empty on launch. Pre-v6 presets missing these keys load as `False` (matches prior default-off behavior).

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
| `ohlcv/{TICKER}.parquet` | DataFrame | yfinance (or polygon if added) | Daily OHLCV cache, `OHLCV_HISTORY_YEARS` (default 5, Advanced-configurable) |
| `universe.csv` | CSV | NASDAQ FTP + GitHub + SEC | Ticker universe with metadata |
| `sector_map.parquet` | DataFrame | Finnhub `/stock/profile2` (with FinanceDatabase + yfinance fallback) | ticker → (sector, sector_etf); 56-key SECTOR_ETF_MAP routes Finnhub sub-industries to SPDR ETFs |
| `earnings_dates.parquet` | DataFrame | Reconcile output (`nasdaq > yahoo > finviz > zacks > finnhub` priority chain) | last/next earnings dates per ticker (1:1 with ticker) |
| `earnings_history.parquet` | DataFrame | Finviz scrape + Zacks scraper + Finnhub `/stock/earnings` | Per-quarter EPS / revenue history (`EARNINGS_HISTORY_YEARS`, default 10); per-slot priority dedup (finviz > zacks > finnhub) |
| `earnings_raw/{source}/<run_id>.parquet` | DataFrame | Each fill's raw response | Append-only audit/replay layer; pruned at startup if older than `RAW_RETENTION_DAYS` (30) |
| `earnings_disagreements.csv` | CSV | `report_cross_source_disagreements` (rewritten at every canonical history save) | Report-only cross-source EPS disagreement findings; always reflects the latest save |
| `.finviz_bulk_checkpoint.json` / `.finnhub_bulk_checkpoint.json` | JSON | `fill_framework` | Resumable bulk-fill progress; cleared only on natural completion (preserved on stop / block-halt / spike-halt) |
| `.gap_fill_dedup_v1.done` | sentinel | `migrate_to_gap_fill_dedup` | One-time gap-fill dedup migration marker |
| `zacks_cookies.txt` | text | Firefox cookie capture | Imperva session tokens (DPAPI-encrypted at rest) |
| `firefox_zacks_profile/` | Firefox profile | Firefox itself | Persistent profile (login, cookies) |
| `blacklist.txt` | comma-separated text | User | Tickers to skip during refresh |
| `zacks_blacklist.txt` | newline-separated text | Auto + user | Zacks-specific skip list (auto-added on FAIL_NOT_FOUND); one ticker per line |
| `finnhub_blacklist.txt` | newline-separated text | Auto + user | Finnhub-specific skip list (auto-added on empty `/stock/earnings` response — typically ETFs); one ticker per line |
| `finviz_blacklist.txt` | newline-separated text | Auto + user | Finviz-specific skip list (auto-added on definitive FAIL_EMPTY — uncovered tickers); one ticker per line |
| `sec_contact.txt` | text | User (Settings → Set SEC Contact Email…) | Contact email SEC EDGAR requires in the request User-Agent for the universe download. Absent → SEC source skipped. Also settable via the `SEC_CONTACT_EMAIL` env var. |
| `user_config.json` | JSON | Settings → Advanced… (`config.save_user_config`) | User overrides for `OHLCV_HISTORY_YEARS`, `EARNINGS_HISTORY_YEARS`, `REFERENCE_TICKERS`, `PREFETCH_OHLCV_AT_LAUNCH`. Clamped on load; corrupt file degrades to defaults. Gitignored with the rest of `scanner_data/` |
| `presets/{name}.json` | JSON | User | Saved indicator + scan-window configs |
| `scan_history.json` | JSON | `scan_history.py` (written on every scan completion) | F2 watchlist diffing: latest per-(preset-or-adhoc, period) ticker set + bounded rolling run summary (200 entries). Atomic writes; corrupt file degrades to "no prior run" |
| `schedules.json` | JSON | `gui/scheduler.py` (Scans → Schedule… dialog + per-fire marker updates) | F3 scan scheduler entries ({label, preset, time, days, enabled} + last-fired date). Atomic writes; corrupt file degrades to "no schedules" |
| `exports/scan_*.xlsx` | XLSX | Quick Export (Scans → Quick Export, or auto after a scheduled scan) | Timestamped no-dialog result snapshots — all periods, current visible columns. Dir created lazily |
| `logs/*.log` | text | LogPanel | Per-session diagnostic logs (`scan_*`, `ohlcv_*`, `universe_*`, `bridge_*`) |
| `failed_tickers.log` | text | `ticker_universe` | Tickers dropped during yfinance universe validation |
| `ftp_raw/` | text | NASDAQ FTP (when SAVE_FTP_RAW=True) | Raw downloads for debugging |

QSettings (registry-backed under `HKCU\Software\trade_scanner_fh\Trade_Scanner_FH\`) holds:
- Cookie-browser monitor preference
- Window geometry (Qt default behavior)
- Hotkey sender config (`hotkey/` group), color-match tolerance
  (`match_color/tolerance_days`), menu toggles (backoff, Nasdaq
  auto-refresh), and assorted view/session checkbox states

The four JSON state files (`user_config.json`, `scan_history.json`,
`schedules.json`, presets) deliberately live in `scanner_data/` rather
than QSettings so they survive a registry wipe and travel with the
data directory.

---

## Testing

Test suite at `trade_scanner_fh/tests/` — **1,238 tests, all passing** as of 2026-06. (The once-flaky calendar-drift fixture in `test_yahoo_fill.py` was made relative-to-today on 2026-06-07; there are no known failures.) Run all:

```bash
cd c:/python/EDA_Project/Trade_Scanner_FH
c:/python/envs/eda-pipeline/python.exe -m pytest trade_scanner_fh/tests -q
```

**Coverage**: `pytest-cov` is wired in — line coverage was **78%** at the
last measure (2026-06):

```bash
c:/python/envs/eda-pipeline/python.exe -m pytest trade_scanner_fh/tests -q \
    --cov=trade_scanner_fh --cov-report=term
```

**Canonical fixtures** live in
[`tests/conftest.py`](trade_scanner_fh/tests/conftest.py) (centralized
2026-06 from per-file copies): `_qapp` (module-scoped QApplication — the
suite doesn't use pytest-qt), `tmp_parquets`, and `fake_scan_cache`.
The latter two redirect `config.DATA_DIR` **plus every import-time
DATA_DIR-derived path** — both earnings parquets, the finviz/finnhub
bulk checkpoints, and the `earnings_raw/` root — via
`_redirect_data_dir_derived_paths`. That redirect is a known trap:
monkeypatching `DATA_DIR` alone leaves the module-level `Path` constants
pointing at the user's REAL `scanner_data/`, and a fill under test will
silently write into it. Per-file fixtures remain only where they differ
meaningfully (e.g. per-source `tmp_world` trees that neutralize a
client's rate limiter).

### Test file layout

| File | Coverage |
|------|----------|
| `test_scanner.py` | Funnel pipeline end-to-end |
| `test_indicators.py` | Each pure indicator function |
| `test_data_engine.py` | OHLCV cache + parquet I/O |
| `test_earnings_history.py` | History parquet, `compute_consecutive_beats` (incl. divergent-cadence regression), per-slot priority dedup (finviz > zacks > finnhub), backfill-estimates, integrity checks (incl. cross_source_slot_overlap) |
| `test_finviz_client.py` | earningsData extraction (raw_decode, brackets-in-strings), failure kinds (empty/blocked/429/parse) |
| `test_finviz_fill.py` | Adjusted-field mapping, forward-row skip, history-years cap, report_time bucketing, fetch classification, spot-fill write path |
| `test_earnings_cache.py` | earnings_dates.parquet schema + IO |
| `test_earnings_reconcile.py` | Multi-source priority chain (nasdaq → yahoo → finviz → zacks → finnhub), aug-label generation, stale-date filtering, finnhub-as-last-resort |
| `test_earnings_aligned_dates.py` | Date-alignment + match-color anchoring |
| `test_earnings_raw.py` | Raw audit/replay layer per source |
| `test_zacks_scraper.py` | curl_cffi parser, fetch, session |
| `test_finnhub_client.py` | Finnhub REST primitives, rate limiter, key storage |
| `test_finnhub_fill.py` | Bulk/gap/spot fills, period_ending day-1 normalization, fiscal-year multi-record dedup, canonicalization handling |
| `test_nasdaq_fill.py` | finance-calendars bulk fill |
| `test_nasdaq_weekly_auto.py` | Daily calendar-day auto-trigger (`_is_nasdaq_refresh_due` fresh/same-day/prior-day/stale/corrupt-ISO), toggle persistence, `_maybe_run_nasdaq_refresh` gating, and same-launch smart-refresh chaining (`_pending_smart_refresh` armed/disabled/not-due paths) |
| `test_yahoo_fill.py` | yfinance gap + spot fills |
| `test_imperva_pause.py` | Auto-pause heuristic on Zacks block streak |
| `test_browser_cookie_autorefresh.py` | Firefox launcher, cookie capture, mid-flight capture |
| `test_cookie_dialog_smoke.py` | Cookie-paste dialog |
| `test_zacks_failure_breakdown.py` | FAIL_* sentinel classification |
| `test_smart_refresh.py` | Candidate selection (gap / just-reported / long-stale) |
| `test_smart_refresh_workers.py` | Concurrent smart-refresh worker bringup (targeted mode) |
| `test_earnings_coordinator_spawns.py` | EarningsRefreshCoordinator delegate parity + worker spawn wiring |
| `test_blacklist_manager.py` | `gui/blacklists.py` load/save/normalize plumbing |
| `test_parse_spike.py` | Parse-failure spike alarm (threshold math, checkpoint preservation, no-blacklist guarantee) |
| `test_disagreements.py` | Cross-source EPS disagreement report (detection tolerances, CSV rewrite, report-only guarantee) |
| `test_rvol_atr_stop.py` | RVOL indicator + funnel stage + panel row; ATR Stop derived column |
| `test_adr_dollar_stops.py` | ADR% ratio-form formula + lookback-20 default; $ADR indicator/filter/panel row; ADR Stop column; configurable stop multipliers; preset back-compat |
| `test_watchlist_diff.py` | scan_history persistence, Chg column stamping, baseline-poisoning guards, 90-day prune |
| `test_scheduler.py` | Schedule persistence, due-entry math, once-per-day firing, toast/quick-export chain |
| `test_prefetch_wiring.py` | Launch-time OHLCV prefetch gating + stop + no-contention ordering |
| `test_user_config.py` | user_config.json load/save/clamp/corrupt-file handling |
| `test_phase1_quickwins.py` | Observability wave: `_log_error`/`_start_worker` helpers, window-title version, silent-except logging |
| `test_version_info.py` | version_info.txt ↔ `trade_scanner_fh.__version__` sync |
| `test_end_date_anchor.py` | Quick-range End anchoring + stale-End warning |
| `test_most_recent_proxy.py` | Proxy-row exclusion from most-recent-quarter pick (keep-if-orphan) |
| `test_etf_adr_auto_skip.py` | ETF/ADR auto-skip parity across fills |
| `test_ohlcv_gate.py` | OHLCV freshness gate |
| `test_market_close.py` | `last_market_close` / trading-day helpers |
| `test_hotkey_gui.py` | HOTKEY GUI wiring (button, eventFilter cue) |
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
- Scanner integration tests use the canonical `fake_scan_cache` / `tmp_parquets` conftest fixtures, which redirect `config.DATA_DIR` AND every import-time DATA_DIR-derived path (parquets, checkpoints, raw dir) to a tmp path — never write to the real `scanner_data/` from a test.
- GUI widget tests use the `_qapp` module-scoped QApplication fixture (also in conftest).
- The `tests/_*.py` files (underscore prefix) are diagnostic scripts NOT run by pytest collection — use them for manual smoke tests against live endpoints.

---

## Build & deploy

### Dev environment

```bash
cd c:/python/EDA_Project/Trade_Scanner_FH
# Shared interpreter at C:\python\envs\eda-pipeline (Python 3.11+) is the
# expected dev/run environment. Required packages:
pip install numpy pandas yfinance pyarrow pyautogui PyQt6 \
            pyinstaller requests finance-calendars beautifulsoup4 \
            financedatabase keyring lxml openpyxl curl_cffi psutil \
            pywin32 finnhub-python
# For the test suite + coverage:
pip install pytest pytest-cov
```

(scipy is NOT a dependency — the app has zero scipy imports, and the
spec explicitly excludes it from the frozen build. The shared env carries
it for sibling projects only. PyInstaller in this env: **6.15.0**.)

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
ls -lh dist/Trade_Scanner_FH.exe          # fresh timestamp (~184 MB —
                                          #  the scipy exclusion shaves
                                          #  ~18 MB vs the ~201 MB
                                          #  pre-2026-06 builds; measured
                                          #  183,753,263 B on 2026-06-17, v5.2.0)
ls dist/scanner_data/                     # all data files preserved
```

### Spec file (`Trade_Scanner_FH.spec`)

PyInstaller spec (built with PyInstaller 6.15.0) — pulls C-library DLLs from `BASE_PREFIX/Library/bin` so it works in venv or conda. Adds explicit hidden imports for lazy modules (yfinance, lxml, keyring, openpyxl, curl_cffi, psutil, win32api, finnhub).

When adding a new data source that uses lazy imports, update the `hiddenimports` list in the spec.

**Qt binding exclusion.** The spec's `excludes` list drops `matplotlib`, `tkinter`, `test`, `unittest`, **`PySide6` / `shiboken6`, and `scipy`**. The shared build environment also carries PySide6 (sibling projects use it), and PyInstaller aborts the build the moment it detects two Qt bindings packages. This app is PyQt6-only, so PySide6 / shiboken6 are excluded explicitly — without that, the build fails at the `hook-PySide6` stage. `scipy` (added to the excludes 2026-06) is safe to drop because the app has zero scipy imports and pandas/yfinance only lazy-import it on paths never hit here (yfinance `repair=True` is never passed) — worth ~18 MB of exe.

### Version stamping

The single source of truth for the app version is
**`trade_scanner_fh.__version__`** (`trade_scanner_fh/__init__.py`).
Two consumers mirror it:

- The window title (`MainWindow` sets `Trading Scanner v{__version__}`),
  so screenshots and bug reports self-identify the build.
- The Windows VERSIONINFO resource: the spec's `EXE(..., version='version_info.txt')`
  embeds `version_info.txt` (repo root) into the exe — FileVersion /
  ProductVersion strings and the numeric `filevers`/`prodvers` tuples all
  derive from `__version__`. The resource intentionally carries **no
  personal fields** (no author name/email).

`version_info.txt` is hand-maintained; a version bump must touch both
files. [`tests/test_version_info.py`](trade_scanner_fh/tests/test_version_info.py)
pins the sync — bump `__version__` without updating `version_info.txt`
and the suite goes red.

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
24. **NaN handling for earnings filters is driven by TWO independent global flags — `ScanParams.earnings_dates_only` (gates `days_since/until` filters) and `ScanParams.earnings_data_only` (gates the 6 most-recent-quarter earnings filters + consec beats).** Per-row `*_include_no_data` flags were removed (9 of them). Both `_build_filter_stages` and `_compute_display_only_fails` consult these flags so funnel filtering and red-on-fail coloring stay consistent. Mask form: `(v >= t) | (v.isna() & nan_passes)` — NaN-safe in both branches.
25. **Dates ⊇ Data invariant.** A row passing the earnings-data filter MUST also pass the earnings-dates filter (data implies date). Enforced at both layers: the funnel masks for the dates filters explicitly OR in `_data_present_mask(df)` so a row with NaN date columns but populated data columns still passes; the view filter's dates check OR's in the data coverage mask. Tests: `test_earnings_dates_filter_respects_data_implies_date_invariant`, `test_view_filter_dates_supersets_data`.
26. **View-only filters (Earnings Dates / Earnings Data / Color Match Only)** affect display + export but NEVER the underlying scan results. `MainWindow._period_results` always holds the unfiltered scan output; `_apply_view_filters(df)` produces a fresh filtered copy on every render and on every export. Toggling a view filter off restores all rows without re-scanning.
27. **View-only "Earnings Data" coverage signal** = at least one non-NaN value across the 6 most-recent-quarter columns (`reported_eps`, `surprise_eps_*`, `reported_rev`, `surprise_rev_*`) OR any of the q-beats data columns (`q{1..20}_reported_eps`, `q{1..20}_surprise_*`, etc.). The q-beats inclusion lets a ticker pass when it has multi-quarter beats data even if its most-recent-quarter cells are NaN. Only columns actually present in the rendered df are checked.
28. **View-only "Earnings Dates" coverage signal** = at least one non-NaN value across calendar columns (`last_report_date`, `next_earnings_date`, `days_since_er`, `days_until_er`) OR any q-beats date column (`q{1..20}_report_date_eps`, `q{1..20}_report_date_rev`) OR any earnings DATA column (data ⇒ date). Always passes a strict superset of what the data view filter passes.
29. **View-only "Color Match Only" coverage signal** = `_earnings_aligned_dates` is a non-empty list. Empty list, NaN, or missing column all evaluate to "no match" → row dropped.

### Earnings source policy (post-EDGAR gap-fill rewrite)

32. **Per-`(ticker, period_ending)` priority dedup** runs at WRITE time inside `save_earnings_history` (gated on `dedup=True` which defaults to the value of `sort`). Canonical (dedup=True) saves leave at most one row per slot on disk; per-flush mid-fill writes (`sort=False, dedup=False`) can transiently leave multi-source slots, which is why read-side consumers (`get_ticker_history`, the scanner lookup) re-dedup. Priority chain: `finviz > zacks > finnhub`. Within the same source, the most-recently-updated row wins.
33. **Gap-fill across periods is preserved.** A single ticker can carry rows from multiple sources covering different fiscal-quarter slots — Finviz for most quarters + Zacks for a quarter Finviz hasn't pulled + Finnhub for a quarter neither covers. Only same-slot multi-source rows trigger the per-slot dedup.
34. *(Historical — EDGAR removed 2026-05-31; kept for the forking reference in §14.)* **EDGAR rows always carried NaN for `estimated_*` and `surprise_*`** — SEC XBRL has no analyst-estimate data, so EDGAR-only quarters surfaced only the YoY signal post-`compute_yoy_columns`.
35. *(Historical — EDGAR removed 2026-05-31.)* **EDGAR's `report_date` was the filing date** (always past), so EDGAR rows could never win the reconciler's next_earnings slot. Their `report_date_proxy` was always False (filings are real, not proxies).
36. **One-time `migrate_to_gap_fill_dedup` on first launch** re-dedups the on-disk parquet under the new per-slot priority. Sentinel at `scanner_data/.gap_fill_dedup_v1.done` prevents re-running. Dropped rows are preserved in `earnings_raw/`.
37. **Reconciler chain for earnings_dates is `nasdaq > yahoo > finviz > zacks > finnhub`.** Live calendar feeds outrank history-derived sources; among the history sources finviz leads (real announcement dates + times) and finnhub stays last.
38. *(Historical — EDGAR removed 2026-05-31; the principle lives on in every per-source skip list.)* **Auto-blacklisting is gated on definitive permanent misses only.** Transient failures (`rate_limited` / `forbidden` / `server_error` / `network` / `parse_error`) are NEVER auto-blacklisted — a rate-limit storm must not silently orphan thousands of good tickers.
39. *(Historical — EDGAR removed 2026-05-31; the same two-stage policy now lives in `fill_framework.run_fill_loop` for finviz/finnhub.)* **Block-recovery rewinds once, then skips.** First block in a failure window rewinds to retry the cluster under fresh state. Second+ block in the same window advances past the cluster instead. Skipped tickers stay un-completed in the checkpoint so a future `gap_fill` picks them up.
40. *(Historical — EDGAR removed 2026-05-31; the same rule applies to `.finviz_bulk_checkpoint.json` / `.finnhub_bulk_checkpoint.json` via `fill_framework`.)* **Checkpoint preserved on partial halts.** Only natural completion clears a bulk checkpoint. Block-triggered halts, user stops, parse-spike halts, and "stop" callback returns all preserve it so the next launch resumes — fixed after a prior buggy halt cleared the checkpoint and orphaned ~4200 already-processed tickers.
41. **ETF/ADR auto-skip applies to every earnings bulk + gap fill.** Computed dynamically from `universe.csv` flags via `_etf_adr_auto_skip_set()`; layered into the three combined-skip helpers. NOT persisted to per-source `*_blacklist.txt` files (universe refreshes flow through immediately). Spot fill uses a user-only skip check so the user can manually test an ETF / ADR if needed.

### Build / deploy

42. **`scanner_data/` survives rebuilds.** Verified by checking ohlcv parquet count + presets dir + cookies file before and after.
43. **`__pycache__/` MUST be cleared before any rebuild.** Stale bytecode can mask source edits — burned us once with the multi-column-drag and Display-Only regressions.
44. **SEC contact email never lives in source.** `SEC_CONTACT_DEFAULT` in `config.py` is a non-functional placeholder; the SEC universe source stays dormant until a real contact email is supplied via the gitignored `scanner_data/sec_contact.txt` (Settings → Set SEC Contact Email…) or the `SEC_CONTACT_EMAIL` env var. The dev copy and the frozen exe each need their own `sec_contact.txt` (under `trade_scanner_fh/scanner_data/` and `dist/scanner_data/` respectively). Rebuilds land in `dist/Trade_Scanner_FH.exe`; the legacy public-distribution build at `dist2/Trade_Scanner_FH_public.exe` predates this scheme and must never be touched by a rebuild from this tree.

### 2026-06 additions

45. **`version_info.txt` mirrors `trade_scanner_fh.__version__`.** A version bump must update both; `tests/test_version_info.py` pins the sync (string fields AND numeric tuples). The VERSIONINFO resource carries no personal fields.
46. **`user_config.json` can never crash the app.** `config.load_user_config()` clamps int overrides to their declared ranges, validates types, and degrades to compiled defaults on a corrupt / non-object / unreadable file. `_USER_CONFIG_DEFAULTS` snapshots the compiled values BEFORE overrides so a deleted file always has a safe fallback.
47. **Watchlist-diff baselines are never poisoned.** A stopped/crashed scan must not overwrite the per-(preset, period) baseline in `scan_history.json`; one-off period labels are skipped; a diff/persist failure never blocks scan completion; corrupt `scan_history.json` / `schedules.json` degrade to empty state, not a crash.
48. **The parse-failure spike alarm halts loudly, preserves the checkpoint, and never blacklists.** ≥`PARSE_SPIKE_FAIL_PCT`% parse failures over ≥`PARSE_SPIKE_MIN_SAMPLE` attempts means the page format drifted — that's upstream's fault, not the tickers'. `FAIL_PARSE_ERROR` is reserved for genuinely unparseable pages; a readable-but-empty payload stays a coverage miss (FAIL_NOT_FOUND / FAIL_EMPTY).
49. **The launch-time OHLCV prefetch never contends with the startup updater.** It starts only after the update finishes (or is skipped), runs at most once per launch, is stoppable, and a mid-flight stop is safe (per-symbol loads are independent reads).
50. **Scheduled scans degrade gracefully unattended.** A schedule fire routes would-be modal dialogs to the log; it is skipped (and retried on a later tick) when a scan is already running, consumed for the day when the preset no longer exists, and each entry fires at most once per day.

---

## Disclaimer

**This software is for informational and educational purposes only and does not constitute financial advice, investment advice, trading advice, or any other kind of advice.** You should not treat any of the software's output as a recommendation to buy, sell, or hold any security or financial instrument.

The developer(s) of this software:

- Make no representations or warranties regarding the accuracy, completeness, or reliability of any data, calculations, or scan results produced by this application.
- Are not responsible for any financial losses, damages, or other consequences arising from the use of this software or reliance on its output.
- Do not guarantee that the data sourced from third-party providers (including but not limited to Yahoo Finance, NASDAQ, SEC EDGAR, Zacks, Finviz, Finnhub, or any future provider) is accurate, timely, or complete.
- Accept no liability whatsoever for any order, trade, or other action accidentally or unintentionally triggered by the automated input features (HOTKEY mode / Send to Watchlist) interacting with your trading or order-entry software.

**Automated-input / hotkey safety — test in a simulator first.** HOTKEY mode and Send to Watchlist send simulated mouse clicks and keystrokes into whatever external application is focused, including order-entry platforms. Those keystrokes can be intercepted by your trading software's own hotkeys and may trigger unintended orders. **You must test all autohotkey functionality with YOUR specific order-entry software in simulated / paper-trading mode before using it for live trading.** Verify the full click-and-type sequence behaves as expected against your exact platform and configuration; you assume all risk for any orders it produces.

**Use at your own risk.** All investment decisions should be made based on your own research and judgment, ideally in consultation with a qualified financial professional. Past performance of any security identified by this scanner is not indicative of future results.

The Zacks scraper is built on a derivation of [TinyEarn](https://github.com/hussien-hussien/TinyEarn) (© 2018–2020 Hussien Hussien, MIT licensed) and uses TLS impersonation to bypass Imperva's bot-detection layer. Users are responsible for ensuring their use of this scraper complies with Zacks' terms of service.
