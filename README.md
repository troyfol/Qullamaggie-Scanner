# Equity Momentum Scanner

A standalone desktop application for scanning the entire US equity universe against configurable technical indicators. Built with PyQt6 and packaged as a single-file Windows executable.

## Features

- **Full US Equity Universe** — Automatically downloads and maintains 15,000+ tickers from NASDAQ FTP, GitHub, and SEC EDGAR sources
- **14 Configurable Indicators** across 4 categories:
  - *Trend Filters*: SMA crossover (x2), Stockbee Trend Intensity, Distance from High
  - *Momentum / Prior Move*: % Gain, Top Percentile, Consecutive Gap-Ups, Current Gap, ADR%
  - *Volatility Contraction*: Bollinger Band Width, ATR Ratio (Short/Long)
  - *Volume / Liquidity*: Volume Dry-Up Ratio, Minimum Price, Average Volume, Dollar Volume
- **Incremental OHLCV Updates** — Parquet-based cache with daily incremental downloads; market-hours-aware staleness detection
- **Universe Filters** — Include/exclude ETFs, ADRs; IPO Mode to scan only recent listings
- **TradeStation Bridge** — Send scan results directly to TradeStation via automated symbol entry
- **Preset System** — Save, load, and delete named indicator presets
- **Rate-Limit Handling** — Configurable backoff with consecutive-failure detection, adjustable thresholds, and session reset
- **Ticker Blacklist** — Persistent list of tickers to skip during refresh (editable via GUI)
- **Dark Theme** — Full dark UI with styled menus, tables, and controls

## Architecture

```
trading_scanner/
├── config.py            # Paths, constants, tunable defaults
├── data_engine.py       # OHLCV download, parquet cache, validation
├── ticker_universe.py   # Universe download from 3 sources, CSV cache
├── indicators.py        # All 14 indicator calculations (pure functions)
├── scanner.py           # ScanParams dataclass, funnel pipeline, run_scan()
├── tradestation.py      # TradeStationBridge, BridgeConfig, pyautogui automation
├── gui.py               # PyQt6 GUI, background workers, Data menu
├── __main__.py          # Package entry point (python -m trading_scanner)
└── __init__.py
```

**Data flow**: Universe CSV → OHLCV Parquets → Indicator calculations → Funnel filter → Results table → (optional) TradeStation

## Data Menu

| Action | Description |
|--------|-------------|
| Force Universe Refresh | Re-download ticker list from all 3 sources. Does not delete price data. |
| Force OHLCV Refresh | Re-check all tickers for stale/missing data and download updates. |
| Download Missing Tickers Only | Only download tickers with no cached data at all. |
| Stop OHLCV Refresh | Immediately stop a running background update. Progress is kept. |
| Reset yfinance Session | Clear cookie/crumb caches to get a fresh HTTP session. |
| Enable Rate-Limit Backoff | Toggle automatic pause-and-retry on consecutive failures. |
| Backoff Settings... | Configure fail threshold, wait time, and max retries. |
| Ticker Blacklist... | Edit comma-separated list of tickers to always skip. |

## Data Storage

All data is stored in `scanner_data/` next to the executable:

```
scanner_data/
├── ohlcv/           # One .parquet file per ticker (e.g. AAPL.parquet)
├── logs/            # Application logs
├── ftp_raw/         # Raw FTP downloads from NASDAQ
├── presets/         # Saved indicator presets (.json)
├── universe.csv     # Cached ticker universe with metadata
└── blacklist.txt    # Comma-separated blacklisted tickers
```

## Building from Source

Requires a conda environment with Python 3.11+ and the following packages:

```
numpy pandas scipy yfinance pyarrow pyautogui PyQt6 pyinstaller
```

Build command:

```bash
cd <project_root>
python -m PyInstaller TradingScanner.spec --distpath trading_scanner --workpath build_ts --clean -y
```

The spec file handles conda DLL bundling (ffi, expat, SSL, Qt6, ICU) automatically.

## Disclaimer

**This software is for informational and educational purposes only and does not constitute financial advice, investment advice, trading advice, or any other kind of advice.** You should not treat any of the software's output as a recommendation to buy, sell, or hold any security or financial instrument.

The developer(s) of this software:

- Make no representations or warranties regarding the accuracy, completeness, or reliability of any data, calculations, or scan results produced by this application
- Are not responsible for any financial losses, damages, or other consequences arising from the use of this software or reliance on its output
- Do not guarantee that the data sourced from third-party providers (including but not limited to Yahoo Finance, NASDAQ, and SEC EDGAR) is accurate, timely, or complete

**Use at your own risk.** All investment decisions should be made based on your own research and judgment, and ideally in consultation with a qualified financial professional. Past performance of any security identified by this scanner is not indicative of future results.
