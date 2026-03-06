# Quick Start Guide

## First Launch

1. **Run `TradingScanner.exe`** — A console window and the GUI will open.

2. **Wait for the universe download** — On first run, the app downloads the full US ticker universe (~15,000 symbols) from three sources. This takes 1–2 minutes. Progress appears in the log panel and status bar.

3. **Wait for OHLCV data** — After the universe loads, the app begins downloading 5 years of daily price data for every ticker. This is a large initial download (several hours depending on connection speed). The status bar on the bottom-right shows progress (e.g. "OHLCV update: 3200/15419 (21%)").

4. **You can scan immediately** — You don't need to wait for the full download. Any tickers that already have cached data are scannable right away.

> **Tip**: If you get rate-limited by Yahoo Finance, use a VPN and try **Data > Reset yfinance Session** to clear cookies.

## Running a Scan

1. **Set your date range** using the Start/End pickers or quick-range buttons (1D, 1W, 1M, 3M, 6M).

2. **Enable indicators** by checking the boxes on the left panel. Adjust parameters as needed:
   - Blue checkbox = enabled and will be used in the scan
   - Unchecked = indicator is skipped

3. **Apply universe filters** on the toolbar:
   - *Include ETFs* — unchecked by default (most scans focus on individual stocks)
   - *Include ADRs* — checked by default
   - *IPO Mode* — when checked, only scans tickers with ≤ N days of price history

4. **Click "Run Scan"** — Results populate the table sorted by % Gain. The log panel shows the funnel (how many tickers passed each filter stage).

5. **Click column headers** to re-sort results by any metric.

## Sending to TradeStation

1. Run a scan and get results.
2. Click **"Send to TradeStation"** — A dialog opens with transfer settings.
3. Configure:
   - *Max Tickers*: How many to send (top N by current sort)
   - *Delay between entries*: Milliseconds between keystrokes (increase if TradeStation is slow)
   - *Target window*: Where to type (Symbol Entry, Chart, or RadarScreen)
4. Click **Start Transfer**, then quickly click into the TradeStation input field. The app will type each symbol automatically.

## Saving Presets

1. Configure your indicators the way you want.
2. Type a name in the Preset dropdown and click **Save**.
3. To reload: select the preset from the dropdown and click **Load**.
4. Presets are saved as JSON files in `scanner_data/presets/`.

## Managing Data (Data Menu)

| What you want to do | Menu action |
|---|---|
| Pick up new IPOs / ticker changes | **Force Universe Refresh** |
| Update all stale price data | **Force OHLCV Refresh** |
| Only download tickers with no data | **Download Missing Tickers Only** |
| Stop a long-running download | **Stop OHLCV Refresh** |
| Unblock after rate limiting | **Reset yfinance Session** + VPN |
| Skip problematic tickers forever | **Ticker Blacklist...** |
| Tune retry behavior | **Backoff Settings...** |

## Automatic Updates

Every time you launch the app:
- The ticker universe is refreshed if it's more than 7 days old
- All OHLCV data is checked for staleness and updated in the background
- The app is market-hours-aware: it only expects today's data after 5 PM Eastern

## Troubleshooting

| Problem | Solution |
|---|---|
| "No cached OHLCV data yet" | Wait for the background download, or check your internet |
| Many consecutive download errors | Yahoo rate limiting — enable VPN, use **Reset yfinance Session** |
| App seems frozen during download | Check the console window and status bar — the download runs in the background |
| Blacklist editor crashes | Fixed — the app now handles Unicode characters. Paste freely. |
| TradeStation transfer too fast | Increase the delay in the transfer dialog (try 200–500ms) |
| DPI warning in console | Cosmetic only — safe to ignore |
