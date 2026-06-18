"""trade_scanner_fh — Zacks earnings fork of trading_scanner.

This fork extends the parent project with a Playwright-based Zacks
scraper for full per-quarter earnings history (EPS / revenue + surprises),
new filterable scan inputs (Reported EPS, Surprise EPS/Rev $/%, Consecutive
Beats), and a wide-format multi-quarter results display when beats filters
are active. Yahoo remains available as a dates-only fallback for tickers
that Zacks doesn't cover.

Forked from trading_scanner. See TINYEARNINGS_FORK.md for the spec.
"""
__version__ = "5.3.0"
__title__ = "trade_scanner_fh"
