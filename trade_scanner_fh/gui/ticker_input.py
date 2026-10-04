"""
Ticker-list parsing for every dialog that takes typed or pasted tickers
(v8.0.2).

Before this, each dialog split its text its own way: some on commas only,
some on commas and newlines, Lookup on commas, newlines, spaces and
semicolons. Two consequences:

  * A TradeStation column (one symbol per line) pasted into the OHLCV
    Blacklist or Greylist editor became ONE entry with newlines inside it —
    nothing was actually blacklisted, and the newline broke the one-line
    shape of blacklist.txt / greylist.txt.
  * A finviz screener list carries qualifiers in parentheses — ``AGL(HB)``
    is AGL, hard to borrow — and every dialog kept the tag as part of the
    symbol.

Every list dialog now goes through `parse_ticker_list`; the single-ticker
Spot Fill prompts use `strip_qualifiers`. Symbols themselves are left
alone apart from case and dashes: live universe symbols carry ``$`` and
``^`` inside them (``ABR$D``, ``AIIA^``), so only a LEADING ``$`` (the
``$AAPL`` cashtag form) is dropped. On 2026-10-02 no universe, cache or
skip-list symbol contained a parenthesis or whitespace, so re-saving an
existing list through this parser cannot change it.

No Qt imports — the module stays importable headless.
"""

from __future__ import annotations

import re

from .blacklists import normalize_ticker

# A parenthesised qualifier anywhere in the text: finviz's (HB) and the like.
_QUALIFIER = re.compile(r"\([^()]*\)")
# Commas, semicolons and any whitespace — a TradeStation column is one
# symbol per line, typed lists use commas, pasted ones use all of them.
_SPLIT = re.compile(r"[,;\s]+")

# Dialog wording, shared so every ticker box describes the same rules.
# INPUT_PROMPT heads the Manual Input STW and Lookup boxes; FORMAT_NOTE goes
# inside the editors' existing explanations.
INPUT_PROMPT = ("Enter tickers — comma-separated or one per line (a "
                "TradeStation column pastes as-is). Finviz tags such as (HB) "
                "are ignored.")
FORMAT_NOTE = ("comma-separated or one per line; finviz tags such as (HB) "
               "are ignored")


def strip_qualifiers(text: str) -> str:
    """``text`` with every ``(...)`` group removed. A group becomes a space,
    so ``AGL(HB)APPS`` still separates into two symbols."""
    return _QUALIFIER.sub(" ", text or "")


def parse_ticker_list(text: str) -> list:
    """Tickers from free text, in order, without duplicates.

    Accepts commas, semicolons, newlines (a TradeStation column), tabs and
    spaces as separators, drops parenthesised qualifiers such as finviz's
    ``(HB)``, upper-cases, folds Unicode dashes to ``-`` and drops a
    leading ``$``. A stray unmatched parenthesis ends the symbol, so a
    truncated ``AGL(HB`` still yields ``AGL``."""
    out, seen = [], set()
    for tok in _SPLIT.split(strip_qualifiers(text)):
        t = normalize_ticker(tok.split("(", 1)[0].replace(")", "")).lstrip("$")
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out
