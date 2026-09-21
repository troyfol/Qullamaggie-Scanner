"""Finviz quote-page snapshot table — parsing and field definitions.

The snapshot table is the ~84 label/value grid at the top of
``quote.ashx?t=SYM``. It carries valuation, ownership, margins, performance
and technicals, and it costs nothing extra to obtain: the SAME table is
embedded in the ``&ty=ea`` earnings page the earnings fill already downloads.

Three properties of the real page drive the design here, all verified against
live responses rather than assumed:

1. **Labels are not unique.** ``EPS next Y`` appears TWICE — once as an
   absolute estimate (``9.61``) and once as a growth rate (``8.76%``). A dict
   keyed on the label silently keeps one and loses the other, so parsing is
   POSITIONAL: the nth occurrence of a label maps to its own field.

2. **Seven cells pack two values.** ``Volatility`` is ``1.60% 2.23%`` (week,
   month); ``52W High`` is ``344.57 -2.45%`` (level, distance). Each is split
   into two separate fields.

3. **The field set varies by instrument.** An ETF returns a shorter table
   (SPY: 144 cells vs AAPL's 168), so nothing may assume a fixed count or a
   fixed ordering of the whole grid.
"""

from __future__ import annotations

import html as _html
import re
from typing import Optional

# Every cell of the snapshot grid carries this class. Label and value cells
# both use it, alternating label, value, label, value.
_CELL_RE = re.compile(r'<td[^>]*snapshot-td2[^>]*>(.*?)</td>', re.S)
_TAG_RE = re.compile(r"<[^>]+>")

# Finviz's own "we don't have this ticker" response. A 404 carrying this text
# is the ONLY signal that permits an automatic skip-list add — a 429, a 403 or
# a challenge page means we were blocked, which says nothing about coverage.
_NOT_FOUND_RE = re.compile(r'Ticker\s+"[^"]+"\s+not found', re.I)

# Magnitude suffixes finviz uses on large numbers.
_SUFFIX = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}


def is_ticker_not_found(status_code: int, html: str) -> bool:
    """True only for finviz's definitive "no such ticker" response.

    Requires BOTH the 404 and the body text. A bare 404 could be a routing
    blip and the text alone could appear in unrelated markup; together they
    are unambiguous, and this gate is what decides whether a ticker is
    permanently skip-listed. A 429, a 403 or a challenge page means we were
    BLOCKED, which says nothing about coverage and must never skip-list.

    Tags are stripped before matching: finviz wraps the symbol in markup
    (``Ticker <b>"XYZ"</b> not found.``), so a regex run against raw HTML
    matches nothing and every uncovered ticker would be retried forever.
    """
    if status_code != 404:
        return False
    text = _html.unescape(_TAG_RE.sub(" ", html or ""))
    return bool(_NOT_FOUND_RE.search(text))


def _clean(cell: str) -> str:
    """Strip tags and entities from one cell, collapsing whitespace."""
    return _html.unescape(" ".join(_TAG_RE.sub(" ", cell).split()))


def to_number(raw) -> Optional[float]:
    """Parse one finviz scalar into a float, or None when it carries no value.

    Handles the magnitude suffixes (``4905.54B``), thousands separators,
    percentages (returned as the number, so ``-2.27%`` -> ``-2.27``),
    parenthesised negatives, and every flavour of finviz's "no data" marker
    (``-``, ``- -``, empty, ``N/A``).
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s in {"-", "--", "- -", "N/A", "NA"}:
        return None
    s = s.replace(",", "").replace("%", "").strip()
    negative = s.startswith("(") and s.endswith(")")
    if negative:
        s = s[1:-1].strip()
    mult = 1.0
    if s and s[-1].upper() in _SUFFIX:
        mult = _SUFFIX[s[-1].upper()]
        s = s[:-1].strip()
    try:
        val = float(s)
    except (TypeError, ValueError):
        return None
    return -(val * mult) if negative else val * mult


# ----------------------------------------------------------------------
# Field map
# ----------------------------------------------------------------------
#
# (finviz label, occurrence index, field name(s), kind)
#
# `occurrence` disambiguates a repeated label — 0 is the first time the label
# appears on the page, 1 the second. Only `EPS next Y` currently needs it, but
# the mechanism is general because finviz has changed this grid before.
#
# `kind`:
#   "num"   one numeric field
#   "num2"  two numerics in one cell, split on whitespace
#   "paren" "VALUE (PCT%)" — an absolute plus its percentage
#   "text"  kept verbatim (Index membership, earnings timing, IPO date)
#   "yesno" "Yes / No" -> two booleans
_FIELDS: tuple = (
    ("Index",             0, ("index_membership",),               "text"),
    ("Market Cap",        0, ("market_cap",),                     "num"),
    ("Enterprise Value",  0, ("enterprise_value",),               "num"),
    ("Income",            0, ("income",),                         "num"),
    ("Sales",             0, ("sales",),                          "num"),
    ("Book/sh",           0, ("book_per_sh",),                    "num"),
    ("Cash/sh",           0, ("cash_per_sh",),                    "num"),
    ("Dividend Est.",     0, ("dividend_est", "dividend_est_pct"), "paren"),
    ("Dividend TTM",      0, ("dividend_ttm", "dividend_ttm_pct"), "paren"),
    ("Dividend Ex-Date",  0, ("dividend_ex_date",),               "text"),
    ("Dividend Gr. 3/5Y", 0, ("dividend_gr_3y", "dividend_gr_5y"), "num2"),
    ("Payout",            0, ("payout_pct",),                     "num"),
    ("Employees",         0, ("employees",),                      "num"),
    ("IPO",               0, ("ipo_date",),                       "text"),
    ("P/E",               0, ("pe",),                             "num"),
    ("Forward P/E",       0, ("forward_pe",),                     "num"),
    ("PEG",               0, ("peg",),                            "num"),
    ("P/S",               0, ("ps",),                             "num"),
    ("P/B",               0, ("pb",),                             "num"),
    ("P/C",               0, ("pc",),                             "num"),
    ("P/FCF",             0, ("pfcf",),                           "num"),
    ("EV/EBITDA",         0, ("ev_ebitda",),                      "num"),
    ("EV/Sales",          0, ("ev_sales",),                       "num"),
    ("Quick Ratio",       0, ("quick_ratio",),                    "num"),
    ("Current Ratio",     0, ("current_ratio",),                  "num"),
    ("Debt/Eq",           0, ("debt_eq",),                        "num"),
    ("LT Debt/Eq",        0, ("lt_debt_eq",),                     "num"),
    ("Option/Short",      0, ("optionable", "shortable"),         "yesno"),
    ("EPS (ttm)",         0, ("eps_ttm",),                        "num"),
    # First `EPS next Y` is the absolute estimate...
    ("EPS next Y",        0, ("eps_next_y",),                     "num"),
    ("EPS next Q",        0, ("eps_next_q",),                     "num"),
    ("EPS this Y",        0, ("eps_this_y_pct",),                 "num"),
    # ...the second is the growth rate. Same label, different meaning.
    ("EPS next Y",        1, ("eps_next_y_pct",),                 "num"),
    ("EPS next 5Y",       0, ("eps_next_5y_pct",),                "num"),
    ("EPS past 3/5Y",     0, ("eps_past_3y_pct", "eps_past_5y_pct"), "num2"),
    ("Sales past 3/5Y",   0, ("sales_past_3y_pct", "sales_past_5y_pct"), "num2"),
    ("EPS Y/Y TTM",       0, ("eps_yoy_ttm_pct",),                "num"),
    ("Sales Y/Y TTM",     0, ("sales_yoy_ttm_pct",),              "num"),
    ("EPS Q/Q",           0, ("eps_qoq_pct",),                    "num"),
    ("Sales Q/Q",         0, ("sales_qoq_pct",),                  "num"),
    ("Earnings",          0, ("earnings_timing",),                "text"),
    ("EPS/Sales Surpr.",  0, ("eps_surprise_pct", "sales_surprise_pct"), "num2"),
    ("Insider Own",       0, ("insider_own_pct",),                "num"),
    ("Insider Trans",     0, ("insider_trans_pct",),              "num"),
    ("Inst Own",          0, ("inst_own_pct",),                   "num"),
    ("Inst Trans",        0, ("inst_trans_pct",),                 "num"),
    ("ROA",               0, ("roa_pct",),                        "num"),
    ("ROE",               0, ("roe_pct",),                        "num"),
    ("ROIC",              0, ("roic_pct",),                       "num"),
    ("Gross Margin",      0, ("gross_margin_pct",),               "num"),
    ("Oper. Margin",      0, ("oper_margin_pct",),                "num"),
    ("Profit Margin",     0, ("profit_margin_pct",),              "num"),
    ("SMA20",             0, ("sma20_pct",),                      "num"),
    ("SMA50",             0, ("sma50_pct",),                      "num"),
    ("SMA200",            0, ("sma200_pct",),                     "num"),
    ("Shs Outstand",      0, ("shs_outstanding",),                "num"),
    ("Shs Float",         0, ("shs_float",),                      "num"),
    ("Short Float",       0, ("short_float_pct",),                "num"),
    ("Short Ratio",       0, ("short_ratio",),                    "num"),
    ("Short Interest",    0, ("short_interest",),                 "num"),
    ("52W High",          0, ("high_52w", "high_52w_pct"),        "num2"),
    ("52W Low",           0, ("low_52w", "low_52w_pct"),          "num2"),
    # The pair the Options header wants: week and month volatility.
    ("Volatility",        0, ("volatility_week_pct", "volatility_month_pct"), "num2"),
    ("ATR (14)",          0, ("finviz_atr14",),                   "num"),
    ("RSI (14)",          0, ("finviz_rsi14",),                   "num"),
    ("Beta",              0, ("finviz_beta",),                    "num"),
    ("Rel Volume",        0, ("finviz_rel_volume",),              "num"),
    ("Avg Volume",        0, ("finviz_avg_volume",),              "num"),
    ("Volume",            0, ("finviz_volume",),                  "num"),
    ("Price",             0, ("finviz_price",),                   "num"),
    ("Prev Close",        0, ("finviz_prev_close",),              "num"),
    # Label is literally "Change %", not "Change" — verified against the live
    # page. The shorter spelling silently produced no value at all.
    ("Change %",          0, ("finviz_change_pct",),              "num"),
    ("Recom",             0, ("recom",),                          "num"),
    ("Target Price",      0, ("target_price",),                   "num"),
    ("Perf Week",         0, ("perf_week_pct",),                  "num"),
    ("Perf Month",        0, ("perf_month_pct",),                 "num"),
    ("Perf Quarter",      0, ("perf_quarter_pct",),               "num"),
    ("Perf Half Y",       0, ("perf_half_y_pct",),                "num"),
    ("Perf YTD",          0, ("perf_ytd_pct",),                   "num"),
    ("Perf Year",         0, ("perf_year_pct",),                  "num"),
    ("Perf 3Y",           0, ("perf_3y_pct",),                    "num"),
    ("Perf 5Y",           0, ("perf_5y_pct",),                    "num"),
    ("Perf 10Y",          0, ("perf_10y_pct",),                   "num"),
)

# (label, occurrence) -> (field names, kind)
_FIELD_LOOKUP: dict = {
    (label, occ): (names, kind) for label, occ, names, kind in _FIELDS
}

# Every field this module can emit, in page order. The storage layer uses this
# to build a stable column set even when a given ticker omits some of them.
SNAPSHOT_FIELDS: tuple = tuple(
    name for _label, _occ, names, _kind in _FIELDS for name in names
)

# Fields kept as strings. Everything else is numeric.
TEXT_FIELDS: frozenset = frozenset(
    name
    for _label, _occ, names, kind in _FIELDS
    if kind == "text"
    for name in names
)
BOOL_FIELDS: frozenset = frozenset(
    name
    for _label, _occ, names, kind in _FIELDS
    if kind == "yesno"
    for name in names
)

# Numeric fields, in page order — what the Finviz Additional filter section
# offers and what the results table renders.
NUMERIC_FIELDS: tuple = tuple(
    f for f in SNAPSHOT_FIELDS if f not in TEXT_FIELDS and f not in BOOL_FIELDS
)


def _split_two(value: str) -> tuple:
    """Split a two-value cell. Returns (first, second), either may be None.

    Finviz writes these space-separated (``1.60% 2.23%``). A cell holding only
    one token yields (token, None) rather than raising — the ETF tables do
    this on some rows.
    """
    parts = [p for p in str(value).split() if p]
    if not parts:
        return None, None
    if len(parts) == 1:
        return parts[0], None
    return parts[0], parts[1]


_PAREN_RE = re.compile(r"^([^()]+?)\s*\(([^)]*)\)\s*$")


def parse_snapshot(html: str) -> dict:
    """Parse the snapshot grid out of a finviz quote page.

    Returns ``{field_name: value}`` holding only the fields this page actually
    carried — a shorter ETF table simply yields fewer keys rather than a dict
    padded with None. An unrecognised label is skipped silently: finviz adds
    rows from time to time, and an unknown row is not a parse failure.

    Returns ``{}`` when the grid is absent entirely (a block page, a 404, or a
    layout change), which the caller distinguishes from a real empty result.
    """
    if not html:
        return {}
    cells = [_clean(c) for c in _CELL_RE.findall(html)]
    if len(cells) < 2:
        return {}

    out: dict = {}
    seen: dict = {}
    # Cells alternate label, value. Walk in pairs; a trailing odd cell (seen
    # on malformed responses) is ignored rather than paired with nothing.
    for i in range(0, len(cells) - 1, 2):
        label, raw = cells[i], cells[i + 1]
        occ = seen.get(label, 0)
        seen[label] = occ + 1
        spec = _FIELD_LOOKUP.get((label, occ))
        if spec is None:
            continue
        names, kind = spec

        if kind == "text":
            out[names[0]] = raw or None
        elif kind == "num":
            out[names[0]] = to_number(raw)
        elif kind == "num2":
            a, b = _split_two(raw)
            out[names[0]] = to_number(a)
            out[names[1]] = to_number(b)
        elif kind == "paren":
            m = _PAREN_RE.match(raw or "")
            if m:
                out[names[0]] = to_number(m.group(1))
                out[names[1]] = to_number(m.group(2))
            else:
                out[names[0]] = to_number(raw)
                out[names[1]] = None
        elif kind == "yesno":
            a, b = (raw or "").split("/") if "/" in (raw or "") else (raw, "")
            out[names[0]] = a.strip().lower() == "yes" if a else None
            out[names[1]] = b.strip().lower() == "yes" if b else None
    return out


# ----------------------------------------------------------------------
# Store
# ----------------------------------------------------------------------
#
# One parquet, one row per symbol, plus a `fetched_at` UTC timestamp. Unlike
# the earnings store there is no history dimension: these are point-in-time
# characteristics and the user's explicit decision was that filtering on them
# means filtering on the MOST RECENT values across every scanned period. Not
# keeping history is therefore a spec, not a shortcut.

import datetime as _dt  # noqa: E402  (module is import-light above by design)
import logging  # noqa: E402

import pandas as pd  # noqa: E402

from . import config  # noqa: E402

log = logging.getLogger("scanner.finviz_snapshot")

SYMBOL_COL = "symbol"
FETCHED_COL = "fetched_at"


def load_store() -> pd.DataFrame:
    """The snapshot store, or an empty frame with the right columns.

    Never raises: a corrupt or half-written parquet returns empty and logs,
    because a snapshot store is a convenience layer and losing it must not
    stop a scan from running.
    """
    path = config.FINVIZ_SNAPSHOT_PARQUET
    cols = [SYMBOL_COL, FETCHED_COL, *SNAPSHOT_FIELDS]
    try:
        if not path.exists():
            return pd.DataFrame(columns=cols)
        df = pd.read_parquet(path)
    except Exception as exc:
        log.warning("finviz snapshot store unreadable (%s) — treating as "
                    "empty; the next sweep will rebuild it.", exc)
        return pd.DataFrame(columns=cols)
    if SYMBOL_COL not in df.columns:
        log.warning("finviz snapshot store has no %s column — ignoring.",
                    SYMBOL_COL)
        return pd.DataFrame(columns=cols)
    return df


def merge_rows(rows: "list[dict] | pd.DataFrame") -> int:
    """Merge snapshot rows into the store, newest wins. Returns rows written.

    MERGE, never replace. Two producers write here — the free scavenge off the
    earnings fill and the paced sweep — and they run independently, so a
    wholesale write from either would discard the other's work. Same
    merge-never-replace rule the OHLCV deep refresh learned the hard way.

    A symbol appearing twice in one call keeps its LAST occurrence, matching
    the newest-wins rule applied against the existing store.
    """
    if rows is None:
        return 0
    incoming = pd.DataFrame(rows) if not isinstance(rows, pd.DataFrame) else rows
    if incoming.empty or SYMBOL_COL not in incoming.columns:
        return 0

    incoming = incoming.copy()
    incoming[SYMBOL_COL] = incoming[SYMBOL_COL].astype(str).str.upper().str.strip()
    incoming = incoming[incoming[SYMBOL_COL] != ""]
    if incoming.empty:
        return 0
    if FETCHED_COL not in incoming.columns:
        incoming[FETCHED_COL] = pd.Timestamp.now(tz="UTC")
    incoming = incoming.drop_duplicates(subset=[SYMBOL_COL], keep="last")

    existing = load_store()
    if not existing.empty:
        existing = existing[~existing[SYMBOL_COL].isin(incoming[SYMBOL_COL])]
        combined = pd.concat([existing, incoming], ignore_index=True)
    else:
        combined = incoming.reset_index(drop=True)

    # Stable column order: identity, timestamp, then page order. A ticker that
    # omitted a field (every ETF omits most of them) gets NaN rather than the
    # column vanishing for everyone.
    ordered = [SYMBOL_COL, FETCHED_COL] + [
        c for c in SNAPSHOT_FIELDS if c in combined.columns
    ]
    extra = [c for c in combined.columns if c not in ordered]
    combined = combined[ordered + extra]

    try:
        config.FINVIZ_SNAPSHOT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
        config.atomic_write_parquet(combined, config.FINVIZ_SNAPSHOT_PARQUET,
                                    index=False)
    except Exception as exc:
        log.error("Could not write the finviz snapshot store: %s", exc)
        return 0
    return int(len(incoming))


def last_updated() -> "pd.Timestamp | None":
    """Newest `fetched_at` in the store, or None when it is empty.

    Drives the manual-run confirmation dialog, which quotes how long it has
    been since the last sweep before spending hours of requests.
    """
    df = load_store()
    if df.empty or FETCHED_COL not in df.columns:
        return None
    ts = pd.to_datetime(df[FETCHED_COL], errors="coerce", utc=True).dropna()
    return None if ts.empty else ts.max()


def stale_symbols(
    universe: "list[str]", *, stale_days: "int | None" = None,
    skip: "set[str] | None" = None,
) -> list:
    """Universe members whose snapshot is missing or older than `stale_days`.

    Ordered oldest-first (never-fetched first), so a sweep interrupted partway
    through has still refreshed the most out-of-date names rather than a
    random slice.
    """
    if stale_days is None:
        stale_days = config.FINVIZ_SNAPSHOT_STALE_DAYS
    skip = {s.upper() for s in (skip or set())}
    syms = [s.upper().strip() for s in universe if s and str(s).strip()]
    syms = [s for s in syms if s not in skip]

    df = load_store()
    if df.empty:
        return syms
    ts = pd.to_datetime(df.get(FETCHED_COL), errors="coerce", utc=True)
    seen = dict(zip(df[SYMBOL_COL].astype(str).str.upper(), ts))
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=int(stale_days))

    never, aged = [], []
    for s in syms:
        t = seen.get(s)
        if t is None or pd.isna(t):
            never.append(s)
        elif t < cutoff:
            aged.append((t, s))
    aged.sort()
    return never + [s for _t, s in aged]


def missing_symbols(
    universe: "list[str]", *, skip: "set[str] | None" = None,
) -> list:
    """Universe members with NO snapshot row at all - the gap-fill target.

    Kept separate from `stale_symbols` rather than bolted on as a flag: that
    function drives the weekly refresh, and a gap fill must never widen into
    re-fetching rows that merely aged. Any existing row counts as covered,
    however old. Universe order is kept and duplicates are dropped.
    """
    skip = {s.upper() for s in (skip or set())}
    df = load_store()
    have = (set(df[SYMBOL_COL].astype(str).str.upper())
            if not df.empty and SYMBOL_COL in df.columns else set())
    out, seen = [], set()
    for s in universe:
        if not s or not str(s).strip():
            continue
        s = str(s).upper().strip()
        if s in skip or s in have or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


# ----------------------------------------------------------------------
# Panel grouping
# ----------------------------------------------------------------------
#
# Drives BOTH the collapsible Finviz Additional sub-sections and the results
# columns, so the two can never drift out of step.
#
# `OPTIONS_FIELDS` are lifted out into their own top-level header rather than
# living under Finviz Additional — they are the goal-3 set the user screens on
# directly, and burying Beta three levels down would be unhelpful.

OPTIONS_FIELDS: tuple = (
    ("finviz_beta",          "Beta (finviz)"),
    ("volatility_week_pct",  "Volatility W %"),
    ("volatility_month_pct", "Volatility M %"),
    ("optionable",           "Optionable"),
    ("shortable",            "Shortable"),
)

# Deliberately NOT offered as filters.
#
# The first four are LATEST-DAY values. On a weekly sweep they are up to seven
# days stale, and on a backdated scan they describe a date with no relation to
# the scan window — a "Price" filter that silently means "price last Tuesday"
# while every other filter honours the period is a trap, not a feature.
#
# The last three duplicate indicators this scanner already computes PER PERIOD
# from its own OHLCV. Two similarly-named columns that disagree is a support
# burden with no upside.
#
# All seven are still parsed, stored and rendered as columns — only the filter
# row is withheld. `finviz_rsi14` is deliberately NOT here: nothing else in
# the app computes RSI, so it is additive rather than conflicting.
NON_FILTERABLE_NUMERIC: frozenset = frozenset({
    "finviz_price", "finviz_prev_close", "finviz_volume", "finviz_change_pct",
    "finviz_atr14", "finviz_rel_volume", "finviz_avg_volume",
})

FINVIZ_GROUPS: tuple = (
    ("Valuation", (
        ("market_cap", "Market Cap"), ("enterprise_value", "Enterprise Value"),
        ("pe", "P/E"), ("forward_pe", "Forward P/E"), ("peg", "PEG"),
        ("ps", "P/S"), ("pb", "P/B"), ("pc", "P/C"), ("pfcf", "P/FCF"),
        ("ev_ebitda", "EV/EBITDA"), ("ev_sales", "EV/Sales"),
        ("target_price", "Target Price"), ("recom", "Recom"),
    )),
    ("Financials", (
        ("income", "Income"), ("sales", "Sales"),
        ("book_per_sh", "Book/sh"), ("cash_per_sh", "Cash/sh"),
        ("quick_ratio", "Quick Ratio"), ("current_ratio", "Current Ratio"),
        ("debt_eq", "Debt/Eq"), ("lt_debt_eq", "LT Debt/Eq"),
        ("employees", "Employees"),
    )),
    ("Dividends", (
        ("dividend_est", "Dividend Est"), ("dividend_est_pct", "Dividend Est %"),
        ("dividend_ttm", "Dividend TTM"), ("dividend_ttm_pct", "Dividend TTM %"),
        ("dividend_gr_3y", "Div Growth 3Y %"),
        ("dividend_gr_5y", "Div Growth 5Y %"),
        ("payout_pct", "Payout %"),
    )),
    ("Growth & Estimates", (
        ("eps_ttm", "EPS (ttm)"), ("eps_next_y", "EPS next Y $"),
        ("eps_next_q", "EPS next Q $"), ("eps_this_y_pct", "EPS this Y %"),
        ("eps_next_y_pct", "EPS next Y %"),
        ("eps_next_5y_pct", "EPS next 5Y %"),
        ("eps_past_3y_pct", "EPS past 3Y %"),
        ("eps_past_5y_pct", "EPS past 5Y %"),
        ("sales_past_3y_pct", "Sales past 3Y %"),
        ("sales_past_5y_pct", "Sales past 5Y %"),
        ("eps_yoy_ttm_pct", "EPS Y/Y TTM %"),
        ("sales_yoy_ttm_pct", "Sales Y/Y TTM %"),
        ("eps_qoq_pct", "EPS Q/Q %"), ("sales_qoq_pct", "Sales Q/Q %"),
        ("eps_surprise_pct", "EPS Surprise %"),
        ("sales_surprise_pct", "Sales Surprise %"),
    )),
    ("Ownership & Short", (
        ("insider_own_pct", "Insider Own %"),
        ("insider_trans_pct", "Insider Trans %"),
        ("inst_own_pct", "Inst Own %"), ("inst_trans_pct", "Inst Trans %"),
        ("shs_outstanding", "Shs Outstanding"), ("shs_float", "Shs Float"),
        ("short_float_pct", "Short Float %"), ("short_ratio", "Short Ratio"),
        ("short_interest", "Short Interest"),
    )),
    ("Margins & Returns", (
        ("roa_pct", "ROA %"), ("roe_pct", "ROE %"), ("roic_pct", "ROIC %"),
        ("gross_margin_pct", "Gross Margin %"),
        ("oper_margin_pct", "Oper Margin %"),
        ("profit_margin_pct", "Profit Margin %"),
    )),
    ("Technicals", (
        ("sma20_pct", "SMA20 %"), ("sma50_pct", "SMA50 %"),
        ("sma200_pct", "SMA200 %"),
        ("high_52w", "52W High"), ("high_52w_pct", "52W High Dist %"),
        ("low_52w", "52W Low"), ("low_52w_pct", "52W Low Dist %"),
        ("finviz_rsi14", "RSI 14 (fv)"),
    )),
    ("Performance", (
        ("perf_week_pct", "Perf Week %"), ("perf_month_pct", "Perf Month %"),
        ("perf_quarter_pct", "Perf Quarter %"),
        ("perf_half_y_pct", "Perf Half Y %"), ("perf_ytd_pct", "Perf YTD %"),
        ("perf_year_pct", "Perf Year %"), ("perf_3y_pct", "Perf 3Y %"),
        ("perf_5y_pct", "Perf 5Y %"), ("perf_10y_pct", "Perf 10Y %"),
    )),
)

# Every field that gets a filter row, in panel order: the Options header's
# numeric pair first, then each group.
FILTERABLE_FIELDS: tuple = tuple(
    k for k, _l in OPTIONS_FIELDS if k not in BOOL_FIELDS
) + tuple(k for _g, items in FINVIZ_GROUPS for k, _l in items)

FIELD_LABELS: dict = dict(
    list(OPTIONS_FIELDS)
    + [(k, l) for _g, items in FINVIZ_GROUPS for k, l in items]
)

# Fields carried as columns but never filterable: the withheld seven plus the
# four free-text ones.
COLUMN_ONLY_FIELDS: tuple = tuple(
    f for f in SNAPSHOT_FIELDS
    if f not in FILTERABLE_FIELDS and f not in BOOL_FIELDS
)


# ----------------------------------------------------------------------
# Write buffer
# ----------------------------------------------------------------------
#
# Both producers write one ticker at a time. `merge_rows` rewrites the whole
# parquet, so calling it per ticker would rewrite a ~10k-row file every four
# seconds for eleven hours. Rows are buffered and flushed in batches instead.
#
# The buffer is process-local and deliberately NOT durable: a crash loses at
# most `_FLUSH_EVERY` rows, and those tickers simply look stale to the next
# sweep and get re-fetched. Paying for durability here would mean the
# per-ticker rewrite this exists to avoid.

_FLUSH_EVERY = 50
_pending: list = []


def queue_row(row: dict) -> int:
    """Buffer one snapshot row, flushing automatically when the batch fills.

    Returns the number of rows written to disk by this call (0 when the row
    was only buffered).
    """
    if not row or not row.get(SYMBOL_COL):
        return 0
    _pending.append(dict(row))
    if len(_pending) >= _FLUSH_EVERY:
        return flush_queue()
    return 0


def flush_queue() -> int:
    """Write any buffered rows. Safe to call when the buffer is empty.

    Callers invoke this at the end of a run and on cancellation, so a stopped
    sweep keeps the work it already paid for.
    """
    global _pending
    if not _pending:
        return 0
    batch, _pending = _pending, []
    try:
        return merge_rows(batch)
    except Exception as exc:
        log.error("finviz snapshot flush failed (%d rows lost): %s",
                  len(batch), exc)
        return 0


def pending_count() -> int:
    """Rows buffered but not yet on disk — for tests and status lines."""
    return len(_pending)


# ----------------------------------------------------------------------
# Per-field spinbox ranges
# ----------------------------------------------------------------------
#
# Every filter row needs bounds, defaults, a step and a decimal count that
# match what the field actually measures. A shared +/-1e12 sentinel made the
# panel unreadable: "Market Cap min -1000000000000" tells you nothing about
# the scale you are screening on, and a 0.01 step on a figure in the billions
# is unusable.
#
# `RANGE_KINDS` maps a kind to (spin_min, spin_max, default_min, default_max,
# step, decimals). A bound left sitting AT its spinbox limit is treated as
# OPEN by `_finviz_scan_params`, so "Short Float >= 20" with the maximum at
# 100 does not silently also assert "<= 100" - dragging a bound to the end of
# its range is the natural way to say "no limit on this side".
RANGE_KINDS: dict = {
    #                  spin_lo   spin_hi   def_lo   def_hi   step  dp
    "pct_0_100":     (      0.0,    100.0,     0.0,   100.0,   1.0, 2),
    "pct_signed":    (   -100.0,   1000.0,  -100.0,  1000.0,   5.0, 2),
    "pct_growth":    (   -100.0,  10000.0,  -100.0, 10000.0,  10.0, 2),
    "pct_margin":    (   -500.0,    500.0,  -500.0,   500.0,   5.0, 2),
    "ratio":         (      0.0,   1000.0,     0.0,  1000.0,   0.5, 2),
    "ratio_signed":  (   -100.0,    100.0,  -100.0,   100.0,   0.1, 2),
    "money_large":   (      0.0,   5.0e12,     0.0,  5.0e12, 1.0e8, 0),
    "shares":        (      0.0,   5.0e11,     0.0,  5.0e11, 1.0e6, 0),
    "price":         (      0.0,  100000.0,    0.0, 100000.0,  1.0, 2),
    "eps":           (  -1000.0,   1000.0, -1000.0,  1000.0,   0.1, 2),
    "count":         (      0.0,  5.0e6,       0.0,   5.0e6, 1000.0, 0),
    "recom":         (      1.0,      5.0,     1.0,     5.0,   0.1, 2),
    "rsi":           (      0.0,    100.0,     0.0,   100.0,   1.0, 2),
    "beta":          (     -5.0,     10.0,    -5.0,    10.0,   0.1, 2),
    "volatility":    (      0.0,    100.0,     0.0,   100.0,   0.5, 2),
}

# field -> kind. Anything unlisted falls back to "ratio", which is the least
# surprising general-purpose numeric shape.
FIELD_KINDS: dict = {
    # Valuation
    "market_cap": "money_large", "enterprise_value": "money_large",
    "pe": "ratio", "forward_pe": "ratio", "peg": "ratio", "ps": "ratio",
    "pb": "ratio", "pc": "ratio", "pfcf": "ratio",
    "ev_ebitda": "ratio", "ev_sales": "ratio",
    "target_price": "price", "recom": "recom",
    # Financials
    "income": "money_large", "sales": "money_large",
    "book_per_sh": "eps", "cash_per_sh": "eps",
    "quick_ratio": "ratio", "current_ratio": "ratio",
    "debt_eq": "ratio", "lt_debt_eq": "ratio",
    "employees": "count",
    # Dividends
    "dividend_est": "eps", "dividend_est_pct": "pct_0_100",
    "dividend_ttm": "eps", "dividend_ttm_pct": "pct_0_100",
    "dividend_gr_3y": "pct_signed", "dividend_gr_5y": "pct_signed",
    "payout_pct": "pct_0_100",
    # Growth & estimates
    "eps_ttm": "eps", "eps_next_y": "eps", "eps_next_q": "eps",
    "eps_this_y_pct": "pct_growth", "eps_next_y_pct": "pct_growth",
    "eps_next_5y_pct": "pct_growth",
    "eps_past_3y_pct": "pct_growth", "eps_past_5y_pct": "pct_growth",
    "sales_past_3y_pct": "pct_growth", "sales_past_5y_pct": "pct_growth",
    "eps_yoy_ttm_pct": "pct_growth", "sales_yoy_ttm_pct": "pct_growth",
    "eps_qoq_pct": "pct_growth", "sales_qoq_pct": "pct_growth",
    "eps_surprise_pct": "pct_signed", "sales_surprise_pct": "pct_signed",
    # Ownership & short
    "insider_own_pct": "pct_0_100", "insider_trans_pct": "pct_signed",
    "inst_own_pct": "pct_0_100", "inst_trans_pct": "pct_signed",
    "shs_outstanding": "shares", "shs_float": "shares",
    "short_float_pct": "pct_0_100", "short_ratio": "ratio",
    "short_interest": "shares",
    # Margins & returns
    "roa_pct": "pct_margin", "roe_pct": "pct_margin", "roic_pct": "pct_margin",
    "gross_margin_pct": "pct_margin", "oper_margin_pct": "pct_margin",
    "profit_margin_pct": "pct_margin",
    # Technicals
    "sma20_pct": "pct_signed", "sma50_pct": "pct_signed",
    "sma200_pct": "pct_signed",
    "high_52w": "price", "high_52w_pct": "pct_signed",
    "low_52w": "price", "low_52w_pct": "pct_signed",
    "finviz_rsi14": "rsi",
    # Options header
    "finviz_beta": "beta",
    "volatility_week_pct": "volatility", "volatility_month_pct": "volatility",
    # Performance
    "perf_week_pct": "pct_signed", "perf_month_pct": "pct_signed",
    "perf_quarter_pct": "pct_signed", "perf_half_y_pct": "pct_signed",
    "perf_ytd_pct": "pct_signed", "perf_year_pct": "pct_signed",
    "perf_3y_pct": "pct_growth", "perf_5y_pct": "pct_growth",
    "perf_10y_pct": "pct_growth",
}


def field_range(name: str) -> tuple:
    """(spin_lo, spin_hi, default_lo, default_hi, step, decimals) for a field."""
    return RANGE_KINDS[FIELD_KINDS.get(name, "ratio")]


def is_open_bound(name: str, value, *, upper: bool) -> bool:
    """Is this bound sitting at its limit, i.e. meaning "no limit"?"""
    if value is None:
        return True
    lo, hi = field_range(name)[0], field_range(name)[1]
    return value >= hi if upper else value <= lo
