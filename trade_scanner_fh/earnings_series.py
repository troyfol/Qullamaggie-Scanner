"""Quarter-series analytics over `earnings_history.parquet`.

Two families of multi-quarter earnings filters live here, both built on
the same quarter-slot primitive:

  * **Consecutive YoY growth** (`consecutive_growth_run`) - the longest
    run of quarters whose YoY growth clears a threshold.
  * **Consecutive accelerating quarters** (`accelerating_series`) - the
    longest / most recent / anchored run of quarters whose metric value
    steps *up* by at least a fixed number of percentage points each
    quarter.

Both are pure functions over a per-ticker slice of the earnings history
and carry no GUI or I/O dependency, so they are directly unit-testable.

Why a separate module rather than more of `earnings_history.py`: the
series machinery is a self-contained algorithm with its own vocabulary
(slots, chains, anchors) and its own acceptance-test suite.
`compute_consecutive_beats` stays where it is - it is a one-pass
trailing-streak counter with none of this structure.

Vocabulary
----------
**Point**   A fiscal quarter in the pool that has a finite value for the
            metric under test.
**Slot**    A fiscal quarter position. A slot with no row, or a row whose
            metric value is NaN, is a *missing* slot - both are treated
            identically (spec 3.1, "missing / empty periods").
**Chain**   A maximal run of points linked by the step rules (growth
            threshold + sign rule + gap rule).
**Series**  The tail of a chain that starts at the first point clearing
            the starting threshold.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Sequence

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# A fiscal quarter is 3 months. `period_ending` is normalised to day-1 of
# the fiscal-quarter month at row-construction time across every source
# (see the README's period_ending normalisation note), so month
# arithmetic - not day counting - is the exact way to measure how many
# quarters separate two periods.
_MONTHS_PER_QUARTER = 3

# How many consecutive missing fiscal periods a chain may bridge.
# Spec 3.1: one may be skipped, two or more break the chain.
MAX_BRIDGED_MISSING = 1

# Series-selection modes for `accelerating_series`.
SELECT_LONGEST = "longest"
SELECT_MOST_RECENT = "most_recent"

# The four metrics the accelerating filters run over. Maps the filter's
# short key to the `earnings_history.parquet` column holding the value,
# already expressed in percent.
METRIC_COLUMNS: dict[str, str] = {
    "eps_surp": "surprise_eps_pct",
    "rev_surp": "surprise_rev_pct",
    "eps_yoy": "yoy_eps_pct",
    "rev_yoy": "yoy_rev_pct",
}


# ----------------------------------------------------------------------
# Quarter pool construction
# ----------------------------------------------------------------------

@dataclass(frozen=True)
class QuarterPoint:
    """One fiscal quarter that actually carries a value for the metric.

    `missing_before` is the number of empty fiscal periods between the
    previous point and this one - 0 for adjacent quarters, 1 when one
    quarter was skipped, and so on. It is derived from `period_ending`
    month arithmetic, so a quarter that is absent from the parquet and a
    quarter present with a NaN metric value produce the same count.
    """
    value: float
    period: pd.Timestamp
    report_date: Optional[pd.Timestamp]
    missing_before: int


def _period_steps(older: pd.Timestamp, newer: pd.Timestamp) -> int:
    """Number of fiscal-quarter steps from `older` to `newer`.

    1 means adjacent quarters. Month-based rather than day-based so a
    52/53-week filer whose period_ending drifts by a few weeks still
    reads as one quarter apart - the day-count test used by
    `compute_consecutive_beats` (>135 days) tolerates that drift too,
    but only for the single-step case; a series needs the actual count.
    """
    delta_months = (
        (newer.year - older.year) * 12 + (newer.month - older.month)
    )
    if delta_months <= 0:
        return 0
    return max(1, int(round(delta_months / _MONTHS_PER_QUARTER)))


def build_quarter_points(
    ticker_history: Optional[pd.DataFrame],
    metric_col: str,
    *,
    quarter_cap: int = 0,
) -> list[QuarterPoint]:
    """Project one ticker's earnings history onto an oldest-to-newest
    list of quarters carrying a finite value for `metric_col`.

    `ticker_history` is expected in the same shape the scanner already
    hands `compute_consecutive_beats`: rows for one ticker, restricted
    to `report_date <= scan end`, real-announcement rows preferred over
    finnhub calendar proxies, sorted `report_date` DESC.

    `quarter_cap` (spec 3.4) defines the *pool*: the N most recently
    reported quarters. 0 means no cap. The cap is applied on the
    report_date-DESC ordering - the same slicing basis the beats Q Cap
    uses - before the pool is re-sorted into fiscal order, because
    "look back N quarters" is a statement about reporting recency.
    """
    if ticker_history is None or ticker_history.empty:
        return []
    if metric_col not in ticker_history.columns:
        return []
    if "period_ending" not in ticker_history.columns:
        # Without period_ending there is no way to tell an adjacent
        # quarter from a two-year hole, and every series rule depends on
        # that distinction. Refuse rather than guess.
        log.debug(
            "build_quarter_points: history has no period_ending column - "
            "series filters cannot run on this slice."
        )
        return []

    df = ticker_history
    cap = int(quarter_cap or 0)
    if cap > 0:
        df = df.head(cap)

    periods = pd.to_datetime(df["period_ending"], errors="coerce")
    values = pd.to_numeric(df[metric_col], errors="coerce")
    if "report_date" in df.columns:
        reports = pd.to_datetime(df["report_date"], errors="coerce")
    else:
        reports = pd.Series(pd.NaT, index=df.index)

    # A row with no usable period cannot be positioned on the quarter
    # grid, so it can neither contribute a point nor open a hole.
    ordered: list[tuple] = []
    seen_periods: set = set()
    for period, value, report in zip(periods, values, reports):
        if pd.isna(period):
            continue
        period = pd.Timestamp(period).normalize()
        # The frame arrives report_date DESC and is already deduped per
        # (ticker, period_ending) by `dedupe_history`; keep-first here is
        # a defensive second line so a duplicated slot can never be
        # counted as two quarters.
        if period in seen_periods:
            continue
        seen_periods.add(period)
        ordered.append((
            period,
            float(value) if pd.notna(value) else float("nan"),
            None if pd.isna(report) else pd.Timestamp(report),
        ))

    ordered.sort(key=lambda t: t[0])

    points: list[QuarterPoint] = []
    prev_period: Optional[pd.Timestamp] = None
    for period, value, report in ordered:
        if not np.isfinite(value):
            # A present-but-NaN quarter is a missing slot, exactly like
            # an absent row. It is not appended; the period delta to the
            # next real point accounts for it (spec 3.1).
            continue
        if prev_period is None:
            missing_before = 0
        else:
            missing_before = max(0, _period_steps(prev_period, period) - 1)
        points.append(QuarterPoint(
            value=value, period=period, report_date=report,
            missing_before=missing_before,
        ))
        prev_period = period
    return points


# ----------------------------------------------------------------------
# Shared step rules (spec 3.1, 3.2)
# ----------------------------------------------------------------------

def _sign_flip(prev_value: float, value: float) -> bool:
    """Spec 3.2 - a move from strictly negative to strictly positive
    breaks the chain regardless of its size. Zero is neither positive nor
    negative, so `-5 -> 0` and `0 -> +5` both survive."""
    return prev_value < 0.0 and value > 0.0


def _gap_ok(point: QuarterPoint) -> bool:
    """Spec 3.1 - one missing period may be bridged, two or more break."""
    return point.missing_before <= MAX_BRIDGED_MISSING


# ----------------------------------------------------------------------
# Part 1 - consecutive YoY growth
# ----------------------------------------------------------------------

def consecutive_growth_run(
    points: Sequence[QuarterPoint], min_growth_pct: float,
) -> int:
    """Longest run of quarters whose value clears `min_growth_pct`.

    A quarter is a *hit* when `value >= min_growth_pct` (spec 3.3 - all
    thresholds inclusive). A run continues from one hit to the next when
    the pair bridges at most one missing period and does not flip sign.
    Bridged quarters do not count toward the length (spec 3.1).

    Returns the length of the longest run anywhere in the pool, per the
    spec's "the stock passes if there *exists* a run of hits with length
    >= Minimum Consecutive Quarters". This is deliberately *not* the
    trailing-only semantic of `compute_consecutive_beats`.
    """
    best = 0
    current = 0
    for i, point in enumerate(points):
        if not (point.value >= min_growth_pct):
            current = 0
            continue
        if current == 0:
            current = 1
        else:
            prev = points[i - 1]
            if _gap_ok(point) and not _sign_flip(prev.value, point.value):
                current += 1
            else:
                current = 1
        if current > best:
            best = current
    return best


# ----------------------------------------------------------------------
# Part 2 - consecutive accelerating quarters
# ----------------------------------------------------------------------

@dataclass(frozen=True)
class AcceleratingSeries:
    """The series a single accelerating filter resolved to for a ticker.

    `qualifies` is False when the best candidate fell short of Minimum
    Series Count. The series is still reported in that case so the
    display-only column shows how far the ticker actually got instead of
    a bare blank.
    """
    length: int
    start_value: float
    end_value: float
    start_period: pd.Timestamp
    end_period: pd.Timestamp
    start_report_date: Optional[pd.Timestamp]
    end_report_date: Optional[pd.Timestamp]
    qualifies: bool


def _link_ok(
    prev: QuarterPoint, point: QuarterPoint, min_step_pct: float,
) -> bool:
    """Can `point` extend a chain ending at `prev`?

    All three step rules at once: the gap rule (spec 3.1), the
    acceleration threshold measured in **percentage points** (Part 2
    terminology), and the sign rule (spec 3.2).
    """
    if not _gap_ok(point):
        return False
    if _sign_flip(prev.value, point.value):
        return False
    return (point.value - prev.value) >= min_step_pct


def _candidate_for_terminal(
    points: Sequence[QuarterPoint],
    terminal: int,
    min_step_pct: float,
    min_start_pct: float,
) -> Optional[tuple]:
    """Two-pass series construction for one terminal quarter.

    Pass 1 extends backward from `terminal` as far as the step rules
    permit, producing that terminal's maximal raw chain. Pass 2 walks
    the chain from its oldest end forward to the first quarter clearing
    the starting threshold; the series begins there.

    A chain whose oldest quarters sit below the starting threshold is
    *not* rejected - the series simply starts later, which is what lets
    a stock accelerating up through the threshold from below qualify
    (spec T7). Returns `(start_index, terminal)` or None when no quarter
    in the chain clears the starting threshold.
    """
    chain_start = terminal
    while chain_start > 0 and _link_ok(
        points[chain_start - 1], points[chain_start], min_step_pct,
    ):
        chain_start -= 1
    for i in range(chain_start, terminal + 1):
        if points[i].value >= min_start_pct:
            return (i, terminal)
    return None


def accelerating_series(
    points: Sequence[QuarterPoint],
    *,
    min_start_pct: float,
    min_step_pct: float,
    min_count: int,
    selection: str = SELECT_LONGEST,
    backward_only: bool = False,
) -> Optional[AcceleratingSeries]:
    """Resolve the accelerating series for one ticker, or None when the
    pool holds no quarter with data for the metric.

    Selection (ignored when `backward_only`):
      ``longest``      greatest quarter count; ties broken by the newest
                       terminal quarter.
      ``most_recent``  newest terminal quarter. Each terminal yields at
                       most one series, so no tie-break is needed. Note
                       this ranks by recency - it does not require the
                       series to end on the newest quarter in the pool.

    ``backward_only`` anchors the series on the newest quarter that has
    data for the metric and builds only that one candidate. If the
    anchor's series is short, the stock fails; the anchor is never
    stepped back to hunt for a different series.

    When no candidate reaches `min_count`, the best sub-threshold
    candidate is returned with ``qualifies=False`` rather than None, so
    display-only mode can show the length the ticker did reach and the
    red-on-fail colouring has a value to mark.
    """
    n = len(points)
    if n == 0:
        return None

    terminals = [n - 1] if backward_only else list(range(n))

    candidates: list[tuple] = []
    for terminal in terminals:
        cand = _candidate_for_terminal(
            points, terminal, min_step_pct, min_start_pct,
        )
        if cand is not None:
            candidates.append(cand)
    if not candidates:
        return None

    floor = max(1, int(min_count))
    qualifying = [c for c in candidates if (c[1] - c[0] + 1) >= floor]
    pool = qualifying if qualifying else candidates
    qualifies = bool(qualifying)

    if backward_only:
        best = pool[0]
    elif selection == SELECT_MOST_RECENT:
        # Rank by terminal quarter, newest first. Each terminal produces
        # at most one candidate, so the maximum is unique.
        best = max(pool, key=lambda c: c[1])
    else:
        # Longest: greatest quarter count, ties to the newest terminal.
        best = max(pool, key=lambda c: (c[1] - c[0] + 1, c[1]))

    start_i, end_i = best
    start_pt, end_pt = points[start_i], points[end_i]
    return AcceleratingSeries(
        length=end_i - start_i + 1,
        start_value=start_pt.value,
        end_value=end_pt.value,
        start_period=start_pt.period,
        end_period=end_pt.period,
        start_report_date=start_pt.report_date,
        end_report_date=end_pt.report_date,
        qualifies=qualifies,
    )
