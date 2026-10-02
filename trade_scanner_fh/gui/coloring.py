"""
Colouring rule engine (v8.0.0).

Replaces the three hard-coded colour schemes the results table used to paint
(green for quarters inside a beats streak, red for a display-only value that
would fail its filter, and the random palette that pairs an indicator date
with an earnings report date) with an ordered list of user rules. The three
old schemes ship as `default_rules()`, built so that — with the defaults
untouched — every cell renders exactly the colour it did before. A
differential test holds that line against a copy of the old renderer.

A RULE is:
  * scope      "row"      conditions are tested once per result row;
               "quarter"  tested once per quarter Q-1..Q-N, with `{k}` in a
                          column name standing for the quarter number;
  * match      "all" / "any" of the conditions;
  * conditions value / filter / date / quarter / earnings_match (below);
  * target     "row"      every cell in the row;
               "columns"  the listed columns (in quarter scope a quarter
                          TYPE such as `q_reported_eps` means that type at
                          the matching quarter);
               "matched"  the cells the true conditions looked at (plus, with
                          `expand_units`, every cell whose anchor date is one
                          of the matched dates — the old "unit" colouring);
  * skip_blank leave cells that read N/A unpainted (v8.0.0);
  * style      text colour, background colour, bold — each independent.
               A colour is none / fixed (any hex colour) / random from a
               palette; random is stable per ticker and group, and distinct
               between groups in one row, exactly as the old date-match
               palette was.

PRECEDENCE: rules are ranked, first = strongest, and each style CHANNEL is
resolved on its own — the strongest rule that sets text colour for a cell
wins its text colour, independently of which rule wins its background.

Pure data + pandas: no Qt objects are created here, so the same evaluation
drives the table and the Excel export (which can therefore colour every
period, not only the one on screen).
"""

from __future__ import annotations

import logging
import math
import random
import re
import uuid
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger("scanner.gui")

# 2 (v8.0.0): the streak rules test run MEMBERSHIP ("in_run") instead of
# `k <= streak`, skip N/A cells, and two growth-run rules joined the defaults.
# `rules_from_json` upgrades a version-1 list — see `_upgrade_v1`.
RULES_VERSION = 2

# The old match-colour palette — cool hues only, chosen for the user's
# colour-vision and readability preferences. Order matters: random picks
# index into it, and the default earnings rule must reproduce old colours.
DEFAULT_PALETTE: tuple = (
    "#4a90d9", "#26c6da", "#00bcd4", "#42a5f5", "#5c6bc0",
    "#3949ab", "#ab47bc", "#7e57c2", "#c084fc", "#26a69a",
)
STREAK_GREEN = "#4caf50"
FAIL_RED = "#e74c3c"
# Background for quarters counted in a growth run. A cool, dark blue: it has
# to sit under the default light text AND under STREAK_GREEN (a quarter can be
# in both runs) and stay readable, and it keeps to the palette's cool hues.
GROWTH_BG = "#1d3f5c"

SCOPES = ("row", "quarter")
MATCHES = ("all", "any")
KINDS = ("value", "filter", "date", "quarter", "earnings_match")
VALUE_OPS = (">", ">=", "<", "<=", "==", "!=", "between", "not_between",
             "blank", "not_blank", "top_pct", "bottom_pct",
             "contains", "not_contains")
FILTER_OPS = ("fails", "passes")
DATE_OPS = ("within", "before", "after")
QUARTER_OPS = (">", ">=", "<", "<=", "==", "!=", "in_run")
TARGETS = ("row", "columns", "matched")
COLOR_MODES = ("none", "fixed", "random")

# Series filters whose counted quarters the "in_run" quarter test can read.
# The scanner writes each one's Q-X numbers to `run_key(prefix)`.
RUN_SOURCES: tuple = (
    ("consec_eps_beats", "EPS beats streak"),
    ("consec_rev_beats", "Rev beats streak"),
    ("consec_eps_growth", "YoY EPS growth run"),
    ("consec_rev_growth", "YoY Rev growth run"),
    ("accel_eps_surp", "Accel EPS surprise series"),
    ("accel_rev_surp", "Accel Rev surprise series"),
    ("accel_eps_yoy", "Accel YoY EPS series"),
    ("accel_rev_yoy", "Accel YoY Rev series"),
)


def run_key(prefix: str) -> str:
    """The table-internal column holding a series filter's counted Q-X
    numbers (see scanner._write_run_quarters)."""
    return f"_{prefix}_qs"


ANY_FILTER = "*"               # filter condition: any display-only filter
ANY_REPORT_DATE = "*reports*"  # date condition: any report date in the row
K = "{k}"                      # quarter placeholder in column names

MAX_QUARTERS = 40
_HEX = re.compile(r"^#[0-9a-fA-F]{6}$")
_Q_COL = re.compile(r"^q(\d+)_(.+)$")
_Q_REPORT_DATE = re.compile(r"^q\d+_report_date_(eps|rev)$")


# ======================================================================
# Model
# ======================================================================

def _hex_or(value, default: str) -> str:
    return value if isinstance(value, str) and _HEX.match(value) else default


def _num_or_none(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


@dataclass
class ColorSpec:
    mode: str = "none"
    color: str = "#ffffff"
    palette: list = field(default_factory=lambda: list(DEFAULT_PALETTE))

    def to_dict(self) -> dict:
        return {"mode": self.mode, "color": self.color,
                "palette": list(self.palette)}

    @classmethod
    def from_dict(cls, d) -> "ColorSpec":
        d = d if isinstance(d, dict) else {}
        mode = d.get("mode", "none")
        palette = [c for c in (d.get("palette") or []) if
                   isinstance(c, str) and _HEX.match(c)]
        return cls(
            mode=mode if mode in COLOR_MODES else "none",
            color=_hex_or(d.get("color"), "#ffffff"),
            palette=palette or list(DEFAULT_PALETTE),
        )


@dataclass
class Style:
    text: ColorSpec = field(default_factory=ColorSpec)
    background: ColorSpec = field(default_factory=ColorSpec)
    bold: bool = False

    def to_dict(self) -> dict:
        return {"text": self.text.to_dict(),
                "background": self.background.to_dict(),
                "bold": bool(self.bold)}

    @classmethod
    def from_dict(cls, d) -> "Style":
        d = d if isinstance(d, dict) else {}
        return cls(text=ColorSpec.from_dict(d.get("text")),
                   background=ColorSpec.from_dict(d.get("background")),
                   bold=bool(d.get("bold", False)))

    def is_empty(self) -> bool:
        return (self.text.mode == "none" and self.background.mode == "none"
                and not self.bold)


@dataclass
class Condition:
    kind: str = "value"
    column: str = ""       # left operand (value / filter / date)
    op: str = ">="
    value: object = None   # constant right operand (number or text)
    value2: object = None  # upper bound for between / not_between
    other: str = ""        # column right operand (value / date / quarter)
    days: int = 0          # date tolerance / minimum gap

    def to_dict(self) -> dict:
        return {"kind": self.kind, "column": self.column, "op": self.op,
                "value": self.value, "value2": self.value2,
                "other": self.other, "days": int(self.days)}

    @classmethod
    def from_dict(cls, d) -> Optional["Condition"]:
        if not isinstance(d, dict) or d.get("kind") not in KINDS:
            return None
        kind = d["kind"]
        ops = {"value": VALUE_OPS, "filter": FILTER_OPS, "date": DATE_OPS,
               "quarter": QUARTER_OPS, "earnings_match": ("match",)}[kind]
        op = d.get("op")
        value = d.get("value")
        if not isinstance(value, str):
            value = _num_or_none(value)
        try:
            days = max(0, int(d.get("days") or 0))
        except (TypeError, ValueError):
            days = 0
        return cls(kind=kind, column=str(d.get("column") or ""),
                   op=op if op in ops else ops[0], value=value,
                   value2=_num_or_none(d.get("value2")),
                   other=str(d.get("other") or ""), days=days)

    def columns(self) -> list:
        """Column keys (possibly templated with {k}) this condition reads."""
        out = []
        if self.kind in ("value", "date") and self.column:
            out.append(self.column)
        if self.kind == "filter" and self.column and self.column != ANY_FILTER:
            out.append(self.column)
        if self.kind == "quarter" and self.op == "in_run":
            if self.other:
                out.append(run_key(self.other))
            return out
        if self.kind in ("value", "date", "quarter") and self.other \
                and self.other != ANY_REPORT_DATE:
            out.append(self.other)
        return out


@dataclass
class Rule:
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    name: str = "New rule"
    enabled: bool = True
    scope: str = "row"
    match: str = "all"
    conditions: list = field(default_factory=list)
    target: str = "matched"
    target_columns: list = field(default_factory=list)
    expand_units: bool = False
    style: Style = field(default_factory=Style)
    # Leave a target cell unpainted when it reads N/A. Colour on an empty
    # cell says "this value qualified" about a value that does not exist.
    skip_blank: bool = False

    def to_dict(self) -> dict:
        return {"id": self.id, "name": self.name, "enabled": self.enabled,
                "scope": self.scope, "match": self.match,
                "conditions": [c.to_dict() for c in self.conditions],
                "target": self.target,
                "target_columns": list(self.target_columns),
                "expand_units": bool(self.expand_units),
                "skip_blank": bool(self.skip_blank),
                "style": self.style.to_dict()}

    @classmethod
    def from_dict(cls, d) -> Optional["Rule"]:
        if not isinstance(d, dict):
            return None
        conds = [c for c in (Condition.from_dict(x)
                             for x in (d.get("conditions") or [])) if c]
        scope = d.get("scope", "row")
        match = d.get("match", "all")
        target = d.get("target", "matched")
        return cls(
            id=str(d.get("id") or uuid.uuid4().hex[:12]),
            name=str(d.get("name") or "Rule"),
            enabled=bool(d.get("enabled", True)),
            scope=scope if scope in SCOPES else "row",
            match=match if match in MATCHES else "all",
            conditions=conds,
            target=target if target in TARGETS else "matched",
            target_columns=[str(k) for k in (d.get("target_columns") or [])],
            expand_units=bool(d.get("expand_units", False)),
            style=Style.from_dict(d.get("style")),
            skip_blank=bool(d.get("skip_blank", False)),
        )

    def copy(self) -> "Rule":
        return Rule.from_dict(self.to_dict())


def rules_to_json(rules) -> dict:
    return {"version": RULES_VERSION,
            "rules": [r.to_dict() for r in rules or []]}


def rules_from_json(data) -> list:
    """Tolerant load. A malformed rule is dropped with a log line rather than
    failing the whole preset; an unreadable payload yields the defaults.

    A version-1 payload (8.0.0 test builds) is upgraded by `_upgrade_v1`."""
    version = RULES_VERSION
    if isinstance(data, dict):
        items = data.get("rules")
        try:
            version = int(data.get("version") or 1)
        except (TypeError, ValueError):
            version = 1
    elif isinstance(data, list):
        items = data
        version = 1
    else:
        return default_rules()
    if not isinstance(items, list):
        return default_rules()
    out = []
    for item in items:
        rule = Rule.from_dict(item)
        if rule is None:
            log.warning("colour rules: skipped an unreadable rule: %r", item)
            continue
        out.append(rule)
    if version < 2:
        out = _upgrade_v1(out)
    return out


def _v1_streak_rule(side: str) -> "Rule":
    """A version-1 default streak rule, exactly as 8.0.0 test builds saved it."""
    types = _EPS_Q_TYPES if side == "eps" else _REV_Q_TYPES
    label = "EPS" if side == "eps" else "Rev"
    return Rule(id=f"default_{side}_streak",
                name=f"Quarter inside the {label} beats streak",
                scope="quarter",
                conditions=[Condition(kind="quarter", op="<=",
                                      other=f"consec_{side}_beats")],
                target="columns", target_columns=list(types),
                style=Style(text=ColorSpec(mode="fixed", color=STREAK_GREEN)))


def _upgrade_v1(rules: list) -> list:
    """Bring a version-1 rule list up to version 2.

    Only UNTOUCHED defaults are replaced: a default rule whose content equals
    its version-1 form (its on/off switch aside, which is kept) becomes the
    version-2 rule. A default the user edited is theirs and is left exactly
    as it is. The two growth-run defaults did not exist in version 1, so they
    are added — after the Rev streak rule if it is there, else at the end —
    unless a rule with their id already exists.
    """
    v2 = {r.id: r for r in default_rules()}
    v1 = {r.id: r.to_dict() for r in (_v1_streak_rule("eps"),
                                      _v1_streak_rule("rev"))}
    out = []
    for rule in rules:
        old = v1.get(rule.id)
        if old is not None and {**rule.to_dict(), "enabled": True} == old:
            fresh = v2[rule.id].copy()
            fresh.enabled = rule.enabled
            rule = fresh
        out.append(rule)
    ids = {r.id for r in out}
    at = next((i + 1 for i, r in enumerate(out)
               if r.id == "default_rev_streak"), len(out))
    for new_id in ("default_eps_growth_run", "default_rev_growth_run"):
        if new_id not in ids:
            out.insert(at, v2[new_id].copy())
            at += 1
    return out


# ======================================================================
# Defaults — the three pre-v8 schemes, rule for rule
# ======================================================================

_EPS_Q_TYPES = ["q_report_date_eps", "q_reported_eps", "q_surprise_eps_dollar",
                "q_surprise_eps_pct", "q_yoy_eps_pct"]
_REV_Q_TYPES = ["q_report_date_rev", "q_reported_rev", "q_surprise_rev_dollar",
                "q_surprise_rev_pct", "q_yoy_rev_pct"]


def default_rules() -> list:
    """Top = strongest, matching the old paint order (streak green first,
    overridden by fail red, overridden by the date-match colour).

    v8.0.0 (rules version 2): the streak rules colour the quarters the streak
    actually COUNTED ("in_run") and leave N/A cells alone; the old
    `k <= streak` test was only right while the streak ended on Q-1 in report
    order. The growth-run rules shade the YoY cell of each quarter a growth
    run counted, on the background channel so they combine with streak
    green."""
    return [
        Rule(id="default_earnings_match",
             name="Earnings date match",
             conditions=[Condition(kind="earnings_match", op="match")],
             target="matched", expand_units=True,
             style=Style(text=ColorSpec(mode="random"))),
        Rule(id="default_display_only_fail",
             name="Display-only value fails its filter",
             conditions=[Condition(kind="filter", column=ANY_FILTER,
                                   op="fails")],
             target="matched",
             style=Style(text=ColorSpec(mode="fixed", color=FAIL_RED))),
        Rule(id="default_eps_streak",
             name="Quarter inside the EPS beats streak",
             scope="quarter",
             conditions=[Condition(kind="quarter", op="in_run",
                                   other="consec_eps_beats")],
             target="columns", target_columns=list(_EPS_Q_TYPES),
             skip_blank=True,
             style=Style(text=ColorSpec(mode="fixed", color=STREAK_GREEN))),
        Rule(id="default_rev_streak",
             name="Quarter inside the Rev beats streak",
             scope="quarter",
             conditions=[Condition(kind="quarter", op="in_run",
                                   other="consec_rev_beats")],
             target="columns", target_columns=list(_REV_Q_TYPES),
             skip_blank=True,
             style=Style(text=ColorSpec(mode="fixed", color=STREAK_GREEN))),
        Rule(id="default_eps_growth_run",
             name="Quarter counted in the YoY EPS growth run",
             scope="quarter",
             conditions=[Condition(kind="quarter", op="in_run",
                                   other="consec_eps_growth")],
             target="columns", target_columns=["q_yoy_eps_pct"],
             skip_blank=True,
             style=Style(background=ColorSpec(mode="fixed", color=GROWTH_BG))),
        Rule(id="default_rev_growth_run",
             name="Quarter counted in the YoY Rev growth run",
             scope="quarter",
             conditions=[Condition(kind="quarter", op="in_run",
                                   other="consec_rev_growth")],
             target="columns", target_columns=["q_yoy_rev_pct"],
             skip_blank=True,
             style=Style(background=ColorSpec(mode="fixed", color=GROWTH_BG))),
    ]


# ======================================================================
# Evaluation
# ======================================================================

class RowStyles:
    """Winning (rank, value) per channel for a row and its cells.

    `rank` is higher-is-stronger; `resolve` compares the row-wide entry and
    the cell entry for each channel independently.
    """
    __slots__ = ("row", "cells")

    def __init__(self):
        self.row: dict = {}
        self.cells: dict = {}

    def _put(self, bucket: dict, channel: str, rank: int, value) -> None:
        cur = bucket.get(channel)
        if cur is None or rank > cur[0]:
            bucket[channel] = (rank, value)

    def set_row(self, channel, rank, value):
        self._put(self.row, channel, rank, value)

    def set_cell(self, key, channel, rank, value):
        self._put(self.cells.setdefault(key, {}), channel, rank, value)

    def resolve(self, key) -> tuple:
        """(text_hex | None, background_hex | None, bold: bool)."""
        cell = self.cells.get(key, {})
        out = []
        for channel in ("text", "background", "bold"):
            a, b = self.row.get(channel), cell.get(channel)
            win = a if b is None else b if a is None else (b if b[0] > a[0] else a)
            out.append(None if win is None else win[1])
        return out[0], out[1], bool(out[2])


def _norm_iso(v):
    if v is None:
        return None
    try:
        ts = pd.Timestamp(v)
    except (TypeError, ValueError):
        return None
    if pd.isna(ts):
        return None
    return ts.normalize().date().isoformat()


def _pick_random(palette, seed: str, used: set) -> str:
    """The old match-colour picker: seeded base index, then linear probing so
    distinct groups in one row get distinct entries."""
    n = len(palette)
    base = random.Random(seed).randrange(n)
    idx = base
    for _ in range(n):
        if idx not in used:
            break
        idx = (idx + 1) % n
    used.add(idx)
    return palette[idx]


def quarter_count(columns) -> int:
    """Highest k with any `q{k}_...` column, capped at MAX_QUARTERS."""
    best = 0
    for c in columns:
        m = _Q_COL.match(str(c))
        if m:
            best = max(best, int(m.group(1)))
    return min(best, MAX_QUARTERS)


def _sub(col: str, k) -> str:
    return col.replace(K, str(k)) if (k is not None and col) else col


def _expand_target(key: str, k) -> str:
    """A quarter TYPE id (`q_reported_eps`) at quarter k is `q{k}_reported_eps`;
    anything else is taken as a column key (templated or literal)."""
    if k is not None and key.startswith("q_") and not _Q_COL.match(key):
        return f"q{k}_{key[2:]}"
    return _sub(key, k)


class _Ctx:
    """Per-evaluation shared state."""

    def __init__(self, df, records, layout_keys):
        self.df = df
        self.records = records
        self.layout = list(layout_keys)
        self.layout_set = set(self.layout)
        self.n = len(df)
        self.missing: set = set()


def _false(n):
    return np.zeros(n, dtype=bool)


def _eval_value(c: Condition, ctx: _Ctx, k):
    n, df = ctx.n, ctx.df
    col = _sub(c.column, k)
    if not col or col not in df.columns:
        if col:
            ctx.missing.add(col)
        return _false(n), None
    s = df[col]
    op = c.op
    cells = {col}
    if op in ("blank", "not_blank"):
        blank = s.isna().to_numpy() | (s.astype(str).str.strip()
                                       .isin(["", "nan", "None", "NaT"]))\
            .to_numpy()
        return (blank if op == "blank" else ~blank), cells
    if op in ("contains", "not_contains"):
        needle = "" if c.value is None else str(c.value)
        hit = s.astype(str).str.contains(needle, case=False, regex=False)\
            .fillna(False).to_numpy() & s.notna().to_numpy()
        if op == "contains":
            return hit, cells
        return s.notna().to_numpy() & ~hit, cells
    left = pd.to_numeric(s, errors="coerce")
    if op in ("top_pct", "bottom_pct"):
        pct = _num_or_none(c.value)
        valid = left.dropna()
        if pct is None or valid.empty:
            return _false(n), cells
        pct = min(max(pct, 0.0), 100.0)
        if op == "top_pct":
            thr = valid.quantile(1.0 - pct / 100.0)
            return (left >= thr).fillna(False).to_numpy(), cells
        thr = valid.quantile(pct / 100.0)
        return (left <= thr).fillna(False).to_numpy(), cells
    if op in ("between", "not_between"):
        lo, hi = _num_or_none(c.value), _num_or_none(c.value2)
        if lo is None or hi is None:
            return _false(n), cells
        lo, hi = min(lo, hi), max(lo, hi)
        inside = (left >= lo) & (left <= hi)
        res = inside if op == "between" else (~inside & left.notna())
        return res.fillna(False).to_numpy(), cells
    other = _sub(c.other, k)
    if other:
        if other not in df.columns:
            ctx.missing.add(other)
            return _false(n), cells
        right = pd.to_numeric(df[other], errors="coerce")
        cells = {col, other}
    else:
        rv = _num_or_none(c.value)
        if rv is None:
            return _false(n), cells
        right = rv
    res = {">": left > right, ">=": left >= right, "<": left < right,
           "<=": left <= right, "==": left == right,
           "!=": left != right}[op]
    valid = left.notna()
    if isinstance(right, pd.Series):
        valid &= right.notna()
    return (res & valid).fillna(False).to_numpy(), cells


def _eval_filter(c: Condition, ctx: _Ctx, k):
    n = ctx.n
    col = _sub(c.column, k) if c.column != ANY_FILTER else ANY_FILTER
    mask = _false(n)
    per_row = [set() for _ in range(n)]
    for i, rec in enumerate(ctx.records):
        flags = rec.get("_display_only_fails")
        flags = flags if isinstance(flags, dict) else {}
        failing = {key for key, v in flags.items() if v is True}
        if c.op == "fails":
            if col == ANY_FILTER:
                if failing:
                    mask[i] = True
                    per_row[i] = failing
            elif col in failing:
                mask[i] = True
                per_row[i] = {col}
        else:  # passes
            if col == ANY_FILTER:
                mask[i] = not failing
            else:
                v = rec.get(col)
                present = v is not None and not (
                    isinstance(v, float) and math.isnan(v))
                if present and col not in failing:
                    mask[i] = True
                    per_row[i] = {col}
    if col != ANY_FILTER and col not in ctx.df.columns:
        ctx.missing.add(col)
    return mask, per_row


def _eval_quarter(c: Condition, ctx: _Ctx, k):
    if k is None:
        return _false(ctx.n), None
    if c.op == "in_run":
        # Quarter k was COUNTED by that series filter: the scanner lists the
        # Q-X numbers each run used, bridged quarters excluded.
        key = run_key(c.other) if c.other else ""
        if not key or key not in ctx.df.columns:
            if c.other:
                ctx.missing.add(c.other)
            return _false(ctx.n), None
        mask = _false(ctx.n)
        for i, rec in enumerate(ctx.records):
            qs = rec.get(key)
            if isinstance(qs, (list, tuple, np.ndarray)) and k in list(qs):
                mask[i] = True
        return mask, set()
    if c.other:
        other = _sub(c.other, k)
        if other not in ctx.df.columns:
            ctx.missing.add(other)
            return _false(ctx.n), None
        # A blank count reads as 0 — the old streak renderer's rule (a ticker
        # with no beats data has "no streak", not an unknown one).
        right = pd.to_numeric(ctx.df[other], errors="coerce").fillna(0)\
            .to_numpy(dtype=float)
    else:
        rv = _num_or_none(c.value)
        if rv is None:
            return _false(ctx.n), None
        right = np.full(ctx.n, rv)
    left = float(k)
    res = {">": left > right, ">=": left >= right, "<": left < right,
           "<=": left <= right, "==": left == right,
           "!=": left != right}[c.op]
    return np.asarray(res, dtype=bool), set()


def _anchor_units(ctx: _Ctx, rec, isos_to_group: dict, lookup=None) -> dict:
    """Cells whose anchor date (see widgets._anchor_date_candidates) maps to
    one of `isos_to_group`'s dates → {cell_key: group}. The first candidate
    that maps wins, as it did in the old renderer."""
    from .widgets import _anchor_date_candidates
    out = {}
    for key in ctx.layout:
        for anchor in _anchor_date_candidates(key, rec):
            iso = _norm_iso(anchor)
            if iso is None:
                continue
            iso = lookup.get(iso, iso) if lookup else iso
            if iso in isos_to_group:
                out[key] = isos_to_group[iso]
                break
    return out


def _eval_earnings_match(c: Condition, ctx: _Ctx, k):
    """The scanner's own indicator-date ↔ report-date matching (tolerance from
    the Settings match-colour setting), exactly as the old renderer read it,
    including the earnings-anchor gate: a group is coloured only when a
    rendered EARNINGS cell anchors to its report date."""
    from .widgets import _anchor_date_candidates, _is_earnings_anchor_key
    n = ctx.n
    mask = _false(n)
    per_row = [dict() for _ in range(n)]
    for i, rec in enumerate(ctx.records):
        aligned = rec.get("_earnings_aligned_dates")
        if not isinstance(aligned, list) or not aligned:
            continue
        canon = rec.get("_earnings_aligned_canon")
        canon = canon if isinstance(canon, dict) else None
        seeds = sorted(set(canon.values())) if canon else sorted(set(aligned))
        anchored = set()
        for key in ctx.layout:
            if not _is_earnings_anchor_key(key):
                continue
            for anchor in _anchor_date_candidates(key, rec):
                iso = _norm_iso(anchor)
                if iso is None:
                    continue
                anchored.add(canon.get(iso, iso) if canon else iso)
        seeds = [s for s in seeds if s in anchored]
        if not seeds:
            continue
        units = _anchor_units(ctx, rec, {s: s for s in seeds}, canon)
        if units:
            mask[i] = True
            # The FULL gated seed list rides along: the old picker drew a
            # colour for every one of them in sorted order, so a seed that
            # ends up owning no cell still consumed a palette slot.
            per_row[i] = (units, seeds)
    return mask, per_row


def _date_candidates(ctx: _Ctx, rec, other: str, k):
    if other == ANY_REPORT_DATE:
        keys = [key for key in rec if _Q_REPORT_DATE.match(str(key))]
        if "last_report_date" in rec:
            keys.append("last_report_date")
    else:
        key = _sub(other, k)
        if key not in ctx.df.columns:
            ctx.missing.add(key)
            return []
        keys = [key]
    out = []
    for key in keys:
        iso = _norm_iso(rec.get(key))
        if iso is not None:
            out.append((key, iso))
    return out


def _eval_date(c: Condition, ctx: _Ctx, k, expand: bool):
    """Date A vs date B (or any report date in the row).

    within: |A - B| <= days; before: A earlier than B by >= max(days, 1);
    after: A later than B by >= max(days, 1). The group (for a random colour)
    is the nearest matching B date — the "report date is canonical" rule the
    old matcher used — so A, B and, with `expand`, their linked value cells
    share one colour."""
    n = ctx.n
    mask = _false(n)
    per_row = [dict() for _ in range(n)]
    col = _sub(c.column, k)
    if not col or col not in ctx.df.columns:
        if col:
            ctx.missing.add(col)
        return mask, per_row
    for i, rec in enumerate(ctx.records):
        a_iso = _norm_iso(rec.get(col))
        if a_iso is None:
            continue
        a = pd.Timestamp(a_iso)
        best = None
        for key, b_iso in _date_candidates(ctx, rec, c.other, k):
            if key == col:
                continue
            d = (a - pd.Timestamp(b_iso)).days
            ok = (abs(d) <= c.days if c.op == "within"
                  else d <= -max(c.days, 1) if c.op == "before"
                  else d >= max(c.days, 1))
            if not ok:
                continue
            if best is None or abs(d) < best[0] or (
                    abs(d) == best[0] and b_iso < best[2]):
                best = (abs(d), key, b_iso)
        if best is None:
            continue
        _d, b_key, group = best
        cells = {col: group, b_key: group}
        if expand:
            cells.update(_anchor_units(
                ctx, rec, {a_iso: group, group: group}))
        mask[i] = True
        per_row[i] = (cells, [group])
    return mask, per_row


def _eval_condition(c: Condition, ctx: _Ctx, k, expand: bool):
    """-> (mask[n] bool, cells) where cells is None, a static set of keys,
    a per-row list of sets, or a per-row list of {key: group} dicts."""
    if c.kind == "value":
        return _eval_value(c, ctx, k)
    if c.kind == "filter":
        return _eval_filter(c, ctx, k)
    if c.kind == "quarter":
        return _eval_quarter(c, ctx, k)
    if c.kind == "date":
        return _eval_date(c, ctx, k, expand)
    if c.kind == "earnings_match":
        return _eval_earnings_match(c, ctx, k)
    return _false(ctx.n), None


def _cells_for_row(cells, i) -> tuple:
    """Normalise one condition's cells for row i to ({key: group|None},
    [ordered groups])."""
    if cells is None:
        return {}, []
    if isinstance(cells, (set, frozenset)):
        return {key: None for key in cells}, []
    row = cells[i]
    if isinstance(row, tuple):
        units, order = row
        return dict(units), list(order)
    if isinstance(row, dict):
        return dict(row), []
    return {key: None for key in row}, []


def _is_q_type(key: str) -> bool:
    return key.startswith("q_") and not _Q_COL.match(key)


def _is_blank(v) -> bool:
    """Does this cell read N/A? None, NaN / NaT, or empty text."""
    if v is None:
        return True
    if isinstance(v, str):
        return not v.strip()
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False            # a list or other container: not blank


def _apply(rule: Rule, rank: int, ctx: _Ctx, out: list, k=None):
    """Evaluate `rule` (at quarter k in quarter scope) and paint `out`.
    Returns the boolean mask of rows that matched."""
    results = [_eval_condition(c, ctx, k, rule.expand_units)
               for c in rule.conditions]
    masks = [m for m, _c in results]
    mask = (np.logical_and.reduce(masks) if rule.match == "all"
            else np.logical_or.reduce(masks))
    for i in np.flatnonzero(mask):
        rec = ctx.records[i]
        order: list = []
        if rule.target == "row":
            targets = None
        elif rule.target == "columns":
            targets = {}
            for key in rule.target_columns:
                if k is None and _is_q_type(key):
                    # A quarter TYPE on a row-scope rule means that type at
                    # every quarter the table shows.
                    suffix = key[2:]
                    for lk in ctx.layout:
                        m = _Q_COL.match(lk)
                        if m and m.group(2) == suffix:
                            targets[lk] = None
                else:
                    targets[_expand_target(key, k)] = None
        else:
            targets = {}
            for (_m, cells), c_mask in zip(results, masks):
                if c_mask[i]:
                    cell_groups, groups = _cells_for_row(cells, i)
                    targets.update(cell_groups)
                    order.extend(groups)
        if targets is not None:
            targets = {key: g for key, g in targets.items()
                       if key in ctx.layout_set}
            if not targets:
                continue
        if rule.skip_blank:
            if targets is None:
                targets = {key: None for key in ctx.layout}
            targets = {key: g for key, g in targets.items()
                       if not _is_blank(rec.get(key))}
            if not targets:
                continue
        rs = out[i]
        if rs is None:
            rs = out[i] = RowStyles()
        _paint(rs, rule.style, rank, rule, str(rec.get("symbol", "")),
               targets, k, order)
    return mask


def _paint(rs: RowStyles, style: Style, rank: int, rule: Rule, sym: str,
           targets, k, order) -> None:
    """Resolve fixed / random colours and write them into `rs`."""
    for channel, spec in (("text", style.text),
                          ("background", style.background)):
        if spec.mode == "none":
            continue
        if spec.mode == "fixed":
            if targets is None:
                rs.set_row(channel, rank, spec.color)
            else:
                for key in targets:
                    rs.set_cell(key, channel, rank, spec.color)
            continue
        # Random. A cell's group is the date it matched on when it has one
        # (so a date pair shares a colour and distinct pairs in a row
        # differ, drawn in sorted order exactly like the old palette);
        # otherwise one group per rule per row, per quarter in quarter
        # scope. Background draws are salted so a rule randomising both
        # channels does not paint text the colour of its own background.
        palette = list(spec.palette) or list(DEFAULT_PALETTE)
        salt = "" if channel == "text" else "|bg"
        fallback = rule.id if k is None else f"{rule.id}|q{k}"
        if targets is None:
            rs.set_row(channel, rank,
                       _pick_random(palette, f"{sym}|{fallback}{salt}", set()))
            continue
        groups = sorted(set(order) | {
            g if g is not None else fallback for g in targets.values()})
        used: set = set()
        colour = {g: _pick_random(palette, f"{sym}|{g}{salt}", used)
                  for g in groups}
        for key, g in targets.items():
            rs.set_cell(key, channel, rank,
                        colour[g if g is not None else fallback])
    if style.bold:
        if targets is None:
            rs.set_row("bold", rank, True)
        else:
            for key in targets:
                rs.set_cell(key, "bold", rank, True)


def evaluate(df, rules, layout_keys, *, records=None,
             report: Optional[dict] = None) -> list:
    """Style every row of `df` for the columns in `layout_keys`.

    Returns a list (one per row) of `RowStyles` or None. `report`, when
    given, is filled with {rule.id: {"rows": n, "missing": [cols]}} for the
    rule editor's status line; `rows` counts rows the conditions matched
    (any quarter, in quarter scope). A rule that raises is skipped and
    reported rather than costing the table its colours.
    """
    if df is None or len(df) == 0:
        return []
    if records is None:
        records = df.to_dict("records")
    ctx = _Ctx(df, records, layout_keys)
    out: list = [None] * ctx.n
    rules = list(rules or [])
    quarters = None
    # Weakest first. Ranks decide the result regardless of order; a fixed
    # order just keeps the evaluation reproducible.
    for idx in range(len(rules) - 1, -1, -1):
        rule = rules[idx]
        if not (rule.enabled and rule.conditions
                and not rule.style.is_empty()):
            if report is not None:
                report[rule.id] = {"rows": 0, "missing": [],
                                   "inactive": True}
            continue
        rank = len(rules) - idx
        ctx.missing = set()
        try:
            if rule.scope == "quarter":
                if quarters is None:
                    quarters = quarter_count(df.columns)
                hit = _false(ctx.n)
                for k in range(1, quarters + 1):
                    hit |= _apply(rule, rank, ctx, out, k)
            else:
                hit = _apply(rule, rank, ctx, out)
        except Exception as exc:
            log.warning("colour rule %r failed and was skipped: %s",
                        rule.name, exc, exc_info=True)
            if report is not None:
                report[rule.id] = {"rows": 0, "missing": [],
                                   "error": str(exc)}
            continue
        if report is not None:
            report[rule.id] = {"rows": int(np.count_nonzero(hit)),
                               "missing": sorted(ctx.missing)}
    return out


# ======================================================================
# Requirements (v8.0.1 — colour-rule favorites)
# ======================================================================

# Stands for "at least one Q-X quarter column": what a quarter-scope rule
# needs before it has anything to test.
ANY_QUARTER = "q{k}_*"


def _present(key: str, columns) -> bool:
    """Is `key` available among `columns`? A `{k}` template, or a quarter
    TYPE id such as `q_reported_eps`, counts as present when ANY quarter of
    that type is; ANY_QUARTER when any Q-X column at all is."""
    if key == ANY_QUARTER:
        return quarter_count(columns) >= 1
    if K in key:
        pattern = re.compile(
            "^" + re.escape(key).replace(re.escape(K), r"\d+") + "$")
        return any(pattern.match(str(c)) for c in columns)
    if _is_q_type(key):
        suffix = key[2:]
        return any((m := _Q_COL.match(str(c))) and m.group(2) == suffix
                   for c in columns)
    return key in columns


def unmet_requirements(rule: Rule, columns) -> tuple:
    """``(missing_inputs, missing_targets)`` for `rule` against the column
    keys a scan produced — a static check, nothing is evaluated.

    * missing_inputs: every column a condition reads (`Condition.columns`,
      which includes an "in the run of" filter's run data) that is absent.
      A quarter-scope rule whose inputs are all there still needs Q-X
      columns to test; ANY_QUARTER is reported then — only then, so the
      reason names the specific filter whenever there is one. The
      earnings-date match and "any display-only filter" tests read the
      scan's own per-row data and need no particular column.
    * missing_targets: a "Chosen columns" rule none of whose targets exist —
      all of them, since any one would do. Empty otherwise.
    """
    cols = set(columns)
    missing: list = []
    for cond in rule.conditions:
        for key in cond.columns():
            if key not in missing and not _present(key, cols):
                missing.append(key)
    if rule.scope == "quarter" and not missing \
            and not _present(ANY_QUARTER, cols):
        missing.append(ANY_QUARTER)
    targets: list = []
    if rule.target == "columns" and rule.target_columns and not any(
            _present(k, cols) for k in rule.target_columns):
        targets = list(rule.target_columns)
    return missing, targets
