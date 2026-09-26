"""v8.0.0 phase 3 — the colouring rule engine (gui/coloring.py).

The first block is the load-bearing one: with `default_rules()` the engine
must paint EXACTLY what the pre-v8 renderer painted. The oracle below is the
colour logic of `ResultsTable._populate_row` as of 7.0.2 (git HEAD 86b31bc),
transcribed verbatim minus the Qt item plumbing, and it is compared cell by
cell against the engine on hand-built edge cases and on a seeded random
corpus.

ONE deliberate amendment (rules version 2, 2026-09-26), marked inline in the
oracle: streak green is no longer painted on a cell that reads N/A — the user
read a green N/A YoY cell as "N/A counted as a beat". Everything else is
still the 7.0.2 renderer. The frames also carry `_consec_*_beats_qs`, the
quarters a streak counted, which the scanner now writes and the streak rule
reads; for the trailing streaks these rows describe that is Q-1..Q-n, the
exact set the old `k <= streak` test chose.
"""
from __future__ import annotations

import random

import numpy as np
import pandas as pd
import pytest

from trade_scanner_fh.gui import coloring as C
from trade_scanner_fh.gui import widgets as W


@pytest.fixture(scope="module", autouse=True)
def qapp():
    from PyQt6.QtWidgets import QApplication
    return QApplication.instance() or QApplication([])


# ======================================================================
# Oracle: the 7.0.2 renderer's colour decisions (text colour only — it
# never set background or bold)
# ======================================================================

_OLD_PALETTE = ["#4a90d9", "#26c6da", "#00bcd4", "#42a5f5", "#5c6bc0",
                "#3949ab", "#ab47bc", "#7e57c2", "#c084fc", "#26a69a"]


def _blank(v) -> bool:
    if v is None:
        return True
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False


def legacy_colors(row_data, cols) -> dict:
    eps_streak = W._safe_streak(row_data.get("consec_eps_beats"))
    rev_streak = W._safe_streak(row_data.get("consec_rev_beats"))
    fail_flags = row_data.get("_display_only_fails")
    if not isinstance(fail_flags, dict):
        fail_flags = {}
    aligned_iso = row_data.get("_earnings_aligned_dates")
    canon_map = row_data.get("_earnings_aligned_canon")
    if not isinstance(canon_map, dict):
        canon_map = None
    aligned_color_map = {}
    if isinstance(aligned_iso, list) and aligned_iso:
        seed_isos = (sorted(set(canon_map.values()))
                     if canon_map else sorted(set(aligned_iso)))
        earnings_anchored_isos = set()
        for _h, _k, _f in cols:
            if not W._is_earnings_anchor_key(_k):
                continue
            for anchor_v in W._anchor_date_candidates(_k, row_data):
                try:
                    _ts = pd.Timestamp(anchor_v)
                    if pd.isna(_ts):
                        continue
                except (TypeError, ValueError):
                    continue
                _v_iso = _ts.normalize().date().isoformat()
                _lookup = (canon_map.get(_v_iso, _v_iso)
                           if canon_map else _v_iso)
                earnings_anchored_isos.add(_lookup)
        seed_isos = [iso for iso in seed_isos if iso in earnings_anchored_isos]
        palette_n = len(_OLD_PALETTE)
        used = set()
        symbol = row_data.get("symbol", "")
        for iso in seed_isos:
            base = random.Random(f"{symbol}|{iso}").randrange(palette_n)
            idx = base
            for _ in range(palette_n):
                if idx not in used:
                    break
                idx = (idx + 1) % palette_n
            used.add(idx)
            aligned_color_map[iso] = _OLD_PALETTE[idx]
    out = {}
    for _header, key, _fmt in cols:
        color = None
        qm = W._Q_COL_RE.match(key)
        if qm is not None:
            q_num = int(qm.group(1))
            suffix = qm.group(2)
            is_rev = (suffix.endswith("_rev")
                      or suffix.startswith("reported_rev")
                      or suffix.startswith("surprise_rev")
                      or suffix.startswith("yoy_rev")
                      or suffix == "report_date_rev")
            is_eps = (suffix.endswith("_eps")
                      or suffix.startswith("reported_eps")
                      or suffix.startswith("surprise_eps")
                      or suffix.startswith("yoy_eps")
                      or suffix == "report_date_eps")
            if is_rev and q_num <= rev_streak:
                color = "#4caf50"
            elif is_eps and q_num <= eps_streak:
                color = "#4caf50"
            # --- v8.0.0 amendment: streak green skips N/A cells ---
            if color == "#4caf50" and _blank(row_data.get(key)):
                color = None
        if fail_flags.get(key) is True:
            color = "#e74c3c"
        if aligned_color_map:
            for anchor_val in W._anchor_date_candidates(key, row_data):
                try:
                    ts = pd.Timestamp(anchor_val)
                    if pd.isna(ts):
                        continue
                    v_iso = ts.normalize().date().isoformat()
                    lookup_iso = (canon_map.get(v_iso, v_iso)
                                  if canon_map else v_iso)
                    c = aligned_color_map.get(lookup_iso)
                    if c is not None:
                        color = c
                        break
                except Exception:
                    continue
        if color is not None:
            out[key] = color
    return out


def engine_colors(df, cols, rules=None) -> list:
    keys = [k for _h, k, _f in cols]
    styles = C.evaluate(df, rules if rules is not None else C.default_rules(),
                        keys)
    out = []
    for rs in styles:
        row = {}
        if rs is not None:
            for key in keys:
                text, _bg, _bold = rs.resolve(key)
                if text is not None:
                    row[key] = text
        out.append(row)
    return out


def assert_same_as_legacy(df, hide=()):
    cols, _n_eps, _n_rev = W._build_dynamic_columns(
        df, hidden_keys=frozenset(hide))
    records = df.to_dict("records")
    got = engine_colors(df, cols)
    for i, rec in enumerate(records):
        want = legacy_colors(rec, cols)
        assert got[i] == want, (i, rec.get("symbol"), got[i], want)


# ----------------------------------------------------------------------

D = pd.Timestamp


def _beats_row(sym, n_q=4, eps_streak=2, rev_streak=1, **extra):
    row = {"symbol": sym, "close": 10.0, "pct_gain": 5.0,
           "gain_start_date": D("2026-01-02"),
           "consec_eps_beats": eps_streak, "consec_rev_beats": rev_streak}
    for k in range(1, n_q + 1):
        date = D("2026-02-15") - pd.Timedelta(days=91 * (k - 1))
        for side in ("eps", "rev"):
            row[f"q{k}_report_date_{side}"] = date
            row[f"q{k}_reported_{side}"] = 1.0 + k
            row[f"q{k}_surprise_{side}_dollar"] = 0.1 * k
            row[f"q{k}_surprise_{side}_pct"] = 5.0 * k
            row[f"q{k}_yoy_{side}_pct"] = 10.0 * k
    row.update(extra)
    _stamp_counted_quarters(row)
    return row


def _stamp_counted_quarters(row):
    """What the scanner writes for a trailing streak: Q-1..Q-n."""
    for side in ("eps", "rev"):
        n = row.get(f"consec_{side}_beats")
        key = f"_consec_{side}_beats_qs"
        if isinstance(n, (int, float)) and n == n:
            row[key] = list(range(1, int(n) + 1))
        else:
            row.pop(key, None)


def test_defaults_match_legacy_streaks_fails_and_exact_match():
    rows = [
        _beats_row("AAA", max_gap_date=D("2025-11-16"), max_gap_pct=12.0,
                   _earnings_aligned_dates=["2025-11-16"],
                   _display_only_fails={"q2_surprise_eps_pct": True,
                                        "q1_reported_rev": True}),
        _beats_row("BBB", eps_streak=np.nan, rev_streak=3),
        _beats_row("CCC", eps_streak=4, rev_streak=0,
                   _display_only_fails={"q1_yoy_eps_pct": False}),
    ]
    assert_same_as_legacy(pd.DataFrame(rows))


def test_defaults_match_legacy_fuzzy_match_with_canon_map():
    rows = [_beats_row(
        "DDD", surge_start_date=D("2025-11-17"), surge_pct=40.0,
        up_gap_start_date=D("2025-08-18"), consec_gaps=2,
        _earnings_aligned_dates=["2025-08-17", "2025-08-18",
                                 "2025-11-16", "2025-11-17"],
        _earnings_aligned_canon={"2025-11-17": "2025-11-16",
                                 "2025-11-16": "2025-11-16",
                                 "2025-08-18": "2025-08-17",
                                 "2025-08-17": "2025-08-17"})]
    assert_same_as_legacy(pd.DataFrame(rows))


def test_defaults_match_legacy_when_the_earnings_gate_blocks():
    """Two indicator dates match an earnings date that no RENDERED earnings
    cell anchors to — the old gate refuses to colour them."""
    rows = [{"symbol": "EEE", "close": 1.0, "pct_gain": 1.0,
             "gain_start_date": D("2026-01-02"),
             "max_gap_date": D("2026-03-01"), "max_gap_pct": 9.0,
             "surge_start_date": D("2026-03-01"), "surge_pct": 30.0,
             "_earnings_aligned_dates": ["2026-03-01"]}]
    assert_same_as_legacy(pd.DataFrame(rows))


def _accel_rows(n):
    return [{"symbol": f"F{i:03d}", "close": 1.0, "pct_gain": 1.0,
             "gain_start_date": D("2026-01-02"),
             "accel_eps_surp_len": 3, "accel_eps_surp_span": "x",
             "accel_eps_surp_vals": "y",
             "_accel_eps_surp_start_date": D("2025-05-15"),
             "_accel_eps_surp_end_date": D("2025-11-14"),
             "max_gap_date": D("2025-05-15"), "max_gap_pct": 7.0,
             "surge_start_date": D("2025-11-14"), "surge_pct": 30.0,
             "last_report_date": D("2025-11-14"), "reported_eps": 1.2,
             "_earnings_aligned_dates": ["2025-05-15", "2025-11-14"]}
            for i in range(n)]


def test_defaults_match_legacy_accel_span_anchoring_two_seeds():
    rows = _accel_rows(20)
    assert_same_as_legacy(pd.DataFrame(rows))


def test_defaults_match_legacy_when_a_seed_owns_no_cell():
    """The palette-slot edge. An accelerating-series span anchors on BOTH
    ends of its series, so both matched dates pass the earnings gate — but
    the span cells take the END date's colour, and with the gap-date column
    HIDDEN no rendered cell is left anchoring the START date. The old picker
    still drew a colour for the start date first, and whenever the two
    dates' seeds collided that pushed the end date's colour one palette slot
    along. 80 tickers make a collision all but certain (1 in 10 each)."""
    df = pd.DataFrame(_accel_rows(80))
    assert_same_as_legacy(df, hide=("max_gap_date", "max_gap_pct"))


def _random_row(rng, sym):
    n_q = rng.randint(0, 6)
    row = _beats_row(
        sym, n_q=n_q,
        eps_streak=rng.choice([0, 1, 2, 3, 5, np.nan, 2.0]),
        rev_streak=rng.choice([0, 1, 4, np.nan]))
    if n_q == 0:
        for k in ("consec_eps_beats", "consec_rev_beats"):
            row.pop(k)
        _stamp_counted_quarters(row)
    # N/A cells inside and outside streaks, so the v8 amendment is exercised.
    for k in range(1, n_q + 1):
        for side in ("eps", "rev"):
            for field in (f"reported_{side}", f"surprise_{side}_dollar",
                          f"surprise_{side}_pct", f"yoy_{side}_pct"):
                if rng.random() < 0.12:
                    row[f"q{k}_{field}"] = np.nan
    report_dates = [row[f"q{k}_report_date_eps"] for k in range(1, n_q + 1)]
    ind_cols = ["max_gap_date", "min_gap_date", "up_gap_start_date",
                "down_gap_start_date", "surge_start_date", "surge_end_date"]
    dates = {}
    for col in rng.sample(ind_cols, rng.randint(0, 4)):
        if report_dates and rng.random() < 0.7:
            base = rng.choice(report_dates)
        else:
            base = D("2025-01-01")
        dates[col] = base + pd.Timedelta(days=rng.choice([0, 0, 1, -1, 30]))
        row[col] = dates[col]
    for col, val in (("max_gap_pct", 5.0), ("surge_pct", 20.0),
                     ("consec_gaps", 2)):
        if rng.random() < 0.5:
            row[col] = val
    if rng.random() < 0.4:
        row["last_report_date"] = (report_dates[0] if report_dates
                                   else D("2025-06-01"))
        row["reported_eps"] = 1.0
    aligned = set()
    canon = {}
    for col, d in dates.items():
        for r in report_dates:
            if abs((d - r).days) <= 1:
                aligned |= {d.date().isoformat(), r.date().isoformat()}
                canon[d.date().isoformat()] = r.date().isoformat()
                canon[r.date().isoformat()] = r.date().isoformat()
    if aligned:
        row["_earnings_aligned_dates"] = sorted(aligned)
        if any(k != v for k, v in canon.items()):
            row["_earnings_aligned_canon"] = canon
    keys = [k for k in row if not k.startswith("_") and k != "symbol"]
    fails = {k: True for k in rng.sample(keys, rng.randint(0, 3))}
    if fails:
        row["_display_only_fails"] = fails
    return row


def test_defaults_match_legacy_on_a_random_corpus():
    rng = random.Random(20260926)
    rows = [_random_row(rng, f"R{i:03d}{rng.choice('ABCDEFG')}")
            for i in range(400)]
    df = pd.DataFrame(rows)
    assert_same_as_legacy(df)
    # And the corpus really exercises every scheme.
    got = engine_colors(df, W._build_dynamic_columns(df)[0])
    colours = {c for row in got for c in row.values()}
    assert "#4caf50" in colours and "#e74c3c" in colours
    assert colours & set(_OLD_PALETTE)


# ======================================================================
# Engine unit tests
# ======================================================================

def _df():
    return pd.DataFrame([
        {"symbol": "AAA", "rvol": 3.0, "pct_gain": 20.0, "close": 10.0,
         "sma50": 9.0, "index_membership": "S&P 500", "note": None},
        {"symbol": "BBB", "rvol": 1.0, "pct_gain": 5.0, "close": 8.0,
         "sma50": 9.0, "index_membership": "-", "note": "x"},
        {"symbol": "CCC", "rvol": np.nan, "pct_gain": 50.0, "close": 12.0,
         "sma50": np.nan, "index_membership": "RUT", "note": ""},
        {"symbol": "DDD", "rvol": 2.0, "pct_gain": -3.0, "close": 5.0,
         "sma50": 4.0, "index_membership": "NDX, S&P 500", "note": "y"},
    ])


LAYOUT = ["symbol", "rvol", "pct_gain", "close", "sma50", "index_membership"]


def _rule(*conds, match="all", target="matched", cols=(), text="#ff0000",
          bg=None, bold=False, scope="row", rid=None):
    return C.Rule(
        id=rid or C.uuid.uuid4().hex[:8], name="t", match=match, scope=scope,
        conditions=list(conds), target=target, target_columns=list(cols),
        style=C.Style(
            text=(C.ColorSpec("fixed", text) if text else C.ColorSpec()),
            background=(C.ColorSpec("fixed", bg) if bg else C.ColorSpec()),
            bold=bold))


def _hits(styles, key="rvol"):
    return [rs is not None and rs.resolve(key)[0] is not None
            for rs in styles]


@pytest.mark.parametrize("op,value,value2,expected", [
    (">", 1.5, None, [True, False, False, True]),
    (">=", 2.0, None, [True, False, False, True]),
    ("<", 2.0, None, [False, True, False, False]),
    ("==", 1.0, None, [False, True, False, False]),
    ("!=", 1.0, None, [True, False, False, True]),
    ("between", 1.0, 2.0, [False, True, False, True]),
    ("not_between", 1.0, 2.0, [True, False, False, False]),
    ("blank", None, None, [False, False, True, False]),
    ("not_blank", None, None, [True, True, False, True]),
])
def test_value_operators(op, value, value2, expected):
    r = _rule(C.Condition("value", "rvol", op, value, value2))
    assert _hits(C.evaluate(_df(), [r], LAYOUT)) == expected


def test_value_against_another_column():
    r = _rule(C.Condition("value", "close", ">", other="sma50"))
    styles = C.evaluate(_df(), [r], LAYOUT)
    assert _hits(styles, "close") == [True, False, False, True]
    # "matched" paints BOTH operands.
    assert styles[0].resolve("sma50")[0] == "#ff0000"


def test_top_and_bottom_percent_rank_within_the_period():
    top = _rule(C.Condition("value", "pct_gain", "top_pct", 25))
    assert _hits(C.evaluate(_df(), [top], LAYOUT), "pct_gain") == \
        [False, False, True, False]
    bottom = _rule(C.Condition("value", "pct_gain", "bottom_pct", 25))
    assert _hits(C.evaluate(_df(), [bottom], LAYOUT), "pct_gain") == \
        [False, False, False, True]


def test_text_contains():
    r = _rule(C.Condition("value", "index_membership", "contains", "s&p"))
    assert _hits(C.evaluate(_df(), [r], LAYOUT), "index_membership") == \
        [True, False, False, True]


def test_all_versus_any():
    a = C.Condition("value", "rvol", ">=", 2.0)
    b = C.Condition("value", "pct_gain", ">=", 10.0)
    both = C.evaluate(_df(), [_rule(a, b, target="row")], LAYOUT)
    either = C.evaluate(_df(), [_rule(a, b, match="any", target="row")],
                        LAYOUT)
    assert [s is not None for s in both] == [True, False, False, False]
    assert [s is not None for s in either] == [True, False, True, True]


def test_any_paints_only_the_true_conditions_cells():
    a = C.Condition("value", "rvol", ">=", 2.0)
    b = C.Condition("value", "pct_gain", ">=", 10.0)
    styles = C.evaluate(_df(), [_rule(a, b, match="any")], LAYOUT)
    ccc = styles[2]           # rvol blank, pct_gain 50 -> only pct_gain
    assert ccc.resolve("pct_gain")[0] == "#ff0000"
    assert ccc.resolve("rvol")[0] is None


def test_targets_row_and_columns():
    cond = C.Condition("value", "rvol", ">=", 3.0)
    row = C.evaluate(_df(), [_rule(cond, target="row", bg="#112233")], LAYOUT)
    assert all(row[0].resolve(k)[1] == "#112233" for k in LAYOUT)
    cols = C.evaluate(_df(), [_rule(cond, target="columns",
                                    cols=["close", "symbol"])], LAYOUT)
    assert cols[0].resolve("close")[0] == "#ff0000"
    assert cols[0].resolve("symbol")[0] == "#ff0000"
    assert cols[0].resolve("rvol")[0] is None


def test_targets_outside_the_layout_are_ignored():
    cond = C.Condition("value", "rvol", ">=", 3.0)
    styles = C.evaluate(_df(), [_rule(cond, target="columns",
                                      cols=["not_rendered"])], LAYOUT)
    assert styles[0] is None


def test_precedence_is_per_channel_and_top_wins():
    cond = C.Condition("value", "rvol", ">=", 3.0)
    strong = _rule(cond, target="row", text="#aaaaaa")
    weak = _rule(cond, target="columns", cols=["rvol"], text="#bbbbbb",
                 bg="#cccccc", bold=True)
    rs = C.evaluate(_df(), [strong, weak], LAYOUT)[0]
    text, bg, bold = rs.resolve("rvol")
    assert text == "#aaaaaa", "stronger rule wins the text channel"
    assert bg == "#cccccc", "…while the weaker one still owns background"
    assert bold is True
    rs2 = C.evaluate(_df(), [weak, strong], LAYOUT)[0]
    assert rs2.resolve("rvol")[0] == "#bbbbbb", "order decides, not target"


def test_disabled_rules_and_empty_styles_do_nothing():
    cond = C.Condition("value", "rvol", ">=", 0.0)
    off = _rule(cond)
    off.enabled = False
    empty = _rule(cond, text=None)
    report = {}
    styles = C.evaluate(_df(), [off, empty], LAYOUT, report=report)
    assert all(s is None for s in styles)
    assert report[off.id]["inactive"] and report[empty.id]["inactive"]


def test_missing_columns_are_reported_and_inert():
    r = _rule(C.Condition("value", "hv_rank", ">", 50))
    report = {}
    styles = C.evaluate(_df(), [r], LAYOUT, report=report)
    assert all(s is None for s in styles)
    assert report[r.id] == {"rows": 0, "missing": ["hv_rank"]}


def test_a_broken_rule_is_skipped_not_fatal(monkeypatch):
    good = _rule(C.Condition("value", "rvol", ">=", 3.0), rid="good")
    bad = _rule(C.Condition("value", "rvol", ">=", 3.0), rid="bad")

    real = C._apply

    def boom(rule, *a, **k):
        if rule.id == "bad":
            raise RuntimeError("kaboom")
        return real(rule, *a, **k)
    monkeypatch.setattr(C, "_apply", boom)
    report = {}
    styles = C.evaluate(_df(), [good, bad], LAYOUT, report=report)
    assert styles[0].resolve("rvol")[0] == "#ff0000"
    assert "kaboom" in report["bad"]["error"]


def test_random_colour_is_stable_and_distinct_by_group():
    cond = C.Condition("value", "rvol", ">=", 0.0)
    r = _rule(cond, target="row", text=None)
    r.style.text = C.ColorSpec("random")
    first = [s.resolve("rvol")[0] if s else None
             for s in C.evaluate(_df(), [r], LAYOUT)]
    again = [s.resolve("rvol")[0] if s else None
             for s in C.evaluate(_df(), [r], LAYOUT)]
    assert first == again, "stable across renders"
    assert all(c in C.DEFAULT_PALETTE for c in first if c)


def test_random_background_is_salted_away_from_text():
    # A FIXED rule id: the random draw is seeded from it, and with the
    # default uuid id this test failed about one run in eight (3 rows x a
    # 2-colour palette). 30 tickers make the property itself unmistakable.
    df = pd.DataFrame({"symbol": [f"T{i:02d}" for i in range(30)],
                       "rvol": [1.0] * 30})
    cond = C.Condition("value", "rvol", ">=", 0.0)
    r = _rule(cond, target="row", text=None, rid="salted")
    r.style.text = C.ColorSpec("random", palette=["#111111", "#222222"])
    r.style.background = C.ColorSpec("random", palette=["#111111", "#222222"])
    styles = C.evaluate(df, [r], ["symbol", "rvol"])
    pairs = [(s.resolve("rvol")[0], s.resolve("rvol")[1]) for s in styles]
    # Different seeds for the two channels — at least one row must draw
    # different entries (identical seeds could not).
    assert any(t != b for t, b in pairs)


def test_custom_palette_is_honoured():
    cond = C.Condition("value", "rvol", ">=", 0.0)
    r = _rule(cond, target="row", text=None)
    r.style.text = C.ColorSpec("random", palette=["#010203"])
    styles = C.evaluate(_df(), [r], LAYOUT)
    assert {s.resolve("rvol")[0] for s in styles if s} == {"#010203"}


# --- filter conditions -------------------------------------------------

def _fail_df():
    return pd.DataFrame([
        {"symbol": "A", "rvol": 1.0, "hv": 5.0,
         "_display_only_fails": {"rvol": True, "hv": True}},
        {"symbol": "B", "rvol": 3.0, "hv": 50.0, "_display_only_fails": None},
        {"symbol": "C", "rvol": np.nan, "hv": 7.0,
         "_display_only_fails": {"hv": True}},
    ])


def test_filter_fails_any_and_specific():
    layout = ["symbol", "rvol", "hv"]
    anyf = C.evaluate(_fail_df(), [_rule(C.Condition(
        "filter", C.ANY_FILTER, "fails"))], layout)
    assert anyf[0].resolve("rvol")[0] and anyf[0].resolve("hv")[0]
    assert anyf[1] is None
    one = C.evaluate(_fail_df(), [_rule(C.Condition(
        "filter", "rvol", "fails"))], layout)
    assert [s is not None for s in one] == [True, False, False]


def test_filter_passes():
    layout = ["symbol", "rvol", "hv"]
    styles = C.evaluate(_fail_df(), [_rule(C.Condition(
        "filter", "rvol", "passes"))], layout)
    assert [s is not None for s in styles] == [False, True, False], \
        "passes needs a value that is not failing"


# --- date conditions ---------------------------------------------------

def _date_df():
    return pd.DataFrame([
        {"symbol": "A", "max_gap_date": D("2026-02-16"), "max_gap_pct": 9.0,
         "q1_report_date_eps": D("2026-02-15"), "q1_reported_eps": 1.0,
         "q2_report_date_eps": D("2025-11-15"), "q2_reported_eps": 0.8},
        {"symbol": "B", "max_gap_date": D("2026-01-01"), "max_gap_pct": 5.0,
         "q1_report_date_eps": D("2026-02-15"), "q1_reported_eps": 1.0,
         "q2_report_date_eps": D("2025-11-15"), "q2_reported_eps": 0.8},
    ])


DLAYOUT = ["symbol", "max_gap_date", "max_gap_pct", "q1_report_date_eps",
           "q1_reported_eps", "q2_report_date_eps", "q2_reported_eps"]


def test_date_within_any_report_date_with_linked_cells():
    cond = C.Condition("date", "max_gap_date", "within",
                       other=C.ANY_REPORT_DATE, days=1)
    r = _rule(cond)
    r.expand_units = True
    styles = C.evaluate(_date_df(), [r], DLAYOUT)
    a = styles[0]
    for key in ("max_gap_date", "max_gap_pct", "q1_report_date_eps",
                "q1_reported_eps"):
        assert a.resolve(key)[0] == "#ff0000", key
    assert a.resolve("q2_reported_eps")[0] is None
    assert styles[1] is None


def test_date_exact_tolerance_zero_misses_a_one_day_gap():
    cond = C.Condition("date", "max_gap_date", "within",
                       other=C.ANY_REPORT_DATE, days=0)
    assert all(s is None for s in C.evaluate(_date_df(), [_rule(cond)],
                                             DLAYOUT))


def test_date_before_and_after():
    before = C.Condition("date", "max_gap_date", "before",
                         other="q1_report_date_eps", days=30)
    styles = C.evaluate(_date_df(), [_rule(before)], DLAYOUT)
    assert [s is not None for s in styles] == [False, True]
    after = C.Condition("date", "max_gap_date", "after",
                        other="q2_report_date_eps", days=1)
    styles = C.evaluate(_date_df(), [_rule(after)], DLAYOUT)
    assert [s is not None for s in styles] == [True, True]


def test_date_pairs_share_one_random_colour():
    cond = C.Condition("date", "max_gap_date", "within",
                       other=C.ANY_REPORT_DATE, days=1)
    r = _rule(cond, text=None)
    r.style.text = C.ColorSpec("random")
    rs = C.evaluate(_date_df(), [r], DLAYOUT)[0]
    assert rs.resolve("max_gap_date")[0] == rs.resolve("q1_report_date_eps")[0]


# --- quarter scope -----------------------------------------------------

def _q_df():
    row = {"symbol": "Q", "consec_eps_beats": 2}
    for k in range(1, 5):
        row[f"q{k}_surprise_eps_pct"] = [60.0, 10.0, 80.0, 5.0][k - 1]
        row[f"q{k}_reported_eps"] = 1.0
    return pd.DataFrame([row])


QLAYOUT = ["symbol"] + [f"q{k}_{s}" for k in range(1, 5)
                        for s in ("surprise_eps_pct", "reported_eps")]


def test_quarter_scope_value_with_template_paints_matching_quarters():
    r = _rule(C.Condition("value", "q{k}_surprise_eps_pct", ">=", 50),
              scope="quarter")
    rs = C.evaluate(_q_df(), [r], QLAYOUT)[0]
    painted = [k for k in range(1, 5)
               if rs.resolve(f"q{k}_surprise_eps_pct")[0]]
    assert painted == [1, 3]
    assert rs.resolve("q1_reported_eps")[0] is None, "matched = its own cell"


def test_quarter_scope_type_target_expands_per_quarter():
    r = _rule(C.Condition("quarter", op="<=", other="consec_eps_beats"),
              scope="quarter", target="columns", cols=["q_reported_eps"])
    rs = C.evaluate(_q_df(), [r], QLAYOUT)[0]
    assert [k for k in range(1, 5) if rs.resolve(f"q{k}_reported_eps")[0]] \
        == [1, 2]


def test_row_scope_quarter_type_target_means_every_quarter():
    r = _rule(C.Condition("value", "consec_eps_beats", ">=", 2),
              target="columns", cols=["q_reported_eps"])
    rs = C.evaluate(_q_df(), [r], QLAYOUT + ["consec_eps_beats"])[0]
    assert all(rs.resolve(f"q{k}_reported_eps")[0] for k in range(1, 5))


def test_quarter_rows_matched_is_reported():
    r = _rule(C.Condition("value", "q{k}_surprise_eps_pct", ">=", 50),
              scope="quarter")
    report = {}
    C.evaluate(_q_df(), [r], QLAYOUT, report=report)
    assert report[r.id]["rows"] == 1


# --- serialisation -----------------------------------------------------

def test_round_trip_is_lossless():
    rules = C.default_rules() + [_rule(
        C.Condition("value", "rvol", "between", 1.0, 2.0),
        C.Condition("date", "max_gap_date", "within",
                    other=C.ANY_REPORT_DATE, days=2),
        match="any", target="columns", cols=["rvol"], bg="#010101",
        bold=True)]
    back = C.rules_from_json(C.rules_to_json(rules))
    assert [r.to_dict() for r in back] == [r.to_dict() for r in rules]


def test_tolerant_load_drops_junk_and_clamps_fields():
    data = {"version": 2, "rules": [
        "not a rule",
        {"name": "ok", "scope": "sideways", "match": "most",
         "target": "everything",
         "conditions": [{"kind": "value", "column": "rvol", "op": "~",
                         "value": "nan"},
                        {"kind": "telepathy"}],
         "style": {"text": {"mode": "fixed", "color": "red"},
                   "background": {"mode": "random", "palette": ["#zzz"]}}},
    ]}
    rules = C.rules_from_json(data)
    assert len(rules) == 1
    r = rules[0]
    assert (r.scope, r.match, r.target) == ("row", "all", "matched")
    assert len(r.conditions) == 1 and r.conditions[0].op == ">"
    assert r.style.text.color == "#ffffff"
    assert r.style.background.palette == list(C.DEFAULT_PALETTE)


def test_unreadable_payload_yields_defaults():
    assert [r.id for r in C.rules_from_json(None)] == \
        [r.id for r in C.default_rules()]
