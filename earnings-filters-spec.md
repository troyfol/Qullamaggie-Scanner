# Spec: Consecutive Growth & Accelerating Quarters Filters

Implementation instructions for six new earnings filters. Read the whole spec before writing code — Part 3 (Shared Rules) governs both parts and is where most of the edge-case behavior lives.

---

## Part 1 — Consecutive YoY Growth Filters (2 new filters)

Add two filters that mirror the existing **Consecutive Beats** filters in structure, parameters, and UI, but measure YoY growth instead of surprise:

1. **Consecutive YoY EPS Growth**
2. **Consecutive YoY Revenue Growth**

### Parameters (identical shape to the existing Consecutive Beats filters)

| Parameter | Type | Description |
|---|---|---|
| Minimum Growth Threshold | % (may be negative) | A quarter qualifies if its YoY growth ≥ this value. |
| Minimum Consecutive Quarters | integer ≥ 1 | Length of the required qualifying streak. |
| Quarter Cap | integer ≥ 1 | Max quarters to look back from the scan start date. Same semantics as the existing Consecutive Beats quarter cap — reuse that implementation, do not reinvent it. |

### Logic

Walk the quarter series within the cap window. A quarter is a **hit** if `YoY_growth >= Minimum Growth Threshold`. The stock passes if there exists a run of hits with length ≥ Minimum Consecutive Quarters, subject to the gap and sign rules in Part 3.

These two filters have **no** starting-threshold, series-selection, or Backward Only parameters. They are a straight port of Consecutive Beats onto the YoY metrics.

---

## Part 2 — Consecutive Accelerating Quarters Filters (4 new filters)

Add four filters, one per metric:

1. **Consecutive Accelerating Quarters — EPS Surprise**
2. **Consecutive Accelerating Quarters — Revenue Surprise**
3. **Consecutive Accelerating Quarters — YoY EPS Growth**
4. **Consecutive Accelerating Quarters — YoY Revenue Growth**

All four share identical logic. Only the underlying metric differs. Implement the logic **once**, parameterized by metric, and instantiate it four times.

### Terminology

- `V(q)` = the metric value for quarter `q`, expressed in percent (e.g. a 12.5% YoY EPS gain is `V = 12.5`).
- A **series** is an ordered run of consecutive quarters (oldest → newest) where each step-up meets the acceleration rule.
- **Acceleration is measured in percentage points, not relative change.** `V = 20` → `V = 25` is a move of **+5**, not +25%. This applies everywhere in this spec.

### Parameters

| Parameter | Type | Description |
|---|---|---|
| Minimum Starting % Threshold | % (may be negative) | The **first (oldest) quarter of the series** must satisfy `V(start) >= this`. Applies only to the first quarter; later quarters in the series are unconstrained in absolute terms and governed only by the growth threshold. |
| Minimum % Growth Threshold | percentage points, ≥ 0 | Every step must satisfy `V(n) - V(n-1) >= this`. |
| Minimum Series Count | integer ≥ 2 | Minimum **number of quarters** in the series, **inclusive of the starting quarter**. A count of 3 means 3 quarters and therefore 2 qualifying steps. |
| Quarter Cap | integer ≥ 1 | Max quarters to look back from the scan start date. Same semantics as the existing filters' quarter cap. |
| Series Selection | enum: `Longest` \| `Most Recent` | Which qualifying series to return when more than one exists. Disabled (greyed out) when Backward Only is on. |
| Backward Only | boolean, default off | See below. Overrides and greys out Series Selection. |

### Series construction

The two thresholds constrain different things and must be applied in this order: the **growth threshold governs every step**, while the **starting threshold governs only the first quarter**. They can disagree about where a series begins, so build each series in two passes:

1. **Extend by the chain rules.** From each terminal quarter, extend backward as far as the growth threshold, sign rule, and gap rule permit. This produces one raw chain per terminal quarter.
2. **Locate the start point.** Walking from the oldest end of the chain forward, the series begins at the **first quarter satisfying `V >= Minimum Starting % Threshold`**. Quarters before that point are not part of the series and do not count toward Minimum Series Count. If no quarter in the chain satisfies the threshold, that chain yields no valid series.

A chain whose oldest quarters sit below the starting threshold is **not** disqualified — the series simply begins later, at the first quarter that clears it. This is deliberate: it catches stocks accelerating up *through* the threshold from below. See T7.

Do not emit a sub-series of a longer qualifying series as a separate candidate. Because pass 1 extends maximally and pass 2 resolves deterministically, **each terminal quarter produces at most one valid series.**

A series is valid if **all** of the following hold:

- `V(start) >= Minimum Starting % Threshold`
- For every consecutive pair in the series: `V(n) - V(n-1) >= Minimum % Growth Threshold`
- No step violates the sign rule (Part 3.2)
- No step spans a disqualifying data gap (Part 3.1)
- Quarter count ≥ Minimum Series Count

If no valid series exists in the window, the stock fails the filter.

### Series Selection — `Longest` (default)

Scan the entire quarter pool. Return the valid series with the **greatest quarter count**. Ties are possible here — two series of equal length ending on different quarters. Tie-break: the series whose most recent quarter is **newest** wins.

### Series Selection — `Most Recent`

Return the valid series whose **most recent quarter is newest**. No tie-break is needed: each terminal quarter yields at most one valid series, so ranking by terminal quarter is already unique.

**Important:** `Most Recent` ranks by recency; it does **not** require the series to terminate on the most recent quarter in the scan period. A series ending three quarters ago still qualifies if it is the newest-ending valid series in the pool. This is the distinction from Backward Only.

### Backward Only mode

When on:

- Greys out / disables Series Selection. Selection is not applicable.
- **Anchor** = the most recent quarter in the scan period that has data for the metric.
- The anchor is the **obligatory final (newest) quarter of the series**. Build the series using the anchor as the terminal quarter, applying the same two-pass construction as above (extend backward by the chain rules, then locate the start point via the starting threshold).
- If the resulting series has fewer quarters than Minimum Series Count, or no chain can be formed at the anchor at all — **the stock fails the filter.** Do **not** step the anchor back to an earlier quarter to look for a different series.

This is a strict "accelerating right now" mode. The absence of a fallback is intentional.

---

## Part 3 — Shared Rules (apply to all six filters)

### 3.1 Missing / empty periods

- **A single missing period may be skipped.** Bridge across it and compare the two quarters on either side directly. Example: `Q1 = 10, Q2 = missing, Q3 = 20` with a growth threshold of 5 → the step `10 → 20` is `+10`, which qualifies.
- **Two or more consecutive missing periods break the chain.** No bridging.
- A skipped period does **not** count toward Minimum Series Count / Minimum Consecutive Quarters. Only quarters with actual data are counted.

### 3.2 Sign rule

A step where the metric value moves from **negative to positive breaks the chain**, regardless of the size of the move. `V = -5` → `V = +10` is a +15pp move but does **not** qualify — it is a swing out of contraction, not acceleration within a trend.

Negative-to-less-negative progressions **do** qualify normally. With Minimum Starting % Threshold = −50 and Minimum % Growth Threshold = 5, the sequence `-45, -40, -35` is a valid 3-quarter series.

**Assumption to confirm:** zero is treated as neither positive nor negative, so `-5 → 0` and `0 → +5` are both permitted; only a strict `V(n-1) < 0` paired with `V(n) > 0` breaks the chain. Flag this in the PR if you disagree.

### 3.3 Comparison operators

All thresholds are inclusive (`>=`). A value exactly equal to the threshold passes.

### 3.4 Quarter cap semantics

The quarter cap defines the pool of quarters included in the scan. All series are built from that pool and nothing outside it is reachable — no additional boundary check is required.

Reuse the existing quarter-cap implementation verbatim — same counting basis, same treatment of the scan start date. Do not introduce a second convention.

### 3.5 Filter output

Each Accelerating Quarters filter should expose, for the returned series:

- Series length (quarter count)
- Start quarter and end quarter
- `V(start)` and `V(end)`

so results are auditable in the scan output.

---

## Part 4 — Acceptance Tests

Values are oldest → newest. Implement these as unit tests.

### T1 — Longest vs. Most Recent diverge
`Q1=8, Q2=12, Q3=18, Q4=25, Q5=33, Q6=30, Q7=36, Q8=42`
Params: Start ≥ 10, Growth ≥ 4, Count ≥ 3, cap covers all.

- Valid series A: `Q2–Q5` (12→18→25→33), 4 quarters.
- Valid series B: `Q6–Q8` (30→36→42), 3 quarters. (`Q5→Q6` is −3, breaks.)
- **Longest** → returns A (Q2–Q5).
- **Most Recent** → returns B (Q6–Q8).
- **Backward Only** → anchor Q8, walks back to Q6, stops at `Q6→Q5`. Returns Q6–Q8. Passes.

### T2 — Negative progression qualifies
`-45, -40, -35`. Start ≥ −50, Growth ≥ 5, Count ≥ 3. → **Pass**, 3-quarter series.

### T3 — Sign flip breaks
`-8, -3, +4`. Start ≥ −10, Growth ≥ 4, Count ≥ 3.
`-8 → -3` is +5, qualifies. `-3 → +4` is +7 but is a negative-to-positive flip → breaks. Longest valid series is 2 quarters. → **Fail**.

### T4 — Single gap bridges
`Q1=10, Q2=missing, Q3=20, Q4=26`. Start ≥ 5, Growth ≥ 5, Count ≥ 3.
Bridges Q2. Steps: `10→20` (+10), `20→26` (+6). Series is 3 quarters (Q1, Q3, Q4). → **Pass**.

### T5 — Double gap breaks
`Q1=10, Q2=missing, Q3=missing, Q4=20`. Same params. → **Fail** (no series ≥ 3).

### T6 — Backward Only strict anchor
`Q1=10, Q2=16, Q3=22, Q4=28, Q5=25`. Start ≥ 5, Growth ≥ 5, Count ≥ 3.
Anchor is Q5. `Q4→Q5` is −3 → no series terminates at the anchor. → **Fail**, despite a valid 4-quarter series at Q1–Q4. Confirms no anchor fallback.

### T7 — Series accelerates up through the starting threshold (PASS case)
`Q1=2, Q2=9, Q3=16, Q4=23, Q5=30`. Start ≥ 15, Growth ≥ 5, Count ≥ 3.
Pass 1 builds an unbroken chain `Q1–Q5` — every step is +7. Pass 2 finds the first quarter clearing 15%: that is Q3. Series is `Q3–Q5`, 3 quarters. → **Pass**.
This is the key test. An implementation that rejects the chain because its oldest quarter (2) is below 15 will wrongly fail this stock.

### T7b — Same rule, insufficient length (FAIL case)
`Q1=2, Q2=9, Q3=16`. Start ≥ 15, Growth ≥ 5, Count ≥ 3.
Chain is `Q1–Q3`; the series begins at Q3 and is 1 quarter long. → **Fail**.
Confirms that quarters below the starting threshold do not count toward Minimum Series Count.

### T8 — Percentage points, not relative
`Q1=20, Q2=25`. Growth ≥ 5. → the step qualifies (`+5` points). Confirms the relative-change interpretation (which would require 21) is not implemented.

---

## Part 5 — Open Items for the Implementer

Flag these in the PR rather than deciding silently:

1. The zero-handling assumption in §3.2.
2. Whether Minimum Series Count should have an enforced lower bound of 2 (a "series" of 1 quarter has no acceleration step and is meaningless).
3. Whether the four Accelerating filters should share a single UI component with a metric selector, or ship as four discrete filter entries in the filter list — match whatever pattern the existing beats filters use.
