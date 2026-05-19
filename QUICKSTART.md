# Quick Start — Trading Scanner (Finnhub Fork)

A practical, do-this-then-that guide for getting a fresh install of
`Trade_Scanner_FH.exe` from first launch to a useful scan.

---

## TL;DR — the order of operations

| # | Step | Where | Needed for |
|---|------|-------|-----------|
| 1 | Launch the exe; answer (or skip) the two credential prompts | First-launch dialogs | Finnhub + Zacks features |
| 2 | Let the **universe download** + **OHLCV download** finish | Automatic, background | Any scan at all |
| 3 | (Optional) Set the **SEC contact email**, then Force Universe Refresh | Settings menu → Data menu | SEC EDGAR universe source |
| 4 | **Bulk Fill Sector Map** | Data menu | Relative-strength / sector-ETF filters |
| 5 | Fill **earnings dates** (Nasdaq) and **earnings history** (Zacks / Finnhub) | Data menu | Any earnings filter |
| 6 | Configure the indicator panel and **Run Scan** | Main window | Results |

Steps 3–5 are optional and depend on which filters you intend to use. The
scanner runs a price/technical scan with only steps 1–2 done.

---

## 1. First launch

Run `Trade_Scanner_FH.exe`. Before the main window appears, two **optional**
credential prompts show in sequence:

1. **Finnhub API key** — single-line password field. Get a free key at
   <https://finnhub.io/dashboard> (free tier = 60 req/min). Stored in the
   Windows Credential Manager, never in plaintext on disk. Leave blank /
   Cancel to skip.
2. **Zacks cookies** — multi-line paste field. Enables the Zacks earnings
   scraper. Leave blank / Cancel to skip.

You can skip both and set them later (see [§3](#3-credentials--what-each-unlocks)).
The scanner is fully usable for price/technical scans without either.

Once the window opens, two things start **automatically in the background**:

- **Universe download** — the full US equity ticker list (NASDAQ FTP +
  GitHub; SEC EDGAR is included only if a contact email is configured —
  see [§3](#3-optional-sec-contact-email)). Written to
  `scanner_data/universe.csv`. On later launches this only re-runs if the
  snapshot is older than 7 days.
- **OHLCV download** — up to 5 years of daily bars per ticker from
  yfinance into `scanner_data/ohlcv/`. **This is the longest first-run
  step** — thousands of rate-limited downloads. Watch the OHLCV status
  label in the toolbar; it turns green when complete.

You can start scanning with whatever OHLCV has cached so far, but full
universe coverage needs this download to finish.

---

## 2. Credentials — what each unlocks

The scanner has three independent credentials. None is required to launch;
each one unlocks a specific subsystem.

| Credential | Set via | Stored in | Unlocks |
|-----------|---------|-----------|---------|
| **Finnhub API key** | First-launch prompt, or `Data → Set Finnhub API Key…` | Windows Credential Manager (keyring) | Finnhub earnings history fills, best-quality sector map |
| **Zacks cookies** | First-launch prompt, or `Data → Set Zacks Cookies…` / `Refresh Zacks Cookies (Open Browser)…` | `scanner_data/zacks_cookies.txt` | Zacks per-quarter earnings history (primary earnings source) |
| **SEC contact email** | `Settings → Set SEC Contact Email…` | `scanner_data/sec_contact.txt` (or `SEC_CONTACT_EMAIL` env var) | SEC EDGAR ticker-universe source |

### Getting Zacks cookies

Open <https://zacks.com> in a logged-in browser → DevTools → Application →
Cookies → copy the full `key=value; key=value; …` header string → paste it
into the dialog. When Zacks' Imperva layer later starts blocking, use
`Data → Refresh Zacks Cookies (Open Browser)…` — it launches a managed
Firefox profile, you solve any challenge, and fresh cookies are captured
automatically (even mid-session).

---

## 3. (Optional) SEC contact email

SEC EDGAR's fair-access policy requires a real contact email in the request
User-Agent. Without one configured, the scanner **skips the SEC source** and
builds the universe from NASDAQ FTP + GitHub only — which is already
comprehensive, so this step is genuinely optional.

To include the SEC source:

1. `Settings → Set SEC Contact Email…` → enter a real email address.
2. `Data → Force Universe Refresh` to re-pull the universe with SEC
   included.

> To have the SEC source active from the very first launch, set the
> `SEC_CONTACT_EMAIL` environment variable before starting the exe — the
> first-run universe download will then pick it up automatically.

The email is read file-first, then env var, then a non-functional
placeholder. It is never committed to source.

---

## 4. Build the sector map

`Data → Bulk Fill Sector Map` maps each ticker to its sector and the
corresponding SPDR sector ETF (used by relative-strength filters). It pulls
from Finnhub `/stock/profile2` when a key is set, falling back to
FinanceDatabase and yfinance otherwise. Output: `scanner_data/sector_map.parquet`.

Use `Data → Targeted Fill Sector Map` afterwards to fill only tickers still
missing a mapping.

---

## 5. Fill earnings data (only if you use earnings filters)

Earnings come from four independent sources writing to two parquet files.
Fill only what your filters need.

### Earnings dates (last / next report date)

| Action | Source | Notes |
|--------|--------|-------|
| `Data → Bulk Fill Earnings Dates (Nasdaq)` | Nasdaq calendar | Fast; ±90-day window. Best first step. |
| `Data → Targeted Fill Earnings Dates (Yahoo)` | yfinance | Gap-fills tickers Nasdaq missed. |

A once-per-week Nasdaq calendar sweep also auto-fires at launch (toggle:
`Data → Auto-refresh Nasdaq calendar weekly`).

### Earnings history (per-quarter EPS / revenue)

| Action | Source | Notes |
|--------|--------|-------|
| `Data → Bulk Fill Earnings (Zacks)` | Zacks scraper | **Primary source.** ~6.5 h for the full universe; needs Zacks cookies. |
| `Data → Bulk Fill Earnings (Finnhub)` | Finnhub | Needs a Finnhub key. Resumable. Fills only tickers Zacks doesn't cover. |
| `Data → Targeted / Gap Fill …` | either | Fills just the tickers with no rows yet — much faster than a bulk run. |

Zacks wins at the ticker level: any ticker with Zacks rows drops all its
Finnhub rows. Each fill auto-reconciles affected tickers. After a manual
multi-source fill you can also run `Data → Reconcile earnings_dates.parquet`.

### Check coverage

`Data → Diagnostics → Earnings Coverage Report` shows the zacks-only /
finnhub-only / both / neither breakdown. `Verify earnings_history Integrity`
runs schema/policy checks with one-click auto-fix.

---

## 6. Run your first scan

1. In the **indicator panel** (left), enable filters. Each row is a
   3-state control:
   - **Off** — not computed, no column.
   - **Filter** (checkbox) — computed, rows below threshold are dropped.
   - **Display Only** (checkbox) — computed and shown, but *not* filtered;
     cells render **red** when the value would have failed the threshold.

   The threshold input is always editable in both modes.
2. Pick a **Timeframe** in the toolbar (or set a custom date range /
   sequenced run).
3. Click **Run Scan**.
4. Results render in the table. Use the **view-only checkboxes** next to
   the Timeframe dropdown (Earnings Dates / Earnings Data / Color Match
   Only) to filter what's *displayed* without re-scanning.

Useful extras once you have results:

- **Columns ▾** toolbar button — reorder / hide columns; drag rows in the
  popup. Layout saves into presets.
- Right-click rows or column headers to delete, cut, or paste.
- **Excel** button — export the active (or all) periods to XLSX/CSV, with
  optional cell coloring.
- **Save Preset** — stores the full filter + window + column layout to
  `scanner_data/presets/{name}.json`.

---

## 7. Where your data lives

Everything persistent sits in `scanner_data/` next to the exe. This folder
is **sacred** — no rebuild, update, or migration ever touches it.

```
scanner_data/
  ohlcv/*.parquet            per-ticker daily OHLCV (5 yr)
  universe.csv               ticker universe snapshot
  sector_map.parquet         ticker → sector ETF
  earnings_dates.parquet     last / next earnings dates
  earnings_history.parquet   per-quarter EPS + revenue
  zacks_cookies.txt          Zacks session tokens
  sec_contact.txt            SEC EDGAR contact email
  presets/                   saved scan configs
  logs/                      per-session diagnostic logs
```

The Finnhub API key lives in the Windows Credential Manager, not in this
folder. GUI preferences (window geometry, cookie-browser monitor) live in
the registry via QSettings.

---

## 8. Troubleshooting quick reference

| Symptom | Fix |
|---------|-----|
| Scan returns few/no rows | OHLCV download not finished — wait for the toolbar status to go green, or `Data → Download Missing Tickers Only`. |
| Universe missing SEC tickers | No SEC contact email — see [§3](#3-optional-sec-contact-email). |
| Earnings filters all blank | Earnings parquets not filled — see [§5](#5-fill-earnings-data-only-if-you-use-earnings-filters). |
| Zacks fill keeps pausing | Imperva is blocking — `Data → Refresh Zacks Cookies (Open Browser)…`. |
| Prices look wrong (e.g. post-split) | `Data → Rebuild Tickers…` for the affected symbols. |
| Need to re-pull the universe now | `Data → Force Universe Refresh` (ignores the 7-day staleness guard). |

Per-session logs are written under `scanner_data/logs/` (`scan_*`,
`ohlcv_*`, `universe_*`, `bridge_*`) — check these first when something
misbehaves.

---

> **Disclaimer.** This software is for informational and educational
> purposes only and is not financial advice. Data from third-party
> providers may be inaccurate or incomplete. Use at your own risk. See the
> Disclaimer section of [README.md](README.md) for the full text.
