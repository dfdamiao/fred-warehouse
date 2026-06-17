# FRED Series Catalog

This tool ships with a curated catalog of **2,186 FRED series** worth tracking,
organized by release frequency. You don't have to use it — you can download any
FRED series by ID — but it's a strong, opinionated starting set.

There are two files:

- **[`series_catalog.csv`](series_catalog.csv)** — every recommended series with
  its `series_id`, `title`, `frequency`, and `units`. Open it in a spreadsheet,
  or `grep`/sort it to find what you want before downloading.
- **`fred/recommended_series.py`** — the same lists, as importable Python
  (`from fred.recommended_series import ESSENTIAL_SERIES, DAILY_SERIES, ...`).

*****************************************************************************

## Buckets

| List | Count | What it is |
|---|---:|---|
| `ESSENTIAL_SERIES` | 10 | The 10 headline macro indicators (see below) |
| `DAILY_SERIES` | 644 | Market-sensitive daily data (yields, spreads, FX, commodities) |
| `WEEKLY_SERIES` | 130 | Mortgage rates, bank balance sheets, weekly treasuries |
| `MONTHLY_SERIES` | 1,297 | Employment, CPI, housing, production, sentiment |
| `QUARTERLY_SERIES` | 647 | GDP components and quarterly aggregates |
| `ANNUAL_SERIES` | 681 | Annual / structural series |
| `QUICK_UPDATE` | 440 | The fast-moving subset worth polling often |
| `ALL_RECOMMENDED` | 2,186 | Union of the above (after de-duplication) |

`TREASURY_SERIES` (4) and `FX_SERIES` (3) are small convenience subsets.

*****************************************************************************

## The 10 essentials

A fast way to get a feel for the data — `python -m examples.quickstart` builds a
database with exactly these:

| Series ID | Title |
|---|---|
| `T10Y2Y` | 10-Year minus 2-Year Treasury (yield-curve slope) |
| `T10Y3M` | 10-Year minus 3-Month Treasury |
| `T10YIE` | 10-Year Breakeven Inflation Rate |
| `UNRATE` | Unemployment Rate |
| `CIVPART` | Labor Force Participation Rate |
| `M2SL` | M2 Money Stock |
| `MORTGAGE30US` | 30-Year Fixed Mortgage Average |
| `MSPUS` | Median Sales Price of Houses Sold |
| `CSUSHPINSA` | Case-Shiller U.S. National Home Price Index |
| `DRCCLACBS` | Credit-Card Delinquency Rate, All Commercial Banks |

*****************************************************************************

## Picking series

- Browse `series_catalog.csv` (titles + frequency + units) and copy the IDs you
  want.
- Download a specific set:
  `python -m fred.update_series FEDFUNDS GDP UNRATE` (downloads if missing).
- Download by popularity: `python -m fred.download_popular_fred_series --top-n 500`.
- Or import a list in code:
  `from fred.recommended_series import MONTHLY_SERIES`.

Each series is stored with all **9 FRED transformations** (levels, change,
percent change, year-over-year, log, …) so you can query whichever form you need.
