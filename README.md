# fred-warehouse

Build your own local **FRED economic-data warehouse**. Point it at your free
[FRED API key](https://fred.stlouisfed.org/docs/api/api_key.html), choose where
the database lives, and it downloads, stores, and keeps up to date thousands of
U.S. economic time series in a single [DuckDB](https://duckdb.org) file you can
query with SQL, pandas, or export to CSV/Parquet.

Ships with a curated **2,186-series catalog** and full metadata, so it's useful
the moment you clone it.

*****************************************************************************

## What is FRED?

[FRED](https://fred.stlouisfed.org) (Federal Reserve Economic Data, St. Louis
Fed) is the canonical free source for U.S. macro data — interest rates, GDP,
inflation, employment, housing, money supply, and ~800,000 more series. This
tool turns the FRED API into a fast local database you own.

## Features

- **One config file** for your API key + database location — put the DB anywhere.
- **Curated catalog** of 2,186 series by frequency, with titles/units in
  [`series_catalog.csv`](series_catalog.csv) (see [SERIES_CATALOG.md](SERIES_CATALOG.md)).
- **Add any series by ID** — `python -m fred.add_series FEDFUNDS GDP`.
- **9 FRED transformations** per series (levels, change, % change, YoY, log, …).
- **Multi-key, rate-limited client** with automatic round-robin + backoff.
- **Resumable bulk download** and **incremental, frequency-aware updates**.
- **Query** as pandas, aligned multi-series panels, latest values, or export
  to CSV/Parquet.

*****************************************************************************

## Install

```bash
git clone <this-repo> fred-warehouse
cd fred-warehouse
pip install -r requirements.txt
```

## Get a free API key

Request one (instant, free) at
https://fred.stlouisfed.org/docs/api/api_key.html.

## Configure

```bash
cp config.example.json config.json
```

Edit `config.json` — paste your key and set the database path to **wherever you
want it**:

```json
{
  "fred_api": {
    "api_key_primary": "abcdef0123456789abcdef0123456789",
    "api_key_secondary": "",
    "api_key_tertiary": "",
    "base_url": "https://api.stlouisfed.org/fred"
  },
  "database": {
    "duckdb_path": "/where/you/want/fred_data.db"
  }
}
```

`config.json` is git-ignored. Extra keys are optional (they raise your effective
rate limit). You can also override either value with the `FRED_API_KEY_PRIMARY`
and `FRED_DB_PATH` environment variables.

*****************************************************************************

## Quick start

Build a small database with the 10 headline indicators:

```bash
python -m examples.quickstart
```

Then grow it:

```bash
# add specific series by ID
python -m fred.add_series FEDFUNDS GDP UNRATE

# add a series with all 9 transformations (M2SL_lin, M2SL_pch, ...)
python -m fred.add_series M2SL --transforms

# bulk-download the most popular series
python -m fred.download_popular_fred_series --top-n 500 --workers 3
```

## The series catalog

You don't need to know FRED IDs by heart. Browse
[`series_catalog.csv`](series_catalog.csv) (`series_id, title, frequency,
units`) or import the lists in code:

```python
from fred.recommended_series import ESSENTIAL_SERIES, DAILY_SERIES, MONTHLY_SERIES
```

See [SERIES_CATALOG.md](SERIES_CATALOG.md) for the buckets and the 10 essentials.

*****************************************************************************

## Retrieving data (several formats)

```python
from fred import FREDDataAccess

fred = FREDDataAccess()

# 1) one series as a pandas DataFrame
unrate = fred.get_series_data("UNRATE", period="1y")

# 2) several series aligned on one date index (forward-filled)
panel = fred.get_multiple_series(["UNRATE", "M2SL", "T10Y2Y"], period="2y")

# 3) just the latest value of each
latest = fred.get_latest_values(["UNRATE", "M2SL", "T10Y2Y"])

# 4) export
fred.export_to_csv(["UNRATE", "M2SL"], "macro.csv", period="5y")
fred.export_to_parquet(["UNRATE", "M2SL"], "macro.parquet", period="5y")

# 5) a transformation (year-over-year %), if you downloaded transforms
m2_yoy = fred.get_series_data("M2SL_pc1", period="2y")
```

`period` accepts `1mo | 3mo | 6mo | 1y | 2y | 5y | ytd | max` (or pass explicit
`start_date` / `end_date`). A runnable version of the above is in
[`examples/retrieve_data.py`](examples/retrieve_data.py):

```bash
python -m examples.retrieve_data
```

*****************************************************************************

## Database schema

A single DuckDB file with two tables:

**`series_metadata`** — one row per series:
`series_id, title, units, frequency, seasonal_adjustment, observation_start,
observation_end, last_updated, popularity, notes`

**`series_data`** — one row per (series, date):
`series_id, date, value` (primary key `(series_id, date)`).

## CLI reference

All commands run as modules from the repo root:

| Command | What it does |
|---|---|
| `python -m examples.quickstart` | Build a starter DB with the 10 essentials |
| `python -m fred.add_series ID [ID ...] [--transforms] [--replace]` | Add series by ID |
| `python -m fred.download_popular_fred_series [--top-n N] [--min-popularity P] [--series-file CSV] [--workers W]` | Bulk download |
| `python -m fred.update_series [ID ...]` | Update existing series (all, or specific) |
| `python -m fred.smart_update_series [--max-age D] [--check-only] [--workers W]` | Incremental, frequency-aware update |
| `python -m fred.maintain_database [--phase health\|fix-meta\|update\|download\|transforms] [--workers W]` | Full maintenance |
| `python -m fred.fred_status_report` | Print a database status report |

## Troubleshooting

- **"No FRED API key configured"** — you haven't created `config.json` (copy
  `config.example.json`) or set `FRED_API_KEY_PRIMARY`.
- **Rate limited (HTTP 429)** — add a `api_key_secondary` / `api_key_tertiary`,
  or lower `--workers`. The client already backs off and rotates keys.
- **Where's my database?** — at `database.duckdb_path` from `config.json`
  (default `./fred_data.db`).

## License

MIT — see [LICENSE](LICENSE).
