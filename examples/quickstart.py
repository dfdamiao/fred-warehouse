"""Quick start: build a tiny FRED database with the 10 essential indicators.

Prerequisites:
    1. pip install -r requirements.txt
    2. cp config.example.json config.json   and paste your free FRED API key
       (https://fred.stlouisfed.org/docs/api/api_key.html)

Run (from the repo root):
    python -m examples.quickstart
"""
from fred import FREDDataAccess
from fred.recommended_series import ESSENTIAL_SERIES


def main():
    fred = FREDDataAccess()
    print(f"Building a database at {fred.storage.db_path}")
    print(f"Downloading {len(ESSENTIAL_SERIES)} essential series...\n")

    for sid in ESSENTIAL_SERIES:
        ok = fred.download_series(sid)
        print(f"  {'OK  ' if ok else 'FAIL'} {sid}")

    print("\nSample — Unemployment Rate (UNRATE), last 5 observations:")
    print(fred.get_series_data("UNRATE", period="1y").tail())

    print("\nDatabase stats:")
    print(fred.get_system_stats())


if __name__ == "__main__":
    main()
