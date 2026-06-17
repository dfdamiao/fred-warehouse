"""Use case: retrieve FRED data in several formats.

Assumes you've already built a database, e.g.:
    python -m examples.quickstart
    python -m fred.add_series M2SL --transforms

Run (from the repo root):
    python -m examples.retrieve_data
"""
from fred import FREDDataAccess


def main():
    fred = FREDDataAccess()

    # 1) A single series as a pandas DataFrame (last year).
    unrate = fred.get_series_data("UNRATE", period="1y")
    print("1) UNRATE as a DataFrame (last 5 rows):")
    print(unrate.tail(), "\n")

    # 2) Several series aligned on one date index, forward-filled.
    panel = fred.get_multiple_series(
        ["UNRATE", "M2SL", "T10Y2Y"], period="2y", fill_method="ffill"
    )
    print("2) Aligned multi-series panel (last 3 rows):")
    print(panel.tail(3), "\n")

    # 3) Just the latest value of each series.
    print("3) Latest value of each series:")
    print(fred.get_latest_values(["UNRATE", "M2SL", "T10Y2Y"]), "\n")

    # 4) Export to CSV and Parquet.
    fred.export_to_csv(["UNRATE", "M2SL"], "macro.csv", period="5y")
    fred.export_to_parquet(["UNRATE", "M2SL"], "macro.parquet", period="5y")
    print("4) Wrote macro.csv and macro.parquet\n")

    # 5) A transformation (year-over-year % change) — requires the *_pc1 series,
    #    which you get with: python -m fred.add_series M2SL --transforms
    try:
        yoy = fred.get_series_data("M2SL_pc1", period="2y")
        print("5) M2 year-over-year % change (last 3 rows):")
        print(yoy.tail(3))
    except Exception:
        print("5) Tip: add transformations with "
              "`python -m fred.add_series M2SL --transforms`")


if __name__ == "__main__":
    main()
