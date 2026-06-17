#!/usr/bin/env python3
"""Add one or more FRED series to your database by ID.

The simplest way to grow your database: give it series IDs and it fetches them.

Usage:
    python -m fred.add_series FEDFUNDS GDP UNRATE        # add three series (levels)
    python -m fred.add_series DGS10 --transforms         # add + all 9 transformations
    python -m fred.add_series M2SL --replace             # re-fetch even if present
"""
import argparse

from fred import FREDDataAccess


def main():
    parser = argparse.ArgumentParser(
        description="Add one or more FRED series to your database by ID."
    )
    parser.add_argument(
        "series_ids", nargs="+", help="FRED series IDs, e.g. FEDFUNDS GDP UNRATE"
    )
    parser.add_argument(
        "--transforms",
        action="store_true",
        help="Also store all 9 FRED transformations (e.g. M2SL_lin, M2SL_pch, ...)",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Re-fetch and overwrite series that are already in the database",
    )
    args = parser.parse_args()

    fred = FREDDataAccess()
    print(f"Adding {len(args.series_ids)} series into {fred.storage.db_path}\n")

    added = 0
    for sid in args.series_ids:
        if args.transforms:
            # Reuse the bulk-downloader helper: stores sid_lin, sid_pch, ... etc.
            from fred.download_popular_fred_series import (
                download_series_with_transformations,
            )

            res = download_series_with_transformations(sid, fred)
            ok = res.get("success", 0) > 0
            print(f"  {'OK  ' if ok else 'FAIL'} {sid}  ({res.get('success', 0)}/9 transforms)")
        else:
            ok = fred.download_series(sid, replace=args.replace)
            print(f"  {'OK  ' if ok else 'FAIL'} {sid}")
        added += 1 if ok else 0

    print(f"\nDone: {added}/{len(args.series_ids)} series added.")


if __name__ == "__main__":
    main()
