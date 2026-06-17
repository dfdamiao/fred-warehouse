#!/usr/bin/env python3
"""
Update existing FRED series with latest data.

Automatically detects and applies transformations for transformed series
(e.g., GDP_ch1, FEDFUNDS_log, etc.)

Usage:
    python -m fred.update_series                  # Update all stored series
    python -m fred.update_series FEDFUNDS GDP     # Update specific series
    python -m fred.update_series GDP_ch1 GDP_log  # Update transformed series
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from fred import FREDDataAccess

# Valid FRED transformation codes
TRANSFORMATION_CODES = {"lin", "chg", "ch1", "pch", "pc1", "pca", "cch", "cca", "log"}


def extract_transformation(series_id: str) -> tuple:
    """
    Extract base series ID and transformation code from series name.

    Args:
        series_id: Series name (e.g., "GDP_ch1", "FEDFUNDS")

    Returns:
        (base_series_id, transformation_code) or (series_id, None)
    """
    if "_" in series_id:
        parts = series_id.rsplit("_", 1)  # Split from right, max 1 split
        if len(parts) == 2 and parts[1] in TRANSFORMATION_CODES:
            return parts[0], parts[1]
    return series_id, None


def main():
    fred = FREDDataAccess()

    # Determine what to update
    if len(sys.argv) > 1:
        series_ids = sys.argv[1:]
        print(f"Updating {len(series_ids)} series...")
    else:
        series_ids = fred.get_available_series()
        print(f"Updating all {len(series_ids)} stored series...")

    if not series_ids:
        print("No series to update!")
        return

    # Update each series
    success_count = 0
    failed_count = 0

    for i, series_id in enumerate(series_ids):
        if (i + 1) % 10 == 0:
            print(f"Progress: {i+1}/{len(series_ids)}...")

        # Extract transformation code if present (e.g., "GDP_ch1" → "ch1")
        base_id, units = extract_transformation(series_id)

        if fred.update_series(series_id, units=units):
            success_count += 1
        else:
            failed_count += 1

    # Results
    print("\n" + "=" * 60)
    print("Update Complete!")
    print("=" * 60)
    print(f"Success: {success_count}")
    print(f"Failed: {failed_count}")

    # Show stats
    print("\nDatabase statistics:")
    db_stats = fred.get_system_stats()
    print(f"  Total series: {db_stats['total_series']}")
    print(f"  Total data points: {db_stats['total_data_points']:,}")
    print(f"  Database size: {db_stats['database_size_mb']:.2f} MB")


if __name__ == "__main__":
    main()
