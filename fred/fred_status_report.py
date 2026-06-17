#!/usr/bin/env python3
"""
Comprehensive FRED data status report.
Shows both original series and transformations.
"""

from datetime import datetime
from pathlib import Path

import duckdb

from fred.config import FREDConfig

_cfg = FREDConfig()

def main():
    print("=" * 100)
    print("FRED DATABASE STATUS REPORT")
    print("=" * 100)
    print(f"Database: {_cfg.db_path}")
    print(f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 100)

    # Connect to database
    conn = duckdb.connect(_cfg.db_path, read_only=True)

    # 1. Overall Statistics
    print("\n" + "=" * 100)
    print("1. OVERALL DATABASE STATISTICS")
    print("=" * 100)

    overall = conn.execute("""
        SELECT
            COUNT(DISTINCT series_id) as total_series,
            COUNT(DISTINCT
                CASE
                    WHEN series_id LIKE '%_lin' OR series_id LIKE '%_chg' OR
                         series_id LIKE '%_ch1' OR series_id LIKE '%_pch' OR
                         series_id LIKE '%_pc1' OR series_id LIKE '%_pca' OR
                         series_id LIKE '%_cch' OR series_id LIKE '%_cca' OR
                         series_id LIKE '%_log'
                    THEN NULL
                    ELSE series_id
                END
            ) as original_series,
            COUNT(DISTINCT
                CASE
                    WHEN series_id LIKE '%_lin' OR series_id LIKE '%_chg' OR
                         series_id LIKE '%_ch1' OR series_id LIKE '%_pch' OR
                         series_id LIKE '%_pc1' OR series_id LIKE '%_pca' OR
                         series_id LIKE '%_cch' OR series_id LIKE '%_cca' OR
                         series_id LIKE '%_log'
                    THEN series_id
                    ELSE NULL
                END
            ) as transformed_series
        FROM series_metadata
    """).fetchdf()

    print(f"\nTotal series in database: {overall['total_series'].iloc[0]:,}")
    print(f"Original series (no transformation): {overall['original_series'].iloc[0]:,}")
    print(f"Transformed series: {overall['transformed_series'].iloc[0]:,}")

    # Data points
    data_stats = conn.execute("""
        SELECT
            COUNT(*) as total_data_points,
            MIN(date) as earliest_date,
            MAX(date) as latest_date
        FROM series_data
    """).fetchdf()

    print(f"\nTotal data points: {data_stats['total_data_points'].iloc[0]:,}")
    print(f"Date range: {data_stats['earliest_date'].iloc[0]} to {data_stats['latest_date'].iloc[0]}")

    # Database size
    db_size_mb = Path(_cfg.db_path).stat().st_size / (1024 * 1024)
    print(f"Database size: {db_size_mb:,.1f} MB")

    # 2. Frequency Breakdown
    print("\n" + "=" * 100)
    print("2. SERIES BY FREQUENCY")
    print("=" * 100)

    freq_breakdown = conn.execute("""
        SELECT
            frequency,
            COUNT(*) as total_count,
            COUNT(CASE WHEN series_id LIKE '%_lin' THEN 1 END) as lin_count,
            COUNT(CASE WHEN series_id LIKE '%_chg' THEN 1 END) as chg_count,
            COUNT(CASE WHEN series_id LIKE '%_ch1' THEN 1 END) as ch1_count,
            COUNT(CASE WHEN series_id LIKE '%_pch' THEN 1 END) as pch_count,
            COUNT(CASE WHEN series_id LIKE '%_pc1' THEN 1 END) as pc1_count,
            COUNT(CASE WHEN series_id LIKE '%_pca' THEN 1 END) as pca_count,
            COUNT(CASE WHEN series_id LIKE '%_cch' THEN 1 END) as cch_count,
            COUNT(CASE WHEN series_id LIKE '%_cca' THEN 1 END) as cca_count,
            COUNT(CASE WHEN series_id LIKE '%_log' THEN 1 END) as log_count
        FROM series_metadata
        GROUP BY frequency
        ORDER BY total_count DESC
        LIMIT 15
    """).fetchdf()

    print(f"\n{freq_breakdown.to_string(index=False)}")

    # 3. Transformation Coverage
    print("\n" + "=" * 100)
    print("3. TRANSFORMATION COVERAGE")
    print("=" * 100)

    transformations = {
        'lin': 'Levels (no transformation)',
        'chg': 'Change',
        'ch1': 'Change from Year Ago',
        'pch': 'Percent Change',
        'pc1': 'Percent Change from Year Ago',
        'pca': 'Compounded Annual Rate',
        'cch': 'Continuously Compounded Rate',
        'cca': 'Continuously Compounded Annual Rate',
        'log': 'Natural Log'
    }

    print("\nTransformation | Description                              | Count")
    print("-" * 85)

    for code, desc in transformations.items():
        count = conn.execute(f"""
            SELECT COUNT(*) as count
            FROM series_metadata
            WHERE series_id LIKE '%_{code}'
        """).fetchone()[0]
        print(f"{code:12} | {desc:40} | {count:,}")

    # 4. Base Series with Full Transformation Coverage
    print("\n" + "=" * 100)
    print("4. BASE SERIES TRANSFORMATION COVERAGE")
    print("=" * 100)

    # Extract base series name from transformed series
    base_coverage = conn.execute("""
        WITH base_series AS (
            SELECT
                CASE
                    WHEN series_id LIKE '%_lin' THEN SUBSTR(series_id, 1, LENGTH(series_id) - 4)
                    WHEN series_id LIKE '%_chg' THEN SUBSTR(series_id, 1, LENGTH(series_id) - 4)
                    WHEN series_id LIKE '%_ch1' THEN SUBSTR(series_id, 1, LENGTH(series_id) - 4)
                    WHEN series_id LIKE '%_pch' THEN SUBSTR(series_id, 1, LENGTH(series_id) - 4)
                    WHEN series_id LIKE '%_pc1' THEN SUBSTR(series_id, 1, LENGTH(series_id) - 4)
                    WHEN series_id LIKE '%_pca' THEN SUBSTR(series_id, 1, LENGTH(series_id) - 4)
                    WHEN series_id LIKE '%_cch' THEN SUBSTR(series_id, 1, LENGTH(series_id) - 4)
                    WHEN series_id LIKE '%_cca' THEN SUBSTR(series_id, 1, LENGTH(series_id) - 4)
                    WHEN series_id LIKE '%_log' THEN SUBSTR(series_id, 1, LENGTH(series_id) - 4)
                    ELSE series_id
                END as base_id,
                series_id
            FROM series_metadata
            WHERE series_id LIKE '%_lin' OR series_id LIKE '%_chg' OR
                  series_id LIKE '%_ch1' OR series_id LIKE '%_pch' OR
                  series_id LIKE '%_pc1' OR series_id LIKE '%_pca' OR
                  series_id LIKE '%_cch' OR series_id LIKE '%_cca' OR
                  series_id LIKE '%_log'
        )
        SELECT
            base_id,
            COUNT(*) as num_transformations
        FROM base_series
        GROUP BY base_id
        ORDER BY num_transformations DESC, base_id
    """).fetchdf()

    # Count how many have all 9 transformations
    full_coverage = base_coverage[base_coverage['num_transformations'] == 9]
    partial_coverage = base_coverage[base_coverage['num_transformations'] < 9]

    print(f"\nBase series with all 9 transformations: {len(full_coverage):,}")
    print(f"Base series with partial transformations: {len(partial_coverage):,}")

    if len(partial_coverage) > 0:
        print("\nPartial coverage breakdown:")
        partial_dist = partial_coverage['num_transformations'].value_counts().sort_index(ascending=False)
        for num, count in partial_dist.items():
            print(f"  {num} transformations: {count:,} series")

    # 5. Sample Key Economic Indicators
    print("\n" + "=" * 100)
    print("5. KEY ECONOMIC INDICATORS STATUS")
    print("=" * 100)

    key_series = [
        'FEDFUNDS', 'GDP', 'UNRATE', 'CPIAUCSL', 'DGS10', 'DGS2', 'T10Y2Y',
        'DCOILWTICO', 'DEXUSEU', 'PAYEMS', 'INDPRO', 'HOUST', 'RETAILSMNSA'
    ]

    print("\nIndicator | Transformations Available")
    print("-" * 50)

    for series in key_series:
        # Check how many transformations exist
        trans_count = conn.execute(f"""
            SELECT COUNT(*) as count
            FROM series_metadata
            WHERE series_id LIKE '{series}_%'
        """).fetchone()[0]

        if trans_count == 9:
            status = "✓ Complete (9/9)"
        elif trans_count > 0:
            status = f"Partial ({trans_count}/9)"
        else:
            status = "✗ Missing"

        print(f"{series:12} | {status}")

    # 6. Recent Update Activity
    print("\n" + "=" * 100)
    print("6. RECENT UPDATE ACTIVITY")
    print("=" * 100)

    recent_updates = conn.execute("""
        SELECT
            DATE_TRUNC('day', last_updated) as update_date,
            COUNT(*) as series_updated
        FROM series_metadata
        WHERE last_updated IS NOT NULL
        GROUP BY update_date
        ORDER BY update_date DESC
        LIMIT 10
    """).fetchdf()

    if not recent_updates.empty:
        print(f"\n{recent_updates.to_string(index=False)}")
    else:
        print("\nNo recent update timestamps found.")

    # 7. Sample Series with Full Coverage
    print("\n" + "=" * 100)
    print("7. SAMPLE SERIES WITH COMPLETE TRANSFORMATION COVERAGE")
    print("=" * 100)

    if len(full_coverage) > 0:
        sample_full = full_coverage.head(20)
        print(f"\nShowing {len(sample_full)} of {len(full_coverage):,} series with all 9 transformations:\n")
        for i, row in sample_full.iterrows():
            print(f"  {row['base_id']}")
    else:
        print("\nNo series have complete transformation coverage.")

    # 8. Transformation Download Progress
    print("\n" + "=" * 100)
    print("8. TRANSFORMATION DOWNLOAD PROGRESS")
    print("=" * 100)

    # Check download logs
    log_dir = Path(__file__).parent / 'logs'
    if log_dir.exists():
        log_files = sorted(log_dir.glob('download_progress_*.json'))
        if log_files:
            import json
            latest_log = log_files[-1]
            with open(latest_log, 'r') as f:
                log_data = json.load(f)

            print(f"\nLatest download log: {latest_log.name}")
            print(f"Total items: {log_data.get('total_items', 'N/A'):,}")
            print(f"Processed: {log_data.get('processed', 'N/A'):,}")
            print(f"Successful: {log_data.get('successful', 'N/A'):,}")
            print(f"Failed: {log_data.get('failed', 'N/A')}")
            print(f"Timestamp: {log_data.get('timestamp', 'N/A')}")

    conn.close()

    print("\n" + "=" * 100)
    print("END OF REPORT")
    print("=" * 100)


if __name__ == "__main__":
    main()
