#!/usr/bin/env python3
"""
Download popular FRED series with all transformations.

Downloads FRED series (by popularity ranking or custom list)
with all 9 transformation formats.
"""
import logging
import multiprocessing as mp
from datetime import datetime
from functools import partial
from pathlib import Path

import pandas as pd

from fred.data_access import FREDDataAccess

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# All FRED transformations
TRANSFORMATIONS = {
    "lin": "Levels (no transformation)",
    "chg": "Change",
    "ch1": "Change from Year Ago",
    "pch": "Percent Change",
    "pc1": "Percent Change from Year Ago",
    "pca": "Compounded Annual Rate of Change",
    "cch": "Continuously Compounded Rate of Change",
    "cca": "Continuously Compounded Annual Rate of Change",
    "log": "Natural Log",
}

# Optional default CSV of series to download (override with --series-file)
DEFAULT_MISSING_SERIES_FILE = Path(__file__).parent / "missing_fred_series.csv"
CHECKPOINT_FILE = Path(__file__).parent / "download_checkpoint.pkl"


def worker_download_series_api_only(series_id: str, existing_series: set) -> dict:
    """
    Worker function to download series data from FRED API only (no DB writes).

    Returns the downloaded data for later batch insertion into database.
    This avoids DuckDB concurrency issues.

    Args:
        series_id: Base FRED series ID (e.g., "GDP")
        existing_series: Set of already downloaded series IDs

    Returns:
        dict with series_id, downloaded data, and metadata
    """
    # Check if already downloaded (all transformations exist)
    transformed_series = [f"{series_id}_{code}" for code in TRANSFORMATIONS.keys()]
    if all(ts in existing_series for ts in transformed_series):
        return {
            "series_id": series_id,
            "status": "skipped",
            "data": None
        }

    # Create worker-specific FRED client for API calls only
    from fred.client import FREDClient
    from fred.config import FREDConfig

    _cfg = FREDConfig()
    client = FREDClient(
        _cfg.api_key_primary,
        extra_keys=_cfg.extra_keys,
    )

    results = {
        "series_id": series_id,
        "status": "processed",
        "transformations": []  # List of (transform_code, data, metadata) tuples
    }

    # Get metadata once (same for all transformations)
    try:
        metadata_base = client.get_series_info(series_id)
    except Exception:
        metadata_base = {}

    for transform_code, description in TRANSFORMATIONS.items():
        try:
            # Download from API with transformation
            df = client.get_series_data(series_id, units=transform_code)

            if not df.empty:
                # Create metadata with transformation info
                metadata = metadata_base.copy()
                metadata['transformation'] = transform_code
                metadata['transformation_desc'] = description

                # Store for batch insertion
                results["transformations"].append({
                    "transform_code": transform_code,
                    "data": df,
                    "metadata": metadata
                })

        except Exception:
            # Failed to download this transformation
            pass

    return results


def download_series_with_transformations(series_id: str, fred: FREDDataAccess) -> dict:
    """
    Download a single series in all transformation formats.

    Args:
        series_id: Base FRED series ID (e.g., "GDP")
        fred: FREDDataAccess instance

    Returns:
        dict with success counts
    """
    results = {"success": 0, "failed": 0, "failed_transforms": []}

    for transform_code, description in TRANSFORMATIONS.items():
        try:
            # Store with transformed name
            transformed_id = f"{series_id}_{transform_code}"

            logger.debug(f"  Downloading {transformed_id}...")

            # Pass base series_id to API with units parameter
            success = fred.download_series(series_id, replace=True, units=transform_code)

            if success:
                # Get the data that was stored under base series_id
                data = fred.storage.get_data(series_id)
                metadata = fred.storage.get_metadata(series_id)

                if not data.empty:
                    # Store under transformed name
                    fred.storage.store_data(transformed_id, data)

                if metadata:
                    # Update metadata to include transformation info
                    metadata['transformation'] = transform_code
                    metadata['transformation_desc'] = description
                    fred.storage.store_metadata(transformed_id, metadata)

                # Delete the base series entry (we only want transformed versions)
                with fred.storage.connection() as conn:
                    conn.execute("DELETE FROM series_data WHERE series_id = ?", [series_id])
                    conn.execute("DELETE FROM series_metadata WHERE series_id = ?", [series_id])

                results["success"] += 1
            else:
                results["failed"] += 1
                results["failed_transforms"].append(transform_code)

        except Exception as e:
            logger.debug(f"    Failed {transformed_id}: {e}")
            results["failed"] += 1
            results["failed_transforms"].append(transform_code)

    return results


def load_popular_missing_series(min_popularity: int = None, top_n: int = None, series_file: str = None):
    """
    Load popular missing series from the comparison results.

    Args:
        min_popularity: Minimum popularity threshold (e.g., 50)
        top_n: Number of top popular series to download (alternative to min_popularity)
        series_file: Path to CSV file with series list (optional)

    Returns:
        List of series IDs sorted by popularity
    """
    missing_series_file = Path(series_file) if series_file else DEFAULT_MISSING_SERIES_FILE

    logger.info(f"Loading popular missing series from {missing_series_file}...")

    if not missing_series_file.exists():
        logger.error(f"Missing series file not found: {missing_series_file}")
        logger.error("No default series file found. Use --series-file, --top-n, or --essential.")
        return []

    df = pd.read_csv(missing_series_file)

    # Sort by popularity if available
    if 'popularity' in df.columns:
        df = df.sort_values('popularity', ascending=False)

        # Filter by minimum popularity threshold
        if min_popularity is not None:
            df_filtered = df[df['popularity'] >= min_popularity]
            logger.info(f"Filtering by popularity >= {min_popularity}")
            logger.info(f"  Total series with popularity >= {min_popularity}: {len(df_filtered)}")
            series_list = df_filtered['id'].tolist()
        elif top_n is not None:
            # Get top N
            series_list = df.head(top_n)['id'].tolist()
            logger.info(f"Selecting top {top_n} series by popularity")
        else:
            logger.error("Must specify either min_popularity or top_n")
            return []
    else:
        logger.warning("No popularity column found, using top_n")
        series_list = df.head(top_n or 500)['id'].tolist()

    logger.info(f"Selected {len(series_list)} series to download")

    # Display top 20
    logger.info("\nTop 20 series to download:")
    for idx, row in df.head(20).iterrows():
        series_id = row['id']
        title = row.get('title', 'N/A')
        pop = row.get('popularity', 'N/A')
        logger.info(f"  {series_id}: {title} (popularity: {pop})")

    return series_list


def main(min_popularity: int = None, top_n: int = None, batch_size: int = 50, num_workers: int = 3, series_file: str = None):
    """
    Download popular FRED series using parallel workers.

    Args:
        min_popularity: Minimum popularity threshold (e.g., 50)
        top_n: Number of top popular series to download (alternative)
        batch_size: Checkpoint every N series (default: 50)
        num_workers: Number of parallel workers (default: 3, conservative for FRED API)
        series_file: Path to CSV file with series list (optional)
    """
    logger.info("=" * 80)
    logger.info("FRED SERIES DOWNLOAD (PARALLEL)")
    logger.info("=" * 80)

    if min_popularity is not None:
        logger.info(f"Target: All series with popularity >= {min_popularity}")
    elif top_n is not None:
        logger.info(f"Target: Top {top_n} popular series")
    else:
        logger.error("Must specify either --min-popularity or --top-n")
        return

    logger.info(f"Transformations: {len(TRANSFORMATIONS)} per series")
    logger.info(f"Workers: {num_workers} parallel workers")
    logger.info("=" * 80)

    start_time = datetime.now()

    # Load popular missing series
    series_to_download = load_popular_missing_series(
        min_popularity=min_popularity,
        top_n=top_n,
        series_file=series_file
    )

    if not series_to_download:
        logger.error("No series to download!")
        return

    # Initialize FRED to get existing series
    logger.info("\nChecking existing series...")
    fred = FREDDataAccess()
    existing_series = set(fred.get_available_series())
    logger.info(f"Current database has {len(existing_series)} series")

    # Track stats
    stats = {
        "total_series": len(series_to_download),
        "processed": 0,
        "skipped": 0,
        "successful_series": 0,
        "failed_series": 0,
        "total_transformations": 0,
        "successful_transformations": 0,
        "failed_transformations": 0,
    }

    # Download with parallel workers (API only, then batch DB insert)
    logger.info(f"\nStarting parallel download with {num_workers} workers...")
    logger.info("Phase 1: API downloads (parallel)")
    logger.info("Phase 2: Database writes (serial batches)\n")

    # Phase 1: Parallel API downloads
    downloaded_data = []

    # Load checkpoint if exists
    if CHECKPOINT_FILE.exists():
        import pickle
        try:
            with open(CHECKPOINT_FILE, 'rb') as f:
                checkpoint = pickle.load(f)
                downloaded_data = checkpoint.get('downloaded_data', [])
                completed_series = set(checkpoint.get('completed_series', []))
                logger.info(f"Resuming from checkpoint: {len(downloaded_data)} series already downloaded")

                # Filter out already completed series
                series_to_download = [s for s in series_to_download if s not in completed_series]
                stats['skipped'] = len(completed_series)
                logger.info(f"Remaining to download: {len(series_to_download)} series\n")
        except Exception as e:
            logger.warning(f"Failed to load checkpoint: {e}")
            logger.info("Starting fresh download\n")

    completed_series = set()

    with mp.Pool(processes=num_workers) as pool:
        # Use partial to bind existing_series
        worker_func = partial(worker_download_series_api_only, existing_series=existing_series)

        # Process series in parallel (API calls only)
        for i, result in enumerate(pool.imap_unordered(worker_func, series_to_download), 1):
            series_id = result["series_id"]

            if result["status"] == "skipped":
                logger.info(f"[{i}/{len(series_to_download)}] {series_id}: Already exists, skipped")
                stats["skipped"] += 1
            else:
                # Store downloaded data for batch insertion
                num_transforms = len(result["transformations"])

                if num_transforms > 0:
                    downloaded_data.append(result)
                    completed_series.add(series_id)
                    stats["successful_series"] += 1
                    logger.info(f"[{i}/{len(series_to_download)}] {series_id}: Downloaded {num_transforms}/{len(TRANSFORMATIONS)} transformations")
                else:
                    stats["failed_series"] += 1
                    logger.warning(f"[{i}/{len(series_to_download)}] {series_id}: All transformations failed")

            # Save checkpoint every 25 series
            if i % 25 == 0:
                import pickle
                checkpoint = {
                    'downloaded_data': downloaded_data,
                    'completed_series': list(completed_series),
                    'stats': stats
                }
                with open(CHECKPOINT_FILE, 'wb') as f:
                    pickle.dump(checkpoint, f)
                logger.info(f"  Checkpoint saved: {len(completed_series)} series")

            # Progress update
            if i % 50 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                logger.info(f"\n{'='*80}")
                logger.info(f"API DOWNLOAD PROGRESS - {i}/{len(series_to_download)} series")
                logger.info(f"  Time elapsed: {elapsed/60:.1f} minutes")
                logger.info(f"  Downloaded: {len(downloaded_data)} series")
                logger.info(f"  Skipped: {stats['skipped']}")
                logger.info(f"  Avg time per series: {elapsed/i:.1f} seconds")
                logger.info(f"={'='*80}\n")

    # Phase 2: Batch database insertion
    logger.info(f"\n{'='*80}")
    logger.info(f"Phase 2: Writing {len(downloaded_data)} series to database...")
    logger.info(f"{'='*80}\n")

    if not downloaded_data:
        logger.info("No new data to write to database")
    else:
        # Create single FRED instance for all DB writes
        fred_db = FREDDataAccess()

        # Prepare all data and metadata for batch insertion
        all_data_rows = []
        all_metadata_rows = []

        for result in downloaded_data:
            series_id = result["series_id"]
            try:
                for transform_data in result["transformations"]:
                    transform_code = transform_data["transform_code"]
                    data = transform_data["data"]
                    metadata = transform_data["metadata"]

                    transformed_id = f"{series_id}_{transform_code}"

                    # Prepare data rows
                    data_copy = data.reset_index()
                    data_copy["series_id"] = transformed_id
                    data_rows = [(transformed_id, row["date"], row["value"])
                                 for _, row in data_copy.iterrows()]
                    all_data_rows.extend(data_rows)

                    # Prepare metadata row
                    all_metadata_rows.append((
                        transformed_id,
                        metadata.get("title"),
                        metadata.get("units"),
                        metadata.get("frequency"),
                        metadata.get("seasonal_adjustment"),
                        metadata.get("last_updated"),
                        metadata.get("observation_start"),
                        metadata.get("observation_end"),
                        metadata.get("popularity", 0),
                        metadata.get("notes")
                    ))

                    stats["successful_transformations"] += 1

                stats["processed"] += 1

            except Exception as e:
                logger.error(f"  Failed to prepare {series_id}: {e}")
                stats["failed_series"] += 1

        # Single batch write to database
        logger.info(f"Writing {len(all_data_rows)} data points and {len(all_metadata_rows)} series to database...")

        with fred_db.storage._write_lock:
            with fred_db.storage.connection() as conn:
                # Batch insert metadata
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO series_metadata
                    (series_id, title, units, frequency, seasonal_adjustment,
                     last_updated, observation_start, observation_end, popularity, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    all_metadata_rows
                )

                # Batch insert data
                conn.executemany(
                    "INSERT OR REPLACE INTO series_data (series_id, date, value) VALUES (?, ?, ?)",
                    all_data_rows
                )

                conn.commit()

        logger.info(f"Database write complete: {len(all_metadata_rows)} series, {len(all_data_rows)} points")

        # Delete checkpoint file after successful completion
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
            logger.info("Checkpoint file removed (download complete)")

    # Final summary
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("\n" + "=" * 80)
    logger.info("DOWNLOAD COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Time elapsed: {elapsed/60:.1f} minutes ({elapsed/3600:.2f} hours)")
    logger.info("\nSeries Statistics:")
    logger.info(f"  Total targeted: {stats['total_series']}")
    logger.info(f"  Processed: {stats['processed']}")
    logger.info(f"  Skipped (already exists): {stats['skipped']}")
    logger.info(f"  Successful: {stats['successful_series']}")
    logger.info(f"  Failed: {stats['failed_series']}")
    logger.info("\nTransformation Statistics:")
    logger.info(f"  Total attempted: {stats['total_transformations']}")
    logger.info(f"  Successful: {stats['successful_transformations']}")
    logger.info(f"  Failed: {stats['failed_transformations']}")
    if stats['total_transformations'] > 0:
        success_rate = stats['successful_transformations'] / stats['total_transformations'] * 100
        logger.info(f"  Success rate: {success_rate:.1f}%")
    logger.info("=" * 80)

    # Update FRED catalog (now has more series)
    logger.info("\nNext step: Re-run export_etf_data.py to export new FRED data to parquet files")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download FRED series with all transformations (parallel)")
    parser.add_argument("--min-popularity", type=int, help="Minimum popularity threshold (e.g., 50)")
    parser.add_argument("--top-n", type=int, help="Number of top popular series to download (alternative to --min-popularity)")
    parser.add_argument("--series-file", type=str, help="Path to CSV file with series list (default: ./missing_fred_series.csv)")
    parser.add_argument("--batch-size", type=int, default=50, help="Checkpoint frequency (default: 50)")
    parser.add_argument("--workers", type=int, default=3, help="Number of parallel workers (default: 3, conservative for FRED API)")

    args = parser.parse_args()

    # Require either min_popularity or top_n
    if args.min_popularity is None and args.top_n is None:
        parser.error("Must specify either --min-popularity or --top-n")

    main(
        min_popularity=args.min_popularity,
        top_n=args.top_n,
        batch_size=args.batch_size,
        num_workers=args.workers,
        series_file=args.series_file
    )
