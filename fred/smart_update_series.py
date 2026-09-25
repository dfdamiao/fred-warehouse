#!/usr/bin/env python3
"""
Intelligent incremental FRED series updater.

Features:
- Only downloads missing dates (incremental updates)
- Frequency-aware staleness: skips series that can't have new data yet
- Validates data continuity (no gaps)
- Detects and reports disruptions
- Handles both original (_lin) and transformed series
- Parallel processing with progress tracking
- Detailed reporting of issues

Usage:
    python -m fred.smart_update_series                    # Update all (frequency-aware)
    python -m fred.smart_update_series --no-freq-filter   # Update all (ignore frequency)
    python -m fred.smart_update_series FEDFUNDS_lin GDP_lin  # Specific series
    python -m fred.smart_update_series --max-age 14       # Only update if >14 days old
    python -m fred.smart_update_series --check-only       # Dry run - report what needs updating
"""

import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from fred import FREDDataAccess
from fred.client import FREDClient
from fred.config import FREDConfig

_cfg = FREDConfig()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Valid FRED transformation codes
TRANSFORMATION_CODES = {"lin", "chg", "ch1", "pch", "pc1", "pca", "cch", "cca", "log"}

# Frequency-aware staleness thresholds (days).
# Series younger than this are skipped — they can't have new data yet.
# Includes a small buffer beyond the release cadence.
FREQ_MAX_AGE: Dict[str, int] = {
    "daily": 2,  # update if >2 days old
    "weekly": 8,  # update if >8 days old
    "monthly": 35,  # update if >35 days old
    "quarterly": 100,  # update if >100 days old
    "annual": 370,  # update if >370 days old
}


def _freq_max_age(frequency: str) -> int:
    """Map a FRED frequency string to its staleness threshold in days."""
    if not frequency:
        return 2  # unknown → treat as daily (safe default)
    fl = frequency.lower()
    for key, days in FREQ_MAX_AGE.items():
        if key in fl:
            return days
    return 2  # unknown → daily


class SmartFREDUpdater:
    """Intelligent FRED series updater with gap detection."""

    def __init__(self, resume_file: Optional[str] = None):
        self.fred = FREDDataAccess()
        self.client = FREDClient(
            _cfg.api_key_primary,
            extra_keys=[
                k for k in [_cfg.api_key_secondary, _cfg.api_key_tertiary] if k
            ],
        )

        # Resume capability
        self.resume_file = resume_file or str(
            Path(__file__).parent / "logs" / "update_progress.json"
        )
        self.completed_series = set()

        self.stats = {
            "total": 0,
            "up_to_date": 0,
            "updated": 0,
            "failed": 0,
            "gaps_detected": 0,
            "disruptions": [],
            "failed_series": [],
        }

        # Load previous progress if exists
        self._load_progress()

    def _load_progress(self):
        """Load previous progress from resume file."""
        resume_path = Path(self.resume_file)
        if resume_path.exists():
            try:
                with open(resume_path, "r") as f:
                    progress = json.load(f)
                    self.completed_series = set(progress.get("completed_series", []))
                    if self.completed_series:
                        logger.info(
                            f"Resuming: {len(self.completed_series)} series already completed"
                        )
            except Exception as e:
                logger.warning(f"Could not load progress file: {e}")
                self.completed_series = set()

    def _save_progress(self, series_id: str = None, force: bool = False):
        """Save progress (batched to reduce I/O)."""
        if series_id:
            self.completed_series.add(series_id)

        # Only save every 50 series or when forced
        if not force and len(self.completed_series) % 50 != 0:
            return

        # Save to file
        resume_path = Path(self.resume_file)
        resume_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(resume_path, "w") as f:
                json.dump(
                    {
                        "completed_series": list(self.completed_series),
                        "last_updated": datetime.now().isoformat(),
                        "stats": self.stats,
                    },
                    f,
                    indent=2,
                )
        except Exception as e:
            logger.warning(f"Could not save progress: {e}")

    def _clear_progress(self):
        """Clear progress file when update completes successfully."""
        resume_path = Path(self.resume_file)
        if resume_path.exists():
            try:
                resume_path.unlink()
                logger.info("Progress file cleared")
            except Exception as e:
                logger.warning(f"Could not clear progress file: {e}")

    def extract_transformation(self, series_id: str) -> Tuple[str, Optional[str]]:
        """
        Extract base series ID and transformation code.

        Args:
            series_id: Full series ID (e.g., "FEDFUNDS_lin", "GDP_pch")

        Returns:
            (base_id, transformation_code) or (series_id, None)
        """
        if "_" in series_id:
            parts = series_id.rsplit("_", 1)
            if len(parts) == 2 and parts[1] in TRANSFORMATION_CODES:
                return parts[0], parts[1]
        return series_id, None

    def get_series_age_days(self, series_id: str) -> Optional[int]:
        """
        Calculate days since last data point.

        Returns:
            Days since last update, or None if no data
        """
        try:
            existing_data = self.fred.storage.get_data(series_id)
            if existing_data.empty:
                return None

            last_date = existing_data.index.max()
            days_old = (datetime.now() - last_date).days
            return days_old
        except Exception:
            return None

    def check_data_continuity(
        self, series_id: str, data: pd.DataFrame, frequency: str
    ) -> Tuple[bool, List[str]]:
        """
        Check for gaps in time series data.

        Args:
            series_id: Series ID
            data: DataFrame with DatetimeIndex
            frequency: Series frequency (Daily, Monthly, etc.)

        Returns:
            (has_gaps, list_of_gap_descriptions)
        """
        if data.empty or len(data) < 2:
            return False, []

        gaps = []

        # Determine expected frequency
        if "Daily" in frequency:
            max_gap = 7  # Allow weekend gaps
        elif "Weekly" in frequency:
            max_gap = 10  # Allow ~1 week gap
        elif "Monthly" in frequency:
            max_gap = 45  # Allow ~1 month gap
        elif "Quarterly" in frequency:
            max_gap = 100  # Allow ~1 quarter gap
        elif "Annual" in frequency:
            max_gap = 400  # Allow ~1 year gap
        else:
            max_gap = 30  # Default

        # Check gaps
        sorted_dates = data.index.sort_values()
        date_diffs = sorted_dates.to_series().diff()

        # Find gaps larger than expected
        large_gaps = date_diffs[date_diffs > pd.Timedelta(days=max_gap)]

        for gap_end, gap_size in large_gaps.items():
            gap_start_idx = sorted_dates.get_loc(gap_end) - 1
            gap_start = sorted_dates[gap_start_idx]
            gaps.append(
                f"{gap_start.strftime('%Y-%m-%d')} → {gap_end.strftime('%Y-%m-%d')} "
                f"({gap_size.days} days)"
            )

        return len(gaps) > 0, gaps

    def _mark_checked(self, series_id: str, metadata: Optional[Dict]) -> None:
        """Record that we checked the API and the series is current, so the
        frequency-aware skip can short-circuit it next run. ``last_updated``
        means 'when we last confirmed the series current' — it must advance on
        up-to-date checks too, not only when data actually changes, else
        unchanged series are re-fetched every run."""
        if metadata:
            metadata["last_updated"] = datetime.now()
            self.fred.storage.store_metadata(series_id, metadata)

    def _seed_metadata(self, base_id: str) -> Optional[Dict]:
        """Fetch metadata for a base series when the suffixed key has none.

        Used when seeding a key that has no metadata row at all — without a
        ``frequency`` the freq-aware skip would treat it as daily forever.
        """
        try:
            return dict(self.client.get_series_info(base_id))
        except Exception as e:
            logger.warning(f"Could not fetch metadata for {base_id}: {e}")
            return None

    def update_single_series(
        self,
        series_id: str,
        max_age_days: Optional[int] = None,
        check_only: bool = False,
        freq_aware: bool = True,
    ) -> Dict:
        """
        Update a single series with validation.

        Args:
            series_id: Series ID to update
            max_age_days: Only update if older than this many days (overrides freq_aware)
            check_only: If True, only check what would be updated
            freq_aware: If True, auto-skip based on series frequency

        Returns:
            Dict with update status and details
        """
        result = {
            "series_id": series_id,
            "status": "unknown",
            "days_old": None,
            "new_points": 0,
            "revised_points": 0,
            "gaps_found": [],
            "error": None,
        }

        try:
            # Extract transformation code if present
            base_id, units = self.extract_transformation(series_id)

            # Get existing data
            existing_data = self.fred.storage.get_data(series_id)
            metadata = self.fred.storage.get_metadata(series_id)

            if existing_data.empty:
                # Orphan: a series_metadata row with zero series_data rows.
                # These are in the work list, and the recommended-series
                # adder skips them as "already present" — returning here left
                # them permanently unfetchable. Seed full history instead; a
                # genuinely empty upstream still reports no_data.
                if check_only:
                    result["status"] = "needs_update"
                    return result
                seed = self.client.get_series_data(base_id, units=units)
                if seed.empty:
                    result["status"] = "no_data"
                    result["error"] = "Series has no existing data"
                    return result
                self.fred.storage.store_data(series_id, seed, replace_all=True)
                self._mark_checked(series_id, metadata or self._seed_metadata(base_id))
                result["status"] = "seeded"
                result["new_points"] = len(seed)
                logger.info(
                    f"✓ {series_id}: seeded {len(seed)} points "
                    f"({seed.index.min().strftime('%Y-%m-%d')} → "
                    f"{seed.index.max().strftime('%Y-%m-%d')})"
                )
                return result

            # Calculate age of last data point
            last_date = existing_data.index.max()
            days_old = (datetime.now() - last_date).days
            result["days_old"] = days_old

            # Frequency-aware skip: use metadata last_updated (when we
            # last checked the API), NOT last data point date.
            # Quarterly GDP always has data from months ago even if just checked.
            if freq_aware and metadata and not max_age_days:
                threshold = _freq_max_age(metadata.get("frequency", ""))
                last_checked = metadata.get("last_updated")
                if last_checked:
                    if isinstance(last_checked, str):
                        try:
                            last_checked_dt = datetime.fromisoformat(last_checked)
                        except ValueError:
                            last_checked_dt = None
                    else:
                        last_checked_dt = last_checked
                    if last_checked_dt:
                        days_since_check = (datetime.now() - last_checked_dt).days
                        if days_since_check < threshold:
                            result["status"] = "skipped_fresh"
                            return result

            # Explicit --max-age uses last data point date
            if max_age_days and days_old < max_age_days:
                result["status"] = "skipped_fresh"
                return result

            # Even if data is old (e.g. quarterly GDP), skip if API was
            # checked within max_age_days — avoids re-fetching unchanged series
            if max_age_days and metadata:
                last_checked = metadata.get("last_updated")
                if last_checked:
                    if isinstance(last_checked, str):
                        try:
                            last_checked_dt = datetime.fromisoformat(last_checked)
                        except ValueError:
                            last_checked_dt = None
                    else:
                        last_checked_dt = last_checked
                    if last_checked_dt:
                        days_since_check = (datetime.now() - last_checked_dt).days
                        if days_since_check < max_age_days:
                            result["status"] = "skipped_fresh"
                            return result

            # Check if already up to date (last data point is today)
            if days_old < 1:
                self._mark_checked(series_id, metadata)
                result["status"] = "up_to_date"
                return result

            if check_only:
                result["status"] = "needs_update"
                return result

            # Re-pull a trailing WINDOW, not just points strictly after the last
            # stored date, so FRED revisions to recent observations (which keep
            # their original date) are re-fetched. store_data(replace_all=False)
            # is INSERT OR REPLACE, so overlapping dates upsert the revised value.
            revision_lookback = 6  # observations
            if len(existing_data) > revision_lookback:
                refetch_from = existing_data.index[-revision_lookback]
            else:
                refetch_from = existing_data.index[0]
            start_date = refetch_from.strftime("%Y-%m-%d")

            # Use base_id for API call (FRED API doesn't recognize suffixed IDs)
            new_data = self.client.get_series_data(
                base_id, start_date=start_date, units=units
            )

            if new_data.empty:
                result["status"] = "up_to_date"
                return result

            # Split into genuinely-new points (after last stored date) and revised
            # existing points (date already stored, value differs / newly present).
            fresh = new_data[new_data.index > last_date]
            overlap = new_data[new_data.index <= last_date]
            prev = existing_data["value"].reindex(overlap.index)
            revised = overlap[
                overlap["value"].notna()
                & ((overlap["value"].round(10) != prev.round(10)) | prev.isna())
            ]

            if fresh.empty and revised.empty:
                self._mark_checked(series_id, metadata)
                result["status"] = "up_to_date"
                return result

            to_store = pd.concat([revised, fresh])
            to_store = to_store[~to_store.index.duplicated(keep="last")].sort_index()

            # Compute new latest date before storing (avoids mixed-type index issues)
            new_last_date = max(last_date, to_store.index.max())

            # Validate continuity between last existing point and genuinely-new
            # data (revisions to already-stored dates don't create gaps).
            frequency = metadata.get("frequency", "Unknown") if metadata else "Unknown"
            gaps = []
            if not fresh.empty:
                first_new_date = fresh.index.min()
                gap_days = (first_new_date - last_date).days

                # Determine if gap is unexpected based on frequency
                if "Daily" in frequency:
                    max_expected_gap = 7  # Weekends OK
                elif "Weekly" in frequency:
                    max_expected_gap = 10
                elif "Monthly" in frequency:
                    max_expected_gap = 45
                elif "Quarterly" in frequency:
                    max_expected_gap = 100
                elif "Annual" in frequency:
                    max_expected_gap = 400
                else:
                    max_expected_gap = 30

                if gap_days > max_expected_gap:
                    gaps.append(
                        f"{last_date.strftime('%Y-%m-%d')} → {first_new_date.strftime('%Y-%m-%d')} "
                        f"({gap_days} days, expected ≤{max_expected_gap})"
                    )

                # Also check for gaps within the new data itself
                if len(fresh) > 1:
                    _, new_gaps = self.check_data_continuity(
                        series_id, fresh, frequency
                    )
                    gaps.extend(new_gaps)

            if gaps:
                result["gaps_found"] = gaps
                self.stats["gaps_detected"] += 1
                # Only show summary for gap warnings (detailed list in final report)
                logger.warning(
                    f"{series_id}: Found {len(gaps)} gap(s) in recent update"
                )

            # Upsert new + revised points (incremental; INSERT OR REPLACE).
            self.fred.storage.store_data(series_id, to_store, replace_all=False)

            # Update metadata timestamp
            if metadata:
                metadata["last_updated"] = datetime.now()
                self.fred.storage.store_metadata(series_id, metadata)

            result["status"] = "updated"
            result["new_points"] = len(fresh)
            result["revised_points"] = len(revised)

            logger.info(
                f"✓ {series_id}: +{len(fresh)} new / {len(revised)} revised "
                f"({last_date.strftime('%Y-%m-%d')} → {new_last_date.strftime('%Y-%m-%d')})"
            )

        except Exception as e:
            result["status"] = "failed"
            result["error"] = str(e)
            logger.error(f"✗ {series_id}: {e}")

        return result

    def update_all_series(
        self,
        series_ids: Optional[List[str]] = None,
        max_age_days: Optional[int] = None,
        check_only: bool = False,
        filter_pattern: Optional[str] = None,
        workers: int = 3,
        freq_aware: bool = True,
    ):
        """
        Update multiple series with parallel processing.

        Args:
            series_ids: List of series to update, or None for all
            max_age_days: Only update series older than this many days
            check_only: If True, only report what would be updated
            filter_pattern: Only update series matching pattern (e.g., "_lin" for originals)
            workers: Number of parallel workers (default: 3)
            freq_aware: If True, auto-skip based on series frequency
        """
        # Get series list
        if series_ids is None:
            series_ids = self.fred.get_available_series()

        # Apply filter if specified
        if filter_pattern:
            series_ids = [s for s in series_ids if filter_pattern in s]

        # Filter out already completed
        series_to_process = [s for s in series_ids if s not in self.completed_series]

        self.stats["total"] = len(series_ids)
        self.stats["up_to_date"] = len(self.completed_series)

        logger.info("=" * 100)
        logger.info("SMART FRED SERIES UPDATE (PARALLEL)")
        logger.info("=" * 100)
        logger.info(f"Total series: {len(series_ids):,}")
        logger.info(f"Already completed: {len(self.completed_series):,}")
        logger.info(f"Remaining: {len(series_to_process):,}")
        logger.info(f"Parallel workers: {workers}")
        logger.info(f"Frequency-aware filtering: {'ON' if freq_aware else 'OFF'}")
        if freq_aware and not max_age_days:
            logger.info(
                f"  Thresholds: daily={FREQ_MAX_AGE['daily']}d, "
                f"weekly={FREQ_MAX_AGE['weekly']}d, "
                f"monthly={FREQ_MAX_AGE['monthly']}d, "
                f"quarterly={FREQ_MAX_AGE['quarterly']}d, "
                f"annual={FREQ_MAX_AGE['annual']}d"
            )
        if max_age_days:
            logger.info(f"Max age filter: {max_age_days} days (overrides freq-aware)")
        if filter_pattern:
            logger.info(f"Pattern filter: {filter_pattern}")
        if check_only:
            logger.info("MODE: Check-only (dry run)")
        logger.info("=" * 100)
        logger.info("")

        start_time = datetime.now()
        processed_count = len(self.completed_series)

        # Process in parallel
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all tasks
            future_to_series = {
                executor.submit(
                    self.update_single_series,
                    series_id,
                    max_age_days,
                    check_only,
                    freq_aware,
                ): series_id
                for series_id in series_to_process
            }

            # Process completed tasks
            for future in as_completed(future_to_series):
                series_id = future_to_series[future]
                processed_count += 1

                try:
                    result = future.result()

                    # Save progress (batched)
                    self._save_progress(series_id)

                    # Track stats
                    if result["status"] in (
                        "up_to_date",
                        "skipped_age",
                        "skipped_fresh",
                    ):
                        self.stats["up_to_date"] += 1
                    elif result["status"] in ("updated", "seeded"):
                        self.stats["updated"] += 1
                    elif result["status"] == "failed":
                        self.stats["failed"] += 1
                        self.stats["failed_series"].append(
                            {"series_id": series_id, "error": result["error"]}
                        )

                    if result["gaps_found"]:
                        self.stats["disruptions"].append(
                            {"series_id": series_id, "gaps": result["gaps_found"]}
                        )

                    # Progress updates every 500 series
                    if processed_count % 500 == 0:
                        elapsed = (datetime.now() - start_time).total_seconds()
                        rate = processed_count / elapsed if elapsed > 0 else 0
                        remaining = len(series_ids) - processed_count
                        eta_seconds = remaining / rate if rate > 0 else 0
                        pct = processed_count / len(series_ids) * 100

                        logger.info("")
                        logger.info("=" * 80)
                        logger.info(
                            f"  PROGRESS: {processed_count:,}/{len(series_ids):,} "
                            f"({pct:.1f}%) | ETA: {eta_seconds / 60:.1f} min"
                        )
                        logger.info(
                            f"  Skipped: {self.stats['up_to_date']:,} | "
                            f"Updated: {self.stats['updated']:,} | "
                            f"Failed: {self.stats['failed']}"
                        )
                        logger.info(
                            f"  Rate: {rate:.1f} series/sec | "
                            f"Elapsed: {elapsed / 60:.1f} min"
                        )
                        logger.info("=" * 80)
                        logger.info("")

                except Exception as e:
                    logger.error(f"✗ {series_id}: Unexpected error: {e}")
                    self.stats["failed"] += 1
                    self._save_progress(series_id)

        # Force final progress save
        self._save_progress(force=True)

        # Final report
        self._print_final_report(start_time, check_only)

        # Clear progress file on successful completion
        if not check_only:
            self._clear_progress()

    def _print_final_report(self, start_time: datetime, check_only: bool):
        """Print comprehensive final report."""
        elapsed = (datetime.now() - start_time).total_seconds()

        logger.info("\n" + "=" * 100)
        logger.info("UPDATE COMPLETE" if not check_only else "CHECK COMPLETE")
        logger.info("=" * 100)
        logger.info(f"Time elapsed: {elapsed:.0f}s ({elapsed / 60:.1f} minutes)")
        logger.info(f"Total series processed: {self.stats['total']:,}")
        logger.info(f"Already up-to-date: {self.stats['up_to_date']:,}")
        logger.info(f"Successfully updated: {self.stats['updated']:,}")
        logger.info(f"Failed: {self.stats['failed']}")
        logger.info(f"Series with gaps: {self.stats['gaps_detected']}")

        # Show failed series
        if self.stats["failed_series"]:
            logger.info("\n" + "-" * 100)
            logger.info(f"FAILED SERIES ({len(self.stats['failed_series'])})")
            logger.info("-" * 100)
            for item in self.stats["failed_series"][:20]:  # Show first 20
                logger.info(f"  {item['series_id']}: {item['error']}")
            if len(self.stats["failed_series"]) > 20:
                logger.info(f"  ... and {len(self.stats['failed_series']) - 20} more")

        # Show disruptions
        if self.stats["disruptions"]:
            logger.info("\n" + "-" * 100)
            logger.info(f"DATA CONTINUITY ISSUES ({len(self.stats['disruptions'])})")
            logger.info("-" * 100)
            for item in self.stats["disruptions"][:10]:  # Show first 10
                # Only show first 3 gaps for each series, plus summary
                gaps_to_show = item["gaps"][:3]
                total_gaps = len(item["gaps"])

                logger.info(f"  {item['series_id']}: {total_gaps} gap(s)")
                for gap in gaps_to_show:
                    logger.info(f"    {gap}")
                if total_gaps > 3:
                    logger.info(f"    ... and {total_gaps - 3} more gaps")

            if len(self.stats["disruptions"]) > 10:
                logger.info(
                    f"  ... and {len(self.stats['disruptions']) - 10} more series with gaps"
                )

        # Database stats
        logger.info("\n" + "-" * 100)
        logger.info("DATABASE STATISTICS")
        logger.info("-" * 100)
        db_stats = self.fred.get_system_stats()
        logger.info(f"  Total series: {db_stats['total_series']:,}")
        logger.info(f"  Total data points: {db_stats['total_data_points']:,}")
        logger.info(f"  Database size: {db_stats['database_size_mb']:.2f} MB")

        logger.info("=" * 100)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Smart FRED series updater with gap detection and auto-resume"
    )
    parser.add_argument(
        "series_ids", nargs="*", help="Specific series to update (default: all)"
    )
    parser.add_argument(
        "--max-age", type=int, default=None, help="Only update series older than N days"
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Dry run - only check what needs updating",
    )
    parser.add_argument(
        "--filter",
        type=str,
        default=None,
        help="Only update series matching pattern (e.g., '_lin' for originals)",
    )
    parser.add_argument(
        "--no-resume", action="store_true", help="Start fresh, ignore previous progress"
    )
    parser.add_argument(
        "--no-freq-filter",
        action="store_true",
        help="Disable frequency-aware filtering (update ALL series regardless)",
    )
    parser.add_argument(
        "--workers", type=int, default=3, help="Number of parallel workers (default: 3)"
    )

    args = parser.parse_args()

    # Clear progress file if --no-resume
    if args.no_resume:
        progress_file = Path(__file__).parent / "logs" / "update_progress.json"
        if progress_file.exists():
            progress_file.unlink()
            logger.info("Cleared previous progress")

    updater = SmartFREDUpdater()

    try:
        updater.update_all_series(
            series_ids=args.series_ids if args.series_ids else None,
            max_age_days=args.max_age,
            check_only=args.check_only,
            filter_pattern=args.filter,
            workers=args.workers,
            freq_aware=not args.no_freq_filter,
        )
    except KeyboardInterrupt:
        logger.warning("\n\nUpdate interrupted by user. Progress saved.")
        logger.warning("Run the same command again to resume from where you left off.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n\nUpdate failed with error: {e}")
        logger.warning("Progress saved. Run again to resume.")
        raise


if __name__ == "__main__":
    main()
