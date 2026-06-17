#!/usr/bin/env python3
"""
Comprehensive FRED database maintenance script.

Phases:
  1. Health check — report DB state, detect corruption, null metadata
  2. Fix metadata — re-fetch metadata for series with NULL fields
  3. Incremental update — update all existing series with new data
  4. Download missing — download any recommended series not yet in DB
  5. Verify transformations — ensure all 9 transforms exist for key series
  6. Final report — summary of everything done

Usage:
    python -m fred.maintain_database                    # Full maintenance
    python -m fred.maintain_database --phase health     # Health check only
    python -m fred.maintain_database --phase fix-meta   # Fix null metadata only
    python -m fred.maintain_database --phase update     # Update existing only
    python -m fred.maintain_database --phase download   # Download missing only
    python -m fred.maintain_database --phase transforms # Verify transforms only
    python -m fred.maintain_database --workers 20       # More parallel workers
    python -m fred.maintain_database --max-age 7        # Only update if >7 days old
"""

import argparse
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import duckdb

from fred import FREDDataAccess
from fred.client import FREDClient
from fred.config import FREDConfig
from fred.recommended_series import ALL_RECOMMENDED

_cfg = FREDConfig()

# ── Logging ──────────────────────────────────────────────────────

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

log_file = LOG_DIR / f"maintain_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file),
    ],
)
logger = logging.getLogger(__name__)

TRANSFORMATION_CODES = ["lin", "chg", "ch1", "pch", "pc1", "pca", "cch", "cca", "log"]


class FREDMaintenance:
    """Comprehensive FRED database maintenance."""

    def __init__(self, workers: int = 3, max_age_days: int | None = None):
        self._check_drive()
        self.fred = FREDDataAccess()
        self.client = FREDClient(
            _cfg.api_key_primary,
            extra_keys=[k for k in [_cfg.api_key_secondary, _cfg.api_key_tertiary] if k],
        )
        self.workers = workers
        self.max_age_days = max_age_days
        self._consecutive_io_failures = 0
        self._max_consecutive_io_failures = 50  # abort if drive disappears
        self.progress_file = LOG_DIR / "maintain_progress.json"
        self.stats = {
            "phase": "",
            "metadata_fixed": 0,
            "metadata_failed": 0,
            "series_updated": 0,
            "series_up_to_date": 0,
            "series_failed": 0,
            "new_points_total": 0,
            "series_downloaded": 0,
            "series_download_failed": 0,
            "transforms_added": 0,
            "transforms_failed": 0,
            "failed_series": [],
        }

    @staticmethod
    def _check_drive():
        """Verify the external drive is mounted and DB is accessible."""
        db_path = Path(_cfg.db_path)
        if not db_path.exists():
            raise RuntimeError(
                f"Database not found: {db_path}\n"
                f"Is the external drive mounted at {db_path.parent}?"
            )
        # Quick read test
        try:
            conn = duckdb.connect(str(db_path), read_only=True)
            conn.execute("SELECT 1").fetchone()
            conn.close()
        except Exception as e:
            raise RuntimeError(f"Cannot read database: {e}") from e

    def _is_io_error(self, error_msg: str) -> bool:
        """Check if an error is a drive/IO issue (not a FRED API error)."""
        io_markers = ["No such file or directory", "Permission denied",
                      "IO Error", "Cannot open file"]
        return any(m in error_msg for m in io_markers)

    def _track_io_failure(self, error_msg: str):
        """Track consecutive IO failures; abort if drive likely disconnected."""
        if self._is_io_error(error_msg):
            self._consecutive_io_failures += 1
            if self._consecutive_io_failures >= self._max_consecutive_io_failures:
                raise RuntimeError(
                    f"ABORTING: {self._consecutive_io_failures} consecutive IO errors. "
                    f"External drive likely disconnected."
                )
        else:
            self._consecutive_io_failures = 0  # reset on non-IO error

        # Resume file for long-running updates
        self.progress_file = LOG_DIR / "maintain_progress.json"

    # ── Phase 1: Health Check ────────────────────────────────────

    def phase_health_check(self) -> dict:
        """Report full database health."""
        logger.info("=" * 100)
        logger.info("PHASE 1: HEALTH CHECK")
        logger.info("=" * 100)

        db_path = Path(_cfg.db_path)
        logger.info(f"Database: {db_path}")
        logger.info(f"Size: {db_path.stat().st_size / (1024**2):.1f} MB")

        with duckdb.connect(str(db_path), read_only=True) as conn:
            # Basic counts
            total_series = conn.execute(
                "SELECT COUNT(*) FROM series_metadata"
            ).fetchone()[0]
            total_points = conn.execute(
                "SELECT COUNT(*) FROM series_data"
            ).fetchone()[0]

            logger.info(f"Total series (metadata): {total_series:,}")
            logger.info(f"Total data points: {total_points:,}")

            # Null metadata
            null_meta = conn.execute("""
                SELECT COUNT(*) FROM series_metadata
                WHERE title IS NULL OR frequency IS NULL
                    OR observation_start IS NULL
            """).fetchone()[0]
            logger.info(f"Series with NULL metadata: {null_meta}")

            # Series with no data
            no_data = conn.execute("""
                SELECT COUNT(*) FROM series_metadata m
                LEFT JOIN (SELECT DISTINCT series_id FROM series_data) d
                    ON m.series_id = d.series_id
                WHERE d.series_id IS NULL
            """).fetchone()[0]
            logger.info(f"Series with no data rows: {no_data}")

            # Age analysis
            age_stats = conn.execute("""
                SELECT
                    MIN(observation_end) as oldest_end,
                    MAX(observation_end) as newest_end,
                    MIN(last_updated) as oldest_update,
                    MAX(last_updated) as newest_update
                FROM series_metadata
                WHERE observation_end IS NOT NULL
            """).fetchone()
            logger.info(f"Oldest observation_end: {age_stats[0]}")
            logger.info(f"Newest observation_end: {age_stats[1]}")
            logger.info(f"Oldest last_updated: {age_stats[2]}")
            logger.info(f"Newest last_updated: {age_stats[3]}")

            # Frequency breakdown
            logger.info("\nFrequency breakdown:")
            for row in conn.execute("""
                SELECT frequency, COUNT(*) as cnt
                FROM series_metadata
                GROUP BY frequency ORDER BY cnt DESC
                LIMIT 15
            """).fetchall():
                logger.info(f"  {str(row[0]):30s} {row[1]:,}")

            # Transformation coverage
            logger.info("\nTransformation coverage:")
            for t in TRANSFORMATION_CODES:
                cnt = conn.execute(
                    f"SELECT COUNT(*) FROM series_metadata WHERE series_id LIKE '%_{t}'"
                ).fetchone()[0]
                logger.info(f"  _{t}: {cnt:,}")

            # Check recommended series coverage
            existing_ids = set(
                r[0]
                for r in conn.execute(
                    "SELECT series_id FROM series_metadata"
                ).fetchall()
            )

        # Base IDs from existing
        existing_base = set()
        transforms_set = set(TRANSFORMATION_CODES)
        for s in existing_ids:
            parts = s.rsplit("_", 1)
            if len(parts) == 2 and parts[1] in transforms_set:
                existing_base.add(parts[0])
            else:
                existing_base.add(s)

        missing_recommended = [s for s in ALL_RECOMMENDED if s not in existing_base]
        logger.info(f"\nRecommended series in DB: {len(ALL_RECOMMENDED) - len(missing_recommended)}/{len(ALL_RECOMMENDED)}")
        logger.info(f"Missing recommended: {len(missing_recommended)}")

        if missing_recommended and len(missing_recommended) <= 20:
            for s in missing_recommended:
                logger.info(f"  Missing: {s}")

        report = {
            "total_series": total_series,
            "total_points": total_points,
            "null_metadata": null_meta,
            "no_data_series": no_data,
            "missing_recommended": len(missing_recommended),
            "missing_list": missing_recommended,
            "existing_base_ids": existing_base,
            "existing_ids": existing_ids,
        }

        logger.info("=" * 100)
        return report

    # ── Phase 2: Fix Null Metadata ───────────────────────────────

    def phase_fix_metadata(self):
        """Re-fetch metadata for series with NULL fields."""
        logger.info("=" * 100)
        logger.info("PHASE 2: FIX NULL METADATA")
        logger.info("=" * 100)

        with duckdb.connect(str(_cfg.db_path), read_only=True) as conn:
            null_series = [
                r[0]
                for r in conn.execute("""
                    SELECT series_id FROM series_metadata
                    WHERE title IS NULL OR frequency IS NULL
                        OR observation_start IS NULL OR last_updated IS NULL
                """).fetchall()
            ]

        if not null_series:
            logger.info("No series with NULL metadata — skipping")
            return

        logger.info(f"Found {len(null_series)} series with NULL metadata")

        # Group by base ID to avoid redundant API calls
        base_to_suffixed = {}
        transforms_set = set(TRANSFORMATION_CODES)
        for sid in null_series:
            parts = sid.rsplit("_", 1)
            if len(parts) == 2 and parts[1] in transforms_set:
                base_id = parts[0]
            else:
                base_id = sid
            base_to_suffixed.setdefault(base_id, []).append(sid)

        logger.info(f"Unique base series to fetch: {len(base_to_suffixed)}")

        def fix_one_base(base_id: str, suffixed_ids: list[str]) -> tuple[int, int]:
            """Fetch metadata for base_id, apply to all suffixed variants."""
            fixed, failed = 0, 0
            try:
                meta = self.client.get_series_info(base_id)
                for sid in suffixed_ids:
                    try:
                        # Determine the transform suffix for this series
                        parts = sid.rsplit("_", 1)
                        units = parts[1] if len(parts) == 2 and parts[1] in transforms_set else None

                        # Store metadata with the suffixed series_id
                        meta_copy = dict(meta)
                        meta_copy["series_id"] = sid
                        if units:
                            meta_copy["title"] = f"{meta.get('title', '')} ({units})"
                        self.fred.storage.store_metadata(sid, meta_copy)
                        fixed += 1
                    except Exception as e:
                        logger.warning(f"  Failed to store metadata for {sid}: {e}")
                        failed += 1
            except Exception as e:
                logger.warning(f"  Failed to fetch metadata for {base_id}: {e}")
                failed += len(suffixed_ids)
            return fixed, failed

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {
                executor.submit(fix_one_base, base_id, sids): base_id
                for base_id, sids in base_to_suffixed.items()
            }
            for future in as_completed(futures):
                fixed, failed = future.result()
                self.stats["metadata_fixed"] += fixed
                self.stats["metadata_failed"] += failed

        logger.info(f"Metadata fixed: {self.stats['metadata_fixed']}")
        logger.info(f"Metadata failed: {self.stats['metadata_failed']}")
        logger.info("=" * 100)

    # ── Phase 3: Incremental Update (delegates to SmartFREDUpdater) ─

    def phase_update_existing(self):
        """Incrementally update all existing series via SmartFREDUpdater.

        Delegates to smart_update_series.py which provides:
        - Frequency-aware staleness (skips quarterly/annual when fresh)
        - Gap detection and reporting
        - Resume capability
        """
        from fred.smart_update_series import SmartFREDUpdater

        logger.info("=" * 100)
        logger.info("PHASE 3: INCREMENTAL UPDATE (freq-aware)")
        logger.info("=" * 100)

        updater = SmartFREDUpdater()
        updater.update_all_series(
            max_age_days=self.max_age_days,
            workers=self.workers,
            freq_aware=True,
        )

        # Propagate stats back
        self.stats["series_updated"] = updater.stats["updated"]
        self.stats["series_up_to_date"] = updater.stats["up_to_date"]
        self.stats["series_failed"] = updater.stats["failed"]
        self.stats["failed_series"] = updater.stats["failed_series"]

        logger.info("=" * 100)

    # ── Phase 4: Download Missing Series ─────────────────────────

    def phase_download_missing(self, missing_list: list[str] | None = None):
        """Download recommended series not yet in DB, with all 9 transforms."""
        logger.info("=" * 100)
        logger.info("PHASE 4: DOWNLOAD MISSING SERIES")
        logger.info("=" * 100)

        if missing_list is None:
            # Re-check what's missing
            existing_ids = set(self.fred.get_available_series())
            existing_base = set()
            transforms_set = set(TRANSFORMATION_CODES)
            for s in existing_ids:
                parts = s.rsplit("_", 1)
                if len(parts) == 2 and parts[1] in transforms_set:
                    existing_base.add(parts[0])
                else:
                    existing_base.add(s)
            missing_list = [s for s in ALL_RECOMMENDED if s not in existing_base]

        if not missing_list:
            logger.info("All recommended series already in DB — skipping")
            return

        logger.info(f"Missing series to download: {len(missing_list)}")
        total_downloads = len(missing_list) * len(TRANSFORMATION_CODES)
        logger.info(f"Total downloads (with transforms): {total_downloads}")

        def download_with_transforms(base_id: str) -> tuple[int, int]:
            """Download one series with all 9 transformations."""
            success, failed = 0, 0

            # First fetch and store metadata
            try:
                meta = self.client.get_series_info(base_id)
            except Exception as e:
                logger.warning(f"  x {base_id}: metadata fetch failed: {e}")
                return 0, len(TRANSFORMATION_CODES)

            for units in TRANSFORMATION_CODES:
                series_id = f"{base_id}_{units}"
                try:
                    data = self.client.get_series_data(
                        base_id, units=units
                    )
                    if data.empty:
                        failed += 1
                        continue

                    # Store metadata for this transform variant
                    meta_copy = dict(meta)
                    meta_copy["series_id"] = series_id
                    meta_copy["title"] = f"{meta.get('title', '')} ({units})"
                    self.fred.storage.store_metadata(series_id, meta_copy)

                    # Store data
                    self.fred.storage.store_data(series_id, data, replace_all=True)
                    success += 1

                except Exception as e:
                    logger.warning(f"  x {series_id}: {e}")
                    failed += 1

            return success, failed

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {
                executor.submit(download_with_transforms, base_id): base_id
                for base_id in missing_list
            }

            done = 0
            for future in as_completed(futures):
                base_id = futures[future]
                done += 1
                try:
                    success, failed = future.result()
                    self.stats["series_downloaded"] += success
                    self.stats["series_download_failed"] += failed
                    if success > 0:
                        logger.info(f"  + {base_id}: {success}/{len(TRANSFORMATION_CODES)} transforms")
                except Exception as e:
                    logger.error(f"  x {base_id}: unexpected: {e}")
                    self.stats["series_download_failed"] += len(TRANSFORMATION_CODES)

                if done % 50 == 0:
                    logger.info(f"  Progress: {done}/{len(missing_list)}")

        logger.info(f"\nDownloaded: {self.stats['series_downloaded']:,}")
        logger.info(f"Failed: {self.stats['series_download_failed']}")
        logger.info("=" * 100)

    # ── Phase 5: Verify Transformations ──────────────────────────

    def phase_verify_transforms(self):
        """Ensure key series have all 9 transformations."""
        logger.info("=" * 100)
        logger.info("PHASE 5: VERIFY TRANSFORMATIONS")
        logger.info("=" * 100)

        existing_ids = set(self.fred.get_available_series())

        # Find base series with incomplete transforms
        base_transform_count: dict[str, list[str]] = {}
        transforms_set = set(TRANSFORMATION_CODES)

        for sid in existing_ids:
            parts = sid.rsplit("_", 1)
            if len(parts) == 2 and parts[1] in transforms_set:
                base_id = parts[0]
                base_transform_count.setdefault(base_id, []).append(parts[1])

        incomplete = {
            base_id: existing_transforms
            for base_id, existing_transforms in base_transform_count.items()
            if len(existing_transforms) < 9
        }

        if not incomplete:
            logger.info("All base series have complete transformations")
            return

        logger.info(f"Series with incomplete transforms: {len(incomplete)}")

        # Only fix recommended series (don't waste API calls on obscure ones)
        recommended_set = set(ALL_RECOMMENDED)
        to_fix = {
            base_id: transforms
            for base_id, transforms in incomplete.items()
            if base_id in recommended_set
        }

        logger.info(f"Recommended series to fix: {len(to_fix)}")

        def fix_transforms(base_id: str, existing_transforms: list[str]) -> tuple[int, int]:
            """Download missing transforms for a base series."""
            missing = [t for t in TRANSFORMATION_CODES if t not in existing_transforms]
            success, failed = 0, 0

            # Fetch metadata once
            try:
                meta = self.client.get_series_info(base_id)
            except Exception:
                return 0, len(missing)

            for units in missing:
                series_id = f"{base_id}_{units}"
                try:
                    data = self.client.get_series_data(base_id, units=units)
                    if data.empty:
                        failed += 1
                        continue

                    meta_copy = dict(meta)
                    meta_copy["series_id"] = series_id
                    meta_copy["title"] = f"{meta.get('title', '')} ({units})"
                    self.fred.storage.store_metadata(series_id, meta_copy)
                    self.fred.storage.store_data(series_id, data, replace_all=True)
                    success += 1
                except Exception as e:
                    logger.warning(f"  x {series_id}: {e}")
                    failed += 1

            return success, failed

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {
                executor.submit(fix_transforms, base_id, transforms): base_id
                for base_id, transforms in to_fix.items()
            }
            for future in as_completed(futures):
                base_id = futures[future]
                try:
                    success, failed = future.result()
                    self.stats["transforms_added"] += success
                    self.stats["transforms_failed"] += failed
                    if success > 0:
                        logger.info(f"  + {base_id}: added {success} transforms")
                except Exception as e:
                    logger.error(f"  x {base_id}: {e}")

        logger.info(f"\nTransforms added: {self.stats['transforms_added']}")
        logger.info(f"Transforms failed: {self.stats['transforms_failed']}")
        logger.info("=" * 100)

    # ── Phase 6: Final Report ────────────────────────────────────

    def phase_final_report(self):
        """Print comprehensive final report."""
        logger.info("")
        logger.info("=" * 100)
        logger.info("FINAL MAINTENANCE REPORT")
        logger.info("=" * 100)
        logger.info(f"Timestamp: {datetime.now():%Y-%m-%d %H:%M:%S}")
        logger.info(f"Log file: {log_file}")
        logger.info("")

        # Current DB stats (may fail if drive disconnected)
        logger.info(f"Database: {_cfg.db_path}")
        try:
            db_stats = self.fred.get_system_stats()
            logger.info(f"  Total series: {db_stats['total_series']:,}")
            logger.info(f"  Total data points: {db_stats['total_data_points']:,}")
            logger.info(f"  Database size: {db_stats['database_size_mb']:.1f} MB")
        except Exception as e:
            logger.error(f"  Cannot read DB stats: {e}")
        logger.info("")

        logger.info("Work done:")
        logger.info(f"  Metadata fixed: {self.stats['metadata_fixed']}")
        logger.info(f"  Series updated: {self.stats['series_updated']:,}")
        logger.info(f"  New data points added: {self.stats['new_points_total']:,}")
        logger.info(f"  Series already up-to-date: {self.stats['series_up_to_date']:,}")
        logger.info(f"  New series downloaded: {self.stats['series_downloaded']}")
        logger.info(f"  Transforms added: {self.stats['transforms_added']}")
        logger.info("")

        total_failed = (
            self.stats["metadata_failed"]
            + self.stats["series_failed"]
            + self.stats["series_download_failed"]
            + self.stats["transforms_failed"]
        )
        logger.info(f"  Total failures: {total_failed}")

        if self.stats["failed_series"]:
            logger.info("\n  Failed series detail (first 20):")
            for item in self.stats["failed_series"][:20]:
                logger.info(f"    {item['series_id']}: {item['error']}")

        # Save full report as JSON
        report_file = LOG_DIR / f"maintain_report_{datetime.now():%Y%m%d_%H%M%S}.json"
        with open(report_file, "w") as f:
            json.dump(
                {
                    "timestamp": datetime.now().isoformat(),
                    "db_stats": db_stats,
                    "maintenance_stats": self.stats,
                },
                f,
                indent=2,
                default=str,
            )
        logger.info(f"\nFull report saved: {report_file}")
        logger.info("=" * 100)

    # ── Helpers ──────────────────────────────────────────────────

    def _save_progress(self, completed: set):
        """Save resume checkpoint."""
        try:
            with open(self.progress_file, "w") as f:
                json.dump(
                    {
                        "phase3_completed": list(completed),
                        "last_saved": datetime.now().isoformat(),
                    },
                    f,
                )
        except Exception as e:
            logger.warning(f"Could not save progress: {e}")

    def _clear_progress(self):
        """Clear progress file after successful completion."""
        if self.progress_file.exists():
            self.progress_file.unlink()

    # ── Main Runner ──────────────────────────────────────────────

    def run_full(self):
        """Run all maintenance phases."""
        logger.info("=" * 100)
        logger.info("FRED DATABASE FULL MAINTENANCE")
        logger.info(f"Started: {datetime.now():%Y-%m-%d %H:%M:%S}")
        logger.info(f"Workers: {self.workers}")
        if self.max_age_days:
            logger.info(f"Max age filter: {self.max_age_days} days")
        logger.info("=" * 100)

        start = datetime.now()

        # Phase 1
        report = self.phase_health_check()

        # Phase 2
        self._check_drive()
        self.phase_fix_metadata()

        # Phase 3
        self._check_drive()
        self.phase_update_existing()

        # Phase 4
        self._check_drive()
        self.phase_download_missing(report.get("missing_list"))

        # Phase 5
        self._check_drive()
        self.phase_verify_transforms()

        # Phase 6
        self.phase_final_report()

        # Cleanup — only clear if no failures suggest incomplete work
        if self.stats["series_failed"] == 0:
            self._clear_progress()

        elapsed = (datetime.now() - start).total_seconds()
        logger.info(f"\nTotal time: {elapsed / 60:.1f} minutes")

    def run_phase(self, phase: str):
        """Run a specific phase."""
        phases = {
            "health": self.phase_health_check,
            "fix-meta": self.phase_fix_metadata,
            "update": self.phase_update_existing,
            "download": self.phase_download_missing,
            "transforms": self.phase_verify_transforms,
        }

        if phase not in phases:
            logger.error(f"Unknown phase: {phase}. Valid: {list(phases.keys())}")
            return

        phases[phase]()
        self.phase_final_report()


def main():
    parser = argparse.ArgumentParser(
        description="Comprehensive FRED database maintenance"
    )
    parser.add_argument(
        "--phase",
        type=str,
        default=None,
        help="Run specific phase: health, fix-meta, update, download, transforms",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=3,
        help="Number of parallel workers (default: 3)",
    )
    parser.add_argument(
        "--max-age",
        type=int,
        default=None,
        help="Only update series older than N days",
    )

    args = parser.parse_args()

    maintainer = FREDMaintenance(
        workers=args.workers, max_age_days=args.max_age
    )

    try:
        if args.phase:
            maintainer.run_phase(args.phase)
        else:
            maintainer.run_full()
    except KeyboardInterrupt:
        logger.warning("\n\nInterrupted. Progress saved — run again to resume.")
        maintainer.phase_final_report()
        sys.exit(1)
    except Exception as e:
        logger.error(f"\nFailed: {e}")
        maintainer.phase_final_report()
        raise


if __name__ == "__main__":
    main()
