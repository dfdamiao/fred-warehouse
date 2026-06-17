"""Main FRED data access interface (mirrors MarketDataAccess pattern)."""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .client import FREDClient
from .config import FREDConfig
from .storage import FREDStorage

logger = logging.getLogger(__name__)


def parse_period(period: str) -> tuple:
    """Parse period string to start/end dates."""
    end_date = datetime.now()

    if period == "1d":
        start_date = end_date - timedelta(days=1)
    elif period == "1mo":
        start_date = end_date - timedelta(days=30)
    elif period == "3mo":
        start_date = end_date - timedelta(days=90)
    elif period == "6mo":
        start_date = end_date - timedelta(days=180)
    elif period == "1y":
        start_date = end_date - timedelta(days=365)
    elif period == "2y":
        start_date = end_date - timedelta(days=730)
    elif period == "5y":
        start_date = end_date - timedelta(days=1825)
    elif period == "ytd":
        start_date = datetime(end_date.year, 1, 1)
    elif period == "max":
        start_date = datetime(1900, 1, 1)
    else:
        start_date = end_date - timedelta(days=365)

    return start_date, end_date


class FREDDataAccess:
    """
    Main interface for FRED data access.
    Mirrors MarketDataAccess pattern for consistency.
    """

    def __init__(self, db_path: str = None, config: FREDConfig = None):
        cfg = config or FREDConfig()
        self.storage = FREDStorage(db_path or cfg.db_path)
        self.client = FREDClient(
            primary_key=cfg.api_key_primary,
            extra_keys=cfg.extra_keys,
            base_url=cfg.base_url,
        )

    def get_series_data(
        self,
        series_id: str,
        period: str = None,
        start_date: str = None,
        end_date: str = None,
    ) -> pd.DataFrame:
        """
        Get data for a single series.

        Args:
            series_id: FRED series ID (e.g., "FEDFUNDS", "GDP")
            period: Period string ("1mo", "1y", "ytd", "max")
            start_date: Start date YYYY-MM-DD
            end_date: End date YYYY-MM-DD

        Returns:
            DataFrame with date index and value column
        """
        if period and not (start_date and end_date):
            start_dt, end_dt = parse_period(period)
            start_date = start_dt.strftime("%Y-%m-%d")
            end_date = end_dt.strftime("%Y-%m-%d")

        data = self.storage.get_data(series_id, start_date, end_date)

        if data.empty:
            logger.warning(f"No data for {series_id}")

        return data

    def get_multiple_series(
        self,
        series_ids: List[str],
        period: str = None,
        start_date: str = None,
        end_date: str = None,
        fill_method: str = "ffill",
    ) -> pd.DataFrame:
        """
        Get data for multiple series as aligned DataFrame.

        Args:
            series_ids: List of FRED series IDs
            period: Period string
            start_date: Start date
            end_date: End date
            fill_method: "ffill", "bfill", or None

        Returns:
            DataFrame with date index, columns = series_ids
        """
        all_data = {}

        for series_id in series_ids:
            data = self.get_series_data(series_id, period, start_date, end_date)
            if not data.empty:
                all_data[series_id] = data["value"]

        if not all_data:
            return pd.DataFrame()

        combined = pd.DataFrame(all_data)

        if fill_method == "ffill":
            combined = combined.ffill()
        elif fill_method == "bfill":
            combined = combined.bfill()

        return combined

    def get_latest_values(self, series_ids: List[str]) -> pd.DataFrame:
        """
        Get latest values for multiple series.

        Returns:
            DataFrame with series_id, latest_date, latest_value
        """
        results = []

        for series_id in series_ids:
            data = self.get_series_data(series_id, period="max")
            if not data.empty:
                results.append(
                    {
                        "series_id": series_id,
                        "latest_date": data.index[-1],
                        "latest_value": data["value"].iloc[-1],
                    }
                )

        return pd.DataFrame(results) if results else pd.DataFrame()

    def export_to_csv(
        self,
        series_ids: List[str],
        output_path: str,
        period: str = "max",
        single_file: bool = True,
    ):
        """
        Export series to CSV.

        Args:
            series_ids: List of series IDs
            output_path: Output file or directory path
            period: Period to export
            single_file: If True, combine all series in one file
        """
        output_path = Path(output_path)

        if single_file:
            combined = self.get_multiple_series(series_ids, period=period)
            combined.to_csv(output_path)
            logger.info(f"Exported {len(series_ids)} series to {output_path}")
        else:
            output_path.mkdir(parents=True, exist_ok=True)
            for series_id in series_ids:
                data = self.get_series_data(series_id, period=period)
                if not data.empty:
                    file_path = output_path / f"{series_id}.csv"
                    data.to_csv(file_path)
            logger.info(f"Exported {len(series_ids)} series to {output_path}/")

    def export_to_parquet(
        self,
        series_ids: List[str],
        output_path: str,
        period: str = "max",
    ):
        """Export series to Parquet format."""
        combined = self.get_multiple_series(series_ids, period=period)
        combined.to_parquet(output_path, compression="snappy")
        logger.info(f"Exported {len(series_ids)} series to {output_path}")

    def get_correlation_matrix(
        self, series_ids: List[str], period: str = "2y"
    ) -> pd.DataFrame:
        """Calculate correlation matrix for multiple series."""
        combined = self.get_multiple_series(
            series_ids, period=period, fill_method="ffill"
        )

        if combined.empty:
            return pd.DataFrame()

        return combined.corr()

    def get_metadata(self, series_id: str) -> Optional[Dict]:
        """Get metadata for a series."""
        return self.storage.get_metadata(series_id)

    def get_available_series(self) -> List[str]:
        """Get list of all available series."""
        return self.storage.get_all_series()

    def get_system_stats(self) -> Dict:
        """Get system statistics."""
        return self.storage.get_stats()

    def download_series(self, series_id: str, replace: bool = True, units: str = None) -> bool:
        """
        Download a series from FRED API and store locally.

        Args:
            series_id: FRED series ID
            replace: If True, replace existing data
            units: Transformation code (lin, chg, ch1, pch, pc1, pca, cch, cca, log)

        Returns:
            True if successful
        """
        try:
            # Get metadata
            metadata = self.client.get_series_info(series_id)
            self.storage.store_metadata(series_id, metadata)

            # Get data with optional transformation
            data = self.client.get_series_data(series_id, units=units)
            if not data.empty:
                self.storage.store_data(series_id, data)
                units_str = f" (units={units})" if units else ""
                logger.info(f"Downloaded {series_id}{units_str}: {len(data)} points")
                return True
            else:
                logger.warning(f"No data available for {series_id}")
                return False

        except Exception as e:
            logger.error(f"Failed to download {series_id}: {e}")
            return False

    def download_multiple_series(
        self, series_ids: List[str], show_progress: bool = True
    ) -> Dict:
        """
        Download multiple series.

        Returns:
            Dictionary with success/failure counts
        """
        stats = {
            "total": len(series_ids),
            "success": 0,
            "failed": 0,
            "failed_series": [],
        }

        for i, series_id in enumerate(series_ids):
            if show_progress and (i + 1) % 10 == 0:
                print(f"Progress: {i+1}/{len(series_ids)} series...")

            if self.download_series(series_id):
                stats["success"] += 1
            else:
                stats["failed"] += 1
                stats["failed_series"].append(series_id)

        return stats

    # Old method kept for reference - from Claude
    # def update_series(self, series_id: str) -> bool:
    #     """
    #     Update an existing series (only new data points).

    #     Returns:
    #         True if successful
    #     """
    #     try:
    #         # Get latest date we have
    #         existing_data = self.storage.get_data(series_id)

    #         if existing_data.empty:
    #             # No existing data, do full download
    #             return self.download_series(series_id)

    #         last_date = existing_data.index.max().strftime("%Y-%m-%d")

    #         # Fetch new data starting from last date
    #         new_data = self.client.get_series_data(series_id, start_date=last_date)

    #         if not new_data.empty:
    #             # Combine and store
    #             combined = pd.concat([existing_data, new_data]).drop_duplicates()
    #             self.storage.store_data(series_id, combined)
    #             logger.info(f"Updated {series_id}: {len(new_data)} new points")
    #             return True
    #         else:
    #             logger.info(f"{series_id} already up to date")
    #             return True

    #     except Exception as e:
    #         logger.error(f"Failed to update {series_id}: {e}")
    #         return False

    # def update_series(self, series_id: str) -> bool:
    #     """Safe update using existing storage methods."""
    #     try:
    #         # Get current data
    #         existing_data = self.storage.get_data(series_id)
    #         if existing_data.empty:
    #             return self.download_series(series_id)

    #         # Fetch ONLY new data (strictly after last date)
    #         last_date = existing_data.index.max()
    #         start_date = (last_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    #         new_data = self.client.get_series_data(series_id, start_date=start_date)
    #         if new_data.empty:
    #             return True

    #         # Combine + dedupe IN MEMORY before storage
    #         combined = (
    #             pd.concat([existing_data, new_data]).drop_duplicates().sort_index()
    #         )
    #         self.storage.store_data(series_id, combined)  # This handles REPLACE safely
    #         print(
    #             f"✓ {series_id}: +{len(new_data)} points"
    #         )  # Use print for script visibility
    #         return True

    #     except Exception as e:
    #         print(f"✗ Failed {series_id}: {e}")
    #         return False

    # Failed to update A31SNO: Constraint Error: PRIMARY KEY or UNIQUE constraint violation: duplicate key "A31SNO, 2025-08-01"
    # Failed to update A32SNO: Constraint Error: PRIMARY KEY or UNIQUE constraint violation: duplicate key "A32SNO, 2025-08-01"
    # Failed to update A33SNO: Constraint Error: PRIMARY KEY or UNIQUE constraint violation: duplicate key "A33SNO, 2025-08-01"
    # Need to know if onyl new data is being fetched or all of it
    # Error after last fix : ✗ Failed WPU46: Constraint Error: PRIMARY KEY or UNIQUE constraint violation: duplicate key "WPU46, 2025-08-01"
    # ✗ Failed WPU461: Constraint Error: PRIMARY KEY or UNIQUE constraint violation: duplicate key "WPU461, 2025-08-01"
    # ✗ Failed WPU4611: Constraint Error: PRIMARY KEY or UNIQUE constraint violation: duplicate key "WPU4611, 2025-08-01"
    # ✗ Failed WPU461101: Constraint Error: PRIMARY KEY or UNIQUE constraint violation: duplicate key "WPU461101, 2025-08-01"
    # ✗ Failed WPU46110101: Constraint Error: PRIMARY KEY or UNIQUE constraint violation: duplicate key "WPU46110101, 2025-08-01"
    # ✗ Failed WPU462: Constraint Error: PRIMARY KEY or UNIQUE constraint violation: duplicate key "WPU462, 2025-08-01"
    # Progress: 3110/3399...

    def update_series(self, series_id: str, units: str = None) -> bool:
        """
        Update an existing series with new data.

        Args:
            series_id: FRED series ID
            units: Optional transformation code (lin, chg, ch1, pch, pc1, pca, cch, cca, log)

        Returns:
            True if successful
        """
        try:
            existing_data = self.storage.get_data(series_id)
            if existing_data.empty:
                return self.download_series(series_id, units=units)

            # Get new data only
            last_date = existing_data.index.max()
            start_date = (last_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            new_data = self.client.get_series_data(series_id, start_date=start_date, units=units)

            if new_data.empty:
                return True

            # ORIGINAL MAGIC: concat + drop_duplicates on INDEX (dates)
            combined = (
                pd.concat([existing_data, new_data]).drop_duplicates().sort_index()
            )
            self.storage.store_data(series_id, combined)

            print(f"✓ {series_id}: +{len(new_data)} points")
            return True

        except Exception as e:
            print(f"✗ Failed {series_id}: {e}")
            return False
