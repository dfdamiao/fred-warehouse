"""DuckDB storage for FRED data."""

import duckdb
import pandas as pd
import logging
from pathlib import Path
from contextlib import contextmanager
from typing import Dict, List, Optional
from threading import Lock

logger = logging.getLogger(__name__)


class FREDStorage:
    """Simple DuckDB storage for FRED economic data."""

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = Lock()  # Thread-safe writes
        self._init_db()

    @contextmanager
    def connection(self):
        """Context manager for database connections."""
        conn = duckdb.connect(str(self.db_path))
        try:
            yield conn
        finally:
            conn.close()

    def _init_db(self):
        """Initialize database schema."""
        with self.connection() as conn:
            # Metadata table
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS series_metadata (
                    series_id VARCHAR PRIMARY KEY,
                    title VARCHAR,
                    units VARCHAR,
                    frequency VARCHAR,
                    seasonal_adjustment VARCHAR,
                    last_updated TIMESTAMP,
                    observation_start DATE,
                    observation_end DATE,
                    popularity INTEGER,
                    notes TEXT
                )
            """
            )

            # Data table
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS series_data (
                    series_id VARCHAR,
                    date DATE,
                    value DOUBLE,
                    PRIMARY KEY (series_id, date)
                )
            """
            )

            # Indexes
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_series_data_id ON series_data(series_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_series_data_date ON series_data(date)"
            )

        logger.info(f"Database initialized: {self.db_path}")

    def store_metadata(self, series_id: str, metadata: Dict):
        """Store series metadata - thread-safe."""
        with self._write_lock:
            with self.connection() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO series_metadata
                    (series_id, title, units, frequency, seasonal_adjustment,
                     last_updated, observation_start, observation_end, popularity, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    [
                        series_id,
                        metadata.get("title"),
                        metadata.get("units"),
                        metadata.get("frequency"),
                        metadata.get("seasonal_adjustment"),
                        metadata.get("last_updated"),
                        metadata.get("observation_start"),
                        metadata.get("observation_end"),
                        metadata.get("popularity", 0),
                        metadata.get("notes"),
                    ],
                )
                conn.commit()

    def store_data(self, series_id: str, data: pd.DataFrame, replace_all: bool = True):
        """
        Store series data - thread-safe.

        Args:
            series_id: Series identifier
            data: DataFrame with 'date' index and 'value' column
            replace_all: If True, delete existing data first. If False, insert/update only new rows.
        """
        if data.empty:
            logger.warning(f"No data to store for {series_id}")
            return

        # Prepare DataFrame with correct column order BEFORE lock
        data_copy = data.reset_index()
        data_copy["series_id"] = series_id
        data_copy = data_copy[["series_id", "date", "value"]]

        # Convert to list of tuples for batch insert (avoids DataFrame reference issues)
        values_to_insert = [tuple(row) for row in data_copy.values]

        # Use lock to prevent concurrent writes
        with self._write_lock:
            with self.connection() as conn:
                if replace_all:
                    # Full replace: delete all existing data for this series
                    conn.execute("DELETE FROM series_data WHERE series_id = ?", [series_id])
                    # Batch insert using executemany
                    conn.executemany(
                        "INSERT INTO series_data (series_id, date, value) VALUES (?, ?, ?)",
                        values_to_insert
                    )
                else:
                    # Incremental: use INSERT OR REPLACE for atomic upsert
                    # This handles duplicates without explicit DELETE
                    conn.executemany(
                        "INSERT OR REPLACE INTO series_data (series_id, date, value) VALUES (?, ?, ?)",
                        values_to_insert
                    )

                conn.commit()

        logger.info(f"Stored {len(data)} points for {series_id}")

    def get_data(
        self,
        series_id: str,
        start_date: str = None,
        end_date: str = None,
    ) -> pd.DataFrame:
        """Retrieve series data."""
        with self.connection() as conn:
            query = "SELECT date, value FROM series_data WHERE series_id = ?"
            params = [series_id]

            if start_date:
                query += " AND date >= ?"
                params.append(start_date)
            if end_date:
                query += " AND date <= ?"
                params.append(end_date)

            query += " ORDER BY date"

            df = conn.execute(query, params).df()

            if not df.empty:
                df["date"] = pd.to_datetime(df["date"])
                df.set_index("date", inplace=True)

            return df

    def get_metadata(self, series_id: str) -> Optional[Dict]:
        """Get series metadata."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT * FROM series_metadata WHERE series_id = ?", [series_id]
            ).fetchone()

            if result:
                columns = [desc[0] for desc in conn.description]
                return dict(zip(columns, result))

            return None

    def get_all_series(self) -> List[str]:
        """Get list of all stored series."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT series_id FROM series_metadata ORDER BY series_id"
            ).fetchall()
            return [row[0] for row in result]

    def get_stats(self) -> Dict:
        """Get database statistics."""
        with self.connection() as conn:
            series_count = conn.execute(
                "SELECT COUNT(*) FROM series_metadata"
            ).fetchone()[0]
            data_points = conn.execute("SELECT COUNT(*) FROM series_data").fetchone()[0]

            return {
                "total_series": series_count,
                "total_data_points": data_points,
                "database_size_mb": (
                    self.db_path.stat().st_size / (1024 * 1024)
                    if self.db_path.exists()
                    else 0
                ),
            }

    # Method from Perplexity request
    def append_data(self, series_id: str, new_data: pd.DataFrame):
        """Append new data rows only (handles duplicates safely)."""
        # Format data for table
        new_data = (
            new_data.reset_index()
            .rename(columns={"DATE": "date", series_id: "value"})
            .assign(series_id=series_id)[["series_id", "date", "value"]]
        )

        # UPSERT: Ignore duplicates on PRIMARY KEY
        new_data.to_sql("fred_data", self.conn, if_exists="append", index=False)
