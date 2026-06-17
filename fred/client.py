"""FRED API client with dual key rotation."""
import time
import logging
from datetime import datetime, timedelta
from typing import Dict
from threading import Lock
import requests
import pandas as pd

logger = logging.getLogger(__name__)


class FREDClient:
    """FRED API client with dual-key round-robin and thread-safe rate limiting.

    When two API keys are provided, requests alternate between them so each
    key stays under its own 120 req/min ceiling — effectively doubling
    throughput to ~240 req/min.
    """

    REQUESTS_PER_MIN = 120  # FRED API limit per key
    SAFE_THRESHOLD = 110    # switch/sleep before hitting hard limit

    def __init__(
        self,
        primary_key: str,
        secondary_key: str | None = None,
        base_url: str | None = None,
        extra_keys: list[str] | None = None,
    ):
        self.base_url = base_url or "https://api.stlouisfed.org/fred"

        # Build key pool — all provided keys
        self._keys: list[str] = [primary_key]
        if secondary_key:
            self._keys.append(secondary_key)
        if extra_keys:
            self._keys.extend(extra_keys)

        # Per-key rate tracking
        self._key_times: list[list[datetime]] = [[] for _ in self._keys]
        self._next_key = 0
        self._lock = Lock()

    def _pick_key(self) -> str:
        """Round-robin key selection with per-key rate limiting (thread-safe)."""
        with self._lock:
            now = datetime.now()
            cutoff = now - timedelta(minutes=1)
            num_keys = len(self._keys)

            # Try each key starting from the next in rotation
            for attempt in range(num_keys):
                idx = (self._next_key + attempt) % num_keys
                times = self._key_times[idx]

                # Purge requests older than 1 min
                times[:] = [t for t in times if t > cutoff]

                if len(times) < self.SAFE_THRESHOLD:
                    # This key has headroom — use it
                    times.append(now)
                    self._next_key = (idx + 1) % num_keys
                    return self._keys[idx]

            # All keys near limit — wait on the one closest to freeing a slot
            idx = self._next_key
            times = self._key_times[idx]
            times[:] = [t for t in times if t > cutoff]
            if times:
                sleep_time = 60 - (now - times[0]).total_seconds()
                if sleep_time > 0:
                    logger.info(f"All keys near limit — sleeping {sleep_time:.1f}s")
                    time.sleep(sleep_time)
                times[:] = []

            times.append(datetime.now())
            self._next_key = (idx + 1) % num_keys
            return self._keys[idx]

    def _request(self, endpoint: str, params: Dict) -> Dict:
        """Make API request with round-robin key selection and retry."""
        max_retries = 3
        backoff_delays = [30, 60, 120]

        for attempt in range(max_retries):
            api_key = self._pick_key()
            params["api_key"] = api_key
            params["file_type"] = "json"
            url = f"{self.base_url}/{endpoint}"

            try:
                response = requests.get(url, params=params, timeout=30)

                if response.status_code == 200:
                    return response.json()

                if response.status_code == 403:
                    delay = backoff_delays[min(attempt, len(backoff_delays) - 1)]
                    logger.warning(
                        f"403 Forbidden (Akamai IP block). "
                        f"Attempt {attempt + 1}/{max_retries}, "
                        f"sleeping {delay}s..."
                    )
                    time.sleep(delay)
                    continue

                if response.status_code == 429:
                    delay = backoff_delays[min(attempt, len(backoff_delays) - 1)]
                    logger.warning(
                        f"429 Rate limit. "
                        f"Attempt {attempt + 1}/{max_retries}, "
                        f"sleeping {delay}s..."
                    )
                    time.sleep(delay)
                    continue

                raise Exception(
                    f"API error {response.status_code}: {response.text}"
                )

            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(backoff_delays[attempt])
                    continue
                raise Exception(f"Request failed: {str(e)}")

        raise Exception(
            f"Failed after {max_retries} retries (403/429 rate limiting)"
        )

    def get_series_info(self, series_id: str) -> Dict:
        """Get series metadata."""
        response = self._request("series", {"series_id": series_id})

        if "seriess" in response and response["seriess"]:
            return response["seriess"][0]

        raise Exception(f"Series not found: {series_id}")

    def get_series_data(
        self, series_id: str, start_date: str = None, end_date: str = None, units: str = None
    ) -> pd.DataFrame:
        """
        Get series observations as DataFrame.

        Args:
            series_id: FRED series ID
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            units: Transformation code (lin, chg, ch1, pch, pc1, pca, cch, cca, log)
                   lin = Levels (no transformation)
                   chg = Change
                   ch1 = Change from Year Ago
                   pch = Percent Change
                   pc1 = Percent Change from Year Ago
                   pca = Compounded Annual Rate of Change
                   cch = Continuously Compounded Rate of Change
                   cca = Continuously Compounded Annual Rate of Change
                   log = Natural Log

        Returns:
            DataFrame with date index and value column
        """
        params = {"series_id": series_id}

        if start_date:
            params["observation_start"] = start_date
        if end_date:
            params["observation_end"] = end_date
        if units:
            params["units"] = units

        response = self._request("series/observations", params)

        if "observations" not in response or not response["observations"]:
            logger.warning(f"No data for series {series_id}")
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(response["observations"])
        df["date"] = pd.to_datetime(df["date"])
        df["value"] = df["value"].replace(".", None)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["value"])
        df.set_index("date", inplace=True)

        return df[["value"]]

    def test_connection(self) -> bool:
        """Test API connection."""
        try:
            self.get_series_info("GDP")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
