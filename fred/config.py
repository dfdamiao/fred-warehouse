"""FRED configuration loader — the single source for the API key(s) and the
database path.

Resolution order for the config file:
    1. ``$FRED_CONFIG_PATH`` (if set)
    2. ``./config.json`` (current working directory)
    3. ``<repo root>/config.json``

Individual values can also be overridden by environment variables
(``FRED_API_KEY_PRIMARY``, ``FRED_API_KEY_SECONDARY``, ``FRED_API_KEY_TERTIARY``,
``FRED_DB_PATH``), which take precedence over the file. This means you can use a
``.env``-style workflow instead of (or on top of) ``config.json``.

Copy ``config.example.json`` to ``config.json`` and paste your free FRED API key
(https://fred.stlouisfed.org/docs/api/api_key.html) to get started.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

DEFAULT_BASE_URL = "https://api.stlouisfed.org/fred"
DEFAULT_DB_PATH = "./fred_data.db"
_PLACEHOLDER_KEY = "YOUR_FRED_API_KEY"


def _find_config_file() -> Path | None:
    env_path = os.environ.get("FRED_CONFIG_PATH")
    if env_path:
        return Path(env_path)
    for candidate in (
        Path.cwd() / "config.json",
        Path(__file__).resolve().parent.parent / "config.json",
    ):
        if candidate.exists():
            return candidate
    return None


class FREDConfig:
    """Load and access FRED configuration (JSON file + environment overrides)."""

    def __init__(self, config_path: str | os.PathLike | None = None):
        path = Path(config_path) if config_path else _find_config_file()
        self.config_path = path
        if path and path.exists():
            with open(path) as f:
                self._config = json.load(f)
        else:
            # No file is fine as long as the key comes from the environment.
            self._config = {}

    def get(self, key: str, default: Any = None) -> Any:
        """Get a config value using dot notation, e.g. 'database.duckdb_path'."""
        value: Any = self._config
        for k in key.split("."):
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
            if value is None:
                return default
        return value

    @property
    def api_key_primary(self) -> str:
        """Primary FRED API key (required). Env var wins over the file."""
        key = os.environ.get("FRED_API_KEY_PRIMARY") or self.get(
            "fred_api.api_key_primary", ""
        )
        if not key or key == _PLACEHOLDER_KEY:
            raise RuntimeError(
                "No FRED API key configured. Copy config.example.json to "
                "config.json and set fred_api.api_key_primary to your free key "
                "(https://fred.stlouisfed.org/docs/api/api_key.html), or set the "
                "FRED_API_KEY_PRIMARY environment variable."
            )
        return key

    @property
    def api_key_secondary(self) -> str:
        return os.environ.get("FRED_API_KEY_SECONDARY") or self.get(
            "fred_api.api_key_secondary", ""
        )

    @property
    def api_key_tertiary(self) -> str:
        return os.environ.get("FRED_API_KEY_TERTIARY") or self.get(
            "fred_api.api_key_tertiary", ""
        )

    @property
    def extra_keys(self) -> list[str]:
        """Optional extra keys (secondary, tertiary) for higher rate limits."""
        return [k for k in (self.api_key_secondary, self.api_key_tertiary) if k]

    @property
    def base_url(self) -> str:
        return self.get("fred_api.base_url") or DEFAULT_BASE_URL

    @property
    def db_path(self) -> str:
        """Where the DuckDB database lives. Put it wherever you want."""
        return (
            os.environ.get("FRED_DB_PATH")
            or self.get("database.duckdb_path")
            or DEFAULT_DB_PATH
        )
