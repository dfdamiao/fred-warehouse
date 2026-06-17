"""
FRED Data Access - Simplified economic data system.

Usage:
    from fred import FREDDataAccess

    fred = FREDDataAccess()
    data = fred.get_series_data("FEDFUNDS", period="1y")
"""
from .data_access import FREDDataAccess
from .client import FREDClient
from .storage import FREDStorage

__all__ = ["FREDDataAccess", "FREDClient", "FREDStorage"]
