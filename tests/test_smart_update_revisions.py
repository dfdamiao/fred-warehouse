"""The incremental updater must re-fetch a trailing window and seed orphans.

FRED revises recent observations in place (same date, new value). Fetching
only points strictly after the last stored date can never pick those up, so
the warehouse silently keeps the first-print value forever. The updater must
re-pull a trailing window and upsert the overlap.

A series that has a metadata row but zero data rows (an orphan) used to be
reported as ``no_data`` and never fetched; it must be seeded with full history.

Offline: storage and the FRED client are faked.
"""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from fred.smart_update_series import SmartFREDUpdater


class _FakeStorage:
    def __init__(self, existing: pd.DataFrame, metadata: dict | None):
        self._existing = existing
        self._metadata = metadata
        self.stored: list[tuple[str, pd.DataFrame, bool]] = []
        self.metadata_writes: list[dict] = []

    def get_data(self, series_id: str) -> pd.DataFrame:
        return self._existing

    def get_metadata(self, series_id: str) -> dict | None:
        return self._metadata

    def store_data(self, series_id: str, data: pd.DataFrame, replace_all: bool = True):
        self.stored.append((series_id, data, replace_all))

    def store_metadata(self, series_id: str, metadata: dict) -> None:
        self.metadata_writes.append(dict(metadata))


class _FakeClient:
    def __init__(self, upstream: pd.DataFrame):
        self._upstream = upstream
        self.calls: list[dict] = []

    def get_series_data(self, series_id, start_date=None, units=None, **_kw):
        self.calls.append({"series_id": series_id, "start_date": start_date})
        df = self._upstream
        if start_date is not None:
            df = df[df.index >= pd.Timestamp(start_date)]
        return df.copy()

    def get_series_info(self, series_id):
        return {"id": series_id, "frequency": "Monthly"}


def _updater(storage: _FakeStorage, client: _FakeClient) -> SmartFREDUpdater:
    up = object.__new__(SmartFREDUpdater)
    up.fred = SimpleNamespace(storage=storage)
    up.client = client
    up.stats = {"gaps_detected": 0}
    return up


def _monthly(values: list[float], start: str) -> pd.DataFrame:
    idx = pd.date_range(start, periods=len(values), freq="MS")
    return pd.DataFrame({"value": values}, index=idx)


def test_update_refetches_trailing_window_and_upserts_revisions() -> None:
    existing = _monthly([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], "2025-11-01")
    upstream = existing.copy()
    upstream.loc["2026-05-01", "value"] = 7.5  # FRED revised the May print
    upstream = pd.concat([upstream, _monthly([9.0], "2026-07-01")])  # new point
    storage = _FakeStorage(existing, {"frequency": "Monthly", "last_updated": None})
    client = _FakeClient(upstream)

    result = _updater(storage, client).update_single_series("TEST_lin")

    assert result["status"] == "updated", result
    assert client.calls[0]["start_date"] < "2026-06-01", (
        "must re-fetch a trailing window, not only points after the last date"
    )
    stored = pd.concat([df for _sid, df, _ra in storage.stored])
    assert pd.Timestamp("2026-05-01") in stored.index, "revised point not stored"
    assert stored.loc["2026-05-01", "value"] == 7.5
    assert pd.Timestamp("2026-07-01") in stored.index, "new point not stored"
    assert result["new_points"] == 1
    assert result["revised_points"] == 1
    assert all(replace_all is False for _s, _d, replace_all in storage.stored)


def test_unchanged_overlap_is_up_to_date() -> None:
    existing = _monthly([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], "2025-11-01")
    storage = _FakeStorage(existing, {"frequency": "Monthly", "last_updated": None})
    client = _FakeClient(existing.copy())

    result = _updater(storage, client).update_single_series("TEST_lin")

    assert result["status"] == "up_to_date", result
    assert storage.stored == []


def test_orphan_series_is_seeded_with_full_history() -> None:
    upstream = _monthly([1.0, 2.0, 3.0, 4.0, 5.0], "2026-03-01")
    storage = _FakeStorage(pd.DataFrame(), None)
    client = _FakeClient(upstream)

    result = _updater(storage, client).update_single_series("TEST_lin")

    assert result["status"] == "seeded", result
    assert result["new_points"] == 5
    assert len(storage.stored) == 1
    _sid, data, replace_all = storage.stored[0]
    assert replace_all is True and len(data) == 5
    assert client.calls[0]["start_date"] is None, "seed must pull full history"


def test_orphan_series_check_only_reports_needs_update() -> None:
    storage = _FakeStorage(pd.DataFrame(), None)
    client = _FakeClient(_monthly([1.0], "2026-03-01"))

    result = _updater(storage, client).update_single_series("TEST_lin", check_only=True)

    assert result["status"] == "needs_update", result
    assert client.calls == []
