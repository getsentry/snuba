from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.datasets.entities.storage_selectors import QueryStorageSelector
from snuba.datasets.entities.storage_selectors.selector import QueryStorageSelectorError
from snuba.datasets.storage import EntityStorageConnection, ReadableTableStorage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.logical import Query
from snuba.query.query_settings import OutcomesQuerySettings, QuerySettings

# Matches the hourly table TTL: timestamp + toIntervalDay(90).
HOURLY_RETENTION_DAYS = 90
HOURLY_RETENTION = timedelta(days=HOURLY_RETENTION_DAYS)


def hourly_retention_cutoff(now: datetime | None = None) -> datetime:
    if now is None:
        now = datetime.now(UTC)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    return now - HOURLY_RETENTION


class OutcomesStorageSelector(QueryStorageSelector):
    """
    Outcomes storage selector that decides whether to query the hourly or
    daily outcomes tables.

    Routing priority:
    1. OutcomesQuerySettings(use_daily=True) — explicit opt-in to daily.
    2. Time-range — if the query's lower timestamp bound is older than
       the hourly table's 90-day retention, route to daily.
    3. Referrer — if the referrer starts with "billing.", route to daily
       (billing queries need 13-month retention only available in the daily
       table).
    4. Default — hourly.

    OutcomesQuerySettings(use_daily=False) is not an opt-out: it falls
    through to time-range and referrer routing so old windows still reach
    the daily table.
    """

    def __init__(self) -> None:
        self.hourly_storage = StorageKey("outcomes_hourly")
        self.daily_storage = StorageKey("outcomes_daily")

    def select_storage(
        self,
        query: Query,
        query_settings: QuerySettings,
        storage_connections: Sequence[EntityStorageConnection],
    ) -> EntityStorageConnection:
        if isinstance(query_settings, OutcomesQuerySettings) and query_settings.get_use_daily():
            outcomes_key = self.daily_storage
        else:
            outcomes_key = self._select_storage_key(query, query_settings)

        for storage_connection in storage_connections:
            assert isinstance(storage_connection.storage, ReadableTableStorage)
            if storage_connection.storage.get_storage_key() == outcomes_key:
                return storage_connection

        raise QueryStorageSelectorError(
            "The specified storage in selector does not exist in storage list."
        )

    def _select_storage_key(self, query: Query, query_settings: QuerySettings) -> StorageKey:
        lower_bound, _ = get_time_range(query, "timestamp")
        if lower_bound is not None:
            if lower_bound.tzinfo is None:
                lower_bound = lower_bound.replace(tzinfo=UTC)
            if lower_bound < hourly_retention_cutoff():
                return self.daily_storage

        if query_settings.referrer.startswith("billing."):
            return self.daily_storage

        return self.hourly_storage
