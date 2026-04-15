from datetime import datetime, timedelta, timezone
from typing import Sequence

from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.datasets.entities.storage_selectors import QueryStorageSelector
from snuba.datasets.entities.storage_selectors.selector import QueryStorageSelectorError
from snuba.datasets.storage import EntityStorageConnection, ReadableTableStorage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.logical import Query
from snuba.query.query_settings import OutcomesQuerySettings, QuerySettings

# Queries starting earlier than this threshold are routed to the daily table
# because the hourly table only retains ~90 days of data.
_DAILY_THRESHOLD = timedelta(days=90)


class OutcomesStorageSelector(QueryStorageSelector):
    """
    Outcomes storage selector that decides whether to query the hourly or
    daily outcomes tables.

    Routing priority:
    1. OutcomesQuerySettings — honours the explicit use_daily flag.
    2. Time-range — if the query's lower timestamp bound is older than 90
       days, route to daily (the hourly table does not retain data that far
       back).
    3. Default — hourly.
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
        if isinstance(query_settings, OutcomesQuerySettings):
            outcomes_key = (
                self.daily_storage if query_settings.get_use_daily() else self.hourly_storage
            )
        else:
            outcomes_key = self._route_by_time_range(query)

        for storage_connection in storage_connections:
            assert isinstance(storage_connection.storage, ReadableTableStorage)
            if storage_connection.storage.get_storage_key() == outcomes_key:
                return storage_connection

        raise QueryStorageSelectorError(
            "The specified storage in selector does not exist in storage list."
        )

    def _route_by_time_range(self, query: Query) -> StorageKey:
        # Route to daily if the query reaches beyond the hourly table's
        # retention window (~90 days).
        lower_bound, _ = get_time_range(query, "timestamp")
        if lower_bound is not None:
            cutoff = datetime.now(timezone.utc) - _DAILY_THRESHOLD
            if lower_bound < cutoff:
                return self.daily_storage

        return self.hourly_storage
