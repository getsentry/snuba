from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.datasets.entities.storage_selectors import QueryStorageSelector
from snuba.datasets.entities.storage_selectors.selector import QueryStorageSelectorError
from snuba.datasets.storage import EntityStorageConnection, ReadableTableStorage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.logical import Query
from snuba.query.query_settings import OutcomesQuerySettings, QuerySettings

# Hourly TTL is 90 days. ClickHouse drops parts after that, so route a day
# early and include the cutoff itself to avoid querying data that may
# already have been evicted.
_HOURLY_RETENTION = timedelta(days=90)
_HOURLY_RETENTION_BUFFER = timedelta(days=1)


class OutcomesStorageSelector(QueryStorageSelector):
    """
    Outcomes storage selector that decides whether to query the hourly or
    daily outcomes tables.

    Routing priority:
    1. OutcomesQuerySettings(use_daily=True) — explicit opt-in to daily.
    2. Time-range — if the query's lower timestamp bound is at or beyond
       hourly retention (90 days, minus a 1-day buffer), route to daily.
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
            outcomes_key = self._route_by_time_and_referrer(query, query_settings)

        for storage_connection in storage_connections:
            assert isinstance(storage_connection.storage, ReadableTableStorage)
            if storage_connection.storage.get_storage_key() == outcomes_key:
                return storage_connection

        raise QueryStorageSelectorError(
            "The specified storage in selector does not exist in storage list."
        )

    def _route_by_time_and_referrer(
        self, query: Query, query_settings: QuerySettings
    ) -> StorageKey:
        # Route to daily if the query reaches beyond the hourly table's
        # retention window (~90 days).
        lower_bound, _ = get_time_range(query, "timestamp")
        if lower_bound is not None:
            lower_bound_tz = (
                lower_bound if lower_bound.tzinfo is not None else lower_bound.replace(tzinfo=UTC)
            )
            cutoff = datetime.now(UTC) - _HOURLY_RETENTION + _HOURLY_RETENTION_BUFFER
            if lower_bound_tz <= cutoff:
                return self.daily_storage

        # Billing queries need 13-month retention available only in the
        # daily table. The referrer is set by sentry's UsageService.
        if query_settings.referrer.startswith("billing."):
            return self.daily_storage

        return self.hourly_storage
