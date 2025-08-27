from typing import Sequence

from snuba.datasets.entities.storage_selectors import QueryStorageSelector
from snuba.datasets.entities.storage_selectors.selector import QueryStorageSelectorError
from snuba.datasets.storage import EntityStorageConnection, ReadableTableStorage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.logical import Query
from snuba.query.query_settings import OutcomesQuerySettings, QuerySettings


class OutcomesStorageSelector(QueryStorageSelector):
    """
    Outcomes storage selector to decide whether to query the hourly or daily
    outcomes tables
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
                self.daily_storage
                if query_settings.get_use_daily()
                else self.hourly_storage
            )
        else:
            outcomes_key = self.hourly_storage

        for storage_connection in storage_connections:
            assert isinstance(storage_connection.storage, ReadableTableStorage)
            if storage_connection.storage.get_storage_key() == outcomes_key:
                return storage_connection
        raise QueryStorageSelectorError(
            "The specified storage in selector does not exist in storage list."
        )
