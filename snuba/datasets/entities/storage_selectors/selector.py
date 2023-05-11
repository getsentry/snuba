from typing import Sequence

from snuba.datasets.entities.storage_selectors import QueryStorageSelector
from snuba.datasets.storage import EntityStorageConnection, ReadableTableStorage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.logical import Query
from snuba.query.query_settings import QuerySettings


class QueryStorageSelectorError(Exception):
    pass


class DefaultQueryStorageSelector(QueryStorageSelector):
    """
    A default query storage selector which chooses the only storage specified in config.
    Entities which define multiple storages should not use this selector and should use
    custom ones.
    """

    def select_storage(
        self,
        query: Query,
        query_settings: QuerySettings,
        storage_connections: Sequence[EntityStorageConnection],
    ) -> EntityStorageConnection:
        assert len(storage_connections) == 1
        return storage_connections[0]


class SimpleQueryStorageSelector(QueryStorageSelector):
    """
    A simple query storage selector which selects the storage passed an input.
    """

    def __init__(self, storage: str):
        self.storage_key = StorageKey(storage)

    def select_storage(
        self,
        query: Query,
        query_settings: QuerySettings,
        storage_connections: Sequence[EntityStorageConnection],
    ) -> EntityStorageConnection:
        for storage_connection in storage_connections:
            assert isinstance(storage_connection.storage, ReadableTableStorage)
            if storage_connection.storage.get_storage_key() == self.storage_key:
                return storage_connection
        raise QueryStorageSelectorError(
            "The specified storage in selector does not exist in storage list."
        )
