from typing import Sequence

from snuba import state
from snuba.datasets.entities.storage_selectors import QueryStorageSelector
from snuba.datasets.storage import (
    EntityStorageConnection,
    EntityStorageConnectionNotFound,
    ReadableTableStorage,
    WritableTableStorage,
)
from snuba.query.logical import Query
from snuba.query.query_settings import QuerySettings


class ErrorsQueryStorageSelector(QueryStorageSelector):
    # NOTE: This storage selector does not support multiple readable storages.
    def select_storage(
        self,
        query: Query,
        query_settings: QuerySettings,
        storage_connections: Sequence[EntityStorageConnection],
    ) -> EntityStorageConnection:
        use_readonly_storage = (
            state.get_config("enable_events_readonly_table", False)
            and not query_settings.get_consistent()
        )

        if use_readonly_storage:
            for storage_connection in storage_connections:
                if (
                    not storage_connection.is_writable
                    and type(storage_connection.storage) is ReadableTableStorage
                ):
                    return storage_connection
        else:
            for storage_connection in storage_connections:
                if storage_connection.is_writable and isinstance(
                    storage_connection.storage, WritableTableStorage
                ):
                    return storage_connection
        raise EntityStorageConnectionNotFound("Cannot find storage.")
