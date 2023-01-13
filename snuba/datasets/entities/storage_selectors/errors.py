from typing import List

from snuba import state
from snuba.datasets.entities.storage_selectors.selector import QueryStorageSelector
from snuba.datasets.storage import (
    EntityStorageConnection,
    EntityStorageConnectionNotFound,
)
from snuba.query.logical import Query
from snuba.query.query_settings import QuerySettings


class ErrorsQueryStorageSelector(QueryStorageSelector):
    # NOTE: This storage selector does not support multiple readable storages.
    def select_storage(
        self,
        query: Query,
        query_settings: QuerySettings,
        storage_connections: List[EntityStorageConnection],
    ) -> EntityStorageConnection:
        use_readonly_storage = (
            state.get_config("enable_events_readonly_table", False)
            and not query_settings.get_consistent()
        )

        if use_readonly_storage:
            storage = self.get_readable_storage_connection(storage_connections)
        else:
            storage = self.get_writable_storage_connection(storage_connections)
        if storage:
            return storage
        raise EntityStorageConnectionNotFound("Cannot find storage.")
