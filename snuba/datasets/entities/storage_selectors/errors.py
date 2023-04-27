import random
from typing import List

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


def _should_use_readonly_storage(need_consistent: bool) -> bool:
    """
    Helper method to determine if we should use the readonly storage for the
    errors storage selector.
    """
    if not state.get_config("enable_events_readonly_table", False):
        return False

    if need_consistent:
        return False

    force_consistent_sample_rate = state.get_config(
        "errors_force_consistent_sample_rate", 0.0
    )
    if (
        force_consistent_sample_rate is not None
        and random.random() < force_consistent_sample_rate
    ):
        return False

    return True


class ErrorsQueryStorageSelector(QueryStorageSelector):
    # NOTE: This storage selector does not support multiple readable storages.
    def select_storage(
        self,
        query: Query,
        query_settings: QuerySettings,
        storage_connections: List[EntityStorageConnection],
    ) -> EntityStorageConnection:
        use_readonly_storage = _should_use_readonly_storage(
            query_settings.get_consistent()
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
