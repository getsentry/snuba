from typing import List

from snuba import state
from snuba.datasets.entities.storage_selectors.selector import QueryStorageSelector
from snuba.datasets.storage import StorageAndMappers, StorageAndMappersNotFound
from snuba.query.logical import Query
from snuba.query.query_settings import QuerySettings


class ErrorsQueryStorageSelector(QueryStorageSelector):
    def select_storage(
        self,
        query: Query,
        query_settings: QuerySettings,
        storage_and_mappers: List[StorageAndMappers],
    ) -> StorageAndMappers:
        use_readonly_storage = (
            state.get_config("enable_events_readonly_table", False)
            and not query_settings.get_consistent()
        )

        if use_readonly_storage:
            storage = self.get_readable_storage_mapping(storage_and_mappers)
        else:
            storage = self.get_writable_storage_mapping(storage_and_mappers)
        if storage:
            return storage
        raise StorageAndMappersNotFound("Cannot find storage.")
