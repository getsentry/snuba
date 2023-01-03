from typing import List

from snuba import state
from snuba.datasets.entities.storage_selectors.selector import QueryStorageSelector
from snuba.datasets.storage import StorageAndMappers
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.logical import Query
from snuba.query.query_settings import QuerySettings


class ErrorsQueryStorageSelector(QueryStorageSelector):
    def __init__(self) -> None:
        self.__errors_table = StorageKey.ERRORS
        self.__errors_ro_table = StorageKey.ERRORS_RO

    def select_storage(
        self,
        query: Query,
        query_settings: QuerySettings,
        storage_and_mappers_list: List[StorageAndMappers],
    ) -> StorageAndMappers:
        use_readonly_storage = (
            state.get_config("enable_events_readonly_table", False)
            and not query_settings.get_consistent()
        )

        if use_readonly_storage:
            return self.get_storage_mapping_pair(
                self.__errors_ro_table, storage_and_mappers_list
            )
        return self.get_storage_mapping_pair(
            self.__errors_table, storage_and_mappers_list
        )
