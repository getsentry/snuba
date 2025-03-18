from __future__ import annotations

from typing import Optional, Sequence, cast

import sentry_sdk

from snuba.clickhouse.query import Query
from snuba.clusters.cluster import ClickhouseCluster
from snuba.datasets.entities.storage_selectors import QueryStorageSelector
from snuba.datasets.entities.storage_selectors.selector import QueryStorageSelectorError
from snuba.datasets.plans.cluster_selector import ColumnBasedStorageSliceSelector
from snuba.datasets.plans.storage_processing import (
    check_storage_readiness,
    get_query_data_source,
)
from snuba.datasets.plans.translator.query import QueryTranslator
from snuba.datasets.slicing import is_storage_set_sliced
from snuba.datasets.storage import (
    EntityStorageConnection,
    ReadableStorage,
    ReadableTableStorage,
)
from snuba.query.logical import EntityQuery
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings
from snuba.state import explain_meta


class EntityProcessingExecutor:
    """
    An executor created by an entity that is responsible for applying everything related
    to entity processing to a query. This executor applies the following processing steps:
        1. Applies entity processors defined on the entity
        2. Selects a storage based on the storage selector
        3. Applies translation mappers
        4. Translates the logical query to physical query
    """

    def __init__(
        self,
        storages: Sequence[EntityStorageConnection],
        selector: QueryStorageSelector,
        post_processors: Optional[Sequence[ClickhouseQueryProcessor]] = None,
        partition_key_column_name: Optional[str] = None,
    ) -> None:
        # A list of storages and the translation mappers they are associated with.
        # This list will only contain one storage and mappers for single storage entities.
        # If there are more than one storage and mappers, a selector is required
        self.__storages = storages
        # A storage selector class to determine which to use in query plan
        self.__selector = selector
        # This is a set of query processors that have to be executed on the
        # query after the storage selection but that are defined by the dataset.
        self.__post_processors = post_processors or []
        self.__partition_key_column_name = partition_key_column_name

    def get_storage(
        self, query: LogicalQuery, settings: QuerySettings
    ) -> EntityStorageConnection:
        with sentry_sdk.start_span(
            op="build_plan.storage_query_plan_builder", description="select_storage"
        ):
            return self.__selector.select_storage(query, settings, self.__storages)

    def get_cluster(
        self, storage: ReadableStorage, query: LogicalQuery, settings: QuerySettings
    ) -> ClickhouseCluster:
        if is_storage_set_sliced(storage.get_storage_set_key()):
            with sentry_sdk.start_span(
                op="build_plan.sliced_storage", description="select_storage"
            ):
                assert (
                    self.__partition_key_column_name is not None
                ), "partition key column name must be defined for a sliced storage"
                assert isinstance(storage, ReadableTableStorage)
                return ColumnBasedStorageSliceSelector(
                    storage=storage.get_storage_key(),
                    storage_set=storage.get_storage_set_key(),
                    partition_key_column_name=self.__partition_key_column_name,
                ).select_cluster(query, settings)
        return storage.get_cluster()

    def translate_query_and_apply_mappers(
        self, query: LogicalQuery, settings: QuerySettings
    ) -> Query:
        if len(self.__storages) < 1:
            raise QueryStorageSelectorError("No storages specified to select from.")
        storage_connection = self.get_storage(query, settings)
        storage = storage_connection.storage
        mappers = storage_connection.translation_mappers

        check_storage_readiness(storage)

        with sentry_sdk.start_span(
            op="build_plan.storage_query_plan_builder", description="translate"
        ):
            # The QueryTranslator class should be instantiated once for each call to
            # translate_query_and_apply_mappers to avoid cache conflicts.
            clickhouse_query = QueryTranslator(mappers).translate(query)

        with sentry_sdk.start_span(
            op="build_plan.storage_query_plan_builder", description="set_from_clause"
        ):
            print("sourceee", storage.get_schema().get_data_source())
            clickhouse_query.set_from_clause(
                get_query_data_source(
                    storage.get_schema().get_data_source(),
                    allocation_policies=storage.get_allocation_policies(),
                    final=query.get_final(),
                    sampling_rate=query.get_sample(),
                    storage_key=storage.get_storage_key(),
                )
            )

        if settings.get_dry_run():
            explain_meta.add_transform_step(
                "storage_planning", "mappers", str(query), str(clickhouse_query)
            )

        return clickhouse_query

    def execute(self, query: EntityQuery, settings: QuerySettings) -> Query:
        from snuba.pipeline.processors import execute_entity_processors

        execute_entity_processors(query, settings)
        return self.translate_query_and_apply_mappers(query, settings)


def run_entity_processing_executor(
    query: LogicalQuery, query_settings: QuerySettings
) -> Query:
    from snuba.datasets.entities.entity_key import EntityKey
    from snuba.datasets.entities.factory import get_entity
    from snuba.datasets.pluggable_entity import PluggableEntity

    # if we're running this function, this should not be a storage query and if it is, it won't work anyways
    entity_key = query.get_from_clause().key
    assert isinstance(entity_key, EntityKey)
    entity = get_entity(entity_key)
    assert isinstance(entity, PluggableEntity)
    entity_processing_executor = entity.get_processing_executor()
    physical_query = entity_processing_executor.execute(
        cast(EntityQuery, query), query_settings
    )
    return physical_query
