from __future__ import annotations

from typing import Optional, Sequence

import sentry_sdk

from snuba import settings as snuba_settings
from snuba import state
from snuba.clickhouse.query import Query
from snuba.clusters.cluster import ClickhouseCluster
from snuba.datasets.entities.storage_selectors import QueryStorageSelector
from snuba.datasets.entities.storage_selectors.selector import QueryStorageSelectorError
from snuba.datasets.plans.cluster_selector import ColumnBasedStorageSliceSelector
from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    ClickhouseQueryPlanBuilder,
    QueryPlanExecutionStrategy,
    QueryRunner,
)
from snuba.datasets.plans.splitters import QuerySplitStrategy
from snuba.datasets.plans.translator.query import QueryTranslator
from snuba.datasets.schemas import RelationalSource
from snuba.datasets.schemas.tables import TableSource
from snuba.datasets.slicing import is_storage_set_sliced
from snuba.datasets.storage import (
    EntityStorageConnection,
    ReadableStorage,
    ReadableTableStorage,
    StorageNotAvailable,
)
from snuba.query.allocation_policies import AllocationPolicy
from snuba.query.data_source.simple import Table
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.processors.physical.conditions_enforcer import (
    MandatoryConditionEnforcer,
)
from snuba.query.processors.physical.mandatory_condition_applier import (
    MandatoryConditionApplier,
)
from snuba.query.query_settings import QuerySettings
from snuba.state import explain_meta
from snuba.utils.metrics.util import with_span

# TODO: Importing snuba.web here is just wrong. What's need to be done to avoid this
# dependency is a refactoring of the methods that return RawQueryResult to make them
# depend on Result + some debug data structure instead. Also It requires removing
# extra data from the result of the query.
from snuba.web import QueryResult


class SimpleQueryPlanExecutionStrategy(QueryPlanExecutionStrategy[Query]):
    def __init__(
        self,
        cluster: ClickhouseCluster,
        db_query_processors: Sequence[ClickhouseQueryProcessor],
        splitters: Optional[Sequence[QuerySplitStrategy]] = None,
    ) -> None:
        self.__cluster = cluster
        self.__query_processors = db_query_processors
        self.__splitters = splitters or []

    @with_span()
    def execute(
        self,
        query: Query,
        query_settings: QuerySettings,
        runner: QueryRunner,
    ) -> QueryResult:
        def process_and_run_query(
            query: Query, query_settings: QuerySettings
        ) -> QueryResult:
            for processor in self.__query_processors:
                with sentry_sdk.start_span(
                    description=type(processor).__name__, op="processor"
                ):
                    if query_settings.get_dry_run():
                        with explain_meta.with_query_differ(
                            "storage_processor", type(processor).__name__, query
                        ):
                            processor.process_query(query, query_settings)
                    else:
                        processor.process_query(query, query_settings)

            return runner(
                clickhouse_query=query,
                query_settings=query_settings,
                reader=self.__cluster.get_reader(),
            )

        use_split = state.get_config("use_split", 1)
        if use_split:
            for splitter in self.__splitters:
                with sentry_sdk.start_span(
                    description=type(splitter).__name__, op="splitter"
                ):
                    result = splitter.execute(
                        query, query_settings, process_and_run_query
                    )
                    if result is not None:
                        return result

        return process_and_run_query(query, query_settings)


def get_query_data_source(
    relational_source: RelationalSource,
    allocation_policies: list[AllocationPolicy],
    final: bool,
    sampling_rate: Optional[float],
) -> Table:
    assert isinstance(relational_source, TableSource)
    return Table(
        table_name=relational_source.get_table_name(),
        schema=relational_source.get_columns(),
        allocation_policies=allocation_policies,
        final=final,
        sampling_rate=sampling_rate,
        mandatory_conditions=relational_source.get_mandatory_conditions(),
    )


class StorageQueryPlanBuilder(ClickhouseQueryPlanBuilder):
    """
    Builds the Clickhouse Query Execution Plan for a dataset which supports single storages,
    multiple storages, unsliced storages, and sliced storages.
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
        # Query processors defined by a Storage must be executable independently
        # from the context the Storage is used (whether the storage is used by
        # itself or whether it is joined with another storage).
        # In a joined query we would have processors defined by multiple storages.
        # that would have to be executed only once (like Prewhere). That is a
        # candidate to be added here as post process.
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

    @with_span()
    def build_and_rank_plans(
        self, query: LogicalQuery, settings: QuerySettings
    ) -> Sequence[ClickhouseQueryPlan]:
        if len(self.__storages) < 1:
            raise QueryStorageSelectorError("No storages specified to select from.")

        storage_connection = self.get_storage(query, settings)
        storage = storage_connection.storage
        mappers = storage_connection.translation_mappers

        # Return failure if storage readiness state is not supported in current environment
        if snuba_settings.READINESS_STATE_FAIL_QUERIES:
            assert isinstance(storage, ReadableTableStorage)
            readiness_state = storage.get_readiness_state()
            if readiness_state.value not in snuba_settings.SUPPORTED_STATES:
                raise StorageNotAvailable(
                    StorageNotAvailable.__name__,
                    f"The selected storage={storage.get_storage_key().value} is not available in this environment yet. To enable it, consider bumping the storage's readiness_state.",
                )

        # If storage is supported in this environment, we can safely check get the existing cluster.
        cluster = self.get_cluster(storage, query, settings)

        with sentry_sdk.start_span(
            op="build_plan.storage_query_plan_builder", description="translate"
        ):
            # The QueryTranslator class should be instantiated once for each call to build_plan,
            # to avoid cache conflicts.
            clickhouse_query = QueryTranslator(mappers).translate(query)

        with sentry_sdk.start_span(
            op="build_plan.storage_query_plan_builder", description="set_from_clause"
        ):
            clickhouse_query.set_from_clause(
                get_query_data_source(
                    storage.get_schema().get_data_source(),
                    allocation_policies=storage.get_allocation_policies(),
                    final=query.get_final(),
                    sampling_rate=query.get_sample(),
                )
            )

        if settings.get_dry_run():
            explain_meta.add_transform_step(
                "storage_planning", "mappers", str(query), str(clickhouse_query)
            )

        db_query_processors = [
            *storage.get_query_processors(),
            *self.__post_processors,
            MandatoryConditionApplier(),
            MandatoryConditionEnforcer(storage.get_mandatory_condition_checkers()),
        ]

        return [
            ClickhouseQueryPlan(
                query=clickhouse_query,
                plan_query_processors=[],
                db_query_processors=db_query_processors,
                storage_set_key=storage.get_storage_set_key(),
                execution_strategy=SimpleQueryPlanExecutionStrategy(
                    cluster=cluster,
                    db_query_processors=db_query_processors,
                    splitters=storage.get_query_splitters(),
                ),
            )
        ]
