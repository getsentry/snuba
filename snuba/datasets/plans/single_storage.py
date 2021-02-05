from typing import Optional, Sequence

import sentry_sdk
from snuba import state
from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.clusters.cluster import ClickhouseCluster
from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    ClickhouseQueryPlanBuilder,
    QueryPlanExecutionStrategy,
    QueryRunner,
)
from snuba.datasets.plans.split_strategy import QuerySplitStrategy
from snuba.datasets.plans.translator.query import QueryTranslator
from snuba.datasets.schemas import RelationalSource
from snuba.datasets.schemas.tables import TableSource
from snuba.datasets.storage import QueryStorageSelector, ReadableStorage
from snuba.query.data_source.simple import Table
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors.mandatory_condition_applier import MandatoryConditionApplier
from snuba.request.request_settings import RequestSettings

# TODO: Importing snuba.web here is just wrong. What's need to be done to avoid this
# dependency is a refactoring of the methods that return RawQueryResult to make them
# depend on Result + some debug data structure instead. Also It requires removing
# extra data from the result of the query.
from snuba.util import with_span
from snuba.web import QueryResult


class SimpleQueryPlanExecutionStrategy(QueryPlanExecutionStrategy[Query]):
    def __init__(
        self,
        cluster: ClickhouseCluster,
        db_query_processors: Sequence[QueryProcessor],
        splitters: Optional[Sequence[QuerySplitStrategy]] = None,
    ) -> None:
        self.__cluster = cluster
        self.__query_processors = db_query_processors
        self.__splitters = splitters or []

    @with_span()
    def execute(
        self, query: Query, request_settings: RequestSettings, runner: QueryRunner
    ) -> QueryResult:
        def process_and_run_query(
            query: Query, request_settings: RequestSettings
        ) -> QueryResult:
            for processor in self.__query_processors:
                with sentry_sdk.start_span(
                    description=type(processor).__name__, op="processor"
                ):
                    processor.process_query(query, request_settings)
            return runner(query, request_settings, self.__cluster.get_reader())

        use_split = state.get_config("use_split", 1)
        if use_split:
            for splitter in self.__splitters:
                with sentry_sdk.start_span(
                    description=type(splitter).__name__, op="splitter"
                ):
                    result = splitter.execute(
                        query, request_settings, process_and_run_query
                    )
                    if result is not None:
                        return result

        return process_and_run_query(query, request_settings)


def get_query_data_source(
    relational_source: RelationalSource, final: bool, sampling_rate: Optional[float]
) -> Table:
    assert isinstance(relational_source, TableSource)
    return Table(
        table_name=relational_source.get_table_name(),
        schema=relational_source.get_columns(),
        final=final,
        sampling_rate=sampling_rate,
        mandatory_conditions=relational_source.get_mandatory_conditions(),
        prewhere_candidates=relational_source.get_prewhere_candidates(),
    )


class SingleStorageQueryPlanBuilder(ClickhouseQueryPlanBuilder):
    """
    Builds the Clickhouse Query Execution Plan for a dataset that is based on
    a single storage.
    """

    def __init__(
        self,
        storage: ReadableStorage,
        mappers: Optional[TranslationMappers] = None,
        post_processors: Optional[Sequence[QueryProcessor]] = None,
    ) -> None:
        # The storage the query is based on
        self.__storage = storage
        # The translation mappers to be used when translating the logical query
        # into the clickhouse query.
        self.__mappers = mappers if mappers is not None else TranslationMappers()
        # This is a set of query processors that have to be executed on the
        # query after the storage selection but that are defined by the dataset.
        # Query processors defined by a Storage must be executable independently
        # from the context the Storage is used (whether the storage is used by
        # itself or whether it is joined with another storage).
        # In a joined query we would have processors defined by multiple storages.
        # that would have to be executed only once (like Prewhere). That is a
        # candidate to be added here as post process.
        self.__post_processors = post_processors or []

    @with_span()
    def build_and_rank_plans(
        self, query: LogicalQuery, settings: RequestSettings
    ) -> Sequence[ClickhouseQueryPlan]:
        with sentry_sdk.start_span(
            op="build_plan.single_storage", description="translate"
        ):
            # The QueryTranslator class should be instantiated once for each call to build_plan,
            # to avoid cache conflicts.
            clickhouse_query = QueryTranslator(self.__mappers).translate(query)

        with sentry_sdk.start_span(
            op="build_plan.single_storage", description="set_from_clause"
        ):
            clickhouse_query.set_from_clause(
                get_query_data_source(
                    self.__storage.get_schema().get_data_source(),
                    final=query.get_final(),
                    sampling_rate=query.get_sample(),
                )
            )

        cluster = self.__storage.get_cluster()

        db_query_processors = [
            *self.__storage.get_query_processors(),
            *self.__post_processors,
            MandatoryConditionApplier(),
        ]

        return [
            ClickhouseQueryPlan(
                query=clickhouse_query,
                plan_query_processors=[],
                db_query_processors=db_query_processors,
                storage_set_key=self.__storage.get_storage_set_key(),
                execution_strategy=SimpleQueryPlanExecutionStrategy(  # type: ignore
                    cluster=cluster,
                    db_query_processors=db_query_processors,
                    splitters=self.__storage.get_query_splitters(),
                ),
            )
        ]


class SelectedStorageQueryPlanBuilder(ClickhouseQueryPlanBuilder):
    """
    A query plan builder that selects one of multiple storages in the dataset.
    """

    def __init__(
        self,
        selector: QueryStorageSelector,
        post_processors: Optional[Sequence[QueryProcessor]] = None,
    ) -> None:
        self.__selector = selector
        self.__post_processors = post_processors or []

    @with_span()
    def build_and_rank_plans(
        self, query: LogicalQuery, settings: RequestSettings
    ) -> Sequence[ClickhouseQueryPlan]:
        with sentry_sdk.start_span(
            op="build_plan.selected_storage", description="select_storage"
        ):
            storage, mappers = self.__selector.select_storage(query, settings)

        with sentry_sdk.start_span(
            op="build_plan.selected_storage", description="translate"
        ):
            # The QueryTranslator class should be instantiated once for each call to build_plan,
            # to avoid cache conflicts.
            clickhouse_query = QueryTranslator(mappers).translate(query)

        with sentry_sdk.start_span(
            op="build_plan.selected_storage", description="set_from_clause"
        ):
            clickhouse_query.set_from_clause(
                get_query_data_source(
                    storage.get_schema().get_data_source(),
                    final=query.get_final(),
                    sampling_rate=query.get_sample(),
                )
            )

        cluster = storage.get_cluster()

        db_query_processors = [
            *storage.get_query_processors(),
            *self.__post_processors,
            MandatoryConditionApplier(),
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
