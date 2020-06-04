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
from snuba.datasets.storage import QueryStorageSelector, ReadableStorage
from snuba.request import Request
from snuba.request.request_settings import RequestSettings

# TODO: Importing snuba.web here is just wrong. What's need to be done to avoid this
# dependency is a refactoring of the methods that return RawQueryResult to make them
# depend on Result + some debug data structure instead. Also It requires removing
# extra data from the result of the query.
from snuba.util import with_span
from snuba.web import QueryResult


class SimpleQueryPlanExecutionStrategy(QueryPlanExecutionStrategy):
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
    def build_plan(self, request: Request) -> ClickhouseQueryPlan:
        clickhouse_query = QueryTranslator(self.__mappers).translate(request.query)
        clickhouse_query.set_data_source(
            self.__storage.get_schemas().get_read_schema().get_data_source()
        )

        cluster = self.__storage.get_cluster()

        return ClickhouseQueryPlan(
            query=clickhouse_query,
            plan_processors=[],
            execution_strategy=SimpleQueryPlanExecutionStrategy(
                cluster=cluster,
                db_query_processors=[
                    *self.__storage.get_query_processors(),
                    *self.__post_processors,
                ],
                splitters=self.__storage.get_query_splitters(),
            ),
        )


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
    def build_plan(self, request: Request) -> ClickhouseQueryPlan:
        storage, mappers = self.__selector.select_storage(
            request.query, request.settings
        )
        clickhouse_query = QueryTranslator(mappers).translate(request.query)
        clickhouse_query.set_data_source(
            storage.get_schemas().get_read_schema().get_data_source()
        )

        cluster = storage.get_cluster()

        return ClickhouseQueryPlan(
            query=clickhouse_query,
            plan_processors=[],
            execution_strategy=SimpleQueryPlanExecutionStrategy(
                cluster=cluster,
                db_query_processors=[
                    *storage.get_query_processors(),
                    *self.__post_processors,
                ],
                splitters=storage.get_query_splitters(),
            ),
        )
