from typing import Optional, Sequence

from snuba.datasets.storages.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    QueryPlanBuilder,
    QueryPlanExecutionStrategy,
    QueryRunner,
)
from snuba.datasets.plans.translators import QueryTranslator
from snuba.datasets.storage import QueryStorageSelector, ReadableStorage
from snuba.request import Request
from snuba.request.request_settings import RequestSettings

# TODO: Importing snuba.web here is just wrong. What's need to be done to avoid this
# dependency is a refactoring of the methods that return RawQueryResult to make them
# depend on Result + some debug data structure instead. Also It requires removing
# extra data from the result of the query.
from snuba.web import QueryResult


class SimpleQueryPlanExecutionStrategy(QueryPlanExecutionStrategy):
    def execute(
        self, query: Query, request_settings: RequestSettings, runner: QueryRunner,
    ) -> QueryResult:
        return runner(query, request_settings)


class SingleStorageQueryPlanBuilder(QueryPlanBuilder[ClickhouseQueryPlan]):
    """
    Builds the Clickhouse Query Execution Plan for a dataset that is based on
    a single storage.
    """

    def __init__(
        self,
        storage: ReadableStorage[Query],
        post_processors: Optional[Sequence[QueryProcessor[Query]]] = None,
    ) -> None:
        # The storage the query is based on
        self.__storage = storage
        # This is a set of query processors that have to be executed on the
        # query after the storage selection but that are defined by the dataset.
        # Query processors defined by a Storage must be executable independently
        # from the context the Storage is used (whether the storage is used by
        # itself or whether it is joined with another storage).
        # In a joined query we would have processors defined by multiple storages.
        # that would have to be executed only once (like Prewhere). That is a
        # candidate to be added here as post process.
        self.__post_processors = post_processors or []

    def build_plan(self, request: Request) -> ClickhouseQueryPlan:
        # TODO: The translator is going to be configured with a mapping between logical
        # and physical schema that is a property of the relation between dataset (later
        # entity) and storage.
        clickhouse_query = QueryTranslator().translate(request.query)
        clickhouse_query.set_data_source(
            self.__storage.get_schemas().get_read_schema().get_data_source()
        )

        return ClickhouseQueryPlan(
            query=clickhouse_query,
            query_processors=[
                *self.__storage.get_query_processors(),
                *self.__post_processors,
            ],
            execution_strategy=SimpleQueryPlanExecutionStrategy(),
        )


class SelectedStorageQueryPlanBuilder(QueryPlanBuilder[ClickhouseQueryPlan]):
    """
    A query plan builder that selects one of multiple storages in the dataset.
    """

    def __init__(
        self,
        selector: QueryStorageSelector[Query],
        post_processors: Optional[Sequence[QueryProcessor[Query]]] = None,
    ) -> None:
        self.__selector = selector
        self.__post_processors = post_processors or []

    def build_plan(self, request: Request) -> ClickhouseQueryPlan:
        storage = self.__selector.select_storage(request.query, request.settings)
        clickhouse_query = QueryTranslator().translate(request.query)
        clickhouse_query.set_data_source(
            storage.get_schemas().get_read_schema().get_data_source()
        )

        return ClickhouseQueryPlan(
            query=clickhouse_query,
            query_processors=[
                *storage.get_query_processors(),
                *self.__post_processors,
            ],
            execution_strategy=SimpleQueryPlanExecutionStrategy(),
        )
