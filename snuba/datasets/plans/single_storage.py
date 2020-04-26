from typing import Optional, Sequence

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.datasets.plans.query_plan import (
    QueryPlanExecutionStrategy,
    QueryRunner,
    ClickhouseQueryPlan,
    ClickhouseQueryPlanBuilder,
)
from snuba.datasets.plans.translators import CopyTranslator
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


class SingleStorageQueryPlanBuilder(ClickhouseQueryPlanBuilder):
    """
    Builds the Storage Query Execution Plan for a dataset that is based on
    a single storage.
    """

    def __init__(
        self,
        storage: ReadableStorage,
        post_processors: Optional[Sequence[QueryProcessor]] = None,
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
        # TODO: Clearly the QueryTranslator instance  will be dependent on the storage.
        # Setting the data_source on the query should become part of the translation
        # as well.
        clickhouse_query = CopyTranslator().translate(request.query)
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


class SelectedStorageQueryPlanBuilder(ClickhouseQueryPlanBuilder):
    """
    A query plan builder that selects one of multiple storages in the
    dataset.
    """

    def __init__(
        self,
        selector: QueryStorageSelector,
        post_processors: Optional[Sequence[QueryProcessor]] = None,
    ) -> None:
        self.__selector = selector
        self.__post_processors = post_processors or []

    def build_plan(self, request: Request) -> ClickhouseQueryPlan:
        storage, translator = self.__selector.select_storage(
            request.query, request.settings
        )
        # TODO: This code is likely to change with multi-table storages, since the
        # storage will be hiding the translation process. But it will take a while
        # to get there.
        clickhouse_query = translator.translate(request.query)
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
