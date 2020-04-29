from typing import Optional, Sequence

from snuba import state
from snuba.clusters.cluster import ClickhouseCluster
from snuba.datasets.plans.query_plan import (
    QueryPlanExecutionStrategy,
    QueryRunner,
    StorageQueryPlan,
    StorageQueryPlanBuilder,
)
from snuba.datasets.plans.split_strategy import StorageQuerySplitStrategy
from snuba.datasets.storage import QueryStorageSelector, ReadableStorage

# TODO: Importing snuba.web here is just wrong. What's need to be done to avoid
# this dependency is a refactoring of the methods that return QueryResult to
# make them depend on Result + some debug data structure instead. Also It
# requires removing extra data from the result of the query.
from snuba.web import QueryResult
from snuba.query.query_processor import QueryProcessor
from snuba.request import Request


class SimpleQueryPlanExecutionStrategy(QueryPlanExecutionStrategy):
    def __init__(
        self,
        cluster: ClickhouseCluster,
        db_query_processors: Sequence[QueryProcessor],
        splitters: Optional[Sequence[StorageQuerySplitStrategy]] = None,
    ) -> None:
        self.__cluster = cluster
        self.__query_processors = db_query_processors
        self.__splitters = splitters or []

    def execute(self, request: Request, runner: QueryRunner) -> QueryResult:
        (use_split,) = state.get_configs([("use_split", 0)])
        if use_split:
            for splitter in self.__splitters:
                result = splitter.execute(request, runner, self.__query_processors)
                if result is not None:
                    return result

        for processor in self.__query_processors:
            processor.process_query(request.query, request.settings)
        return runner(request, self.__cluster.get_reader())


class SingleStorageQueryPlanBuilder(StorageQueryPlanBuilder):
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

    def build_plan(self, request: Request) -> StorageQueryPlan:
        # the Request object is immutable, the query processing still depends on the same
        # request object to be moved around, thus the Query has to be mutable and cannot
        # be simply rebuilt and replaced.
        request.query.set_data_source(
            self.__storage.get_schemas().get_read_schema().get_data_source()
        )

        cluster = self.__storage.get_cluster()

        return StorageQueryPlan(
            plan_processors=[],
            execution_strategy=SimpleQueryPlanExecutionStrategy(
                cluster,
                [*self.__storage.get_query_processors(), *self.__post_processors],
                self.__storage.get_query_splitters(),
            ),
        )


class SelectedStorageQueryPlanBuilder(StorageQueryPlanBuilder):
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

    def build_plan(self, request: Request) -> StorageQueryPlan:
        storage = self.__selector.select_storage(request.query, request.settings)
        request.query.set_data_source(
            storage.get_schemas().get_read_schema().get_data_source()
        )

        cluster = storage.get_cluster()

        return StorageQueryPlan(
            plan_processors=[],
            execution_strategy=SimpleQueryPlanExecutionStrategy(
                cluster,
                [*storage.get_query_processors(), *self.__post_processors],
                storage.get_query_splitters(),
            ),
        )
