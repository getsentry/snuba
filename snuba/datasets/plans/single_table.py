from typing import Sequence

from snuba.datasets.plans.query_plan import (
    QueryPlanExecutionStrategy,
    RawQueryResult,
    SingleQueryRunner,
    StorageQueryPlan,
    StorageQueryPlanBuilder,
)
from snuba.datasets.storage import QueryStorageSelector, Storage
from snuba.query.query_processor import QueryProcessor
from snuba.request import Request


class SimpleQueryPlanExecutionStrategy(QueryPlanExecutionStrategy):
    def execute(self, request: Request, runner: SingleQueryRunner) -> RawQueryResult:
        return runner(request)


class SingleTableQueryPlanBuilder(StorageQueryPlanBuilder):
    def __init__(
        self, storage: Storage, post_processors: Sequence[QueryProcessor]
    ) -> None:
        self.__storage = storage
        self.__post_processors = post_processors

    def build_plan(self, request: Request) -> StorageQueryPlan:
        request.query.set_data_source(
            self.__storage.get_schemas().get_read_schema().get_data_source()
        )
        return StorageQueryPlan(
            query_processors=self.__storage.get_query_processors()
            + self.__post_processors,
            plan_executor=SimpleQueryPlanExecutionStrategy(),
        )


class SelectedTableQueryPlanBuilder(StorageQueryPlanBuilder):
    def __init__(
        self, selector: QueryStorageSelector, post_processors: Sequence[QueryProcessor]
    ) -> None:
        self.__selector = selector
        self.__post_processor = post_processors

    def build_plan(self, request: Request) -> StorageQueryPlan:
        storage = self.__selector.select_storage(request.query, request.settings)
        return SingleTableQueryPlanBuilder(storage, self.__post_processor,).build_plan(
            request,
        )
