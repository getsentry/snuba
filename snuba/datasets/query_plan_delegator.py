from concurrent.futures import ThreadPoolExecutor
import sentry_sdk
from typing import (
    Callable,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
)

from snuba.clickhouse.query import Query
from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    ClickhouseQueryPlanBuilder,
    QueryPlanExecutionStrategy,
    QueryRunner,
)
from snuba.datasets.plans.translator.query import identity_translate
from snuba.request import Request
from snuba.request.request_settings import RequestSettings
from snuba.util import with_span
from snuba.web import QueryResult


BuilderId = str
QueryResults = List[Tuple[BuilderId, QueryResult]]
CallbackFunc = Callable[[QueryResults], None]


class QueryPlanDelegator(ClickhouseQueryPlanBuilder):
    """
    QueryPlanDelegator is a query plan builder that delegates a request to one or
    more other query plan builders.

    It can be used as a way to switch between different query plan builders depending
    on the query received and the selector function provided. It can also be used to
    run a single query against multiple query plans annd their associated storages
    in order to test and compare the results and performance of various query
    plan builders before rolling them out to users.

    The query plans that are built and executed on a particular query are determined
    by the selector function which returns a sequence of query builder IDs. The
    first item in the list corresponds to the query builder that will be used to
    ultimately return a result. Hence the selector function must return
    a list with at least one value in it.

    Subsequent (if any) query plan builders that are also returned by the selector
    function will be executed in separate threads. The results of these other queries
    are provided to the callback function if one is provided.

    TODO: Add metrics
    """

    def __init__(
        self,
        query_plan_builders: Mapping[BuilderId, ClickhouseQueryPlanBuilder],
        selector_func: Callable[[Query], List[BuilderId]],
        callback_func: Optional[CallbackFunc] = None,
    ) -> None:
        self.__query_plan_builders = query_plan_builders
        self.__selector_func = selector_func
        self.__callback_func = callback_func

    def build_plan(self, request: Request) -> ClickhouseQueryPlan:
        return ClickhouseQueryPlan(
            query=identity_translate(request.query),
            plan_processors=[],
            execution_strategy=ThreadedQueryPlanExecutionStrategy(
                request=request,
                query_plan_builders=self.__query_plan_builders,
                selector_func=self.__selector_func,
                callback_func=self.__callback_func,
            ),
        )


class ThreadedQueryPlanExecutionStrategy(QueryPlanExecutionStrategy):
    def __init__(
        self,
        request: Request,
        query_plan_builders: Mapping[BuilderId, ClickhouseQueryPlanBuilder],
        selector_func: Callable[[Query], List[BuilderId]],
        callback_func: Optional[CallbackFunc] = None,
    ) -> None:
        self.__request = request
        self.__query_plan_builders = query_plan_builders
        self.__selector_func = selector_func
        self.__callback_func = callback_func

    @with_span()
    def execute(
        self, query: Query, request_settings: RequestSettings, runner: QueryRunner
    ) -> QueryResult:
        def execute_query(
            query_plan_builder: ClickhouseQueryPlanBuilder,
        ) -> QueryResult:
            query_plan = query_plan_builder.build_plan(self.__request)

            for clickhouse_processor in query_plan.plan_processors:
                with sentry_sdk.start_span(
                    description=type(clickhouse_processor).__name__, op="processor"
                ):
                    clickhouse_processor.process_query(
                        query_plan.query, request_settings
                    )

            return query_plan.execution_strategy.execute(
                query_plan.query, request_settings, runner
            )

        def execute_queries() -> Iterator[Tuple[BuilderId, QueryResult]]:
            builder_ids = self.__selector_func(query)
            primary_builder_id = builder_ids.pop(0)

            if len(builder_ids) == 0:
                yield primary_builder_id, execute_query(
                    self.__query_plan_builders[primary_builder_id]
                )
            else:
                executor = ThreadPoolExecutor()

                with executor:
                    futures = [
                        (
                            builder_id,
                            executor.submit(
                                execute_query,
                                query_plan_builder=self.__query_plan_builders[
                                    builder_id
                                ],
                            ),
                        )
                        for builder_id in builder_ids
                    ]

                    yield primary_builder_id, execute_query(
                        self.__query_plan_builders[primary_builder_id]
                    )

                    yield from [
                        (builder_id, future.result())
                        for (builder_id, future) in futures
                    ]

        generator = execute_queries()

        query_results: QueryResults = []

        try:
            builder_id, query_result = next(generator)
            query_results.append((builder_id, query_result))
            return query_result
        finally:
            for builder_id, query_result in generator:
                query_results.append((builder_id, query_result))

            if self.__callback_func is not None:
                self.__callback_func(query_results)
