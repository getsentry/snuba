import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterator, List, Mapping, Optional, Tuple

from snuba.datasets.plans.query_plan import ClickhouseQueryPlanBuilder, QueryRunner
from snuba.pipeline.query_pipeline import (
    QueryPipeline,
    QueryPipelineBuilder,
    _execute_query_plan_processors,
)
from snuba.query.logical import Query
from snuba.request import Request
from snuba.web import QueryException, QueryResult

BuilderId = str
QueryPlanBuilders = Mapping[BuilderId, ClickhouseQueryPlanBuilder]
QueryResults = List[Tuple[BuilderId, QueryResult]]
SelectorFunc = Callable[[Query], List[BuilderId]]
CallbackFunc = Callable[[QueryResults], None]

logger = logging.getLogger(__name__)

executor = ThreadPoolExecutor()


class MultipleQueryPlanPipeline(QueryPipeline):
    """
    A query pipeline that executes a request against one or more query plans in parallel.

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

    This component is produced by the QueryPlanDelegator.
    """

    def __init__(
        self,
        request: Request,
        runner: QueryRunner,
        query_plan_builders: QueryPlanBuilders,
        selector_func: SelectorFunc,
        callback_func: Optional[CallbackFunc],
    ):
        self.__request = request
        self.__runner = runner
        self.__query_plan_builders = query_plan_builders
        self.__selector_func = selector_func
        self.__callback_func = callback_func

    def execute(self) -> QueryResult:
        def execute_query(
            query_plan_builder: ClickhouseQueryPlanBuilder,
        ) -> QueryResult:

            query_plan = query_plan_builder.build_plan(self.__request)

            _execute_query_plan_processors(query_plan, self.__request)

            return query_plan.execution_strategy.execute(
                query_plan.query, self.__request.settings, self.__runner
            )

        def execute_queries() -> Iterator[Tuple[BuilderId, QueryResult]]:
            builder_ids = self.__selector_func(self.__request.query)
            primary_builder_id = builder_ids.pop(0)

            futures = [
                (
                    builder_id,
                    executor.submit(
                        execute_query,
                        query_plan_builder=self.__query_plan_builders[builder_id],
                    ),
                )
                for builder_id in builder_ids
            ]

            yield primary_builder_id, execute_query(
                self.__query_plan_builders[primary_builder_id]
            )

            yield from [
                (builder_id, future.result()) for (builder_id, future) in futures
            ]

        generator = execute_queries()

        query_results: QueryResults = []

        try:
            builder_id, query_result = next(generator)
            query_results.append((builder_id, query_result))
            return query_result
        finally:
            try:
                for builder_id, query_result in generator:
                    query_results.append((builder_id, query_result))

                if self.__callback_func is not None:
                    self.__callback_func(query_results)
            except QueryException as error:
                logger.exception(error)


class QueryPlanDelegator(QueryPipelineBuilder):
    """
    Builds a pipeline which is able to run one or more query plans in parallel.
    """

    def __init__(
        self,
        query_plan_builders: QueryPlanBuilders,
        selector_func: SelectorFunc,
        callback_func: Optional[CallbackFunc] = None,
    ) -> None:
        self.__query_plan_builders = query_plan_builders
        self.__selector_func = selector_func
        self.__callback_func = callback_func

    def build_pipeline(self, request: Request, runner: QueryRunner) -> QueryPipeline:
        return MultipleQueryPlanPipeline(
            request=request,
            runner=runner,
            query_plan_builders=self.__query_plan_builders,
            selector_func=self.__selector_func,
            callback_func=self.__callback_func,
        )
