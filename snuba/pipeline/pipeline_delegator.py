from functools import partial
from typing import Callable, List, Mapping, Optional, Tuple

from snuba.datasets.plans.query_plan import ClickhouseQueryPlan, QueryRunner
from snuba.pipeline.query_pipeline import (
    QueryExecutionPipeline,
    QueryPipelineBuilder,
    QueryPlanner,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.request import Request
from snuba.request.request_settings import RequestSettings
from snuba.utils.threaded_function_delegator import Result, ThreadedFunctionDelegator
from snuba.web import QueryResult

BuilderId = str
Timing = float
QueryPipelineBuilders = Mapping[BuilderId, QueryPipelineBuilder[ClickhouseQueryPlan]]
QueryResults = List[Result[QueryResult]]
SelectorFunc = Callable[[LogicalQuery, str], Tuple[BuilderId, List[BuilderId]]]
CallbackFunc = Callable[[LogicalQuery, str, QueryResults], None]


class MultipleConcurrentPipeline(QueryExecutionPipeline):
    """
    A query pipeline that executes a request against one or more other pipelines
    in parallel.

    It can be used as a way to switch between different pipelines depending on the
    query received and the selector function provided. It can also be used to run
    a single query against multiple pipelines, and their associated query plans
    and storages in order to test and compare the results and performance of
    various pipelines before rolling them out to users.

    The pipelines that are built and executed on a particular query are determined
    by the selector function which returns a sequence of query builder IDs. The
    first item in the list corresponds to the pipeline that will be used to ultimately
    return a result. Hence the selector function must return a list with at least
    one value in it.

    Subsequent (if any) pipelines that are also returned by the selector function
    will be executed in separate threads. The results of these other queries are
    provided to the callback function if one is provided.

    This component is produced by the PipelineDelegator.
    """

    def __init__(
        self,
        request: Request,
        runner: QueryRunner,
        query_pipeline_builders: QueryPipelineBuilders,
        selector_func: SelectorFunc,
        callback_func: Optional[CallbackFunc],
    ):
        self.__request = request
        self.__runner = runner
        self.__query_pipeline_builders = query_pipeline_builders

        self.__selector_func = lambda query: selector_func(
            query, self.__request.referrer
        )

        self.__callback_func = (
            partial(
                callback_func,
                self.__request.query,
                self.__request.settings,
                self.__request.referrer,
            )
            if callback_func
            else None
        )

    def execute(self) -> QueryResult:
        executor = ThreadedFunctionDelegator[LogicalQuery, QueryResult](
            callables={
                builder_id: builder.build_execution_pipeline(
                    self.__request, self.__runner
                ).execute
                for builder_id, builder in self.__query_pipeline_builders.items()
            },
            selector_func=self.__selector_func,
            callback_func=self.__callback_func,
        )
        assert isinstance(self.__request.query, LogicalQuery)
        return executor.execute(self.__request.query)


class PipelineDelegator(QueryPipelineBuilder[ClickhouseQueryPlan]):
    """
    Builds a pipeline which is able to run one or more other pipelines in parallel.
    """

    def __init__(
        self,
        query_pipeline_builders: QueryPipelineBuilders,
        selector_func: SelectorFunc,
        callback_func: Optional[CallbackFunc] = None,
    ) -> None:
        self.__query_pipeline_builders = query_pipeline_builders
        self.__selector_func = selector_func
        self.__callback_func = callback_func

    def build_execution_pipeline(
        self, request: Request, runner: QueryRunner
    ) -> QueryExecutionPipeline:
        return MultipleConcurrentPipeline(
            request=request,
            runner=runner,
            query_pipeline_builders=self.__query_pipeline_builders,
            selector_func=self.__selector_func,
            callback_func=self.__callback_func,
        )

    def build_planner(
        self, query: LogicalQuery, settings: RequestSettings
    ) -> QueryPlanner[ClickhouseQueryPlan]:
        # For composite queries, we just build the primary pipeline / query plan;
        # running multiple concurrent composite queries is not currently supported.
        primary_builder_id, _others = self.__selector_func(query, "")
        query_pipeline_builder = self.__query_pipeline_builders[primary_builder_id]
        return query_pipeline_builder.build_planner(query, settings)
