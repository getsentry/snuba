import copy
from dataclasses import replace
from functools import partial
from typing import Callable, List, Mapping, Optional, Tuple

from snuba import state
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan, QueryRunner
from snuba.pipeline.query_pipeline import (
    QueryExecutionPipeline,
    QueryPipelineBuilder,
    QueryPlanner,
)
from snuba.pipeline.settings_delegator import RateLimiterDelegate
from snuba.query.logical import Query as LogicalQuery
from snuba.query.query_settings import QuerySettings
from snuba.request import Request
from snuba.state import get_config
from snuba.utils.threaded_function_delegator import Result, ThreadedFunctionDelegator
from snuba.web import QueryResult

BuilderId = str
QueryPipelineBuilders = Mapping[BuilderId, QueryPipelineBuilder[ClickhouseQueryPlan]]
QueryResults = List[Result[QueryResult]]
SelectorFunc = Callable[[LogicalQuery, str], Tuple[BuilderId, List[BuilderId]]]
CallbackFunc = Callable[
    [LogicalQuery, QuerySettings, str, Optional[Result[QueryResult]], QueryResults],
    None,
]


def _is_query_copying_disallowed(current_referrer: str) -> bool:
    """
    Check if the given referrer is disallowed from making a copy of the query.
    """
    pipeline_config = state.get_config("pipeline-delegator-disallow-query-copy", False)
    if isinstance(pipeline_config, str):
        referrers_disallowed = pipeline_config.split(";")
        for referrer in referrers_disallowed:
            if current_referrer == referrer:
                return True

    return False


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
        split_rate_limiter: bool,
        ignore_secondary_exceptions: bool,
        callback_func: Optional[CallbackFunc],
    ):
        self.__request = request
        self.__runner = runner
        self.__split_rate_limtier = split_rate_limiter
        self.__ignore_secondary_exceptions = ignore_secondary_exceptions
        self.__query_pipeline_builders = query_pipeline_builders

        self.__selector_func = lambda query: selector_func(
            query, self.__request.referrer
        )

        self.__callback_func = (
            partial(
                callback_func,
                self.__request.query,
                self.__request.query_settings,
                self.__request.referrer,
            )
            if callback_func
            else None
        )

    def execute(self) -> QueryResult:
        def build_delegate_request(request: Request, builder_id: str) -> Request:
            """
            Build a new request that contains a RateLimiterDelegate in order
            to separate the rate limiter namespace and avoid double counting
            when enforcing the quota.
            """
            query = (
                request.query
                if _is_query_copying_disallowed(request.query_settings.referrer)
                else copy.deepcopy(request.query)
            )

            if not get_config("pipeline_split_rate_limiter", 0):
                return replace(request, query=query)

            return replace(
                request,
                query=query,
                query_settings=RateLimiterDelegate(builder_id, request.query_settings),
            )

        executor = ThreadedFunctionDelegator[LogicalQuery, QueryResult](
            callables={
                builder_id: builder.build_execution_pipeline(
                    build_delegate_request(self.__request, builder_id)
                    if self.__split_rate_limtier
                    else self.__request,
                    self.__runner,
                ).execute
                for builder_id, builder in self.__query_pipeline_builders.items()
            },
            selector_func=self.__selector_func,
            callback_func=self.__callback_func,
            ignore_secondary_exceptions=self.__ignore_secondary_exceptions,
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
        split_rate_limiter: bool,
        ignore_secondary_exceptions: bool,
        callback_func: Optional[CallbackFunc] = None,
    ) -> None:
        self.__query_pipeline_builders = query_pipeline_builders
        self.__selector_func = selector_func
        self.__callback_func = callback_func
        self.__split_rate_limiter = split_rate_limiter
        self.__ignore_secondary_exceptions = ignore_secondary_exceptions

    def build_execution_pipeline(
        self, request: Request, runner: QueryRunner
    ) -> QueryExecutionPipeline:
        return MultipleConcurrentPipeline(
            request=request,
            runner=runner,
            query_pipeline_builders=self.__query_pipeline_builders,
            selector_func=self.__selector_func,
            split_rate_limiter=self.__split_rate_limiter,
            ignore_secondary_exceptions=self.__ignore_secondary_exceptions,
            callback_func=self.__callback_func,
        )

    def build_planner(
        self, query: LogicalQuery, settings: QuerySettings
    ) -> QueryPlanner[ClickhouseQueryPlan]:
        # For composite queries, we just build the primary pipeline / query plan;
        # running multiple concurrent composite queries is not currently supported.
        primary_builder_id, _others = self.__selector_func(query, "")
        query_pipeline_builder = self.__query_pipeline_builders[primary_builder_id]
        return query_pipeline_builder.build_planner(query, settings)
