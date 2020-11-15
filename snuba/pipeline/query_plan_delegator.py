from typing import Callable, List, Mapping, Tuple

from snuba.clickhouse.query import Query
from snuba.datasets.plans.query_plan import ClickhouseQueryPlanBuilder, QueryRunner
from snuba.pipeline.query_pipeline import (
    QueryExecutionPipeline,
    QueryPipelineBuilder,
    QueryProcessingPipeline,
)
from snuba.request import Request
from snuba.web import QueryResult

BuilderId = str
QueryPlanBuilders = Mapping[BuilderId, ClickhouseQueryPlanBuilder]
QueryResults = List[Tuple[BuilderId, QueryResult]]
SelectorFunc = Callable[[Query], List[BuilderId]]
CallbackFunc = Callable[[QueryResults], None]


class MultipleQueryPlanPipeline(QueryExecutionPipeline):
    """
    A query pipeline that executes a request against one or more query plans in parallel.
    """

    def __init__(
        self,
        request: Request,
        runner: QueryRunner,
        query_plan_builders: QueryPlanBuilders,
        selector_func: SelectorFunc,
        callback_func: CallbackFunc,
    ):
        self.__request = request
        self.__query_plan_builders = query_plan_builders

    def execute(self, input: Request) -> QueryResult:
        raise NotImplementedError


class QueryPlanDelegator(QueryPipelineBuilder):
    """
    Builds a pipeline which is able to run one or more query plans in parallel.
    """

    def __init__(
        self,
        query_plan_builders: QueryPlanBuilders,
        selector_func: SelectorFunc,
        callback_func: CallbackFunc,
    ) -> None:
        self.__query_plan_builders = query_plan_builders
        self.__selector_func = selector_func
        self.__callback_func = callback_func

    def build_execution_pipeline(
        self, request: Request, runner: QueryRunner
    ) -> QueryExecutionPipeline:
        return MultipleQueryPlanPipeline(
            request=request,
            runner=runner,
            query_plan_builders=self.__query_plan_builders,
            selector_func=self.__selector_func,
            callback_func=self.__callback_func,
        )

    def build_processing_pipeline(
        self, request: Request, runner: QueryRunner
    ) -> QueryProcessingPipeline:
        raise NotImplementedError
