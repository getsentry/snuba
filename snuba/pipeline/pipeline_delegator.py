from typing import Callable, List, Mapping, Optional, Tuple

from snuba.datasets.plans.query_plan import QueryRunner
from snuba.pipeline.query_pipeline import (
    QueryPipeline,
    QueryPipelineBuilder,
)
from snuba.query.logical import Query
from snuba.request import Request
from snuba.utils.threaded_function_delegator import ThreadedFunctionDelegator
from snuba.web import QueryResult


BuilderId = str
QueryPipelineBuilders = Mapping[BuilderId, QueryPipelineBuilder]
QueryResults = List[Tuple[BuilderId, QueryResult]]
SelectorFunc = Callable[[Query], Tuple[BuilderId, List[BuilderId]]]
CallbackFunc = Callable[[QueryResults], None]


class MultipleConcurrentPipeline(QueryPipeline):
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
        self.__selector_func = selector_func
        self.__callback_func = callback_func

    def execute(self) -> QueryResult:
        executor = ThreadedFunctionDelegator[Query, QueryResult](
            callables={
                builder_id: builder.build_pipeline(
                    self.__request, self.__runner
                ).execute
                for builder_id, builder in self.__query_pipeline_builders.items()
            },
            selector_func=self.__selector_func,
            callback_func=self.__callback_func,
        )
        return executor.execute(self.__request.query)


class PipelineDelegator(QueryPipelineBuilder):
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

    def build_pipeline(self, request: Request, runner: QueryRunner) -> QueryPipeline:
        return MultipleConcurrentPipeline(
            request=request,
            runner=runner,
            query_pipeline_builders=self.__query_pipeline_builders,
            selector_func=self.__selector_func,
            callback_func=self.__callback_func,
        )
