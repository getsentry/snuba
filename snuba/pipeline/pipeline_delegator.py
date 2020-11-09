import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterator, List, Mapping, Optional, Tuple

from snuba.datasets.plans.query_plan import QueryRunner
from snuba.pipeline.query_pipeline import (
    QueryPipeline,
    QueryPipelineBuilder,
)
from snuba.query.logical import Query
from snuba.request import Request
from snuba.web import QueryException, QueryResult

BuilderId = str
QueryPipelineBuilders = Mapping[BuilderId, QueryPipelineBuilder]
QueryResults = List[Tuple[BuilderId, QueryResult]]
SelectorFunc = Callable[[Query], List[BuilderId]]
CallbackFunc = Callable[[QueryResults], None]

logger = logging.getLogger(__name__)

executor = ThreadPoolExecutor()


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
        def build_and_execute_pipeline(
            query_pipeline_builder: QueryPipelineBuilder,
        ) -> QueryResult:
            return query_pipeline_builder.build_pipeline(
                self.__request, self.__runner
            ).execute()

        def execute_pipelines() -> Iterator[Tuple[BuilderId, QueryResult]]:
            builder_ids = self.__selector_func(self.__request.query)
            primary_builder_id = builder_ids.pop(0)

            futures = [
                (
                    builder_id,
                    executor.submit(
                        build_and_execute_pipeline,
                        query_pipeline_builder=self.__query_pipeline_builders[
                            builder_id
                        ],
                    ),
                )
                for builder_id in builder_ids
            ]

            yield primary_builder_id, build_and_execute_pipeline(
                self.__query_pipeline_builders[primary_builder_id]
            )

            yield from [
                (builder_id, future.result()) for (builder_id, future) in futures
            ]

        generator = execute_pipelines()

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
