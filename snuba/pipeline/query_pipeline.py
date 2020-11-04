from abc import ABC, abstractmethod

import sentry_sdk

from snuba.datasets.plans.query_plan import ClickhouseQueryPlan, QueryRunner
from snuba.request import Request
from snuba.web import QueryResult


class QueryPipeline(ABC):
    """
    Contains the instructions to build the query plans for the requested query
    (this can be one or multiple query plans). The QueryPipeline runs the query
    plan processors, executes the query plans and returns the relevant result.

    This component is produced by the QueryPipelineBuilder.

    TODO: In future, the query pipeline wiil be composed of segments, which will
    each perform individual processing steps and can be easily reused.
    Examples of transformations performed by segments are entity processing, query
    plan building and processing, storage processing, as well as splitters
    and collectors to help break apart and reassemble joined queries.
    """

    @abstractmethod
    def execute(self) -> QueryResult:
        raise NotImplementedError


class QueryPipelineBuilder(ABC):
    """
    Builds a query pipeline, which contains the directions for building and running
    query plans.
    """

    @abstractmethod
    def build_pipeline(self, request: Request, runner: QueryRunner) -> QueryPipeline:
        raise NotImplementedError


def _execute_query_plan_processors(
    query_plan: ClickhouseQueryPlan, request: Request
) -> None:
    for clickhouse_processor in query_plan.plan_processors:
        with sentry_sdk.start_span(
            description=type(clickhouse_processor).__name__, op="processor"
        ):
            clickhouse_processor.process_query(query_plan.query, request.settings)
