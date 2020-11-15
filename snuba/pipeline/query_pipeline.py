from abc import ABC, abstractmethod


from snuba.datasets.plans.query_plan import QueryRunner
from snuba.request import Request
from snuba.web import QueryResult
from snuba.pipeline import Segment


class QueryExecutionPipeline(Segment[Request, QueryResult], ABC):
    """
    Contains the instructions to build the query plans for the requested query.
    The QueryExecutionPipeline runs the query plan processors, executes the query plans and
    returns the relevant result.

    Most of the time, a single query plan is built by the SingleQueryPlanPipeline,
    however under certain circumstances we may have a pipeline with multiple query
    plans. For example, the MultipleQueryPlanPipeline provides a way to build and
    execute more than one query plan and compare their results, which provides a
    way to experiment with different query plans in production without actually
    using their results yet.

    This component is produced by the QueryExecutionPipelineBuilder.
    """

    @abstractmethod
    def execute(self, input: Request) -> QueryResult:
        raise NotImplementedError


class QueryExecutionPipelineBuilder(ABC):
    """
    Builds a query pipeline, which contains the directions for building and running
    query plans.
    """

    @abstractmethod
    def build_pipeline(
        self, request: Request, runner: QueryRunner
    ) -> QueryExecutionPipeline:
        raise NotImplementedError
