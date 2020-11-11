from abc import ABC, abstractmethod


from snuba.datasets.plans.query_plan import QueryRunner
from snuba.request import Request
from snuba.web import QueryResult


class QueryPipeline(ABC):
    """
    Contains the instructions to build the query plans for the requested query.
    The QueryPipeline runs the query plan processors, executes the query plans and
    returns the relevant result.

    Most of the time, a single pipeline and query plan is built by the SimplePipeline.
    However, we can also use the MultipleConcurrentPipeline in order to build and
    execute more than one other pipeline and compare their results, which provides
    a way to experiment with different pipeline in production without actually using
    their results yet.

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
