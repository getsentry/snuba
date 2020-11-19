from abc import ABC, abstractmethod

from snuba.datasets.plans.query_plan import ClickhouseQueryPlan, QueryRunner
from snuba.query.logical import Query as LogicalQuery
from snuba.request import Request
from snuba.request.request_settings import RequestSettings
from snuba.web import QueryResult


class EntityQueryProcessingPipeline(ABC):
    """
    A EntityQueryProcessingPipeline contains a series of steps that, given
    a logical single entity query and request settings, performs the
    query processing steps to the point where the query is ready to
    be executed.

    the query is returned as a ClickhouseQueryPlan.
    """

    @abstractmethod
    def execute(self) -> ClickhouseQueryPlan:
        raise NotImplementedError


class QueryExecutionPipeline(ABC):
    """
    Contains the instructions to execute a query.
    The QueryExecutionPipeline performs the all query processing steps and,
    executes the query plan and returns the result.

    Most of the time, a single query plan is built by the SimplePipeline.
    However, we can also use the MultipleConcurrentPipeline in order to build and
    execute more than one other pipeline and compare their results, which provides
    a way to experiment with different pipeline in production without actually using
    their results yet.

    This component is produced by the QueryPipelineBuilder.
    """

    @abstractmethod
    def execute(self) -> QueryResult:
        raise NotImplementedError


class QueryPipelineBuilder(ABC):
    """
    Builds a query pipeline, which contains the directions for building
    processing and running a single entity query or a subquery of a
    composite query.
    """

    @abstractmethod
    def build_execution_pipeline(
        self, request: Request, runner: QueryRunner
    ) -> QueryExecutionPipeline:
        """
        Returns a pipeline to execute a query
        """
        raise NotImplementedError

    @abstractmethod
    def build_processing_pipeline(
        self, query: LogicalQuery, settings: RequestSettings
    ) -> EntityQueryProcessingPipeline:
        """
        Returns a pipeline that executes the processing phase of a single
        entity query.
        """
        raise NotImplementedError
