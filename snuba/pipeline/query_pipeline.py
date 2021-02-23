from abc import ABC, abstractmethod
from typing import Generic, Sequence, TypeVar, Union

from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    CompositeQueryPlan,
    QueryRunner,
)
from snuba.query.logical import Query
from snuba.querylog.query_metadata import SnubaQueryMetadata
from snuba.request import Request
from snuba.request.request_settings import RequestSettings
from snuba.web import QueryResult

TPlan = TypeVar("TPlan", bound=Union[ClickhouseQueryPlan, CompositeQueryPlan])


class QueryPlanner(ABC, Generic[TPlan]):
    """
    A QueryPlanner contains a series of steps that, given a logical
    query and request settings, executes all the logical query processing
    translates the query and compiles a query plan that can be used
    to execute the query.

    The returned query plan structure may be different between different
    query types but it must provide the query, all clickhouse query
    processors, and a strategy to execute the query.
    """

    @abstractmethod
    def build_best_plan(self) -> TPlan:
        raise NotImplementedError

    @abstractmethod
    def build_and_rank_plans(self) -> Sequence[TPlan]:
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


class QueryPipelineBuilder(ABC, Generic[TPlan]):
    """
    Builds a query pipeline, which contains the directions for building
    processing and running a single entity query or a subquery of a
    composite query.
    """

    @abstractmethod
    def build_execution_pipeline(
        self, request: Request, runner: QueryRunner, query_metadata: SnubaQueryMetadata
    ) -> QueryExecutionPipeline:
        """
        Returns a pipeline to execute a query
        """
        raise NotImplementedError

    @abstractmethod
    def build_planner(
        self, query: Query, settings: RequestSettings
    ) -> QueryPlanner[ClickhouseQueryPlan]:
        raise NotImplementedError
