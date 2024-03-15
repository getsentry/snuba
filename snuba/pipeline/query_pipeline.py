from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Optional, Sequence, TypeVar, Union

from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    CompositeQueryPlan,
    QueryRunner,
)
from snuba.query.logical import Query
from snuba.query.query_settings import QuerySettings
from snuba.request import Request
from snuba.web import QueryResult

TPlan = TypeVar("TPlan", bound=Union[ClickhouseQueryPlan, CompositeQueryPlan])
Tin = TypeVar("Tin")
Tout = TypeVar("Tout")


class QueryPlanner(ABC, Generic[TPlan]):
    """
    A QueryPlanner contains a series of steps that, given a logical
    query and request.query_settings, executes all the logical query processing
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
        self, request: Request, runner: QueryRunner
    ) -> QueryExecutionPipeline:
        """
        Returns a pipeline to execute a query
        """
        raise NotImplementedError

    @abstractmethod
    def build_planner(
        self, query: Query, settings: QuerySettings
    ) -> QueryPlanner[ClickhouseQueryPlan]:
        raise NotImplementedError


class QueryPipelineStage(Generic[Tin, Tout]):
    """
    This class represents a single stage in the snuba query execution pipeline.
    The purpose of this class is to provide an organized and transparent interface to
    execute specific processing steps on the query with clearly defined inputs and outputs.
    These stages are designed to be composed and/or swapped among each other to form a
    a flexible query pipeline.

    Some examples of a query pipeline stage may include:
    * Execute all entity query processors defined on the entity yaml
    * Apply query transformation from logical representation to Clickhouse representation
    * Execute all storage processors defined on the storage yaml
    * Run query execution
    * Query reporting

    To create a Query Pipeline Stage, the main components to specify are:
    an input type, an output type, and a execution function which returns the output wrapped with QueryPipelineResult.
    ==============================================
        >>> class MyQueryPipelineStage(QueryPipelineStage[LogicalQuery, LogicalQuery]):
        >>>    def _execute(self, input: QueryPipelineResult[LogicalQuery]) -> QueryPipelineResult[LogicalQuery]:
        >>>         try:
        >>>             result = my_complex_processing_function(input.data)
        >>>             return QueryPipelineResult(result, None)
        >>>         except Exception as e:
        >>>             return QueryPipelineResult(None, e)
    """

    @abstractmethod
    def _execute(self, input: QueryPipelineResult[Tin]) -> QueryPipelineResult[Tout]:
        raise NotImplementedError

    def execute(self, input: QueryPipelineResult[Tin]) -> QueryPipelineResult[Tout]:
        if input.error:
            # Forward the error to next stage of pipeline
            return QueryPipelineResult(None, input.error)
        return self._execute(input)


@dataclass
class QueryPipelineResult(Generic[Tout]):
    """
    A container to represent the result of a query pipeline stage.
    """

    data: Optional[Tout]
    error: Optional[Exception]
