from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Optional, Sequence, TypeVar, Union, cast

from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    CompositeQueryPlan,
    QueryRunner,
)
from snuba.query.logical import Query
from snuba.query.query_settings import QuerySettings
from snuba.request import Request
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryResult

TPlan = TypeVar("TPlan", bound=Union[ClickhouseQueryPlan, CompositeQueryPlan])
T = TypeVar("T")
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
    an input type, an output type, and a execution function which returns the output. The PipelineStage
    will handle wrapping results/errors in the QueryPipelineResult type
    ==============================================
        >>> class MyQueryPipelineStage(QueryPipelineStage[LogicalQuery, LogicalQuery]):
        >>>    def _process_data(self, input: QueryPipelineData[LogicalQuery]) -> QueryPipelineResult[LogicalQuery]:
        >>>         result = my_complex_processing_function(input.data)

        >>> class MyQueryPipelineStage2(QueryPipelineStage[LogicalQuery, PhysicalQuery]):
        >>>     def _process_data(self, input: QueryPipelineResult[LogicalQuery]) -> PhysicalQuery:
        >>>         translate_query(input)

        >>> input_query = QueryPipelineResult(data=query, error=None, ...)
        >>> transformed_query = MyQueryPipelineStage().execute(query)
        >>> stage_2 = MyQueryPipelineStage2().execute(transformed_query)
        >>> print("PhysicalQuery: ", stage_2.data)
    """

    def _process_error(
        self, pipe_input: QueryPipelineError[Tin]
    ) -> Union[Tout, Exception]:
        """default behaviour is to just pass through to the next stage of the pipeline
        Can be overridden to do something else"""
        logging.exception(pipe_input.error)
        return pipe_input.error

    @abstractmethod
    def _process_data(self, pipe_input: QueryPipelineData[Tin]) -> Tout:
        raise NotImplementedError

    def execute(
        self, pipe_input: QueryPipelineResult[Tin]
    ) -> QueryPipelineResult[Tout]:
        if pipe_input.error:
            res = self._process_error(pipe_input.as_error())
            if isinstance(res, Exception):
                return QueryPipelineResult(
                    data=None,
                    query_settings=pipe_input.query_settings,
                    error=res,
                    timer=pipe_input.timer,
                )
            else:
                return QueryPipelineResult(
                    data=res,
                    query_settings=pipe_input.query_settings,
                    error=None,
                    timer=pipe_input.timer,
                )
        try:
            return QueryPipelineResult(
                data=self._process_data(pipe_input.as_data()),
                query_settings=pipe_input.query_settings,
                timer=pipe_input.timer,
                error=None,
            )
        except Exception as e:
            return QueryPipelineResult(
                data=None,
                query_settings=pipe_input.query_settings,
                timer=pipe_input.timer,
                error=e,
            )


class InvalidQueryPipelineResult(Exception):
    pass


@dataclass
class QueryPipelineResult(ABC, Generic[T]):
    """
    A container to represent the result of a query pipeline stage.
    """

    data: Optional[T]
    error: Optional[Exception]
    query_settings: QuerySettings
    timer: Timer

    def __post_init__(self) -> None:
        if not (self.data is None) ^ (self.error is None):
            raise InvalidQueryPipelineResult(
                f"QueryPipelineResult must have exclusively data or error set, data: {self.data}, error: {self.error}"
            )

    def as_data(self) -> QueryPipelineData[T]:
        return cast(QueryPipelineData[T], self)

    def as_error(self) -> QueryPipelineError[T]:
        return cast(QueryPipelineError[T], self)


# these classes are just for typing purposes. It avoids the user of a QueryPipelineStage
# having to `assert pipe_input.data is not None` at every call, it sucks because we have to repeat
# the class strucure a few times but I think it's a little more user friendly (Volo)
@dataclass
class QueryPipelineData(QueryPipelineResult[T]):
    data: T
    error: None


@dataclass
class QueryPipelineError(QueryPipelineResult[T]):
    data: None
    error: Exception
