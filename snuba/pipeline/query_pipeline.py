from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Optional, TypeVar, Union, cast

from snuba.query.query_settings import QuerySettings
from snuba.utils.metrics.timer import Timer

T = TypeVar("T")
Tin = TypeVar("Tin")
Tout = TypeVar("Tout")


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
