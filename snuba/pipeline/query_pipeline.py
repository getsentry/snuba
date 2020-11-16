from abc import ABC, abstractmethod
from typing import Tuple

from snuba.datasets.plans.query_plan import ClickhouseQueryPlan, QueryRunner
from snuba.pipeline import Segment
from snuba.query.logical import Query as LogicalQuery
from snuba.request import Request
from snuba.request.request_settings import RequestSettings
from snuba.web import QueryResult

EntityProcessingPayload = Tuple[LogicalQuery, RequestSettings]


class QueryProcessingPipeline(
    Segment[EntityProcessingPayload, ClickhouseQueryPlan], ABC
):
    """
    A QueryProcessingPipeline contains a series of steps that, given
    a logical single entity query and request settings, Performs the
    query processing steps to the point where the query is ready to
    be executed.

    the query is returned as a ClickhouseQueryPlan.

    TODO: If we will build a processing pipeline for composite query
    the return type of this pipeline will have to change.
    """

    @abstractmethod
    def execute(self, input: EntityProcessingPayload) -> ClickhouseQueryPlan:
        raise NotImplementedError


class QueryExecutionPipeline(Segment[Request, QueryResult], ABC):
    """
    Contains the instructions to execute a query.
    The QueryExecutionPipeline performs the all query processing steps and,
    executes the query plan and returns the result.

    Most of the time, a single query plan is built by the SinglePlanExecutionPipeline,
    however under certain circumstances we may have a pipeline with multiple query
    plans. For example, the MultipleQueryPlanPipeline provides a way to build and
    execute more than one query plan and compare their results, which provides a
    way to experiment with different query plans in production without actually
    using their results yet.

    This component is produced by the QueryPipelineBuilder.
    """

    @abstractmethod
    def execute(self, input: Request) -> QueryResult:
        raise NotImplementedError


class QueryPipelineBuilder(ABC):
    """
    Builds a query pipeline, which contains the directions for building
    and running the query.
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
    def build_processing_pipeline(self, request: Request) -> QueryProcessingPipeline:
        """
        Returns a pipeline that executes the processing phase of a query.

        TODO: Split this method out of this class so that we can reuse
        the execution builder for the dataset for composite queries.
        In those cases the processing does not need to be split from
        execution.
        """
        raise NotImplementedError
