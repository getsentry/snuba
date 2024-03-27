from abc import ABC, abstractmethod
from typing import Sequence

from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    ClickhouseQueryPlanBuilder,
    QueryPlan,
    QueryRunner,
)
from snuba.pipeline.processors import execute_plan_processors
from snuba.pipeline.query_pipeline import QueryPipelineBuilder, QueryPlanner
from snuba.query.logical import Query as LogicalQuery
from snuba.query.query_settings import QuerySettings
from snuba.request import Request
from snuba.web import QueryResult


class QueryExecutionPipelineNew(ABC):
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
    def create_plan(self) -> QueryPlan:
        raise NotImplementedError

    @abstractmethod
    def execute(self) -> QueryResult:
        raise NotImplementedError


class EntityQueryPlannerNew(QueryPlanner[ClickhouseQueryPlan]):
    """
    Executes the processing phase of a single plan query. Which means a
    query based on a single entity, that would produce a query plan based
    on a single storage.

    This should not be used if the plan execution strategy is then used
    to execute the query as it executes all query processors.
    The main use case is for subqueries.
    """

    def __init__(
        self,
        query: LogicalQuery,
        settings: QuerySettings,
        query_plan_builder: ClickhouseQueryPlanBuilder,
    ) -> None:
        self.__query = query
        self.__settings = settings
        self.__query_plan_builder = query_plan_builder

    def build_best_plan(self) -> ClickhouseQueryPlan:
        return self.__query_plan_builder.build_best_plan(self.__query, self.__settings)

    def build_and_rank_plans(self) -> Sequence[ClickhouseQueryPlan]:
        return self.__query_plan_builder.build_and_rank_plans(
            self.__query, self.__settings
        )


class SimpleExecutionPipelineNew(QueryExecutionPipelineNew):
    """
    Executes a simple (single entity) query.
    """

    def __init__(
        self,
        query: LogicalQuery,
        query_settings: QuerySettings,
    ) -> None:
        self.__query = query
        self.__query_settings = query_settings

    def create_plan(
        self, query_plan_builder: ClickhouseQueryPlanBuilder
    ) -> ClickhouseQueryPlan:
        plan = EntityQueryPlannerNew(
            self.__query,
            self.__query_settings,
            query_plan_builder,
        )
        query_plan = plan.build_best_plan()
        execute_plan_processors(query_plan, self.__query_settings)
        return query_plan

    def execute(
        self, query_plan: ClickhouseQueryPlan, runner: QueryRunner
    ) -> QueryResult:
        return query_plan.execution_strategy.execute(
            query_plan.query, self.__query_settings, runner
        )


class SimplePipelineBuilderNew(QueryPipelineBuilder[ClickhouseQueryPlan]):
    def __init__(self, query_plan_builder: ClickhouseQueryPlanBuilder) -> None:
        self.__query_plan_builder = query_plan_builder

    def build_execution_pipeline(
        self, request: Request, runner: QueryRunner
    ) -> QueryExecutionPipelineNew:
        assert isinstance(request.query, LogicalQuery)
        return SimpleExecutionPipelineNew(request, runner)
