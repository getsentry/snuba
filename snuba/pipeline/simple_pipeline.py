from typing import Sequence

from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    ClickhouseQueryPlanBuilder,
    QueryRunner,
)
from snuba.pipeline.processors import execute_entity_processors, execute_plan_processors
from snuba.pipeline.query_pipeline import (
    QueryExecutionPipeline,
    QueryPipelineBuilder,
    QueryPlanner,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.query.query_settings import QuerySettings
from snuba.request import Request
from snuba.web import QueryResult


class EntityQueryPlanner(QueryPlanner[ClickhouseQueryPlan]):
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
        execute_entity_processors(self.__query, self.__settings)

        return self.__query_plan_builder.build_best_plan(self.__query, self.__settings)

    def build_and_rank_plans(self) -> Sequence[ClickhouseQueryPlan]:
        execute_entity_processors(self.__query, self.__settings)

        return self.__query_plan_builder.build_and_rank_plans(
            self.__query, self.__settings
        )


class SimpleExecutionPipeline(QueryExecutionPipeline):
    """
    Executes a simple (single entity) query.
    """

    def __init__(
        self,
        request: Request,
        runner: QueryRunner,
        query_planner: EntityQueryPlanner,
    ):
        self.__request = request
        self.__runner = runner
        self.__query_planner = query_planner

    def execute(self) -> QueryResult:
        settings = self.__request.query_settings
        query_plan = self.__query_planner.build_best_plan()
        execute_plan_processors(query_plan, settings)

        return query_plan.execution_strategy.execute(
            query_plan.query, settings, self.__runner
        )


class SimplePipelineBuilder(QueryPipelineBuilder[ClickhouseQueryPlan]):
    def __init__(self, query_plan_builder: ClickhouseQueryPlanBuilder) -> None:
        self.__query_plan_builder = query_plan_builder

    def build_execution_pipeline(
        self, request: Request, runner: QueryRunner
    ) -> QueryExecutionPipeline:
        assert isinstance(request.query, LogicalQuery)
        return SimpleExecutionPipeline(
            request,
            runner,
            self.build_planner(request.query, request.query_settings),
        )

    def build_planner(
        self,
        query: LogicalQuery,
        settings: QuerySettings,
    ) -> EntityQueryPlanner:
        return EntityQueryPlanner(query, settings, self.__query_plan_builder)
