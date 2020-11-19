from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    ClickhouseQueryPlanBuilder,
    QueryRunner,
)
from snuba.pipeline.processors import (
    execute_all_clickhouse_processors,
    execute_entity_processors,
    execute_pre_strategy_processors,
)
from snuba.pipeline.query_pipeline import (
    EntityQueryProcessingPipeline,
    QueryExecutionPipeline,
    QueryPipelineBuilder,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.request import Request
from snuba.request.request_settings import RequestSettings
from snuba.web import QueryResult


class SimpleQueryProcessingPipeline(EntityQueryProcessingPipeline):
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
        settings: RequestSettings,
        query_plan_builder: ClickhouseQueryPlanBuilder,
    ) -> None:
        self.__query = query
        self.__settings = settings
        self.__query_plan_builder = query_plan_builder

    def execute(self) -> ClickhouseQueryPlan:
        execute_entity_processors(self.__query, self.__settings)

        query_plan = self.__query_plan_builder.build_plan(self.__query, self.__settings)
        execute_all_clickhouse_processors(query_plan, self.__settings)
        return query_plan


class SimpleExecutionPipeline(QueryExecutionPipeline):
    """
    Executes a simple (single entity) query.
    """

    def __init__(
        self,
        request: Request,
        runner: QueryRunner,
        query_plan_builder: ClickhouseQueryPlanBuilder,
    ):
        self.__request = request
        self.__runner = runner
        self.__query_plan_builder = query_plan_builder

    def execute(self) -> QueryResult:
        settings = self.__request.settings
        query = self.__request.query

        execute_entity_processors(query, settings)

        query_plan = self.__query_plan_builder.build_plan(query, settings)
        execute_pre_strategy_processors(query_plan, settings)

        return query_plan.execution_strategy.execute(
            query_plan.query, settings, self.__runner
        )


class SimplePipelineBuilder(QueryPipelineBuilder):
    def __init__(self, query_plan_builder: ClickhouseQueryPlanBuilder) -> None:
        self.__query_plan_builder = query_plan_builder

    def build_execution_pipeline(
        self, request: Request, runner: QueryRunner
    ) -> QueryExecutionPipeline:
        return SimpleExecutionPipeline(request, runner, self.__query_plan_builder)

    def build_processing_pipeline(
        self, query: LogicalQuery, settings: RequestSettings,
    ) -> EntityQueryProcessingPipeline:
        return SimpleQueryProcessingPipeline(query, settings, self.__query_plan_builder)
