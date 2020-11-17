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
    QueryExecutionPipeline,
    QueryPipelineBuilder,
    QueryProcessingPipeline,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.request import Request
from snuba.request.request_settings import RequestSettings
from snuba.web import QueryResult


class SinglePlanProcessingPipeline(QueryProcessingPipeline):
    """
    Executes the processing phase of a single plan query. Which means a
    query based on a single entity, that would produce a query plan based
    on a single storage.

    This should not be used if the plan execution strategy is then used
    to execute the query as it executes all query processors.
    The main use case is for subqueries.
    """

    def __init__(self, query_plan_builder: ClickhouseQueryPlanBuilder,) -> None:
        self.__query_plan_builder = query_plan_builder

    def execute(
        self, query: LogicalQuery, settings: RequestSettings
    ) -> ClickhouseQueryPlan:
        execute_entity_processors(query, settings)

        query_plan = self.__query_plan_builder.build_plan(query, settings)
        execute_all_clickhouse_processors(query_plan, settings)
        return query_plan


class SinglePlanExecutionPipeline(QueryExecutionPipeline):
    """
    Executes a simple (single entity) query.
    """

    def __init__(
        self, runner: QueryRunner, query_plan_builder: ClickhouseQueryPlanBuilder,
    ):
        self.__runner = runner
        self.__query_plan_builder = query_plan_builder

    def execute(self, input: Request) -> QueryResult:
        settings = input.settings
        query = input.query

        execute_entity_processors(query, settings)

        query_plan = self.__query_plan_builder.build_plan(query, settings)
        execute_pre_strategy_processors(query_plan, settings)

        return query_plan.execution_strategy.execute(
            query_plan.query, settings, self.__runner
        )


class SingleQueryPlanPipelineBuilder(QueryPipelineBuilder):
    def __init__(self, query_plan_builder: ClickhouseQueryPlanBuilder) -> None:
        self.__query_plan_builder = query_plan_builder

    def build_execution_pipeline(
        self, request: Request, runner: QueryRunner
    ) -> QueryExecutionPipeline:
        return SinglePlanExecutionPipeline(runner, self.__query_plan_builder)

    def build_processing_pipeline(self, request: Request) -> QueryProcessingPipeline:
        return SinglePlanProcessingPipeline(self.__query_plan_builder)
