from abc import ABC

from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    ClickhouseQueryPlanBuilder,
    QueryRunner,
)
from snuba.pipeline.processors import (
    EntityProcessorsExecutor,
    ClickhouseProcessorsExecutor,
)
from snuba.pipeline.query_pipeline import (
    EntityProcessingPayload,
    QueryExecutionPipeline,
    QueryPipelineBuilder,
    QueryProcessingPipeline,
)
from snuba.request import Request
from snuba.web import QueryResult


class SinglePlanProcessingPipeline(QueryProcessingPipeline, ABC):
    """
    Executes the processing phase of a single plan query. Which means a
    query based on a single entity, that would produce a query plan based
    on a single storage.
    """

    def __init__(
        self,
        query_plan_builder: ClickhouseQueryPlanBuilder,
        processors: ClickhouseProcessorsExecutor,
    ) -> None:
        self.__query_plan_builder = query_plan_builder
        self.__entity_processors = EntityProcessorsExecutor()
        self.__clickhouse_processors = processors

    def execute(self, query_payload: EntityProcessingPayload) -> ClickhouseQueryPlan:
        self.__entity_processors.execute(query_payload)

        query, settings = query_payload
        query_plan = self.__query_plan_builder.build_plan(query, settings)
        self.__clickhouse_processors.execute((query_plan, settings))
        return query_plan


class PreStrategyProcessingPipeline(SinglePlanProcessingPipeline):
    """
    Executes the query processing of a single query plan query up to the
    plan query processors. It does not execute the db query processors
    since they are ran by the execution strategy.

    This is the processing pipeline to use when executing a simple query
    that references a single entity and a single storage.
    """

    def __init__(self, query_plan_builder: ClickhouseQueryPlanBuilder) -> None:
        super().__init__(
            query_plan_builder,
            ClickhouseProcessorsExecutor(lambda plan: plan.plan_processors),
        )


class SinglePlanExecutionPipeline(QueryExecutionPipeline):
    """
    Executes a simple (single entity) query.
    This relies on a PreStrategyProcessingPipeline to perform the processing
    and then relies on the execution strategy provided by the query plan to
    actually execute the query.
    """

    def __init__(
        self, runner: QueryRunner, query_plan_builder: ClickhouseQueryPlanBuilder,
    ):
        self.__runner = runner
        self.__processing_pipeline = PreStrategyProcessingPipeline(query_plan_builder)

    def execute(self, input: Request) -> QueryResult:
        settings = input.settings

        plan = self.__processing_pipeline.execute((input.query, settings))
        return plan.execution_strategy.execute(plan.query, settings, self.__runner)


class SingleQueryPlanPipelineBuilder(QueryPipelineBuilder):
    def __init__(self, query_plan_builder: ClickhouseQueryPlanBuilder) -> None:
        self.__query_plan_builder = query_plan_builder

    def build_execution_pipeline(
        self, request: Request, runner: QueryRunner
    ) -> QueryExecutionPipeline:
        return SinglePlanExecutionPipeline(runner, self.__query_plan_builder)

    def build_processing_pipeline(self, request: Request) -> QueryProcessingPipeline:
        return PreStrategyProcessingPipeline(self.__query_plan_builder)
