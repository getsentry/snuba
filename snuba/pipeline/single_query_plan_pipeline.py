from snuba.pipeline.query_pipeline import (
    QueryPipeline,
    QueryPipelineBuilder,
)
from snuba.pipeline.processors import (
    QueryPlanProcessorsExecutor,
    EntityProcessingExecutor,
)
from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    ClickhouseQueryPlanBuilder,
    QueryRunner,
)
from snuba.request import Request
from snuba.web import QueryResult


class SingleQueryPlanPipeline(QueryPipeline):
    """
    A query pipeline for a single query plan.

    TODO: Currently only query plan building and processing is done by the query
    pipeline, eventually the rest of the query processing sequence will move into
    the pipeline as well.
    """

    def __init__(
        self,
        request: Request,
        runner: QueryRunner,
        query_plan_builder: ClickhouseQueryPlanBuilder,
        entity_processors: EntityProcessingExecutor,
        clickhouse_processors: QueryPlanProcessorsExecutor,
    ):
        self.__request = request
        self.__runner = runner
        self.__query_plan_builder = query_plan_builder
        self.__entity_processors = entity_processors
        self.__clickhouse_processors = clickhouse_processors

    def execute(self) -> QueryResult:
        settings = self.__request.settings

        # Execute entity processors
        self.__entity_processors.execute((self.__request.query, settings))

        # Build and execute query plan
        self.__query_plan = self.__query_plan_builder.build_plan(self.__request)
        self.__clickhouse_processors.execute((self.__query_plan, settings))
        return self.__query_plan.execution_strategy.execute(
            self.__query_plan.query, self.__request.settings, self.__runner
        )

    @property
    def query_plan(self) -> ClickhouseQueryPlan:
        return self.__query_plan


class SingleQueryPlanPipelineBuilder(QueryPipelineBuilder):
    def __init__(self, query_plan_builder: ClickhouseQueryPlanBuilder) -> None:
        self.__query_plan_builder = query_plan_builder

    def build_pipeline(self, request: Request, runner: QueryRunner) -> QueryPipeline:
        return SingleQueryPlanPipeline(
            request,
            runner,
            self.__query_plan_builder,
            EntityProcessingExecutor(),
            QueryPlanProcessorsExecutor(),
        )
