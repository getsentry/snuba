from snuba.datasets.pipeline.query_pipeline import (
    QueryPipeline,
    QueryPipelineBuilder,
    _execute_query_plan_processors,
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
    """

    def __init__(
        self, request: Request, query_plan_builder: ClickhouseQueryPlanBuilder
    ):
        self.__request = request
        self.__query_plan = query_plan_builder.build_plan(request)
        _execute_query_plan_processors(self.__query_plan, self.__request)

    def execute(self, runner: QueryRunner) -> QueryResult:
        return self.__query_plan.execution_strategy.execute(
            self.__query_plan.query, self.__request.settings, runner
        )

    @property
    def query_plan(self) -> ClickhouseQueryPlan:
        return self.__query_plan


class SingleQueryPlanPipelineBuilder(QueryPipelineBuilder):
    def __init__(self, query_plan_builder: ClickhouseQueryPlanBuilder) -> None:
        self.__query_plan_builder = query_plan_builder

    def build_pipeline(self, request: Request) -> QueryPipeline:
        return SingleQueryPlanPipeline(request, self.__query_plan_builder)
