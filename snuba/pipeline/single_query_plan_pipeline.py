from snuba.pipeline.query_pipeline import (
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

    TODO: Currently only query plan building and processing is done by the query
    pipeline, eventually the rest of the query processing sequence will move into
    the pipeline as well.
    """

    def __init__(
        self,
        request: Request,
        runner: QueryRunner,
        query_plan_builder: ClickhouseQueryPlanBuilder,
    ):
        self.__request = request
        self.__runner = runner
        self.__query_plan = query_plan_builder.build_plan(request)

    def execute(self) -> QueryResult:
        _execute_query_plan_processors(self.__query_plan, self.__request)
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
        return SingleQueryPlanPipeline(request, runner, self.__query_plan_builder)
