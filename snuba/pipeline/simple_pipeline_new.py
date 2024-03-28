from typing import Sequence

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    ClickhouseQueryPlanBuilder,
    QueryRunner,
)
from snuba.datasets.plans.storage_plan_builder_new import (
    ClickhouseQueryPlanBuilderNew,
    apply_storage_processors,
)
from snuba.pipeline.processors import execute_entity_processors
from snuba.pipeline.query_pipeline import (
    QueryExecutionPipeline,
    QueryPipelineBuilder,
    QueryPlanner,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.query.query_settings import QuerySettings
from snuba.request import Request


class StorageQueryPlanner(QueryPlanner[ClickhouseQueryPlan]):
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
        query: ClickhouseQuery,
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


class SimpleExecutionPipelineNew(QueryExecutionPipeline):
    """
    An execution pipeline for a simple (single entity) query.
    This class contains methods used by a QueryPipelineStage which does
    thins like entity processing, storage processing, and query execution
    """

    def __init__(
        self,
        query_settings: QuerySettings,
    ) -> None:
        self.__query_settings = query_settings

    def translate_query_and_apply_mappers(
        self, query: LogicalQuery, query_plan_builder: ClickhouseQueryPlanBuilderNew
    ) -> ClickhouseQuery:
        """
        Used by the EntityProcessingStage to apply entity processors, translate the query,
        and apply translation mappers.
        """
        execute_entity_processors(query, self.__query_settings)
        clickhouse_query = query_plan_builder.translate_query_and_apply_mappers(
            query, self.__query_settings
        )
        return clickhouse_query

    def apply_storage_processors(self, query: ClickhouseQuery) -> ClickhouseQuery:
        """
        Used by the StorageProcessing stage to apply storage/clickhouse processors and
        create the ClickHouseQueryPlan.
        """
        apply_storage_processors(query, self.__query_settings)
        return query


class SimplePipelineBuilderNew(QueryPipelineBuilder[ClickhouseQueryPlan]):
    def build_execution_pipeline(
        self, request: Request, runner: QueryRunner
    ) -> QueryExecutionPipeline:
        assert isinstance(request.query, LogicalQuery)
        return SimpleExecutionPipelineNew(request, runner)
