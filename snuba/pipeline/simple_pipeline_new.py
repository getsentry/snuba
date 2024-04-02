from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan, QueryRunner
from snuba.datasets.plans.storage_plan_builder_new import (
    ClickhouseQueryPlanBuilderNew,
    apply_storage_processors,
)
from snuba.pipeline.processors import execute_entity_processors
from snuba.pipeline.query_pipeline import QueryExecutionPipeline, QueryPipelineBuilder
from snuba.query.logical import Query as LogicalQuery
from snuba.query.query_settings import QuerySettings
from snuba.request import Request


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
