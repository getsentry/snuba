from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.plans.storage_plan_builder_new import StorageQueryPlanBuilderNew
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.pipeline.processors import execute_entity_processors
from snuba.pipeline.query_pipeline import QueryPipelineData, QueryPipelineStage
from snuba.pipeline.simple_pipeline_new import apply_storage_processors
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Table
from snuba.query.logical import Query as LogicalQuery
from snuba.request import Request


class EntityProcessingStage(
    QueryPipelineStage[Request, ClickhouseQuery | CompositeQuery[Table]]
):
    def _process_data(
        self, pipe_input: QueryPipelineData[Request]
    ) -> ClickhouseQuery | CompositeQuery[Table]:

        # TODO: support composite queries
        assert isinstance(pipe_input.data.query, LogicalQuery)
        entity = get_entity(pipe_input.data.query.get_from_clause().key)
        assert isinstance(entity, PluggableEntity)
        execute_entity_processors(pipe_input.data.query, pipe_input.query_settings)
        query_plan_builder = entity.get_new_query_plan_builder()
        assert isinstance(query_plan_builder, StorageQueryPlanBuilderNew)
        clickhouse_query = query_plan_builder.translate_query_and_apply_mappers(
            pipe_input.data.query, pipe_input.query_settings
        )
        return clickhouse_query


class StorageProcessingStage(
    QueryPipelineStage[
        ClickhouseQuery | CompositeQuery[Table],
        ClickhouseQuery | CompositeQuery[Table],
    ]
):
    def _process_data(
        self, pipe_input: QueryPipelineData[ClickhouseQuery | CompositeQuery[Table]]
    ) -> ClickhouseQuery | CompositeQuery[Table]:
        # TODO: support composite queries
        assert isinstance(pipe_input.data, ClickhouseQuery)

        query = apply_storage_processors(pipe_input.data, pipe_input.query_settings)

        return query
