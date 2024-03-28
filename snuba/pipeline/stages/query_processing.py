from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.pipeline.query_pipeline import QueryPipelineData, QueryPipelineStage
from snuba.pipeline.simple_pipeline_new import SimpleExecutionPipelineNew
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
        assert isinstance(pipe_input.data.query, LogicalQuery)
        execution_pipeline = SimpleExecutionPipelineNew(pipe_input.query_settings)
        entity = get_entity(pipe_input.data.query.get_from_clause().key)
        assert isinstance(entity, PluggableEntity)
        storage_query = execution_pipeline.translate_query_and_apply_mappers(
            pipe_input.data.query, entity.get_new_query_plan_builder()
        )
        return storage_query


class StorageProcessingStage(
    QueryPipelineStage[
        ClickhouseQuery | CompositeQuery[Table],
        ClickhouseQuery | CompositeQuery[Table],
    ]
):
    def _process_data(
        self, pipe_input: QueryPipelineData[ClickhouseQuery | CompositeQuery[Table]]
    ) -> ClickhouseQuery | CompositeQuery[Table]:
        assert isinstance(pipe_input.data, ClickhouseQuery)
        execution_pipeline = SimpleExecutionPipelineNew(pipe_input.query_settings)
        query = execution_pipeline.apply_storage_processors(pipe_input.data)

        return query
