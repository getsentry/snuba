from typing import Any, cast

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.plans.query_plan import QueryRunner
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.pipeline.composite_new import CompositeExecutionPipelineNew
from snuba.pipeline.query_pipeline import QueryPipelineData, QueryPipelineStage
from snuba.pipeline.simple_pipeline_new import SimpleExecutionPipelineNew
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity, Table
from snuba.query.logical import Query as LogicalQuery
from snuba.request import Request


class EntityProcessingStage(
    QueryPipelineStage[Request, LogicalQuery | CompositeQuery[Entity]]
):
    def _process_data(
        self, pipe_input: QueryPipelineData[Request]
    ) -> LogicalQuery | CompositeQuery[Entity]:
        from snuba.pipeline.processors import execute_entity_processors

        if isinstance(pipe_input.data.query, LogicalQuery):
            execute_entity_processors(pipe_input.data.query, pipe_input.query_settings)
        else:
            pass
        return pipe_input.data.query


class StorageProcessingStage(
    QueryPipelineStage[
        LogicalQuery | CompositeQuery[Entity], ClickhouseQuery | CompositeQuery[Table]
    ]
):
    def _process_data(
        self, pipe_input: QueryPipelineData[LogicalQuery | CompositeQuery[Entity]]
    ) -> ClickhouseQuery | CompositeQuery[Table]:
        _clickhouse_query = None

        def _query_runner(
            clickhouse_query: ClickhouseQuery | CompositeQuery[Table],
            *args: Any,
            **kwargs: str
        ) -> None:
            nonlocal _clickhouse_query
            _clickhouse_query = clickhouse_query

        if isinstance(pipe_input.data, LogicalQuery):
            execution_pipeline = SimpleExecutionPipelineNew(
                pipe_input.data, pipe_input.query_settings
            )
            # TODO: Figure out how to do this without entity
            entity = get_entity(pipe_input.data.get_from_clause().key)
            assert isinstance(entity, PluggableEntity)
            query_plan = execution_pipeline.create_plan(entity.get_query_plan_builder())
        else:
            execution_pipeline = CompositeExecutionPipelineNew(
                pipe_input.data,
                pipe_input.query_settings,
                cast(QueryRunner, _query_runner),
            )
            query_plan = execution_pipeline.create_plan()

        return query_plan.query
