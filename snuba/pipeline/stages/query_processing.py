from typing import Union

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.entities.factory import get_entity
from snuba.pipeline.composite import CompositeExecutionPipeline
from snuba.pipeline.query_pipeline import QueryPipelineResult, QueryPipelineStage
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Table
from snuba.query.logical import Query as LogicalQuery
from snuba.request import Request


class EntityAndStoragePipelineStage(QueryPipelineStage[Request, ClickhouseQuery]):
    def _process_data(
        self, pipe_input: QueryPipelineResult[Request]
    ) -> ClickhouseQuery:
        _clickhouse_query = None

        def _query_runner(
            clickhouse_query: Union[ClickhouseQuery, CompositeQuery[Table]],
            *args,
            **kwargs
        ) -> None:
            nonlocal _clickhouse_query
            _clickhouse_query = clickhouse_query

        if isinstance(pipe_input.data.query, LogicalQuery):
            entity = get_entity(pipe_input.data.query.get_from_clause().key)
            execution_pipeline = (
                entity.get_query_pipeline_builder().build_execution_pipeline(
                    pipe_input.data, _query_runner
                )
            )
        else:
            execution_pipeline = CompositeExecutionPipeline(
                pipe_input.data.query, pipe_input.query_settings, _query_runner
            )

        execution_pipeline.execute()
        return _clickhouse_query
