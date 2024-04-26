from typing import Any, cast

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.plans.entity_processing import run_entity_processing_executor
from snuba.datasets.plans.entity_validation import run_entity_validators
from snuba.datasets.plans.query_plan import QueryRunner
from snuba.datasets.plans.storage_processing import (
    apply_storage_processors,
    build_best_plan,
)
from snuba.pipeline.composite import CompositeExecutionPipeline
from snuba.pipeline.composite_entity_processing import translate_composite_query
from snuba.pipeline.composite_storage_processing import (
    apply_composite_storage_processors,
    build_best_plan_for_composite_query,
)
from snuba.pipeline.query_pipeline import QueryPipelineData, QueryPipelineStage
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Table
from snuba.query.logical import Query as LogicalQuery
from snuba.request import Request


class EntityAndStoragePipelineStage(
    QueryPipelineStage[Request, ClickhouseQuery | CompositeQuery[Table]]
):
    def _process_data(
        self, pipe_input: QueryPipelineData[Request]
    ) -> ClickhouseQuery | CompositeQuery[Table]:
        _clickhouse_query = None

        def _query_runner(
            clickhouse_query: ClickhouseQuery | CompositeQuery[Table],
            *args: Any,
            **kwargs: str
        ) -> None:
            nonlocal _clickhouse_query
            _clickhouse_query = clickhouse_query

        if isinstance(pipe_input.data.query, LogicalQuery):
            entity = get_entity(pipe_input.data.query.get_from_clause().key)
            execution_pipeline = (
                entity.get_query_pipeline_builder().build_execution_pipeline(
                    pipe_input.data, cast(QueryRunner, _query_runner)
                )
            )
        else:
            execution_pipeline = CompositeExecutionPipeline(
                pipe_input.data.query,
                pipe_input.query_settings,
                cast(QueryRunner, _query_runner),
            )
        execution_pipeline.execute()
        assert _clickhouse_query is not None
        return _clickhouse_query


class EntityProcessingStage(
    QueryPipelineStage[Request, ClickhouseQuery | CompositeQuery[Table]]
):
    def _process_data(
        self, pipe_input: QueryPipelineData[Request]
    ) -> ClickhouseQuery | CompositeQuery[Table]:
        # Execute entity validators on logical query
        run_entity_validators(pipe_input.data.query, pipe_input.query_settings)

        # Run entity processors on the query and transform logical query into a physical query
        if isinstance(pipe_input.data.query, LogicalQuery):
            return run_entity_processing_executor(
                pipe_input.data.query, pipe_input.query_settings
            )
        else:
            return translate_composite_query(
                pipe_input.data.query, pipe_input.query_settings
            )


class StorageProcessingStage(
    QueryPipelineStage[
        ClickhouseQuery | CompositeQuery[Table],
        ClickhouseQuery | CompositeQuery[Table],
    ]
):
    def _process_data(
        self, pipe_input: QueryPipelineData[ClickhouseQuery | CompositeQuery[Table]]
    ) -> ClickhouseQuery | CompositeQuery[Table]:
        if isinstance(pipe_input.data, ClickhouseQuery):
            query_plan = build_best_plan(pipe_input.data, pipe_input.query_settings, [])
            return apply_storage_processors(query_plan, pipe_input.query_settings)
        else:
            composite_query_plan = build_best_plan_for_composite_query(
                pipe_input.data, pipe_input.query_settings, []
            )
            return apply_composite_storage_processors(
                composite_query_plan, pipe_input.query_settings
            )
