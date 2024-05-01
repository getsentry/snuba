from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.plans.entity_processing import run_entity_processing_executor
from snuba.datasets.plans.entity_validation import run_entity_validators
from snuba.datasets.plans.storage_processing import (
    apply_storage_processors,
    build_best_plan,
    transform_subscriptables,
)
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
    def _apply_default_subscriptable_mapping(
        self, query: ClickhouseQuery | CompositeQuery[Table]
    ) -> None:
        query.transform_expressions(transform_subscriptables)

    def _process_data(
        self, pipe_input: QueryPipelineData[ClickhouseQuery | CompositeQuery[Table]]
    ) -> ClickhouseQuery | CompositeQuery[Table]:
        self._apply_default_subscriptable_mapping(pipe_input.data)
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
