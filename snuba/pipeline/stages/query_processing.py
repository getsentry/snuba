from typing import cast

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
from snuba.pipeline.storage_query_identity_translate import try_translate_storage_query
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity, Table
from snuba.query.logical import EntityQuery
from snuba.query.logical import Query as LogicalQuery
from snuba.request import Request


class EntityProcessingStage(
    QueryPipelineStage[Request, ClickhouseQuery | CompositeQuery[Table]]
):
    def _process_data(
        self, pipe_input: QueryPipelineData[Request]
    ) -> ClickhouseQuery | CompositeQuery[Table]:
        query = pipe_input.data.query
        translated_storage_query = try_translate_storage_query(query)
        if translated_storage_query:
            return translated_storage_query

        run_entity_validators(cast(EntityQuery, query), pipe_input.query_settings)
        if isinstance(query, LogicalQuery) and isinstance(
            query.get_from_clause(), Entity
        ):
            return run_entity_processing_executor(query, pipe_input.query_settings)
        elif isinstance(query, CompositeQuery):
            # if we were not able to translate the storage query earlier and we got to this point, this is
            # definitely a composite entity query
            return translate_composite_query(
                cast(CompositeQuery[Entity], query),
                pipe_input.query_settings,
            )
        else:
            raise NotImplementedError(f"Unknown query type {type(query)}, {query}")


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
        if pipe_input.query_settings.get_apply_default_subscriptable_mapping():
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
