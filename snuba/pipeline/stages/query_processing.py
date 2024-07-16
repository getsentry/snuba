import typing
from typing import cast

from snuba import state
from snuba.clickhouse.query import Query
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.plans.entity_processing import run_entity_processing_executor
from snuba.datasets.plans.entity_validation import run_entity_validators
from snuba.datasets.plans.storage_processing import (
    apply_storage_processors,
    build_best_plan,
    transform_subscriptables,
)
from snuba.datasets.storage import ReadableTableStorage
from snuba.pipeline.composite_entity_processing import translate_composite_query
from snuba.pipeline.composite_storage_processing import (
    apply_composite_storage_processors,
    build_best_plan_for_composite_query,
)
from snuba.pipeline.query_pipeline import QueryPipelineResult, QueryPipelineStage
from snuba.pipeline.storage_query_identity_translate import try_translate_storage_query
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity, Table
from snuba.query.exceptions import MaxRowsEnforcerException
from snuba.query.logical import EntityQuery
from snuba.query.logical import Query as LogicalQuery
from snuba.request import Request


class EntityProcessingStage(
    QueryPipelineStage[Request, ClickhouseQuery | CompositeQuery[Table]]
):
    def _process_data(
        self, pipe_input: QueryPipelineResult[Request]
    ) -> ClickhouseQuery | CompositeQuery[Table]:
        pipe_input_data = pipe_input.as_data().data
        query = pipe_input_data.query
        translated_storage_query = try_translate_storage_query(query)
        if translated_storage_query:
            return translated_storage_query

        if isinstance(query, LogicalQuery) and isinstance(
            query.get_from_clause(), Entity
        ):
            run_entity_validators(cast(EntityQuery, query), pipe_input.query_settings)
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
        if state.get_config("apply_default_subscriptable_mapping", 1):
            query.transform_expressions(transform_subscriptables)

    def _process_data(
        self, pipe_input: QueryPipelineResult[ClickhouseQuery | CompositeQuery[Table]]
    ) -> ClickhouseQuery | CompositeQuery[Table]:
        pipe_input_data = pipe_input.as_data().data
        self._apply_default_subscriptable_mapping(pipe_input_data)
        if isinstance(pipe_input_data, ClickhouseQuery):
            query_plan = build_best_plan(pipe_input_data, pipe_input.query_settings, [])
            return apply_storage_processors(
                pipe_input, query_plan, pipe_input.query_settings
            )
        else:
            composite_query_plan = build_best_plan_for_composite_query(
                pipe_input_data, pipe_input.query_settings, []
            )
            return apply_composite_storage_processors(
                composite_query_plan, pipe_input.query_settings
            )


class MaxRowsEnforcerStage(
    QueryPipelineStage[
        ClickhouseQuery | CompositeQuery[Table],
        ClickhouseQuery | CompositeQuery[Table],
    ]
):
    def _process_data(
        self, pipe_input: QueryPipelineResult[ClickhouseQuery | CompositeQuery[Table]]
    ) -> ClickhouseQuery | CompositeQuery[Table]:
        storage = typing.cast(ReadableTableStorage, pipe_input.storage)
        deletion_settings = storage.get_deletion_settings()
        clickhouse = storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.QUERY
        )
        rows_to_delete = 0
        for table in deletion_settings.tables:
            ret = clickhouse.execute(
                f"SELECT COUNT() FROM {table} {pipe_input.request_query}"
            )
            rows_to_delete += ret.results[0][0]

        if rows_to_delete > deletion_settings.max_rows_to_delete:
            raise MaxRowsEnforcerException(
                f"Query wants to delete {rows_to_delete} rows, but the maximum allowed is {deletion_settings.max_rows_to_delete}"
            )

        return typing.cast(Query | CompositeQuery[Table], pipe_input.data)
