from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.plans.entity_processing import run_entity_processing_executor
from snuba.datasets.plans.entity_validation import run_entity_validators
from snuba.datasets.plans.storage_processing import (
    apply_storage_processors,
    build_best_plan,
    get_query_data_source,
)
from snuba.datasets.plans.translator.query import identity_translate
from snuba.datasets.storages.factory import get_storage
from snuba.pipeline.composite_entity_processing import translate_composite_query
from snuba.pipeline.composite_storage_processing import (
    apply_composite_storage_processors,
    build_best_plan_for_composite_query,
)
from snuba.pipeline.query_pipeline import QueryPipelineData, QueryPipelineStage
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Table
from snuba.query.logical import Query as LogicalQuery
from snuba.query.logical import StorageQuery
from snuba.request import Request


class EntityProcessingStage(
    QueryPipelineStage[Request, ClickhouseQuery | CompositeQuery[Table]]
):
    def _process_data(
        self, pipe_input: QueryPipelineData[Request]
    ) -> ClickhouseQuery | CompositeQuery[Table]:
        # TODO: support composite queries for storage queries
        if isinstance(pipe_input.data.query, StorageQuery):
            res = identity_translate(pipe_input.data.query)
            storage = get_storage(pipe_input.data.query.get_from_clause().key)
            res.set_from_clause(
                get_query_data_source(
                    storage.get_schema().get_data_source(),
                    allocation_policies=storage.get_allocation_policies(),
                    final=pipe_input.data.query.get_final(),
                    sampling_rate=pipe_input.data.query.get_sample(),
                    storage_key=storage.get_storage_key(),
                )
            )
            return res
        elif isinstance(pipe_input.data.query, LogicalQuery):
            run_entity_validators(pipe_input.data.query, pipe_input.query_settings)
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
