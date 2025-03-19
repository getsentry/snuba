from __future__ import annotations

import re
from typing import Optional, Sequence, TypeVar, Union

import sentry_sdk

from snuba import settings as snuba_settings
from snuba.clickhouse.query import Query
from snuba.clickhouse.translators.snuba.mappers import SubscriptableMapper
from snuba.clickhouse.translators.snuba.mapping import (
    SnubaClickhouseMappingTranslator,
    TranslationMappers,
)
from snuba.clusters.cluster import get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan
from snuba.datasets.schemas import RelationalSource
from snuba.datasets.schemas.tables import TableSource
from snuba.datasets.slicing import is_storage_set_sliced
from snuba.datasets.storage import (
    ReadableStorage,
    ReadableTableStorage,
    StorageNotAvailable,
)
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.utils.storage_finder import StorageKeyFinder
from snuba.query import ProcessableQuery
from snuba.query import Query as AbstractQuery
from snuba.query.allocation_policies import AllocationPolicy
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Expression, SubscriptableReference
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.processors.physical.conditions_enforcer import (
    MandatoryConditionEnforcer,
)
from snuba.query.processors.physical.mandatory_condition_applier import (
    MandatoryConditionApplier,
)
from snuba.query.query_settings import QuerySettings
from snuba.state import explain_meta
from snuba.utils.metrics.util import with_span

TQuery = TypeVar("TQuery", bound=AbstractQuery)


def get_query_data_source(
    relational_source: RelationalSource,
    allocation_policies: list[AllocationPolicy],
    final: bool,
    sampling_rate: Optional[float],
    storage_key: StorageKey,
) -> Table:
    assert isinstance(relational_source, TableSource)
    return Table(
        table_name=relational_source.get_table_name(),
        schema=relational_source.get_columns(),
        allocation_policies=allocation_policies,
        final=final,
        sampling_rate=sampling_rate,
        mandatory_conditions=relational_source.get_mandatory_conditions(),
        storage_key=storage_key,
    )


def check_storage_readiness(storage: ReadableStorage) -> None:
    # Return failure if storage readiness state is not supported in current environment
    if snuba_settings.READINESS_STATE_FAIL_QUERIES:
        assert isinstance(storage, ReadableTableStorage)
        readiness_state = storage.get_readiness_state()
        if readiness_state.value not in snuba_settings.SUPPORTED_STATES:
            raise StorageNotAvailable(
                StorageNotAvailable.__name__,
                f"The selected storage={storage.get_storage_key().value} is not available in this environment yet. To enable it, consider bumping the storage's readiness_state.",
            )


def _is_downsampled_storage_key(storage_key: str) -> bool:
    return bool(re.match(r"^StorageKey\.EAP_ITEMS_DOWNSAMPLE_\d+$", storage_key))


def _get_corresponding_table(storage_key: str) -> str:
    downsampling_factor = re.search(
        r"StorageKey\.EAP_ITEMS_DOWNSAMPLE_(\d+)", storage_key
    )
    assert downsampling_factor is not None
    return (
        f"eap_items_1_downsample_{downsampling_factor.group(1)}_local"
        if get_cluster(StorageSetKey.EVENTS_ANALYTICS_PLATFORM).is_single_node()
        else f"eap_items_1_downsample_{downsampling_factor.group(1)}_dist"
    )


def build_best_plan(
    physical_query: Union[Query, ProcessableQuery[Table]],
    settings: QuerySettings,
    post_processors: Sequence[ClickhouseQueryProcessor] = [],
) -> ClickhouseQueryPlan:
    storage_key = StorageKeyFinder().visit(physical_query)
    if _is_downsampled_storage_key(str(storage_key)):
        storage = get_storage(StorageKey.EAP_ITEMS)
    else:
        storage = get_storage(storage_key)

    # Return failure if storage readiness state is not supported in current environment
    check_storage_readiness(storage)

    db_query_processors = [
        *storage.get_query_processors(),
        *post_processors,
        MandatoryConditionApplier(),
        MandatoryConditionEnforcer(storage.get_mandatory_condition_checkers()),
    ]

    return ClickhouseQueryPlan(
        query=physical_query,
        plan_query_processors=[],
        db_query_processors=db_query_processors,
        storage_set_key=storage.get_storage_set_key(),
    )


def transform_subscriptables(expression: Expression) -> Expression:
    # IF we are in the storage processing stage and we have a subscriptable reference, we just
    # assume that it maps to an associative array column with the same name and `value` as the
    # name of the value column
    if isinstance(expression, SubscriptableReference):
        res = SubscriptableMapper(
            expression.column.table_name,
            expression.column.column_name,
            expression.column.table_name,
            expression.column.column_name,
            "value",
        ).attempt_map(
            expression, SnubaClickhouseMappingTranslator(TranslationMappers())
        )
        if res is None:
            raise ValueError(f"Could not map subscriptable reference: {expression}")
        return res
    return expression


@with_span()
def apply_storage_processors(
    query_plan: ClickhouseQueryPlan,
    settings: QuerySettings,
    post_processors: Sequence[ClickhouseQueryProcessor] = [],
) -> Query:
    # storage selection should not be done through the entity anymore.
    storage_key = StorageKeyFinder().visit(query_plan.query)
    if _is_downsampled_storage_key(str(storage_key)):
        storage = get_storage(StorageKey.EAP_ITEMS)
    else:
        storage = get_storage(storage_key)

    if is_storage_set_sliced(storage.get_storage_set_key()):
        raise NotImplementedError("sliced storages not supported in new pipeline")

    check_storage_readiness(storage)

    with sentry_sdk.start_span(
        op="build_plan.storage_query_plan_builder", description="set_from_clause"
    ):
        query_plan.query.set_from_clause(
            get_query_data_source(
                storage.get_schema().get_data_source(),
                allocation_policies=storage.get_allocation_policies(),
                final=query_plan.query.get_from_clause().final,
                sampling_rate=query_plan.query.get_from_clause().sampling_rate,
                storage_key=storage.get_storage_key(),
            )
        )

    if _is_downsampled_storage_key(str(storage_key)):
        original_table = query_plan.query.get_from_clause()
        query_plan.query.set_from_clause(
            Table(
                table_name=_get_corresponding_table(str(storage_key)),
                schema=original_table.schema,
                storage_key=original_table.storage_key,
                allocation_policies=original_table.allocation_policies,
                final=original_table.final,
                sampling_rate=original_table.sampling_rate,
                mandatory_conditions=original_table.mandatory_conditions,
            )
        )

    assert isinstance(query_plan.query, Query)
    for processor in query_plan.db_query_processors:
        with sentry_sdk.start_span(
            description=type(processor).__name__, op="processor"
        ):
            if settings.get_dry_run():
                with explain_meta.with_query_differ(
                    "storage_processor", type(processor).__name__, query_plan.query
                ):
                    processor.process_query(query_plan.query, settings)
            else:
                processor.process_query(query_plan.query, settings)

    return query_plan.query
