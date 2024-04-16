from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Generic, Optional, Sequence, TypeVar, Union

import sentry_sdk

from snuba import settings as snuba_settings
from snuba.clickhouse.query import Query
from snuba.clusters.storage_sets import StorageSetKey
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
from snuba.query import Query as AbstractQuery
from snuba.query.allocation_policies import AllocationPolicy
from snuba.query.data_source.simple import Table
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


@dataclass(frozen=True)
class QueryPlanNew(ABC, Generic[TQuery]):
    """
    Provides the directions to execute a Clickhouse Query against one
    storage or multiple joined ones.

    This is produced in the storage processing stage of the query pipeline.

    It embeds the Clickhouse Query (the query to run on the storage
    after translation). It also provides a plan execution strategy
    that takes care of coordinating the execution of the query against
    the database.

    When running a query we need a cluster, the cluster is picked according
    to the storages sets containing the storages used in the query.
    So the plan keeps track of the storage set as well.
    There must be only one storage set per query.
    """

    query: TQuery
    storage_set_key: StorageSetKey


@dataclass(frozen=True)
class ClickhouseQueryPlanNew(QueryPlanNew[Query]):
    """
    Query plan for a single entity, single storage query.

    It provides the sequence of storage specific QueryProcessors
    to apply to the query after the the storage has been selected.
    These are divided in two sequences: plan processors and DB
    processors.
    Plan processors and DB Query Processors are both executed only
    once per plan.
    """

    # Per https://github.com/python/mypy/issues/10039, this has to be redeclared
    # to avoid a mypy error.
    plan_query_processors: Sequence[ClickhouseQueryProcessor]
    db_query_processors: Sequence[ClickhouseQueryProcessor]


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


from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause


def build_best_plan(
    physical_query: Union[
        Query, ProcessableQuery, CompositeQuery, JoinClause, IndividualNode
    ],
    settings: QuerySettings,
    post_processors: Sequence[ClickhouseQueryProcessor] = [],
) -> ClickhouseQueryPlanNew:
    storage_key = StorageKeyFinder().visit(physical_query)
    storage = get_storage(storage_key)

    # Return failure if storage readiness state is not supported in current environment
    check_storage_readiness(storage)

    db_query_processors = [
        *storage.get_query_processors(),
        *post_processors,
        MandatoryConditionApplier(),
        MandatoryConditionEnforcer(storage.get_mandatory_condition_checkers()),
    ]

    return ClickhouseQueryPlanNew(
        query=physical_query,
        plan_query_processors=[],
        db_query_processors=db_query_processors,
        storage_set_key=storage.get_storage_set_key(),
    )


@with_span()
def apply_storage_processors(
    query_plan: ClickhouseQueryPlanNew,
    settings: QuerySettings,
    post_processors: Sequence[ClickhouseQueryProcessor] = [],
) -> Query:
    # storage selection should not be done through the entity anymore.
    storage_key = StorageKeyFinder().visit(query_plan.query)
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
