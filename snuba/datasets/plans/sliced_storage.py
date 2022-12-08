from abc import ABC, abstractmethod
from typing import Optional, Sequence

import sentry_sdk

from snuba import state
from snuba.clickhouse.query_dsl.accessors import get_object_ids_in_query_ast
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.clusters.cluster import ClickhouseCluster, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    ClickhouseQueryPlanBuilder,
)
from snuba.datasets.plans.single_storage import (
    SimpleQueryPlanExecutionStrategy,
    get_query_data_source,
)
from snuba.datasets.plans.translator.query import QueryTranslator
from snuba.datasets.slicing import (
    is_storage_set_sliced,
    map_logical_partition_to_slice,
    map_org_id_to_logical_partition,
)
from snuba.datasets.storage import ReadableStorage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.processors.physical.conditions_enforcer import (
    MandatoryConditionEnforcer,
)
from snuba.query.processors.physical.mandatory_condition_applier import (
    MandatoryConditionApplier,
)
from snuba.query.query_settings import QuerySettings
from snuba.util import with_span

MEGA_CLUSTER_RUNTIME_CONFIG_PREFIX = "slicing_mega_cluster_partitions"


class StorageClusterSelector(ABC):
    """
    The component provided by a dataset and used at the beginning of the
    execution of a query to pick the storage set a query should be executed
    onto.
    """

    @abstractmethod
    def select_cluster(
        self, query: LogicalQuery, query_settings: QuerySettings
    ) -> ClickhouseCluster:
        raise NotImplementedError


def _should_use_mega_cluster(
    storage_set: StorageSetKey, logical_partition: int
) -> bool:
    """
    Helper method to find out whether a logical partition of a sliced storage
    set needs to send queries to the mega cluster.

    Mega cluster needs to be used when a logical partition's data might be
    across multiple slices. This happens when a logical partition is mapped
    to a new slice. In such cases, the old data resides in some different
    slice than what the new mapping says.
    """
    key = f"{MEGA_CLUSTER_RUNTIME_CONFIG_PREFIX}_{storage_set.value}"
    state.get_config(key, None)
    slicing_read_override_config = state.get_config(key, None)

    if slicing_read_override_config is None:
        return False

    slicing_read_override_config = slicing_read_override_config[1:-1]
    if slicing_read_override_config:
        logical_partition_overrides = [
            int(p.strip()) for p in slicing_read_override_config.split(",")
        ]
        if logical_partition in logical_partition_overrides:
            return True

    return False


class ColumnBasedStorageSliceSelector(StorageClusterSelector):
    """
    Storage slice selector based on a specific column of the query. This is
    needed in order to select the cluster to use for a query. The cluster
    selection depends on the value of the partition_key_column_name.

    In most cases, the partition_key_column_name would get mapped to a logical
    partition and then to a slice. But when a logical partition is being
    transitioned to another slice, the old data resides in a different slice. In
    those cases, we need to use the mega cluster to query both the old data and
    the new data.
    """

    def __init__(
        self,
        storage: StorageKey,
        storage_set: StorageSetKey,
        partition_key_column_name: str,
    ) -> None:
        self.storage = storage
        self.storage_set = storage_set
        self.partition_key_column_name = partition_key_column_name

    def select_cluster(
        self, query: LogicalQuery, query_settings: QuerySettings
    ) -> ClickhouseCluster:
        """
        Selects the cluster to use for a query if the storage set is sliced.
        If the storage set is not sliced, it returns the default cluster.
        """
        if not is_storage_set_sliced(self.storage_set):
            return get_cluster(self.storage_set)

        org_ids = get_object_ids_in_query_ast(query, self.partition_key_column_name)
        assert org_ids is not None
        assert len(org_ids) == 1
        org_id = org_ids.pop()

        logical_partition = map_org_id_to_logical_partition(org_id)
        if _should_use_mega_cluster(self.storage_set, logical_partition):
            return get_cluster(self.storage_set)
        else:
            slice_id = map_logical_partition_to_slice(
                self.storage_set, logical_partition
            )
            cluster = get_cluster(self.storage_set, slice_id)
            return cluster


class SlicedStorageQueryPlanBuilder(ClickhouseQueryPlanBuilder):
    """
    Builds the Clickhouse Query Execution Plan for a dataset that is
    sliced.
    """

    def __init__(
        self,
        storage: ReadableStorage,
        storage_cluster_selector: StorageClusterSelector,
        mappers: Optional[TranslationMappers] = None,
        post_processors: Optional[Sequence[ClickhouseQueryProcessor]] = None,
    ) -> None:
        self.__storage = storage
        self.__storage_cluster_selector = storage_cluster_selector
        self.__mappers = mappers if mappers is not None else TranslationMappers()
        self.__post_processors = post_processors or []

    @with_span()
    def build_and_rank_plans(
        self, query: LogicalQuery, settings: QuerySettings
    ) -> Sequence[ClickhouseQueryPlan]:
        with sentry_sdk.start_span(
            op="build_plan.sliced_storage", description="select_storage"
        ):
            cluster = self.__storage_cluster_selector.select_cluster(query, settings)

        with sentry_sdk.start_span(
            op="build_plan.sliced_storage", description="translate"
        ):
            # The QueryTranslator class should be instantiated once for each call to build_plan,
            # to avoid cache conflicts.
            clickhouse_query = QueryTranslator(self.__mappers).translate(query)

        with sentry_sdk.start_span(
            op="build_plan.sliced_storage", description="set_from_clause"
        ):
            clickhouse_query.set_from_clause(
                get_query_data_source(
                    self.__storage.get_schema().get_data_source(),
                    final=query.get_final(),
                    sampling_rate=query.get_sample(),
                )
            )

        db_query_processors = [
            *self.__storage.get_query_processors(),
            *self.__post_processors,
            MandatoryConditionApplier(),
            MandatoryConditionEnforcer(
                self.__storage.get_mandatory_condition_checkers()
            ),
        ]

        return [
            ClickhouseQueryPlan(
                query=clickhouse_query,
                plan_query_processors=[],
                db_query_processors=db_query_processors,
                storage_set_key=self.__storage.get_storage_set_key(),
                execution_strategy=SimpleQueryPlanExecutionStrategy(
                    cluster=cluster,
                    db_query_processors=db_query_processors,
                    splitters=self.__storage.get_query_splitters(),
                ),
            )
        ]
