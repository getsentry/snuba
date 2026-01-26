from abc import ABC, abstractmethod

from snuba import state
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clickhouse.query_dsl.accessors import get_object_ids_in_query_ast
from snuba.clusters.cluster import ClickhouseCluster, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.slicing import (
    is_storage_set_sliced,
    map_logical_partition_to_slice,
    map_org_id_to_logical_partition,
)
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.logical import Query as LogicalQuery
from snuba.query.query_settings import QuerySettings

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


def _should_use_mega_cluster(storage_set: StorageSetKey, logical_partition: int) -> bool:
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
        self, query: LogicalQuery | ClickhouseQuery, query_settings: QuerySettings
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
            slice_id = map_logical_partition_to_slice(self.storage_set, logical_partition)
            cluster = get_cluster(self.storage_set, slice_id)
            return cluster
