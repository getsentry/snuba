"""
The number of logical partitions used to distinguish between where records
should be stored. These do not require individual physical partitions but allow
for repartitioning with less code changes per physical change.
"""
from snuba.clusters.storage_sets import StorageSetKey

SENTRY_LOGICAL_PARTITIONS = 256


def map_org_id_to_logical_partition(org_id: int) -> int:
    """
    Maps an org_id to a logical partition. Since SENTRY_LOGICAL_PARTITIONS is
    fixed, an org id will always be mapped to the same logical partition.
    """
    return org_id % SENTRY_LOGICAL_PARTITIONS


def map_logical_partition_to_physical_partition(logical_partition: int) -> int:
    """
    Maps a logical partition to a physical partition.
    """
    from snuba.settings import LOGICAL_PARTITION_MAPPING

    return LOGICAL_PARTITION_MAPPING[str(logical_partition)]


def is_storage_set_partitioned(storage_set: StorageSetKey) -> bool:
    """
    Returns whether the storage set is partitioned.
    """
    from snuba.settings import PARTITIONED_STORAGES

    return True if storage_set.value in PARTITIONED_STORAGES else False
