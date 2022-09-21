"""
The number of logical partitions used to distinguish between where records
should be stored. These do not require individual physical partitions but allow
for repartitioning with less code changes per physical change.
"""
from snuba.clusters.storage_sets import StorageSetKey
from snuba.settings import LOGICAL_PARTITION_MAPPING

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
    return LOGICAL_PARTITION_MAPPING[str(logical_partition)]


def map_physical_partition_to_storage_set(
    base_storage_set: StorageSetKey, physical_partition: int
) -> StorageSetKey:
    """
    Maps a physical partition to a specific storage set. Each storage
    set would be suffixed with the physical partition it is mapped to.

    If the physical partition is 0, then the storage set suffix is empty
    which means that use the storage set without any suffix.

    For non-zero physical partitions, the storage set suffix is
    """
    if physical_partition == 0:
        return base_storage_set
    else:
        return StorageSetKey(f"{base_storage_set.value}_{physical_partition}")
