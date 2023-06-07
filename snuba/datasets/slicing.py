"""
The number of logical partitions used to distinguish between where records
should be stored. These do not require individual physical partitions but allow
for repartitioning with less code changes per physical change.
"""
from typing import Optional

from snuba.clusters.storage_sets import StorageSetKey

SENTRY_LOGICAL_PARTITIONS = 256


def map_org_id_to_logical_partition(org_id: int) -> int:
    """
    Maps an org id to a logical partition. Since SENTRY_LOGICAL_PARTITIONS is
    fixed, an org id will always be mapped to the same logical partition.
    """
    return org_id % SENTRY_LOGICAL_PARTITIONS


def map_logical_partition_to_slice(
    storage_set: StorageSetKey, logical_partition: int
) -> int:
    """
    Maps a logical partition to a slice.
    """
    from snuba.settings import LOGICAL_PARTITION_MAPPING

    assert is_storage_set_sliced(
        storage_set
    ), f"cannot retrieve slice of non-sliced storage set {storage_set}"
    assert (
        storage_set.value in LOGICAL_PARTITION_MAPPING
    ), f"logical partition mapping missing for storage set {storage_set}"

    return LOGICAL_PARTITION_MAPPING[storage_set.value][logical_partition]


def is_storage_set_sliced(storage_set: StorageSetKey) -> bool:
    """
    Returns whether the storage set is sliced.
    """
    from snuba.settings import SLICED_STORAGE_SETS

    return True if storage_set.value in SLICED_STORAGE_SETS.keys() else False
