"""
The number of logical partitions used to distinguish between where records
should be stored. These do not require individual physical partitions but allow
for repartitioning with less code changes per physical change.
"""
from typing import Optional

from snuba.datasets.storages.storage_key import StorageKey

SENTRY_LOGICAL_PARTITIONS = 256


def map_org_id_to_logical_partition(partition_key: int) -> int:
    """
    Maps a partition key to a logical partition. Since SENTRY_LOGICAL_PARTITIONS is
    fixed, an org id will always be mapped to the same logical partition.
    """
    return partition_key % SENTRY_LOGICAL_PARTITIONS


def map_logical_partition_to_slice(storage: StorageKey, logical_partition: int) -> int:
    """
    Maps a logical partition to a slice.
    """
    from snuba.settings import LOGICAL_PARTITION_MAPPING

    assert is_storage_sliced(
        storage
    ), f"cannot retrieve slice of non-sliced storage {storage}"
    assert (
        storage.value in LOGICAL_PARTITION_MAPPING
    ), f"logical partition mapping missing for {storage}"

    return LOGICAL_PARTITION_MAPPING[storage.value][logical_partition]


def is_storage_sliced(storage: StorageKey) -> bool:
    """
    Returns whether the storage set is sliced.
    """
    from snuba.settings import SLICED_STORAGES

    return True if storage.value in SLICED_STORAGES.keys() else False


def validate_passed_slice(storage_key: StorageKey, slice_id: Optional[int]) -> None:
    """
    Verifies that the given storage can be sliced
    and that the slice_id passed in is within the range
    of the total number of slices for the given storage
    """
    from snuba.settings import SLICED_STORAGES

    if slice_id is not None:
        assert storage_key.value in SLICED_STORAGES
        assert slice_id < SLICED_STORAGES[storage_key.value]
