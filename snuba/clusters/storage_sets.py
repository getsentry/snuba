from typing import FrozenSet

from snuba.clusters.storage_set_key import StorageSetKey

# Storage sets enabled only when development features are enabled.
DEV_STORAGE_SETS: FrozenSet[StorageSetKey] = frozenset()

# Storage sets in a group share the same query and distributed nodes but
# do not have the same local node cluster configuration.
# Joins can be performed across storage sets in the same group.
JOINABLE_STORAGE_SETS: FrozenSet[FrozenSet[StorageSetKey]] = frozenset(
    {
        frozenset({StorageSetKey.EVENTS, StorageSetKey.EVENTS_RO, StorageSetKey.CDC}),
        frozenset({StorageSetKey.ERRORS_V2, StorageSetKey.ERRORS_V2_RO}),
    }
)


def is_valid_storage_set_combination(*storage_sets: StorageSetKey) -> bool:
    all_storage_sets = set(storage_sets)

    if len(all_storage_sets) <= 1:
        return True

    for group in JOINABLE_STORAGE_SETS:
        if all_storage_sets.issubset(group):
            return True

    return False
