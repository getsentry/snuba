from enum import Enum
from typing import FrozenSet, Tuple


class StorageSetKey(Enum):
    """
    A storage set key is a unique identifier for a storage set.

    A storage set represents a collection of storages that must be physically located
    on the same cluster.

    Storages in the same storage sets are:
    - Storages that join queries are performed on
    - Raw and materialized views (potentially moving into the same storage in future)

    Storage sets are assigned to clusters via configuration.
    """

    CDC = "cdc"
    DISCOVER = "discover"
    EVENTS = "events"
    EVENTS_RO = "events_ro"
    METRICS = "metrics"
    MIGRATIONS = "migrations"
    OUTCOMES = "outcomes"
    QUERYLOG = "querylog"
    SESSIONS = "sessions"
    TRANSACTIONS = "transactions"


# Storage sets enabled only when development features are enabled.
DEV_STORAGE_SETS: FrozenSet[StorageSetKey] = frozenset({StorageSetKey.METRICS})

# Storage sets in a group share the same query and distributed nodes but
# do not have the same local node cluster configuration.
# Joins can be performed across storage sets in the same group.
STORAGE_SET_GROUPS: FrozenSet[Tuple[StorageSetKey, ...]] = frozenset(
    {(StorageSetKey.EVENTS, StorageSetKey.EVENTS_RO, StorageSetKey.CDC)}
)


def is_valid_storage_set_combination(*storage_sets: StorageSetKey) -> bool:
    first = storage_sets[0]

    for storage_set in storage_sets[1:]:
        if storage_set == first:
            continue

        for group in STORAGE_SET_GROUPS:
            if first in group and storage_set in group:
                continue
            return False

    return True
