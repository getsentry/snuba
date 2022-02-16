from enum import Enum
from typing import FrozenSet


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
    TRANSACTIONS_RO = "transactions_ro"
    TRANSACTIONS_V2 = "transactions_v2"


# Storage sets enabled only when development features are enabled.
DEV_STORAGE_SETS: FrozenSet[StorageSetKey] = frozenset({})

# Storage sets in a group share the same query and distributed nodes but
# do not have the same local node cluster configuration.
# Joins can be performed across storage sets in the same group.
JOINABLE_STORAGE_SETS: FrozenSet[FrozenSet[StorageSetKey]] = frozenset(
    {frozenset({StorageSetKey.EVENTS, StorageSetKey.EVENTS_RO, StorageSetKey.CDC})}
)


def is_valid_storage_set_combination(*storage_sets: StorageSetKey) -> bool:
    all_storage_sets = set(storage_sets)

    if len(all_storage_sets) <= 1:
        return True

    for group in JOINABLE_STORAGE_SETS:
        if all_storage_sets.issubset(group):
            return True

    return False
