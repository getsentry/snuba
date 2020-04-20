from typing import Mapping

from enum import Enum
from snuba.datasets.storages import StorageKey


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
    EVENTS = "events"
    OUTCOMES = "outcomes"
    QUERYLOG = "querylog"
    SESSIONS = "sessions"
    TRANSACTIONS = "transactions"


STORAGE_SETS: Mapping[StorageKey, StorageSetKey] = {
    # Events
    StorageKey.EVENTS: StorageSetKey.EVENTS,
    StorageKey.ERRORS: StorageSetKey.EVENTS,
    StorageKey.GROUPEDMESSAGES: StorageSetKey.EVENTS,
    StorageKey.GROUPASSIGNEES: StorageSetKey.EVENTS,
    # TODO: Remove once groups are no longer storages
    StorageKey.GROUPS: StorageSetKey.EVENTS,
    # Querylog
    StorageKey.QUERYLOG: StorageSetKey.QUERYLOG,
    # Outcomes
    StorageKey.OUTCOMES_RAW: StorageSetKey.OUTCOMES,
    StorageKey.OUTCOMES_HOURLY: StorageSetKey.OUTCOMES,
    # Sessions
    StorageKey.SESSIONS_RAW: StorageSetKey.SESSIONS,
    StorageKey.SESSIONS_HOURLY: StorageSetKey.SESSIONS,
    # Transactions
    StorageKey.TRANSACTIONS: StorageSetKey.TRANSACTIONS,
}
