from typing import Mapping

from enum import Enum
from snuba.datasets.storages import StorageKey


class StorageSetKey(Enum):
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
