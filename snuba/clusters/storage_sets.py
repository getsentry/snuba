from typing import Mapping, Set

from enum import Enum
from snuba.datasets.storages import StorageKey


class StorageSetKey(Enum):
    EVENTS = "events"
    GROUPASSIGNEES = "groupassignees"
    OUTCOMES = "outcomes"
    QUERYLOG = "querylog"
    SESSIONS = "sessions"
    TRANSACTIONS = "transactions"


STORAGE_SETS: Mapping[StorageSetKey, Set[StorageKey]] = {
    StorageSetKey.EVENTS: {
        StorageKey.ERRORS,
        StorageKey.EVENTS,
        StorageKey.GROUPEDMESSAGES,
        # TODO: Remove once groups are no longer storages
        StorageKey.GROUPS,
    },
    # Should group assignee be on the events cluster?
    StorageSetKey.GROUPASSIGNEES: {StorageKey.GROUPASSIGNEES},
    StorageSetKey.OUTCOMES: {StorageKey.OUTCOMES_RAW, StorageKey.OUTCOMES_HOURLY},
    StorageSetKey.QUERYLOG: {StorageKey.QUERYLOG},
    StorageSetKey.SESSIONS: {StorageKey.SESSIONS_RAW, StorageKey.SESSIONS_HOURLY},
    StorageSetKey.TRANSACTIONS: {StorageKey.TRANSACTIONS},
}
