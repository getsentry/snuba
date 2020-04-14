from typing import Mapping, Set

from enum import Enum
from snuba.datasets.storages import StorageKey


class StorageSet(Enum):
    EVENTS = "events"
    GROUPASSIGNEES = "groupassignees"
    OUTCOMES = "outcomes"
    QUERYLOG = "querylog"
    SESSIONS = "sessions"
    TRANSACTIONS = "transactions"


STORAGE_SETS: Mapping[StorageSet, Set[StorageKey]] = {
    StorageSet.EVENTS: {StorageKey.ERRORS, StorageKey.EVENTS, StorageKey.GROUPEDMESSAGES},
    # Should group assignee be on the events cluster?
    StorageSet.GROUPASSIGNEES: {StorageKey.GROUPASSIGNEES},
    StorageSet.OUTCOMES: {StorageKey.OUTCOMES_RAW, StorageKey.OUTCOMES_HOURLY},
    StorageSet.QUERYLOG: {StorageKey.QUERYLOG},
    StorageSet.SESSIONS: {StorageKey.SESSIONS_RAW, StorageKey.SESSIONS_HOURLY},
    StorageSet.TRANSACTIONS: {StorageKey.TRANSACTIONS},
}
