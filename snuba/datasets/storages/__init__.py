from enum import Enum
from functools import lru_cache


class StorageKey(Enum):
    """
    A storage key is a unique identifier for a storage.
    """

    DISCOVER = "discover"
    EVENTS = "events"
    EVENTS_RO = "events_ro"
    ERRORS = "errors"
    ERRORS_RO = "errors_ro"
    GROUPEDMESSAGES = "groupedmessages"
    GROUPASSIGNEES = "groupassignees"
    METRICS_BUCKETS = "metrics_buckets"
    METRICS_COUNTERS_BUCKETS = "metrics_counters_buckets"
    METRICS_COUNTERS = "metrics_counters"
    METRICS_DISTRIBUTIONS = "metrics_distributions"
    METRICS_DISTRIBUTIONS_BUCKETS = "metrics_distributions_buckets"
    METRICS_SETS = "metrics_sets"
    OUTCOMES_RAW = "outcomes_raw"
    OUTCOMES_HOURLY = "outcomes_hourly"
    QUERYLOG = "querylog"
    SESSIONS_RAW = "sessions_raw"
    SESSIONS_HOURLY = "sessions_hourly"
    ORG_SESSIONS = "org_sessions"
    SPANS = "spans"
    TRANSACTIONS = "transactions"
    TRANSACTIONS_RO = "transactions_ro"
    TRANSACTIONS_V2 = "transactions_v2"
    ERRORS_V2 = "errors_v2"
    ERRORS_V2_RO = "errors_v2_ro"


IDENTICAL_STORAGES = frozenset(
    {
        frozenset({StorageKey.TRANSACTIONS, StorageKey.TRANSACTIONS_V2}),
        frozenset({StorageKey.ERRORS, StorageKey.ERRORS_V2}),
    }
)


@lru_cache(20)
def are_writes_identical(this: StorageKey, other: StorageKey) -> bool:
    for group in IDENTICAL_STORAGES:
        if {this, other}.issubset(group):
            return True

    return False
