from enum import Enum


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
