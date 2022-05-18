from enum import Enum


class EntityKey(Enum):
    """
    A entity key is a unique identifier for an entity.
    """

    DISCOVER = "discover"
    EVENTS = "events"
    GROUPS = "groups"
    GROUPASSIGNEE = "groupassignee"
    # TODO: This has an S on the end in solidarity with storages, but it's got to go
    GROUPEDMESSAGES = "groupedmessage"
    METRICS_SETS = "metrics_sets"
    METRICS_COUNTERS = "metrics_counters"
    ORG_METRICS_COUNTERS = "org_metrics_counters"
    METRICS_DISTRIBUTIONS = "metrics_distributions"
    OUTCOMES = "outcomes"
    OUTCOMES_RAW = "outcomes_raw"
    SESSIONS = "sessions"
    ORG_SESSIONS = "org_sessions"
    TRANSACTIONS = "transactions"
    DISCOVER_TRANSACTIONS = "discover_transactions"
    DISCOVER_EVENTS = "discover_events"
    PROFILES = "profiles"
    FUNCTIONS = "functions"
