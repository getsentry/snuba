from enum import Enum


class EntityKey(Enum):
    """
    A entity key is a unique identifier for an entity.
    """

    DISCOVER = "discover"
    ERRORS = "errors"
    EVENTS = "events"
    GROUPS = "groups"
    GROUPASSIGNEE = "groupassignee"
    # TODO: This has an S on the end in solidarity with storages, but it's got to go
    GROUPEDMESSAGES = "groupedmessage"
    METRICS_SETS = "metrics_sets"
    METRICS_COUNTERS = "metrics_counters"
    OUTCOMES = "outcomes"
    OUTCOMES_RAW = "outcomes_raw"
    SESSIONS = "sessions"
    SPANS = "spans"
    TRANSACTIONS = "transactions"
    DISCOVER_TRANSACTIONS = "discover_transactions"
    DISCOVER_EVENTS = "discover_events"
