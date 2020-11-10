from enum import Enum


class StorageKey(Enum):
    """
    A storage key is a unique identifier for a storage.
    """

    EVENTS = "events"
    EVENTS_RO = "events_ro"
    ERRORS = "errors"
    ERRORS_RO = "errors_ro"
    GROUPEDMESSAGES = "groupedmessages"
    GROUPASSIGNEES = "groupassignees"
    OUTCOMES_RAW = "outcomes_raw"
    OUTCOMES_HOURLY = "outcomes_hourly"
    QUERYLOG = "querylog"
    SESSIONS_RAW = "sessions_raw"
    SESSIONS_HOURLY = "sessions_hourly"
    SPANS = "spans"
    TRANSACTIONS = "transactions"
