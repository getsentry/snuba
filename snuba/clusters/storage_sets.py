from enum import Enum


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

    DISCOVER = "discover"
    EVENTS = "events"
    EVENTS_RO = "events_ro"
    MIGRATIONS = "migrations"
    OUTCOMES = "outcomes"
    QUERYLOG = "querylog"
    SESSIONS = "sessions"
    TRANSACTIONS = "transactions"
