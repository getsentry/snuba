from __future__ import annotations

from typing import Any, Dict, Iterator

HARDCODED_STORAGE_SET_KEYS = {
    "CDC": "cdc",
    "DISCOVER": "discover",
    "EVENTS": "events",
    "EVENTS_RO": "events_ro",
    "METRICS": "metrics",
    "MIGRATIONS": "migrations",
    "OUTCOMES": "outcomes",
    "QUERYLOG": "querylog",
    "SESSIONS": "sessions",
    "TRANSACTIONS": "transactions",
    "TRANSACTIONS_RO": "transactions_ro",
    "TRANSACTIONS_V2": "transactions_v2",
    "ERRORS_V2": "errors_v2",
    "ERRORS_V2_RO": "errors_v2_ro",
    "PROFILES": "profiles",
    "FUNCTIONS": "functions",
    "REPLAYS": "replays",
    "GENERIC_METRICS_SETS": "generic_metrics_sets",
    "GENERIC_METRICS_DISTRIBUTIONS": "generic_metrics_distributions",
}


REGISTERED_STORAGE_SET_KEYS: Dict[str, str] = {}


class _StorageSetKey(type):
    def __getattr__(self, attr: str) -> "StorageSetKey":
        if (
            attr not in HARDCODED_STORAGE_SET_KEYS
            and attr not in REGISTERED_STORAGE_SET_KEYS
        ):
            raise AttributeError(attr)

        return StorageSetKey(attr.lower())

    def __iter__(self) -> Iterator[StorageSetKey]:
        return iter(
            StorageSetKey(value)
            for value in {
                **HARDCODED_STORAGE_SET_KEYS,
                **REGISTERED_STORAGE_SET_KEYS,
            }.values()
        )


class StorageSetKey(metaclass=_StorageSetKey):
    """
    A storage set key is a unique identifier for a storage set.

    A storage set represents a collection of storages that must be physically located
    on the same cluster.

    Storages in the same storage sets are:
    - Storages that join queries are performed on
    - Raw and materialized views (potentially moving into the same storage in future)

    Storage sets are assigned to clusters via configuration.
    """

    def __init__(self, value: str):
        self.value = value

    def __hash__(self) -> int:
        return hash(self.value)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, StorageSetKey) and other.value == self.value

    def __repr__(self) -> str:
        return f"StorageSetKey.{self.value.upper()}"


def register_storage_set_key(key: str) -> StorageSetKey:
    REGISTERED_STORAGE_SET_KEYS[key.upper()] = key.lower()
    return StorageSetKey(key)
