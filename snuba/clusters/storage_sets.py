from __future__ import annotations

from typing import Any, FrozenSet, Iterator

_HARDCODED_STORAGE_SET_KEYS = {
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
    "PROFILES": "profiles",
    "FUNCTIONS": "functions",
    "SEARCH_ISSUES": "search_issues",
    "SPANS": "spans",
    "GROUP_ATTRIBUTES": "group_attributes",
    "METRICS_SUMMARIES": "metrics_summaries",
}


_REGISTERED_STORAGE_SET_KEYS: dict[str, str] = {}


class _StorageSetKey(type):
    def __getattr__(self, attr: str) -> "StorageSetKey":
        if (
            attr not in _HARDCODED_STORAGE_SET_KEYS
            and attr not in _REGISTERED_STORAGE_SET_KEYS
        ):
            raise AttributeError(attr)

        return StorageSetKey(attr.lower())

    def __iter__(self) -> Iterator[StorageSetKey]:
        return iter(
            StorageSetKey(value)
            for value in {
                **_HARDCODED_STORAGE_SET_KEYS,
                **_REGISTERED_STORAGE_SET_KEYS,
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
    _REGISTERED_STORAGE_SET_KEYS[key.upper()] = key.lower()
    return StorageSetKey(key)


# Storage sets enabled only when development features are enabled.
DEV_STORAGE_SETS: FrozenSet[StorageSetKey] = frozenset(StorageSetKey.METRICS_SUMMARIES)

# Storage sets in a group share the same query and distributed nodes but
# do not have the same local node cluster configuration.
# Joins can be performed across storage sets in the same group.
JOINABLE_STORAGE_SETS: FrozenSet[FrozenSet[StorageSetKey]] = frozenset(
    {
        frozenset({StorageSetKey.EVENTS, StorageSetKey.EVENTS_RO, StorageSetKey.CDC}),
        frozenset(
            {
                StorageSetKey.EVENTS,
                StorageSetKey.EVENTS_RO,
                StorageSetKey.GROUP_ATTRIBUTES,
            }
        ),
        frozenset({StorageSetKey.SEARCH_ISSUES, StorageSetKey.GROUP_ATTRIBUTES}),
    }
)


def is_valid_storage_set_combination(*storage_sets: StorageSetKey) -> bool:
    all_storage_sets = set(storage_sets)

    if len(all_storage_sets) <= 1:
        return True

    for group in JOINABLE_STORAGE_SETS:
        if all_storage_sets.issubset(group):
            return True

    return False
