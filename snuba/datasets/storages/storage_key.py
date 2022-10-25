from __future__ import annotations

from functools import lru_cache
from typing import Any, Iterator

HARDCODED_STORAGE_KEYS = {
    "DISCOVER": "discover",
    "EVENTS": "events",
    "EVENTS_RO": "events_ro",
    "ERRORS": "errors",
    "ERRORS_RO": "errors_ro",
    "GROUPEDMESSAGES": "groupedmessages",
    "GROUPASSIGNEES": "groupassignees",
    "METRICS_COUNTERS": "metrics_counters",
    "ORG_METRICS_COUNTERS": "org_metrics_counters",
    "METRICS_DISTRIBUTIONS": "metrics_distributions",
    "METRICS_SETS": "metrics_sets",
    "METRICS_RAW": "metrics_raw",
    "OUTCOMES_RAW": "outcomes_raw",
    "OUTCOMES_HOURLY": "outcomes_hourly",
    "QUERYLOG": "querylog",
    "SESSIONS_RAW": "sessions_raw",
    "SESSIONS_HOURLY": "sessions_hourly",
    "ORG_SESSIONS": "org_sessions",
    "SPANS": "spans",
    "TRANSACTIONS": "transactions",
    "PROFILES": "profiles",
    "FUNCTIONS": "functions",
    "FUNCTIONS_RAW": "functions_raw",
    "REPLAYS": "replays",
}

REGISTERED_STORAGE_KEYS: dict[str, str] = {}


class _StorageKey(type):
    def __getattr__(cls, attr: str) -> "StorageKey":
        if attr not in HARDCODED_STORAGE_KEYS and attr not in REGISTERED_STORAGE_KEYS:
            raise AttributeError(attr)

        return StorageKey(attr.lower())

    def __iter__(cls) -> Iterator[StorageKey]:
        return iter(
            StorageKey(value)
            for value in {**HARDCODED_STORAGE_KEYS, **REGISTERED_STORAGE_KEYS}.values()
        )


class StorageKey(metaclass=_StorageKey):
    def __init__(self, value: str) -> None:
        self.value = value

    def __hash__(self) -> int:
        return hash(self.value)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, StorageKey) and other.value == self.value

    def __repr__(self) -> str:
        return f"StorageKey.{self.value.upper()}"


def register_storage_key(key: str) -> StorageKey:
    REGISTERED_STORAGE_KEYS[key.upper()] = key.lower()
    return StorageKey(key)


IDENTICAL_STORAGES = frozenset()


@lru_cache(20)
def are_writes_identical(this: StorageKey, other: StorageKey) -> bool:
    for group in IDENTICAL_STORAGES:
        if {this, other}.issubset(group):
            return True

    return False
