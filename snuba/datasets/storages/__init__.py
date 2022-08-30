from functools import lru_cache
from typing import Any

from snuba.datasets.configuration.loader import load_config_built_storage_keys

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
    "TRANSACTIONS_RO": "transactions_ro",
    "TRANSACTIONS_V2": "transactions_v2",
    "ERRORS_V2": "errors_v2",
    "PROFILES": "profiles",
    "FUNCTIONS": "functions",
    "FUNCTIONS_RAW": "functions_raw",
    "REPLAYS": "replays",
    "ERRORS_V2_RO": "errors_v2_ro",
}

CONFIG_BUILT_STORAGE_KEYS = load_config_built_storage_keys()

ALL_STORAGE_KEYS = {**HARDCODED_STORAGE_KEYS, **CONFIG_BUILT_STORAGE_KEYS}


class _StorageKey(type):
    def __getattr__(cls, attr: str) -> "StorageKey":
        if attr not in ALL_STORAGE_KEYS:
            raise AttributeError(attr)

        return StorageKey(attr.lower())


class StorageKey(metaclass=_StorageKey):
    def __init__(self, value: str) -> None:
        self.value = value

    def __hash__(self) -> int:
        return hash(self.value)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, StorageKey) and other.value == self.value

    def __repr__(self) -> str:
        return f"StorageKey.{self.value.upper()}"


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
