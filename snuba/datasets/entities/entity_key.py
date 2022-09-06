from __future__ import annotations

from typing import Any, Iterator

HARDCODED_ENTITY_KEYS = {
    "DISCOVER": "discover",
    "EVENTS": "events",
    "GROUPS": "groups",
    "GROUPASSIGNEE": "groupassignee",
    "GROUPEDMESSAGE": "groupedmessage",
    "METRICS_SETS": "metrics_sets",
    "METRICS_COUNTERS": "metrics_counters",
    "ORG_METRICS_COUNTERS": "org_metrics_counters",
    "METRICS_DISTRIBUTIONS": "metrics_distributions",
    "OUTCOMES": "outcomes",
    "OUTCOMES_RAW": "outcomes_raw",
    "SESSIONS": "sessions",
    "ORG_SESSIONS": "org_sessions",
    "TRANSACTIONS": "transactions",
    "DISCOVER_TRANSACTIONS": "discover_transactions",
    "DISCOVER_EVENTS": "discover_events",
    "PROFILES": "profiles",
    "FUNCTIONS": "functions",
    "REPLAYS": "replays",
}

REGISTERED_ENTITY_KEYS: dict[str, str] = {}


class _EntityKey(type):
    def __getattr__(cls, attr: str) -> "EntityKey":
        if attr not in HARDCODED_ENTITY_KEYS and attr not in REGISTERED_ENTITY_KEYS:
            raise AttributeError(attr)

        return EntityKey(attr.lower())

    def __iter__(cls) -> Iterator[EntityKey]:
        return iter(
            EntityKey(key)
            for key in {**HARDCODED_ENTITY_KEYS, **REGISTERED_ENTITY_KEYS}
        )


class EntityKey(metaclass=_EntityKey):
    def __init__(self, value: str) -> None:
        self.value = value

    def __hash__(self) -> int:
        return hash(self.value)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, EntityKey) and other.value == self.value

    def __repr__(self) -> str:
        return f"EntityKey.{self.value.upper()}"


def register_entity_key(key: str) -> EntityKey:
    print(f"registering {key}")
    REGISTERED_ENTITY_KEYS[key.upper()] = key.lower()
    return EntityKey(key)
