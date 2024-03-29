from __future__ import annotations

from typing import Any, Iterator

REGISTERED_ENTITY_KEYS: dict[str, str] = {}


class _EntityKey(type):
    def __getattr__(cls, attr: str) -> "EntityKey":
        if attr not in REGISTERED_ENTITY_KEYS:
            raise AttributeError(attr)

        return EntityKey(attr.lower())

    def __iter__(cls) -> Iterator[EntityKey]:
        return iter(EntityKey(value) for value in REGISTERED_ENTITY_KEYS.values())


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
    REGISTERED_ENTITY_KEYS[key.upper()] = key.lower()
    return EntityKey(key)
