from __future__ import annotations

from typing import Any, Iterator

REGISTERED_STORAGE_KEYS: dict[str, str] = {}


class _StorageKey(type):
    def __getattr__(cls, attr: str) -> "StorageKey":
        if attr not in REGISTERED_STORAGE_KEYS:
            raise AttributeError(attr)

        return StorageKey(attr.lower())

    def __iter__(cls) -> Iterator[StorageKey]:
        return iter(StorageKey(value) for value in REGISTERED_STORAGE_KEYS.values())


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
