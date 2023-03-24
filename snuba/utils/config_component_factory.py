from typing import Generic, TypeVar

T = TypeVar("T")
KeyType = TypeVar("KeyType")


class ConfigComponentFactory(Generic[T, KeyType]):
    def get(self, name: KeyType) -> T:
        raise NotImplementedError
