from typing import Generator, Generic, TypeVar

T = TypeVar("T")
KeyType = TypeVar("KeyType")


class ConfigComponentFactory(Generic[T, KeyType]):
    def iter_all(self) -> Generator[T, None, None]:
        raise NotImplementedError

    def get(self, name: KeyType) -> T:
        raise NotImplementedError
