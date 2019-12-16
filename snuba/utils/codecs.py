from abc import ABC, abstractmethod
from typing import Generic, TypeVar


TEncoded = TypeVar("TEncoded")

TDecoded = TypeVar("TDecoded")


class Codec(Generic[TEncoded, TDecoded], ABC):
    @abstractmethod
    def encode(self, value: TDecoded) -> TEncoded:
        raise NotImplementedError

    @abstractmethod
    def decode(self, value: TEncoded) -> TDecoded:
        raise NotImplementedError
