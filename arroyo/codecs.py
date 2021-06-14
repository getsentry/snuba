from abc import ABC, abstractmethod
from typing import Generic, TypeVar

TEncoded = TypeVar("TEncoded")

TDecoded = TypeVar("TDecoded")


class Encoder(Generic[TEncoded, TDecoded], ABC):
    @abstractmethod
    def encode(self, value: TDecoded) -> TEncoded:
        raise NotImplementedError


class Decoder(Generic[TEncoded, TDecoded], ABC):
    @abstractmethod
    def decode(self, value: TEncoded) -> TDecoded:
        raise NotImplementedError


class Codec(
    Encoder[TEncoded, TDecoded], Decoder[TEncoded, TDecoded],
):
    pass
