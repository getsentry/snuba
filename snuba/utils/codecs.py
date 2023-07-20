from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from snuba.utils.serializable_exception import SerializableException

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
    Encoder[TEncoded, TDecoded],
    Decoder[TEncoded, TDecoded],
):
    pass


class ExceptionAwareCodec(Codec[TEncoded, TDecoded]):
    @abstractmethod
    def encode_exception(self, value: SerializableException) -> TEncoded:
        raise NotImplementedError
