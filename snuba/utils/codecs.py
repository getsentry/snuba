from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

import rapidjson


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


T = TypeVar("T")


class PassthroughCodec(Generic[T], Codec[T, T]):
    def encode(self, value: T) -> T:
        return value

    def decode(self, value: T) -> T:
        return value


JSONData = Any


class JSONCodec(Codec[bytes, JSONData]):
    def encode(self, value: JSONData) -> bytes:
        return rapidjson.dumps(value)  # type: ignore

    def decode(self, value: bytes) -> JSONData:
        return rapidjson.loads(value)
