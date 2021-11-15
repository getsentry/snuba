from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from arroyo import Message
from arroyo.backends.kafka import KafkaPayload

TPayload = TypeVar("TPayload")


class StreamMessageFilter(ABC, Generic[TPayload]):
    """
    A filter over messages coming from a stream. Can be used to pre filter
    messages during consumption but potentially for other use cases as well.
    """

    @abstractmethod
    def should_drop(self, message: Message[TPayload]) -> bool:
        raise NotImplementedError


class PassthroughKafkaFilter(StreamMessageFilter[KafkaPayload]):
    def should_drop(self, message: Message[KafkaPayload]) -> bool:
        return False


class KafkaHeaderFilter(StreamMessageFilter[KafkaPayload]):
    def __init__(self, header_key: str, header_value: str):
        self.__header_key = header_key
        self.__header_value = header_value

    def should_drop(self, message: Message[KafkaPayload]) -> bool:
        for key, value in message.payload.headers:
            if key != self.__header_key:
                continue

            str_value = value.decode("utf-8")
            return True if str_value == self.__header_value else False

        return False
