from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, TypeVar, cast

from arroyo import Message
from arroyo.backends.kafka import KafkaPayload

from snuba.utils.registered_class import RegisteredClass

TPayload = TypeVar("TPayload")

KAFKA_ONLY_PARTITION = 0  # CDC only works with single partition topics. So partition must be 0


class StreamMessageFilter(ABC, Generic[TPayload], metaclass=RegisteredClass):
    """
    A filter over messages coming from a stream. Can be used to pre filter
    messages during consumption but potentially for other use cases as well.
    """

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> "StreamMessageFilter[TPayload]":
        return cast("StreamMessageFilter[TPayload]", cls.class_from_name(name))

    @classmethod
    def from_kwargs(cls, **kwargs: str) -> "StreamMessageFilter[TPayload]":
        return cls(**kwargs)

    @abstractmethod
    def should_drop(self, message: Message[TPayload]) -> bool:
        raise NotImplementedError


@dataclass
class KafkaHeaderSelectFilter(StreamMessageFilter[KafkaPayload]):
    """
    A filter over messages coming from a stream which matches whether the given message
    has the provided header key and if it matches the provided header value. If there is
    a match, the message gets processed.
    """

    header_key: str
    header_value: str

    def should_drop(self, message: Message[KafkaPayload]) -> bool:
        for key, value in message.payload.headers:
            if key != self.header_key:
                continue

            str_value = value.decode("utf-8")
            return str_value != self.header_value

        return True


class CdcTableNameMessageFilter(StreamMessageFilter[KafkaPayload]):
    """
    Removes all messages from the stream that are not change events (insert,
    update, delete) on the specified table. (This also removes transactional
    events, such as begin and commit events, as they are not directed at a
    specific table.) This filtering utilizes the table header and does not
    require parsing the payload value.
    """

    def __init__(self, postgres_table: str) -> None:
        self.__postgres_table = postgres_table

    def should_drop(self, message: Message[KafkaPayload]) -> bool:
        assert [p.index for p in message.committable.keys()] == [KAFKA_ONLY_PARTITION], (
            "CDC can only work with single partition topics for consistency"
        )

        table_name = next((value for key, value in message.payload.headers if key == "table"), None)

        return not table_name or table_name.decode("utf-8") != self.__postgres_table
