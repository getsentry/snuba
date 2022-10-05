from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, TypeVar, cast

from arroyo import Message
from arroyo.backends.kafka import KafkaPayload

from snuba.utils.registered_class import RegisteredClass

TPayload = TypeVar("TPayload")

KAFKA_ONLY_PARTITION = (
    0  # CDC only works with single partition topics. So partition must be 0
)


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


class PassthroughKafkaFilter(StreamMessageFilter[KafkaPayload]):
    def should_drop(self, message: Message[KafkaPayload]) -> bool:
        return False


@dataclass
class KafkaHeaderFilter(StreamMessageFilter[KafkaPayload]):
    """
    A filter over messages coming from a stream which matches whether the given message
    has the provided header key and if it matches the provided header value. If there is
    a match, the message gets dropped.
    """

    header_key: str
    header_value: str

    def should_drop(self, message: Message[KafkaPayload]) -> bool:
        for key, value in message.payload.headers:
            if key != self.header_key:
                continue

            str_value = value.decode("utf-8")
            return True if str_value == self.header_value else False

        return False


@dataclass
class KafkaHeaderSelectFilter(StreamMessageFilter[KafkaPayload]):
    """
    A filter over messages coming from a stream which matches whether the given message
    has the provided header key and if it matches the provided header value. If there is
    a match, the message gets processed. (This is the inverse of KafkaHeaderFilter)
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


@dataclass
class KafkaHeaderWithBypassFilter(KafkaHeaderFilter):
    """
    A special case filter which is similar to KafkaHeaderFilter but allows a message to
    which would have been dropped to pass through after reaching a consecutive limit.
    The reason to use this has to do with the way Kafka works. Kafka works with offsets
    of messages which need to be committed to the broker to acknowledge them.
    In cases where there is a shared topic, and only one type of messages on the topic,
    if we drop all messages then that consumer would never commit offsets to the broker
    which would result in increase in consumer group lag of the consumer group.
    This leads to false positives regrading consumer group having some issues.

    This implementation gives us the benefit of reducing CPU cycles by filtering messages
    and avoiding unnecessary increase in consumer group lag in edge cases.
    """

    consecutive_drop_limit: int
    consecutive_drop_count: int = 0

    def should_drop(self, message: Message[KafkaPayload]) -> bool:
        if not super().should_drop(message):
            self.consecutive_drop_count = 0
            return False

        self.consecutive_drop_count += 1
        if self.consecutive_drop_count < self.consecutive_drop_limit:
            return True
        else:
            self.consecutive_drop_count = 0
            return False


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
        assert (
            message.partition.index == KAFKA_ONLY_PARTITION
        ), "CDC can only work with single partition topics for consistency"

        table_name = next(
            (value for key, value in message.payload.headers if key == "table"), None
        )

        return not table_name or table_name.decode("utf-8") != self.__postgres_table
