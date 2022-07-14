from abc import ABC, abstractmethod
from dataclasses import dataclass
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
            return False if str_value == self.header_value else True

        return True


@dataclass
class KafkaHeaderFilterWithBypass(KafkaHeaderFilter):
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
