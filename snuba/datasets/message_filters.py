from abc import ABC, abstractmethod
from typing import Generic

from snuba.utils.streams.backends.kafka import KafkaPayload
from snuba.utils.streams.types import Message, TPayload


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
