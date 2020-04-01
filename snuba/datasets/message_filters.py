from abc import ABC, abstractmethod
from typing import Generic

from snuba.utils.streams.kafka import KafkaPayload
from snuba.utils.streams.types import Message, TPayload


class StreamMessageFilter(ABC, Generic[TPayload]):
    @abstractmethod
    def should_drop(self, message: Message[TPayload]) -> bool:
        raise NotImplementedError


class PassthroughKafkaFilter(StreamMessageFilter[KafkaPayload]):
    def should_drop(self, message: Message[KafkaPayload]) -> bool:
        return True
