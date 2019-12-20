from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import Future
from typing import Generic, NamedTuple

from snuba.utils.streams.types import Partition, TPayload, Topic


class MessageDetails(NamedTuple):
    partition: Partition
    offset: int


class Producer(Generic[TPayload], ABC):
    @abstractmethod
    def produce(self, topic: Topic, payload: TPayload) -> Future[MessageDetails]:
        """
        Produce to a topic.
        """
        # TODO: This also needs to be able to target a specific partition --
        # maybe by accepting Union[Topic, Partition] as the destination?
        raise NotImplementedError

    @abstractmethod
    def close(self) -> Future[None]:
        """
        Close the producer.
        """
        raise NotImplementedError
