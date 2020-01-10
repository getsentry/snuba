from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import Future
from typing import Generic, NamedTuple, Union

from snuba.utils.streams.types import Partition, TPayload, Topic


class MessageDetails(NamedTuple):
    partition: Partition
    offset: int


class Producer(Generic[TPayload], ABC):
    @abstractmethod
    def produce(
        self, destination: Union[Topic, Partition], payload: TPayload
    ) -> Future[MessageDetails]:
        """
        Produce to a topic or partition.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> Future[None]:
        """
        Close the producer.
        """
        raise NotImplementedError
