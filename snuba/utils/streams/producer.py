from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import Future
from typing import Generic, Union

from snuba.utils.streams.types import Message, Partition, TPayload, Topic


class Producer(Generic[TPayload], ABC):
    @abstractmethod
    def produce(
        self, destination: Union[Topic, Partition], payload: TPayload
    ) -> Future[Message[TPayload]]:
        """
        Produce to a topic or partition.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> Future[None]:
        """
        Close the producer.

        This method returns a ``Future`` that will have its result set when
        there are no more messages in flight.
        """
        raise NotImplementedError
