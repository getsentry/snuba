from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from concurrent.futures import Future
from typing import Generic, MutableMapping, MutableSet, Optional, Sequence, Union
from uuid import UUID

from snuba.utils.streams.abstract import (
    Consumer,
    EndOfStream,
    Message,
    Producer,
    TOffset,
    TStream,
)


logger = logging.getLogger(__name__)


Timestamp = int


@dataclass(frozen=True)
class Subscription:
    __slots__ = ["frequency"]

    frequency: int


@dataclass(frozen=True)
class SubscriptionUpdateRequest:
    __slots__ = ["uuid", "subscription"]

    uuid: UUID
    subscription: Subscription


@dataclass(frozen=True)
class SubscriptionDeleteRequest:
    __slots__ = ["uuid"]

    uuid: UUID


@dataclass(frozen=True)
class SubscriptionRenewalRequest:
    __slots__ = ["uuid"]

    uuid: UUID


@dataclass(frozen=True)
class SubscriptionRenewalResponse:
    __slots__ = ["uuid"]

    uuid: UUID


SubscriptionMessage = Union[
    SubscriptionUpdateRequest,
    SubscriptionDeleteRequest,
    SubscriptionRenewalRequest,
    SubscriptionRenewalResponse,
]


class SubscriptionConsumerState(ABC):
    @abstractmethod
    def handle(self, future: Future[SubscriptionMessage]) -> SubscriptionConsumerState:
        raise NotImplementedError


class InitializingState(SubscriptionConsumerState):
    def __init__(
        self, stream: TStream, producer: Producer[TStream, SubscriptionMessage]
    ) -> None:
        self.__stream = stream
        self.__producer = producer

        self.__subscriptions: MutableMapping[UUID, Subscription] = {}
        self.__renewals: MutableSet[UUID] = set()

    def handle(
        self, future: Future[SubscriptionMessage]
    ) -> Union[InitializingState, StreamingState]:
        try:
            message: SubscriptionMessage = future.result()
        except EndOfStream:
            for uuid in self.__renewals:
                try:
                    subscription = self.__subscriptions[uuid]
                except KeyError:
                    logger.warning(
                        "Unable to fulfill subscription renewal request for %r, no subscription exists.",
                        uuid,
                    )
                else:
                    self.__producer.produce(
                        self.__stream, SubscriptionUpdateRequest(uuid, subscription)
                    )
                self.__producer.produce(
                    self.__stream, SubscriptionRenewalResponse(uuid)
                )
            return StreamingState(self.__stream, self.__producer)

        if isinstance(message, SubscriptionUpdateRequest):
            self.__subscriptions[message.uuid] = message.subscription
        elif isinstance(message, SubscriptionDeleteRequest):
            try:
                del self.__subscriptions[message.uuid]
            except KeyError:
                logger.debug(
                    "Unable to fulfill subscription deletion request for %r, no subscription exists.",
                    message.uuid,
                )
        elif isinstance(message, SubscriptionRenewalRequest):
            self.__renewals.add(message.uuid)
        elif isinstance(message, SubscriptionRenewalResponse):
            try:
                self.__renewals.remove(message.uuid)
            except KeyError:
                pass
        else:
            raise TypeError

        return self


class StreamingState(SubscriptionConsumerState):
    def __init__(
        self, stream: TStream, producer: Producer[TStream, SubscriptionMessage]
    ) -> None:
        self.__stream = stream
        self.__producer = producer

        self.__subscriptions: MutableMapping[UUID, Subscription] = {}

    def handle(self, future: Future[SubscriptionMessage]) -> StreamingState:
        try:
            message = future.result()
        except EndOfStream:
            return self

        if isinstance(message, SubscriptionUpdateRequest):
            self.__subscriptions[message.uuid] = message.subscription
        elif isinstance(message, SubscriptionDeleteRequest):
            try:
                del self.__subscriptions[message.uuid]
            except KeyError:
                logger.debug(
                    "Unable to fulfill subscription deletion request for %r, no subscription exists.",
                    message.uuid,
                )
        elif isinstance(message, SubscriptionRenewalRequest):
            try:
                subscription = self.__subscriptions[message.uuid]
            except KeyError:
                logger.warning(
                    "Unable to fulfill subscription renewal request for %r, no subscription exists.",
                    message.uuid,
                )
            else:
                self.__producer.produce(
                    self.__stream, SubscriptionUpdateRequest(message.uuid, subscription)
                )
        elif isinstance(message, SubscriptionRenewalResponse):
            pass
        else:
            raise TypeError

        return self


class SubscriptionConsumer(Generic[TStream]):
    def __init__(
        self,
        consumer: Consumer[TStream, TOffset, SubscriptionMessage],
        producer: Producer[TStream, SubscriptionMessage],
    ) -> None:
        self.__consumer = consumer
        self.__producer = producer
        self.__streams: MutableMapping[TStream, SubscriptionConsumerState] = {}

    def assign(self, streams: Sequence[TStream]) -> None:
        # XXX: This is a naive implementation that assumes ``assign`` is only
        # called once and will likely need to be modified for real-world use.

        # TODO: This need to include the initial offset.
        self.__consumer.assign(streams)

        for stream in streams:
            self.__streams[stream] = InitializingState(stream, self.__producer)

    def poll(self, timeout: Optional[float] = None) -> None:
        future: Future[SubscriptionMessage]

        try:
            message = self.__consumer.poll(timeout)
        except Exception as error:
            future = Future()
            future.set_exception(error)
            if isinstance(error, EndOfStream):
                self.__streams[error.stream].handle(future)
        else:
            if message is not None:
                future = Future()
                future.set_result(message.value)
                self.__streams[message.stream].handle(future)
