from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from concurrent.futures import Future
from typing import MutableMapping, Union
from uuid import UUID

from snuba.utils.streams.abstract import EndOfStream
from snuba.subscriptions.protocol import (
    SubscriptionDeleteRequest,
    SubscriptionMessage,
    SubscriptionUpdateRequest,
)
from snuba.subscriptions.types import Subscription

logger = logging.getLogger(__name__)


class SubscriptionMessageProcessorState(ABC):
    @abstractmethod
    def handle(
        self, future: Future[SubscriptionMessage]
    ) -> SubscriptionMessageProcessorState:
        raise NotImplementedError


class Initializing(SubscriptionMessageProcessorState):
    def __init__(self) -> None:
        self.__subscriptions: MutableMapping[UUID, Subscription] = {}

    def handle(
        self, future: Future[SubscriptionMessage]
    ) -> Union[Initializing, Streaming]:
        try:
            message: SubscriptionMessage = future.result()
        except EndOfStream:
            return Streaming()

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
        else:
            raise TypeError

        return self


class Streaming(SubscriptionMessageProcessorState):
    def __init__(self) -> None:
        self.__subscriptions: MutableMapping[UUID, Subscription] = {}

    def handle(self, future: Future[SubscriptionMessage]) -> Streaming:
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
        else:
            raise TypeError

        return self
