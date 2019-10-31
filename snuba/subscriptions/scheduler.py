from typing import Mapping
from uuid import UUID

from snuba.subscriptions.types import Subscription


class Scheduler:
    def __init__(self, subscriptions: Mapping[UUID, Subscription]) -> None:
        self.__subscriptions = subscriptions

    def set(self, key: UUID, value: Subscription) -> None:
        raise NotImplementedError

    def delete(self, key: UUID) -> None:
        raise NotImplementedError
