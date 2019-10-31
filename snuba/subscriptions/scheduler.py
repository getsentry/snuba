import math
from typing import Iterator, Mapping, Tuple
from uuid import UUID

from snuba.subscriptions.types import Interval, Subscription, Timestamp


class Scheduler:
    def __init__(self, subscriptions: Mapping[UUID, Subscription]) -> None:
        self.__subscriptions = subscriptions

    def set(self, key: UUID, value: Subscription) -> None:
        raise NotImplementedError

    def delete(self, key: UUID) -> None:
        raise NotImplementedError

    def find(
        self, interval: Interval[Timestamp]
    ) -> Iterator[Tuple[Timestamp, Tuple[UUID, Subscription]]]:
        for uuid, subscription in self.__subscriptions.items():
            for i in range(
                math.ceil(interval.lower / subscription.frequency),
                math.floor(interval.upper / subscription.frequency),
            ):
                yield i * subscription.frequency, (uuid, subscription)
