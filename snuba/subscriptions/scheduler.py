import math
from dataclasses import dataclass
from typing import Iterator, MutableMapping, Tuple
from uuid import UUID


@dataclass(frozen=True)
class Subscription:
    frequency: int


class Schedule:
    """
    """

    def __init__(self):
        self.__subscriptions: MutableMapping[UUID, Subscription] = {}

    def set(self, key: UUID, value: Subscription) -> None:
        self.__subscriptions[key] = value

    def find(self, lower: int, upper: int) -> Iterator[Tuple[int, Tuple[UUID, Subscription]]]:
        for uuid, subscription in self.__subscriptions.items():
            for i in range(math.ceil(lower / subscription.frequency), math.floor(upper / subscription.frequency)):
                yield i * subscription.frequency, (uuid, subscription)
