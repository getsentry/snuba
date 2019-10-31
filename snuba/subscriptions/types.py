from dataclasses import dataclass
from typing import Generic, TypeVar, Union
from uuid import UUID

from snuba.query.query import Query


Timestamp = int


T = TypeVar("T")


@dataclass(frozen=True)
class Interval(Generic[T]):
    lower: T
    upper: T


@dataclass(frozen=True)
class Subscription:
    __slots__ = ["frequency"]

    frequency: int

    def build_query(self, interval: Interval[Timestamp]) -> Query:
        raise NotImplementedError  # TODO


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
