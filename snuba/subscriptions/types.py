from dataclasses import dataclass
from typing import Generic, TypeVar

from snuba.query.query import Query


Timestamp = int


T = TypeVar("T")


@dataclass(frozen=True)
class Interval(Generic[T]):
    lower: T
    upper: T


@dataclass(frozen=True)
class Subscription:
    frequency: int

    def build_query(self, interval: Interval[Timestamp]) -> Query:
        raise NotImplementedError  # TODO
