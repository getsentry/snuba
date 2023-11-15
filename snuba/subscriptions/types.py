from dataclasses import dataclass
from typing import Any, Generic, Protocol, TypeVar

TComparable = TypeVar("TComparable", contravariant=True)


class Comparable(Protocol[TComparable]):
    """
    Defines the protocol for comparable objects. Objects that satisfy this
    protocol are assumed to have an ordering via the "rich comparison"
    methods defined here.

    "An ordering" does not necessarily imply a *total* ordering, or even that
    the ordering itself is deterministic. The Python documentation provides a
    more detailed explanation about the guarantees (or lack thereof) provided
    by these methods: https://docs.python.org/3/reference/datamodel.html#object.__lt__

    This class exists primarily to satisfy the type checker when dealing with
    generics that will be directly compared, and secondarily to provide
    documentation via type annotations.

    In reality, this class provides little to no practical benefit, since all
    of these methods defined in the protocol are part of the ``object`` class
    definition (which is shared by all classes by default) but returns
    ``NotImplemented`` rather than returning a valid result. (This protocol
    is not defined as runtime checkable for that reason.)
    """

    def __lt__(self, other: TComparable) -> bool:
        raise NotImplementedError

    def __le__(self, other: TComparable) -> bool:
        raise NotImplementedError

    def __gt__(self, other: TComparable) -> bool:
        raise NotImplementedError

    def __ge__(self, other: TComparable) -> bool:
        raise NotImplementedError

    def __eq__(self, other: object) -> bool:
        raise NotImplementedError

    def __ne__(self, other: object) -> bool:
        raise NotImplementedError


T = TypeVar("T", bound=Comparable[Any])


@dataclass(frozen=True)
class InvalidRangeError(ValueError, Generic[T]):
    lower: T
    upper: T


@dataclass(frozen=True)
class Interval(Generic[T]):
    lower: T
    upper: T

    def __post_init__(self) -> None:
        if self.lower is None or self.upper is None:
            raise InvalidRangeError(self.lower, self.upper)
        if not self.upper >= self.lower:
            raise InvalidRangeError(self.lower, self.upper)
