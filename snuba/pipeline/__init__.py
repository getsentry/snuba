from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

TIn = TypeVar("TIn")
TOut = TypeVar("TOut")


class Segment(ABC, Generic[TIn, TOut]):
    """
    Represents a segment in a pipeline (generally a query execution
    pipeline). The only goal of this class is to make it easier to
    reuse pieces of a pipeline by imposing that the execute method
    takes the input and returns the output in a stateless way.
    """

    @abstractmethod
    def execute(self, input: TIn) -> TOut:
        raise NotImplementedError
