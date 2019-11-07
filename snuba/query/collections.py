from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Generic, Iterator, TypeVar


TNode = TypeVar("TNode")


class NodeContainer(Generic[TNode], ABC):
    """
    A container of nodes in the query.
    This is an Iterable so we can traverse the tree and it provides
    a map method to run a map on the content of the collection in
    place.
    """

    @abstractmethod
    def __iter__(self) -> Iterator[TNode]:
        raise NotImplementedError

    @abstractmethod
    def map(self, func: Callable[[TNode], TNode]) -> None:
        """
        Maps the content of the container in place.
        Defining a map function that would return a node to replace
        the mapped one in the tree is impractical since nodes may
        represent either sequences (parameters of functions) or individual
        nodes. This makes it very hard to provide a meaningful return
        type for the map function.

        The solution is that we can call map only on containers and
        they will transparently apply map on their content, but will
        not produce a new container to replace the original one.
        """
        raise NotImplementedError
