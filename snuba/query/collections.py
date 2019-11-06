from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Generic, Iterable, Iterator, TypeVar


TNode = TypeVar("TNode")


class NodeContainer(Generic[TNode], Iterable[TNode], ABC):
    """
    A container of ndoes in the query.
    This is an Iterable so we can traverse the tree and it provides
    a map method to run a map on the content of the collection in
    place.
    """

    @abstractmethod
    def map(self, closure: Callable[[TNode], TNode]) -> None:
        """
        Maps the content of the container in place.
        Defining a map function that qould return a node to replace
        the mapped one in the tree is impractical since nodes may
        represent either sequences (parameters of functions) or individual
        nodes. This makes it very hard to provide a meaningful return
        type for the map function.

        The solution is that we can call map only on containers and
        they will transparently apply map on their content, but will
        not produce a new container to replace the original one.
        """
        raise NotImplementedError


class CompositeNodeContainer(Generic[TNode], NodeContainer[TNode]):
    """
    Container of containers. It can be used to iterate and map
    over several containers of nodes of the same type.
    """

    def __init__(
        self,
        containers: Iterable[NodeContainer[TNode]],
    ) -> None:
        self.__containers = containers

    def __iter__(self) -> Iterator[TNode]:
        for c in self.__containers:
            for element in c:
                yield element

    def map(self, closure: Callable[[TNode], TNode]) -> None:
        for c in self.__containers:
            c.map(closure)
