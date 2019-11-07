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
        """
        Used to iterate over the nodes in this container. The exact
        semantics depends on the structure of the container (if the
        container is linear this would work like iterating over a
        sequence, if it is a tree, this would be a traversal).
        See the implementations for more details.
        """
        raise NotImplementedError

    @abstractmethod
    def transform(self, func: Callable[[TNode], TNode]) -> None:
        """
        Transforms the content of the container in place.
        Defining a collection style map function that would return a
        node to replace the mapped one in the tree is impractical since
        nodes may represent either sequences (parameters of functions)
        or individual nodes. This makes it very hard to provide a
        meaningful return type for the map function.

        The solution is that we can call map only on containers and
        they will transparently apply map on their content, but will
        not produce a new container to replace the original one.
        """
        raise NotImplementedError
