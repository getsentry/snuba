from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, Iterable, Iterator, TypeVar


TNode = TypeVar("TNode")


class NodeContainer(Generic[TNode], ABC):
    """
    A container of nodes in the query.
    This is an Iterable so we can traverse the tree. It does not mandate
    the data structure.
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


class CompositeNodeContainer(NodeContainer[TNode]):
    """
    Iterates over multiple containers transparently. Main usage
    is the iteration over multiple lists of expressions in the
    Query object.
    """

    def __init__(self, containers: Iterable[NodeContainer[TNode]]):
        self.__containers = containers

    def __iter__(self) -> Iterator[TNode]:
        for container in self.__containers:
            for element in container:
                yield element
