from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Optional


class Node(ABC):
    """
    A Node in the Query tree. This can be a leaf (like a column) or an inteermediate
    composite node (like a nested expression).
    This class exists with the sole purpose of having a common abstraction when
    iterating over all the nodes of the query.

    Remark: the elements on the first level of nodes (column list, conditions, etc)
    are not represented through this class yet, their children are instead represented
    as Nodes. This is to reduce the complexty.
    This may be revisited when working on formatting.
    """
    @abstractmethod
    def iterate(self) -> Iterable[Node]:
        """
        Returns an iterable that will iterate over all the children of this Node.
        This can be used, for example, to iterate over the entire query to get
        the list of referenced columns.
        """
        raise NotImplementedError


class FormattableNode(Node, ABC):
    """
    A Node in the query tree that knows how to format itself for the clickhouse
    query.
    """

    @abstractmethod
    def format(self) -> str:
        """
        Turn this node into a string. for Clickhouse
        TODO: provide a clickhouse formatter to this method so that, through
        a strategy pattern, this class will provide the content for the query
        and the clickhouse formatter will provide the format.
        """
        raise NotImplementedError


class AliasedNode(Node, ABC):
    """
    Abstract representation of a node that can be given an alias in a query.
    """

    def __init__(self, alias: Optional[str]):
        self.__alias = alias

    @abstractmethod
    def _format_impl(self) -> str:
        """
        Template method for subclasses to provide their formatting logic.
        """
        raise NotImplementedError

    def format(self) -> str:
        # TODO: do something and call _format_impl
        raise NotImplementedError
