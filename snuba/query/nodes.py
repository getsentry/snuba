from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


class Node(ABC):
    """
    A node in the Query AST. This can be a leaf or an intermediate node.
    The tree contains different types of nodes depending on the level and
    on the subtree (example: conditions have a different type than expressions).

    The root of the tree is not a Node itself yet (since it is the Query object).
    Representing the root as a node itself does not seem very useful right now
    since we never traverse the full tree. We could revisit that later.
    """
    pass


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


@dataclass
class AliasedNode(FormattableNode, ABC):
    """
    Abstract representation of a node that can be given an alias in a query.
    """
    alias: Optional[str]

    @abstractmethod
    def _format_impl(self) -> str:
        """
        Template method for subclasses to provide their formatting logic.
        """
        raise NotImplementedError

    def format(self) -> str:
        # TODO: do something and call _format_impl
        raise NotImplementedError
