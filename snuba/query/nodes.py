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

    @abstractmethod
    def format(self) -> str:
        """
        Turn this node into a string. for Clickhouse
        TODO: provide a clickhouse formatter to this method so that, through
        a strategy pattern, this class will provide the content for the query
        and the clickhouse formatter will provide the format.
        """
        raise NotImplementedError
