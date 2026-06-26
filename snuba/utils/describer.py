from __future__ import annotations

from abc import ABC
from collections.abc import Sequence
from dataclasses import dataclass
from typing import NamedTuple


class Property(NamedTuple):
    name: str
    value: str


class DescriptionVisitor(ABC):  # noqa: B024 - intentional ABC; methods raise NotImplementedError rather than being abstract
    def visit_header(self, header: str | None) -> None:
        raise NotImplementedError

    def visit_description(self, desc: Description) -> None:
        raise NotImplementedError

    def visit_string(self, string: str) -> None:
        raise NotImplementedError

    def visit_property(self, property: Property) -> None:
        raise NotImplementedError


@dataclass(frozen=True)
class Description:
    """
    Abstract representation of a hierarchical datastructure that
    we want to print out either through CLI commands, UI or via
    an API.

    The serialization method is independent on the structure.
    """

    header: str | None
    content: Sequence[Description | str | Property]

    def accept(self, visitor: DescriptionVisitor) -> None:
        visitor.visit_header(self.header)
        for c in self.content:
            if isinstance(c, str):
                visitor.visit_string(c)
            elif isinstance(c, Property):
                visitor.visit_property(c)
            else:
                visitor.visit_description(c)


class Describable(ABC):  # noqa: B024 - intentional ABC; methods raise NotImplementedError rather than being abstract
    """
    Class to be extended by any data structure we want to describe
    either via CLI commands, UI or API.
    """

    # TODO: Use this approach for query formatting for tracing.
    def describe(self) -> Description:
        raise NotImplementedError
