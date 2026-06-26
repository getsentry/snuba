from abc import ABC
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FormattedNode(ABC):  # noqa: B024 - methods raise NotImplementedError rather than using @abstractmethod to preserve runtime behavior
    """
    After formatting all the clauses of a query, we may serialize the
    query itself as a string or exporting it in a structured format for
    tracing and debugging.
    This data structure is a node in a tree of formatted expressions
    that can be serialized as a string or as a or in a structured form.
    """

    def __str__(self) -> str:
        """
        This serializes the formatted node as a string. For example,
        in a query, this would return the SQL.
        """
        raise NotImplementedError

    def structured(self) -> str | Sequence[Any]:
        """
        This exports the query as a Sequence of clauses. Each clause
        is either a string or a Sequence itself (like for subqueries).
        """
        raise NotImplementedError


@dataclass(frozen=True)
class StringNode(FormattedNode):
    value: str

    def __str__(self) -> str:
        return self.value

    def structured(self) -> str:
        return self.value


@dataclass(frozen=True)
class SequenceNode(FormattedNode):
    content: Sequence[FormattedNode]
    separator: str = " "

    def __str__(self) -> str:
        return self.separator.join([str(c) for c in self.content])

    def structured(self) -> Sequence[Any]:
        return [v.structured() for v in self.content]


@dataclass(frozen=True)
class PaddingNode(FormattedNode):
    prefix: str | None
    node: FormattedNode
    suffix: str | None = None

    def __str__(self) -> str:
        prefix = f"{self.prefix} " if self.prefix else ""
        suffix = f" {self.suffix}" if self.suffix else ""
        return f"{prefix}{str(self.node)}{suffix}"

    def structured(self) -> Sequence[Any]:
        ret: Sequence[Any]
        if self.prefix is not None:
            ret = [self.prefix, self.node.structured()]
        else:
            ret = [
                self.node.structured(),
            ]
        if self.suffix is not None:
            return ret + [
                self.suffix,
            ]
        return ret


@dataclass(frozen=True)
class FormattedQuery(SequenceNode):
    """
    Used to move around a query data structure after all clauses have
    been formatted but such that it can be still serialized
    differently for different usages (running the query or tracing).
    """

    def get_sql(self, format: str | None = None) -> str:
        query = str(self)
        if format is not None:
            query = f"{query} FORMAT {format}"

        return query


@dataclass(frozen=True)
class FormattedSubQuery(SequenceNode):
    def __str__(self) -> str:
        return f"({super().__str__()})"
