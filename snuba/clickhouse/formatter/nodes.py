from abc import ABC
from dataclasses import dataclass
from typing import Any, Optional, Sequence, Union


@dataclass(frozen=True)
class FormattedNode(ABC):
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

    def structured(self) -> Union[str, Sequence[Any]]:
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

    def __str__(self) -> str:
        return " ".join([str(c) for c in self.content])

    def structured(self) -> Sequence[Any]:
        return [v.structured() for v in self.content]


@dataclass(frozen=True)
class PaddingNode(FormattedNode):
    prefix: Optional[str]
    node: FormattedNode
    suffix: Optional[str] = None

    def __str__(self) -> str:
        prefix = f"{self.prefix} " if self.prefix else ""
        suffix = f" {self.suffix}" if self.suffix else ""
        return f"{prefix}{str(self.node)}{suffix}"

    def structured(self) -> Union[str, Sequence[Any]]:
        ret: Union[str, Sequence[Any]]
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
        else:
            return ret


@dataclass(frozen=True)
class FormattedQuery(SequenceNode):
    """
    Used to move around a query data structure after all clauses have
    been formatted but such that it can be still serialized
    differently for different usages (running the query or tracing).
    """

    def get_sql(self, format: Optional[str] = None) -> str:
        query = str(self)
        if format is not None:
            query = f"{query} FORMAT {format}"

        return query


@dataclass(frozen=True)
class FormattedSubQuery(SequenceNode):
    def __str__(self) -> str:
        return f"({super().__str__()})"
