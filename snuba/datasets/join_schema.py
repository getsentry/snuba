from dataclasses import dataclass
from enum import Enum
from typing import Optional, Sequence

from snuba.datasets.schema import Schema


class JoinType(Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"


@dataclass(frozen=True)
class JoinMapping():
    left_alias: str
    left_column: str
    right_alias: str
    right_column: str

    def for_query(self) -> str:
        return f"{self.left_alias}.{self.left_column} = " \
            f"{self.right_alias}.{self.right_column}"


@dataclass(frozen=True)
class JoinedSource():
    source: Schema
    alias: Optional[str]


@dataclass(frozen=True)
class JoinSchemaStorage:
    """
    Keep track of a basic multiple join.
    This builds a tree of sources joined with each other.
    Every side of the expression has an alias which is used
    to build the join and the mapping is based on the aliases
    themselves.

    This does not validate the join makes sense nor it checks
    the aliases are valid.
    """
    left_expression: JoinedSource
    right_expression: JoinedSource
    mapping: Sequence[JoinMapping]
    join_type: JoinType

    def for_query(self) -> str:
        left_expr = self.left_expression.source.for_query()
        left_alias = self.left_expression.alias
        left_str = f"{left_expr} {left_alias or ''}"

        right_expr = self.right_expression.source.for_query()
        right_alias = self.right_expression.alias
        right_str = f"{right_expr} {right_alias or ''}"

        on_clause = " AND ".join([m.for_query() for m in self.mapping])

        return f"({left_str} {self.join_type.value} JOIN {right_str} ON {on_clause})"


class JoinedSchema(Schema):

    def __init__(self,
        join_root: JoinSchemaStorage,
        migration_function=None,
    ) -> None:
        self.__join_storage = join_root
        super().__init__(
            columns=None,  # TODO: process the joined table to build the columns list
            migration_function=migration_function
        )

    def get_columns(self):
        raise NotImplementedError("Not implemented yet.")

    def for_query(self) -> str:
        return self.__join_storage.for_query()
