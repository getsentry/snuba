from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import NamedTuple, Optional, Sequence

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.schemas import Schema


class JoinType(Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"


class JoinConditionExpression(NamedTuple):
    """
    Represent one qualified column [alias.column] in the
    ON clause within the join expression.
    """
    table_alias: str
    column: str


@dataclass(frozen=True)
class JoinCondition:
    """
    Represent a condition in the ON clause in the JOIN expression
    """
    left: JoinConditionExpression
    right: JoinConditionExpression

    def get_join_condition(self) -> str:
        return f"{self.left.table_alias}.{self.left.column} = " \
            f"{self.right.table_alias}.{self.right.column}"


class JoinedSource(ABC):
    """
    Represent an abstract node in the Join Structure tree. It can be
    a schema that will have an alias or another join structure.
    This class only knows how to print itself in the Join clause.
    """
    @abstractmethod
    def print(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class SchemaJoinedSource(JoinedSource):
    """
    Represent one qualified data source in the JOIN expression.
    It can be a table or a view.
    """
    alias: str
    schema: Schema

    def print(self) -> str:
        return f"{self.schema.get_clickhouse_source()} {self.alias}"


@dataclass(frozen=True)
class SubJoinSource(JoinedSource):
    """
    Represents a sub expression in the join clause, which is a join on its own.
    """
    structure: JoinStructure

    def print(self) -> str:
        return f"{self.structure.get_clickhouse_source()}"


@dataclass(frozen=True)
class JoinStructure:
    """
    Abstracts the join clause as a tree.
    Every node in the tree is either a join itself or a
    schema with an alias.
    Traversing the tree it is possible to build the join clause.

    This does not validate the join makes sense nor it checks
    the aliases are valid.
    """
    left_source: JoinedSource
    right_source: JoinedSource
    mapping: Sequence[JoinCondition]
    join_type: JoinType

    def get_clickhouse_source(self) -> str:
        left_str = self.left_source.print()
        right_str = self.right_source.print()

        on_clause = " AND ".join([m.get_join_condition() for m in self.mapping])

        return f"({left_str} {self.join_type.value} JOIN {right_str} ON {on_clause})"


class JoinedSchema(Schema):
    """
    Read only schema that represent multiple joined schemas.
    The join clause is defined by the JoinStructure object
    that keeps reference to the schemas we are joining.
    """

    def __init__(self,
        join_root: JoinStructure,
    ) -> None:
        super().__init__(
            columns=ColumnSet([]),  # TODO: process the joined table to build the columns list
        )
        self.__join_storage = join_root

    def get_columns(self):
        """
        In this class we need to return the combination of
        the columns of all the joined tables prefixed with
        the alias, since the client will provide aliases in the
        query to disambiguate columns.
        """
        raise NotImplementedError("Not implemented yet.")

    def get_clickhouse_source(self) -> str:
        return self.__join_storage.get_clickhouse_source()
