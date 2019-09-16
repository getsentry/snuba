from dataclasses import dataclass
from enum import Enum
from typing import Callable, Mapping, Optional, Sequence

from snuba.clickhouse.columns import ColumnSet
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

    def get_from_clause(self) -> str:
        return f"{self.left_alias}.{self.left_column} = " \
            f"{self.right_alias}.{self.right_column}"


@dataclass(frozen=True)
class JoinedSource():
    source: Schema
    alias: Optional[str]


@dataclass(frozen=True)
class JoinStructure:
    """
    Abstracts the join clause as a tree.
    Every node in the tree is either a join itself or a
    schema with an alias.
    Traversing the tree it is possible to build the join
    clause.

    This does not validate the join makes sense nor it checks
    the aliases are valid.
    """
    left_schema: JoinedSource
    right_schema: JoinedSource
    mapping: Sequence[JoinMapping]
    join_type: JoinType

    def get_from_clause(self) -> str:
        left_expr = self.left_schema.source.get_from_clause()
        left_alias = self.left_schema.alias
        left_str = f"{left_expr} {left_alias or ''}"

        right_expr = self.right_schema.source.get_from_clause()
        right_alias = self.right_schema.alias
        right_str = f"{right_expr} {right_alias or ''}"

        on_clause = " AND ".join([m.get_from_clause() for m in self.mapping])

        return f"({left_str} {self.join_type.value} JOIN {right_str} ON {on_clause})"


class JoinedSchema(Schema):
    """
    Read only schema that represent multiple joined schemas.
    The join clause is defined by the JoinStructure object
    that keeps reference to the schemas we are joining.
    """

    def __init__(self,
        join_root: JoinStructure,
        migration_function: Optional[Callable[[str, Mapping[str, str]], Sequence[str]]]=None,
    ) -> None:
        self.__join_storage = join_root
        super().__init__(
            columns=ColumnSet([]),  # TODO: process the joined table to build the columns list
            migration_function=migration_function
        )

    def get_columns(self):
        """
        In this class we need to return the combination of
        the columns of all the joined tables prefixed with
        the alias, since the client will provide aliases in the
        query to disambiguate columns.
        """
        raise NotImplementedError("Not implemented yet.")

    def get_from_clause(self) -> str:
        return self.__join_storage.get_from_clause()
