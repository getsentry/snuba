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


class JoinedSource(NamedTuple):
    """
    Represent one qualified data source in the JOIN expression.
    It can be a table, a view or another join expression.
    """
    alias: Optional[str]
    source: Schema


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
    left_schema: JoinedSource
    right_schema: JoinedSource
    mapping: Sequence[JoinCondition]
    join_type: JoinType

    def get_clickhouse_source(self) -> str:
        left_expr = self.left_schema.source.get_clickhouse_source()
        left_alias = self.left_schema.alias
        left_str = f"{left_expr} {left_alias or ''}"

        right_expr = self.right_schema.source.get_clickhouse_source()
        right_alias = self.right_schema.alias
        right_str = f"{right_expr} {right_alias or ''}"

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
