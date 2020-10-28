from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Generic, Mapping, NamedTuple, Optional, Sequence, Union

from snuba.clickhouse.columns import ColumnSet, QualifiedColumnSet, SchemaModifiers
from snuba.query import ProcessableQuery, TSimpleDataSource
from snuba.query.data_source import DataSource


class JoinType(Enum):
    INNER = "INNER"
    LEFT = "LEFT"


class JoinModifier(Enum):
    ANY = "ANY"
    SEMI = "SEMI"


class JoinNode(ABC, Generic[TSimpleDataSource]):
    alias: str

    @abstractmethod
    def get_column_sets(self) -> Mapping[str, ColumnSet[SchemaModifiers]]:
        raise NotImplementedError


class IndividualNode(JoinNode[TSimpleDataSource], Generic[TSimpleDataSource]):
    data_source: Union[TSimpleDataSource, ProcessableQuery[TSimpleDataSource]]

    def get_column_sets(self) -> Mapping[str, ColumnSet[SchemaModifiers]]:
        return {self.alias: self.data_source.get_columns()}


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


@dataclass(frozen=True)
class JoinClause(DataSource, JoinNode[TSimpleDataSource], Generic[TSimpleDataSource]):
    left_node: IndividualNode[TSimpleDataSource]
    right_node: JoinNode[TSimpleDataSource]
    keys: Sequence[JoinCondition]
    join_type: JoinType
    join_modifier: Optional[JoinModifier]

    def get_column_sets(self) -> Mapping[str, ColumnSet[SchemaModifiers]]:
        return {
            **self.left_node.get_column_sets(),
            **self.right_node.get_column_sets(),
        }

    def get_columns(self) -> ColumnSet[SchemaModifiers]:
        return QualifiedColumnSet(self.get_column_sets())

    def __post_init__(self) -> None:
        column_set = self.get_columns()
        for condition in self.keys:
            assert f"{condition.left.table_alias}.{condition.left.column}" in column_set
            assert (
                f"{condition.right.table_alias}.{condition.right.column}" in column_set
            )
