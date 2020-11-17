from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Generic, Mapping, NamedTuple, Optional, Sequence, TypeVar, Union

from snuba.clickhouse.columns import ColumnSet, QualifiedColumnSet
from snuba.query import ProcessableQuery, TSimpleDataSource
from snuba.query.data_source import DataSource


class JoinType(Enum):
    INNER = "INNER"
    LEFT = "LEFT"


class JoinModifier(Enum):
    ANY = "ANY"
    SEMI = "SEMI"


class JoinClass(Enum):
    ONE_2_ZERO_OR_ONE = "1-0/1"
    ONE_2_ONE = "1-1"
    ONE_2_N = "1-N"
    N_2_ZERO_OR_ONE = "N-0/1"
    N_2_ONE = "N-1"
    N_2_N = "N-N"


@dataclass(frozen=True)
class JoinNode(ABC, Generic[TSimpleDataSource]):
    """
    Represent a Node in the join tree data structure.
    Join nodes can be simple data sources, join expressions and queries.
    """

    @abstractmethod
    def get_column_sets(self) -> Mapping[str, ColumnSet]:
        raise NotImplementedError

    @abstractmethod
    def accept(self, visitor: JoinVisitor[TReturn, TSimpleDataSource]) -> TReturn:
        raise NotImplementedError


@dataclass(frozen=True)
class IndividualNode(JoinNode[TSimpleDataSource], Generic[TSimpleDataSource]):
    """
    Join node that represent an individual data source: an entity/table
    or a subquery.

    It also assign an alias to a node in a join expression.
    The alias is used in the join condition and in all the expressions
    that rely on the join in the query.
    """

    alias: str
    data_source: Union[TSimpleDataSource, ProcessableQuery[TSimpleDataSource]]

    def get_column_sets(self) -> Mapping[str, ColumnSet]:
        return (
            {self.alias: self.data_source.get_columns()}
            if self.alias is not None
            else {}
        )

    def accept(self, visitor: JoinVisitor[TReturn, TSimpleDataSource]) -> TReturn:
        return visitor.visit_individual_node(self)


class JoinConditionExpression(NamedTuple):
    """
    Represent one qualified column [alias.column] in the
    ON clause within the join expression.
    """

    table_alias: str
    column: str


class JoinCondition(NamedTuple):
    """
    Represents a condition in the ON clause in the JOIN expression.
    """

    left: JoinConditionExpression
    right: JoinConditionExpression


@dataclass(frozen=True)
class JoinClause(DataSource, JoinNode[TSimpleDataSource], Generic[TSimpleDataSource]):
    """
    Joins two JoinNodes.

    For a simple join between two entities/tables or two subqueries, both
    left and right node are IndividualNodes.

    For a more complex joins between multiple nodes, nodes are associative
    on the left:
    `(a INNER JOIN b ON condition) INNER JOIN c ON condition`

    This means the left node can be a join on its own while the right node
    still needs to be an IndividualNode.
    """

    left_node: JoinNode[TSimpleDataSource]
    right_node: IndividualNode[TSimpleDataSource]
    keys: Sequence[JoinCondition]
    join_type: JoinType
    join_modifier: Optional[JoinModifier] = None

    def get_column_sets(self) -> Mapping[str, ColumnSet]:
        return {
            **self.left_node.get_column_sets(),
            **self.right_node.get_column_sets(),
        }

    def get_columns(self) -> ColumnSet:
        return QualifiedColumnSet(self.get_column_sets())

    def __post_init__(self) -> None:
        column_set = self.get_columns()

        for condition in self.keys:
            assert f"{condition.left.table_alias}.{condition.left.column}" in column_set
            assert (
                f"{condition.right.table_alias}.{condition.right.column}" in column_set
            )

    def accept(self, visitor: JoinVisitor[TReturn, TSimpleDataSource]) -> TReturn:
        return visitor.visit_join_clause(self)


TReturn = TypeVar("TReturn")


class JoinVisitor(ABC, Generic[TReturn, TSimpleDataSource]):
    @abstractmethod
    def visit_individual_node(self, node: IndividualNode[TSimpleDataSource]) -> TReturn:
        raise NotImplementedError

    @abstractmethod
    def visit_join_clause(self, node: JoinClause[TSimpleDataSource]) -> TReturn:
        raise NotImplementedError
