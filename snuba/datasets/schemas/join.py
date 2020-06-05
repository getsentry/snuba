from __future__ import annotations

from abc import ABC, abstractmethod
from collections import ChainMap
from dataclasses import dataclass
from enum import Enum
from typing import List, Mapping, NamedTuple, Optional, Sequence


from snuba.datasets.schemas import MandatoryCondition
from snuba.clickhouse.columns import ColumnSet, QualifiedColumnSet
from snuba.datasets.schemas import Schema, RelationalSource
from snuba.datasets.schemas.tables import TableSource
from snuba.query.types import Condition


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

    def __str__(self) -> str:
        return (
            f"{self.left.table_alias}.{self.left.column} = "
            f"{self.right.table_alias}.{self.right.column}"
        )


class JoinNode(RelationalSource, ABC):
    """
    Represent an abstract node in the Join Structure tree. It can be
    a schema that will have an alias or another join structure.
    This class only knows how to print itself in the Join clause and
    how to return all the schemas (with aliases) included in the subtrees.
    """

    @abstractmethod
    def get_tables(self) -> Mapping[str, TableSource]:
        """
        Returns the mapping between alias and schema in the joined expression.
        This is called when navigating the join tree to build a comprehensive
        mapping of all the referenced schemas with their aliases.
        """
        raise NotImplementedError

    def supports_sample(self) -> bool:
        """
        Disable the sample clause in joins
        """
        return False


class TableJoinNode(TableSource, JoinNode):
    """
    Represent one qualified data source in the JOIN expression.
    It can be a table or a view.
    """

    def __init__(
        self,
        table_name: str,
        columns: ColumnSet,
        mandatory_conditions: Optional[Sequence[MandatoryCondition]],
        prewhere_candidates: Optional[Sequence[str]],
        alias: str,
    ) -> None:
        super().__init__(table_name, columns, mandatory_conditions, prewhere_candidates)
        self.__alias = alias

    def format_from(self) -> str:
        return f"{super().format_from()} {self.__alias}"

    def get_tables(self) -> Mapping[str, TableSource]:
        return {self.__alias: self}

    def supports_sample(self) -> bool:
        """
        Individual tables support SAMPLE
        """
        return True


@dataclass(frozen=True)
class JoinClause(JoinNode):
    """
    Abstracts the join clause as a tree.
    Every node in the tree is either a join itself or a
    schema with an alias.
    Traversing the tree it is possible to build the join clause.

    This does not validate the join makes sense nor it checks
    the aliases are valid.
    """

    left_node: JoinNode
    right_node: JoinNode
    mapping: Sequence[JoinCondition]
    join_type: JoinType

    def format_from(self) -> str:
        on_clause = " AND ".join([str(m) for m in self.mapping])
        return f"{self.left_node.format_from()} {self.join_type.value} JOIN {self.right_node.format_from()} ON {on_clause}"

    def get_tables(self) -> Mapping[str, TableSource]:
        left = self.left_node.get_tables()
        right = self.right_node.get_tables()
        overlapping_aliases = left.keys() & right.keys()
        for alias in overlapping_aliases:
            # Ensures none defines the same alias twice in the join referring
            # to different tables.
            assert left[alias] == right[alias]
        return ChainMap(left, right)

    def get_columns(self) -> QualifiedColumnSet:
        """
        Extracts all the columns recursively from the joined schemas and
        builds a column set that preserves the structure.
        """
        tables = self.get_tables()
        column_sets = {alias: table.get_columns() for alias, table in tables.items()}
        return QualifiedColumnSet(column_sets)

    def get_mandatory_conditions(self) -> Sequence[MandatoryCondition]:
        tables = self.get_tables()
        all_conditions: List[MandatoryCondition] = []
        for table in tables.values():
            all_conditions.extend(table.get_mandatory_conditions())
        return all_conditions

    def get_prewhere_candidates(self) -> Sequence[str]:
        """
        The pre where condition can only come from the leftmost table in the
        join.
        """
        return self.left_node.get_prewhere_candidates()


class JoinedSchema(Schema):
    """
    Read only schema that represent multiple joined schemas.
    The join clause is defined by the JoinClause object
    that keeps reference to the schemas we are joining.
    """

    def __init__(self, join_root: JoinNode,) -> None:
        self.__source = join_root

    def get_data_source(self) -> RelationalSource:
        return self.__source
