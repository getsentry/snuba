from __future__ import annotations

from abc import ABC, abstractmethod
from collections import ChainMap
from dataclasses import dataclass
from enum import Enum
from typing import Mapping, NamedTuple, Sequence


from snuba.clickhouse.columns import QualifiedColumnSet
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

    def __str__(self) -> str:
        return f"{self.left.table_alias}.{self.left.column} = " \
            f"{self.right.table_alias}.{self.right.column}"


class JoinedSource(ABC):
    """
    Represent an abstract node in the Join Structure tree. It can be
    a schema that will have an alias or another join structure.
    This class only knows how to print itself in the Join clause and
    how to return all the schemas (with aliases) included in the subtrees.
    """

    @abstractmethod
    def get_schemas(self) -> Mapping[str, Schema]:
        """
        Returns the mapping between alias and schema in the joined expression.
        This is called when navigating the join tree to build a comprehensive
        mapping of all the referenced schemas with their aliases.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class SchemaJoinedSource(JoinedSource):
    """
    Represent one qualified data source in the JOIN expression.
    It can be a table or a view.
    """
    alias: str
    schema: Schema

    def __str__(self) -> str:
        return f"{self.schema.get_data_source()} {self.alias}"

    def get_schemas(self) -> Mapping[str, Schema]:
        return {self.alias: self.schema}


@dataclass(frozen=True)
class SubJoinSource(JoinedSource):
    """
    Represents a sub expression in the join clause, which is a join on its own.
    """
    structure: JoinStructure

    def __str__(self) -> str:
        return f"{self.structure.get_data_source()}"

    def get_schemas(self) -> Mapping[str, Schema]:
        left = self.structure.left_source.get_schemas()
        right = self.structure.right_source.get_schemas()
        overlapping_aliases = left.keys() & right.keys()
        for alias in overlapping_aliases:
            # Ensures none defines the same alias twice in the join referring
            # to different tables.
            assert left[alias] == right[alias]
        return ChainMap(left, right)


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

    def get_data_source(self) -> str:
        on_clause = " AND ".join([str(m) for m in self.mapping])
        return f"{self.left_source} {self.join_type.value} JOIN {self.right_source} ON {on_clause}"


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
            columns=self.__get_columns(join_root),
        )
        self.__join_structure = join_root

    def __get_columns(self, structure: JoinStructure) -> QualifiedColumnSet:
        """
        Extracts all the columns recursively from the joined schemas and
        builds a column set that preserves the structure.
        """
        schemas = SubJoinSource(structure).get_schemas()
        column_sets = {alias: schema.get_columns() for alias, schema in schemas.items()}
        return QualifiedColumnSet(column_sets)

    def get_data_source(self) -> str:
        return self.__join_structure.get_data_source()
