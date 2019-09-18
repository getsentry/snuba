from dataclasses import dataclass
from enum import Enum
from typing import Mapping, NamedTuple, Optional, Sequence

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

        return f"{left_str} {self.join_type.value} JOIN {right_str} ON {on_clause}"


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

    def __get_schemas(self, structure: JoinStructure) -> Mapping[str, Schema]:
        def get_schemas_rec(source: JoinedSource) -> Mapping[str, Schema]:
            if not isinstance(source.source, JoinStructure):
                return {source.alias: source.source}
            else:
                joined_structure = source.source
                left = get_schemas_rec(joined_structure.left_schema)
                right = get_schemas_rec(joined_structure.right_schema)
                return {**left, **right}

        return {
            **get_schemas_rec(structure.left_schema),
            **get_schemas_rec(structure.right_schema),
        }

    def __get_columns(self, structure: JoinStructure) -> ColumnSet:
        """
        Extracts all the columns recursively from the joined schemas and
        flattens this structure adding the columns into one ColumnSet
        prepended with the schema alias.
        """
        schemas = self.__get_schemas(structure)
        ret = []
        for alias, schema in schemas.items():
            # Iterate over the structured columns. get_columns() flattens nested
            # columns. We need them intact here.
            for column in schema.get_columns().columns:
                ret.append((f"{alias}.{column.name}", column.type))
        return ColumnSet(ret)

    def get_clickhouse_source(self) -> str:
        return self.__join_structure.get_clickhouse_source()
