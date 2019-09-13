from abc import abstractmethod, ABC
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Sequence, Set


def local_dataset_mode():
    return settings.DATASET_MODE == "local"


class SchemaSource(ABC):
    """
    Abstraction that represent the clickhouse source to be
    provided by dataset schema objects.
    This can represent a single table or a join between mutliple
    tables.

    In this implementation the class itself is able to generate
    the Clickhouse portion of the query (whether it is a SELECT or
    INSERT statement).
    Later (when we will have an abstract schema and a clickhouse one)
    we will split the abstract definition from the Clickhouse
    code generation.
    """

    TEST_TABLE_PREFIX = "test_"

    @abstractmethod
    def for_query(self) -> str:
        raise NotImplementedError


class TableSchemaSource(SchemaSource):
    def __init__(self, table_name: str) -> None:
        self.__table_name = table_name

    def for_query(self) -> str:
        return self.__table_name


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
    source: SchemaSource
    alias: Optional[str]


class JoinSchemaSource(SchemaSource):
    """
    Keep track of a basic multiple join.
    This builds a tree of sources joined with each other.
    Every side of the expression has an alias which is used
    to build the join and the mapping is based on the aliases
    themselves.

    This does not validate the join makes sense nor it checks
    the aliases are valid.
    """

    def __init__(self,
        left_expression: JoinedSource,
        right_expression: JoinedSource,
        mapping: Sequence[JoinMapping],
        join_type: JoinType
    ) -> None:
        self.__left_expression = left_expression
        self.__right_expression = right_expression
        self.__mapping = mapping
        self.__join_type = join_type

    def for_query(self) -> str:
        left_expr = self.__left_expression.source.for_query()
        left_alias = self.__left_expression.alias
        left_str = f"{left_expr} {left_alias or ''}"

        right_expr = self.__right_expression.source.for_query()
        right_alias = self.__right_expression.alias
        right_str = f"{right_expr} {right_alias or ''}"

        on_clause = " AND ".join([m.for_query() for m in self.__mapping])

        return f"({left_str} {self.__join_type.value} JOIN {right_str} ON {on_clause})"
