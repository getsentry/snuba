from __future__ import annotations

from abc import ABC, abstractmethod
from collections import ChainMap
from dataclasses import dataclass
from enum import Enum
from typing import List, Mapping, NamedTuple, Optional, Sequence, Union

from dataclasses import dataclass
from snuba.query.data_source.simple import Entity
from snuba.query import ProcessableQuery
from snuba.query.data_source import DataSource
from snuba.clickhouse.columns import ColumnSet, QualifiedColumnSet, SchemaModifiers
from snuba.datasets.schemas import RelationalSource, Schema
from snuba.datasets.schemas.tables import TableSource
from snuba.query.expressions import FunctionCall


class JoinType(Enum):
    INNER = "INNER"
    LEFT = "LEFT"


class JoinModifier(Enum):
    ANY = "ANY"
    SEMI = "SEMI"


Joinable = Union[
    Entity,
    RelationalSource,
    ProcessableQuery[Entity],
    ProcessableQuery[RelationalSource],
    JoinClause,
]


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
class JoinClause(DataSource):
    left_node: Joinable
    right_node: Joinable
    mapping: Sequence[JoinCondition]
    join_type: JoinType
    join_modifier: Optional[JoinModifier]

    def get_columns(self) -> ColumnSet[SchemaModifiers]:
        raise NotImplementedError
