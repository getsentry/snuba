from abc import ABC
from dataclasses import dataclass, field
from typing import Optional, Sequence

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities import EntityKey
from snuba.query.data_source import DataSource
from snuba.query.expressions import FunctionCall


class SimpleDataSource(DataSource, ABC):
    """
    Marker type to identify data sources that are not nested and not
    composed by other data sources. For example Entities and
    RelationalSources (tables) are simple data sources while nested
    queries and joins are not SimpleDataSources.

    This class does not add anything but it is useful to define
    subclasses of the Query class that can only reference simple
    data sources.
    """

    pass


@dataclass(frozen=True)
class Entity(SimpleDataSource):
    """
    Represents an Entity in the logical query.
    """

    key: EntityKey
    schema: ColumnSet
    sample: Optional[float] = None

    def get_columns(self) -> ColumnSet:
        return self.schema


@dataclass(frozen=True)
class Table(SimpleDataSource):
    """
    Represents a table or a view in the physical query.
    """

    table_name: str
    schema: ColumnSet
    final: bool = False
    sampling_rate: Optional[float] = None
    # TODO: Move mandatory connditions out of
    # here as they are structural property of a storage. This requires
    # the processors that consume these fields to access the storage.
    mandatory_conditions: Sequence[FunctionCall] = field(default_factory=list)

    def get_columns(self) -> ColumnSet:
        return self.schema
