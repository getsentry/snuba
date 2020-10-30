from abc import ABC
from dataclasses import dataclass

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities import EntityKey
from snuba.query.data_source import DataSource


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

    def get_columns(self) -> ColumnSet:
        return self.schema
