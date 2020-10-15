from dataclasses import dataclass
from snuba.query.data_source import DataSource
from snuba.clickhouse.columns import ColumnSet


@dataclass(frozen=True)
class Entity(DataSource):
    """
    Represents an Entity in the logical query.
    """

    key: str
    schema: ColumnSet

    def get_columns(self) -> ColumnSet:
        return self.schema
