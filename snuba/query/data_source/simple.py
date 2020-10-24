from dataclasses import dataclass

from snuba.clickhouse.columns import ColumnSet, SchemaModifiers
from snuba.datasets.entities import EntityKey
from snuba.query.data_source import DataSource


@dataclass(frozen=True)
class Entity(DataSource):
    """
    Represents an Entity in the logical query.
    """

    key: EntityKey
    schema: ColumnSet[SchemaModifiers]

    def get_columns(self) -> ColumnSet[SchemaModifiers]:
        return self.schema
