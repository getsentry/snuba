from dataclasses import dataclass
from typing import Optional

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities import EntityKey
from snuba.query.data_source import DataSource


@dataclass(frozen=True)
class Entity(DataSource):
    """
    Represents an Entity in the logical query.
    """

    key: EntityKey
    schema: ColumnSet
    sample_rate: Optional[float] = None

    def get_columns(self) -> ColumnSet:
        return self.schema
