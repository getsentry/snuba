from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field
from typing import Optional, Sequence

from snuba.clickhouse.columns import ColumnSet as PhysicalColumnSet
from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.query.allocation_policies import DEFAULT_PASSTHROUGH_POLICY, AllocationPolicy
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

    @property
    def human_readable_id(self) -> str:
        """A property that identifies this datasource, for human consumption only"""
        raise NotImplementedError

    pass


@dataclass(frozen=True)
class Entity(SimpleDataSource):
    """
    Represents an Entity in the logical query.
    """

    key: EntityKey
    schema: EntityColumnSet
    sample: Optional[float] = None

    def get_columns(self) -> EntityColumnSet:
        return self.schema

    @property
    def human_readable_id(self) -> str:
        return f"Entity({self.key.value})"


@dataclass(frozen=True)
class Table(SimpleDataSource):
    """
    Represents a table or a view in the physical query.
    """

    table_name: str
    schema: PhysicalColumnSet
    # By default a table has a regular passthrough policy.
    # this is overwridden by the query pipeline if there
    # is one defined on the storage.
    allocation_policies: list[AllocationPolicy] = field(
        default_factory=lambda: [DEFAULT_PASSTHROUGH_POLICY]
    )
    final: bool = False
    sampling_rate: Optional[float] = None
    # TODO: Move mandatory connditions out of
    # here as they are structural property of a storage. This requires
    # the processors that consume these fields to access the storage.
    mandatory_conditions: Sequence[FunctionCall] = field(default_factory=list)

    def get_columns(self) -> PhysicalColumnSet:
        return self.schema

    @property
    def human_readable_id(self) -> str:
        return f"Table({self.table_name})"
