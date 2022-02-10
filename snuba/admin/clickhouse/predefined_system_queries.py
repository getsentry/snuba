from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Sequence, Type


class _QueryRegistry:
    """Keep a mapping of SystemQueries to their names"""

    def __init__(self) -> None:
        self.__mapping: Dict[str, Type[SystemQuery]] = {}

    def register_class(self, cls: Type[SystemQuery]) -> None:
        existing_class = self.__mapping.get(cls.__name__)
        if not existing_class:
            self.__mapping[cls.__name__] = cls

    @property
    def all_queries(self) -> Sequence[Type[SystemQuery]]:
        return list(self.__mapping.values())


_QUERY_REGISTRY = _QueryRegistry()


@dataclass
class SystemQuery:
    sql: str

    @classmethod
    def to_json(cls) -> Dict[str, str]:
        return {
            "sql": cls.sql,
            "description": cls.__doc__ or "",
            "name": cls.__name__,
        }

    def __init_subclass__(cls) -> None:
        _QUERY_REGISTRY.register_class(cls)
        return super().__init_subclass__()

    @classmethod
    def all_queries(cls) -> Sequence[Type[SystemQuery]]:
        return _QUERY_REGISTRY.all_queries


class CurrentMerges(SystemQuery):
    """Currently executing merges"""

    sql = """
    SELECT
        table,
        elapsed,
        progress,
        result_part_name
    FROM system.merges
    """


class ActivePartsPerTable(SystemQuery):
    """Number of parts grouped by table. Parts should not be in the high thousands."""

    sql = """
    SELECT
        count(),
        partition,
        table
    FROM system.parts
    WHERE active = 1
    GROUP BY table, partition
    ORDER BY count()
    """


class PartitionSizeByTable(SystemQuery):
    """Sum of the size of parts within a partition on a given table."""

    sql = """
    SELECT
        partition,
        table,
        count(),
        formatReadableSize(sum(bytes_on_disk) as bytes) as size
    FROM system.parts
    WHERE active = 1
    GROUP BY partition, table
    ORDER BY partition ASC
    """


class PartSizeWithinPartition(SystemQuery):
    """Gets the size of parts within a specific partition. You'll need to input
    partition = '(90,"2022-01-24")' with the partition you want.

    To get partitions you can use SELECT partition FROM system.parts
    """

    sql = """
    SELECT name, formatReadableSize(bytes_on_disk) as size
    FROM system.parts
    WHERE
        partition = '(90,"2022-01-24")'  AND
        active = 1 AND
        bytes_on_disk > 1000000
    ORDER BY bytes_on_disk DESC
    """
