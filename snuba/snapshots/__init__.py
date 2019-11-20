from __future__ import annotations

from abc import ABC, abstractmethod

from contextlib import contextmanager
from typing import Any, Mapping, NewType, Generator, Iterable, Optional, Sequence
from dataclasses import dataclass

SnapshotId = NewType("SnapshotId", str)
SnapshotTableRow = Mapping[str, Any]


@dataclass(frozen=True)
class TableConfig:
    """
    Represents the snapshot configuration for a table.
    """

    table: str
    columns: Optional[Sequence[str]]


@dataclass(frozen=True)
class SnapshotDescriptor:
    """
    Provides the metadata for the loaded snapshot.
    """

    id: SnapshotId
    tables: Sequence[TableConfig]

    def get_table(self, table_name: str) -> str:
        for t in self.tables:
            if t.table == table_name:
                return t
        raise ValueError(f"Table {table_name} does not exists in the snapshot")


class BulkLoadSource(ABC):
    """
    Represent a source we can bulk load Snuba datasets from.
    The bulk data has to be organized in tables represented through files
    and must be able to provide a descriptor, but there is no constraint on where
    this actually comes from.
    """

    @abstractmethod
    def get_descriptor(self) -> SnapshotDescriptor:
        raise NotImplementedError

    @abstractmethod
    @contextmanager
    def get_table_file(
        self, table: str
    ) -> Generator[Iterable[SnapshotTableRow], None, None]:
        raise NotImplementedError
