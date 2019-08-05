from __future__ import annotations

from abc import ABC, abstractmethod

from contextlib import contextmanager
from typing import NewType, Generator, IO, Optional, Sequence
from dataclasses import dataclass

SnapshotId = NewType("SnapshotId", str)


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


class BulkLoadSource(ABC):
    """
    Represent a source we can bulk load Snuba datasets from.
    The bulk data has to be organized in tables represented through files
    and must be able to provide a descriptor, but there is no constraint on where
    this actually comes from.
    """

    @classmethod
    def load(cls, path: str, product: str) -> BulkLoadSource:
        raise NotImplementedError

    @abstractmethod
    def get_descriptor(self) -> SnapshotDescriptor:
        raise NotImplementedError

    @abstractmethod
    @contextmanager
    def get_table_file(self, table: str) -> Generator[IO[bytes], None, None]:
        raise NotImplementedError
