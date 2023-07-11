from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Generator, Iterator, Mapping, NewType, Optional, Sequence

SnapshotId = NewType("SnapshotId", str)
SnapshotTableRow = Mapping[str, Any]


@dataclass(frozen=True)
class TableConfig:
    """
    Represents the snapshot configuration for a table.
    """

    table: str
    zip: bool
    columns: Optional[Sequence[ColumnConfig]]

    @classmethod
    def from_dict(cls, content: Mapping[str, Any]) -> TableConfig:
        columns = []
        for column in content["columns"]:
            # This has already been validated by the jsonschema validator
            assert isinstance(column, Mapping)
            if column.get("formatter") is not None:
                formatter: Optional[FormatterConfig] = FormatterConfig.from_dict(
                    column["formatter"]
                )
            else:
                formatter = None
            columns.append(ColumnConfig(name=column["name"], formatter=formatter))
        return TableConfig(content["table"], content["zip"], columns)


class FormatterConfig(ABC):
    """
    Parent class to all the the formatter configs.
    """

    @classmethod
    def from_dict(cls, content: Mapping[str, str]) -> FormatterConfig:
        if content["type"] == "datetime":
            return DateTimeFormatterConfig.from_dict(content)
        else:
            raise ValueError("Unknown config for column formatter")


class DateFormatPrecision(Enum):
    SECOND = "second"
    # Add more if/when needed


@dataclass(frozen=True)
class DateTimeFormatterConfig(FormatterConfig):
    precision: DateFormatPrecision

    @classmethod
    def from_dict(cls, content: Mapping[str, str]) -> DateTimeFormatterConfig:
        return DateTimeFormatterConfig(DateFormatPrecision(content["precision"]))


@dataclass(frozen=True)
class ColumnConfig:
    """
    Represents a column in the snapshot configuration.
    """

    name: str
    formatter: Optional[FormatterConfig] = None


@dataclass(frozen=True)
class SnapshotDescriptor:
    """
    Provides the metadata for the loaded snapshot.
    """

    id: SnapshotId
    tables: Sequence[TableConfig]

    def get_table(self, table_name: str) -> TableConfig:
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
    def get_parsed_table_file(
        self, table: str
    ) -> Generator[Iterator[SnapshotTableRow], None, None]:
        raise NotImplementedError

    @abstractmethod
    @contextmanager
    def get_preprocessed_table_file(
        self, table: str
    ) -> Generator[Iterator[bytes], None, None]:
        raise NotImplementedError
