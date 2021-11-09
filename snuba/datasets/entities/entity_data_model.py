from __future__ import annotations

import re
from typing import Iterator, Optional, Sequence

from snuba.clickhouse.columns import (
    Any,
    Array,
    Column,
    ColumnType,
    DateTime,
    FixedString,
    FlattenedColumn,
    Float,
    SchemaModifiers,
    String,
    UInt,
)
from snuba.query.data_source import ColumnSet

__all__ = [
    "Any",
    "Array",
    "FixedString",
    "String",
    "Float",
    "WildcardColumn",
    "EntityColumnSet",
    "UInt",
    "DateTime",
    "Column",
]


NESTED_COL_EXPR_RE = re.compile(r"^([a-zA-Z0-9_\.]+)\[([a-zA-Z0-9_\.:-]+)\]$")


class WildcardColumn(ColumnType[SchemaModifiers]):
    pass


ValidEntityColumns = (WildcardColumn, UInt, Float, String, FixedString)


class EntityColumnSet(ColumnSet):
    """
    Regular column - types?
    Wildcard column (wildcard) -> tags[anything], context[anything]
    """

    def __init__(self, columns: Sequence[Column[SchemaModifiers]]) -> None:
        self.__standard_columns = {
            column.name: column
            for column in columns
            if isinstance(column, ValidEntityColumns)
        }
        self.__wildcard_columns = {
            column.name: column
            for column in columns
            if isinstance(column, WildcardColumn)
        }

        self.__flattened = [column.type.flatten(column.name)[0] for column in columns]

    def __contains__(self, column_name: str) -> bool:
        if column_name in self.__standard_columns:
            return True

        match = NESTED_COL_EXPR_RE.match(column_name)

        if match is not None:
            col_name = match[1]
            if col_name in self.__wildcard_columns:
                return True

        return False

    def __getitem__(self, key: str) -> FlattenedColumn:
        if key in self.__standard_columns:
            return self.__standard_columns[key].type.flatten(key)[0]

        match = NESTED_COL_EXPR_RE.match(key)

        if match is not None:
            col_name = match[1]
            if col_name in self.__wildcard_columns:
                return self.__wildcard_columns[col_name].type.flatten(key)[0]

        raise KeyError(key)

    def __iter__(self) -> Iterator[FlattenedColumn]:
        return iter(self.__flattened)

    def get(
        self, column_name: str, default: Optional[FlattenedColumn] = None
    ) -> Optional[FlattenedColumn]:
        """
        Returns the column if it exists else default

        tags[asdf]
        """
        try:
            return self[column_name]
        except KeyError:
            return default
