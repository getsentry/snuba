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
    "Column",
    "DateTime",
    "EntityColumnSet",
    "FixedString",
    "Float",
    "String",
    "UInt",
    "WildcardColumn",
]


# TODO: Copied here due to circular reference
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
        standard_columns = [
            col for col in columns if not isinstance(col, WildcardColumn)
        ]

        wildcard_columns = [col for col in columns if isinstance(col, WildcardColumn)]

        self.__wildcard_column_map = {col.name: col for col in wildcard_columns}

        self.__flat_wildcard_columns = [
            column.type.flatten(column.name)[0] for column in wildcard_columns
        ]

        super().__init__(standard_columns)

    def __contains__(self, column_name: str) -> bool:
        if super().__contains__(column_name):
            return True

        match = NESTED_COL_EXPR_RE.match(column_name)

        if match is not None:
            col_name = match[1]
            if col_name in self.__wildcard_column_map:
                return True

        return False

    def __getitem__(self, key: str) -> FlattenedColumn:
        standard_col = super().__getitem__(key)

        if standard_col:
            return standard_col

        match = NESTED_COL_EXPR_RE.match(key)

        if match is not None:
            col_name = match[1]
            if col_name in self.__wildcard_column_map:
                return self.__wildcard_column_map[col_name].type.flatten(key)[0]

        raise KeyError(key)

    def __iter__(self) -> Iterator[FlattenedColumn]:
        # Do stuff
        super().__iter__()

        for col in self.__wildcard_column_map.values():
            yield col

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
