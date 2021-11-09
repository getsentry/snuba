from __future__ import annotations

from typing import Iterator, Sequence

from snuba.clickhouse.columns import (
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
from snuba.utils.constants import NESTED_COL_EXPR_RE
from snuba.utils.schemas import ColumnSet

__all__ = [
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
            wildcard_prefix = match[1]
            if wildcard_prefix in self.__wildcard_column_map:
                return self.__wildcard_column_map[wildcard_prefix].type.flatten(key)[0]

        raise KeyError(key)

    def __iter__(self) -> Iterator[FlattenedColumn]:
        for col in self._flattened:
            yield col

        for col in self.__flat_wildcard_columns:
            yield col

    @property
    def columns(self) -> Sequence[Column[SchemaModifiers]]:
        return [*super().columns, *self.__wildcard_column_map.values()]
