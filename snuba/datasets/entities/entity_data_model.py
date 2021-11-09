from __future__ import annotations

from typing import Iterator, Optional, Sequence, cast

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
            col for col in columns if not isinstance(col.type, WildcardColumn)
        ]

        wildcard_columns = [
            col for col in columns if isinstance(col.type, WildcardColumn)
        ]

        self._wildcard_columns = {col.name: col for col in wildcard_columns}

        super().__init__(standard_columns)

    def __eq__(self, other: object) -> bool:
        return (
            super().__eq__(other)
            and self._wildcard_columns == cast(EntityColumnSet, other)._wildcard_columns
        )

    def __contains__(self, column_name: str) -> bool:
        if super().__contains__(column_name):
            return True

        match = NESTED_COL_EXPR_RE.match(column_name)

        if match is not None:
            col_name = match[1]
            if col_name in self._wildcard_columns:
                return True

        return False

    def __getitem__(self, key: str) -> FlattenedColumn:
        try:
            standard_col = super().__getitem__(key)
            if standard_col:
                return standard_col
        except KeyError:
            pass

        match = NESTED_COL_EXPR_RE.match(key)

        if match is not None:
            wildcard_prefix = match[1]
            if wildcard_prefix in self._wildcard_columns:
                return self._wildcard_columns[wildcard_prefix].type.flatten(
                    wildcard_prefix
                )[0]

        raise KeyError(key)

    def get(
        self, key: str, default: Optional[FlattenedColumn] = None
    ) -> Optional[FlattenedColumn]:
        try:
            return self[key]
        except KeyError:
            return default

    def __iter__(self) -> Iterator[FlattenedColumn]:
        for col in self._flattened:
            yield col

        for wildcard_col in self._wildcard_columns.values():
            yield wildcard_col.type.flatten(col.name)[0]

    @property
    def columns(self) -> Sequence[Column[SchemaModifiers]]:
        return [*super().columns, *self._wildcard_columns.values()]
