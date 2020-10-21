from abc import ABC
from typing import cast, Sequence

from snuba.clickhouse.columns import ColumnType


class ColumnTypeWithModifier(ABC, ColumnType):
    def __init__(self, inner_type: ColumnType) -> None:
        self.inner_type = inner_type

    def get_raw(self) -> ColumnType:
        return self.inner_type.get_raw()


class Materialized(ColumnTypeWithModifier):
    def __init__(self, inner_type: ColumnType, expression: str) -> None:
        super().__init__(inner_type)
        self.expression = expression

    def __repr__(self) -> str:
        return "Materialized({}, {})".format(repr(self.inner_type), self.expression)

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.expression == cast(Materialized, other).expression
            and self.inner_type == cast(Materialized, other).inner_type
        )

    def for_schema(self) -> str:
        return "{} MATERIALIZED {}".format(
            self.inner_type.for_schema(), self.expression,
        )


class WithCodecs(ColumnTypeWithModifier):
    def __init__(self, inner_type: ColumnType, codecs: Sequence[str]) -> None:
        super().__init__(inner_type)
        self.__codecs = codecs

    def __repr__(self) -> str:
        return f"WithCodecs({repr(self.inner_type)}, {', '.join(self.__codecs)})"

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.__codecs == cast(WithCodecs, other).__codecs
            and self.inner_type == cast(WithCodecs, other).inner_type
        )

    def for_schema(self) -> str:
        return f"{self.inner_type.for_schema()} CODEC ({', '.join(self.__codecs)})"


class WithDefault(ColumnTypeWithModifier):
    def __init__(self, inner_type: ColumnType, default: str) -> None:
        super().__init__(inner_type)
        self.default = default

    def __repr__(self) -> str:
        return "WithDefault({}, {})".format(repr(self.inner_type), self.default)

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.default == cast(WithDefault, other).default
            and self.inner_type == cast(WithDefault, other).inner_type
        )

    def for_schema(self) -> str:
        return "{} DEFAULT {}".format(self.inner_type.for_schema(), self.default)


class LowCardinality(ColumnTypeWithModifier):
    def __init__(self, inner_type: ColumnType) -> None:
        super().__init__(inner_type)

    def __repr__(self) -> str:
        return "LowCardinality({})".format(repr(self.inner_type))

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.inner_type == cast(LowCardinality, other).inner_type
        )

    def for_schema(self) -> str:
        return "LowCardinality({})".format(self.inner_type.for_schema())
