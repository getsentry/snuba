from typing import Sequence
from snuba.clickhouse.columns import TypeModifier


class Materialized(TypeModifier):
    def __init__(self, expression: str) -> None:
        self.expression = expression

    def __repr__(self) -> str:
        return "Materialized({})".format(self.expression)

    def for_schema(self, content: str) -> str:
        return "{} MATERIALIZED {}".format(content, self.expression,)


class WithCodecs(TypeModifier):
    def __init__(self, codecs: Sequence[str]) -> None:
        self.__codecs = codecs

    def __repr__(self) -> str:
        return f"WithCodecs({', '.join(self.__codecs)})"

    def for_schema(self, content: str) -> str:
        return f"{content} CODEC ({', '.join(self.__codecs)})"


class WithDefault(TypeModifier):
    def __init__(self, default: str) -> None:
        self.default = default

    def __repr__(self) -> str:
        return "WithDefault({})".format(self.default)

    def for_schema(self, content: str) -> str:
        return "{} DEFAULT {}".format(content, self.default)


class LowCardinality(TypeModifier):
    def __repr__(self) -> str:
        return "LowCardinality()"

    def for_schema(self, content: str) -> str:
        return "LowCardinality({})".format(content)
