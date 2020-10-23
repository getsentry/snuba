from typing import Sequence
from dataclasses import dataclass
from snuba.clickhouse.columns import TypeModifier


@dataclass(frozen=True)
class Materialized(TypeModifier):
    def __init__(self, expression: str) -> None:
        self.expression = expression

    def for_schema(self, content: str) -> str:
        return "{} MATERIALIZED {}".format(content, self.expression,)


@dataclass(frozen=True)
class WithCodecs(TypeModifier):
    def __init__(self, codecs: Sequence[str]) -> None:
        self.__codecs = codecs

    def for_schema(self, content: str) -> str:
        return f"{content} CODEC ({', '.join(self.__codecs)})"


@dataclass(frozen=True)
class WithDefault(TypeModifier):
    def __init__(self, default: str) -> None:
        self.default = default

    def for_schema(self, content: str) -> str:
        return "{} DEFAULT {}".format(content, self.default)


@dataclass(frozen=True)
class LowCardinality(TypeModifier):
    def for_schema(self, content: str) -> str:
        return "LowCardinality({})".format(content)
