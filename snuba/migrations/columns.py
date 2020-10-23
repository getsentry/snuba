from typing import Optional, Sequence, List
from dataclasses import dataclass
from snuba.clickhouse.columns import TypeModifier, TypeModifiers, Nullable


@dataclass(frozen=True)
class MigrationModifiers(TypeModifiers):
    nullable: bool = False
    low_cardinality: bool = False
    default: Optional[str] = None
    materialized: Optional[str] = None
    codecs: Optional[Sequence[str]] = None

    def _get_modifiers(self) -> Sequence[TypeModifier]:
        ret: List[TypeModifier] = []
        if self.nullable:
            ret.append(Nullable())
        if self.low_cardinality:
            ret.append(LowCardinality())
        if self.default is not None:
            ret.append(WithDefault(self.default))
        if self.materialized is not None:
            ret.append(Materialized(self.materialized))
        if self.codecs is not None:
            ret.append(WithCodecs(self.codecs))
        return ret


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
