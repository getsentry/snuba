from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence, Union

from snuba.clickhouse.columns import Nullable, TypeModifier, TypeModifiers


@dataclass(frozen=True)
class MigrationModifiers(TypeModifiers):
    """
    Modifiers to be used in migrations.
    """

    nullable: bool = False
    low_cardinality: bool = False
    default: Union[None, str, int] = None
    materialized: Optional[str] = None
    codecs: Optional[Sequence[str]] = None
    ttl: Optional[str] = None

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
        if self.ttl is not None:
            ret.append(WithTTL(self.ttl))
        return ret

    def merge(self, other: MigrationModifiers) -> MigrationModifiers:
        return MigrationModifiers(
            nullable=self.nullable | other.nullable,
            low_cardinality=self.low_cardinality | other.low_cardinality,
            default=self.default if other.default is None else other.default,
            materialized=self.materialized
            if other.materialized is None
            else other.materialized,
            codecs=self.codecs if other.codecs is None else other.codecs,
            ttl=self.ttl if other.ttl is None else other.ttl,
        )


@dataclass(frozen=True)
class Materialized(TypeModifier):
    expression: str

    def for_schema(self, content: str) -> str:
        return "{} MATERIALIZED {}".format(content, self.expression)


@dataclass(frozen=True)
class WithCodecs(TypeModifier):
    codecs: Sequence[str]

    def for_schema(self, content: str) -> str:
        return f"{content} CODEC ({', '.join(self.codecs)})"


@dataclass(frozen=True)
class WithDefault(TypeModifier):
    default: Union[None, str, int]

    def for_schema(self, content: str) -> str:
        return "{} DEFAULT {}".format(content, self.default)


@dataclass(frozen=True)
class LowCardinality(TypeModifier):
    def for_schema(self, content: str) -> str:
        return "LowCardinality({})".format(content)


@dataclass(frozen=True)
class WithTTL(TypeModifier):
    ttl_clause: str

    def for_schema(self, content: str) -> str:
        return f"{content} TTL {self.ttl_clause}"
