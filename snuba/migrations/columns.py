from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from snuba.clickhouse.columns import (
    Nullable,
    SchemaModifiers,
    TypeModifier,
    TypeModifiers,
)


@dataclass(frozen=True)
class MigrationModifiers(TypeModifiers):
    """
    Modifiers to be used in migrations.
    """

    nullable: bool = False
    low_cardinality: bool = False
    default: str | None = None
    materialized: str | None = None
    codecs: Sequence[str] | None = None
    ttl: str | None = None

    def _get_modifiers(self) -> Sequence[TypeModifier]:
        ret: list[TypeModifier] = []
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
            materialized=self.materialized if other.materialized is None else other.materialized,
            codecs=self.codecs if other.codecs is None else other.codecs,
            ttl=self.ttl if other.ttl is None else other.ttl,
        )

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SchemaModifiers):
            return self.nullable == other.nullable
        if isinstance(other, MigrationModifiers):
            return (
                self.nullable == other.nullable
                and self.low_cardinality == other.low_cardinality
                and self.default == other.default
                and self.materialized == other.materialized
                and self.codecs == other.codecs
                and self.ttl == other.ttl
            )
        return False


@dataclass(frozen=True)
class Materialized(TypeModifier):
    expression: str

    def for_schema(self, content: str) -> str:
        return f"{content} MATERIALIZED {self.expression}"


@dataclass(frozen=True)
class WithCodecs(TypeModifier):
    codecs: Sequence[str]

    def for_schema(self, content: str) -> str:
        return f"{content} CODEC ({', '.join(self.codecs)})"


@dataclass(frozen=True)
class WithDefault(TypeModifier):
    default: str

    def for_schema(self, content: str) -> str:
        return f"{content} DEFAULT {self.default}"


@dataclass(frozen=True)
class LowCardinality(TypeModifier):
    def for_schema(self, content: str) -> str:
        return f"LowCardinality({content})"


@dataclass(frozen=True)
class WithTTL(TypeModifier):
    ttl_clause: str

    def for_schema(self, content: str) -> str:
        return f"{content} TTL {self.ttl_clause}"
