import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Tuple

from snuba.clickhouse.translators.snuba import SnubaClickhouseStrictTranslator
from snuba.clickhouse.translators.snuba.allowed import (
    ColumnMapper,
    FunctionCallMapper,
    SubscriptableReferenceMapper,
    ValidColumnMappings,
)
from snuba.query.dsl import arrayElement
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import CurriedFunctionCall, Expression
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.expressions import Literal as LiteralExpr
from snuba.query.expressions import OptionalScalarType, SubscriptableReference
from snuba.query.matchers import (
    Any,
    AnyOptionalString,
    Column,
    FunctionCall,
    Literal,
    Param,
    String,
)
from snuba.utils.registered_class import import_submodules_in_directory


# This is a workaround for a mypy bug, found here: https://github.com/python/mypy/issues/5374
@dataclass(frozen=True)
class _ColumnToExpression:
    from_table_name: Optional[str]
    from_col_name: str


class ColumnToExpression(_ColumnToExpression, ColumnMapper, ABC):
    """
    Provides some common logic for all the mappers that transform a specific
    column identified by table and name into one of the allowed expressions
    a column can be translated into.
    This exists for convenience just to avoid some code duplication.
    """

    def attempt_map(
        self,
        expression: ColumnExpr,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[ValidColumnMappings]:
        if (
            expression.column_name == self.from_col_name
            and expression.table_name == self.from_table_name
        ):
            return self._produce_output(expression)
        else:
            return None

    @abstractmethod
    def _produce_output(self, expression: ColumnExpr) -> ValidColumnMappings:
        raise NotImplementedError


@dataclass(frozen=True)
class ColumnToColumn(ColumnToExpression):
    """
    Maps a column with a name and a table into a column with a different name and table.

    The alias is not transformed.
    """

    to_table_name: Optional[str]
    to_col_name: str

    def _produce_output(self, expression: ColumnExpr) -> ColumnExpr:
        return ColumnExpr(
            alias=expression.alias,
            table_name=self.to_table_name,
            column_name=self.to_col_name,
        )


@dataclass(frozen=True)
class ColumnToLiteral(ColumnToExpression):
    """
    Maps a column name into a hardcoded literal preserving the alias.
    """

    to_literal_value: OptionalScalarType

    def _produce_output(self, expression: ColumnExpr) -> LiteralExpr:
        return LiteralExpr(alias=expression.alias, value=self.to_literal_value)


@dataclass(frozen=True)
class ColumnToFunction(ColumnToExpression):
    """
    Maps a column into a function expression that preserves the alias.
    """

    to_function_name: str
    to_function_params: Tuple[Expression, ...]

    def _produce_output(self, expression: ColumnExpr) -> FunctionCallExpr:
        return FunctionCallExpr(
            alias=expression.alias,
            function_name=self.to_function_name,
            parameters=self.to_function_params,
        )


@dataclass(frozen=True)
class ColumnToFunctionOnColumn(ColumnToExpression):
    """
    Maps a column into a function expression that preserves the alias.
    """

    to_function_name: str
    to_function_column: str

    def _produce_output(self, expression: ColumnExpr) -> FunctionCallExpr:
        return FunctionCallExpr(
            alias=expression.alias,
            function_name=self.to_function_name,
            parameters=(
                ColumnExpr(None, expression.table_name, self.to_function_column),
            ),
        )


@dataclass(frozen=True)
class ColumnToIPAddress(ColumnToFunction):
    """
    Custom column mapper for mapping columns to IP Address.
    TODO: Can remove when we support dynamic expression parsing in config
    """

    def __init__(self, from_table_name: Optional[str], from_col_name: str) -> None:
        to_function_params: Tuple[FunctionCallExpr, ...] = (
            FunctionCallExpr(
                None,
                "IPv4NumToString",
                (ColumnExpr(None, None, "ip_address_v4"),),
            ),
            FunctionCallExpr(
                None,
                "IPv6NumToString",
                (ColumnExpr(None, None, "ip_address_v6"),),
            ),
        )
        super().__init__(from_table_name, from_col_name, "coalesce", to_function_params)


@dataclass(frozen=True)
class ColumnToNullIf(ColumnToFunction):
    """
    Custom column mapper for mapping columns to null.
    TODO: Can remove when we support dynamic expression parsing in config
    """

    def __init__(self, from_table_name: Optional[str], from_col_name: str) -> None:
        to_function_params: Tuple[ColumnExpr, LiteralExpr] = (
            ColumnExpr(None, from_table_name, from_col_name),
            LiteralExpr(None, ""),
        )
        super().__init__(from_table_name, from_col_name, "nullIf", to_function_params)


@dataclass(frozen=True)
class ColumnToCurriedFunction(ColumnToExpression):
    """
    Maps a column into a curried function expression that preserves the alias.
    """

    to_internal_function: FunctionCallExpr
    to_function_params: Tuple[Expression, ...]

    def _produce_output(self, expression: ColumnExpr) -> CurriedFunctionCall:
        return CurriedFunctionCall(
            alias=expression.alias,
            internal_function=self.to_internal_function,
            parameters=self.to_function_params,
        )


@dataclass(frozen=True)
class SubscriptableMapper(SubscriptableReferenceMapper):
    """
    Basic implementation of a tag mapper that transforms a subscriptable
    into a Clickhouse array access.
    """

    from_column_table: Optional[str]
    from_column_name: str
    to_nested_col_table: Optional[str]
    to_nested_col_name: str
    value_subcolumn_name: str = "value"
    nullable: bool = False

    def attempt_map(
        self,
        expression: SubscriptableReference,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[FunctionCallExpr]:
        if (
            expression.column.table_name == self.from_column_table
            and expression.column.column_name == self.from_column_name
        ):
            key = expression.key.accept(children_translator)
            return (
                build_mapping_expr(
                    expression.alias,
                    self.to_nested_col_table,
                    self.to_nested_col_name,
                    key,
                    self.value_subcolumn_name,
                )
                if not self.nullable
                else build_nullable_mapping_expr(
                    expression.alias,
                    self.to_nested_col_table,
                    self.to_nested_col_name,
                    key,
                    self.value_subcolumn_name,
                )
            )
        else:
            return None


@dataclass(frozen=True)
class ColumnToMapping(ColumnToExpression):
    """
    Maps a column into a mapping expression thus into a Clickhouse
    array access.
    """

    to_nested_col_table_name: Optional[str]
    to_nested_col_name: str
    to_nested_mapping_key: str
    nullable: bool = False

    def _produce_output(self, expression: ColumnExpr) -> FunctionCallExpr:
        if not self.nullable:
            return build_mapping_expr(
                expression.alias,
                self.to_nested_col_table_name,
                self.to_nested_col_name,
                LiteralExpr(None, self.to_nested_mapping_key),
                "value",
            )
        else:
            return build_nullable_mapping_expr(
                expression.alias,
                self.to_nested_col_table_name,
                self.to_nested_col_name,
                LiteralExpr(None, self.to_nested_mapping_key),
                "value",
            )


def build_mapping_expr(
    alias: Optional[str],
    table_name: Optional[str],
    col_name: str,
    mapping_key: Expression,
    value_subcolumn_name: str,
) -> FunctionCallExpr:
    return arrayElement(
        alias,
        ColumnExpr(None, table_name, f"{col_name}.{value_subcolumn_name}"),
        FunctionCallExpr(
            None,
            "indexOf",
            (ColumnExpr(None, table_name, f"{col_name}.key"), mapping_key),
        ),
    )


def build_nullable_mapping_expr(
    alias: Optional[str],
    table_name: Optional[str],
    col_name: str,
    mapping_key: Expression,
    value_subcolumn_name: str,
) -> FunctionCallExpr:
    # TODO: Add a pattern for this expression if we need it.
    return FunctionCallExpr(
        alias,
        "if",
        (
            FunctionCallExpr(
                None,
                "has",
                (ColumnExpr(None, table_name, f"{col_name}.key"), mapping_key),
            ),
            build_mapping_expr(
                None, table_name, col_name, mapping_key, value_subcolumn_name
            ),
            LiteralExpr(None, None),
        ),
    )


TABLE_MAPPING_PARAM = "table_name"
VALUE_COL_MAPPING_PARAM = "value_column"
KEY_COL_MAPPING_PARAM = "key_column"
KEY_MAPPING_PARAM = "key"
mapping_pattern = FunctionCall(
    String("arrayElement"),
    (
        Column(
            Param(TABLE_MAPPING_PARAM, AnyOptionalString()),
            Param(VALUE_COL_MAPPING_PARAM, Any(str)),
        ),
        FunctionCall(
            String("indexOf"),
            (
                Column(None, Param(KEY_COL_MAPPING_PARAM, Any(str))),
                Literal(Param(KEY_MAPPING_PARAM, Any(str))),
            ),
        ),
    ),
)


@dataclass(frozen=True)
class FunctionNameMapper(FunctionCallMapper):
    """
    Translates a function name into another preserving aliases
    and parameters. Parameters are translated on their own through
    the children translator.
    """

    from_name: str
    to_name: str

    def attempt_map(
        self,
        expression: FunctionCallExpr,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[FunctionCallExpr]:
        if expression.function_name != self.from_name:
            return None

        return FunctionCallExpr(
            alias=expression.alias,
            function_name=self.to_name,
            parameters=tuple(
                exp.accept(children_translator) for exp in expression.parameters
            ),
        )


# TODO: build more of these mappers.

import_submodules_in_directory(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "legacy_mappers_we_cant_delete"
    ),
    "snuba.clickhouse.translators.snuba.legacy_mappers_we_cant_delete",
)
