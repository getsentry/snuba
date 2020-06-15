from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Tuple

from snuba.clickhouse.translators.snuba import SnubaClickhouseStrictTranslator
from snuba.clickhouse.translators.snuba.allowed import (
    ColumnMapper,
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


@dataclass(frozen=True)
class ColumnToExpression(ColumnMapper, ABC):
    """
    Provides some common logic for all the mappers that transform a specific
    column identified by table and name into one of the allowed expressions
    a column can be translated into.
    This exists for convenience just to avoid some code duplication.
    """

    from_table_name: Optional[str]
    from_col_name: str

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

    def attempt_map(
        self,
        expression: SubscriptableReference,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[FunctionCallExpr]:
        if (
            expression.column.table_name == self.from_column_table
            and expression.column.column_name == self.from_column_name
        ):
            return build_mapping_expr(
                expression.alias,
                self.to_nested_col_table,
                self.to_nested_col_name,
                expression.key.accept(children_translator),
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

    def _produce_output(self, expression: ColumnExpr) -> FunctionCallExpr:
        return build_mapping_expr(
            expression.alias,
            self.to_nested_col_table_name,
            self.to_nested_col_name,
            LiteralExpr(None, self.to_nested_mapping_key),
        )


def build_mapping_expr(
    alias: Optional[str],
    table_name: Optional[str],
    col_name: str,
    mapping_key: Expression,
) -> FunctionCallExpr:
    return arrayElement(
        alias,
        ColumnExpr(None, table_name, f"{col_name}.value"),
        FunctionCallExpr(
            None,
            "indexOf",
            (ColumnExpr(None, table_name, f"{col_name}.key"), mapping_key),
        ),
    )


TABLE_MAPPING_PARAM = "table_name"
VALUE_COL_MAPPING_PARAM = "value_column"
KEY_COL_MAPPING_PARAM = "key_column"
KEY_MAPPING_PARAM = "key"
mapping_pattern = FunctionCall(
    None,
    String("arrayElement"),
    (
        Column(
            None,
            Param(TABLE_MAPPING_PARAM, AnyOptionalString()),
            Param(VALUE_COL_MAPPING_PARAM, Any(str)),
        ),
        FunctionCall(
            None,
            String("indexOf"),
            (
                Column(None, None, Param(KEY_COL_MAPPING_PARAM, Any(str))),
                Literal(None, Param(KEY_MAPPING_PARAM, Any(str))),
            ),
        ),
    ),
)

# TODO: build more of these mappers.
