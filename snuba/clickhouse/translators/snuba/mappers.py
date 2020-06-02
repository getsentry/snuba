from dataclasses import dataclass
from typing import Optional, Tuple

from snuba.clickhouse.translators.snuba import SnubaClickhouseStrictTranslator
from snuba.clickhouse.translators.snuba.allowed import (
    ColumnMapper,
    SubscriptableReferenceMapper,
)
from snuba.query.dsl import arrayElement
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Expression
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
from snuba.util import qualified_column


@dataclass(frozen=True)
class ColumnToColumn(ColumnMapper):
    """
    Maps a column with a name and a table into a column with a different name and table.

    The alias is not transformed.
    """

    from_table_name: Optional[str]
    from_col_name: str
    to_table_name: Optional[str]
    to_col_name: str

    def attempt_map(
        self,
        expression: ColumnExpr,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[ColumnExpr]:
        if (
            expression.column_name == self.from_col_name
            and expression.table_name == self.from_table_name
        ):
            return ColumnExpr(
                alias=expression.alias
                or qualified_column(self.from_col_name, self.from_table_name or ""),
                table_name=self.to_table_name,
                column_name=self.to_col_name,
            )
        else:
            return None


@dataclass(frozen=True)
class ColumnToLiteral(ColumnMapper):
    """
    Maps a column name into a hardcoded literal preserving the alias.
    """

    from_table_name: Optional[str]
    from_col_name: str
    to_literal_value: OptionalScalarType

    def attempt_map(
        self,
        expression: ColumnExpr,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[LiteralExpr]:
        if (
            expression.table_name == self.from_table_name
            and expression.column_name == self.from_col_name
        ):
            return LiteralExpr(
                alias=expression.alias
                or qualified_column(self.from_col_name, self.from_table_name or ""),
                value=self.to_literal_value,
            )
        else:
            return None


@dataclass(frozen=True)
class ColumnToFunction(ColumnMapper):
    """
    Maps a column into a function expression that preserves the alias.
    """

    from_table_name: Optional[str]
    from_col_name: str
    to_function_name: str
    to_function_params: Tuple[Expression, ...]

    def attempt_map(
        self,
        expression: ColumnExpr,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[FunctionCallExpr]:
        if (
            expression.table_name == self.from_table_name
            and expression.column_name == self.from_col_name
        ):
            return FunctionCallExpr(
                alias=expression.alias
                or qualified_column(self.from_col_name, self.from_table_name or ""),
                function_name=self.to_function_name,
                parameters=self.to_function_params,
            )
        else:
            return None


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
class ColumnToMapping(ColumnMapper):
    """
    Maps a column into a mapping expression thus into a Clickhouse
    array access.
    """

    from_column_table: Optional[str]
    from_column_name: str
    to_nested_col_table: Optional[str]
    to_nested_col_name: str
    to_nested_tag_key: str

    def attempt_map(
        self,
        expression: ColumnExpr,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[FunctionCallExpr]:
        if (
            expression.table_name == self.from_column_table
            and expression.column_name == self.from_column_name
        ):
            return build_mapping_expr(
                expression.alias
                or qualified_column(
                    self.from_column_name, self.from_column_table or ""
                ),
                self.to_nested_col_table,
                self.to_nested_col_name,
                LiteralExpr(None, self.to_nested_tag_key),
            )
        else:
            return None


def build_mapping_expr(
    alias: Optional[str], table_name: Optional[str], col_name: str, tag_key: Expression
) -> FunctionCallExpr:
    return arrayElement(
        alias,
        ColumnExpr(None, table_name, f"{col_name}.value"),
        FunctionCallExpr(
            None, "indexOf", (ColumnExpr(None, table_name, f"{col_name}.key"), tag_key),
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


def get_mapping_col_name(nested_col: str) -> str:
    """
    Splits the tag.key/tag.value column names and return the base column
    name. This exists as long as we do not have structured Column classes
    in the AST in order not to spread this logic around the code base.
    """
    return nested_col.split(".")[0]


# TODO: build more of these mappers.
