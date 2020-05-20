from dataclasses import dataclass
from typing import Optional

from snuba.clickhouse.query import Expression as ClickhouseExpression
from snuba.query.expressions import Expression as SnubaExpression
from snuba.clickhouse.translator.visitor import ClickhouseExpressionTranslator
from snuba.datasets.plans.translator.visitor import ExpressionMapper
from snuba.query.dsl import array_element
from snuba.query.expressions import Column
from snuba.query.expressions import FunctionCall, SubscriptableReference


@dataclass(frozen=True)
class ColumnMapper(
    ExpressionMapper[
        SnubaExpression, Column, ClickhouseExpressionTranslator[SnubaExpression]
    ]
):
    """
    Maps a column with a name and a table into a column with a different name and table.

    The alias is not transformed.
    """

    from_col_name: str
    from_table_name: Optional[str]
    to_col_name: str
    to_table_name: Optional[str]

    def attempt_map(
        self,
        expression: SnubaExpression,
        children_translator: ClickhouseExpressionTranslator[SnubaExpression],
    ) -> Optional[Column]:
        if not isinstance(expression, Column):
            return None
        if (
            expression.column_name == self.from_col_name
            and expression.table_name == self.from_table_name
        ):
            return Column(
                alias=expression.alias,
                table_name=self.to_table_name,
                column_name=self.to_col_name,
            )
        else:
            return None


@dataclass(frozen=True)
class TagMapper(
    ExpressionMapper[
        SnubaExpression,
        ClickhouseExpression,
        ClickhouseExpressionTranslator[SnubaExpression],
    ]
):
    """
    Basic implementation of a tag mapper that transforms a subscriptable
    into a Clickhouse array access.
    """

    from_column_name: str
    from_column_table: Optional[str]
    to_col_name: str
    to_table_name: Optional[str]

    def attempt_map(
        self,
        expression: SnubaExpression,
        children_translator: ClickhouseExpressionTranslator[SnubaExpression],
    ) -> Optional[ClickhouseExpression]:
        if not isinstance(expression, SubscriptableReference):
            return None
        if (
            expression.column.column_name != self.from_column_name
            or expression.column.table_name != self.from_column_table
        ):
            return None

        return ClickhouseExpression(
            array_element(
                expression.alias,
                Column(None, f"{self.to_col_name}.value", self.to_table_name),
                FunctionCall(
                    None,
                    "indexOf",
                    (
                        Column(None, f"{self.to_col_name}.key", self.to_table_name,),
                        children_translator.translate_expression(expression.key),
                    ),
                ),
            )
        )


# TODO: build more of these mappers.
