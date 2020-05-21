from abc import ABC
from dataclasses import dataclass
from typing import Optional, Union

from snuba.clickhouse.translator.snuba import SnubaClickhouseTranslator
from snuba.datasets.plans.translator.mapper import ExpressionMapper
from snuba.query.dsl import array_element
from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)


class LiteralMapper(
    ExpressionMapper[Literal, Literal, SnubaClickhouseTranslator], ABC,
):
    pass


class ColumnMapper(
    ExpressionMapper[
        Column, Union[Column, Literal, FunctionCall], SnubaClickhouseTranslator,
    ],
    ABC,
):
    pass


class FunctionCallMapper(
    ExpressionMapper[FunctionCall, FunctionCall, SnubaClickhouseTranslator], ABC,
):
    pass


class CurriedFunctionCallMapper(
    ExpressionMapper[
        CurriedFunctionCall, CurriedFunctionCall, SnubaClickhouseTranslator,
    ],
    ABC,
):
    pass


class SubscriptableReferenceMapper(
    ExpressionMapper[
        SubscriptableReference,
        Union[FunctionCall, SubscriptableReference],
        SnubaClickhouseTranslator,
    ],
    ABC,
):
    pass


class LambdaMapper(
    ExpressionMapper[Lambda, Lambda, SnubaClickhouseTranslator], ABC,
):
    pass


class ArgumentMapper(
    ExpressionMapper[Argument, Argument, SnubaClickhouseTranslator], ABC,
):
    pass


@dataclass(frozen=True)
class SimpleColumnMapper(ColumnMapper):
    """
    Maps a column with a name and a table into a column with a different name and table.

    The alias is not transformed.
    """

    from_col_name: str
    from_table_name: Optional[str]
    to_col_name: str
    to_table_name: Optional[str]

    def attempt_map(
        self, expression: Column, children_translator: SnubaClickhouseTranslator,
    ) -> Optional[Column]:
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
class TagMapper(SubscriptableReferenceMapper):
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
        expression: SubscriptableReference,
        children_translator: SnubaClickhouseTranslator,
    ) -> Optional[FunctionCall]:
        if (
            expression.column.column_name != self.from_column_name
            or expression.column.table_name != self.from_column_table
        ):
            return None

        return array_element(
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


# TODO: build more of these mappers.
