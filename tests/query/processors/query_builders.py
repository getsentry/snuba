from snuba.clickhouse.columns import ColumnSet
from snuba.query.data_source.simple import Table
from typing import Optional, Sequence
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clickhouse.translators.snuba.mappers import build_mapping_expr
from snuba.query import SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.expressions import Column, Expression, FunctionCall, Literal


def build_query(
    selected_columns: Optional[Sequence[Expression]] = None,
    condition: Optional[Expression] = None,
    having: Optional[Expression] = None,
) -> ClickhouseQuery:
    return ClickhouseQuery(
        Table("test", ColumnSet([])),
        selected_columns=[
            SelectedExpression(name=s.alias, expression=s)
            for s in selected_columns or []
        ],
        condition=condition,
        having=having,
    )


def column(name: str, no_alias: bool = False) -> Column:
    return Column(
        alias=name if not no_alias else None, table_name=None, column_name=name
    )


def nested_expression(column: str, key: str) -> FunctionCall:
    return build_mapping_expr(
        alias=f"{column}[{key}]",
        table_name=None,
        col_name=column,
        mapping_key=Literal(None, key),
    )


def nested_condition(
    column_name: str, key: str, operator: str, val: str,
) -> Expression:
    return binary_condition(
        operator, nested_expression(column_name, key), Literal(None, val),
    )
