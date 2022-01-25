from typing import Sequence

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Expression
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.expressions import Literal as LiteralExpr
from snuba.query.matchers import (
    Any,
    Column,
    FunctionCall,
    Integer,
    Literal,
    Or,
    Param,
    String,
)
from snuba.request.request_settings import RequestSettings

"""
This optimizer is necessary because SnQL does not permit conditions like
`has(col, val)`, instead it requires them to be written as `has(col, val) = 1`.
This way of writing the condition does not allow ClickHouse to leverage any
bloomfilter index on the column.

This optimizer rewrites these conditions and such that Clickhouse is able to
leverage the bloom filter index if they exist.
"""


class ArrayHasOptimizer(QueryProcessor):
    def __init__(self, array_columns: Sequence[str]):
        column_name = Param("col", Or([String(column) for column in array_columns]))

        self.__array_has_pattern = FunctionCall(
            String("equals"),
            (
                FunctionCall(
                    String("has"),
                    (Column(column_name=column_name), Literal(Param("val", Any(str))),),
                ),
                Literal(Integer(1)),
            ),
        )

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def replace_expression(expr: Expression) -> Expression:
            match = self.__array_has_pattern.match(expr)

            # The arrayJoins we are looking for are not present, so skip this entirely
            if match is None:
                return expr

            col = match.string("col")
            val = match.string("val")

            return FunctionCallExpr(
                None, "has", (ColumnExpr(None, None, col), LiteralExpr(None, val)),
            )

        query.transform_expressions(replace_expression)
