from typing import cast

from snuba.query.expressions import Column, Expression, FunctionCall
from snuba.query.logical import Query
from snuba.query.matchers import Column as ColumnMatch
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import Param, String
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings


class CalculatedAverageProcessor(LogicalQueryProcessor):
    """Transforms the expression avg(value) -> sum(value) / count(value)

    This processor was introduced for the gauges entity which has sum and count fields
    but not avg. This processor provides syntactic sugar for the product to be able to avg gauges.

    """

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        # use a matched to find something like avg(value)
        matcher = FunctionCallMatch(
            String("avg"),
            (Param("avg_column", ColumnMatch(column_name=String("value"))),),
        )

        def transform_expression(exp: Expression) -> Expression:
            match = matcher.match(exp)
            if match is not None:
                column = cast(Column, match.expression("avg_column"))
                return FunctionCall(
                    alias=exp.alias,
                    function_name="divide",
                    parameters=(
                        FunctionCall(
                            alias=None,
                            function_name="sum",
                            parameters=(
                                Column(
                                    alias=column.alias,
                                    table_name=column.table_name,
                                    column_name="value",
                                ),
                            ),
                        ),
                        FunctionCall(
                            alias=None,
                            function_name="count",
                            parameters=(
                                Column(
                                    alias=column.alias,
                                    table_name=column.table_name,
                                    column_name="value",
                                ),
                            ),
                        ),
                    ),
                )
            return exp

        query.transform_expressions(transform_expression)
