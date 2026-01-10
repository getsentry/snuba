from typing import Sequence

from snuba.clickhouse.query import Query
from snuba.query.expressions import Expression
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
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings

"""
This optimizer is necessary because SnQL does not permit conditions like
`has(col, val)`, instead it requires them to be written as `has(col, val) = 1`.
This way of writing the condition does not allow ClickHouse to leverage any
bloomfilter index on the column.

This optimizer rewrites these conditions and such that Clickhouse is able to
leverage the bloom filter index if they exist.
"""


class ArrayHasOptimizer(ClickhouseQueryProcessor):
    def __init__(self, array_columns: Sequence[str]):
        self.__array_has_pattern = FunctionCall(
            String("equals"),
            (
                Param(
                    "has",
                    FunctionCall(
                        String("has"),
                        (
                            Column(column_name=Or([String(column) for column in array_columns])),
                            Literal(Any(str)),
                        ),
                    ),
                ),
                Literal(Integer(1)),
            ),
        )

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def replace_expression(expr: Expression) -> Expression:
            match = self.__array_has_pattern.match(expr)

            # The has condition we are looking for are not present, so skip this entirely
            if match is None:
                return expr

            return match.expression("has")

        query.transform_expressions(replace_expression)
