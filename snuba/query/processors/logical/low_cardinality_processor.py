from dataclasses import replace

from snuba import state
from snuba.query.expressions import (
    Column,
    Expression,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings


class LowCardinalityProcessor(LogicalQueryProcessor):
    """
    22.8 has some problems when dealing with low cardinality string columns,
    where the actual values in production are high cardinality. For example, in some
    datasets `release` is a LowCardinality(String), but in practice it has a high cardinality.

    This processor will fetch all the LowCardinality columns in the Entity, and wrap them in
    `cast(ifNull(column, ''), 'String')` which strips the LowCardinality property and avoids
    the error in 22.8

    Which columns to use is hardcoded in, because whether or not a column is low cardinality
    is not necessarily exposed in the schema definitions.
    """

    def __init__(self, columns: list[str]) -> None:
        self.low_card_columns = set()
        self.subscriptable_columns: dict[str, set[str]] = dict()
        for c in columns:
            if c.startswith("tags") or c.startswith("contexts"):
                column, key = c.split("[")
                key = key[:-1]
                self.subscriptable_columns.setdefault(column, set()).add(key)
            else:
                self.low_card_columns.add(c)

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def transform_expressions(exp: Expression) -> Expression:
            if isinstance(exp, Column) and exp.column_name in self.low_card_columns:
                return FunctionCall(
                    exp.alias,
                    "cast",
                    (
                        replace(exp, alias=None),
                        Literal(None, "Nullable(String)"),
                    ),
                )
            if isinstance(exp, SubscriptableReference):
                column = exp.column.column_name
                key = exp.key.value
                if (
                    column in self.subscriptable_columns
                    and key in self.subscriptable_columns[column]
                ):
                    return FunctionCall(
                        exp.alias,
                        "cast",
                        (
                            replace(exp, alias=None),
                            Literal(None, "Nullable(String)"),
                        ),
                    )
            return exp

        if state.get_int_config("use.low.cardinality.processor", 1) == 0:
            return

        query.transform_expressions(transform_expressions)
