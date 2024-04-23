from dataclasses import replace

from snuba import settings, state
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
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
        self.low_card_columns = set(columns)

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def transform_expressions(exp: Expression) -> Expression:
            if (
                not isinstance(exp, Column)
                or exp.column_name not in self.low_card_columns
            ):
                return exp

            return FunctionCall(
                exp.alias,
                "cast",
                (
                    replace(exp, alias=None),
                    Literal(None, "Nullable(String)"),
                ),
            )

        if (
            state.get_int_config(
                "use.low.cardinality.processor", settings.USE_CARDINALITY_CASTER
            )
            == 0
        ):
            return

        query.transform_expressions(transform_expressions)
