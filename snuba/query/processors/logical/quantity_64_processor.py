from snuba.query.expressions import Column, Expression
from snuba.query.logical import Query
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings


class Quantity64Processor(LogicalQueryProcessor):
    """Rewrites references to the ``quantity`` column to ``quantity64``.

    The outcomes_raw entity exposes both a 32-bit ``quantity`` column and a
    64-bit ``quantity64`` column. This processor transparently redirects queries
    against ``quantity`` to ``quantity64`` so callers always read the wider
    column without needing to update their queries.
    """

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def transform_expression(exp: Expression) -> Expression:
            if isinstance(exp, Column) and exp.column_name == "quantity":
                return Column(
                    alias=exp.alias,
                    table_name=exp.table_name,
                    column_name="quantity64",
                )
            return exp

        query.transform_expressions(transform_expression)
