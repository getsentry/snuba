from snuba.query.expressions import Expression, FunctionCall
from snuba.query.logical import Query
from snuba.query.matchers import Column as ColumnMatch
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import Or, String
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings


class CalculatedAverageProcessor(LogicalQueryProcessor):
    """Transforms the expression avg(value) -> sum(value) / count(value)

    This processor was introduced for the gauges entity which has sum and count fields
    but not avg. This processor provides syntactic sugar for the product to be able to avg gauges.

    """

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        # use a matcher to find something like avg(value)
        matcher = FunctionCallMatch(
            Or([String("avg"), String("avgIf")]),
            (ColumnMatch(column_name=String("value")),),
            with_optionals=True,
        )

        def transform_expression(exp: Expression) -> Expression:
            match = matcher.match(exp)
            if isinstance(exp, FunctionCall) and match is not None:
                suffix = "If" if exp.function_name == "avgIf" else ""

                return FunctionCall(
                    alias=exp.alias,
                    function_name="divide",
                    parameters=(
                        FunctionCall(
                            alias=None,
                            function_name=f"sum{suffix}",
                            parameters=exp.parameters,
                        ),
                        FunctionCall(
                            alias=None,
                            function_name=f"count{suffix}",
                            parameters=exp.parameters,
                        ),
                    ),
                )
            return exp

        query.transform_expressions(transform_expression)
