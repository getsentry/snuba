from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.expressions import Expression, FunctionCall

from snuba.request.request_settings import RequestSettings


class SliceOfMapOptimizer(QueryProcessor):
    """
    Convert `arraySlice(arrayMap(...))` to `arrayMap(arraySlice(...))`. This is
    a pattern often produced by UUIDArrayColumnProcessor.
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        query.transform_expressions(self._process_expressions)

    def _process_expressions(self, exp: Expression) -> Expression:
        if isinstance(exp, FunctionCall) and exp.function_name == "arraySlice":
            inner_exp = exp.parameters[0]

            if (
                isinstance(inner_exp, FunctionCall)
                and inner_exp.function_name == "arrayMap"
            ):
                lambda_fn = inner_exp.parameters[0]
                innermost_exp = inner_exp.parameters[1]
                slice_args = exp.parameters[1:]

                return FunctionCall(
                    exp.alias,
                    "arrayMap",
                    (
                        lambda_fn,
                        FunctionCall(
                            None, "arraySlice", (innermost_exp,) + slice_args,
                        ),
                    ),
                )

        return exp
