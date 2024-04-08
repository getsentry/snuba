from snuba.query.expressions import (
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Literal,
)
from snuba.query.logical import Query
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings


class CurriedFunctionBucketTransformer(LogicalQueryProcessor):
    """
    Clickhouse has a bug where if the parameters specified in the aggregate function
    function doesn't match the number preserved in the aggregate state, it returns
    an error. Default all curried function calls of the function specified to use
    the max buckets specified.
    """

    def __init__(
        self,
        curried_function: str,
        default_parameter: int,
    ):
        self.curried_function = curried_function
        self.default_parameter = default_parameter

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def transform_expression(exp: Expression) -> Expression:
            if not isinstance(exp, CurriedFunctionCall):
                return exp

            if not exp.internal_function.function_name.startswith(
                self.curried_function
            ):
                return exp

            new_internal = FunctionCall(
                exp.internal_function.alias,
                exp.internal_function.function_name,
                (Literal(None, self.default_parameter),),
            )

            return CurriedFunctionCall(
                exp.alias,
                new_internal,
                exp.parameters,
            )

        query.transform_expressions(transform_expression)
