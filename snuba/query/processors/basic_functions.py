from dataclasses import replace

from snuba.query.expressions import (
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Literal,
)
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.request.request_settings import RequestSettings


class BasicFunctionsProcessor(QueryProcessor):
    """
    Mimics the ad hoc function processing that happens today in utils.function_expr.

    This exists only to preserve the current Snuba syntax and only works on the new AST.
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def process_functions(exp: Expression) -> Expression:
            if isinstance(exp, FunctionCall):
                if exp.function_name == "uniq":
                    return FunctionCall(
                        exp.alias,
                        "ifNull",
                        (replace(exp, alias=None), Literal(None, 0),),
                    )
                if exp.function_name == "emptyIfNull":
                    return FunctionCall(
                        exp.alias,
                        "ifNull",
                        (replace(exp, alias=None), Literal(None, ""),),
                    )
            if isinstance(exp, CurriedFunctionCall):
                if exp.internal_function.function_name == "top":
                    return replace(
                        exp,
                        internal_function=replace(
                            exp.internal_function, function_name="topK"
                        ),
                    )
            return exp

        query.transform_expressions(process_functions)
