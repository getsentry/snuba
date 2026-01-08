from dataclasses import replace

from snuba.query.expressions import (
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Literal,
)
from snuba.query.logical import Query
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings


class BasicFunctionsProcessor(LogicalQueryProcessor):
    """
    Mimics the ad hoc function processing that happens today in utils.function_expr.

    This exists only to preserve the current Snuba syntax and only works on the new AST.
    """

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def process_functions(exp: Expression) -> Expression:
            if isinstance(exp, FunctionCall):
                if exp.function_name == "uniq":
                    return FunctionCall(
                        exp.alias,
                        "ifNull",
                        (
                            replace(exp, alias=None),
                            Literal(None, 0),
                        ),
                    )
                if exp.function_name == "emptyIfNull":
                    return FunctionCall(
                        exp.alias,
                        "ifNull",
                        (
                            replace(exp, alias=None),
                            Literal(None, ""),
                        ),
                    )
                if exp.function_name == "log":
                    return FunctionCall(
                        exp.alias,
                        "ifNotFinite",
                        (
                            replace(exp, alias=None),
                            Literal(None, 0),
                        ),
                    )
            if isinstance(exp, CurriedFunctionCall):
                if exp.internal_function.function_name == "top":
                    return replace(
                        exp,
                        internal_function=replace(exp.internal_function, function_name="topK"),
                    )
            return exp

        query.transform_expressions(process_functions)
