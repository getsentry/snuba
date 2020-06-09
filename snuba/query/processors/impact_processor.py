from snuba.query.dsl import divide, minus, multiply, plus
from snuba.query.expressions import (
    Expression,
    FunctionCall,
    Literal,
)
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.query.processors.performance_expressions import apdex
from snuba.request.request_settings import RequestSettings


class ImpactProcessor(QueryProcessor):
    """
    Resolves the impact function call into

    impact = (1 - (apdex({col}, {satisfied}))) + ((1 - (1 / sqrt(uniq({user_col})))) * 3)
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def process_functions(exp: Expression) -> Expression:
            if isinstance(exp, FunctionCall) and exp.function_name == "impact":
                assert len(exp.parameters) == 3
                column = exp.parameters[0]
                satisfied = exp.parameters[1]
                user_column = exp.parameters[2]

                return plus(
                    minus(Literal(None, 1), apdex(column, satisfied)),
                    multiply(
                        minus(
                            Literal(None, 1),
                            divide(
                                Literal(None, 1),
                                FunctionCall(
                                    None,
                                    "sqrt",
                                    (FunctionCall(None, "uniq", user_column)),
                                ),
                            ),
                        ),
                        Literal(None, 3),
                    ),
                )

            return exp

        query.transform_expressions(process_functions)
