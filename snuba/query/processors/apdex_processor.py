from snuba.query.conditions import (
    binary_condition,
    BooleanFunctions,
    ConditionFunctions,
)
from snuba.query.expressions import (
    Expression,
    FunctionCall,
    Literal,
)
from snuba.query.dsl import div, multiply, plus, count, countIf
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.request.request_settings import RequestSettings


class ApdexProcessor(QueryProcessor):
    """
    Resolves the apdex function call into
    countIf({col} <= {satisfied}) + (countIf(({col} > {satisfied}) AND ({col} <= {tolerated})) / 2)) / count()
    according to the definition of apdex index: https://en.wikipedia.org/wiki/Apdex
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def process_functions(exp: Expression) -> Expression:
            if isinstance(exp, FunctionCall) and exp.function_name == "apdex":
                assert len(exp.parameters) == 2
                column = exp.parameters[0]
                satisfied = exp.parameters[1]
                tolerated = multiply(satisfied, Literal(None, 4))

                return div(
                    plus(
                        countIf(
                            binary_condition(
                                None, ConditionFunctions.LTE, column, satisfied,
                            ),
                        ),
                        div(
                            countIf(
                                binary_condition(
                                    None,
                                    BooleanFunctions.AND,
                                    binary_condition(
                                        None, ConditionFunctions.GT, column, satisfied,
                                    ),
                                    binary_condition(
                                        None, ConditionFunctions.LTE, column, tolerated,
                                    ),
                                ),
                            ),
                            Literal(None, 2),
                        ),
                    ),
                    count(),
                )

            return exp

        query.transform_expressions(process_functions)
