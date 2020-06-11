from snuba.query.expressions import (
    Expression,
    FunctionCall,
)
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.query.processors.performance_expressions import apdex
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
                return apdex(exp.alias, column, satisfied)

            return exp

        query.transform_expressions(process_functions)
