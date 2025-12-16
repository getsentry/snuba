from snuba.query.expressions import Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings


class DummyHandledFunctionsProcessor(LogicalQueryProcessor):
    """
    The product keeps sending queries for notHandled/isHandled to transactions and search issues datasets

    Over 3 years I haven't been able to get them to fix it.
    Rather than failing the functions I just assume the error is handled
    """

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def process_functions(exp: Expression) -> Expression:
            if isinstance(exp, FunctionCall):
                if exp.function_name == "isHandled":
                    return Literal(None, 1)
                if exp.function_name == "notHandled":
                    return Literal(None, 0)
            return exp

        query.transform_expressions(process_functions)
