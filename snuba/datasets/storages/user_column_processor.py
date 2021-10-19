from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.request.request_settings import RequestSettings


class UserColumnProcessor(QueryProcessor):
    """
    Return null instead of empty user to align errors to events storage behavior.
    This translation is applied as a column processor rather than a translator, so
    that it will be properly applied to the promoted sentry:user tag.
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def process_column(exp: Expression) -> Expression:
            if isinstance(exp, Column):
                if exp.column_name == "user":
                    return FunctionCall(
                        exp.alias,
                        "nullIf",
                        (Column(None, None, "user"), Literal(None, "")),
                    )

            return exp

        query.transform_expressions(process_column)
