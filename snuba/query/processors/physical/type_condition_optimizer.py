from snuba.clickhouse.processors import ClickhouseQueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.expressions import Expression
from snuba.query.expressions import Literal as LiteralExpr
from snuba.query.matchers import Column, FunctionCall, Literal, String
from snuba.query.query_settings import QuerySettings


class TypeConditionOptimizer(ClickhouseQueryProcessor):
    """
    Temporary processor that optimizes the type condition by stripping
    any condition matching type != transaction on the errrors storage.
    This condition will eventually be removed from all queries but is
    required for compatibility with the events storage.
    """

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def replace_exp(exp: Expression) -> Expression:
            matcher = FunctionCall(
                String("notEquals"),
                (Column(None, String("type")), Literal(String("transaction"))),
            )

            if matcher.match(exp):
                return LiteralExpr(None, 1)

            return exp

        query.transform_expressions(replace_exp)
