from snuba.query.exceptions import InvalidExpressionException
from snuba.query.expressions import Expression, Literal, SubscriptableReference
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.query.query_settings import QuerySettings


class TagsTypeTransformer(QueryProcessor):
    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def transform_expression(exp: Expression) -> Expression:
            if not isinstance(exp, SubscriptableReference):
                return exp

            key = exp.key
            if not isinstance(key.value, str) or not key.value.isdigit():
                raise InvalidExpressionException.from_args(
                    exp,
                    "Expected a string key containing an integer in subscriptable.",
                )

            return SubscriptableReference(
                exp.alias, exp.column, Literal(None, int(key.value))
            )

        query.transform_expressions(transform_expression)
