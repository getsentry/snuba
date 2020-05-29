from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Lambda, Argument, Literal, Expression
from snuba.query.dsl import arrayElement
from snuba.query.matchers import FunctionCall, Column, String, Or, Param
from snuba.request.request_settings import RequestSettings


class ArrayjoinOptimizer(QueryProcessor):
    """
    Apply two optimizations to reduce the performance impact of tags_key
    and tags_value expressions that are sent to clickhouse as
    arrayJoin(tags.key)

    - it performs the arrayJoin only once in case both tags_key and
        tags_value are present by arrayJoining on on the key, value
        tuples instead of arrayJoining twice, once on keys and the second
        on values.
    - prefilter the keys with arrayFilter inside the arrayJoin if there
        are conditions in the query that specify which tags we want
        instead of doing the entire arrayJoin and filter the results.
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        matcher = FunctionCall(
            None,
            String("arrayJoin"),
            (
                Column(
                    None,
                    None,
                    Param("col", Or([String("tags.key"), String("tags.value")])),
                ),
            ),
        )

        referenced_tags = set()
        for e in query.get_all_expressions():
            match = matcher.match(e)
            if match is not None:
                referenced_tags.add(match.string("col"))

        if len(referenced_tags) < 2:
            return

        replaced = FunctionCallExpr(
            "all_tags",
            "arrayJoin",
            (
                FunctionCallExpr(
                    None,
                    "arrayMap",
                    (
                        Lambda(
                            None,
                            ("x", "y"),
                            FunctionCallExpr(
                                None,
                                "tuple",
                                (Argument(None, "x"), Argument(None, "y")),
                            ),
                        ),
                        ColumnExpr(None, None, "tags.key"),
                        ColumnExpr(None, None, "tags.value"),
                    ),
                ),
            ),
        )

        def replace_expression(expr: Expression) -> Expression:
            match = matcher.match(expr)
            if match is None:
                return expr

            return arrayElement(
                expr.alias,
                replaced,
                Literal(None, 1)
                if match.string("col") == "tags.key"
                else Literal(None, 2),
            )

        query.transform_expressions(replace_expression)
